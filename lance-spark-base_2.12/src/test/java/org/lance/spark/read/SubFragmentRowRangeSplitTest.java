/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.read;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubFragmentRowRangeSplitTest {

  private static List<FragmentMetadata> createFragment(
      BufferAllocator allocator,
      String uri,
      Schema schema,
      String category,
      long startId,
      int numRows) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector catVec = (VarCharVector) root.getVector("cat");
      VarBinaryVector payloadVec = (VarBinaryVector) root.getVector("payload");
      idVec.allocateNew();
      catVec.allocateNew();
      payloadVec.allocateNew();
      byte[] catBytes = category.getBytes(StandardCharsets.UTF_8);
      byte[] payload = new byte[512];
      Arrays.fill(payload, (byte) 11);
      for (int i = 0; i < numRows; i++) {
        idVec.setSafe(i, startId + i);
        catVec.setSafe(i, catBytes);
        payloadVec.setSafe(i, payload);
      }
      root.setRowCount(numRows);
      return Fragment.create(uri, allocator, root, new WriteParams.Builder().build());
    }
  }

  private static List<Long> readIdsFromScan(LanceScan scan) throws Exception {
    InputPartition[] partitions = scan.planInputPartitions();
    List<Long> ids = new ArrayList<>();
    for (InputPartition part : partitions) {
      try (LanceColumnarPartitionReader reader =
          new LanceColumnarPartitionReader((LanceInputPartition) part)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          for (int i = 0; i < batch.numRows(); i++) {
            ids.add(batch.column(0).getLong(i));
            assertEquals(512, batch.column(1).getBinary(i).length);
          }
          batch.close();
        }
      }
    }
    Collections.sort(ids);
    return ids;
  }

  @Test
  void testUnfilteredAndFilteredSubFragmentSplitsWithDeletions(@TempDir Path tempDir)
      throws Exception {
    String uri = tempDir.resolve("sub_fragment_splits.lance").toString();
    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("cat", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)));

    try (BufferAllocator alloc = new RootAllocator()) {
      List<FragmentMetadata> f0 = createFragment(alloc, uri, arrowSchema, "A", 0L, 250);
      List<FragmentMetadata> f1 = createFragment(alloc, uri, arrowSchema, "B", 1000L, 70);
      List<FragmentMetadata> initial = Arrays.asList(f0.get(0), f1.get(0));
      Dataset.commit(
              alloc, uri, new FragmentOperation.Overwrite(initial, arrowSchema), Optional.empty())
          .close();
    }

    StructType fullSchema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("cat", DataTypes.StringType)
            .add("payload", DataTypes.BinaryType);
    StructType readSchema =
        new StructType().add("id", DataTypes.LongType).add("payload", DataTypes.BinaryType);

    // 1. Unfiltered scan without deletions (exercises native offset/limit fast path)
    LanceSparkReadOptions baseOpts = LanceSparkReadOptions.builder().datasetUri(uri).build();
    LanceScanBuilder baseBuilder =
        new LanceScanBuilder(
            fullSchema, baseOpts, Collections.emptyMap(), null, Collections.emptyMap());
    baseBuilder.pruneColumns(readSchema);
    LanceScan baseScan = (LanceScan) baseBuilder.build();
    assertEquals(2, baseScan.planInputPartitions().length);
    List<Long> expectedUnfiltered = readIdsFromScan(baseScan);
    assertEquals(320, expectedUnfiltered.size());

    LanceSparkReadOptions slicedOpts =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_MAX_ROWS_PER_SPLIT, "80"), uri);
    assertEquals(80L, slicedOpts.getMaxRowsPerSplit());
    LanceScanBuilder slicedBuilder =
        new LanceScanBuilder(
            fullSchema, slicedOpts, Collections.emptyMap(), null, Collections.emptyMap());
    slicedBuilder.pruneColumns(readSchema);
    LanceScan slicedScan = (LanceScan) slicedBuilder.build();
    // f0 (250 rows) -> ceil(250/80) = 4 splits; f1 (70 rows <= 80) -> 1 split => total 5 splits
    assertEquals(5, slicedScan.planInputPartitions().length);
    assertEquals(expectedUnfiltered, readIdsFromScan(slicedScan));

    // LIMIT scan keeps whole-fragment splits so limit pruning semantics stay intact
    LanceScanBuilder limitBuilder =
        new LanceScanBuilder(
            fullSchema, slicedOpts, Collections.emptyMap(), null, Collections.emptyMap());
    limitBuilder.pruneColumns(readSchema);
    limitBuilder.pushLimit(50);
    LanceScan limitScan = (LanceScan) limitBuilder.build();
    assertEquals(1, limitScan.planInputPartitions().length);
    assertTrue(
        !((LanceInputPartition) limitScan.planInputPartitions()[0]).getLanceSplit().hasRowRange());

    // 2. Delete rows in f0 & f1, then test both unfiltered-with-deletions and filtered scans
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open(uri, alloc)) {
      ds.delete("id IN (0, 79, 80, 160, 249, 1010)");
    }

    LanceScan postDeleteUnsliced =
        (LanceScan)
            new LanceScanBuilder(
                    fullSchema, baseOpts, Collections.emptyMap(), null, Collections.emptyMap())
                .build();
    // Rebuild with pruned columns for comparison
    LanceScanBuilder postDeleteBaseBuilder =
        new LanceScanBuilder(
            fullSchema, baseOpts, Collections.emptyMap(), null, Collections.emptyMap());
    postDeleteBaseBuilder.pruneColumns(readSchema);
    postDeleteUnsliced = (LanceScan) postDeleteBaseBuilder.build();
    List<Long> expectedPostDelete = readIdsFromScan(postDeleteUnsliced);
    assertEquals(314, expectedPostDelete.size());

    LanceScanBuilder postDeleteSlicedBuilder =
        new LanceScanBuilder(
            fullSchema, slicedOpts, Collections.emptyMap(), null, Collections.emptyMap());
    postDeleteSlicedBuilder.pruneColumns(readSchema);
    LanceScan postDeleteSliced = (LanceScan) postDeleteSlicedBuilder.build();
    assertEquals(5, postDeleteSliced.planInputPartitions().length);
    assertEquals(expectedPostDelete, readIdsFromScan(postDeleteSliced));

    // Filtered scan with deletions (_rowaddr sub-range + pushed filter)
    Predicate catEqA =
        new Predicate(
            "=",
            new Expression[] {
              FieldReference.column("cat"),
              new LiteralValue<>(UTF8String.fromString("A"), DataTypes.StringType)
            });
    Predicate idGte25 =
        new Predicate(
            ">=",
            new Expression[] {
              FieldReference.column("id"), new LiteralValue<>(25L, DataTypes.LongType)
            });

    LanceScanBuilder filteredBaseBuilder =
        new LanceScanBuilder(
            fullSchema, baseOpts, Collections.emptyMap(), null, Collections.emptyMap());
    filteredBaseBuilder.pruneColumns(readSchema);
    filteredBaseBuilder.pushPredicates(new Predicate[] {catEqA, idGte25});
    List<Long> expectedFiltered = readIdsFromScan((LanceScan) filteredBaseBuilder.build());

    LanceScanBuilder filteredSlicedBuilder =
        new LanceScanBuilder(
            fullSchema, slicedOpts, Collections.emptyMap(), null, Collections.emptyMap());
    filteredSlicedBuilder.pruneColumns(readSchema);
    filteredSlicedBuilder.pushPredicates(new Predicate[] {catEqA, idGte25});
    LanceScan filteredSlicedScan = (LanceScan) filteredSlicedBuilder.build();
    assertEquals(5, filteredSlicedScan.planInputPartitions().length);
    assertEquals(expectedFiltered, readIdsFromScan(filteredSlicedScan));
  }

  @Test
  void testByteBasedProjectionAwareSplitSizing(@TempDir Path tempDir) throws Exception {
    String uri = tempDir.resolve("byte_adaptive_splits.lance").toString();
    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("cat", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)));

    try (BufferAllocator alloc = new RootAllocator()) {
      List<FragmentMetadata> f0 = createWideFragment(alloc, uri, arrowSchema, "A", 0L, 300, 4096);
      List<FragmentMetadata> f1 =
          createWideFragment(alloc, uri, arrowSchema, "B", 1000L, 300, 4096);
      Dataset.commit(
              alloc,
              uri,
              new FragmentOperation.Overwrite(Arrays.asList(f0.get(0), f1.get(0)), arrowSchema),
              Optional.empty())
          .close();
    }

    StructType fullSchema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("cat", DataTypes.StringType)
            .add("payload", DataTypes.BinaryType);

    LanceSparkReadOptions byteSplitOpts =
        LanceSparkReadOptions.builder()
            .datasetUri(uri)
            .targetSplitSize(300_000L)
            .openCostInBytes(64_000L)
            .build();

    // 1. Narrow scalar projection (id only): projected read bytes per fragment << 300 KB => 2
    // splits
    StructType narrowSchema = new StructType().add("id", DataTypes.LongType);
    LanceScanBuilder narrowBuilder =
        new LanceScanBuilder(
            fullSchema, byteSplitOpts, Collections.emptyMap(), null, Collections.emptyMap());
    narrowBuilder.pruneColumns(narrowSchema);
    LanceScan narrowScan = (LanceScan) narrowBuilder.build();
    assertEquals(2, narrowScan.planInputPartitions().length);

    // 2. Metadata-only projection (_rowid only): 0 data bytes => 2 splits
    StructType metadataSchema = new StructType().add(LanceConstant.ROW_ID, DataTypes.LongType);
    LanceScanBuilder metaBuilder =
        new LanceScanBuilder(
            fullSchema, byteSplitOpts, Collections.emptyMap(), null, Collections.emptyMap());
    metaBuilder.pruneColumns(metadataSchema);
    LanceScan metaScan = (LanceScan) metaBuilder.build();
    assertEquals(2, metaScan.planInputPartitions().length);

    // 3. Wide binary projection (id + payload, ~1.2 MB per fragment > 300 KB): subdivides fragments
    StructType wideSchema =
        new StructType().add("id", DataTypes.LongType).add("payload", DataTypes.BinaryType);
    LanceScanBuilder wideBuilder =
        new LanceScanBuilder(
            fullSchema, byteSplitOpts, Collections.emptyMap(), null, Collections.emptyMap());
    wideBuilder.pruneColumns(wideSchema);
    LanceScan wideScan = (LanceScan) wideBuilder.build();
    InputPartition[] widePartitions = wideScan.planInputPartitions();
    assertTrue(
        widePartitions.length >= 8,
        "Expected wide binary scan to subdivide 2 x 1.2MB fragments, got " + widePartitions.length);

    List<Long> wideIds = new ArrayList<>();
    for (InputPartition part : widePartitions) {
      try (LanceColumnarPartitionReader reader =
          new LanceColumnarPartitionReader((LanceInputPartition) part)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          for (int i = 0; i < batch.numRows(); i++) {
            wideIds.add(batch.column(0).getLong(i));
            assertEquals(4096, batch.column(1).getBinary(i).length);
          }
          batch.close();
        }
      }
    }
    Collections.sort(wideIds);
    assertEquals(600, wideIds.size());
    assertEquals(0L, wideIds.get(0));
    assertEquals(1299L, wideIds.get(599));
  }

  @Test
  void testComputeEffectiveSplitBytesFormula() {
    long mb128 = 128L * 1024 * 1024;
    long mb4 = 4L * 1024 * 1024;

    // Massive scan (80 GB over 128 cores => 640 MB/core > 128 MB) caps at targetSplitSize (128 MB)
    assertEquals(
        mb128, LanceSplit.computeEffectiveSplitBytes(80L * 1024 * 1024 * 1024, mb128, mb4, 128));

    // Medium pruned scan (640 MB over 64 cores => 10 MB/core) adapts to 10 MB
    assertEquals(
        10L * 1024 * 1024,
        LanceSplit.computeEffectiveSplitBytes(640L * 1024 * 1024, mb128, mb4, 64));

    // Tiny scan (2 MB over 64 cores) floors at openCostInBytes (4 MB)
    assertEquals(mb4, LanceSplit.computeEffectiveSplitBytes(2L * 1024 * 1024, mb128, mb4, 64));

    // Explicit targetSplitSize smaller than openCostInBytes is honored
    assertEquals(
        65_536L, LanceSplit.computeEffectiveSplitBytes(10L * 1024 * 1024, 65_536L, mb4, 8));
  }

  @Test
  void testInvalidSplitOptionsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.builder().datasetUri("/tmp/t.lance").maxRowsPerSplit(0L));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.builder().datasetUri("/tmp/t.lance").maxRowsPerSplit(-5L));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.builder().datasetUri("/tmp/t.lance").targetSplitSize(0L));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.builder().datasetUri("/tmp/t.lance").openCostInBytes(-1L));
  }

  private static List<FragmentMetadata> createWideFragment(
      BufferAllocator allocator,
      String uri,
      Schema schema,
      String category,
      long startId,
      int numRows,
      int payloadBytes) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector catVec = (VarCharVector) root.getVector("cat");
      VarBinaryVector payloadVec = (VarBinaryVector) root.getVector("payload");
      idVec.allocateNew();
      catVec.allocateNew();
      payloadVec.allocateNew();
      byte[] catBytes = category.getBytes(StandardCharsets.UTF_8);
      byte[] payload = new byte[payloadBytes];
      long state = startId * 6364136223846793005L + 1442695040888963407L;
      for (int i = 0; i < numRows; i++) {
        for (int b = 0; b < payloadBytes; b += 8) {
          state = state * 6364136223846793005L + 1442695040888963407L;
          for (int k = 0; k < 8 && (b + k) < payloadBytes; k++) {
            payload[b + k] = (byte) (state >>> (k * 8));
          }
        }
        idVec.setSafe(i, startId + i);
        catVec.setSafe(i, catBytes);
        payloadVec.setSafe(i, payload);
      }
      root.setRowCount(numRows);
      return Fragment.create(uri, allocator, root, new WriteParams.Builder().build());
    }
  }
}

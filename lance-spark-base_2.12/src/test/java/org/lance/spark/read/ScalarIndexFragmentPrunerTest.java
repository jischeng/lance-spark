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
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarIndexFragmentPrunerTest {

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
      byte[] payload = new byte[1024];
      Arrays.fill(payload, (byte) 7);
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
            assertEquals(1024, batch.column(1).getBinary(i).length);
          }
          batch.close();
        }
      }
    }
    Collections.sort(ids);
    return ids;
  }

  @Test
  void testBitmapPruningWithPartiallyIndexedFragments(@TempDir Path tempDir) throws Exception {
    String uri = tempDir.resolve("scalar_pruning.lance").toString();
    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("cat", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)));

    Predicate eq202306 =
        new Predicate(
            "=",
            new Expression[] {
              FieldReference.column("cat"),
              new LiteralValue<>(UTF8String.fromString("202306"), DataTypes.StringType)
            });
    Predicate eqMissing =
        new Predicate(
            "=",
            new Expression[] {
              FieldReference.column("cat"),
              new LiteralValue<>(UTF8String.fromString("199901"), DataTypes.StringType)
            });
    Predicate isNotNull =
        new Predicate("IS_NOT_NULL", new Expression[] {FieldReference.column("cat")});
    Predicate idGte10 =
        new Predicate(
            ">=",
            new Expression[] {
              FieldReference.column("id"), new LiteralValue<>(10L, DataTypes.LongType)
            });

    try (BufferAllocator alloc = new RootAllocator()) {
      List<FragmentMetadata> f0 = createFragment(alloc, uri, arrowSchema, "202306", 0L, 300);
      List<FragmentMetadata> f1 = createFragment(alloc, uri, arrowSchema, "202401", 1000L, 300);
      List<FragmentMetadata> initial = Arrays.asList(f0.get(0), f1.get(0));
      try (Dataset ds =
          Dataset.commit(
              alloc,
              uri,
              new FragmentOperation.Overwrite(initial, arrowSchema),
              Optional.empty())) {
        ds.createIndex(
            Collections.singletonList("cat"),
            IndexType.BITMAP,
            Optional.of("cat_idx"),
            IndexParams.builder()
                .setScalarIndexParams(ScalarIndexParams.create("bitmap", "{}"))
                .build(),
            true);
      }
      // Append unindexed fragment 2 (contains 202306 rows as well) and delete a few rows
      List<FragmentMetadata> f2 = createFragment(alloc, uri, arrowSchema, "202306", 2000L, 250);
      try (Dataset ds =
          Dataset.commit(alloc, uri, new FragmentOperation.Append(f2), Optional.of(2L))) {
        ds.delete("id IN (5, 150, 2010, 2200)");
      }

      LanceSparkReadOptions readOptions = LanceSparkReadOptions.builder().datasetUri(uri).build();
      try (Dataset ds = Dataset.open(uri, alloc)) {
        LanceSplit.ScanPlanResult plan = LanceSplit.planScan(ds, readOptions);
        Map<Integer, Long> rowCounts = plan.getFragmentRowCounts();
        assertEquals(3, rowCounts.size());

        Optional<ScalarIndexFragmentPruner.PruneResult> pruned =
            ScalarIndexFragmentPruner.pruneFragments(
                ds, new Predicate[] {isNotNull, eq202306}, rowCounts);
        assertTrue(pruned.isPresent());
        // Fragment 0 matches 202306, Fragment 1 (202401) is pruned, Fragment 2 is unindexed so
        // retained
        assertEquals(new HashSet<>(Arrays.asList(0, 2)), pruned.get().getSurvivingFragments());

        // When no covered fragment matches, only unindexed Fragment 2 survives
        Optional<ScalarIndexFragmentPruner.PruneResult> prunedMissing =
            ScalarIndexFragmentPruner.pruneFragments(ds, new Predicate[] {eqMissing}, rowCounts);
        assertTrue(prunedMissing.isPresent());
        assertEquals(Collections.singleton(2), prunedMissing.get().getSurvivingFragments());
      }
    }

    StructType fullSchema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("cat", DataTypes.StringType)
            .add("payload", DataTypes.BinaryType);
    StructType prunedReadSchema =
        new StructType().add("id", DataTypes.LongType).add("payload", DataTypes.BinaryType);

    // 1. Scan with use_scalar_index=true: prunes fragment 1 on driver -> 2 partitions
    LanceSparkReadOptions indexEnabledOpts =
        LanceSparkReadOptions.builder().datasetUri(uri).useScalarIndex(true).build();
    LanceScanBuilder enabledBuilder =
        new LanceScanBuilder(
            fullSchema, indexEnabledOpts, Collections.emptyMap(), null, Collections.emptyMap());
    enabledBuilder.pruneColumns(prunedReadSchema);
    enabledBuilder.pushPredicates(new Predicate[] {isNotNull, eq202306, idGte10});
    LanceScan enabledScan = (LanceScan) enabledBuilder.build();
    assertEquals(2, enabledScan.planInputPartitions().length);
    List<Long> prunedIds = readIdsFromScan(enabledScan);

    // 2. Scan with use_scalar_index=false: skips scalar index pruning -> all 3 partitions planned
    LanceSparkReadOptions indexDisabledOpts =
        LanceSparkReadOptions.builder().datasetUri(uri).useScalarIndex(false).build();
    LanceScanBuilder disabledBuilder =
        new LanceScanBuilder(
            fullSchema, indexDisabledOpts, Collections.emptyMap(), null, Collections.emptyMap());
    disabledBuilder.pruneColumns(prunedReadSchema);
    disabledBuilder.pushPredicates(new Predicate[] {isNotNull, eq202306, idGte10});
    LanceScan disabledScan = (LanceScan) disabledBuilder.build();
    assertEquals(3, disabledScan.planInputPartitions().length);
    List<Long> unprunedIds = readIdsFromScan(disabledScan);

    // 300 rows in f0 minus id 0..9 (10 rows) minus id=150 (1 deleted) = 289 rows
    // + 250 rows in f2 minus id=2010,2200 (2 deleted) = 248 rows -> total 537 rows
    assertEquals(537, prunedIds.size());
    assertEquals(unprunedIds, prunedIds);
  }
}

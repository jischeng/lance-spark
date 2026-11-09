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
package org.lance.spark.internal;

import org.lance.Dataset;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class RowIdTakeArrowReader extends ArrowReader {
  private final Dataset dataset;
  private final ArrowReader rowIds;
  private final List<String> columns;
  private final List<String> initialColumns;
  private final Map<String, String> activeCoalesce;
  private final int takeBatchSize;
  private final int sliceIndex;
  private final int numSlices;
  private final int stripeSize;
  private final List<Long> pendingIds = new ArrayList<>();
  private int pendingOffset = 0;
  private boolean rowIdsExhausted = false;
  private List<Long> currentBatchIds = Collections.emptyList();
  private int currentBatchOffset = 0;
  private ArrowReader taken;

  RowIdTakeArrowReader(Dataset dataset, ArrowReader rowIds, List<String> columns) {
    this(dataset, rowIds, columns, 0, 0, 1, Collections.emptyMap());
  }

  RowIdTakeArrowReader(
      Dataset dataset,
      ArrowReader rowIds,
      List<String> columns,
      int takeBatchSize,
      int sliceIndex,
      int numSlices,
      Map<String, String> coalesceColumns) {
    super(dataset.allocator());
    this.dataset = dataset;
    this.rowIds = rowIds;
    this.columns = new ArrayList<>(columns);
    this.takeBatchSize = Math.max(0, takeBatchSize);
    this.sliceIndex = Math.max(0, sliceIndex);
    this.numSlices = Math.max(1, numSlices);
    this.stripeSize = this.takeBatchSize > 0 ? Math.max(this.takeBatchSize, 4096) : 4096;

    this.activeCoalesce = resolveActiveCoalesce(this.columns, coalesceColumns);
    this.initialColumns = computeInitialColumns(this.columns, this.activeCoalesce);
  }

  static Map<String, String> resolveActiveCoalesce(
      List<String> columns, Map<String, String> coalesceColumns) {
    Map<String, String> validCoalesce = new LinkedHashMap<>();
    Set<String> columnSet = new HashSet<>(columns);
    if (coalesceColumns != null && !coalesceColumns.isEmpty()) {
      for (Map.Entry<String, String> entry : coalesceColumns.entrySet()) {
        String primary = entry.getKey();
        String fallback = entry.getValue();
        if (primary != null
            && fallback != null
            && !primary.equals(fallback)
            && columnSet.contains(primary)
            && columnSet.contains(fallback)
            && !validCoalesce.containsValue(primary)
            && !validCoalesce.containsKey(fallback)) {
          validCoalesce.put(primary, fallback);
        }
      }
    }
    return Collections.unmodifiableMap(validCoalesce);
  }

  static List<String> computeInitialColumns(
      List<String> columns, Map<String, String> activeCoalesce) {
    Set<String> fallbackSet = new HashSet<>(activeCoalesce.values());
    return columns.stream().filter(c -> !fallbackSet.contains(c)).collect(Collectors.toList());
  }

  static void populateFallbacksFromRowIdVector(
      Dataset dataset,
      VectorSchemaRoot outRoot,
      Map<String, String> activeCoalesce,
      UInt8Vector rowIdVec,
      int rowCount)
      throws IOException {
    List<String> fullNullFallbacks = null;
    for (Map.Entry<String, String> entry : activeCoalesce.entrySet()) {
      String primaryCol = entry.getKey();
      String fallbackCol = entry.getValue();
      FieldVector primaryVec = outRoot.getVector(primaryCol);
      FieldVector fallbackVec = outRoot.getVector(fallbackCol);
      int nullCount = primaryVec.getNullCount();
      if (nullCount == 0) {
        fallbackVec.reset();
        fallbackVec.setValueCount(rowCount);
      } else if (nullCount == rowCount && rowCount > 0) {
        if (fullNullFallbacks == null) {
          fullNullFallbacks = new ArrayList<>(activeCoalesce.size());
        }
        fullNullFallbacks.add(fallbackCol);
      } else {
        List<Long> nullRowIds = new ArrayList<>(nullCount);
        int[] nullIndices = new int[nullCount];
        int k = 0;
        for (int i = 0; i < rowCount; i++) {
          if (primaryVec.isNull(i)) {
            nullRowIds.add(rowIdVec.get(i));
            nullIndices[k++] = i;
          }
        }
        fetchPartialFallback(dataset, fallbackCol, fallbackVec, nullRowIds, nullIndices, rowCount);
      }
    }
    if (fullNullFallbacks != null && !fullNullFallbacks.isEmpty()) {
      List<Long> allRowIds = new ArrayList<>(rowCount);
      for (int i = 0; i < rowCount; i++) {
        allRowIds.add(rowIdVec.get(i));
      }
      fetchFullNullFallbacks(dataset, outRoot, fullNullFallbacks, allRowIds, rowCount);
    }
  }

  private static void fetchFullNullFallbacks(
      Dataset dataset,
      VectorSchemaRoot outRoot,
      List<String> fullNullFallbacks,
      List<Long> rowIds,
      int rowCount)
      throws IOException {
    int offset = 0;
    boolean allocatedForCopy = false;
    try (ArrowReader fbReader = dataset.takeRows(rowIds, fullNullFallbacks)) {
      while (fbReader.loadNextBatch()) {
        VectorSchemaRoot fbRoot = fbReader.getVectorSchemaRoot();
        int batchRows = fbRoot.getRowCount();
        if (offset == 0 && batchRows == rowCount) {
          List<Field> fbFields = new ArrayList<>(fullNullFallbacks.size());
          List<FieldVector> dstVectors = new ArrayList<>(fullNullFallbacks.size());
          for (String fbCol : fullNullFallbacks) {
            FieldVector dstVec = outRoot.getVector(fbCol);
            dstVec.reset();
            fbFields.add(dstVec.getField());
            dstVectors.add(dstVec);
          }
          try (ArrowRecordBatch batch = new VectorUnloader(fbRoot).getRecordBatch()) {
            new VectorLoader(new VectorSchemaRoot(fbFields, dstVectors, 0)).load(batch);
          }
          return;
        }
        if (!allocatedForCopy) {
          for (String fbCol : fullNullFallbacks) {
            FieldVector fbVec = outRoot.getVector(fbCol);
            fbVec.reset();
            fbVec.allocateNew();
          }
          allocatedForCopy = true;
        }
        for (String fbCol : fullNullFallbacks) {
          FieldVector srcVec = fbRoot.getVector(fbCol);
          FieldVector dstVec = outRoot.getVector(fbCol);
          for (int j = 0; j < batchRows; j++) {
            if (!srcVec.isNull(j)) {
              dstVec.copyFromSafe(j, offset + j, srcVec);
            }
          }
        }
        offset += batchRows;
      }
    }
    if (!allocatedForCopy) {
      for (String fbCol : fullNullFallbacks) {
        FieldVector fbVec = outRoot.getVector(fbCol);
        fbVec.reset();
        fbVec.allocateNew();
      }
    }
    for (String fbCol : fullNullFallbacks) {
      outRoot.getVector(fbCol).setValueCount(rowCount);
    }
  }

  private static void fetchPartialFallback(
      Dataset dataset,
      String fallbackCol,
      FieldVector fallbackVec,
      List<Long> nullRowIds,
      int[] nullIndices,
      int rowCount)
      throws IOException {
    fallbackVec.reset();
    fallbackVec.allocateNew();
    int fetchedIdx = 0;
    try (ArrowReader fbReader =
        dataset.takeRows(nullRowIds, Collections.singletonList(fallbackCol))) {
      while (fbReader.loadNextBatch()) {
        FieldVector srcVec = fbReader.getVectorSchemaRoot().getVector(fallbackCol);
        int srcCount = fbReader.getVectorSchemaRoot().getRowCount();
        for (int j = 0; j < srcCount; j++) {
          if (!srcVec.isNull(j)) {
            fallbackVec.copyFromSafe(j, nullIndices[fetchedIdx], srcVec);
          }
          fetchedIdx++;
        }
      }
    }
    fallbackVec.setValueCount(rowCount);
  }

  @Override
  protected Schema readSchema() {
    return new Schema(
        columns.stream()
            .map(name -> dataset.getSchema().findField(name))
            .collect(Collectors.toList()));
  }

  private List<Long> nextBatchIds() throws IOException {
    if (takeBatchSize <= 0) {
      while (rowIds.loadNextBatch()) {
        UInt8Vector vector = (UInt8Vector) rowIds.getVectorSchemaRoot().getVector("_rowid");
        List<Long> ids = new ArrayList<>(vector.getValueCount());
        for (int index = 0; index < vector.getValueCount(); index++) {
          if (vector.isNull(index)) {
            throw new IOException("Lance returned a null row ID");
          }
          long rowId = vector.get(index);
          if (numSlices > 1) {
            long stripeId = (rowId & 0xFFFFFFFFL) / stripeSize;
            if ((int) (stripeId % numSlices) != sliceIndex) {
              continue;
            }
          }
          ids.add(rowId);
        }
        if (!ids.isEmpty()) {
          return ids;
        }
      }
      return Collections.emptyList();
    }

    while (pendingIds.size() - pendingOffset < takeBatchSize && !rowIdsExhausted) {
      if (!rowIds.loadNextBatch()) {
        rowIdsExhausted = true;
        break;
      }
      UInt8Vector vector = (UInt8Vector) rowIds.getVectorSchemaRoot().getVector("_rowid");
      for (int index = 0; index < vector.getValueCount(); index++) {
        if (vector.isNull(index)) {
          throw new IOException("Lance returned a null row ID");
        }
        long rowId = vector.get(index);
        if (numSlices > 1) {
          long stripeId = (rowId & 0xFFFFFFFFL) / stripeSize;
          if ((int) (stripeId % numSlices) != sliceIndex) {
            continue;
          }
        }
        pendingIds.add(rowId);
      }
    }

    int available = pendingIds.size() - pendingOffset;
    if (available <= 0) {
      return Collections.emptyList();
    }
    int end = pendingOffset + Math.min(takeBatchSize, available);
    List<Long> batchIds = new ArrayList<>(pendingIds.subList(pendingOffset, end));
    pendingOffset = end;
    if (pendingOffset == pendingIds.size()) {
      pendingIds.clear();
      pendingOffset = 0;
    } else if (pendingOffset >= 16384) {
      pendingIds.subList(0, pendingOffset).clear();
      pendingOffset = 0;
    }
    return batchIds;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    while (true) {
      if (taken != null && taken.loadNextBatch()) {
        if (activeCoalesce.isEmpty()) {
          try (ArrowRecordBatch batch =
              new VectorUnloader(taken.getVectorSchemaRoot()).getRecordBatch()) {
            new VectorLoader(getVectorSchemaRoot()).load(batch);
          }
          return true;
        }
        VectorSchemaRoot takenRoot = taken.getVectorSchemaRoot();
        int rowCount = takenRoot.getRowCount();
        VectorSchemaRoot outRoot = getVectorSchemaRoot();
        List<Field> initFields = new ArrayList<>(initialColumns.size());
        List<FieldVector> initVectors = new ArrayList<>(initialColumns.size());
        for (String col : initialColumns) {
          FieldVector v = outRoot.getVector(col);
          initFields.add(v.getField());
          initVectors.add(v);
        }
        try (ArrowRecordBatch batch = new VectorUnloader(takenRoot).getRecordBatch()) {
          new VectorLoader(new VectorSchemaRoot(initFields, initVectors, 0)).load(batch);
        }
        for (Map.Entry<String, String> entry : activeCoalesce.entrySet()) {
          String primaryCol = entry.getKey();
          String fallbackCol = entry.getValue();
          FieldVector primaryVec = outRoot.getVector(primaryCol);
          FieldVector fallbackVec = outRoot.getVector(fallbackCol);
          int nullCount = primaryVec.getNullCount();
          if (nullCount == 0) {
            fallbackVec.reset();
            fallbackVec.setValueCount(rowCount);
          } else {
            List<Long> nullRowIds = new ArrayList<>(nullCount);
            int[] nullIndices = new int[nullCount];
            int k = 0;
            for (int i = 0; i < rowCount; i++) {
              if (primaryVec.isNull(i)) {
                nullRowIds.add(currentBatchIds.get(currentBatchOffset + i));
                nullIndices[k++] = i;
              }
            }
            fallbackVec.reset();
            fallbackVec.allocateNew();
            int fetchedIdx = 0;
            try (ArrowReader fbReader =
                dataset.takeRows(nullRowIds, Collections.singletonList(fallbackCol))) {
              while (fbReader.loadNextBatch()) {
                FieldVector srcVec = fbReader.getVectorSchemaRoot().getVector(fallbackCol);
                int srcCount = fbReader.getVectorSchemaRoot().getRowCount();
                for (int j = 0; j < srcCount; j++) {
                  if (!srcVec.isNull(j)) {
                    fallbackVec.copyFromSafe(j, nullIndices[fetchedIdx], srcVec);
                  }
                  fetchedIdx++;
                }
              }
            }
            fallbackVec.setValueCount(rowCount);
          }
        }
        currentBatchOffset += rowCount;
        outRoot.setRowCount(rowCount);
        return true;
      }
      if (taken != null) {
        taken.close();
        taken = null;
      }
      List<Long> ids = nextBatchIds();
      if (ids.isEmpty()) {
        return false;
      }
      currentBatchIds = ids;
      currentBatchOffset = 0;
      taken = dataset.takeRows(ids, initialColumns);
    }
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      if (taken != null) {
        taken.close();
      }
    } finally {
      rowIds.close();
    }
  }
}

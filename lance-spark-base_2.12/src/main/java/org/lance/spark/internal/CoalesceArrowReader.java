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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Streaming ArrowReader that reads initial (non-fallback) columns directly from a fragment scan and
 * populates COALESCE fallback columns on demand only when the corresponding primary column has
 * NULLs.
 */
final class CoalesceArrowReader extends ArrowReader {
  private final Dataset dataset;
  private final ArrowReader delegate;
  private final List<String> columns;
  private final List<String> initialColumns;
  private final Map<String, String> activeCoalesce;

  CoalesceArrowReader(
      Dataset dataset,
      ArrowReader delegate,
      List<String> columns,
      Map<String, String> coalesceColumns) {
    super(dataset.allocator());
    this.dataset = dataset;
    this.delegate = delegate;
    this.columns = new ArrayList<>(columns);
    this.activeCoalesce = RowIdTakeArrowReader.resolveActiveCoalesce(this.columns, coalesceColumns);
    this.initialColumns =
        RowIdTakeArrowReader.computeInitialColumns(this.columns, this.activeCoalesce);
  }

  @Override
  protected Schema readSchema() {
    return new Schema(
        columns.stream()
            .map(name -> dataset.getSchema().findField(name))
            .collect(Collectors.toList()));
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    if (!delegate.loadNextBatch()) {
      return false;
    }
    VectorSchemaRoot srcRoot = delegate.getVectorSchemaRoot();
    int rowCount = srcRoot.getRowCount();
    VectorSchemaRoot outRoot = getVectorSchemaRoot();

    List<Field> initFields = new ArrayList<>(initialColumns.size());
    List<FieldVector> srcVectors = new ArrayList<>(initialColumns.size());
    List<FieldVector> dstVectors = new ArrayList<>(initialColumns.size());
    for (String col : initialColumns) {
      FieldVector srcVec = srcRoot.getVector(col);
      FieldVector dstVec = outRoot.getVector(col);
      initFields.add(dstVec.getField());
      srcVectors.add(srcVec);
      dstVectors.add(dstVec);
    }
    try (ArrowRecordBatch batch =
        new VectorUnloader(new VectorSchemaRoot(initFields, srcVectors, rowCount))
            .getRecordBatch()) {
      new VectorLoader(new VectorSchemaRoot(initFields, dstVectors, 0)).load(batch);
    }

    UInt8Vector rowIdVec = (UInt8Vector) srcRoot.getVector("_rowid");
    RowIdTakeArrowReader.populateFallbacksFromRowIdVector(
        dataset, outRoot, activeCoalesce, rowIdVec, rowCount);
    outRoot.setRowCount(rowCount);
    return true;
  }

  @Override
  public long bytesRead() {
    return delegate.bytesRead();
  }

  @Override
  protected void closeReadSource() throws IOException {
    delegate.close();
  }
}

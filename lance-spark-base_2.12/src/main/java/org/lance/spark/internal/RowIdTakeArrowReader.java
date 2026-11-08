package org.lance.spark.internal;

import org.lance.Dataset;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class RowIdTakeArrowReader extends ArrowReader {
  private final Dataset dataset;
  private final ArrowReader rowIds;
  private final List<String> columns;
  private ArrowReader taken;

  RowIdTakeArrowReader(Dataset dataset, ArrowReader rowIds, List<String> columns) {
    super(dataset.allocator());
    this.dataset = dataset;
    this.rowIds = rowIds;
    this.columns = new ArrayList<>(columns);
  }

  @Override
  protected Schema readSchema() {
    return new Schema(columns.stream().map(name -> dataset.getSchema().findField(name))
        .collect(Collectors.toList()));
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    while (true) {
      if (taken != null && taken.loadNextBatch()) {
        try (ArrowRecordBatch batch =
            new VectorUnloader(taken.getVectorSchemaRoot()).getRecordBatch()) {
          new VectorLoader(getVectorSchemaRoot()).load(batch);
        }
        return true;
      }
      if (taken != null) {
        taken.close();
        taken = null;
      }
      if (!rowIds.loadNextBatch()) {
        return false;
      }
      UInt8Vector vector = (UInt8Vector) rowIds.getVectorSchemaRoot().getVector("_rowid");
      List<Long> ids = new ArrayList<>(vector.getValueCount());
      for (int index = 0; index < vector.getValueCount(); index++) {
        if (vector.isNull(index)) {
          throw new IOException("Lance returned a null row ID");
        }
        ids.add(vector.get(index));
      }
      if (!ids.isEmpty()) {
        taken = dataset.takeRows(ids, columns);
      }
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

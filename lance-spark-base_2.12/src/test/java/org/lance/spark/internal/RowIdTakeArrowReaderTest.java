package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Utils;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RowIdTakeArrowReaderTest {
  @ParameterizedTest
  @ValueSource(strings = {"b > 0", "b < -100", "b IS NOT NULL"})
  void matchesDirectScanWithBoundedBatches(String filter) throws Exception {
    List<String> columns = Arrays.asList("c", "b");
    try (Dataset dataset = Utils.openDatasetBuilder(TestUtils.TestTable1Config.readOptions).build();
        LanceScanner direct = dataset.newScan(new ScanOptions.Builder().columns(columns)
            .filter(filter).batchSize(1).build());
        ArrowReader expected = direct.scanBatches();
        LanceScanner keys = dataset.newScan(new ScanOptions.Builder()
            .columns(Collections.emptyList()).filter(filter).withRowId(true).batchSize(1).build());
        ArrowReader actual = new RowIdTakeArrowReader(dataset, keys.scanBatches(), columns)) {
      assertEquals(rows(expected), rows(actual));
    }
  }

  private List<List<Object>> rows(ArrowReader reader) throws Exception {
    List<List<Object>> result = new ArrayList<>();
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    while (reader.loadNextBatch()) {
      for (int index = 0; index < root.getRowCount(); index++) {
        List<Object> row = new ArrayList<>();
        for (int column = 0; column < root.getFieldVectors().size(); column++) {
          row.add(root.getVector(column).getObject(index));
        }
        result.add(row);
      }
    }
    return result;
  }
}

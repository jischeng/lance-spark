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
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RowIdTakeArrowReaderTest {
  @ParameterizedTest
  @ValueSource(strings = {"b > 0", "b < -100", "b IS NOT NULL"})
  void matchesDirectScanWithBoundedBatches(String filter) throws Exception {
    List<String> columns = Arrays.asList("c", "b");
    try (Dataset dataset =
            Utils.openDatasetBuilder(TestUtils.TestTable1Config.readOptions).build();
        LanceScanner direct =
            dataset.newScan(
                new ScanOptions.Builder().columns(columns).filter(filter).batchSize(1).build());
        ArrowReader expected = direct.scanBatches();
        LanceScanner keys =
            dataset.newScan(
                new ScanOptions.Builder()
                    .columns(Collections.emptyList())
                    .filter(filter)
                    .withRowId(true)
                    .batchSize(1)
                    .build());
        ArrowReader actual = new RowIdTakeArrowReader(dataset, keys.scanBatches(), columns)) {
      assertEquals(rows(expected), rows(actual));
    }
  }

  @Test
  void coalesceColumnsSkipsFallbackWhenPrimaryNonNull() throws Exception {
    List<String> columns = Arrays.asList("c", "b");
    try (Dataset dataset =
            Utils.openDatasetBuilder(TestUtils.TestTable1Config.readOptions).build();
        LanceScanner direct =
            dataset.newScan(new ScanOptions.Builder().columns(columns).batchSize(2).build());
        ArrowReader expected = direct.scanBatches();
        LanceScanner keys =
            dataset.newScan(
                new ScanOptions.Builder()
                    .columns(Collections.emptyList())
                    .withRowId(true)
                    .batchSize(16)
                    .build());
        ArrowReader actual =
            new RowIdTakeArrowReader(
                dataset,
                keys.scanBatches(),
                columns,
                2,
                0,
                1,
                Collections.singletonMap("c", "b"))) {
      List<List<Object>> expRows = rows(expected);
      List<List<Object>> actRows = rows(actual);
      assertEquals(expRows.size(), actRows.size());
      for (int i = 0; i < expRows.size(); i++) {
        assertEquals(expRows.get(i).get(0), actRows.get(i).get(0));
        if (expRows.get(i).get(0) != null) {
          assertNull(actRows.get(i).get(1));
        } else {
          assertEquals(expRows.get(i).get(1), actRows.get(i).get(1));
        }
      }
    }
  }

  @Test
  void slicesPartitionRowsDisjointly() throws Exception {
    List<String> columns = Arrays.asList("c", "b");
    try (Dataset dataset =
            Utils.openDatasetBuilder(TestUtils.TestTable1Config.readOptions).build();
        LanceScanner direct =
            dataset.newScan(new ScanOptions.Builder().columns(columns).batchSize(10).build());
        ArrowReader expected = direct.scanBatches()) {
      List<List<Object>> allSliceRows = new ArrayList<>();
      for (int s = 0; s < 2; s++) {
        try (LanceScanner keys =
                dataset.newScan(
                    new ScanOptions.Builder()
                        .columns(Collections.emptyList())
                        .withRowId(true)
                        .batchSize(16)
                        .build());
            ArrowReader sliceReader =
                new RowIdTakeArrowReader(
                    dataset, keys.scanBatches(), columns, 2, s, 2, Collections.emptyMap())) {
          allSliceRows.addAll(rows(sliceReader));
        }
      }
      assertEquals(rows(expected), allSliceRows);
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

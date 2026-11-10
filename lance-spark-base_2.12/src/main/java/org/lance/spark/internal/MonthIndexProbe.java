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
import org.lance.Fragment;
import org.lance.file.LanceFileReader;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class MonthIndexProbe {
  private MonthIndexProbe() {}

  public static String discover(String path, String requestedVersion) throws Exception {
    if (!path.startsWith("/")) {
      throw new IllegalArgumentException("month probe requires a managed local Lance path");
    }
    long version = Long.parseLong(requestedVersion);
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri(path).ref(LanceRef.ofMain(version)).build();
    try (Dataset dataset = Utils.openDatasetBuilder(options).build()) {
      if (dataset.version() != version) {
        throw new IllegalStateException("Lance snapshot version mismatch");
      }
      int monthField =
          dataset.getLanceSchema().fields().stream()
              .filter(field -> field.getName().equals("month"))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("month column missing"))
              .getId();
      Set<String> months = new TreeSet<>();
      Set<Integer> covered = new HashSet<>();
      try {
        for (Index index : dataset.getIndexes()) {
          if (index.indexType() != IndexType.BITMAP
              || !index.fields().equals(Collections.singletonList(monthField))) {
            continue;
          }
          if (!index.fragments().isPresent()) {
            throw new IllegalStateException("index fragment coverage unavailable");
          }
          String indexPath = path + "/_indices/" + index.uuid() + "/bitmap_page_lookup.lance";
          try (LanceFileReader file = LanceFileReader.open(indexPath, dataset.allocator());
              ArrowReader reader =
                  file.readAll(Collections.singletonList("keys"), Collections.emptyList(), 1024)) {
            readMonths(reader, "keys", months);
          }
          covered.addAll(index.fragments().get());
        }
      } catch (Exception error) {
        months.clear();
        covered.clear();
      }
      List<Fragment> fragments = dataset.getFragments();
      for (Fragment fragment : fragments) {
        if (covered.contains(fragment.getId())) {
          continue;
        }
        try (LanceScanner scanner =
                fragment.newScan(
                    new ScanOptions.Builder()
                        .columns(Collections.singletonList("month"))
                        .batchSize(65536)
                        .build());
            ArrowReader reader = scanner.scanBatches()) {
          readMonths(reader, "month", months);
        }
      }
      if (months.size() > 1200) {
        throw new IllegalArgumentException("too many distinct months");
      }
      for (String month : months) {
        if (!month.matches("[0-9]{4}(0[1-9]|1[0-2])")) {
          throw new IllegalArgumentException("invalid month value");
        }
      }
      return "{\"version\":"
          + version
          + ",\"index_coverage_ids\":"
          + covered.size()
          + ",\"months\":["
          + String.join(
              ",", months.stream().map(month -> "\"" + month + "\"").collect(Collectors.toList()))
          + "]}";
    }
  }

  private static void readMonths(ArrowReader reader, String column, Set<String> months)
      throws Exception {
    long previous = -1;
    while (reader.loadNextBatch()) {
      VarCharVector vector = (VarCharVector) reader.getVectorSchemaRoot().getVector(column);
      for (int index = 0; index < vector.getValueCount(); index++) {
        if (vector.isNull(index)) {
          continue;
        }
        int offset = vector.getOffsetBuffer().getInt((long) index * 4);
        int end = vector.getOffsetBuffer().getInt((long) (index + 1) * 4);
        if (end - offset != 6) {
          throw new IllegalArgumentException("invalid month length");
        }
        long encoded =
            Integer.toUnsignedLong(vector.getDataBuffer().getInt(offset))
                | ((long) Short.toUnsignedInt(vector.getDataBuffer().getShort(offset + 4)) << 32);
        if (encoded != previous) {
          months.add(vector.getObject(index).toString());
          previous = encoded;
          if (months.size() > 1200) {
            throw new IllegalArgumentException("too many distinct months");
          }
        }
      }
    }
  }
}

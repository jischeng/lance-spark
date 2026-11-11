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
import org.lance.fragment.DataFile;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import com.google.common.base.Preconditions;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283750L;

  private final List<Integer> fragments;
  private final Long startOffset;
  private final Long rowCount;
  private final Long estimatedRows;

  public LanceSplit(List<Integer> fragments) {
    this(fragments, null, null, null);
  }

  public LanceSplit(int fragmentId, long startOffset, long rowCount) {
    this(Collections.singletonList(fragmentId), startOffset, rowCount, rowCount);
  }

  public LanceSplit(int fragmentId, long startOffset, long rowCount, long estimatedRows) {
    this(Collections.singletonList(fragmentId), startOffset, rowCount, estimatedRows);
  }

  private LanceSplit(List<Integer> fragments, Long startOffset, Long rowCount, Long estimatedRows) {
    Preconditions.checkArgument(fragments != null, "fragments must not be null");
    if (startOffset != null || rowCount != null) {
      Preconditions.checkArgument(
          startOffset != null && rowCount != null,
          "startOffset and rowCount must both be specified or both be null");
      Preconditions.checkArgument(
          fragments.size() == 1, "Row-range splits must reference a single fragment");
      Preconditions.checkArgument(startOffset >= 0, "startOffset must be non-negative");
      Preconditions.checkArgument(rowCount > 0, "rowCount must be positive");
    }
    this.fragments = fragments;
    this.startOffset = startOffset;
    this.rowCount = rowCount;
    this.estimatedRows = estimatedRows;
  }

  public List<Integer> getFragments() {
    return fragments;
  }

  public boolean hasRowRange() {
    return startOffset != null && rowCount != null;
  }

  public Long getStartOffset() {
    return startOffset;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public Long getEstimatedRows() {
    return estimatedRows;
  }

  public long effectiveRowCount(Map<Integer, Long> fragmentRowCounts) {
    if (estimatedRows != null) {
      return estimatedRows;
    }
    if (hasRowRange()) {
      return rowCount;
    }
    if (fragmentRowCounts == null || fragmentRowCounts.isEmpty()) {
      return 0L;
    }
    long total = 0L;
    for (Integer fragmentId : fragments) {
      Long count = fragmentRowCounts.get(fragmentId);
      if (count != null && count > 0) {
        total += count;
      }
    }
    return total;
  }

  /**
   * Computes the adaptive target split byte budget following Spark's {@code
   * FilePartition.maxSplitBytes} formula: {@code min(targetSplitSize, max(openCostInBytes,
   * totalProjectedBytes / minPartitionNum))}.
   */
  public static long computeEffectiveSplitBytes(
      long totalProjectedBytes, long targetSplitSize, long openCostInBytes, int minPartitionNum) {
    if (targetSplitSize <= 0L) {
      return 0L;
    }
    int parallelism = Math.max(1, minPartitionNum);
    long nonNegativeTotal = Math.max(0L, totalProjectedBytes);
    long nonNegativeOpenCost = Math.max(0L, openCostInBytes);
    long bytesPerCore = nonNegativeTotal / parallelism;
    return Math.min(targetSplitSize, Math.max(nonNegativeOpenCost, bytesPerCore));
  }

  /**
   * Estimates projected on-disk bytes read per fragment by inspecting each fragment's {@link
   * DataFile} field lists and recorded byte sizes against the columns accessed by projection and
   * pushed filters. Untouched column files contribute zero bytes; shared multi-column files are
   * weighted by Arrow field byte width; fragments without recorded {@link
   * DataFile#getFileSizeBytes()} fall back proportionally to {@code fallbackTotalProjectedBytes}.
   */
  public static Map<Integer, Long> estimateFragmentProjectedBytes(
      LanceSchema lanceSchema,
      StructType projectedSchema,
      Predicate[] pushedPredicates,
      Map<Integer, List<DataFile>> fragmentDataFiles,
      Map<Integer, Long> fragmentRowCounts,
      long fallbackTotalProjectedBytes) {
    if (fragmentRowCounts == null || fragmentRowCounts.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<String> accessedTopColumns = collectAccessedTopColumns(projectedSchema, pushedPredicates);
    if (accessedTopColumns.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<Integer, Long> fieldByteWeights = new HashMap<>();
    Set<Integer> requiredFieldIds = new HashSet<>();
    long touchedTopLevelRowWidth = 0L;
    if (lanceSchema != null && lanceSchema.fields() != null) {
      for (LanceField topField : lanceSchema.fields()) {
        long w = indexFieldWeights(topField, fieldByteWeights);
        if (accessedTopColumns.contains(topField.getName().toLowerCase(Locale.ROOT))) {
          collectFieldIds(topField, requiredFieldIds);
          touchedTopLevelRowWidth += w;
        }
      }
    }

    long totalLogicalRows = 0L;
    for (Long rows : fragmentRowCounts.values()) {
      if (rows != null && rows > 0L) {
        totalLogicalRows += rows;
      }
    }

    Map<Integer, Long> result = new HashMap<>(fragmentRowCounts.size());
    for (Map.Entry<Integer, Long> entry : fragmentRowCounts.entrySet()) {
      int fragmentId = entry.getKey();
      long logicalRows = entry.getValue() != null ? Math.max(0L, entry.getValue()) : 0L;
      if (logicalRows <= 0L) {
        result.put(fragmentId, 0L);
        continue;
      }

      List<DataFile> files = fragmentDataFiles != null ? fragmentDataFiles.get(fragmentId) : null;
      Long fileBasedBytes =
          requiredFieldIds.isEmpty()
              ? null
              : estimateSingleFragmentFromDataFiles(files, requiredFieldIds, fieldByteWeights);
      if (fileBasedBytes != null) {
        result.put(fragmentId, fileBasedBytes);
      } else if (fallbackTotalProjectedBytes > 0L && totalLogicalRows > 0L) {
        long proportional =
            Math.max(
                1L,
                Math.round((double) fallbackTotalProjectedBytes * logicalRows / totalLogicalRows));
        result.put(fragmentId, proportional);
      } else if (touchedTopLevelRowWidth > 0L) {
        result.put(fragmentId, logicalRows * touchedTopLevelRowWidth);
      } else {
        result.put(fragmentId, 0L);
      }
    }
    return result;
  }

  private static Long estimateSingleFragmentFromDataFiles(
      List<DataFile> files, Set<Integer> requiredFieldIds, Map<Integer, Long> fieldByteWeights) {
    if (files == null || files.isEmpty()) {
      return null;
    }
    long sumBytes = 0L;
    for (DataFile dataFile : files) {
      int[] fields = dataFile.getFields();
      if (fields == null || fields.length == 0) {
        continue;
      }
      long totalWeight = 0L;
      long touchedWeight = 0L;
      for (int fieldId : fields) {
        if (fieldId < 0) {
          continue;
        }
        long weight = fieldByteWeights.getOrDefault(fieldId, 8L);
        totalWeight += weight;
        if (requiredFieldIds.contains(fieldId)) {
          touchedWeight += weight;
        }
      }
      if (touchedWeight <= 0L) {
        continue;
      }
      Long fileSize = dataFile.getFileSizeBytes();
      if (fileSize == null || fileSize <= 0L) {
        return null;
      }
      if (touchedWeight >= totalWeight) {
        sumBytes += fileSize;
      } else {
        sumBytes += Math.max(1L, Math.round((double) fileSize * touchedWeight / totalWeight));
      }
    }
    return sumBytes;
  }

  private static Set<String> collectAccessedTopColumns(
      StructType projectedSchema, Predicate[] pushedPredicates) {
    Set<String> cols = new HashSet<>();
    if (projectedSchema != null) {
      for (StructField field : projectedSchema.fields()) {
        String name = field.name();
        if (!isVirtualMetadataColumn(name)) {
          cols.add(stripVirtualBlobSuffix(name).toLowerCase(Locale.ROOT));
        }
      }
    }
    if (pushedPredicates != null) {
      for (Predicate predicate : pushedPredicates) {
        if (predicate == null) {
          continue;
        }
        for (NamedReference ref : predicate.references()) {
          String[] names = ref.fieldNames();
          if (names != null && names.length > 0 && !isVirtualMetadataColumn(names[0])) {
            cols.add(stripVirtualBlobSuffix(names[0]).toLowerCase(Locale.ROOT));
          }
        }
      }
    }
    return cols;
  }

  private static boolean isVirtualMetadataColumn(String name) {
    return LanceConstant.ROW_ID.equalsIgnoreCase(name)
        || LanceConstant.ROW_ADDRESS.equalsIgnoreCase(name)
        || LanceConstant.SCORE.equalsIgnoreCase(name);
  }

  private static String stripVirtualBlobSuffix(String name) {
    if (name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)) {
      return name.substring(0, name.length() - LanceConstant.BLOB_POSITION_SUFFIX.length());
    }
    if (name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)) {
      return name.substring(0, name.length() - LanceConstant.BLOB_SIZE_SUFFIX.length());
    }
    return name;
  }

  private static long indexFieldWeights(LanceField field, Map<Integer, Long> fieldByteWeights) {
    List<LanceField> children = field.getChildren();
    long childSum = 0L;
    if (children != null && !children.isEmpty()) {
      for (LanceField child : children) {
        childSum += indexFieldWeights(child, fieldByteWeights);
      }
    }
    long weight = estimateArrowFieldWeight(field.getType(), children, childSum);
    if (field.getId() >= 0) {
      fieldByteWeights.put(field.getId(), weight);
    }
    return weight;
  }

  private static long estimateArrowFieldWeight(
      ArrowType type, List<LanceField> children, long childSum) {
    if (type == null) {
      return Math.max(8L, childSum);
    }
    if (type instanceof ArrowType.Int) {
      return Math.max(1L, ((ArrowType.Int) type).getBitWidth() / 8L);
    }
    if (type instanceof ArrowType.FloatingPoint) {
      FloatingPointPrecision p = ((ArrowType.FloatingPoint) type).getPrecision();
      if (p == FloatingPointPrecision.HALF) {
        return 2L;
      }
      return p == FloatingPointPrecision.SINGLE ? 4L : 8L;
    }
    if (type instanceof ArrowType.Bool) {
      return 1L;
    }
    if (type instanceof ArrowType.Decimal) {
      return Math.max(8L, ((ArrowType.Decimal) type).getBitWidth() / 8L);
    }
    if (type instanceof ArrowType.FixedSizeBinary) {
      return Math.max(1L, ((ArrowType.FixedSizeBinary) type).getByteWidth());
    }
    if (type instanceof ArrowType.FixedSizeList) {
      int listSize = Math.max(1, ((ArrowType.FixedSizeList) type).getListSize());
      long elemWeight = childSum > 0L ? childSum : 4L;
      return elemWeight * listSize;
    }
    if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
      return 4096L;
    }
    if (type instanceof ArrowType.LargeUtf8) {
      return 64L;
    }
    if (type instanceof ArrowType.Utf8) {
      return 20L;
    }
    if (type instanceof ArrowType.List
        || type instanceof ArrowType.LargeList
        || type instanceof ArrowType.Map) {
      return 16L * Math.max(4L, childSum);
    }
    if (type instanceof ArrowType.Struct) {
      return Math.max(8L, childSum);
    }
    if (type instanceof ArrowType.Date || type instanceof ArrowType.Time) {
      return 4L;
    }
    return 8L;
  }

  private static void collectFieldIds(LanceField field, Set<Integer> out) {
    if (field.getId() >= 0) {
      out.add(field.getId());
    }
    List<LanceField> children = field.getChildren();
    if (children != null) {
      for (LanceField child : children) {
        collectFieldIds(child, out);
      }
    }
  }

  /**
   * Expands single-fragment splits into contiguous physical row-range splits covering {@code [0,
   * physicalRows)} based on projected byte size ({@code effectiveSplitBytes}) and/or an optional
   * explicit {@code maxRowsPerSplit} bound.
   */
  public static List<LanceSplit> expandSplits(
      List<LanceSplit> splits,
      Map<Integer, Long> logicalRowCounts,
      Map<Integer, Long> physicalRowCounts,
      Map<Integer, Long> fragmentProjectedBytes,
      Long effectiveSplitBytes,
      Long maxRowsPerSplit) {
    if (splits.isEmpty() || logicalRowCounts == null || logicalRowCounts.isEmpty()) {
      return splits;
    }
    boolean byteEnabled =
        effectiveSplitBytes != null
            && effectiveSplitBytes > 0L
            && fragmentProjectedBytes != null
            && !fragmentProjectedBytes.isEmpty();
    boolean rowEnabled = maxRowsPerSplit != null && maxRowsPerSplit > 0L;
    if (!byteEnabled && !rowEnabled) {
      return splits;
    }

    List<LanceSplit> expanded = new ArrayList<>(splits.size());
    for (LanceSplit split : splits) {
      if (!split.hasRowRange() && split.fragments.size() == 1) {
        int fragmentId = split.fragments.get(0);
        long logicalRows = logicalRowCounts.getOrDefault(fragmentId, 0L);
        long physicalRows =
            Math.max(
                logicalRows,
                physicalRowCounts != null
                    ? physicalRowCounts.getOrDefault(fragmentId, logicalRows)
                    : logicalRows);
        if (logicalRows > 1L && physicalRows > 1L) {
          long byteSlices = 1L;
          if (byteEnabled) {
            long projBytes = fragmentProjectedBytes.getOrDefault(fragmentId, 0L);
            if (projBytes > effectiveSplitBytes) {
              byteSlices = (projBytes + effectiveSplitBytes - 1L) / effectiveSplitBytes;
            }
          }
          long rowSlices = 1L;
          if (rowEnabled && logicalRows > maxRowsPerSplit) {
            rowSlices = (logicalRows + maxRowsPerSplit - 1L) / maxRowsPerSplit;
          }
          int numSlices =
              (int)
                  Math.min(
                      physicalRows, Math.min(Integer.MAX_VALUE, Math.max(byteSlices, rowSlices)));
          if (numSlices > 1) {
            long estPerSlice = Math.max(1L, (logicalRows + numSlices - 1L) / numSlices);
            for (int i = 0; i < numSlices; i++) {
              long start = (physicalRows * i) / numSlices;
              long end = (physicalRows * (i + 1)) / numSlices;
              if (end > start) {
                expanded.add(new LanceSplit(fragmentId, start, end - start, estPerSlice));
              }
            }
            continue;
          }
        }
      }
      expanded.add(split);
    }
    return expanded;
  }

  /**
   * Expands single-fragment splits whose logical row count exceeds {@code maxRowsPerSplit} into
   * contiguous physical row-range splits covering {@code [0, physicalRows)}.
   */
  public static List<LanceSplit> expandByMaxRowsPerSplit(
      List<LanceSplit> splits,
      Map<Integer, Long> logicalRowCounts,
      Map<Integer, Long> physicalRowCounts,
      Long maxRowsPerSplit) {
    return expandSplits(splits, logicalRowCounts, physicalRowCounts, null, null, maxRowsPerSplit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceSplit that = (LanceSplit) o;
    return Objects.equals(fragments, that.fragments)
        && Objects.equals(startOffset, that.startOffset)
        && Objects.equals(rowCount, that.rowCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fragments, startOffset, rowCount);
  }

  @Override
  public String toString() {
    if (hasRowRange()) {
      return "LanceSplit{fragments="
          + fragments
          + ", startOffset="
          + startOffset
          + ", rowCount="
          + rowCount
          + "}";
    }
    return "LanceSplit{fragments=" + fragments + "}";
  }

  /** Result of scan planning containing splits, resolved version, and per-fragment metadata. */
  public static class ScanPlanResult {
    private final List<LanceSplit> splits;
    private final LanceRef ref;

    /** Per-fragment logical row counts (after deletions). Key is fragment ID. */
    private final Map<Integer, Long> fragmentRowCounts;

    /** Per-fragment physical row counts (before deletions). Key is fragment ID. */
    private final Map<Integer, Long> fragmentPhysicalRows;

    /** Per-fragment data file metadata from the manifest. Key is fragment ID. */
    private final Map<Integer, List<DataFile>> fragmentDataFiles;

    public ScanPlanResult(
        List<LanceSplit> splits, LanceRef ref, Map<Integer, Long> fragmentRowCounts) {
      this(splits, ref, fragmentRowCounts, fragmentRowCounts, Collections.emptyMap());
    }

    public ScanPlanResult(
        List<LanceSplit> splits,
        LanceRef ref,
        Map<Integer, Long> fragmentRowCounts,
        Map<Integer, Long> fragmentPhysicalRows) {
      this(splits, ref, fragmentRowCounts, fragmentPhysicalRows, Collections.emptyMap());
    }

    public ScanPlanResult(
        List<LanceSplit> splits,
        LanceRef ref,
        Map<Integer, Long> fragmentRowCounts,
        Map<Integer, Long> fragmentPhysicalRows,
        Map<Integer, List<DataFile>> fragmentDataFiles) {
      this.splits = splits;
      this.ref = ref;
      this.fragmentRowCounts = fragmentRowCounts;
      this.fragmentPhysicalRows = fragmentPhysicalRows;
      this.fragmentDataFiles =
          fragmentDataFiles != null ? fragmentDataFiles : Collections.emptyMap();
    }

    public List<LanceSplit> getSplits() {
      return splits;
    }

    public LanceRef getRef() {
      return ref;
    }

    public Map<Integer, Long> getFragmentRowCounts() {
      return fragmentRowCounts;
    }

    public Map<Integer, Long> getFragmentPhysicalRows() {
      return fragmentPhysicalRows;
    }

    public Map<Integer, List<DataFile>> getFragmentDataFiles() {
      return fragmentDataFiles;
    }
  }

  /**
   * Generates splits and resolves the dataset version.
   *
   * <p>This method opens the dataset at the specified version (or latest if not specified), gets
   * the fragment IDs and per-fragment row counts, and returns both the splits and the resolved
   * version. The resolved version should be passed to workers to ensure snapshot isolation.
   */
  public static ScanPlanResult planScan(LanceSparkReadOptions readOptions) {
    try (Dataset dataset = Utils.openDatasetBuilder(readOptions).build()) {
      return planScan(dataset, readOptions);
    }
  }

  /**
   * Generates splits and resolves the dataset version using an already-opened dataset.
   *
   * <p>Prefer this overload over {@link #planScan(LanceSparkReadOptions)} when the caller already
   * holds an open {@link Dataset} (e.g. in scan planning), to avoid re-opening the dataset and
   * paying the manifest IO cost twice.
   *
   * <p>The caller retains ownership of the dataset; this method does not close it.
   */
  public static ScanPlanResult planScan(Dataset dataset, LanceSparkReadOptions readOptions) {
    List<Fragment> fragments = dataset.getFragments();
    List<LanceSplit> splits = new ArrayList<>(fragments.size());
    Map<Integer, Long> fragmentRowCounts = new HashMap<>(fragments.size());
    Map<Integer, Long> fragmentPhysicalRows = new HashMap<>(fragments.size());
    Map<Integer, List<DataFile>> fragmentDataFiles = new HashMap<>(fragments.size());
    for (Fragment fragment : fragments) {
      int id = fragment.getId();
      splits.add(new LanceSplit(Collections.singletonList(id)));
      fragmentRowCounts.put(id, fragment.metadata().getNumRows());
      fragmentPhysicalRows.put(id, fragment.metadata().getPhysicalRows());
      List<DataFile> files = fragment.metadata().getFiles();
      if (files != null && !files.isEmpty()) {
        fragmentDataFiles.put(id, new ArrayList<>(files));
      }
    }

    LanceRef ref = Utils.pinOpenedRef(dataset, readOptions.getRef());
    return new ScanPlanResult(
        splits, ref, fragmentRowCounts, fragmentPhysicalRows, fragmentDataFiles);
  }
}

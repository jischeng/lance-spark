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
import org.lance.ManifestSummary;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownV2Filters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private static final Logger LOG = LoggerFactory.getLogger(LanceScanBuilder.class);

  /** Session switch that turns on two-phase month reads for every scan of this query. */
  public static final String CONF_TWO_PHASE_MONTH_READ = "spark.lance.two_phase_month_read";

  /** Session config for maximum rows per LanceSplit when two-phase slicing is enabled. */
  public static final String CONF_MAX_ROWS_PER_SPLIT = "spark.lance.max_rows_per_split";

  /** Session config for primary:fallback column pairs used in COALESCE(primary, fallback). */
  public static final String CONF_COALESCE_COLUMNS = "spark.lance.coalesce_columns";

  private final LanceSparkReadOptions readOptions;

  /**
   * Reads the session-level two-phase month read switch. Catalog-resolved tables ignore the
   * provider OPTIONS map, so a session configuration is the only way to enable it for {@code USING
   * lance OPTIONS (catalog ...)} queries.
   */
  private static boolean twoPhaseMonthReadEnabled() {
    scala.Option<org.apache.spark.sql.SparkSession> session =
        org.apache.spark.sql.SparkSession.getActiveSession();
    if (session.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean(session.get().conf().get(CONF_TWO_PHASE_MONTH_READ, "false"));
  }

  private static int maxRowsPerSplitFromConf() {
    scala.Option<org.apache.spark.sql.SparkSession> session =
        org.apache.spark.sql.SparkSession.getActiveSession();
    if (session.isEmpty()) {
      return 0;
    }
    try {
      return Math.max(0, Integer.parseInt(session.get().conf().get(CONF_MAX_ROWS_PER_SPLIT, "0")));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static Map<String, String> coalesceColumnsFromConf() {
    scala.Option<org.apache.spark.sql.SparkSession> session =
        org.apache.spark.sql.SparkSession.getActiveSession();
    if (session.isEmpty()) {
      return Collections.emptyMap();
    }
    return LanceSparkReadOptions.parseCoalesceColumns(
        session.get().conf().get(CONF_COALESCE_COLUMNS, ""));
  }

  /** Full table schema before column pruning; used to widen nested structs for vectorized reads. */
  private final StructType fullSchema;

  /** Blob v2 column names in the read schema. Filters on these cannot push to Lance. */
  private final Set<String> blobV2Columns;

  private StructType schema;

  private Predicate[] pushedPredicates = new Predicate[0];

  // Set when pushPredicates leaves filters for Spark. pushLimit and friends read this after
  // pushPredicates because Spark pushes filters before those operators.
  private boolean hasResidualPredicates = false;
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private Optional<List<ColumnOrdering>> topNSortOrders = Optional.empty();
  private Optional<Aggregation> pushedAggregation = Optional.empty();
  private LanceLocalScan localScan = null;

  // Lazily opened dataset for reuse during scan building
  private Dataset lazyDataset = null;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  private final ShardingSpec shardingSpec;

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this(schema, readOptions, initialStorageOptions, namespaceImpl, namespaceProperties, null);
  }

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      ShardingSpec shardingSpec) {
    this.fullSchema = BlobUtils.applyBlobV2DescriptorSchema(schema);
    this.blobV2Columns = BlobUtils.blobV2ColumnNames(this.fullSchema);
    this.schema = this.fullSchema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.shardingSpec = shardingSpec;
  }

  /**
   * Gets or opens a dataset for reuse during scan building. The dataset is lazily opened on first
   * access and reused for subsequent calls.
   */
  private Dataset getOrOpenDataset() {
    if (lazyDataset == null) {
      lazyDataset = Utils.openDatasetBuilder(readOptions).build();
    }
    return lazyDataset;
  }

  /** Closes the lazily opened dataset if it was opened. */
  private void closeLazyDataset() {
    if (lazyDataset != null) {
      lazyDataset.close();
      lazyDataset = null;
    }
  }

  @Override
  public Scan build() {
    // Wrap the entire planning body in try/finally to guarantee that the lazily-opened native
    // dataset handle (lazyDataset) is always released, including when intermediate steps such as
    // zonemap loading or LanceSplit.planScan(dataset) throw.
    try {
      // Return LocalScan if we have a metadata-only aggregation result
      if (localScan != null) {
        return localScan;
      }

      // Get statistics from manifest summary before closing dataset
      ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

      // Collect all columns that need zonemap stats: filter columns + sharding columns.
      Set<String> columnsToLoad = extractReferencedColumns(pushedPredicates);
      Dataset dataset = getOrOpenDataset();
      LanceSchema lanceSchema = dataset.getLanceSchema();
      ShardingSpec activeShardingSpec =
          SparkLanceShardingUtils.isEmpty(shardingSpec)
              ? SparkLanceShardingUtils.firstShardingSpec(dataset)
              : shardingSpec;
      for (ShardingField field : SparkLanceShardingUtils.fields(activeShardingSpec)) {
        columnsToLoad.add(SparkLanceShardingUtils.columnName(field, lanceSchema));
      }

      // Load zonemap stats for all requested columns in one pass.
      Map<String, List<ZoneStats>> zonemapStats =
          loadZonemapStats(getOrOpenDataset(), columnsToLoad);

      // Detect sharding-compatible fragments from zonemap stats. Each field checks its column's
      // zones; if every fragment has a single sharding value, we get a fragment-to-key map.
      Map<Integer, Object> fragmentShardingKeys = null;
      Expression activeShardingExpression = null;
      for (ShardingField field : SparkLanceShardingUtils.fields(activeShardingSpec)) {
        String column = SparkLanceShardingUtils.columnName(field, lanceSchema);
        List<ZoneStats> colStats = zonemapStats.get(column);
        if (colStats == null || colStats.isEmpty()) {
          LOG.warn(
              "Sharding column '{}' (transform={}) has no zonemap stats;"
                  + " sharding detection disabled",
              column,
              field.transform().orElse(null));
          continue;
        }
        java.util.Optional<Map<Integer, Object>> keys =
            SparkLanceShardingUtils.detectFragmentKeys(field, lanceSchema, colStats);
        if (keys.isPresent()) {
          fragmentShardingKeys = keys.get();
          activeShardingExpression = SparkLanceShardingUtils.toSparkExpression(field, lanceSchema);
          LOG.info(
              "Detected Lance sharding field {}('{}') with {} fragments",
              field.transform().orElse(null),
              column,
              fragmentShardingKeys.size());
          break;
        }
      }

      // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
      // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
      // to LanceScan to avoid re-computing during planInputPartitions().
      Set<Integer> survivingFragmentIds = null;
      if (pushedPredicates.length > 0 && !zonemapStats.isEmpty()) {
        survivingFragmentIds =
            ZonemapFragmentPruner.pruneFragments(pushedPredicates, zonemapStats).orElse(null);
      }

      // Scale rows and full size by the zonemap fragment-pruning ratio first, then let
      // LanceStatistics.estimateProjected apply the column-width ratio on top
      // (when the projected schema is narrower than the full schema).
      long projectedRows = summary.getTotalRows();
      long projectedFullSize = summary.getTotalFilesSize();
      if (survivingFragmentIds != null && summary.getTotalFragments() > 0) {
        double ratio = (double) survivingFragmentIds.size() / summary.getTotalFragments();
        projectedRows = (long) (projectedRows * ratio);
        projectedFullSize = (long) (projectedFullSize * ratio);
      }
      LanceStatistics statistics =
          LanceStatistics.estimateProjected(projectedRows, projectedFullSize, fullSchema, schema);
      if (survivingFragmentIds != null) {
        LOG.debug(
            "Scan statistics after pruning: {} of {} fragments survive,"
                + " estimatedSize={}, estimatedRows={} (full: size={}, rows={})",
            survivingFragmentIds.size(),
            summary.getTotalFragments(),
            statistics.sizeInBytes(),
            statistics.numRows(),
            summary.getTotalFilesSize(),
            summary.getTotalRows());
      }

      // Pre-compute splits and per-fragment row counts from the same Dataset handle that we
      // already opened above. This consolidates two driver-side opens into one and lets us pin
      // the resolved version onto the read options shipped to workers, providing snapshot
      // isolation across all tasks of this query. The version is kept as a long end-to-end so
      // long-lived high-write-frequency datasets do not silently truncate to a wrong version.
      LanceSplit.ScanPlanResult scanPlan = LanceSplit.planScan(dataset);
      LanceSparkReadOptions resolvedReadOptions =
          readOptions.withVersion(scanPlan.getResolvedVersion());
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "two-phase month read: conf={} option={}",
            twoPhaseMonthReadEnabled(),
            resolvedReadOptions.isTwoPhaseMonthRead());
      }
      if (!resolvedReadOptions.isTwoPhaseMonthRead() && twoPhaseMonthReadEnabled()) {
        resolvedReadOptions = resolvedReadOptions.withTwoPhaseMonthRead(true);
      }
      if (resolvedReadOptions.getMaxRowsPerSplit() <= 0) {
        int confMaxRows = maxRowsPerSplitFromConf();
        if (confMaxRows > 0) {
          resolvedReadOptions = resolvedReadOptions.withMaxRowsPerSplit(confMaxRows);
        }
      }
      if (resolvedReadOptions.getCoalesceColumns().isEmpty()) {
        Map<String, String> confCoalesce = coalesceColumnsFromConf();
        if (!confCoalesce.isEmpty()) {
          resolvedReadOptions = resolvedReadOptions.withCoalesceColumns(confCoalesce);
        }
      }

      Map<Integer, Long> fragmentRowCounts = new HashMap<>(scanPlan.getFragmentRowCounts());
      Set<Integer> monthCoveredFragments = null;
      boolean monthCoveredHasMatches = false;
      if (resolvedReadOptions.isTwoPhaseMonthRead()
          && resolvedReadOptions.isUseScalarIndex()
          && pushedPredicates.length > 0) {
        List<Predicate> monthPredicates = new ArrayList<>();
        for (Predicate p : pushedPredicates) {
          Set<String> refs = extractReferencedColumns(new Predicate[] {p});
          if (refs.size() == 1 && refs.contains("month")) {
            monthPredicates.add(p);
          }
        }
        if (!monthPredicates.isEmpty()) {
          Optional<String> monthWhere =
              FilterPushDown.compileFiltersToSqlWhereClause(
                  monthPredicates.toArray(new Predicate[0]));
          if (monthWhere.isPresent()) {
            try {
              int monthFieldId =
                  dataset.getLanceSchema().fields().stream()
                      .filter(f -> "month".equals(f.getName()))
                      .findFirst()
                      .map(LanceField::getId)
                      .orElse(-1);
              Set<Integer> covered = new HashSet<>();
              if (monthFieldId >= 0) {
                for (Index index : dataset.getIndexes()) {
                  if (index.fields().equals(Collections.singletonList(monthFieldId))
                      && index.fragments().isPresent()) {
                    covered.addAll(index.fragments().get());
                  }
                }
              }
              covered.retainAll(fragmentRowCounts.keySet());
              if (!covered.isEmpty()) {
                monthCoveredFragments = covered;
                Map<Integer, Long> matchedCounts = new HashMap<>();
                try (LanceScanner scanner =
                        dataset.newScan(
                            new ScanOptions.Builder()
                                .fragmentIds(new ArrayList<>(covered))
                                .columns(Collections.singletonList("month"))
                                .filter(monthWhere.get())
                                .withRowId(true)
                                .useScalarIndex(true)
                                .batchSize(65536)
                                .build());
                    ArrowReader reader = scanner.scanBatches()) {
                  while (reader.loadNextBatch()) {
                    UInt8Vector rowIdVec =
                        (UInt8Vector) reader.getVectorSchemaRoot().getVector("_rowid");
                    int count = rowIdVec.getValueCount();
                    for (int i = 0; i < count; i++) {
                      if (!rowIdVec.isNull(i)) {
                        long rowId = rowIdVec.get(i);
                        int fragId = (int) (rowId >>> 32);
                        matchedCounts.merge(fragId, 1L, Long::sum);
                      }
                    }
                  }
                }
                monthCoveredHasMatches = !matchedCounts.isEmpty();
                Set<Integer> monthSurviving = new HashSet<>();
                for (Integer fragId : fragmentRowCounts.keySet()) {
                  if (covered.contains(fragId)) {
                    long c = matchedCounts.getOrDefault(fragId, 0L);
                    fragmentRowCounts.put(fragId, c);
                    if (c > 0) {
                      monthSurviving.add(fragId);
                    }
                  } else {
                    monthSurviving.add(fragId);
                  }
                }
                if (survivingFragmentIds == null) {
                  survivingFragmentIds = monthSurviving;
                } else {
                  survivingFragmentIds = new HashSet<>(survivingFragmentIds);
                  survivingFragmentIds.retainAll(monthSurviving);
                }
                LOG.info(
                    "Month bitmap index pruning: {} of {} covered fragments matched {},"
                        + " {} total surviving fragments",
                    matchedCounts.size(),
                    covered.size(),
                    monthWhere.get(),
                    survivingFragmentIds.size());
              }
            } catch (Exception e) {
              LOG.warn("Failed to prune fragments by month bitmap index: {}", e.getMessage());
            }
          }
        }
      }

      List<LanceSplit> splits = scanPlan.getSplits();
      boolean canTwoPhaseSlice =
          !pushedAggregation.isPresent()
              && !limit.isPresent()
              && !offset.isPresent()
              && !topNSortOrders.isPresent()
              && !schema.isEmpty()
              && Arrays.stream(schema.fields())
                  .noneMatch(f -> f.name().startsWith("_") || f.name().contains("."))
              && BlobUtils.blobV2ColumnNames(schema).isEmpty()
              && Arrays.stream(schema.fields()).noneMatch(BlobUtils::isBlobSparkField);
      int maxRowsPerSplit = resolvedReadOptions.getMaxRowsPerSplit();
      if (canTwoPhaseSlice && maxRowsPerSplit > 0) {
        long totalActiveRows = 0L;
        for (LanceSplit split : splits) {
          if (split.getFragments().size() == 1) {
            int fragId = split.getFragments().get(0);
            if (survivingFragmentIds != null && !survivingFragmentIds.contains(fragId)) {
              continue;
            }
            boolean skipUnindexedSlice =
                monthCoveredFragments != null
                    && monthCoveredHasMatches
                    && !monthCoveredFragments.contains(fragId);
            if (!skipUnindexedSlice) {
              totalActiveRows += fragmentRowCounts.getOrDefault(fragId, 0L);
            }
          }
        }
        long effectiveMaxRowsPerSplit = Math.max((long) maxRowsPerSplit, totalActiveRows / 512L);
        List<LanceSplit> expandedSplits = new ArrayList<>(splits.size());
        for (LanceSplit split : splits) {
          if (split.getFragments().size() != 1) {
            expandedSplits.add(split);
            continue;
          }
          int fragId = split.getFragments().get(0);
          if (survivingFragmentIds != null && !survivingFragmentIds.contains(fragId)) {
            continue;
          }
          boolean skipUnindexedSlice =
              monthCoveredFragments != null
                  && monthCoveredHasMatches
                  && !monthCoveredFragments.contains(fragId);
          long rows = fragmentRowCounts.getOrDefault(fragId, 0L);
          int numSlices =
              (!skipUnindexedSlice && rows > effectiveMaxRowsPerSplit)
                  ? (int)
                      Math.min(
                          16L, (rows + effectiveMaxRowsPerSplit - 1) / effectiveMaxRowsPerSplit)
                  : 1;
          for (int s = 0; s < numSlices; s++) {
            expandedSplits.add(
                new LanceSplit(
                    Collections.singletonList(fragId), s, numSlices, skipUnindexedSlice));
          }
        }
        LOG.info(
            "Expanded splits with effectiveMaxRowsPerSplit={} (configured={}):"
                + " {} base fragments -> {} splits",
            effectiveMaxRowsPerSplit,
            maxRowsPerSplit,
            splits.size(),
            expandedSplits.size());
        splits = expandedSplits;
      }

      Optional<String> whereCondition =
          FilterPushDown.compileFiltersToSqlWhereClause(pushedPredicates);
      return new LanceScan(
          schema,
          resolvedReadOptions,
          whereCondition,
          limit,
          offset,
          topNSortOrders,
          pushedAggregation,
          pushedPredicates,
          statistics,
          zonemapStats,
          survivingFragmentIds,
          splits,
          fragmentRowCounts,
          activeShardingExpression,
          fragmentShardingKeys,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties);
    } finally {
      closeLazyDataset();
    }
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = ReadSchemaNestedStructWidening.widenRequiredSchema(requiredSchema, fullSchema);
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    Predicate[] pushed;
    Predicate[] residual;
    if (!readOptions.isPushDownFilters()) {
      pushed = new Predicate[0];
      residual = predicates;
    } else {
      List<Predicate> pushedList = new ArrayList<>();
      List<Predicate> residualList = new ArrayList<>();
      // Push supported predicates unless they touch a blob v2 column. Those read back as descriptor
      // structs, so Lance cannot evaluate filters on them. Normal-column filters still prune.
      for (Predicate predicate : predicates) {
        if (FilterPushDown.isPredicateSupported(predicate)
            && !FilterPushDown.referencesAny(predicate, blobV2Columns)) {
          pushedList.add(predicate);
        } else {
          residualList.add(predicate);
        }
      }
      pushed = pushedList.toArray(new Predicate[0]);
      residual = residualList.toArray(new Predicate[0]);
    }
    this.pushedPredicates = pushed;
    this.hasResidualPredicates = residual.length > 0;
    return residual;
  }

  @Override
  public Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  @Override
  public boolean pushLimit(int limit) {
    if (hasResidualPredicates) {
      return false;
    }
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    if (hasResidualPredicates) {
      return false;
    }
    // Only one data file can be pushed down the offset.
    List<Integer> fragmentIds =
        getOrOpenDataset().getFragments().stream()
            .map(Fragment::getId)
            .collect(Collectors.toList());
    if (fragmentIds.size() == 1) {
      this.offset = Optional.of(offset);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isPartiallyPushed() {
    return true;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    // The Order by operator will use compute thread in lance.
    // So it's better to have an option to enable it.
    if (!readOptions.isTopNPushDown() || hasResidualPredicates) {
      return false;
    }
    this.limit = Optional.of(limit);
    List<ColumnOrdering> topNSortOrders = new ArrayList<>();
    for (SortOrder sortOrder : orders) {
      ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
      builder.setNullFirst(sortOrder.nullOrdering() == NullOrdering.NULLS_FIRST);
      builder.setAscending(sortOrder.direction() == SortDirection.ASCENDING);
      if (!(sortOrder.expression() instanceof FieldReference)) {
        return false;
      }
      FieldReference reference = (FieldReference) sortOrder.expression();
      builder.setColumnName(reference.fieldNames()[0]);
      topNSortOrders.add(builder.build());
    }
    this.topNSortOrders = Optional.of(topNSortOrders);
    return true;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (hasResidualPredicates) {
      return false;
    }
    AggregateFunc[] funcs = aggregation.aggregateExpressions();
    if (aggregation.groupByExpressions().length > 0) {
      return false;
    }
    if (funcs.length == 1 && funcs[0] instanceof CountStar) {
      // Check if we can use metadata-based count (no filters pushed)
      if (pushedPredicates.length == 0) {
        Optional<Long> metadataCount = getCountFromMetadata(getOrOpenDataset());
        if (metadataCount.isPresent()) {
          // Create LocalScan with pre-computed count result
          StructType countSchema = new StructType().add("count", DataTypes.LongType);
          InternalRow[] rows = new InternalRow[1];
          rows[0] = new GenericInternalRow(new Object[] {metadataCount.get()});
          this.localScan = new LanceLocalScan(countSchema, rows, readOptions.getDatasetUri());
          return true;
        }
      }
      // Fall back to scan-based count (with filters or metadata unavailable)
      this.pushedAggregation = Optional.of(aggregation);
      return true;
    }

    return false;
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Loads zonemap statistics for the requested columns. Only loads stats for columns that have a
   * zonemap index.
   */
  private Map<String, List<ZoneStats>> loadZonemapStats(Dataset dataset, Set<String> columns) {
    if (columns.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> zonemapColumns = findZonemapIndexedColumns(dataset);
    LOG.debug("zonemapColumns={}, requested columns={}", zonemapColumns, columns);

    Map<String, List<ZoneStats>> result = new HashMap<>();
    for (String col : columns) {
      if (zonemapColumns.isEmpty() || zonemapColumns.contains(col)) {
        try {
          List<ZoneStats> stats = dataset.getZonemapStats(col);
          LOG.debug("getZonemapStats('{}') returned {} zones", col, stats.size());
          if (!stats.isEmpty()) {
            result.put(col, stats);
            LOG.debug("Loaded {} zonemap zones for column '{}'", stats.size(), col);
          }
        } catch (Exception e) {
          LOG.debug("Failed to load zonemap stats for column" + " '{}': {}", col, e.getMessage());
        }
      }
    }

    if (!result.isEmpty()) {
      LOG.debug("Loaded zonemap stats for {} columns: {}", result.size(), result.keySet());
    }

    return result;
  }

  private Set<String> findZonemapIndexedColumns(Dataset dataset) {
    Set<String> columns = new HashSet<>();
    try {
      Map<Integer, String> fieldIdToName = new HashMap<>();
      for (LanceField field : dataset.getLanceSchema().fields()) {
        fieldIdToName.put(field.getId(), field.getName());
      }

      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
        LOG.debug(
            "Index '{}' type='{}' fields={}", idx.getName(), idx.getIndexType(), idx.getFieldIds());
        if ("ZONEMAP".equalsIgnoreCase(idx.getIndexType())) {
          for (int fieldId : idx.getFieldIds()) {
            String name = fieldIdToName.get(fieldId);
            if (name != null) {
              columns.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to query zonemap indexes: {}", e.getMessage());
    }
    return columns;
  }

  private static Set<String> extractReferencedColumns(Predicate[] predicates) {
    Set<String> columns = new HashSet<>();
    for (Predicate predicate : predicates) {
      for (NamedReference ref : predicate.references()) {
        String[] names = ref.fieldNames();
        columns.add(names.length == 1 ? names[0] : String.join(".", names));
      }
    }
    return columns;
  }
}

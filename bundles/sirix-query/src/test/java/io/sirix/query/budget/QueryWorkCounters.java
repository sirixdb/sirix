/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import io.sirix.budget.WorkCounter;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.util.List;
import java.util.stream.Stream;

/**
 * The vectorized executor's served-route counters, as {@link WorkCounter}s a budget test can
 * capture.
 *
 * <p>
 * A projection query is answered by exactly one route, and which one decides how much the engine
 * reads: a value-count summary reads one blob, a sliced group-by reads the needed columns, a sorted
 * view reads group summaries, and a query no route accepts falls back to the generic pipeline over
 * every record. A route change is therefore a change in work even when the answer is identical,
 * which is what makes it invisible to a result-only test. The names are the ones the benchmark
 * runners print on their {@code # served:} line ({@code JsonBenchRunMain.printServedCounters}); the
 * inventory of what each route reads is section 7.3 of {@code docs/SEGMENT_PROJECTION_INDEXES.md}.
 *
 * <p>
 * All of them are process-wide totals with no reset, so they are only ever meaningful as the delta
 * a {@link io.sirix.budget.WorkCapture} takes.
 */
public final class QueryWorkCounters {

  private QueryWorkCounters() {
    throw new AssertionError("no instances");
  }

  /** {@code count(for ... where p return $r)} answered from the projection. */
  public static final WorkCounter PREDICATE_COUNTS = WorkCounter.alwaysOn("served.predicateCounts",
      "one predicate count answered from the projection", SirixVectorizedExecutor::projectionCountsServed);

  /** Every group-aggregate serve, whichever arm took it; the routes below are its sub-counts. */
  public static final WorkCounter GROUP_AGGREGATES = WorkCounter.alwaysOn("served.groupAggregates",
      "one group-aggregate query answered from the projection, by any arm",
      SirixVectorizedExecutor::groupAggServedCount);

  public static final WorkCounter NUMERIC_GROUP_BYS = WorkCounter.alwaysOn("served.numericGroupBys",
      "one group-by answered by a numeric group arm", SirixVectorizedExecutor::numericGroupByServedCount);

  /** A group serve that read column slices instead of whole leaves. */
  public static final WorkCounter GROUP_SLICED = WorkCounter.alwaysOn("served.groupSliced",
      "one group-aggregate answered from column slices rather than whole leaves",
      SirixVectorizedExecutor::groupAggSlicedServedCount);

  /** A count-only group-by answered from the persisted per-value row counts; reads no row group. */
  public static final WorkCounter GROUP_SUMMARY =
      WorkCounter.alwaysOn("served.groupSummary", "one count-only group-by answered from the value-count summary",
          SirixVectorizedExecutor::groupAggSummaryServedCount);

  public static final WorkCounter GROUP_DENSE = WorkCounter.alwaysOn("served.groupDense",
      "one ordered group-by answered from the dense global-key table", SirixVectorizedExecutor::groupDenseServedCount);

  public static final WorkCounter SORTED_SCANS = WorkCounter.alwaysOn("served.sortedScans",
      "one ordered record scan answered from projection sort columns", SirixVectorizedExecutor::sortedScanServedCount);

  /** A grouped min/max/span top-K answered from a sorted view; reads no row group. */
  public static final WorkCounter SORTED_GROUP_BYS = WorkCounter.alwaysOn("served.sortedGroupBys",
      "one grouped top-K answered from a sorted view", SirixVectorizedExecutor::groupSortedServedCount);

  public static final WorkCounter PREDICATE_SCANS = WorkCounter.alwaysOn("served.predicateScans",
      "one predicate record scan answered from column slices", SirixVectorizedExecutor::predicateScanServedCount);

  public static final WorkCounter VALUE_EMISSIONS =
      WorkCounter.alwaysOn("served.valueEmissions", "one predicate scan whose projected field came from column slices",
          SirixVectorizedExecutor::predicateValueEmissionsServedCount);

  /** A group-aggregate the projection route looked at and handed to the generic pipeline. */
  public static final WorkCounter GROUP_AGGREGATES_DECLINED = WorkCounter.alwaysOn("declined.groupAggregates",
      "one group-aggregate the projection route declined, leaving it to the generic pipeline",
      SirixVectorizedExecutor::groupAggregateDeclinedCount);

  /** A group-aggregate arm that threw and was answered by the generic pipeline instead. */
  public static final WorkCounter GROUP_AGGREGATES_FAILED = WorkCounter.alwaysOn("failed.groupAggregates",
      "one group-aggregate arm that failed and fell back to the generic pipeline",
      SirixVectorizedExecutor::groupAggFailedCount);

  /** The {@code # served:} line, in the order the benchmark runners print it. */
  public static final List<WorkCounter> SERVED = List.of(PREDICATE_COUNTS, GROUP_AGGREGATES, NUMERIC_GROUP_BYS,
      GROUP_SLICED, GROUP_SUMMARY, GROUP_DENSE, SORTED_SCANS, SORTED_GROUP_BYS, PREDICATE_SCANS, VALUE_EMISSIONS);

  /**
   * The served routes plus the two ways a group-aggregate leaves them. Built from {@link #SERVED}
   * rather than restating it: a route named in one list and forgotten in the other would drop out of
   * every capture the budget tests take, which is the quiet loss of coverage this package exists to
   * prevent.
   */
  public static final List<WorkCounter> ROUTES = Stream
                                                       .concat(SERVED.stream(),
                                                           Stream.of(GROUP_AGGREGATES_DECLINED,
                                                               GROUP_AGGREGATES_FAILED))
                                                       .toList();
}

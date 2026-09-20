/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.settings.VersioningType;

import java.util.List;

/**
 * The storage engine's own work counters, as {@link WorkCounter}s a budget test can capture.
 *
 * <p>
 * Nothing here counts anything itself: every entry reads a figure the engine already maintains, so
 * the numbers are the ones a benchmark campaign or an investigation would quote. Add a counter to
 * the engine only when a path a test must guard has none, keep it off the hot path (see how
 * {@code VersioningType} gates its merge counters and why {@code AbstractReader} does not gate its
 * chunk-read counters), then list it here and in the README beside this class.
 */
public final class EngineWorkCounters {

  private EngineWorkCounters() {
    throw new AssertionError("no instances");
  }

  // ===== HOT leaf pages =====================================================

  /**
   * HOT leaf pages loaded from storage. Every such load passes through the versioned fragment merge
   * exactly once, whichever of its three outcomes answers it, so their sum is the number of leaves a
   * cache-cold operation read. A {@code FULL}-versioned resource bypasses the merge and reads zero.
   */
  public static final WorkCounter HOT_LEAF_LOADS =
      WorkCounter.gated("hot.leafLoads",
          "one HOT leaf page reconstructed from storage (single fragment, complete dump or multi-fragment merge)",
          () -> VersioningType.singleFragmentReads() + VersioningType.completeDumpShortCircuits()
              + VersioningType.multiFragmentMerges(),
          "-Dsirix.hot.mergeDiag=true", VersioningType::hotMergeDiagEnabled);

  /** Older fragments read to reconstruct those leaves: the read amplification of versioning. */
  public static final WorkCounter HOT_FRAGMENTS_WALKED =
      WorkCounter.gated("hot.fragmentsWalked", "one older fragment of a HOT leaf read during a multi-fragment merge",
          VersioningType::fragmentsWalked, "-Dsirix.hot.mergeDiag=true", VersioningType::hotMergeDiagEnabled);

  /** The HOT leaf counters. Gated: the module's {@code test} block must provide the property. */
  public static final List<WorkCounter> HOT_LEAVES = List.of(HOT_LEAF_LOADS, HOT_FRAGMENTS_WALKED);

  // ===== Batched page reads (FILE_CHANNEL) ==================================

  /** Coalesced runs: near-adjacent pages of one batch read with two positional reads in total. */
  public static final WorkCounter READ_RUNS = WorkCounter.alwaysOn("read.coalescedRuns",
      "one coalesced run of near-adjacent pages: a span read plus the last page's body", FileChannelReader::runCount);

  /** Bytes those runs covered; a multiple of the pages' size means a region was read repeatedly. */
  public static final WorkCounter READ_RUN_SPAN_BYTES = WorkCounter.alwaysOn("read.runSpanBytes",
      "one byte covered by a coalesced run's span read, gaps between its pages included",
      FileChannelReader::runSpanBytes);

  /** Run members re-read exactly because their body crossed the next member's offset. */
  public static final WorkCounter READ_RUN_FALLBACKS = WorkCounter.alwaysOn("read.runFallbacks",
      "one page of a coalesced run re-read on its own because it did not end before its successor",
      FileChannelReader::runFallbacks);

  /** Batch members that found no neighbour to coalesce with: the scalar path inside a batch. */
  public static final WorkCounter READ_SINGLETONS = WorkCounter.alwaysOn("read.batchSingletons",
      "one page of a batch read on its own because no other member was near-adjacent",
      FileChannelReader::runSingletons);

  /** The batched-read counters of the file-channel backend; the memory-mapped reader has none. */
  public static final List<WorkCounter> BATCHED_READS =
      List.of(READ_RUNS, READ_RUN_SPAN_BYTES, READ_RUN_FALLBACKS, READ_SINGLETONS);

  // ===== Projection payload materialization =================================

  /**
   * Windowed engagements of a projection handle. The page-level lazy loads that share this figure are
   * counted only under {@code -Dsirix.chunkedBody.diag}; the projection events are unconditional.
   */
  public static final WorkCounter LAZY_LOADS = WorkCounter.alwaysOn("chunked.lazyLoads",
      "one projection handle served through windowed payload loads", ChunkedBodyConfig::lazyLoads);

  public static final WorkCounter CHUNK_MATERIALIZATIONS = WorkCounter.alwaysOn("chunked.chunkMaterializations",
      "one window of projection leaves materialized", ChunkedBodyConfig::chunkMaterializations);

  /**
   * Whole-projection materializations. At scale this must stay zero: a projection over the eager
   * budget that is materialized whole anyway is the entire index pulled into memory for one query.
   */
  public static final WorkCounter EAGER_FALLBACKS = WorkCounter.alwaysOn("chunked.eagerFallbacks",
      "one projection handle materialized whole instead of through windows", ChunkedBodyConfig::eagerFallbacks);

  /** The benchmark runner's {@code # chunked:} line. */
  public static final List<WorkCounter> CHUNKED_BODIES = List.of(LAZY_LOADS, CHUNK_MATERIALIZATIONS, EAGER_FALLBACKS);

  // ===== Transaction intent log =============================================

  /**
   * Record pages promoted into the intent log's pinned region because a page they reference could not
   * be written ahead of the commit. Pinned pages leave only at the final commit, so this growing with
   * the load is memory growing with the load.
   */
  public static final WorkCounter KVL_PAGES_PINNED_BY_PROMOTION = WorkCounter.alwaysOn("til.kvlPagesPinnedByPromotion",
      "one record page pinned until the final commit", TransactionIntentLog::kvlPagesPinnedByPromotion);

  public static final WorkCounter KVL_PAGES_RETRIED_NEXT_EPOCH = WorkCounter.alwaysOn("til.kvlPagesRetriedNextEpoch",
      "one record page whose flush was deferred to the next epoch", TransactionIntentLog::kvlPagesRetriedNextEpoch);

  public static final List<WorkCounter> INTENT_LOG =
      List.of(KVL_PAGES_PINNED_BY_PROMOTION, KVL_PAGES_RETRIED_NEXT_EPOCH);
}

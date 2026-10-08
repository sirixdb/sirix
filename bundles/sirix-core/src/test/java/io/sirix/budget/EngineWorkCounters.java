/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.index.interval.HotOrderedStore;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.VersioningType;
import io.sirix.utils.ReplayWorkDiagnostics;

import java.util.List;
import java.util.function.LongSupplier;

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

  public static final WorkCounter VALID_TIME_INTERVAL_REFS = WorkCounter.gated("validTime.intervalRefs",
      "one record reference emitted by an interval store scan", HotOrderedStore::intervalRefsEmitted,
      "-Dsirix.validTime.scanDiag=true", HotOrderedStore::scanDiagnosticsEnabled);

  public static final WorkCounter VALID_TIME_POSTING_REFS = WorkCounter.gated("validTime.postingRefs",
      "one record reference emitted by a membership, verification or order posting scan",
      HotOrderedStore::postingRefsEmitted, "-Dsirix.validTime.scanDiag=true", HotOrderedStore::scanDiagnosticsEnabled);

  public static final WorkCounter VALID_TIME_POSTING_LOOKUPS =
      WorkCounter.gated("validTime.postingLookups", "one membership, verification or order posting lookup",
          HotOrderedStore::postingLookups, "-Dsirix.validTime.scanDiag=true", HotOrderedStore::scanDiagnosticsEnabled);

  public static final WorkCounter VALID_TIME_POSTING_CHUNKS = WorkCounter.gated("validTime.postingChunks",
      "one compressed posting chunk read without enumerating its references", HotOrderedStore::postingChunksRead,
      "-Dsirix.validTime.scanDiag=true", HotOrderedStore::scanDiagnosticsEnabled);

  // ===== Projection leaf pruning ===========================================

  /**
   * Leaves a projection scan dropped from its keep mask before any column segment was fetched —
   * descriptor zones, string fingerprints, and the index-routed row source's record-key ranges. A
   * masked scan over N leaves of which K hold an admitted key must read K leaves: this is the
   * figure that says the other N − K were never fetched.
   */
  public static final WorkCounter PROJECTION_LEAVES_PRUNED = WorkCounter.alwaysOn("projection.leavesPruned",
      "one projection leaf dropped by a scan's keep mask before its segments were fetched",
      ProjectionColumnScan::leavesPrunedCount);

  // ===== HOT leaf pages =====================================================

  /**
   * Complete HOT leaf reconstructions plus requested-slot fragment walks. A slot walk counts once
   * even when its raw images hit the fragment cache; complete-leaf and mini-page hits skip this work.
   * A {@code FULL}-versioned resource bypasses the merge and contributes zero.
   */
  public static final WorkCounter HOT_LEAF_LOADS = WorkCounter.gated("hot.leafLoads",
      "one HOT leaf resolved from storage (complete reconstruction or requested-slot lookup)",
      () -> VersioningType.singleFragmentReads() + VersioningType.completeDumpShortCircuits()
          + VersioningType.multiFragmentMerges() + VersioningType.pointLeafReads(),
      "-Dsirix.hot.mergeDiag=true", VersioningType::hotMergeDiagEnabled);

  /**
   * Older fragments inspected during reconstruction or slot lookup, including fragment-cache hits.
   */
  public static final WorkCounter HOT_FRAGMENTS_WALKED =
      WorkCounter.gated("hot.fragmentsWalked", "one older HOT fragment inspected during reconstruction or slot lookup",
          () -> VersioningType.fragmentsWalked() + VersioningType.pointFragmentsWalked(), "-Dsirix.hot.mergeDiag=true",
          VersioningType::hotMergeDiagEnabled);

  /** The HOT leaf counters. Gated: the module's {@code test} block must provide the property. */
  public static final List<WorkCounter> HOT_LEAVES = List.of(HOT_LEAF_LOADS, HOT_FRAGMENTS_WALKED);

  /** Native lane reads during a HOT suffix comparison, excluding its fixed length header. */
  public static final WorkCounter HOT_SUFFIX_PROBE_READS =
      WorkCounter.gated("hot.suffixProbeReads", "one native suffix lane inspected during binary search",
          HOTLeafPage::suffixProbeReads, "-Dsirix.hot.mergeDiag=true", VersioningType::hotMergeDiagEnabled);

  /** Side-reference map accesses, including absent overflow references of inline slots. */
  public static final WorkCounter HOT_SIDE_REFERENCE_READS =
      WorkCounter.gated("hot.sideReferenceReads", "one overflow-reference map probe", HOTLeafPage::sideReferenceReads,
          "-Dsirix.hot.mergeDiag=true", VersioningType::hotMergeDiagEnabled);

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

  // ===== Index catalogue ====================================================

  /**
   * Listings of a resource's {@code indexes/} directory. The directory holds one catalogue file per
   * commit that had definitions, so a writer that resolves its catalogue by listing it does work
   * proportional to the number of revisions, on every commit.
   */
  public static final WorkCounter INDEX_CATALOGUE_LISTINGS =
      WorkCounter.alwaysOn("catalogue.directoryListings", "one listing of a resource's index-catalogue directory",
          AbstractResourceSession::indexCatalogueDirectoryListings);

  // ===== Identity replay and source diff bookkeeping =======================

  public static final WorkCounter REPLAY_RECORD_VISITS = replay("replay.recordVisits",
      "one node cursor move, storage lookup or prepare call, including nested fallbacks and commit lifecycle work",
      ReplayWorkDiagnostics::recordVisits);
  public static final WorkCounter REPLAY_PATH_STEPS = replay("replay.pathSteps",
      "one path-summary cursor move, including cached moves and commit-time writer initialization",
      ReplayWorkDiagnostics::pathSteps);
  public static final WorkCounter REPLAY_CREATED_IDENTITIES = replay("replay.createdIdentities",
      "one staged document identity absent from the exact base epoch", ReplayWorkDiagnostics::createdIdentities);
  public static final WorkCounter REPLAY_STAGED_RECORDS = replay("replay.stagedRecords",
      "one detached document record staged before link installation", ReplayWorkDiagnostics::stagedRecords);
  public static final WorkCounter REPLAY_ANCESTOR_STEPS = replay("replay.ancestorSteps",
      "one parent hop while proving changed identities reach the validated document root",
      ReplayWorkDiagnostics::ancestorSteps);
  public static final WorkCounter REPLAY_SIDECAR_READS =
      replay("replay.sidecarReads", "one attempted presentation sidecar read", ReplayWorkDiagnostics::sidecarReads);
  public static final WorkCounter REPLAY_FALLBACK_PAGES = replay("replay.fallbackPages",
      "one authoritative indirect-page or complete-leaf resolution, including guard retries",
      ReplayWorkDiagnostics::fallbackPages);
  public static final WorkCounter DIFF_BOOKKEEPING = replay("diff.bookkeepingOperations",
      "one pending-diff keyed operation, entry visited by an iterator, or entry cleared",
      ReplayWorkDiagnostics::bookkeepingOperations);
  public static final WorkCounter REPLAY_PROJECTION_ROWS = replay("replay.projectionRows",
      "one old row removal or final row insertion queued by a projection identity epoch",
      ReplayWorkDiagnostics::projectionIdentityRows);
  public static final WorkCounter REPLAY_PROJECTION_LABEL_BYTES =
      replay("replay.projectionLabelBytes", "one byte allocated for a final projection identity row's order label",
          ReplayWorkDiagnostics::projectionIdentityLabelBytes);
  public static final WorkCounter REPLAY_VALID_TIME_BOUND_FIELDS = replay("replay.validTimeBoundFields",
      "one direct child inspected while the valid-time listener reconstructs an object's bounds",
      ReplayWorkDiagnostics::validTimeBoundFields);
  public static final WorkCounter REPLAY_PROJECTION_ORDER_SLOTS = replay("replay.projectionOrderSlots",
      "one projection structural-order slot requested, including an absent unlabelled slot",
      ReplayWorkDiagnostics::projectionOrderSlots);
  public static final WorkCounter REPLAY_PROJECTION_RECORD_READS =
      replay("replay.projectionRecordReads", "one document record requested by the projection maintenance listener",
          ReplayWorkDiagnostics::projectionRecordReads);
  public static final List<WorkCounter> REPLAY = List.of(REPLAY_RECORD_VISITS, REPLAY_PATH_STEPS,
      REPLAY_CREATED_IDENTITIES, REPLAY_STAGED_RECORDS, REPLAY_ANCESTOR_STEPS, REPLAY_SIDECAR_READS,
      REPLAY_FALLBACK_PAGES, DIFF_BOOKKEEPING, REPLAY_PROJECTION_ROWS, REPLAY_PROJECTION_LABEL_BYTES,
      REPLAY_VALID_TIME_BOUND_FIELDS, REPLAY_PROJECTION_ORDER_SLOTS, REPLAY_PROJECTION_RECORD_READS);

  private static WorkCounter replay(final String name, final String unit, final LongSupplier read) {
    return WorkCounter.gated(name, unit, read, "-Dsirix.replay.workDiag=true", () -> ReplayWorkDiagnostics.ENABLED);
  }

}

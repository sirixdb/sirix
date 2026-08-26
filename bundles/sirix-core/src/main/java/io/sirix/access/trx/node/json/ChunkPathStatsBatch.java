/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.index.path.summary.PathStatsAccumulator;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.function.BiConsumer;

/**
 * One chunk's per-path-class statistics partials, collected in the parallel importer's build worker
 * at the same value-create sites that feed the index tuples, and merged by the coordinator into its
 * {@link PathSummaryWriter} at chunk adoption.
 *
 * <p>
 * Each partial is a {@link PathStatsAccumulator} — the exact class the cursor path defers through —
 * so every observation semantics (numeric lane dispatch, NaN/overflow policy, fraction carry,
 * byte-bound cloning, HLL hashing, page-witness derivation) is shared with cursor ingestion by
 * construction. The coordinator drains chunks in DOCUMENT order — for most lanes any order would
 * do, but {@code sumFraction} and the overflow flag are order-sensitive and document order makes
 * them deterministic (see {@link PathStatsAccumulator#mergeFrom} for the honest contract).
 *
 * <p>
 * Worker-side single-threaded; dies after the coordinator drains it.
 */
public final class ChunkPathStatsBatch {

  /**
   * Test seam: when non-null, invoked once per (pathNodeKey, partial) just before the partial is
   * merged into the coordinator's summary writer. Exists so the differential gate can prove it
   * CATCHES a corrupted chunk partial (inversion witness); never set in production.
   */
  public static volatile BiConsumer<Long, PathStatsAccumulator> TEST_PARTIAL_PERTURBATION;

  private final Long2ObjectOpenHashMap<PathStatsAccumulator> partials = new Long2ObjectOpenHashMap<>(32);

  private PathStatsAccumulator partialFor(final long pathNodeKey) {
    PathStatsAccumulator acc = partials.get(pathNodeKey);
    if (acc == null) {
      acc = new PathStatsAccumulator();
      partials.put(pathNodeKey, acc);
    }
    return acc;
  }

  void recordString(final long pathNodeKey, final byte[] utf8, final int length, final long nodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    final PathStatsAccumulator acc = partialFor(pathNodeKey);
    acc.addBytes(utf8, 0, length);
    acc.recordPageOfNode(nodeKey);
  }

  void recordNumber(final long pathNodeKey, final Number value, final long nodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    final PathStatsAccumulator acc = partialFor(pathNodeKey);
    acc.addNumber(value);
    acc.recordPageOfNode(nodeKey);
  }

  void recordBoolean(final long pathNodeKey, final boolean value, final long nodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    final PathStatsAccumulator acc = partialFor(pathNodeKey);
    acc.addBoolean(value);
    acc.recordPageOfNode(nodeKey);
  }

  void recordNull(final long pathNodeKey, final long nodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    final PathStatsAccumulator acc = partialFor(pathNodeKey);
    acc.addNull();
    acc.recordPageOfNode(nodeKey);
  }

  /** Whether the chunk recorded nothing (skips the drain call entirely). */
  boolean isEmpty() {
    return partials.isEmpty();
  }

  /**
   * Merge every partial into the coordinator's summary writer. Single-threaded coordinator context;
   * the pre-commit {@code flushPendingStats()} applies the merged deltas through the ordinary COW
   * path.
   */
  void mergeInto(final PathSummaryWriter<JsonNodeReadOnlyTrx> summaryWriter) {
    final BiConsumer<Long, PathStatsAccumulator> perturbation = TEST_PARTIAL_PERTURBATION;
    for (final var entry : partials.long2ObjectEntrySet()) {
      if (perturbation != null) {
        perturbation.accept(entry.getLongKey(), entry.getValue());
      }
      summaryWriter.mergeExternalStats(entry.getLongKey(), entry.getValue());
    }
    partials.clear();
  }
}

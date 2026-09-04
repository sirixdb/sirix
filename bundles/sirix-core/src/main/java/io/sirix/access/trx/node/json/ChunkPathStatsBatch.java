/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.index.path.summary.PathStatsAccumulator;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * One chunk's per-path-class statistics partials, collected in the parallel importer's build worker
 * at the same value-create sites that feed the index tuples, and merged by the coordinator into its
 * {@link PathSummaryWriter} at chunk adoption.
 *
 * <p>
 * Each partial is a {@link PathStatsAccumulator} — the exact class the cursor path defers through —
 * so every observation semantics (numeric lane dispatch, NaN/overflow policy, fraction carry,
 * byte-bound cloning, HLL hashing, page-witness derivation) is shared with cursor ingestion by
 * construction. The coordinator drains chunks in DOCUMENT order — every lane but one is order-free
 * (the integral sum included, since it accumulates in 128 bits), and document order makes the
 * single order-sensitive lane, {@code sumFraction}, deterministic (see
 * {@link PathStatsAccumulator#mergeFrom} for the honest contract).
 *
 * <p>
 * Worker-side single-threaded; dies after the coordinator drains it.
 */
public final class ChunkPathStatsBatch {

  /**
   * Corruption injected into a chunk partial just before it is drained — the differential gate's
   * inversion witness, which is what proves the oracle is not vacuous.
   *
   * <p>
   * Primitive key, no boxing, and package-private in both directions: only same-package code can
   * install one, so the seam cannot be reached from application code or from an unrelated test.
   */
  @FunctionalInterface
  interface PartialPerturbation {
    void perturb(long pathNodeKey, PathStatsAccumulator partial);
  }

  /** Installed only by same-package test support; {@code null} in production. */
  static volatile PartialPerturbation partialPerturbation;

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

  void recordLong(final long pathNodeKey, final long value, final long nodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    final PathStatsAccumulator acc = partialFor(pathNodeKey);
    acc.addLong(value);
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
    final PartialPerturbation perturbation = partialPerturbation;
    for (final var entry : partials.long2ObjectEntrySet()) {
      if (perturbation != null) {
        perturbation.perturb(entry.getLongKey(), entry.getValue());
      }
      summaryWriter.mergeExternalStats(entry.getLongKey(), entry.getValue());
    }
    partials.clear();
  }
}

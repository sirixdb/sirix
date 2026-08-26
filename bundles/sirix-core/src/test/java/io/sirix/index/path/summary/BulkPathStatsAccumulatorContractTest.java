/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link PathStatsAccumulator}'s merge contract — the two lanes that are NOT plainly
 * associative, in the directions the javadoc promises.
 *
 * <ul>
 * <li><b>Same-sign overflow</b> (the ClickBench UserID shape — 64-bit ids overflow a long sum
 * within a few dozen values): sequential adds AND any chunked merge must BOTH end untrusted.</li>
 * <li><b>Mixed-sign boundary overflow</b>: chunk partials can overflow where the sequential running
 * sum does not. The contract is one-sided conservative — the merge may be MORE untrusted than the
 * sequential feed, never less — and everything served-relevant that remains (count, min, max) stays
 * exact.</li>
 * </ul>
 */
final class BulkPathStatsAccumulatorContractTest {

  private static final long BIG = Long.MAX_VALUE / 3;

  @Test
  void sameSignOverflowUntrustsBothArms() {
    final long[] values = {BIG, BIG, BIG, BIG};

    final PathStatsAccumulator sequential = new PathStatsAccumulator();
    for (final long v : values) {
      sequential.addLong(v);
    }
    assertTrue(sequential.valueStatsUntrusted, "sequential same-sign overflow must untrust the sum");

    final PathStatsAccumulator merged = mergeChunks(List.of(new long[] {BIG, BIG}, new long[] {BIG, BIG}));
    assertTrue(merged.valueStatsUntrusted, "chunked same-sign overflow must untrust the sum");

    assertEquals(sequential.count, merged.count, "count is associative");
    assertEquals(sequential.min, merged.min, "min is associative");
    assertEquals(sequential.max, merged.max, "max is associative");
  }

  @Test
  void mixedSignChunkOverflowIsOneSidedConservative() {
    // Sequential order interleaves signs, so the running sum never leaves the long range.
    final PathStatsAccumulator sequential = new PathStatsAccumulator();
    for (int i = 0; i < 4; i++) {
      sequential.addLong(BIG);
      sequential.addLong(-BIG);
    }
    assertFalse(sequential.valueStatsUntrusted, "interleaved signs never overflow sequentially");
    assertEquals(0L, sequential.sum, "the interleaved sum is exactly zero");

    // Chunking splits the same multiset by sign: the positive partial overflows on its own.
    final PathStatsAccumulator merged =
        mergeChunks(List.of(new long[] {BIG, BIG, BIG, BIG}, new long[] {-BIG, -BIG, -BIG, -BIG}));
    assertTrue(merged.valueStatsUntrusted,
        "the positive chunk partial overflows — the merge must be MORE untrusted, never silently wrong");

    // The conservative direction, stated as the boolean order: merged >= sequential.
    assertTrue(!sequential.valueStatsUntrusted || merged.valueStatsUntrusted,
        "a merge may never be LESS untrusted than the sequential feed");

    // Everything else the summary might serve stays exact.
    assertEquals(sequential.count, merged.count);
    assertEquals(sequential.min, merged.min);
    assertEquals(sequential.max, merged.max);
    assertEquals(sequential.nullCount, merged.nullCount);
  }

  @Test
  void orderInsensitiveLanesMatchAcrossMergeOrders() {
    final long[][] chunks = {{1, 5}, {3}, {2, 4, 6}};
    final PathStatsAccumulator forward = mergeChunks(List.of(chunks[0], chunks[1], chunks[2]));
    final PathStatsAccumulator backward = mergeChunks(List.of(chunks[2], chunks[1], chunks[0]));
    assertEquals(forward.count, backward.count);
    assertEquals(forward.sum, backward.sum);
    assertEquals(forward.min, backward.min);
    assertEquals(forward.max, backward.max);
    assertEquals(forward.hll.estimate(), backward.hll.estimate(), "HLL union is order-free");
  }

  @Test
  void nanUntrustsButStillCounts() {
    final PathStatsAccumulator acc = new PathStatsAccumulator();
    acc.addDouble(Double.NaN);
    assertTrue(acc.valueStatsUntrusted, "NaN must untrust the value stats");
    assertEquals(1L, acc.count, "the record still counted — count survives a NaN");
    assertFalse(acc.isEmpty(), "a NaN-only partial is NOT empty — the drain gate must keep it");
  }

  private static PathStatsAccumulator mergeChunks(final List<long[]> chunks) {
    final PathStatsAccumulator target = new PathStatsAccumulator();
    for (final long[] chunk : chunks) {
      final PathStatsAccumulator partial = new PathStatsAccumulator();
      for (final long v : chunk) {
        partial.addLong(v);
      }
      target.mergeFrom(partial);
    }
    return target;
  }
}

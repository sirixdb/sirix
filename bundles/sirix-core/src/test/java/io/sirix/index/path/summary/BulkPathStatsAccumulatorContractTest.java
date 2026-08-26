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
 * Pins {@link PathStatsAccumulator}'s merge contract: a chunked merge and a sequential feed of the
 * SAME observation multiset must be indistinguishable in every lane the summary serves.
 *
 * <ul>
 * <li><b>Same-sign overflow</b> (the ClickBench UserID shape — 64-bit ids leave long range within a
 * few dozen values): sequential adds AND any chunked merge must BOTH end untrusted.</li>
 * <li><b>Mixed-sign boundary overflow</b>: the adversarial order {@code [M, M, -M]} against the
 * chunking {@code [M, -M] | [M]}. A 64-bit running accumulator that drops overflowing addends
 * answers these differently (untrusted with a partial total vs. trusted with another total); the
 * 128-bit accumulator makes both arms agree on the total AND on the trust verdict, because the
 * verdict is a function of the multiset and not of the order.</li>
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
    assertTrue(sequential.isValueStatsUntrusted(), "sequential same-sign overflow must untrust the sum");

    final PathStatsAccumulator merged = mergeChunks(List.of(new long[] {BIG, BIG}, new long[] {BIG, BIG}));
    assertTrue(merged.isValueStatsUntrusted(), "chunked same-sign overflow must untrust the sum");

    assertEquals(sequential.count, merged.count, "count is associative");
    assertEquals(sequential.min, merged.min, "min is associative");
    assertEquals(sequential.max, merged.max, "max is associative");
  }

  /**
   * The order that breaks a 64-bit running accumulator: document order {@code [M, M, -M]} transiently
   * leaves long range and comes back, while the chunking {@code [M, -M] | [M]} never does. Dropping
   * the overflowing addend would end the sequential arm untrusted at {@code M} and the merged arm
   * TRUSTED at {@code M} — same values, different persisted statistics and a different serving
   * decision. Both arms must agree, on the total and on the verdict.
   */
  @Test
  void mixedSignOverflowAgreesAcrossChunkings() {
    final long m = Long.MAX_VALUE;

    final PathStatsAccumulator sequential = new PathStatsAccumulator();
    sequential.addLong(m);
    sequential.addLong(m);
    sequential.addLong(-m);

    final PathStatsAccumulator merged = mergeChunks(List.of(new long[] {m, -m}, new long[] {m}));

    assertEquals(sequential.sumFitsLong(), merged.sumFitsLong(), "the arms disagree on representability");
    assertEquals(sequential.isValueStatsUntrusted(), merged.isValueStatsUntrusted(),
        "the arms disagree on the trust verdict — bulk statistics would diverge from cursor statistics");
    assertEquals(sequential.sumAsLong(), merged.sumAsLong(), "the arms disagree on the sum");

    // The true total is exactly M, which IS representable — so both arms must serve it.
    assertFalse(sequential.isValueStatsUntrusted(), "the true total fits a long; nothing may be untrusted");
    assertEquals(m, sequential.sumAsLong(), "the exact total is Long.MAX_VALUE");

    assertEquals(sequential.count, merged.count);
    assertEquals(sequential.min, merged.min);
    assertEquals(sequential.max, merged.max);
    assertEquals(sequential.nullCount, merged.nullCount);
  }

  /**
   * The same agreement where the TRUE total genuinely leaves long range: every chunking must reach
   * the untrusted verdict, whatever the per-chunk partials look like on their own.
   */
  @Test
  void trueOverflowUntrustsEveryChunking() {
    final long m = Long.MAX_VALUE;

    final PathStatsAccumulator sequential = new PathStatsAccumulator();
    sequential.addLong(m);
    sequential.addLong(m);
    assertTrue(sequential.isValueStatsUntrusted(), "2 * Long.MAX_VALUE does not fit a long");

    final PathStatsAccumulator merged = mergeChunks(List.of(new long[] {m}, new long[] {m}));
    assertTrue(merged.isValueStatsUntrusted(), "the chunked arm must reach the same verdict");

    // Sign-split chunking of a multiset whose true total is zero stays TRUSTED and exact.
    final PathStatsAccumulator signSplit =
        mergeChunks(List.of(new long[] {BIG, BIG, BIG, BIG}, new long[] {-BIG, -BIG, -BIG, -BIG}));
    assertFalse(signSplit.isValueStatsUntrusted(), "a representable total must not be untrusted by chunking");
    assertEquals(0L, signSplit.sumAsLong(), "the sign-split total is exactly zero");
  }

  @Test
  void orderInsensitiveLanesMatchAcrossMergeOrders() {
    final long[][] chunks = {{1, 5}, {3}, {2, 4, 6}};
    final PathStatsAccumulator forward = mergeChunks(List.of(chunks[0], chunks[1], chunks[2]));
    final PathStatsAccumulator backward = mergeChunks(List.of(chunks[2], chunks[1], chunks[0]));
    assertEquals(forward.count, backward.count);
    assertEquals(forward.sumAsLong(), backward.sumAsLong());
    assertEquals(forward.min, backward.min);
    assertEquals(forward.max, backward.max);
    assertEquals(forward.hll.estimate(), backward.hll.estimate(), "HLL union is order-free");
  }

  @Test
  void nanUntrustsButStillCounts() {
    final PathStatsAccumulator acc = new PathStatsAccumulator();
    acc.addDouble(Double.NaN);
    assertTrue(acc.isValueStatsUntrusted(), "NaN must untrust the value stats");
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

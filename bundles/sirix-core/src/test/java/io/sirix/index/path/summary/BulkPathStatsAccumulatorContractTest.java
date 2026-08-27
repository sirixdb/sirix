/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link PathStatsAccumulator}'s merge contract where it actually decides anything: a chunked
 * merge and a sequential feed of the SAME observation multiset must leave the PathNode in the same
 * state, and that state is what a reader consults.
 *
 * <p>
 * Every assertion goes through {@link PathSummaryWriter#applyDeferredStats} — the production fold,
 * on a real {@link PathNode} — and reads back through the accessors the vectorized executor's
 * aggregate short-circuit uses ({@link PathNode#isStatsSumTrustworthy()},
 * {@link PathNode#getStatsSum()}). Asserting a batch-local predicate instead would prove only that
 * a method no production path invokes agrees with itself.
 *
 * <ul>
 * <li><b>Same-sign overflow</b> (the ClickBench UserID shape — 64-bit ids leave long range within a
 * few dozen values): the sequential feed AND any chunked merge must BOTH end untrusted.</li>
 * <li><b>Mixed-sign boundary overflow</b>: the adversarial order {@code [M, M, -M]} against the
 * chunking {@code [M, -M] | [M]}. A 64-bit running accumulator that drops overflowing addends
 * answers these differently (untrusted with a partial total vs. trusted with another total); the
 * 128-bit accumulator makes both arms agree on the served sum AND on the trust verdict, because the
 * verdict is a function of the multiset and not of the order.</li>
 * </ul>
 */
final class BulkPathStatsAccumulatorContractTest {

  private static final long BIG = Long.MAX_VALUE / 3;

  /** What a reader can see of a path after the batches have been folded in. */
  private record Served(long count, long nullCount, long sum, long min, long max, boolean sumTrustworthy) {
  }

  private static PathNode freshPathNode() {
    return new PathNode(new QNm("v"), NodeKind.OBJECT_NAMED_NUMBER, 1, 1, 1L, -1L, -1, 0, (SirixDeweyID) null, -1L, -1L,
        -1L, -1L, 0L, 0L, -1, -1, 42, 0L);
  }

  /** Fold one batch in through the production path and report what a reader would then see. */
  private static Served fold(final PathStatsAccumulator batch) {
    return fold(List.of(batch));
  }

  /** Fold a sequence of batches into ONE PathNode, exactly as consecutive flushes do. */
  private static Served fold(final List<PathStatsAccumulator> batches) {
    final PathNode node = freshPathNode();
    for (final PathStatsAccumulator batch : batches) {
      PathSummaryWriter.applyDeferredStats(node, batch);
    }
    return new Served(node.getStatsValueCount(), node.getStatsNullCount(), node.getStatsSum(), node.getStatsMin(),
        node.getStatsMax(), node.isStatsSumTrustworthy());
  }

  @Test
  void sameSignOverflowUntrustsBothArms() {
    final Served sequential = fold(batchOf(BIG, BIG, BIG, BIG));
    assertFalse(sequential.sumTrustworthy(), "sequential same-sign overflow must untrust the sum");

    final Served merged = fold(mergeChunks(List.of(new long[] {BIG, BIG}, new long[] {BIG, BIG})));
    assertFalse(merged.sumTrustworthy(), "chunked same-sign overflow must untrust the sum");

    assertEquals(sequential, merged, "the arms disagree on what a reader sees");
  }

  /**
   * The order that breaks a 64-bit running accumulator: document order {@code [M, M, -M]} transiently
   * leaves long range and comes back, while the chunking {@code [M, -M] | [M]} never does. Dropping
   * the overflowing addend would leave the sequential arm untrusted and the merged arm serving
   * {@code M} — same values, different persisted statistics and a different serving decision.
   */
  @Test
  void mixedSignOverflowAgreesAcrossChunkings() {
    final long m = Long.MAX_VALUE;

    final Served sequential = fold(batchOf(m, m, -m));
    final Served merged = fold(mergeChunks(List.of(new long[] {m, -m}, new long[] {m})));

    assertEquals(sequential, merged,
        "the arms disagree — bulk-loaded statistics would diverge from cursor-loaded ones");

    // The true total is exactly M, which IS representable — so both arms must serve it.
    assertTrue(sequential.sumTrustworthy(), "the true total fits a long; nothing may be untrusted");
    assertEquals(m, sequential.sum(), "the exact total is Long.MAX_VALUE");
    assertEquals(3L, sequential.count());
    assertEquals(-m, sequential.min());
    assertEquals(m, sequential.max());
  }

  /**
   * The same agreement where the TRUE total genuinely leaves long range: every chunking must reach
   * the untrusted verdict, whatever the per-chunk partials look like on their own.
   */
  @Test
  void trueOverflowUntrustsEveryChunking() {
    final long m = Long.MAX_VALUE;

    final Served sequential = fold(batchOf(m, m));
    assertFalse(sequential.sumTrustworthy(), "2 * Long.MAX_VALUE does not fit a long");

    final Served merged = fold(mergeChunks(List.of(new long[] {m}, new long[] {m})));
    assertFalse(merged.sumTrustworthy(), "the chunked arm must reach the same verdict");
    assertEquals(sequential, merged);

    // Sign-split chunking of a multiset whose true total is zero stays TRUSTED and exact.
    final Served signSplit =
        fold(mergeChunks(List.of(new long[] {BIG, BIG, BIG, BIG}, new long[] {-BIG, -BIG, -BIG, -BIG})));
    assertTrue(signSplit.sumTrustworthy(), "a representable total must not be untrusted by chunking");
    assertEquals(0L, signSplit.sum(), "the sign-split total is exactly zero");
  }

  /**
   * The same property across FLUSH boundaries rather than chunk boundaries: consecutive batches
   * folded into one PathNode must land where a single batch of the same values lands.
   */
  @Test
  void consecutiveFlushesAgreeWithASingleFlush() {
    final long m = Long.MAX_VALUE;

    final Served oneFlush = fold(batchOf(m, m, -m));
    final Served threeFlushes = fold(List.of(batchOf(m), batchOf(m), batchOf(-m)));

    assertEquals(oneFlush, threeFlushes, "the persisted statistics depend on the flush cadence");
    assertTrue(threeFlushes.sumTrustworthy());
    assertEquals(m, threeFlushes.sum());
  }

  @Test
  void orderInsensitiveLanesMatchAcrossMergeOrders() {
    final long[][] chunks = {{1, 5}, {3}, {2, 4, 6}};
    final Served forward = fold(mergeChunks(List.of(chunks[0], chunks[1], chunks[2])));
    final Served backward = fold(mergeChunks(List.of(chunks[2], chunks[1], chunks[0])));
    assertEquals(forward, backward, "every served lane is order-free");
    assertEquals(21L, forward.sum());
  }

  @Test
  void nanUntrustsButStillCounts() {
    final PathStatsAccumulator acc = new PathStatsAccumulator();
    acc.addDouble(Double.NaN);
    assertFalse(acc.isEmpty(), "a NaN-only partial is NOT empty — the drain gate must keep it");

    final Served served = fold(acc);
    assertFalse(served.sumTrustworthy(), "NaN must untrust the value stats");
    assertEquals(1L, served.count(), "the record still counted — count survives a NaN");
  }

  private static PathStatsAccumulator batchOf(final long... values) {
    final PathStatsAccumulator acc = new PathStatsAccumulator();
    for (final long v : values) {
      acc.addLong(v);
    }
    return acc;
  }

  /** One chunk partial per array, merged into a single batch the way the coordinator drains them. */
  private static PathStatsAccumulator mergeChunks(final List<long[]> chunks) {
    final PathStatsAccumulator target = new PathStatsAccumulator();
    for (final long[] chunk : chunks) {
      target.mergeFrom(batchOf(chunk));
    }
    return target;
  }
}

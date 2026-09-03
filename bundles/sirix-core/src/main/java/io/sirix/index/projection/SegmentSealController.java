/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decides WHEN a segment's dictionary may be sealed: when its last page has been ENCODED, and no
 * later page can still be minted into it.
 *
 * <p>
 * This is the condition {@code docs/SEGMENT_SCOPED_DICTIONARIES.md} names as the pipeline's, not the
 * dictionary's. It cannot be a row count. Record pages are encoded on the async flush pool, so the
 * writer passing a segment's last row says nothing about whether that segment's pages have been
 * encoded — a page adopted in segment N can still be sitting in the flush queue while the writer
 * fills N + 2, and a value minted from it after N was sealed would be lost from the dictionary its
 * own page points at.
 * </p>
 *
 * <p>
 * Nor can sealing simply wait for the commit: the values of every segment would then be resident at
 * once, which at the measured shape is 4.85 GB for one ClickBench column. Sealing has to happen as
 * the load runs, which is exactly why it needs a condition rather than a moment.
 * </p>
 *
 * <h2>The condition</h2>
 *
 * A segment is sealable when both hold:
 * <ol>
 * <li><b>nothing outstanding</b> — every page adopted into it has since been encoded, and</li>
 * <li><b>nothing to come</b> — a page of a LATER segment has already been adopted.</li>
 * </ol>
 *
 * The second clause is what makes the first safe. Record pages are adopted in ascending key order
 * during a bulk load, so a later segment's page proves the writer has moved on; without it, a segment
 * that momentarily has no page in flight — between two adoptions — would be sealed while still
 * growing. Anything the load never sealed is caught by {@link #drain()} at commit, so the rule may be
 * conservative but must never be eager.
 *
 * <p>
 * A resource that inserts out of key order (not a bulk load) simply seals fewer segments early and
 * more at commit, which costs residency, never correctness.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentSealController {

  /** Segment to pages adopted but not yet encoded. */
  private final ConcurrentHashMap<Long, AtomicInteger> outstanding = new ConcurrentHashMap<>();

  /** Segments that have been handed out for sealing, so none is sealed twice. */
  private final ConcurrentHashMap<Long, Boolean> sealed = new ConcurrentHashMap<>();

  /** The highest segment any page has been adopted into. */
  private final AtomicLong highWaterMark = new AtomicLong(-1L);

  /**
   * A page was created for {@code segment} and will be encoded later. Called where the page's
   * resolver is installed — on the writer side, in key order.
   */
  public void adopted(final long segment) {
    requireNonNegative(segment);
    if (sealed.containsKey(segment)) {
      throw new IllegalStateException("segment " + segment + " was sealed and cannot adopt another page");
    }
    outstanding.computeIfAbsent(segment, ignored -> new AtomicInteger()).incrementAndGet();
    highWaterMark.accumulateAndGet(segment, Math::max);
  }

  /**
   * A page of {@code segment} has finished encoding — call after the flush window carrying it has
   * been joined, which is the point at which its values are certainly minted.
   */
  public void encoded(final long segment) {
    requireNonNegative(segment);
    final AtomicInteger pending = outstanding.get(segment);
    if (pending == null || pending.get() <= 0) {
      throw new IllegalStateException("segment " + segment + " has no page outstanding to complete");
    }
    pending.decrementAndGet();
  }

  /** Pages adopted into {@code segment} that have not been encoded yet (test observability). */
  public int outstandingIn(final long segment) {
    final AtomicInteger pending = outstanding.get(segment);
    return pending == null
        ? 0
        : pending.get();
  }

  /**
   * Segments that may be sealed NOW, marked as sealed by this call so a later one does not offer them
   * again. Both clauses of the condition are applied here; a caller that wants everything regardless
   * calls {@link #drain()}.
   */
  public List<Long> takeSealable() {
    final long mark = highWaterMark.get();
    final List<Long> ready = new ArrayList<>();
    for (final var entry : outstanding.entrySet()) {
      final long segment = entry.getKey();
      if (segment < mark && entry.getValue().get() == 0 && sealed.putIfAbsent(segment, Boolean.TRUE) == null) {
        ready.add(segment);
      }
    }
    return ready;
  }

  /**
   * Every segment not yet sealed, marked sealed — the commit-time sweep. After this the controller
   * holds no unsealed segment, and a page adopted afterwards is a defect rather than a late arrival.
   *
   * @throws IllegalStateException if any page is still outstanding: sealing then would drop the
   *         values that page is about to mint, and the caller must fence the flush pool first
   */
  public List<Long> drain() {
    final List<Long> ready = new ArrayList<>();
    for (final var entry : outstanding.entrySet()) {
      final long segment = entry.getKey();
      final int pending = entry.getValue().get();
      if (pending != 0) {
        throw new IllegalStateException("segment " + segment + " still has " + pending
            + " page(s) encoding; fence the flush pool before draining");
      }
      if (sealed.putIfAbsent(segment, Boolean.TRUE) == null) {
        ready.add(segment);
      }
    }
    return ready;
  }

  /** Whether {@code segment} has already been handed out for sealing. */
  public boolean isSealed(final long segment) {
    return sealed.containsKey(segment);
  }

  /** Segments handed out for sealing so far (test observability). */
  public int sealedCount() {
    return sealed.size();
  }

  private static void requireNonNegative(final long segment) {
    if (segment < 0) {
      throw new IllegalArgumentException("segment must not be negative: " + segment);
    }
  }
}

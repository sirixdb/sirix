/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Arrays;

/**
 * Decides WHEN a segment's dictionary may be sealed: once every page adopted into the segment has
 * been encoded, and the segment is no longer the one being filled.
 *
 * <h2>Why a set of page keys, not a counter</h2>
 *
 * The writer tells this controller two things per page — {@code adopted(segment, pageKey)} when a
 * page is given its segment view, and {@code encoded(segment, pageKey)} each time the page's bytes
 * are produced. Neither arrives exactly once per page: a page written in two flush epochs is encoded
 * twice; a copy-on-write copy re-adopted through the same factory is adopted twice; and the two can
 * interleave arbitrarily because the flush pool runs concurrently with adoption. A counter that
 * increments on adopt and decrements on encode reads every duplicate as a real event and either
 * seals a segment early (an extra encode) or never (an extra adopt). A SET of outstanding page keys
 * makes both notifications idempotent: a page is outstanding while its key is in the set, whatever
 * the number of times either side spoke.
 *
 * <p>
 * A segment's set holds only the pages the flush pool has not caught up with, and it is dropped the
 * moment the segment is offered. Its worst case is a segment whose every page is still in flight —
 * {@link SegmentBoundaries#SEGMENT_MAX_LEAVES} keys, about a megabyte of longs against that
 * segment's 64 MiB of values.
 * </p>
 *
 * <h2>The high-water segment</h2>
 *
 * The segment currently adopting pages is never offered for sealing even when it happens to have no
 * outstanding page: its next adoption would land a page in a sealed dictionary. It is held back
 * until a HIGHER segment adopts a page (the boundaries never go back), or until the load ends and
 * {@link #drain} sweeps the tail.
 *
 * <h2>Threading</h2>
 *
 * Adoption is the writer thread; encoding is the flush pool; sealing is whichever thread runs the
 * commit. Every method synchronises on this instance. The critical sections are a hash-set update
 * each, taken once per page rather than per value, so the lock is nowhere near the hot path.
 */
public final class SegmentSealController {

  private static final LongOpenHashSet[] NO_SEGMENTS = new LongOpenHashSet[0];
  private static final boolean[] NO_FLAGS = new boolean[0];

  /** Outstanding page keys per segment at index {@code segment}; {@code null} for a segment never adopted into. */
  private LongOpenHashSet[] outstanding = NO_SEGMENTS;
  /** Whether the segment at index {@code segment} has been offered by a take-method. */
  private boolean[] sealed = NO_FLAGS;
  /** Highest segment that has adopted a page; {@code -1} before the first. */
  private int highWaterMark = -1;

  /**
   * A page was given its segment view. Idempotent: re-adopting an outstanding page (a copy-on-write
   * copy through the same factory) changes nothing.
   *
   * @throws IllegalStateException if the segment was already offered for sealing — its dictionary
   *         may be persisted and released, and a page encoded against it would mint into nothing
   */
  public synchronized void adopted(final int segment, final long pageKey) {
    requireNonNegative(segment, "segment");
    requireNonNegative(pageKey, "pageKey");
    if (segment < sealed.length && sealed[segment]) {
      throw new IllegalStateException("page " + pageKey + " adopted into segment " + segment
          + ", which was already sealed");
    }
    ensureCapacity(segment);
    LongOpenHashSet pages = outstanding[segment];
    if (pages == null) {
      pages = new LongOpenHashSet();
      outstanding[segment] = pages;
    }
    pages.add(pageKey);
    if (segment > highWaterMark) {
      highWaterMark = segment;
    }
  }

  /**
   * A page's bytes were produced. Idempotent: a second encode of the same page (a later flush epoch)
   * finds nothing outstanding and changes nothing.
   *
   * @return whether the page was outstanding until now
   */
  public synchronized boolean encoded(final int segment, final long pageKey) {
    requireNonNegative(segment, "segment");
    requireNonNegative(pageKey, "pageKey");
    if (segment >= outstanding.length) {
      return false;
    }
    final LongOpenHashSet pages = outstanding[segment];
    return pages != null && pages.remove(pageKey);
  }

  /** Pages adopted into {@code segment} whose bytes have not been produced since. */
  public synchronized int outstandingIn(final int segment) {
    requireNonNegative(segment, "segment");
    if (segment >= outstanding.length) {
      return 0;
    }
    final LongOpenHashSet pages = outstanding[segment];
    return pages == null
        ? 0
        : pages.size();
  }

  /**
   * Segments that may be sealed now: adopted into, nothing outstanding, below the high-water mark, and
   * not offered before. Each segment is offered exactly once; the caller owns its seal from then on.
   */
  public synchronized IntList takeSealable() {
    return takeSealable(0);
  }

  /**
   * Sealable segments, keeping {@code slack} more of them live below the high-water mark.
   *
   * <p>
   * The slack exists for one race, and it is cheap insurance against it. The document writer decides
   * a segment is finished by ADOPTING a page into a higher one; a consumer that derives from the same
   * row stream — the projection, whose leaf is cut the moment a row's segment differs — mints into
   * the old segment a moment LATER, when that leaf flushes. Sealing at the high-water mark alone
   * would let a commit land in between and refuse a mint that was always going to arrive. One
   * segment of slack costs one segment's values in memory and removes the window entirely.
   * </p>
   */
  public synchronized IntList takeSealable(final int slack) {
    if (slack < 0) {
      throw new IllegalArgumentException("slack must not be negative: " + slack);
    }
    return take(highWaterMark - 1 - slack);
  }

  /**
   * At the end of a load, when no page can be adopted any more: every unsealed segment with nothing
   * outstanding, the high-water segment included.
   */
  public synchronized IntList drain() {
    return take(highWaterMark);
  }

  /**
   * After the caller has fenced the flush pool — every adopted page has been encoded — every unsealed
   * segment. A segment with a page still outstanding here is a contract violation: a page the pool
   * never encoded would be written without its dictionary being sealed against it.
   */
  public synchronized IntList drainAfterFence() {
    for (int segment = 0; segment <= highWaterMark; segment++) {
      final LongOpenHashSet pages = outstanding[segment];
      if (pages != null && !pages.isEmpty()) {
        throw new IllegalStateException("segment " + segment + " has " + pages.size()
            + " page(s) adopted but never encoded after the flush fence");
      }
    }
    return take(highWaterMark);
  }

  /** Whether {@code segment} has been offered for sealing. */
  public synchronized boolean isSealed(final int segment) {
    requireNonNegative(segment, "segment");
    return segment < sealed.length && sealed[segment];
  }

  /** Highest segment that has adopted a page; {@code -1} before the first. */
  public synchronized int highWaterMark() {
    return highWaterMark;
  }

  /** Segments offered so far. */
  public synchronized int sealedCount() {
    int count = 0;
    for (final boolean flag : sealed) {
      if (flag) {
        count++;
      }
    }
    return count;
  }

  private IntList take(final int highestCandidate) {
    if (highestCandidate < 0) {
      return IntLists.EMPTY_LIST;
    }
    IntArrayList sealable = null;
    for (int segment = 0; segment <= highestCandidate; segment++) {
      final LongOpenHashSet pages = outstanding[segment];
      if (pages == null || sealed[segment] || !pages.isEmpty()) {
        continue;
      }
      sealed[segment] = true;
      outstanding[segment] = null; // the set is spent; a re-adoption is refused by the flag
      if (sealable == null) {
        sealable = new IntArrayList();
      }
      sealable.add(segment);
    }
    return sealable == null
        ? IntLists.EMPTY_LIST
        : sealable;
  }

  private void ensureCapacity(final int segment) {
    if (segment < outstanding.length) {
      return;
    }
    final int capacity = Math.max(segment + 1, Math.max(4, outstanding.length << 1));
    outstanding = Arrays.copyOf(outstanding, capacity);
    sealed = Arrays.copyOf(sealed, capacity);
  }

  private static void requireNonNegative(final long value, final String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative: " + value);
    }
  }
}

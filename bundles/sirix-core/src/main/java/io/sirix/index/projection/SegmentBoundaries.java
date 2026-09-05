/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentDictionaryDirectoryNode;

import java.util.Arrays;

/**
 * Where one segment of the document trie ends and the next begins — decided ONCE, on the adopting
 * thread, and never revised for a page that has been adopted.
 *
 * <p>
 * A segment is a page-aligned node-key range {@code [start(s), start(s + 1))} of the document trie (
 * {@code docs/SEGMENT_DICTIONARY_DESIGN.md} §1). Node keys are monotone and never reused, so a
 * node's segment is permanent. The boundaries are the first page key of every segment, strictly
 * ascending from 0; the last one is the OPEN segment, which every page above it belongs to until it
 * closes. The array is what {@link SegmentDictionaryDirectoryNode} persists, so this class produces
 * exactly that node's contract.
 * </p>
 *
 * <h2>When a segment closes</h2>
 *
 * At the next page adoption once the open segment has minted
 * {@link #SEGMENT_DICTIONARY_BUDGET_BYTES} of distinct-value bytes (summed over its tags) or its page
 * keys span {@link #SEGMENT_MAX_LEAVES}. The bytes are what the seal has to sort and store, so
 * closing on them makes every segment dictionary cost about the same to build and to serve whatever
 * the column mix; the span cap bounds the mint map's lifetime for a corpus of few distinct values.
 * Two constants, no properties: the budget is the seal's working set, not a tuning knob.
 *
 * <p>
 * The cap is on the KEY SPAN — the distance from the segment's first adopted page key to the page
 * being adopted — not on a count of adopted leaves. The two agree for a bulk load, whose document
 * page keys are dense; where keys have gaps the segment holds fewer leaves than the cap, never more,
 * and the directory's contract (page-aligned key ranges) is what the span protects.
 * </p>
 *
 * <h2>Why the decision is taken at adoption, and only for a page ABOVE every adopted one</h2>
 *
 * Pages are adopted on the single-threaded writer side in key order, but their values are minted
 * later, on the flush pool. So the byte count a close reacts to lags the adoption by the flush
 * window, and a boundary can only be placed where no already-adopted page could fall on the other
 * side of it: a boundary is appended at page key {@code k} only when {@code k} exceeds every page
 * adopted so far. Then every adopted page keeps its segment for the rest of the load, which is
 * what lets {@link #segmentOf} answer from any thread without a lock — an adopted page's answer is
 * fixed before anyone else can ask for it. A page arriving BELOW the highest adopted key (not a
 * bulk load) is simply placed by the boundaries as they stand; the close is deferred to the next
 * page that is above everything.
 *
 * <p>
 * Adoption is SEQUENTIAL: one adopter at a time, and a hand-over between adopting threads (the bulk
 * importer's coordinator adopts built leaves, the transaction's own thread creates fresh pages)
 * ordered by the caller. The adoption cursors are volatile so an ordered hand-over sees them, and
 * nothing more, because concurrent adoption would mean two writers on one transaction, which every
 * other structure of the writer already forbids. {@link #segmentOf}, {@link #segmentCount} and
 * {@link #starts} may be called from any thread. The start array is replaced, never mutated, so a
 * reader sees one consistent snapshot.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentBoundaries {

  /** Distinct-value bytes minted into a segment, summed over its tags, at which it closes. */
  public static final long SEGMENT_DICTIONARY_BUDGET_BYTES = 64L << 20;

  /** Document leaves a segment may span before it closes regardless of its bytes. */
  public static final long SEGMENT_MAX_LEAVES = 1L << 17;

  private final long budgetBytes;
  private final long maxLeaves;
  private final int maxSegments;

  /** First page key of every segment, strictly ascending from 0. Replaced on close, never mutated. */
  private volatile long[] starts = { 0L };

  /** Highest page key adopted so far, {@code -1} before the first. Adopter only; see the class note. */
  private volatile long highestAdopted = -1L;

  /** First page key adopted into the open segment, {@code -1} while it has none. Adopter only. */
  private volatile long firstAdoptedInOpen = -1L;

  /** Boundaries closing at the production budget and span cap. */
  public SegmentBoundaries() {
    this(SEGMENT_DICTIONARY_BUDGET_BYTES, SEGMENT_MAX_LEAVES);
  }

  /**
   * Boundaries closing at an explicit budget and span cap — for tests that need several segments out
   * of a few pages. Production uses the constants through {@link #SegmentBoundaries()}.
   *
   * @param budgetBytes distinct-value bytes at which the open segment closes, positive
   * @param maxLeaves page keys the open segment may span, positive
   */
  SegmentBoundaries(final long budgetBytes, final long maxLeaves) {
    this(budgetBytes, maxLeaves, SegmentDictionaryDirectoryNode.MAX_SEGMENTS);
  }

  /**
   * Boundaries with an explicit segment cap as well — for tests that drive the directory's limit
   * without opening sixteen million segments.
   *
   * @param maxSegments segments the directory may hold, positive and at most
   *        {@link SegmentDictionaryDirectoryNode#MAX_SEGMENTS}
   */
  SegmentBoundaries(final long budgetBytes, final long maxLeaves, final int maxSegments) {
    if (budgetBytes <= 0L) {
      throw new IllegalArgumentException("budgetBytes must be positive: " + budgetBytes);
    }
    if (maxLeaves <= 0L) {
      throw new IllegalArgumentException("maxLeaves must be positive: " + maxLeaves);
    }
    if (maxSegments <= 0 || maxSegments > SegmentDictionaryDirectoryNode.MAX_SEGMENTS) {
      throw new IllegalArgumentException("maxSegments must be in [1, " + SegmentDictionaryDirectoryNode.MAX_SEGMENTS
          + "]: " + maxSegments);
    }
    this.budgetBytes = budgetBytes;
    this.maxLeaves = maxLeaves;
    this.maxSegments = maxSegments;
  }

  /**
   * Place document page {@code recordPageKey} in a segment, closing the open one first if it is
   * full. One adopter at a time, once per adoption, before the page's resolver is chosen.
   *
   * @param recordPageKey the page's key, at least 0
   * @param openSegmentMintedBytes distinct-value bytes minted into the open segment so far, summed
   *        over its tags; the count the flush pool has reached, not the count it will reach
   * @return the segment the page belongs to
   */
  public int adopt(final long recordPageKey, final long openSegmentMintedBytes) {
    if (recordPageKey < 0L) {
      throw new IllegalArgumentException("recordPageKey must not be negative: " + recordPageKey);
    }
    if (openSegmentMintedBytes < 0L) {
      throw new IllegalArgumentException("openSegmentMintedBytes must not be negative: " + openSegmentMintedBytes);
    }
    if (recordPageKey <= highestAdopted) {
      // Below (or at) a page already adopted: its segment is whatever the boundaries say, and no
      // boundary may be placed here — one already-adopted page could fall on the far side of it.
      return segmentOf(recordPageKey);
    }
    if (firstAdoptedInOpen < 0L) {
      firstAdoptedInOpen = recordPageKey; // the open segment's first page; nothing to close yet
    } else if (openSegmentMintedBytes >= budgetBytes || recordPageKey - firstAdoptedInOpen >= maxLeaves) {
      close(recordPageKey);
      firstAdoptedInOpen = recordPageKey;
    }
    highestAdopted = recordPageKey;
    return starts.length - 1;
  }

  /** Append a boundary at {@code start}: the open segment ends there and a new one begins. */
  private void close(final long start) {
    final long[] current = starts;
    final int count = current.length;
    if (start <= current[count - 1]) {
      throw new IllegalStateException("a boundary at page " + start + " would not ascend past the open segment's start "
          + current[count - 1]);
    }
    if (count >= maxSegments) {
      throw new IllegalStateException("the segment directory holds at most " + maxSegments + " segments; page "
          + start + " would open one more");
    }
    final long[] grown = Arrays.copyOf(current, count + 1);
    grown[count] = start;
    starts = grown; // one volatile write publishes the whole new snapshot
  }

  /**
   * The segment holding document page {@code recordPageKey}: the last segment whose start does not
   * exceed it. Any thread; for a page that has been adopted the answer never changes.
   */
  public int segmentOf(final long recordPageKey) {
    if (recordPageKey < 0L) {
      throw new IllegalArgumentException("recordPageKey must not be negative: " + recordPageKey);
    }
    final long[] snapshot = starts;
    int low = 0;
    int high = snapshot.length - 1;
    while (low < high) {
      final int mid = (low + high + 1) >>> 1;
      if (snapshot[mid] <= recordPageKey) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  /** How many segments exist, the open one included. */
  public int segmentCount() {
    return starts.length;
  }

  /** The open segment: the one every page above the last boundary belongs to. */
  public int openSegment() {
    return starts.length - 1;
  }

  /** First page key of {@code segment}. */
  public long segmentStart(final int segment) {
    final long[] snapshot = starts;
    if (segment < 0 || segment >= snapshot.length) {
      throw new IllegalArgumentException("no segment " + segment + " among " + snapshot.length);
    }
    return snapshot[segment];
  }

  /**
   * The first page key of every segment, a copy in {@link SegmentDictionaryDirectoryNode}'s contract:
   * strictly ascending from 0.
   */
  public long[] starts() {
    final long[] snapshot = starts;
    return Arrays.copyOf(snapshot, snapshot.length);
  }

  /** Highest page key adopted so far, {@code -1} before the first (test observability). */
  long highestAdoptedPageKey() {
    return highestAdopted;
  }

  @Override
  public String toString() {
    return "SegmentBoundaries[segments=" + starts.length + ", highestAdopted=" + highestAdopted + ", budgetBytes="
        + budgetBytes + ", maxLeaves=" + maxLeaves + "]";
  }
}

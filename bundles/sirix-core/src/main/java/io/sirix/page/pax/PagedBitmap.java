package io.sirix.page.pax;

import java.util.Arrays;

/**
 * Fixed-width bitmap with an O(1) rank-1 query.
 *
 * <p>Backs PAX region addressing: a slot's position within a typed region is
 * {@code rank(slot)} in the region's occupancy bitmap (rank-1 = count of set bits
 * strictly before {@code slot}). With a precomputed per-word cumulative popcount
 * summary, rank is a single array read + one masked {@link Long#bitCount}.
 *
 * <h2>HFT-grade guarantees</h2>
 * <ul>
 *   <li>Storage is a fixed-size {@code long[]} ({@code slotCount / 64} words)
 *       plus a {@code int[]} summary of size {@code words + 1}. Both arrays are
 *       allocated once at construction. Never resized.</li>
 *   <li>{@link #set(int)}, {@link #clear(int)}, {@link #get(int)}, {@link #rank(int)},
 *       {@link #select(int)}, {@link #nextSetBit(int)}, {@link #popcount()} are
 *       branch-light, allocation-free, and do no boxing.</li>
 *   <li>Mutation invalidates the rank summary; call {@link #seal()} exactly once
 *       after populating the bitmap before issuing rank/select queries. Asserts
 *       enforce this in debug builds; production builds skip the checks.</li>
 *   <li>{@link #words()} exposes the raw word array for SIMD iteration by callers
 *       that want to batch-popcount via {@link jdk.incubator.vector.LongVector}.</li>
 * </ul>
 *
 * <h2>Addressing convention</h2>
 * <ul>
 *   <li>{@code rank(i)} = popcount of bits in slot range {@code [0, i)}.
 *       Range-1 is strictly less, so {@code rank(0) = 0} and
 *       {@code rank(slotCount) = popcount()}.</li>
 *   <li>{@code select(k)} returns the slot index of the {@code (k+1)}-th set bit
 *       (0-indexed), or {@code -1} when {@code k >= popcount()}.</li>
 * </ul>
 */
public final class PagedBitmap {

  private static final int WORD_BITS = 64;
  private static final int WORD_SHIFT = 6;
  private static final long WORD_MASK = 63L;

  private final int slotCount;
  private final int wordCount;
  private final long[] words;

  /**
   * Cumulative popcount summary — {@code summary[w]} is the count of set bits
   * in {@code words[0..w)}. Length = {@code wordCount + 1}, so
   * {@code summary[wordCount] == popcount()}. Populated by {@link #seal()}.
   */
  private final int[] summary;

  private boolean sealed;

  public PagedBitmap(final int slotCount) {
    if (slotCount <= 0 || (slotCount & WORD_MASK) != 0) {
      throw new IllegalArgumentException("slotCount must be positive and a multiple of 64, got " + slotCount);
    }
    this.slotCount = slotCount;
    this.wordCount = slotCount >>> WORD_SHIFT;
    this.words = new long[wordCount];
    this.summary = new int[wordCount + 1];
  }

  public int slotCount() {
    return slotCount;
  }

  public int wordCount() {
    return wordCount;
  }

  public long[] words() {
    return words;
  }

  /** Set bit {@code slot}. Invalidates the sealed state. */
  public void set(final int slot) {
    words[slot >>> WORD_SHIFT] |= 1L << (slot & WORD_MASK);
    sealed = false;
  }

  /** Clear bit {@code slot}. Invalidates the sealed state. */
  public void clear(final int slot) {
    words[slot >>> WORD_SHIFT] &= ~(1L << (slot & WORD_MASK));
    sealed = false;
  }

  /** Returns {@code true} iff bit {@code slot} is set. */
  public boolean get(final int slot) {
    return (words[slot >>> WORD_SHIFT] & (1L << (slot & WORD_MASK))) != 0L;
  }

  /** Clears every bit. Invalidates the sealed state. */
  public void reset() {
    Arrays.fill(words, 0L);
    sealed = false;
  }

  /**
   * Compute the cumulative popcount summary. Call this after all {@link #set}/{@link #clear}
   * mutations have been applied and before any {@link #rank}/{@link #select}/{@link #popcount}
   * queries. O({@link #wordCount}).
   */
  public void seal() {
    int running = 0;
    for (int w = 0; w < wordCount; w++) {
      summary[w] = running;
      running += Long.bitCount(words[w]);
    }
    summary[wordCount] = running;
    sealed = true;
  }

  /**
   * Rank-1: number of set bits in {@code [0, slot)}. O(1). Requires {@link #seal()}.
   *
   * @param slot a slot index in {@code [0, slotCount]} (inclusive upper bound
   *             allowed so {@code rank(slotCount)} yields the total population).
   */
  public int rank(final int slot) {
    assert sealed : "PagedBitmap.rank called before seal()";
    final int w = slot >>> WORD_SHIFT;
    if (w >= wordCount) {
      return summary[wordCount];
    }
    final long mask = (1L << (slot & WORD_MASK)) - 1L;
    return summary[w] + Long.bitCount(words[w] & mask);
  }

  /**
   * Select: slot index of the {@code (k+1)}-th set bit (0-indexed),
   * or {@code -1} if {@code k >= popcount()}. Requires {@link #seal()}.
   *
   * <p>Uses a linear scan over the summary (fast for small {@link #wordCount};
   * the KVLP bitmap has only 16 words). For larger bitmaps a binary search would
   * be preferable.
   */
  public int select(final int k) {
    assert sealed : "PagedBitmap.select called before seal()";
    if (k < 0 || k >= summary[wordCount]) {
      return -1;
    }
    int w = 0;
    while (w < wordCount && summary[w + 1] <= k) {
      w++;
    }
    final int within = k - summary[w];
    long word = words[w];
    for (int i = 0; i < within; i++) {
      word &= word - 1L;
    }
    return (w << WORD_SHIFT) | Long.numberOfTrailingZeros(word);
  }

  /** Total number of set bits. Requires {@link #seal()}. */
  public int popcount() {
    assert sealed : "PagedBitmap.popcount called before seal()";
    return summary[wordCount];
  }

  /**
   * Next set bit at or after {@code fromSlot}, or {@code -1} if none. Does not
   * require sealing. Branch-light; HFT-safe for forward iteration.
   */
  public int nextSetBit(final int fromSlot) {
    if (fromSlot >= slotCount) {
      return -1;
    }
    int w = fromSlot >>> WORD_SHIFT;
    long word = words[w] & (~0L << (fromSlot & WORD_MASK));
    while (true) {
      if (word != 0L) {
        return (w << WORD_SHIFT) | Long.numberOfTrailingZeros(word);
      }
      if (++w >= wordCount) {
        return -1;
      }
      word = words[w];
    }
  }

  /** Returns whether the bitmap has been sealed since the last mutation. */
  public boolean isSealed() {
    return sealed;
  }
}

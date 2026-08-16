package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;

/**
 * Flat open-addressed hash table for per-group aggregates keyed by a single {@code long} group
 * value: {@code keys[i]} pairs with an inline accumulator block at {@code slots[i * slotWidth]},
 * layout identical to {@link ProjectionIndexByteScan#newGroupAggAcc} — {@code [count, firstSeen,
 * then per aggregate column: presentCount, sum, min, max]}.
 *
 * <p>
 * This replaces {@code Long2ObjectOpenHashMap<long[]>} in the high-cardinality group-by kernel: no
 * boxed accumulator per group, no per-group allocation, and the accumulators of colliding probes
 * sit in the same cache lines as their keys' neighborhood. Key {@code 0} is the empty-bucket
 * sentinel; the real group value {@code 0} lives in a dedicated side slot.
 *
 * <p>
 * NOT thread-safe by design: the kernel builds one table per worker and merges by hash partition,
 * so no table is ever written from two threads.
 */
public final class NumericGroupAggTable {

  /** Grow when {@code size} exceeds {@code capacity * 3 / 4}. */
  private static final int MAX_CAPACITY = 1 << 30;

  private final int slotWidth;
  private final int aggColumns;
  private long[] keys;
  private long[] slots;
  /** One source reference per bucket ({@code null} unless {@code withAux}) — the string kernel
   * stores {@code (leaf << 20) | dictId} of a group's first sighting so only WINNING groups ever
   * materialize their string. */
  private long[] aux;
  private int mask;
  private int size;
  private int growAt;
  private boolean hasZeroKey;
  private final long[] zeroSlot;
  private long zeroAux;

  /**
   * @param aggColumns aggregate columns per group ({@code slotWidth = 2 + 4 * aggColumns})
   * @param expectedEntries sizing hint; the table grows past it without limit
   */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries) {
    this(aggColumns, expectedEntries, false);
  }

  /** @param withAux carry a per-entry source-reference lane (see {@link #auxAtBase}) */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux) {
    if (aggColumns < 0) {
      throw new IllegalArgumentException("aggColumns must be >= 0");
    }
    this.aggColumns = aggColumns;
    this.slotWidth = 2 + 4 * aggColumns;
    int cap = (int) (Long.highestOneBit(Math.max(16, Math.min(MAX_CAPACITY, (long) expectedEntries * 4 / 3)) - 1) << 1);
    if (cap < 16) {
      cap = 16;
    }
    this.keys = new long[cap];
    this.slots = new long[cap * slotWidth];
    this.aux = withAux
        ? new long[cap]
        : null;
    this.mask = cap - 1;
    this.growAt = cap - (cap >>> 2);
    this.zeroSlot = newAcc(slotWidth);
  }

  private static long[] newAcc(final int slotWidth) {
    final long[] acc = new long[slotWidth];
    acc[1] = Long.MAX_VALUE;
    for (int base = 2; base < slotWidth; base += 4) {
      acc[base + 2] = Long.MAX_VALUE;
      acc[base + 3] = Long.MIN_VALUE;
    }
    return acc;
  }

  /** Aggregate columns this table's accumulator blocks carry. */
  public int aggColumns() {
    return aggColumns;
  }

  /** Longs per accumulator block. */
  public int slotWidth() {
    return slotWidth;
  }

  /** The inline accumulator storage; entry {@code i}'s block starts at {@code i * slotWidth()}. */
  public long[] slotsArray() {
    return slots;
  }

  /** Bucket keys; {@code 0} = empty bucket (the real key 0 lives in {@link #zeroSlot()}). */
  public long[] keysArray() {
    return keys;
  }

  /** Bucket count (a power of two). */
  public int capacity() {
    return mask + 1;
  }

  /** Distinct non-zero keys inserted. */
  public int size() {
    return size;
  }

  /** Whether the real group value {@code 0} was seen. */
  public boolean hasZeroKey() {
    return hasZeroKey;
  }

  /** The accumulator block for group value {@code 0} — valid only when {@link #hasZeroKey()}. */
  public long[] zeroSlot() {
    return zeroSlot;
  }

  /** Distinct groups including the zero key. */
  public int sizeIncludingZero() {
    return size + (hasZeroKey
        ? 1
        : 0);
  }

  /** The bucket key owning the accumulator block at {@code slotBase} — lets a caller VALIDATE a
   * cached slot base across rehashes: a base is current iff its bucket still holds the same key
   * (keys are unique, so a match can never be a different group). */
  public long keyAtSlotBase(final int slotBase) {
    return keys[slotBase / slotWidth];
  }

  /** Source reference of the entry at {@code slotBase} (aux lane only). */
  public long auxAtBase(final int slotBase) {
    return aux[slotBase / slotWidth];
  }

  /** Stamp the source reference of the entry at {@code slotBase} (aux lane only). */
  public void setAuxAtBase(final int slotBase, final long value) {
    aux[slotBase / slotWidth] = value;
  }

  /** Source reference of the zero-key group (aux lane only). */
  public long zeroAux() {
    return zeroAux;
  }

  /** Stamp the zero-key group's source reference (aux lane only). */
  public void setZeroAux(final long value) {
    zeroAux = value;
  }

  /**
   * Slot-array base offset of {@code key}'s accumulator block, inserting a fresh block
   * ({@code count 0, firstSeen ordinal, empty aggregates}) on first sight. {@code key} MUST be
   * non-zero — route the zero group through {@link #acquireZero}.
   */
  public int acquire(final long key, final long firstSeenOrdinal) {
    final long[] k = keys;
    final int m = mask;
    int pos = (int) HashCommon.mix(key) & m;
    long cur = k[pos];
    while (cur != 0L) {
      if (cur == key) {
        return pos * slotWidth;
      }
      pos = pos + 1 & m;
      cur = k[pos];
    }
    k[pos] = key;
    final int base = pos * slotWidth;
    initBlock(slots, base, firstSeenOrdinal, slotWidth);
    if (++size > growAt) {
      rehash();
      // The block moved with its key; re-probe in the grown table (guaranteed present).
      return find(key);
    }
    return base;
  }

  /** The zero group's accumulator block, stamping its first-seen ordinal on first sight. */
  public long[] acquireZero(final long firstSeenOrdinal) {
    if (!hasZeroKey) {
      hasZeroKey = true;
      zeroSlot[1] = firstSeenOrdinal;
    }
    return zeroSlot;
  }

  /** Slot base of an EXISTING non-zero key. */
  private int find(final long key) {
    final long[] k = keys;
    final int m = mask;
    int pos = (int) HashCommon.mix(key) & m;
    while (k[pos] != key) {
      pos = pos + 1 & m;
    }
    return pos * slotWidth;
  }

  private static void initBlock(final long[] slots, final int base, final long firstSeenOrdinal, final int slotWidth) {
    slots[base] = 0L;
    slots[base + 1] = firstSeenOrdinal;
    for (int b = base + 2; b < base + slotWidth; b += 4) {
      slots[b] = 0L;
      slots[b + 1] = 0L;
      slots[b + 2] = Long.MAX_VALUE;
      slots[b + 3] = Long.MIN_VALUE;
    }
  }

  private void rehash() {
    final int oldCap = mask + 1;
    if (oldCap >= MAX_CAPACITY) {
      throw new IllegalStateException("group table exceeds " + MAX_CAPACITY + " buckets");
    }
    final int newCap = oldCap << 1;
    final int newMask = newCap - 1;
    final long[] oldKeys = keys;
    final long[] oldSlots = slots;
    final long[] oldAux = aux;
    final long[] newKeys = new long[newCap];
    final long[] newSlots = new long[newCap * slotWidth];
    final long[] newAux = oldAux != null
        ? new long[newCap]
        : null;
    for (int i = 0; i < oldCap; i++) {
      final long key = oldKeys[i];
      if (key == 0L) {
        continue;
      }
      int pos = (int) HashCommon.mix(key) & newMask;
      while (newKeys[pos] != 0L) {
        pos = pos + 1 & newMask;
      }
      newKeys[pos] = key;
      System.arraycopy(oldSlots, i * slotWidth, newSlots, pos * slotWidth, slotWidth);
      if (newAux != null) {
        newAux[pos] = oldAux[i];
      }
    }
    keys = newKeys;
    slots = newSlots;
    aux = newAux;
    mask = newMask;
    growAt = newCap - (newCap >>> 2);
  }

  /**
   * Fold every entry of {@code sources} whose key hashes into {@code partition} into {@code into}
   * — the partition-parallel replacement for the serial whole-map merge: each partition is owned
   * by exactly one worker, so the merge needs no locks and no thread ever folds the full group
   * count. Partition of a key is the TOP {@code 64 - shift} bits of the same mix the buckets use
   * ({@code shift == 64} means a single partition taking everything). The zero group belongs to
   * partition 0.
   *
   * <p>
   * Sum lanes merge with {@link Math#addExact} — the same interpreter-promotes-on-overflow
   * discipline the row fold enforces; the caller treats the {@link ArithmeticException} as a
   * decline.
   */
  public static void mergePartition(final NumericGroupAggTable[] sources, final int partition, final int shift,
      final NumericGroupAggTable into) {
    final int slotWidth = into.slotWidth;
    for (final NumericGroupAggTable src : sources) {
      if (src == null) {
        continue;
      }
      final long[] keys = src.keys;
      final long[] slots = src.slots;
      final long[] srcAux = src.aux;
      for (int i = 0; i < keys.length; i++) {
        final long key = keys[i];
        if (key == 0L) {
          continue;
        }
        if (shift < 64 && (int) (HashCommon.mix(key) >>> shift) != partition) {
          continue;
        }
        final int srcBase = i * slotWidth;
        final int dstBase = into.acquire(key, slots[srcBase + 1]);
        if (srcAux != null && into.slots[dstBase] == 0L) {
          // Fresh in the destination: carry the source reference (any sighting's bytes are the
          // same group value, so first-arrival is as good as first-seen).
          into.setAuxAtBase(dstBase, srcAux[i]);
        }
        mergeBlock(into.slots, dstBase, slots, srcBase, slotWidth);
      }
      if (partition == 0 && src.hasZeroKey) {
        final boolean fresh = !into.hasZeroKey;
        final long[] dst = into.acquireZero(src.zeroSlot[1]);
        if (fresh && src.aux != null) {
          into.zeroAux = src.zeroAux;
        }
        mergeBlockIntoAcc(dst, src.zeroSlot, slotWidth);
      }
    }
  }

  private static void mergeBlock(final long[] dst, final int dstBase, final long[] src, final int srcBase,
      final int slotWidth) {
    dst[dstBase] += src[srcBase];
    if (src[srcBase + 1] < dst[dstBase + 1]) {
      dst[dstBase + 1] = src[srcBase + 1];
    }
    for (int off = 2; off < slotWidth; off += 4) {
      dst[dstBase + off] += src[srcBase + off];
      dst[dstBase + off + 1] = Math.addExact(dst[dstBase + off + 1], src[srcBase + off + 1]);
      if (src[srcBase + off + 2] < dst[dstBase + off + 2]) {
        dst[dstBase + off + 2] = src[srcBase + off + 2];
      }
      if (src[srcBase + off + 3] > dst[dstBase + off + 3]) {
        dst[dstBase + off + 3] = src[srcBase + off + 3];
      }
    }
  }

  private static void mergeBlockIntoAcc(final long[] dst, final long[] src, final int slotWidth) {
    mergeBlock(dst, 0, src, 0, slotWidth);
  }
}

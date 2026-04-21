/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Dict-encoded PAX region for OBJECT_KEY nameKey values. Stores the nameKey
 * ONLY here — the in-record varint is zeroed at serialize time so the page
 * is smaller overall.
 *
 * <h2>Wire format</h2>
 * <pre>
 *   byte   numUnique        // distinct nameKeys (typically 5-10)
 *   int[numUnique] dictKeys // the unique nameKeys (little-endian)
 *   short  okCount          // number of OBJECT_KEY slots
 *   long[16] objectKeyBitmap // which of the 1024 slots are OBJECT_KEY
 *   byte[okCount] dictIds   // per-OBJECT_KEY dict index, bitmap order
 * </pre>
 *
 * <p>For ~465 OBJECT_KEY slots × 5 unique nameKeys:
 * 1 + 20 + 2 + 128 + 465 = 616 bytes. Much smaller than the in-record
 * varint storage it replaces (~930 bytes of nameKey varints + field-offset
 * bytes). LZ4 compresses the dictIds column (5 repeating values) to near
 * zero.
 */
public final class ObjectKeyNameKeyRegion {

  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
  private static final int LANES = BYTE_SPECIES.length();

  // Direct byte[] VarHandles — bypass MemorySegment.ofArray allocation on every
  // hot read. JIT elides the bounds check when the offset is trivially in range
  // (loop-invariant hoisting), which MemorySegment.ofArray pays for explicitly.
  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle SHORT_LE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

  private static long getLong(final byte[] buf, final int off) {
    return (long) LONG_LE.get(buf, off);
  }

  private static int getInt(final byte[] buf, final int off) {
    return (int) INT_LE.get(buf, off);
  }

  private static int getShortU(final byte[] buf, final int off) {
    return ((short) SHORT_LE.get(buf, off)) & 0xFFFF;
  }

  private ObjectKeyNameKeyRegion() {
  }

  /**
   * Encode from parallel arrays (bitmap order).
   */
  public static byte[] encode(final int[] nameKeys, final int[] slots, final int count) {
    // Build dict.
    final int[] dict = new int[256];
    final byte[] dictIds = new byte[count];
    int numUnique = 0;
    for (int i = 0; i < count; i++) {
      final int nk = nameKeys[i];
      int id = -1;
      for (int j = 0; j < numUnique; j++) {
        if (dict[j] == nk) { id = j; break; }
      }
      if (id < 0) {
        if (numUnique >= 255) return null;
        id = numUnique;
        dict[numUnique++] = nk;
      }
      dictIds[i] = (byte) id;
    }

    // Build OBJECT_KEY bitmap.
    final long[] bitmap = new long[16];
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      bitmap[slot >>> 6] |= 1L << (slot & 63);
    }

    // Wire.
    final int size = 1 + numUnique * 4 + 2 + 128 + count;
    final byte[] out = new byte[size];
    final MemorySegment seg = MemorySegment.ofArray(out);
    seg.set(ValueLayout.JAVA_BYTE, 0L, (byte) numUnique);
    long off = 1;
    for (int i = 0; i < numUnique; i++) {
      seg.set(ValueLayout.JAVA_INT_UNALIGNED, off, dict[i]);
      off += 4;
    }
    seg.set(ValueLayout.JAVA_SHORT_UNALIGNED, off, (short) count);
    off += 2;
    for (int i = 0; i < 16; i++) {
      seg.set(ValueLayout.JAVA_LONG_UNALIGNED, off, bitmap[i]);
      off += 8;
    }
    MemorySegment.copy(dictIds, 0, seg, ValueLayout.JAVA_BYTE, off, count);
    return out;
  }

  public static int count(final byte[] payload) {
    if (payload == null || payload.length < 3) return 0;
    final int numUnique = payload[0] & 0xFF;
    final int countOff = 1 + numUnique * 4;
    if (payload.length < countOff + 2) return 0;
    return getShortU(payload, countOff);
  }

  /**
   * Extract the distinct {@code nameKey}s present on this page. Reads
   * directly from the dictKeys header — O(numUnique) with one VarHandle
   * load per entry. Empty array if the region is absent or the page has
   * no OBJECT_KEY slots.
   *
   * <p>Used by the page-skip index builder to determine, per leaf page,
   * which field names are present — the presence set is then folded into
   * a per-{@code nameKey} {@link org.roaringbitmap.RoaringBitmap} of pages
   * so scans can skip pages that have no slot with their anchor field.
   */
  public static int[] uniqueNameKeys(final byte[] payload) {
    if (payload == null) return EMPTY_INT_ARRAY;
    final int numUnique = payload[0] & 0xFF;
    if (numUnique == 0) return EMPTY_INT_ARRAY;
    final int[] out = new int[numUnique];
    for (int i = 0; i < numUnique; i++) {
      out[i] = getInt(payload, 1 + i * 4);
    }
    return out;
  }

  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /**
   * Look up nameKey for the N-th OBJECT_KEY slot (0-based in bitmap order).
   * Returns the nameKey, or -1 if index is out of range.
   */
  public static int nameKeyAt(final byte[] payload, final int bitmapIndex) {
    if (payload == null) return -1;
    final int numUnique = payload[0] & 0xFF;
    final int countOff = 1 + numUnique * 4;
    final int okCount = getShortU(payload, countOff);
    if (bitmapIndex < 0 || bitmapIndex >= okCount) return -1;
    final int dictIdsOff = countOff + 2 + 128;
    final int dictId = payload[dictIdsOff + bitmapIndex] & 0xFF;
    if (dictId >= numUnique) return -1;
    return getInt(payload, 1 + dictId * 4);
  }

  /**
   * Look up nameKey for a given slot index (0-1023). Converts slot to
   * bitmap-order index, then reads from dictIds. Returns -1 if the slot
   * is not an OBJECT_KEY on this page.
   *
   * <p>HFT hot path: called once per OBJECT_KEY slot during
   * {@code buildObjectKeySlotsForNameKey}. Uses direct {@code byte[]} VarHandle
   * reads — no {@code MemorySegment.ofArray} allocation.
   */
  public static int nameKeyForSlot(final byte[] payload, final int slotIndex) {
    if (payload == null || slotIndex < 0 || slotIndex > 1023) return -1;
    final int numUnique = payload[0] & 0xFF;
    final int bitmapOff = 1 + numUnique * 4 + 2;
    // Check if this slot is set in the OBJECT_KEY bitmap.
    final int wordIdx = slotIndex >>> 6;
    final long bit = 1L << (slotIndex & 63);
    final long word = getLong(payload, bitmapOff + wordIdx * 8);
    if ((word & bit) == 0) return -1;
    // Count set bits before this slot to find the bitmap-order index.
    int bitmapIndex = 0;
    for (int w = 0; w < wordIdx; w++) {
      bitmapIndex += Long.bitCount(getLong(payload, bitmapOff + w * 8));
    }
    bitmapIndex += Long.bitCount(word & (bit - 1));
    // Inlined nameKeyAt body — avoids the redundant numUnique/countOff re-read
    // and the nested array-bounds check the JIT occasionally leaves un-hoisted.
    final int countOff = 1 + numUnique * 4;
    final int okCount = getShortU(payload, countOff);
    if (bitmapIndex >= okCount) return -1;
    final int dictIdsOff = countOff + 2 + 128;
    final int dictId = payload[dictIdsOff + bitmapIndex] & 0xFF;
    if (dictId >= numUnique) return -1;
    return getInt(payload, 1 + dictId * 4);
  }

  /**
   * Find the original slot index (0-1023) for the N-th OBJECT_KEY in
   * bitmap order.
   */
  public static int slotForBitmapIndex(final byte[] payload, final int bitmapIndex) {
    if (payload == null) return -1;
    final int numUnique = payload[0] & 0xFF;
    final int bitmapOff = 1 + numUnique * 4 + 2;
    int remaining = bitmapIndex;
    for (int w = 0; w < 16; w++) {
      long word = getLong(payload, bitmapOff + w * 8);
      final int bits = Long.bitCount(word);
      if (remaining < bits) {
        while (remaining > 0) {
          word &= word - 1;
          remaining--;
        }
        return (w << 6) + Long.numberOfTrailingZeros(word);
      }
      remaining -= bits;
    }
    return -1;
  }

  /**
   * Thread-local scratch for {@link #findMatchingSlots}'s bitmap-order mapping.
   * The array is grown to fit the largest {@code okCount} any caller has seen
   * and reused across every subsequent call on the same thread. Reduces per-page
   * GC pressure — alloc-profile at 100M records showed 76K samples worth of
   * {@code findMatchingSlots;int[]} before this optimisation.
   */
  private static final ThreadLocal<int[]> BITMAP_SLOTS_SCRATCH =
      ThreadLocal.withInitial(() -> new int[256]);

  /**
   * SIMD filter: find OBJECT_KEY slots where nameKey == fieldKey.
   * Writes matching slot indices into out[]. Returns match count.
   */
  public static int findMatchingSlots(final byte[] payload, final int fieldKey, final int[] out) {
    if (payload == null || payload.length < 3) return 0;
    final int numUnique = payload[0] & 0xFF;
    if (numUnique == 0) return 0;

    // Dict lookup — byte[] VarHandle read, no MemorySegment wrapper.
    int targetId = -1;
    for (int i = 0; i < numUnique; i++) {
      if (getInt(payload, 1 + i * 4) == fieldKey) {
        targetId = i;
        break;
      }
    }
    if (targetId < 0) return 0;

    final int countOff = 1 + numUnique * 4;
    final int okCount = getShortU(payload, countOff);
    if (okCount == 0) return 0;
    final int bitmapOff = countOff + 2;
    final int dictIdsOff = bitmapOff + 128;

    // SIMD scan of dictIds — build bitmap slot mapping into thread-local scratch.
    final byte needle = (byte) targetId;
    final ByteVector bNeedle = ByteVector.broadcast(BYTE_SPECIES, needle);
    int[] bitmapSlots = BITMAP_SLOTS_SCRATCH.get();
    if (bitmapSlots.length < okCount) {
      bitmapSlots = new int[Math.max(okCount, bitmapSlots.length * 2)];
      BITMAP_SLOTS_SCRATCH.set(bitmapSlots);
    }
    int idx = 0;
    for (int w = 0; w < 16; w++) {
      long word = getLong(payload, bitmapOff + w * 8);
      final int base = w << 6;
      while (word != 0) {
        bitmapSlots[idx++] = base + Long.numberOfTrailingZeros(word);
        word &= word - 1;
      }
    }

    int written = 0;
    int i = 0;
    for (; i <= okCount - LANES; i += LANES) {
      // SIMD load from byte[] directly — ByteVector.fromArray, no MemorySegment wrapper.
      final ByteVector v = ByteVector.fromArray(BYTE_SPECIES, payload, dictIdsOff + i);
      final VectorMask<Byte> mask = v.compare(VectorOperators.EQ, bNeedle);
      if (!mask.anyTrue()) continue;
      for (int lane = 0; lane < LANES; lane++) {
        if (mask.laneIsSet(lane)) {
          out[written++] = bitmapSlots[i + lane];
        }
      }
    }
    for (; i < okCount; i++) {
      if ((payload[dictIdsOff + i] & 0xFF) == targetId) {
        out[written++] = bitmapSlots[i];
      }
    }
    return written;
  }
}

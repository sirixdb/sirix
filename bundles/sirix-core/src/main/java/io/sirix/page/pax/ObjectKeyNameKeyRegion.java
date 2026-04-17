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
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final int numUnique = seg.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF;
    final long countOff = 1L + (long) numUnique * 4;
    if (payload.length < countOff + 2) return 0;
    return seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, countOff) & 0xFFFF;
  }

  /**
   * Look up nameKey for the N-th OBJECT_KEY slot (0-based in bitmap order).
   * Returns the nameKey, or -1 if index is out of range.
   */
  public static int nameKeyAt(final byte[] payload, final int bitmapIndex) {
    if (payload == null) return -1;
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final int numUnique = seg.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF;
    final long countOff = 1L + (long) numUnique * 4;
    final int okCount = seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, countOff) & 0xFFFF;
    if (bitmapIndex < 0 || bitmapIndex >= okCount) return -1;
    final long dictIdsOff = countOff + 2 + 128;
    final int dictId = seg.get(ValueLayout.JAVA_BYTE, dictIdsOff + bitmapIndex) & 0xFF;
    if (dictId >= numUnique) return -1;
    return seg.get(ValueLayout.JAVA_INT_UNALIGNED, 1L + (long) dictId * 4);
  }

  /**
   * Look up nameKey for a given slot index (0-1023). Converts slot to
   * bitmap-order index, then reads from dictIds. Returns -1 if the slot
   * is not an OBJECT_KEY on this page.
   */
  public static int nameKeyForSlot(final byte[] payload, final int slotIndex) {
    if (payload == null || slotIndex < 0 || slotIndex > 1023) return -1;
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final int numUnique = seg.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF;
    final long bitmapOff = 1L + (long) numUnique * 4 + 2;
    // Check if this slot is set in the OBJECT_KEY bitmap.
    final int wordIdx = slotIndex >>> 6;
    final long bit = 1L << (slotIndex & 63);
    final long word = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, bitmapOff + (long) wordIdx * 8);
    if ((word & bit) == 0) return -1;
    // Count set bits before this slot to find the bitmap-order index.
    int bitmapIndex = 0;
    for (int w = 0; w < wordIdx; w++) {
      bitmapIndex += Long.bitCount(seg.get(ValueLayout.JAVA_LONG_UNALIGNED, bitmapOff + (long) w * 8));
    }
    bitmapIndex += Long.bitCount(word & (bit - 1));
    return nameKeyAt(payload, bitmapIndex);
  }

  /**
   * Find the original slot index (0-1023) for the N-th OBJECT_KEY in
   * bitmap order.
   */
  public static int slotForBitmapIndex(final byte[] payload, final int bitmapIndex) {
    if (payload == null) return -1;
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final int numUnique = seg.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF;
    final long bitmapOff = 1L + (long) numUnique * 4 + 2;
    int remaining = bitmapIndex;
    for (int w = 0; w < 16; w++) {
      long word = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, bitmapOff + (long) w * 8);
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
   * SIMD filter: find OBJECT_KEY slots where nameKey == fieldKey.
   * Writes matching slot indices into out[]. Returns match count.
   */
  public static int findMatchingSlots(final byte[] payload, final int fieldKey, final int[] out) {
    if (payload == null || payload.length < 3) return 0;
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final int numUnique = seg.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF;
    if (numUnique == 0) return 0;

    // Dict lookup.
    long off = 1;
    int targetId = -1;
    for (int i = 0; i < numUnique; i++) {
      if (seg.get(ValueLayout.JAVA_INT_UNALIGNED, off) == fieldKey) {
        targetId = i;
        break;
      }
      off += 4;
    }
    if (targetId < 0) return 0;

    final long countOff = 1L + (long) numUnique * 4;
    final int okCount = seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, countOff) & 0xFFFF;
    if (okCount == 0) return 0;
    final long bitmapOff = countOff + 2;
    final long dictIdsOff = bitmapOff + 128;

    // SIMD scan of dictIds.
    final byte needle = (byte) targetId;
    final ByteVector bNeedle = ByteVector.broadcast(BYTE_SPECIES, needle);
    // Pre-scan bitmap to build slot-index mapping.
    final int[] bitmapSlots = new int[okCount];
    int idx = 0;
    for (int w = 0; w < 16; w++) {
      long word = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, bitmapOff + (long) w * 8);
      final int base = w << 6;
      while (word != 0) {
        bitmapSlots[idx++] = base + Long.numberOfTrailingZeros(word);
        word &= word - 1;
      }
    }

    int written = 0;
    int i = 0;
    for (; i <= okCount - LANES; i += LANES) {
      final ByteVector v = ByteVector.fromMemorySegment(BYTE_SPECIES, seg,
          dictIdsOff + i, java.nio.ByteOrder.LITTLE_ENDIAN);
      final VectorMask<Byte> mask = v.compare(VectorOperators.EQ, bNeedle);
      if (!mask.anyTrue()) continue;
      for (int lane = 0; lane < LANES; lane++) {
        if (mask.laneIsSet(lane)) {
          out[written++] = bitmapSlots[i + lane];
        }
      }
    }
    for (; i < okCount; i++) {
      if ((seg.get(ValueLayout.JAVA_BYTE, dictIdsOff + i) & 0xFF) == targetId) {
        out[written++] = bitmapSlots[i];
      }
    }
    return written;
  }
}

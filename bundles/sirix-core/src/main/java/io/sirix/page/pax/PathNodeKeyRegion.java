/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Dict-encoded PAX region for per-slot {@code pathNodeKey} values. Structural kinds
 * (OBJECT, ARRAY, OBJECT_KEY, fused OBJECT_NAMED_*, JSON_DOCUMENT_ROOT, XML_ELEMENT etc.)
 * all carry a {@code pathNodeKey} field; in record-shaped JSON workloads the number of
 * distinct pathNodeKeys per page is tiny (a handful — one per nested field schema), so a
 * per-page dictionary of {@code int pathNodeKey} + 1-byte-per-slot dictId fits in a
 * fraction of the ~2 KB of raw delta-varint bytes the per-record heap previously paid.
 *
 * <p>Mirror of {@link ObjectKeyNameKeyRegion} but (a) indexed by ALL populated slots with
 * a pathNodeKey field (not just OBJECT_KEY) and (b) the bitmap marks which slots have a
 * pathNodeKey field. Lookup: {@code slotIndex → bitmap popcount prefix → dictId → value}.
 *
 * <h2>Wire format</h2>
 * <pre>
 *   byte   numUnique        // distinct pathNodeKey values on the page (≤ 255)
 *   int[numUnique] dictKeys // the unique pathNodeKey values (little-endian)
 *   short  slotCount        // number of slots that have a pathNodeKey field
 *   long[16] bitmap         // which of the 1024 slots have a pathNodeKey field
 *   byte[slotCount] dictIds // per-slot dict index, bitmap order
 * </pre>
 *
 * <p>For ~1030 slots × 5 distinct pathNodeKeys: 1 + 20 + 2 + 128 + 1030 = 1181 bytes.
 * Replaces ~3090 bytes of per-record delta-varint bytes (~3 B avg × 1030 slots).
 *
 * <h2>HFT-grade access</h2>
 * Direct byte-array VarHandle reads bypass {@code MemorySegment.ofArray} allocation on
 * every hot lookup. Lookup is O(1) after a single popcount on the bitmap.
 */
public final class PathNodeKeyRegion {

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

  private PathNodeKeyRegion() {
  }

  /**
   * Encode from parallel arrays (per populated slot with pathNodeKey, bitmap order) directly
   * into the caller-supplied {@code out} buffer. Three scratches are passed in so the hot
   * encode path is zero-alloc.
   *
   * @param pathNodeKeys   per-entry pathNodeKey value (bitmap order)
   * @param slots          per-entry slot index 0..1023 (bitmap order)
   * @param count          number of populated entries
   * @param out            output buffer (pre-sized via {@link #encodedSize})
   * @param dictScratch    caller-supplied 256-int dictionary scratch
   * @param dictIdsScratch caller-supplied &ge;{@code count}-byte dict-id scratch
   * @param bitmapScratch  caller-supplied 16-long bitmap scratch (zeroed on entry)
   * @return bytes written, or {@code -1} when the dictionary would exceed 255 entries
   *         (caller falls back to inline varints).
   */
  public static int encode(final int[] pathNodeKeys, final int[] slots, final int count,
      final byte[] out, final int[] dictScratch, final byte[] dictIdsScratch,
      final long[] bitmapScratch) {
    if (count == 0) return -1;
    int numUnique = 0;
    for (int i = 0; i < count; i++) {
      final int pnk = pathNodeKeys[i];
      int id = -1;
      for (int j = 0; j < numUnique; j++) {
        if (dictScratch[j] == pnk) { id = j; break; }
      }
      if (id < 0) {
        if (numUnique >= 255) return -1;
        id = numUnique;
        dictScratch[numUnique++] = pnk;
      }
      dictIdsScratch[i] = (byte) id;
    }

    for (int i = 0; i < 16; i++) bitmapScratch[i] = 0L;
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      bitmapScratch[slot >>> 6] |= 1L << (slot & 63);
    }

    final int size = 1 + numUnique * 4 + 2 + 128 + count;
    final MemorySegment seg = MemorySegment.ofArray(out);
    seg.set(ValueLayout.JAVA_BYTE, 0L, (byte) numUnique);
    long off = 1;
    for (int i = 0; i < numUnique; i++) {
      seg.set(ValueLayout.JAVA_INT_UNALIGNED, off, dictScratch[i]);
      off += 4;
    }
    seg.set(ValueLayout.JAVA_SHORT_UNALIGNED, off, (short) count);
    off += 2;
    for (int i = 0; i < 16; i++) {
      seg.set(ValueLayout.JAVA_LONG_UNALIGNED, off, bitmapScratch[i]);
      off += 8;
    }
    MemorySegment.copy(dictIdsScratch, 0, seg, ValueLayout.JAVA_BYTE, off, count);
    return size;
  }

  /**
   * Cheap pre-encode size estimate — caller uses this vs raw varint bytes to decide
   * whether the region is profitable for the page.
   *
   * @return encoded size in bytes, or -1 if the dictionary would exceed 255 entries.
   */
  public static int encodedSize(final int[] pathNodeKeys, final int count,
      final int[] dictScratch) {
    if (count == 0) return -1;
    int numUnique = 0;
    for (int i = 0; i < count; i++) {
      final int pnk = pathNodeKeys[i];
      boolean found = false;
      for (int j = 0; j < numUnique; j++) {
        if (dictScratch[j] == pnk) { found = true; break; }
      }
      if (!found) {
        if (numUnique >= 255) return -1;
        dictScratch[numUnique++] = pnk;
      }
    }
    return 1 + numUnique * 4 + 2 + 128 + count;
  }

  /**
   * Look up {@code pathNodeKey} for a given slot index (0-1023). Returns
   * {@code -1} if the slot is not populated with a pathNodeKey-bearing kind.
   *
   * <p>HFT hot path: called once per structural slot whose kind carries a pathNodeKey.
   * Uses direct {@code byte[]} VarHandle reads — no {@code MemorySegment.ofArray}
   * allocation.
   */
  public static int pathNodeKeyForSlot(final byte[] payload, final int slotIndex) {
    if (payload == null || slotIndex < 0 || slotIndex > 1023) return -1;
    final int numUnique = payload[0] & 0xFF;
    final int countOff = 1 + numUnique * 4;
    final int bitmapOff = countOff + 2;
    final int wordIdx = slotIndex >>> 6;
    final long bit = 1L << (slotIndex & 63);
    final long word = getLong(payload, bitmapOff + wordIdx * 8);
    if ((word & bit) == 0) return -1;
    int bitmapIndex = 0;
    for (int w = 0; w < wordIdx; w++) {
      bitmapIndex += Long.bitCount(getLong(payload, bitmapOff + w * 8));
    }
    bitmapIndex += Long.bitCount(word & (bit - 1));
    final int slotCount = getShortU(payload, countOff);
    if (bitmapIndex >= slotCount) return -1;
    final int dictIdsOff = bitmapOff + 128;
    final int dictId = payload[dictIdsOff + bitmapIndex] & 0xFF;
    if (dictId >= numUnique) return -1;
    return getInt(payload, 1 + dictId * 4);
  }

}

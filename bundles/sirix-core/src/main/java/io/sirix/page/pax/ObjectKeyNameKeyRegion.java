/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
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

  // Array VarHandles for the ENCODE path, which builds its output in a byte[] before the region
  // table copies it off-heap. Reads go through the payload segment instead (see the accessors
  // below): payloads are native now, so there is no ofArray wrapper left to avoid.
  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle SHORT_LE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

  private static long getLong(final MemorySegment buf, final long off) {
    return buf.get(LE.LONG, off);
  }

  private static int getInt(final MemorySegment buf, final long off) {
    return buf.get(LE.INT, off);
  }

  private static int getShortU(final MemorySegment buf, final long off) {
    return buf.get(LE.SHORT, off) & 0xFFFF;
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
    final int size = dictIdsOffset(numUnique) + count;
    final byte[] out = new byte[size];
    final MemorySegment seg = MemorySegment.ofArray(out);
    seg.set(ValueLayout.JAVA_BYTE, 0L, (byte) numUnique);
    long off = 1;
    for (int i = 0; i < numUnique; i++) {
      seg.set(LE.INT, off, dict[i]);
      off += 4;
    }
    seg.set(LE.SHORT, off, (short) count);
    off += 2;
    for (int i = 0; i < 16; i++) {
      seg.set(LE.LONG, off, bitmap[i]);
      off += 8;
    }
    MemorySegment.copy(dictIds, 0, seg, ValueLayout.JAVA_BYTE, off, count);
    return out;
  }

  // ── wire layout ─────────────────────────────────────────────────────────────
  // byte           numUnique
  // int[numUnique] dictKeys
  // short          okCount
  // long[16]       slot bitmap (BITMAP_BYTES)
  // byte[okCount]  dictIds, in bitmap order
  //
  // The offsets below are the ONE derivation of that layout. Eight kernels used to inline the
  // same arithmetic; a format change re-derived in seven of them still reads in-bounds bytes at
  // wrong offsets — a shifted bitmap or a wrong okCount that silently returns wrong slots.

  /** Bytes of the slot bitmap. */
  private static final int BITMAP_BYTES = 128;

  /** Offset of the {@code okCount} short. */
  private static int countOffset(final int numUnique) {
    return 1 + numUnique * 4;
  }

  /** Offset of the slot bitmap. */
  private static int bitmapOffset(final int numUnique) {
    return countOffset(numUnique) + 2;
  }

  /** Offset of the dictIds column. */
  private static int dictIdsOffset(final int numUnique) {
    return bitmapOffset(numUnique) + BITMAP_BYTES;
  }

  public static int count(final MemorySegment payload) {
    if (payload == null || payload.byteSize() < 3) return 0;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    final int countOff = countOffset(numUnique);
    if (payload.byteSize() < countOff + 2) return 0;
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
  public static int[] uniqueNameKeys(final MemorySegment payload) {
    if (payload == null) return EMPTY_INT_ARRAY;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
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
  public static int nameKeyAt(final MemorySegment payload, final int bitmapIndex) {
    if (payload == null) return -1;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    final int okCount = getShortU(payload, countOffset(numUnique));
    if (bitmapIndex < 0 || bitmapIndex >= okCount) return -1;
    final int dictId =
        payload.get(ValueLayout.JAVA_BYTE, dictIdsOffset(numUnique) + bitmapIndex) & 0xFF;
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
  public static int nameKeyForSlot(final MemorySegment payload, final int slotIndex) {
    if (payload == null || slotIndex < 0 || slotIndex > 1023) return -1;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    final int bitmapOff = bitmapOffset(numUnique);
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
    final int okCount = getShortU(payload, countOffset(numUnique));
    if (bitmapIndex >= okCount) return -1;
    final int dictId =
        payload.get(ValueLayout.JAVA_BYTE, dictIdsOffset(numUnique) + bitmapIndex) & 0xFF;
    if (dictId >= numUnique) return -1;
    return getInt(payload, 1 + dictId * 4);
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
   * How many OBJECT_KEY slots on this page carry {@code fieldKey} — the count alone, without
   * materializing which slots they are.
   *
   * <p>{@link #findMatchingSlots} has to expand the 1024-bit slot bitmap into bitmap-order slot
   * indices before it can report anything. A caller that only needs the cardinality — the
   * region-scan completeness check, which compares this against the number-region's tag count —
   * pays none of that: it is one dictionary probe plus a SIMD popcount over the dict-id bytes.
   *
   * @param payload the region payload, or {@code null}
   * @param fieldKey the nameKey to count
   * @return the number of matching slots; {@code 0} when the region is absent or the key unknown
   */
  public static int countMatchingSlots(final MemorySegment payload, final int fieldKey) {
    if (payload == null || payload.byteSize() < 3) return 0;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    if (numUnique == 0) return 0;

    int targetId = -1;
    for (int i = 0; i < numUnique; i++) {
      if (getInt(payload, 1 + i * 4) == fieldKey) {
        targetId = i;
        break;
      }
    }
    if (targetId < 0) return 0;

    final int okCount = getShortU(payload, countOffset(numUnique));
    if (okCount == 0) return 0;
    final int dictIdsOff = dictIdsOffset(numUnique);

    final ByteVector bNeedle = ByteVector.broadcast(BYTE_SPECIES, (byte) targetId);
    int matched = 0;
    int i = 0;
    for (; i <= okCount - LANES; i += LANES) {
      final ByteVector v = ByteVector.fromMemorySegment(BYTE_SPECIES, payload,
                                                       (long) dictIdsOff + i,
                                                       ByteOrder.LITTLE_ENDIAN);
      matched += v.compare(VectorOperators.EQ, bNeedle).trueCount();
    }
    for (; i < okCount; i++) {
      if ((payload.get(ValueLayout.JAVA_BYTE, dictIdsOff + i) & 0xFF) == targetId) {
        matched++;
      }
    }
    return matched;
  }

  /**
   * SIMD filter reporting BITMAP-ORDER positions rather than slot numbers.
   *
   * <p>{@link #findMatchingSlots} answers "which page slots carry this field", which is what a
   * record-reading caller needs. A caller that wants to index a parallel per-OBJECT_KEY column —
   * {@link RecordOrdinalRegion}'s ordinals, or the dictIds themselves — needs the position within
   * the OBJECT_KEY sequence instead, and going through slot numbers to get it means expanding the
   * 1024-bit slot bitmap and then inverting the mapping. That work is pure loss here: the positions
   * are exactly where the dict-id scan already finds its matches.
   *
   * @param out destination, must hold at least {@link #count(MemorySegment)} entries
   * @return the number of matches written, ascending
   */
  public static int findMatchingBitmapIndices(final MemorySegment payload, final int fieldKey,
      final int[] out) {
    if (payload == null || payload.byteSize() < 3 || out == null) {
      return 0;
    }
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    if (numUnique == 0) {
      return 0;
    }
    int targetId = -1;
    for (int i = 0; i < numUnique; i++) {
      if (getInt(payload, 1 + i * 4) == fieldKey) {
        targetId = i;
        break;
      }
    }
    if (targetId < 0) {
      return 0;
    }
    final int okCount = getShortU(payload, countOffset(numUnique));
    if (okCount == 0) {
      return 0;
    }
    if (out.length < okCount) {
      throw new IllegalArgumentException(
          "output too small: " + out.length + " entries for " + okCount + " OBJECT_KEY slots");
    }
    final int dictIdsOff = dictIdsOffset(numUnique);

    // Vector compare to locate the matching lanes, then trailing-zero iteration over the mask to
    // write them. The write side cannot vectorize — a compress-store would need the positions as
    // ints, and materializing lane ordinals costs more than walking a mask that is usually sparse.
    final ByteVector bNeedle = ByteVector.broadcast(BYTE_SPECIES, (byte) targetId);
    int matched = 0;
    int i = 0;
    for (; i <= okCount - LANES; i += LANES) {
      final ByteVector v = ByteVector.fromMemorySegment(BYTE_SPECIES, payload,
                                                       (long) dictIdsOff + i,
                                                       ByteOrder.LITTLE_ENDIAN);
      long bits = v.compare(VectorOperators.EQ, bNeedle).toLong();
      while (bits != 0) {
        out[matched++] = i + Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
      }
    }
    for (; i < okCount; i++) {
      if ((payload.get(ValueLayout.JAVA_BYTE, dictIdsOff + i) & 0xFF) == targetId) {
        out[matched++] = i;
      }
    }
    return matched;
  }

  /**
   * The page slot behind {@code bitmapIndex} of the region's bitmap-ordered columns, or {@code -1}
   * when the index is out of range or the payload unreadable.
   *
   * <p>A select-nth-set-bit over the region's own 128-byte slot bitmap — at most sixteen popcounts
   * and one word's bit-clear walk. This is the right tool when ONE position needs resolving (the
   * fused kernel's pending boundary slot); resolving it through {@link #findMatchingSlots} would
   * re-scan the whole dictIds column and expand the bitmap into a scratch array to read a single
   * element.
   */
  public static int slotAt(final MemorySegment payload, final int bitmapIndex) {
    if (payload == null || payload.byteSize() < 3 || bitmapIndex < 0) {
      return -1;
    }
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
    if (numUnique == 0) {
      return -1;
    }
    final int countOff = countOffset(numUnique);
    if (payload.byteSize() < dictIdsOffset(numUnique)) {
      return -1;  // truncated or corrupt: decline, as every reader of this format does, not throw
    }
    final int okCount = getShortU(payload, countOff);
    if (bitmapIndex >= okCount) {
      return -1;
    }
    final int bitmapOff = bitmapOffset(numUnique);
    int remaining = bitmapIndex;
    for (int w = 0; w < 16; w++) {
      long word = getLong(payload, bitmapOff + w * 8);
      final int bits = Long.bitCount(word);
      if (remaining < bits) {
        for (int i = 0; i < remaining; i++) {
          word &= word - 1;
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
  public static int findMatchingSlots(final MemorySegment payload, final int fieldKey, final int[] out) {
    if (payload == null || payload.byteSize() < 3) return 0;
    final int numUnique = payload.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
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

    final int okCount = getShortU(payload, countOffset(numUnique));
    if (okCount == 0) return 0;
    final int bitmapOff = bitmapOffset(numUnique);
    final int dictIdsOff = dictIdsOffset(numUnique);

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
      // Loads straight out of the payload segment. Payloads are native (see ColumnLoad), which is
      // the case fromMemorySegment intrinsifies; the heap-backed case, which does not, no longer
      // arises here.
      final ByteVector v = ByteVector.fromMemorySegment(BYTE_SPECIES, payload,
                                                       (long) dictIdsOff + i,
                                                       ByteOrder.LITTLE_ENDIAN);
      final VectorMask<Byte> mask = v.compare(VectorOperators.EQ, bNeedle);
      if (!mask.anyTrue()) continue;
      for (int lane = 0; lane < LANES; lane++) {
        if (mask.laneIsSet(lane)) {
          out[written++] = bitmapSlots[i + lane];
        }
      }
    }
    for (; i < okCount; i++) {
      if ((payload.get(ValueLayout.JAVA_BYTE, dictIdsOff + i) & 0xFF) == targetId) {
        out[written++] = bitmapSlots[i];
      }
    }
    return written;
  }
}

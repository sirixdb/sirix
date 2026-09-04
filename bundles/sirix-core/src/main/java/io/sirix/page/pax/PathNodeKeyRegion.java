/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Dict-encoded PAX region for per-slot {@code pathNodeKey} values. Structural kinds (OBJECT, ARRAY,
 * OBJECT_KEY, fused OBJECT_NAMED_*, JSON_DOCUMENT_ROOT, XML_ELEMENT etc.) all carry a
 * {@code pathNodeKey} field; in record-shaped JSON workloads the number of distinct pathNodeKeys
 * per page is tiny (a handful — one per nested field schema), so a per-page dictionary of
 * {@code int pathNodeKey} + 1-byte-per-slot dictId fits in a fraction of the ~2 KB of raw
 * delta-varint bytes the per-record heap previously paid.
 *
 * <p>
 * Mirror of {@link ObjectKeyNameKeyRegion} but (a) indexed by ALL populated slots with a
 * pathNodeKey field (not just OBJECT_KEY) and (b) the bitmap marks which slots have a pathNodeKey
 * field. Lookup: {@code slotIndex → bitmap popcount prefix → dictId → value}.
 *
 * <h2>Wire format</h2>
 * 
 * <pre>
 *   byte   numUnique        // distinct pathNodeKey values on the page (≤ 255)
 *   int[numUnique] dictKeys // the unique pathNodeKey values (little-endian)
 *   short  slotCount        // number of slots that have a pathNodeKey field
 *   long[16] bitmap         // which of the 1024 slots have a pathNodeKey field
 *   byte[slotCount] dictIds // per-slot dict index, bitmap order
 * </pre>
 *
 * <p>
 * For ~1030 slots × 5 distinct pathNodeKeys: 1 + 20 + 2 + 128 + 1030 = 1181 bytes. Replaces ~3090
 * bytes of per-record delta-varint bytes (~3 B avg × 1030 slots).
 *
 * <h2>HFT-grade access</h2> Direct byte-array VarHandle reads bypass {@code MemorySegment.ofArray}
 * allocation on every hot lookup. Lookup is O(1) after a single popcount on the bitmap.
 */
public final class PathNodeKeyRegion {

  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
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

  private PathNodeKeyRegion() {}

  /**
   * Encode from parallel arrays (per populated slot with pathNodeKey, bitmap order) directly into the
   * caller-supplied {@code out} buffer. Three scratches are passed in so the hot encode path is
   * zero-alloc.
   *
   * @param pathNodeKeys per-entry pathNodeKey value (bitmap order)
   * @param slots per-entry slot index 0..1023 (bitmap order)
   * @param count number of populated entries
   * @param out output buffer (pre-sized via {@link #encodedSize})
   * @param dictScratch caller-supplied 256-int dictionary scratch
   * @param dictIdsScratch caller-supplied &ge;{@code count}-byte dict-id scratch
   * @param bitmapScratch caller-supplied 16-long bitmap scratch (zeroed on entry)
   * @return bytes written, or {@code -1} when the dictionary would exceed 255 entries (caller falls
   *         back to inline varints).
   */
  public static int encode(final int[] pathNodeKeys, final int[] slots, final int count, final byte[] out,
      final int[] dictScratch, final byte[] dictIdsScratch, final long[] bitmapScratch) {
    if (count == 0)
      return -1;
    int numUnique = 0;
    for (int i = 0; i < count; i++) {
      final int pnk = pathNodeKeys[i];
      int id = -1;
      for (int j = 0; j < numUnique; j++) {
        if (dictScratch[j] == pnk) {
          id = j;
          break;
        }
      }
      if (id < 0) {
        if (numUnique >= 255)
          return -1;
        id = numUnique;
        dictScratch[numUnique++] = pnk;
      }
      dictIdsScratch[i] = (byte) id;
    }

    for (int i = 0; i < 16; i++)
      bitmapScratch[i] = 0L;
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      bitmapScratch[slot >>> 6] |= 1L << (slot & 63);
    }

    final int size = 1 + numUnique * 4 + 2 + 128 + count;
    final MemorySegment seg = MemorySegment.ofArray(out);
    seg.set(ValueLayout.JAVA_BYTE, 0L, (byte) numUnique);
    long off = 1;
    for (int i = 0; i < numUnique; i++) {
      seg.set(LE.INT, off, dictScratch[i]);
      off += 4;
    }
    seg.set(LE.SHORT, off, (short) count);
    off += 2;
    for (int i = 0; i < 16; i++) {
      seg.set(LE.LONG, off, bitmapScratch[i]);
      off += 8;
    }
    MemorySegment.copy(dictIdsScratch, 0, seg, ValueLayout.JAVA_BYTE, off, count);
    return size;
  }

  /**
   * Cheap pre-encode size estimate — caller uses this vs raw varint bytes to decide whether the
   * region is profitable for the page.
   *
   * @return encoded size in bytes, or -1 if the dictionary would exceed 255 entries.
   */
  public static int encodedSize(final int[] pathNodeKeys, final int count, final int[] dictScratch) {
    if (count == 0)
      return -1;
    int numUnique = 0;
    for (int i = 0; i < count; i++) {
      final int pnk = pathNodeKeys[i];
      boolean found = false;
      for (int j = 0; j < numUnique; j++) {
        if (dictScratch[j] == pnk) {
          found = true;
          break;
        }
      }
      if (!found) {
        if (numUnique >= 255)
          return -1;
        dictScratch[numUnique++] = pnk;
      }
    }
    return 1 + numUnique * 4 + 2 + 128 + count;
  }

  // ────────────────────────────────────────────── compact form (marker byte 0)

  /**
   * Marker in place of {@code numUnique} that says the payload is in the compact form.
   *
   * <p>
   * The legacy layout opens with the dictionary size, which is never zero — {@link #encode} refuses a
   * page with nothing to put in the dictionary — so a leading zero is free to mean "read the compact
   * header instead". That keeps both forms self-describing on one wire with no version field, and the
   * legacy form byte-identical to what a pre-change encoder wrote.
   */
  private static final int COMPACT_MARKER = 0;

  /** Flag bits: the low two are the dictionary's per-entry width code, bit 2 the lane's form. */
  private static final int WIDTH_CODE_MASK = 0x03;
  private static final int FLAG_LANE_DELTA_RLE = 0x04;

  /**
   * Flag bit: dictionary entries are stored as the zig-zag delta from the previous entry rather than
   * as the offset from {@code dictMin}.
   *
   * <p>
   * Chosen on a tie, and that is the point. A page's distinct pathNodeKeys are usually a run of
   * consecutive path ids, so the offset form is a byte RAMP — every byte distinct, nothing for the
   * body codec to match — while the delta form is the same byte repeated. Measured on a 106-key
   * dictionary the two are 106 bytes either way and 110 against 8 after LZ77. A delta lane is never
   * LESS compressible than the absolute lane it came from (a repeated key spacing becomes a repeated
   * byte; an irregular one is equally irregular either way), so preferring it on a tie is free.
   */
  private static final int FLAG_DICT_DELTA = 0x08;

  /**
   * Re-encode a legacy payload into the compact form: a frame-of-reference dictionary and, when it
   * pays, a run-length-encoded dict-id lane.
   *
   * <p>
   * Both halves of the legacy layout are laid out for random access rather than for size — the
   * dictionary spends four bytes on keys whose spread is usually under a hundred, and the lane spends
   * a byte per slot on a sequence that, in record-shaped JSON, walks the same field order for every
   * record and is therefore a handful of constant-delta runs. Neither observation is about any
   * particular schema: the encoder measures both forms and keeps the smaller, so a page whose ids are
   * genuinely unpredictable simply stays raw.
   *
   * <p>
   * Nothing reads the compact form directly. {@link #expand} turns it back into the legacy layout in
   * one pass at page load, so every lookup stays the single popcount it always was.
   *
   * @param legacy a payload as {@link #encode} wrote it
   * @param legacyLength its length in bytes
   * @param out destination, at least {@code legacyLength} bytes
   * @return the compact length, or -1 when the compact form would not be smaller
   */
  public static int compact(final byte[] legacy, final int legacyLength, final byte[] out) {
    if (legacy == null || legacyLength <= 0 || out == null) {
      return -1;
    }
    final int numUnique = legacy[0] & 0xFF;
    if (numUnique == COMPACT_MARKER) {
      return -1; // already compact
    }
    final int countOff = 1 + numUnique * 4;
    final int bitmapOff = countOff + 2;
    final int dictIdsOff = bitmapOff + 128;
    if (legacyLength < dictIdsOff) {
      return -1;
    }
    final int slotCount = getShortU(legacy, countOff);
    if (dictIdsOff + slotCount != legacyLength) {
      return -1;
    }

    long minKey = Long.MAX_VALUE;
    long maxKey = Long.MIN_VALUE;
    long maxZigZagDelta = 0;
    long previousKey = 0;
    for (int i = 0; i < numUnique; i++) {
      final long key = getInt(legacy, 1 + i * 4);
      if (key < minKey) {
        minKey = key;
      }
      if (key > maxKey) {
        maxKey = key;
      }
      if (i > 0) {
        final long delta = key - previousKey;
        final long zigZag = (delta << 1) ^ (delta >> 63);
        if (zigZag > maxZigZagDelta) {
          maxZigZagDelta = zigZag;
        }
      }
      previousKey = key;
    }
    // The first entry is stored against dictMin either way, so both forms have to hold that offset.
    final long absoluteSpan = maxKey - minKey;
    final long deltaSpan = Math.max(maxZigZagDelta, getInt(legacy, 1) - minKey);
    final boolean dictDelta = fixedWidthFor(deltaSpan) <= fixedWidthFor(absoluteSpan);
    final long span = dictDelta
        ? deltaSpan
        : absoluteSpan;
    final int dictWidth = fixedWidthFor(span);
    final int widthCode = dictWidth == 1
        ? 0
        : dictWidth == 2
            ? 1
            : 2;

    // Measure the run-length form of the dict-id lane before committing to it.
    final int runBytes = deltaRunBytes(legacy, dictIdsOff, slotCount);
    final boolean rle = runBytes >= 0 && runBytes < slotCount;
    final int laneBytes = rle
        ? runBytes
        : slotCount;
    final int size = 3 + 4 + numUnique * dictWidth + 2 + 128 + laneBytes;
    if (size >= legacyLength || size > out.length) {
      return -1;
    }

    out[0] = (byte) COMPACT_MARKER;
    out[1] = (byte) (widthCode | (rle
        ? FLAG_LANE_DELTA_RLE
        : 0)
        | (dictDelta
            ? FLAG_DICT_DELTA
            : 0));
    out[2] = (byte) numUnique;
    putInt(out, 3, (int) minKey);
    int off = 7;
    long previousStored = 0;
    for (int i = 0; i < numUnique; i++) {
      final long key = getInt(legacy, 1 + i * 4);
      final long stored;
      if (!dictDelta) {
        stored = key - minKey;
      } else if (i == 0) {
        stored = key - minKey;
      } else {
        final long delta = key - previousStored;
        stored = (delta << 1) ^ (delta >> 63);
      }
      previousStored = key;
      for (int b = 0; b < dictWidth; b++) {
        out[off++] = (byte) (stored >>> (b * 8));
      }
    }
    putShort(out, off, slotCount);
    off += 2;
    System.arraycopy(legacy, bitmapOff, out, off, 128);
    off += 128;
    if (rle) {
      off = writeDeltaRuns(legacy, dictIdsOff, slotCount, out, off);
    } else {
      System.arraycopy(legacy, dictIdsOff, out, off, slotCount);
      off += slotCount;
    }
    return off == size
        ? size
        : -1;
  }

  /** The smallest fixed width, in bytes, that holds an unsigned value of {@code span}. */
  private static int fixedWidthFor(final long span) {
    if (span < 0x100L) {
      return 1;
    }
    return span < 0x10000L
        ? 2
        : 4;
  }

  /**
   * The number of bytes {@link #expand} will write for {@code payload}, or -1 when it is already in
   * the legacy layout and needs no expansion.
   *
   * @param payload a payload in either form
   * @param length its length in bytes
   * @return the expanded length, or -1
   */
  public static int expandedSize(final byte[] payload, final int length) {
    if (payload == null || length < 3 || (payload[0] & 0xFF) != COMPACT_MARKER) {
      return -1;
    }
    final int numUnique = payload[2] & 0xFF;
    final int dictWidth = 1 << (payload[1] & WIDTH_CODE_MASK);
    final int slotCountOff = 7 + numUnique * dictWidth;
    if (length < slotCountOff + 2) {
      throw new IllegalArgumentException("pathNodeKey column is too short for its " + numUnique + "-entry dictionary");
    }
    return 1 + numUnique * 4 + 2 + 128 + getShortU(payload, slotCountOff);
  }

  /**
   * Rebuild the legacy layout from a compact payload, so every reader keeps the O(1) lookup it had.
   *
   * @param payload a compact payload
   * @param length its length in bytes
   * @param out destination, at least {@link #expandedSize} bytes
   * @return the number of bytes written
   */
  public static int expand(final byte[] payload, final int length, final byte[] out) {
    final int expanded = expandedSize(payload, length);
    if (expanded < 0) {
      throw new IllegalArgumentException("pathNodeKey column is not in the compact form");
    }
    if (out.length < expanded) {
      throw new IllegalArgumentException(
          "pathNodeKey column expands to " + expanded + " bytes, the buffer holds " + out.length);
    }
    final int flags = payload[1] & 0xFF;
    final int numUnique = payload[2] & 0xFF;
    final int dictWidth = 1 << (flags & WIDTH_CODE_MASK);
    final boolean dictDelta = (flags & FLAG_DICT_DELTA) != 0;
    final int minKey = getInt(payload, 3);
    out[0] = (byte) numUnique;
    int in = 7;
    int previousKey = 0;
    for (int i = 0; i < numUnique; i++) {
      long stored = 0;
      for (int b = 0; b < dictWidth; b++) {
        stored |= (long) (payload[in++] & 0xFF) << (b * 8);
      }
      final int key;
      if (!dictDelta || i == 0) {
        key = minKey + (int) stored;
      } else {
        key = previousKey + (int) ((stored >>> 1) ^ -(stored & 1));
      }
      previousKey = key;
      putInt(out, 1 + i * 4, key);
    }
    final int slotCount = getShortU(payload, in);
    in += 2;
    final int countOff = 1 + numUnique * 4;
    putShort(out, countOff, slotCount);
    final int bitmapOff = countOff + 2;
    if (in + 128 > length) {
      throw new IllegalArgumentException("pathNodeKey column is truncated before its slot bitmap");
    }
    System.arraycopy(payload, in, out, bitmapOff, 128);
    in += 128;
    final int dictIdsOff = bitmapOff + 128;
    if ((flags & FLAG_LANE_DELTA_RLE) != 0) {
      readDeltaRuns(payload, in, length, out, dictIdsOff, slotCount, numUnique);
    } else {
      if (in + slotCount > length) {
        throw new IllegalArgumentException("pathNodeKey column is truncated before its dict-id lane");
      }
      System.arraycopy(payload, in, out, dictIdsOff, slotCount);
    }
    return expanded;
  }

  /**
   * Bytes a run-length encoding of the dict-id lane would take, or -1 when it cannot be built.
   *
   * <p>
   * A run is a maximal stretch over which the id advances by the same step, which is what a page of
   * records with a repeating field order produces: one long +1 run per record and one negative step
   * back to the first field.
   */
  private static int deltaRunBytes(final byte[] lane, final int laneOff, final int slotCount) {
    if (slotCount <= 0) {
      return -1;
    }
    int bytes = 0;
    int runs = 0;
    int index = 0;
    int previous = 0;
    while (index < slotCount) {
      final int delta = (lane[laneOff + index] & 0xFF) - previous;
      int runLength = 1;
      previous = lane[laneOff + index] & 0xFF;
      int next = index + 1;
      while (next < slotCount && (lane[laneOff + next] & 0xFF) - previous == delta) {
        previous = lane[laneOff + next] & 0xFF;
        runLength++;
        next++;
      }
      bytes += varIntSize(zigZag(delta)) + varIntSize(runLength);
      runs++;
      index = next;
    }
    return bytes + varIntSize(runs);
  }

  private static int writeDeltaRuns(final byte[] lane, final int laneOff, final int slotCount, final byte[] out,
      final int outOff) {
    int runs = 0;
    int index = 0;
    int previous = 0;
    while (index < slotCount) {
      final int delta = (lane[laneOff + index] & 0xFF) - previous;
      previous = lane[laneOff + index] & 0xFF;
      int next = index + 1;
      while (next < slotCount && (lane[laneOff + next] & 0xFF) - previous == delta) {
        previous = lane[laneOff + next] & 0xFF;
        next++;
      }
      runs++;
      index = next;
    }
    int off = writeVarInt(out, outOff, runs);
    index = 0;
    previous = 0;
    while (index < slotCount) {
      final int delta = (lane[laneOff + index] & 0xFF) - previous;
      int runLength = 1;
      previous = lane[laneOff + index] & 0xFF;
      int next = index + 1;
      while (next < slotCount && (lane[laneOff + next] & 0xFF) - previous == delta) {
        previous = lane[laneOff + next] & 0xFF;
        runLength++;
        next++;
      }
      off = writeVarInt(out, off, zigZag(delta));
      off = writeVarInt(out, off, runLength);
      index = next;
    }
    return off;
  }

  private static void readDeltaRuns(final byte[] payload, final int inOff, final int length, final byte[] out,
      final int laneOff, final int slotCount, final int numUnique) {
    int in = inOff;
    final long runsAndWidth = readVarInt(payload, in, length);
    int runs = (int) runsAndWidth;
    in += (int) (runsAndWidth >>> 32);
    int written = 0;
    int previous = 0;
    while (runs-- > 0) {
      final long deltaAndWidth = readVarInt(payload, in, length);
      in += (int) (deltaAndWidth >>> 32);
      final int delta = unZigZag((int) deltaAndWidth);
      final long lengthAndWidth = readVarInt(payload, in, length);
      in += (int) (lengthAndWidth >>> 32);
      final int runLength = (int) lengthAndWidth;
      if (runLength <= 0 || written + runLength > slotCount) {
        throw new IllegalArgumentException(
            "pathNodeKey column has a run of " + runLength + " ids beyond its " + slotCount + " slots");
      }
      for (int i = 0; i < runLength; i++) {
        previous += delta;
        if (previous < 0 || previous >= numUnique) {
          throw new IllegalArgumentException(
              "pathNodeKey column decodes dict id " + previous + ", outside its " + numUnique + "-entry dictionary");
        }
        out[laneOff + written++] = (byte) previous;
      }
    }
    if (written != slotCount) {
      throw new IllegalArgumentException(
          "pathNodeKey column decodes " + written + " dict ids, its header declares " + slotCount);
    }
  }

  private static int zigZag(final int value) {
    return (value << 1) ^ (value >> 31);
  }

  private static int unZigZag(final int value) {
    return (value >>> 1) ^ -(value & 1);
  }

  private static int varIntSize(final int value) {
    int size = 1;
    int remaining = value >>> 7;
    while (remaining != 0) {
      size++;
      remaining >>>= 7;
    }
    return size;
  }

  private static int writeVarInt(final byte[] out, final int off, final int value) {
    int cursor = off;
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      out[cursor++] = (byte) ((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    out[cursor++] = (byte) remaining;
    return cursor;
  }

  /** Packs the decoded value in the low 32 bits and the bytes consumed in the high 32. */
  private static long readVarInt(final byte[] payload, final int off, final int limit) {
    int value = 0;
    int shift = 0;
    int cursor = off;
    while (true) {
      if (cursor >= limit || shift > 28) {
        throw new IllegalArgumentException("pathNodeKey column has a malformed varint at offset " + off);
      }
      final int b = payload[cursor++] & 0xFF;
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return ((long) (cursor - off) << 32) | (value & 0xFFFFFFFFL);
  }

  private static void putInt(final byte[] out, final int off, final int value) {
    out[off] = (byte) value;
    out[off + 1] = (byte) (value >>> 8);
    out[off + 2] = (byte) (value >>> 16);
    out[off + 3] = (byte) (value >>> 24);
  }

  private static void putShort(final byte[] out, final int off, final int value) {
    out[off] = (byte) value;
    out[off + 1] = (byte) (value >>> 8);
  }

  /**
   * Look up {@code pathNodeKey} for a given slot index (0-1023). Returns {@code -1} if the slot is
   * not populated with a pathNodeKey-bearing kind.
   *
   * <p>
   * HFT hot path: called once per structural slot whose kind carries a pathNodeKey. Uses direct
   * {@code byte[]} VarHandle reads — no {@code MemorySegment.ofArray} allocation.
   */
  public static int pathNodeKeyForSlot(final byte[] payload, final int slotIndex) {
    if (payload == null || slotIndex < 0 || slotIndex > 1023)
      return -1;
    final int numUnique = payload[0] & 0xFF;
    final int countOff = 1 + numUnique * 4;
    final int bitmapOff = countOff + 2;
    final int wordIdx = slotIndex >>> 6;
    final long bit = 1L << (slotIndex & 63);
    final long word = getLong(payload, bitmapOff + wordIdx * 8);
    if ((word & bit) == 0)
      return -1;
    int bitmapIndex = 0;
    for (int w = 0; w < wordIdx; w++) {
      bitmapIndex += Long.bitCount(getLong(payload, bitmapOff + w * 8));
    }
    bitmapIndex += Long.bitCount(word & (bit - 1));
    final int slotCount = getShortU(payload, countOff);
    if (bitmapIndex >= slotCount)
      return -1;
    final int dictIdsOff = bitmapOff + 128;
    final int dictId = payload[dictIdsOff + bitmapIndex] & 0xFF;
    if (dictId >= numUnique)
      return -1;
    return getInt(payload, 1 + dictId * 4);
  }

}

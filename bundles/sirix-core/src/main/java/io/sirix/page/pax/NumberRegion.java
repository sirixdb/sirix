package io.sirix.page.pax;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * PAX number-region codec. Packs the numeric payload of all
 * {@code OBJECT_NUMBER_VALUE} slots on a {@link io.sirix.page.KeyValueLeafPage}
 * into one contiguous buffer, grouped by parent {@code OBJECT_KEY}
 * {@code nameKey}. Scan operators look up the target field's range via a
 * per-tag directory in the header and iterate only the matching values — no
 * per-entry tag decode, no slot walk, no {@code moveTo}.
 *
 * <h2>Wire format</h2>
 * <pre>
 * byte   encodingKind         // 0 = PLAIN_LONG, 1 = BIT_PACKED  (legacy, no per-tag zone maps)
 *                             // 2 = PLAIN_LONG_ZM, 3 = BIT_PACKED_ZM (with per-tag zone maps)
 * byte   tagKind              // 0 = nameKey-tagged (compression-only)
 *                             // 1 = pathNodeKey-tagged (SIMD-safe for path-scoped scans)
 * int    count                // total values across all tags
 * long   valueMin             // zone-map lower bound (across all tags)
 * long   valueMax             // zone-map upper bound
 * long   valueBase             // BIT_PACKED base; 0 for PLAIN_LONG
 * byte   valueBitWidth        // BIT_PACKED width 1..63; 64 for PLAIN_LONG
 * int    dictSize             // parent-nameKey dictionary size
 * int[dictSize]  dictEntries  // parent nameKey values, ordered by local id
 * int[dictSize]  tagStart     // starting value index for each tag
 * int[dictSize]  tagCount     // number of values with each tag
 * // ZM-variants only (encodingKind == 2 or 3):
 * long[dictSize] tagMin       // per-tag minimum value
 * long[dictSize] tagMax       // per-tag maximum value
 * byte[] valueBytes           // values grouped/sorted by tag
 *                             //   (PLAIN_LONG: count × 8 bytes
 *                             //    BIT_PACKED: count × valueBitWidth bits)
 * </pre>
 *
 * <h2>HFT-grade scan loop</h2>
 * A scan on field {@code F} looks up the tag id (O(dictSize)), reads
 * {@code tagStart[tag]} + {@code tagCount[tag]}, then iterates a tight range
 * of {@link #decodeValueAt(byte[], Header, int)} calls. No conditional per
 * iteration, no tag decode, bit-packing decode reduces to one unaligned
 * 64-bit load + shift + mask.
 */
public final class NumberRegion {

  public static final byte ENC_PLAIN_LONG = 0;
  public static final byte ENC_BIT_PACKED = 1;
  /** PLAIN_LONG with per-tag zone maps appended (tagMin[], tagMax[]). */
  public static final byte ENC_PLAIN_LONG_ZM = 2;
  /** BIT_PACKED with per-tag zone maps appended (tagMin[], tagMax[]). */
  public static final byte ENC_BIT_PACKED_ZM = 3;

  /**
   * {@code tagKind} classifier for the region's tag dictionary. Determines the
   * semantic interpretation of {@link Header#dict}:
   *
   * <ul>
   *   <li>{@link #TAG_KIND_NAME} — tags are parent OBJECT_KEY nameKeys
   *       (compression-only; not SIMD-safe when the same nameKey sits under
   *       multiple pathNodeKeys on one page).</li>
   *   <li>{@link #TAG_KIND_PATH_NODE} — tags are parent OBJECT_KEY pathNodeKeys
   *       truncated to int. SIMD-safe for path-scoped scans: a successful
   *       {@link #lookupTag(Header, int)} implies every value in the tag's
   *       range belongs to the exact requested pathNodeKey.</li>
   * </ul>
   */
  public static final byte TAG_KIND_NAME = 0;
  public static final byte TAG_KIND_PATH_NODE = 1;

  /** @return true if the encoding kind includes per-tag zone-map arrays. */
  public static boolean hasZoneMap(final byte encodingKind) {
    return encodingKind >= ENC_PLAIN_LONG_ZM;
  }

  /** @return the "plain vs bit-packed" flag without the zone-map bit. */
  public static boolean isBitPacked(final byte encodingKind) {
    return (encodingKind & 1) != 0;
  }

  private NumberRegion() {}

  // ───────────────────────────────────────────────────────────────── header

  /** Parsed header. Reused across calls to avoid allocation. */
  public static final class Header {
    public byte encodingKind;
    /** Tag dictionary classification; see {@link #TAG_KIND_NAME}/{@link #TAG_KIND_PATH_NODE}. */
    public byte tagKind;
    public int count;
    public long valueMin;
    public long valueMax;
    public long valueBase;
    public byte valueBitWidth;
    public int dictSize;
    public int[] dict;       // length ≥ dictSize
    public int[] tagStart;   // length ≥ dictSize
    public int[] tagCount;   // length ≥ dictSize
    /** Per-tag minimum value. Populated only when {@link #hasZoneMap(byte)}; else null. */
    public long[] tagMin;
    /** Per-tag maximum value. Populated only when {@link #hasZoneMap(byte)}; else null. */
    public long[] tagMax;
    public int valueBytesOffset;
    public int valueBytesLength;

    public Header parseInto(final byte[] payload) {
      final ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
      encodingKind = bb.get();
      tagKind = bb.get();
      count = bb.getInt();
      valueMin = bb.getLong();
      valueMax = bb.getLong();
      valueBase = bb.getLong();
      valueBitWidth = bb.get();
      dictSize = bb.getInt();
      if (dict == null || dict.length < dictSize) dict = new int[Math.max(4, dictSize)];
      if (tagStart == null || tagStart.length < dictSize) tagStart = new int[Math.max(4, dictSize)];
      if (tagCount == null || tagCount.length < dictSize) tagCount = new int[Math.max(4, dictSize)];
      for (int i = 0; i < dictSize; i++) dict[i] = bb.getInt();
      for (int i = 0; i < dictSize; i++) tagStart[i] = bb.getInt();
      for (int i = 0; i < dictSize; i++) tagCount[i] = bb.getInt();
      if (hasZoneMap(encodingKind)) {
        if (tagMin == null || tagMin.length < dictSize) tagMin = new long[Math.max(4, dictSize)];
        if (tagMax == null || tagMax.length < dictSize) tagMax = new long[Math.max(4, dictSize)];
        for (int i = 0; i < dictSize; i++) tagMin[i] = bb.getLong();
        for (int i = 0; i < dictSize; i++) tagMax[i] = bb.getLong();
      } else {
        tagMin = null;
        tagMax = null;
      }
      valueBytesOffset = bb.position();
      valueBytesLength = bitsToBytes((long) count * (isBitPacked(encodingKind) ? valueBitWidth : 64));
      return this;
    }

    /** Per-tag minimum, or the page-global {@link #valueMin} if no per-tag map is present. */
    public long tagMinOrGlobal(final int tag) {
      return tagMin != null ? tagMin[tag] : valueMin;
    }

    /** Per-tag maximum, or the page-global {@link #valueMax} if no per-tag map is present. */
    public long tagMaxOrGlobal(final int tag) {
      return tagMax != null ? tagMax[tag] : valueMax;
    }
  }

  // ───────────────────────────────────────────────────────────── encoding

  /**
   * Legacy 3-arg entry point. Encodes with {@link #TAG_KIND_NAME}: dict holds
   * parent OBJECT_KEY nameKeys. Kept for test and callers that don't have
   * pathNodeKey information.
   */
  public static byte[] encode(final long[] values, final int[] parentTags, final int count) {
    return encode(values, parentTags, count, TAG_KIND_NAME);
  }

  /**
   * Encode parallel arrays {@code values[i]} and {@code parentTags[i]} into a
   * tag-sorted payload. {@code tagKind} declares the semantic interpretation
   * of {@code parentTags} so downstream scan operators can decide whether a
   * tag match is safe for path-scoped queries.
   *
   * <p>The arrays may be longer than {@code count}; only the prefix is
   * consumed.
   */
  public static byte[] encode(final long[] values, final int[] parentTags, final int count,
      final byte tagKind) {
    // Build parent-tag dictionary (in-place, grow if needed)
    int[] dict = new int[count == 0 ? 1 : Math.min(count, 16)];
    int dictSize = 0;
    final int[] localIds = count == 0 ? new int[0] : new int[count];
    for (int i = 0; i < count; i++) {
      final int nk = parentTags[i];
      int found = -1;
      for (int j = 0; j < dictSize; j++) {
        if (dict[j] == nk) { found = j; break; }
      }
      if (found < 0) {
        if (dictSize == dict.length) {
          final int[] grown = new int[dict.length << 1];
          System.arraycopy(dict, 0, grown, 0, dictSize);
          dict = grown;
        }
        found = dictSize;
        dict[dictSize++] = nk;
      }
      localIds[i] = found;
    }

    // Compute per-tag counts then convert to starts (exclusive-scan style).
    final int[] tagCount = new int[dictSize];
    for (int i = 0; i < count; i++) {
      tagCount[localIds[i]]++;
    }
    final int[] tagStart = new int[dictSize];
    {
      int running = 0;
      for (int t = 0; t < dictSize; t++) {
        tagStart[t] = running;
        running += tagCount[t];
      }
    }

    // Scatter values into their tag's slot of the sorted output; track per-tag min/max.
    final long[] sortedValues = count == 0 ? new long[0] : new long[count];
    final long[] tagMin = new long[dictSize];
    final long[] tagMax = new long[dictSize];
    for (int t = 0; t < dictSize; t++) {
      tagMin[t] = Long.MAX_VALUE;
      tagMax[t] = Long.MIN_VALUE;
    }
    final int[] cursor = tagStart.clone();
    for (int i = 0; i < count; i++) {
      final int t = localIds[i];
      final long v = values[i];
      sortedValues[cursor[t]++] = v;
      if (v < tagMin[t]) tagMin[t] = v;
      if (v > tagMax[t]) tagMax[t] = v;
    }

    // Global min/max is the fold over per-tag bounds.
    long min = 0, max = 0;
    if (count > 0) {
      min = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      for (int t = 0; t < dictSize; t++) {
        if (tagCount[t] == 0) continue;
        if (tagMin[t] < min) min = tagMin[t];
        if (tagMax[t] > max) max = tagMax[t];
      }
    }
    final long spread = count == 0 ? 0 : (max - min);
    final boolean bitPacked = count > 0 && spread >= 0 && spread < (1L << 48);
    // Zone-map-carrying encoding kinds (2/3). The non-ZM kinds (0/1) remain legacy-
    // readable but are no longer written.
    final byte encodingKind = bitPacked ? ENC_BIT_PACKED_ZM : ENC_PLAIN_LONG_ZM;
    final long valueBase = bitPacked ? min : 0L;
    final byte valueBitWidth = bitPacked
        ? (byte) Math.max(1, 64 - Long.numberOfLeadingZeros(spread))
        : (byte) 64;

    final int headerBytes = 1 + 1 + 4 + 8 + 8 + 8 + 1 + 4
        + (4 * dictSize)   // dict
        + (4 * dictSize)   // tagStart
        + (4 * dictSize)   // tagCount
        + (8 * dictSize)   // tagMin
        + (8 * dictSize);  // tagMax
    final int valueBytes = bitsToBytes((long) count * valueBitWidth);
    final byte[] out = new byte[headerBytes + valueBytes];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(encodingKind);
    bb.put(tagKind);
    bb.putInt(count);
    bb.putLong(min);
    bb.putLong(max);
    bb.putLong(valueBase);
    bb.put(valueBitWidth);
    bb.putInt(dictSize);
    for (int i = 0; i < dictSize; i++) bb.putInt(dict[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagStart[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagCount[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMin[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMax[i]);

    final int valueOff = bb.position();
    if (!bitPacked) {
      for (int i = 0; i < count; i++) {
        writeLittleEndianLong(out, valueOff + (i << 3), sortedValues[i]);
      }
    } else {
      bitPackLongs(out, valueOff, sortedValues, count, valueBase, valueBitWidth);
    }
    return out;
  }

  // ───────────────────────────────────────────────────────────── decoding

  /** Decode the value at {@code index} (absolute within the sorted payload). O(1). */
  public static long decodeValueAt(final byte[] payload, final Header h, final int index) {
    if (!isBitPacked(h.encodingKind)) {
      return readLittleEndianLong(payload, h.valueBytesOffset + (index << 3));
    }
    return h.valueBase + bitUnpackLong(payload, h.valueBytesOffset, h.valueBitWidth, index);
  }

  /**
   * Local tag id for a parent tag value, or {@code -1} when absent. O(dictSize).
   * The tag value is interpreted according to {@link Header#tagKind}: nameKey
   * for {@link #TAG_KIND_NAME}, pathNodeKey (int-truncated) for
   * {@link #TAG_KIND_PATH_NODE}.
   */
  public static int lookupTag(final Header h, final int tag) {
    final int[] dict = h.dict;
    if (dict == null) return -1;
    for (int i = 0; i < h.dictSize; i++) {
      if (dict[i] == tag) return i;
    }
    return -1;
  }

  /** Bulk-decode all values (across all tags) into {@code out}. */
  public static void decodeAllValues(final byte[] payload, final Header h, final long[] out) {
    final int count = h.count;
    if (!isBitPacked(h.encodingKind)) {
      int off = h.valueBytesOffset;
      for (int i = 0; i < count; i++, off += 8) {
        out[i] = readLittleEndianLong(payload, off);
      }
    } else {
      final long base = h.valueBase;
      final int bw = h.valueBitWidth;
      for (int i = 0; i < count; i++) {
        out[i] = base + bitUnpackLong(payload, h.valueBytesOffset, bw, i);
      }
    }
  }

  // ──────────────────────────────────────────────────────── bit pack/unpack

  private static int bitsToBytes(final long bits) {
    return (int) ((bits + 7L) >>> 3);
  }

  private static void bitPackLongs(final byte[] out, final int outOff, final long[] values,
      final int count, final long base, final int bitWidth) {
    long buf = 0L;
    int bitsInBuf = 0;
    int writePos = outOff;
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    for (int i = 0; i < count; i++) {
      final long v = (values[i] - base) & mask;
      buf |= v << bitsInBuf;
      bitsInBuf += bitWidth;
      while (bitsInBuf >= 8) {
        out[writePos++] = (byte) buf;
        buf >>>= 8;
        bitsInBuf -= 8;
      }
    }
    if (bitsInBuf > 0) {
      out[writePos] = (byte) buf;
    }
  }

  private static long bitUnpackLong(final byte[] data, final int baseOff, final int bitWidth,
      final int index) {
    final long bitOff = (long) index * bitWidth;
    final int byteOff = (int) (bitOff >>> 3) + baseOff;
    final int bitInByte = (int) (bitOff & 7L);
    final long w0 = readUpToLongLE(data, byteOff);
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    long v = (w0 >>> bitInByte) & mask;
    final int bitsConsumed = 64 - bitInByte;
    if (bitsConsumed < bitWidth) {
      final int extra = bitWidth - bitsConsumed;
      final long next = readByteUnsigned(data, byteOff + 8);
      v |= (next & ((1L << extra) - 1L)) << bitsConsumed;
    }
    return v;
  }

  private static long readUpToLongLE(final byte[] data, final int off) {
    final int avail = data.length - off;
    if (avail >= 8) {
      return readLittleEndianLong(data, off);
    }
    long v = 0L;
    for (int i = 0; i < avail; i++) {
      v |= ((long) (data[off + i] & 0xFF)) << (i << 3);
    }
    return v;
  }

  private static long readByteUnsigned(final byte[] data, final int off) {
    return off < data.length ? (data[off] & 0xFFL) : 0L;
  }

  private static long readLittleEndianLong(final byte[] data, final int off) {
    return  (data[off]     & 0xFFL)
         | ((data[off + 1] & 0xFFL) <<  8)
         | ((data[off + 2] & 0xFFL) << 16)
         | ((data[off + 3] & 0xFFL) << 24)
         | ((data[off + 4] & 0xFFL) << 32)
         | ((data[off + 5] & 0xFFL) << 40)
         | ((data[off + 6] & 0xFFL) << 48)
         | ((data[off + 7] & 0xFFL) << 56);
  }

  private static void writeLittleEndianLong(final byte[] data, final int off, final long v) {
    data[off]     = (byte)  v;
    data[off + 1] = (byte) (v >>>  8);
    data[off + 2] = (byte) (v >>> 16);
    data[off + 3] = (byte) (v >>> 24);
    data[off + 4] = (byte) (v >>> 32);
    data[off + 5] = (byte) (v >>> 40);
    data[off + 6] = (byte) (v >>> 48);
    data[off + 7] = (byte) (v >>> 56);
  }
}

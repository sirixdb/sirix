package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * PAX boolean-region codec. Packs the boolean payload of all
 * {@code OBJECT_BOOLEAN_VALUE} slots on a {@link io.sirix.page.KeyValueLeafPage}
 * into a packed-bit array, grouped by parent {@code OBJECT_KEY}
 * {@code nameKey} or {@code pathNodeKey}.
 *
 * <h2>Motivation</h2>
 * The numeric and string PAX regions give the vectorized executor fully
 * columnar scan paths for aggregate and group-by queries; filter queries
 * that involve a boolean field (e.g. {@code $u.active}) still fall back to
 * per-slot OBJECT_KEY traversal because we have no columnar home for bool
 * values. Adding this region lets the ColumnarScanExecutor AND a numeric
 * predicate bitmap with the boolean column in a single SIMD-friendly pass
 * — exactly the pattern DuckDB / CedarDB / Umbra use.
 *
 * <h2>Wire format</h2>
 * <pre>
 * byte   encodingKind         // 0 = PACKED_BITS (only variant)
 * byte   tagKind              // 0 = nameKey-tagged, 1 = pathNodeKey-tagged
 * int    count                // total boolean values across all tags
 * int    dictSize             // parent-tag dictionary size
 * int[dictSize]  dict         // parent tag values
 * int[dictSize]  tagStart     // starting bit index for each tag
 * int[dictSize]  tagCount     // number of bits per tag
 * byte[ceil(count/8)] bits    // packed value bits, bit i = value i
 * </pre>
 *
 * <h2>HFT-grade scan loop</h2>
 * A filter over field {@code F} looks up the tag id (O(dictSize)), reads
 * {@code tagStart[tag]} + {@code tagCount[tag]}, then scans the bit array
 * one 64-bit word at a time — pair with {@link NumberRegion}-produced
 * bitmaps via bitwise AND for fused multi-column filter evaluation.
 */
public final class BooleanRegion {

  public static final byte ENC_PACKED_BITS = 0;
  public static final byte TAG_KIND_NAME = 0;
  public static final byte TAG_KIND_PATH_NODE = 1;

  private BooleanRegion() {
  }

  public static final class Header {
    public byte encodingKind;
    public byte tagKind;
    public int count;
    public int dictSize;
    public int[] dict;
    public int[] tagStart;
    public int[] tagCount;
    /** Byte offset into payload where the packed-bit value array starts. */
    public int valueBitsOffset;
    /** Length in bytes of the packed-bit value array ({@code ceil(count/8)}). */
    public int valueBitsLength;

    public Header parseInto(final MemorySegment payload) {
      final RegionReader in = new RegionReader(payload);
      encodingKind = in.readByte();
      tagKind = in.readByte();
      count = in.readInt();
      dictSize = in.readInt();
      if (dict == null || dict.length < dictSize) dict = new int[Math.max(4, dictSize)];
      if (tagStart == null || tagStart.length < dictSize) tagStart = new int[Math.max(4, dictSize)];
      if (tagCount == null || tagCount.length < dictSize) tagCount = new int[Math.max(4, dictSize)];
      in.readInts(dict, dictSize);
      in.readInts(tagStart, dictSize);
      in.readInts(tagCount, dictSize);
      valueBitsOffset = in.position();
      valueBitsLength = (count + 7) >>> 3;
      return this;
    }
  }

  /**
   * O({@link Header#dictSize}) lookup of the local tag id for an external
   * {@code nameKey} / {@code pathNodeKey}. Returns {@code -1} when the tag is
   * absent on this page.
   */
  public static int lookupTag(final Header h, final int tag) {
    for (int i = 0; i < h.dictSize; i++) {
      if (h.dict[i] == tag) return i;
    }
    return -1;
  }

  /**
   * Read the {@code index}-th boolean value in the region (absolute, across
   * all tags). Caller ensures {@code 0 <= index < h.count}.
   */
  public static boolean decodeAt(final MemorySegment payload, final Header h, final int index) {
    final long byteOff = h.valueBitsOffset + (index >>> 3);
    final int bit = index & 7;
    return ((payload.get(ValueLayout.JAVA_BYTE, byteOff) >>> bit) & 1) != 0;
  }

  /**
   * Count the {@code true} entries in the range {@code [start, start + n)}.
   *
   * <p>Delegates to {@link BooleanRegionSimd#countTrue}, which sweeps whole 64-bit words eight at
   * a time through the vector unit's population count and masks the partial words at each end.
   * This method used to walk the range a byte at a time, discarding 56 bits of every register it
   * loaded.
   */
  public static int countTrue(final MemorySegment payload, final Header h, final int start, final int n) {
    return (int) BooleanRegionSimd.countTrue(payload, h.valueBitsOffset, start, n);
  }

  /**
   * Live-value counterpart of {@link #countTrue} for the versioned merge; {@code liveBits} is
   * indexed relative to {@code start}.
   */
  public static int countTrueMasked(final MemorySegment payload, final Header h, final int start,
      final int n, final long[] liveBits) {
    return (int) BooleanRegionSimd.countTrueMasked(payload, h.valueBitsOffset, start, n, liveBits);
  }

  /**
   * AND this column into {@code target}, a bitmap indexed relative to {@code start}, so a boolean
   * predicate can be fused with whatever another column's kernel already selected.
   *
   * @param invert apply {@code NOT field} instead of {@code field}
   * @return how many bits remain set
   */
  public static long andInto(final MemorySegment payload, final Header h, final int start, final int n,
      final long[] target, final boolean invert) {
    return BooleanRegionSimd.andInto(payload, h.valueBitsOffset, start, n, target, invert);
  }

  /**
   * Encode the given {@code values[0..count)} array keyed by
   * {@code parentTags[0..count)} into the on-disk format above. Values are
   * re-grouped by tag so the reader can scan a contiguous bit range per
   * field.
   *
   * @param values      per-entry boolean values
   * @param parentTags  per-entry parent nameKey (or pathNodeKey when
   *                    {@code tagKind == TAG_KIND_PATH_NODE})
   * @param count       number of entries
   * @param tagKind     {@link #TAG_KIND_NAME} or {@link #TAG_KIND_PATH_NODE}
   * @return packed payload bytes
   */
  public static byte[] encode(final boolean[] values, final int[] parentTags, final int count,
      final byte tagKind) {
    if (count == 0) return new byte[0];
    // Build tag dictionary in first-seen order.
    final int[] dict = new int[Math.min(count, 256)];
    int dictSize = 0;
    final int[] tagIdPerEntry = new int[count];
    for (int i = 0; i < count; i++) {
      final int tag = parentTags[i];
      int id = -1;
      for (int j = 0; j < dictSize; j++) {
        if (dict[j] == tag) { id = j; break; }
      }
      if (id < 0) {
        if (dictSize == dict.length) {
          return null;  // fallback path
        }
        id = dictSize;
        dict[dictSize++] = tag;
      }
      tagIdPerEntry[i] = id;
    }
    // Bucket entries per tag, preserving insertion order within each bucket.
    final int[] tagCount = new int[dictSize];
    for (int i = 0; i < count; i++) tagCount[tagIdPerEntry[i]]++;
    final int[] tagStart = new int[dictSize];
    for (int i = 1; i < dictSize; i++) tagStart[i] = tagStart[i - 1] + tagCount[i - 1];
    // Write pointer per tag while we fill.
    final int[] tagCursor = new int[dictSize];
    System.arraycopy(tagStart, 0, tagCursor, 0, dictSize);

    final int valueBitsLength = (count + 7) >>> 3;
    final int headerSize = 1 + 1 + 4 + 4 + dictSize * 12;
    final byte[] out = new byte[headerSize + valueBitsLength];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(ENC_PACKED_BITS);
    bb.put(tagKind);
    bb.putInt(count);
    bb.putInt(dictSize);
    for (int i = 0; i < dictSize; i++) bb.putInt(dict[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagStart[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagCount[i]);
    final int valueBitsOff = bb.position();
    for (int i = 0; i < count; i++) {
      if (values[i]) {
        final int absIdx = tagCursor[tagIdPerEntry[i]]++;
        out[valueBitsOff + (absIdx >>> 3)] |= (byte) (1 << (absIdx & 7));
      } else {
        tagCursor[tagIdPerEntry[i]]++;
      }
    }
    return out;
  }
}

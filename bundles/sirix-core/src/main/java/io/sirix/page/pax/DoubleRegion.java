/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The floating-point counterpart of {@link NumberRegion}: per-page, per-tag columns of
 * double-typed field values, with per-tag zone maps.
 *
 * <h2>Why a second numeric region</h2>
 *
 * <p>{@link NumberRegion} is long-only by construction — its bit-packing, delta coding and zone
 * maps are all integer arithmetic. A field whose value happens to be fractional on some records
 * therefore never entered a column at all, and the completeness oracle (rightly) refused every
 * page holding one: the tag's count fell short of the page's slot count, and the whole page went
 * back to record reconstruction because of a handful of values. This is exactly how a typed
 * column store treats it — DuckDB, Umbra and ClickHouse give every physical type its own column
 * and its own min/max summaries — narrowed to the two-type split JSON actually produces.
 *
 * <p>With both regions present, a numeric predicate over a mixed page is the SUM of two kernel
 * passes, and the oracle becomes {@code longCount + doubleCount == anchorSlots}.
 *
 * <h2>Encoding</h2>
 *
 * <p>Values are stored PLAIN as little-endian IEEE-754 doubles, tag-grouped. No bit-packing:
 * doubles do not pack (BtrBlocks' pseudodecimal and ALP exist precisely because they don't, and
 * either can replace this encoding later behind the same header). Plain is what the SIMD compare
 * wants anyway — eight lanes per load with no unpack — and the column is small by construction,
 * because it holds only the values the long region could not.
 *
 * <h2>Wire format</h2>
 *
 * <pre>
 * byte             version   // VERSION_V1
 * byte             tagKind   // NumberRegion.TAG_KIND_NAME or TAG_KIND_PATH_NODE
 * int              count
 * int              dictSize
 * int[dictSize]    dict
 * int[dictSize]    tagStart
 * int[dictSize]    tagCount
 * double[dictSize] tagMin    // NaN-free by construction: JSON has no NaN literal
 * double[dictSize] tagMax
 * double[count]    values    // tag-grouped, page order within a tag
 * </pre>
 *
 * <p>Older readers step over the unknown kind by its length prefix; newer readers treat absence
 * as "no doubles on this page", which is also what it means.
 */
public final class DoubleRegion {

  public static final byte VERSION_V1 = 1;

  private static final int FIXED_BYTES = 1 + 1 + 4 + 4;

  private static final int BYTES_PER_TAG = 4 + 4 + 4 + 8 + 8;

  private DoubleRegion() {
    throw new AssertionError("no instances");
  }

  /** Parsed header, reused across pages so the scan allocates nothing. */
  public static final class Header {
    public byte tagKind;
    public int count;
    public int dictSize;
    public int[] dict;
    public int[] tagStart;
    public int[] tagCount;
    public double[] tagMin;
    public double[] tagMax;
    /** Byte offset of the plain little-endian value doubles. */
    public int valuesOffset;

    /**
     * Parse {@code payload} into this instance.
     *
     * @return {@code this}, or {@code null} for an absent, truncated or future-version payload —
     *         each meaning "no double column", never a wrong one
     */
    public Header parseInto(final MemorySegment payload) {
      if (payload == null || payload.byteSize() < FIXED_BYTES) {
        return null;
      }
      // Locals first, committed only once the payload checks out, so a declined parse cannot
      // leave this shared scratch mixing two pages' numbers.
      final RegionReader in = new RegionReader(payload);
      if (in.readByte() != VERSION_V1) {
        return null;
      }
      final byte readTagKind = in.readByte();
      final int readCount = in.readInt();
      final int readDictSize = in.readInt();
      if (readCount < 0 || readDictSize < 0
          || payload.byteSize() < (long) FIXED_BYTES + (long) readDictSize * BYTES_PER_TAG
              + (long) readCount * Double.BYTES) {
        return null;
      }
      tagKind = readTagKind;
      count = readCount;
      dictSize = readDictSize;
      if (dict == null || dict.length < dictSize) dict = new int[Math.max(4, dictSize)];
      if (tagStart == null || tagStart.length < dictSize) tagStart = new int[Math.max(4, dictSize)];
      if (tagCount == null || tagCount.length < dictSize) tagCount = new int[Math.max(4, dictSize)];
      if (tagMin == null || tagMin.length < dictSize) tagMin = new double[Math.max(4, dictSize)];
      if (tagMax == null || tagMax.length < dictSize) tagMax = new double[Math.max(4, dictSize)];
      in.readInts(dict, dictSize);
      in.readInts(tagStart, dictSize);
      in.readInts(tagCount, dictSize);
      for (int i = 0; i < dictSize; i++) {
        tagMin[i] = Double.longBitsToDouble(in.readLong());
      }
      for (int i = 0; i < dictSize; i++) {
        tagMax[i] = Double.longBitsToDouble(in.readLong());
      }
      valuesOffset = in.position();
      return this;
    }
  }

  /** Local tag id for {@code tag} in the header's own key space, or {@code -1}. */
  public static int lookupTag(final Header h, final int tag) {
    if (h == null) {
      return -1;
    }
    for (int i = 0; i < h.dictSize; i++) {
      if (h.dict[i] == tag) {
        return i;
      }
    }
    return -1;
  }

  /** The value at absolute index {@code index}. */
  public static double decodeValueAt(final MemorySegment payload, final Header h, final int index) {
    return Double.longBitsToDouble(
        payload.get(LE.LONG, h.valuesOffset + (long) index * Double.BYTES));
  }

  /**
   * Encode from parallel arrays. Values are regrouped by tag here, preserving page order within
   * each tag, so callers append in slot order and never pre-sort.
   *
   * @return the payload, or {@code null} when {@code count == 0}
   */
  public static byte[] encode(final double[] values, final int[] tags, final int count,
      final byte tagKind) {
    if (values == null || tags == null || count <= 0) {
      return null;
    }
    // Tag dictionary in first-appearance order.
    final int[] dict = new int[count];
    int dictSize = 0;
    for (int i = 0; i < count; i++) {
      final int t = tags[i];
      boolean seen = false;
      for (int d = 0; d < dictSize; d++) {
        if (dict[d] == t) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        dict[dictSize++] = t;
      }
    }
    final int[] tagStart = new int[dictSize];
    final int[] tagCount = new int[dictSize];
    final double[] tagMin = new double[dictSize];
    final double[] tagMax = new double[dictSize];
    for (int d = 0; d < dictSize; d++) {
      tagMin[d] = Double.POSITIVE_INFINITY;
      tagMax[d] = Double.NEGATIVE_INFINITY;
    }
    for (int i = 0; i < count; i++) {
      final int d = indexOf(dict, dictSize, tags[i]);
      tagCount[d]++;
      if (values[i] < tagMin[d]) tagMin[d] = values[i];
      if (values[i] > tagMax[d]) tagMax[d] = values[i];
    }
    int running = 0;
    for (int d = 0; d < dictSize; d++) {
      tagStart[d] = running;
      running += tagCount[d];
    }

    final byte[] out = new byte[FIXED_BYTES + dictSize * BYTES_PER_TAG + count * Double.BYTES];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(VERSION_V1);
    bb.put(tagKind);
    bb.putInt(count);
    bb.putInt(dictSize);
    for (int d = 0; d < dictSize; d++) bb.putInt(dict[d]);
    for (int d = 0; d < dictSize; d++) bb.putInt(tagStart[d]);
    for (int d = 0; d < dictSize; d++) bb.putInt(tagCount[d]);
    for (int d = 0; d < dictSize; d++) bb.putDouble(tagMin[d]);
    for (int d = 0; d < dictSize; d++) bb.putDouble(tagMax[d]);
    // Scatter values to their tag's range, keeping page order within the tag.
    final int[] cursor = new int[dictSize];
    final int valuesBase = bb.position();
    for (int i = 0; i < count; i++) {
      final int d = indexOf(dict, dictSize, tags[i]);
      bb.putDouble(valuesBase + (tagStart[d] + cursor[d]++) * Double.BYTES, values[i]);
    }
    return out;
  }

  private static int indexOf(final int[] dict, final int dictSize, final int tag) {
    for (int d = 0; d < dictSize; d++) {
      if (dict[d] == tag) {
        return d;
      }
    }
    throw new IllegalStateException("tag " + tag + " missing from the dictionary just built");
  }
}

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
import java.util.Arrays;

/**
 * The per-tag min/max of {@link NumberRegion}, lifted out of the compressed payload into a region
 * of its own.
 *
 * <h2>The problem it solves</h2>
 *
 * <p>
 * {@link NumberRegion} already stores per-tag zone maps, and a range predicate already uses them to
 * answer a whole page without comparing a single value. But those zone maps live in the number
 * region's <em>header</em>, and the number region is LZ77-compressed on the wire. So reading them
 * meant decompressing the very payload the zone map exists to avoid reading. The pruning was real;
 * it just arrived one step too late to save the expensive part.
 *
 * <p>
 * That is exactly backwards for the case pruning matters most in — a cold or larger-than-memory
 * working set, where the cost of a page is dominated by fetching and decompressing it rather than
 * by scanning it. A predicate that can rule a page out ought to cost a couple of comparisons
 * against header longs and nothing else.
 *
 * <p>
 * So the zone maps get their own region and are written first. A scan reads this one and leaves
 * {@link RegionTable#KIND_NUMBER} on the wire, materializing it only on the pages it could not
 * decide. Narrow-schema maps stay raw. A wide-schema map may independently elect the region table's
 * bounded LZ77 envelope, so reading it decodes only the summary rather than the larger number
 * column. It is the same trick {@link StringDictSketch} plays for string equality, where a Bloom
 * filter over the dictionary settles most pages without the dictionary being decompressed at all.
 *
 * <h2>Wire format</h2>
 *
 * <pre>
 * byte           version      // VERSION_V1
 * byte           tagKind      // mirrors NumberRegion's tagKind; nameKey- vs pathNodeKey-tagged
 * long           valueMin     // page-global bounds, for a predicate that rules out every tag
 * long           valueMax
 * int            dictSize
 * int[dictSize]  dict         // tag values, in the number region's tag-id order
 * int[dictSize]  tagCount     // values per tag — needed to answer "all of them match"
 * long[dictSize] tagMin
 * long[dictSize] tagMax
 * </pre>
 *
 * <p>
 * No {@code tagStart}: it addresses into the value bytes, and a caller that has got as far as
 * needing an offset has already decided to materialize the number region, where the real header
 * carries it. This region holds only what a pruning decision needs. Narrow maps stay raw; wide maps
 * may independently elect the region table's bounded LZ77 envelope and are decoded without
 * materializing the number column.
 *
 * <p>
 * At 22 bytes of header plus 24 per tag, a page with three numeric fields spends 94 bytes to
 * potentially skip decompressing a payload measured in kilobytes. Wide maps are sufficiently large
 * that {@link RegionTable} may compress this logical V1 payload on disk; parsing still sees these
 * exact bytes after the per-region envelope is decoded.
 *
 * <h2>Compatibility</h2>
 *
 * <p>
 * Both directions are safe without a format version bump. A reader that predates this region sees a
 * kind ordinal at or above its own {@code KIND_COUNT} and steps over it by its length prefix, which
 * is the same path it takes for any region it was not asked for. A reader that knows this region
 * and meets a page written without one simply finds it absent and falls back to the number region's
 * own zone maps — the behaviour it had before this class existed.
 */
public final class NumberZoneMapRegion {

  /** Maximum tags a 1024-slot leaf page normally contains; used only to pre-size scratch. */
  private static final int PAGE_MAX_DICT_SIZE = 1024;

  /** Returned by {@link #encodeInto} when the source has no zone map to publish. */
  public static final int ENCODE_FAILED = -1;

  /** Fixed-width wire format: four-byte tags and counts, eight-byte bounds. */
  public static final byte VERSION_V1 = 1;

  /**
   * Varint wire format. The same four numbers per tag — tag value, count, minimum, and the maximum as
   * a spread above the minimum — written as LEB128 deltas instead of fixed 24 bytes. A page-wide
   * bound is no longer written at all: it is the fold of the per-tag bounds.
   *
   * <p>
   * This is the same information the {@link NumberRegion#ENC_PER_TAG_FOR} header carries, in the same
   * order, because the two are the same summary: the per-tag frame of reference IS the zone map. It
   * stays a region of its own only so a predicate can rule a page out without decompressing the
   * values it would otherwise have to reach through.
   */
  public static final byte VERSION_V2 = 2;

  /** Fixed bytes before the per-tag arrays. */
  private static final int FIXED_BYTES = 1 + 1 + 8 + 8 + 4;

  /** Bytes each tag contributes: dict + count + min + max. */
  private static final int BYTES_PER_TAG = 4 + 4 + 8 + 8;

  /** Fixed bytes before the per-tag entries in {@link #VERSION_V2}: version and tag kind. */
  private static final int V2_FIXED_BYTES = 1 + 1;

  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private NumberZoneMapRegion() {}

  /** Parsed zone-map region. Reused across pages to keep the scan allocation-free. */
  public static final class Header {
    public byte version;
    /** {@link NumberRegion#TAG_KIND_NAME} or {@link NumberRegion#TAG_KIND_PATH_NODE}. */
    public byte tagKind;
    /** Lowest value anywhere on the page. */
    public long valueMin;
    /** Highest value anywhere on the page. */
    public long valueMax;
    public int dictSize;
    public int[] dict; // length >= dictSize
    public int[] tagCount; // length >= dictSize
    public long[] tagMin; // length >= dictSize
    public long[] tagMax; // length >= dictSize

    /**
     * Parse {@code payload} into this instance.
     *
     * @return {@code this}, or {@code null} when the payload is absent, truncated, or written by a
     *         future version — all of which mean "no zone map available", never a wrong answer
     */
    public Header parseInto(final MemorySegment payload) {
      // The version byte decides the minimum: a varint map with one tag is smaller than the fixed
      // header alone, so the fixed-width minimum can only be applied after the version is known.
      if (payload == null || payload.byteSize() < V2_FIXED_BYTES) {
        return null;
      }
      // Everything is read into locals first and only committed to the reusable fields once the
      // payload has fully checked out. Assigning as we go left this scratch holding one page's
      // dictSize beside the previous page's bounds arrays whenever a parse declined — a mixture
      // that reads as valid and prunes against another page's min/max.
      final RegionReader in = new RegionReader(payload);
      final byte readVersion = in.readByte();
      if (readVersion == VERSION_V2) {
        return parseV2(payload);
      }
      if (readVersion != VERSION_V1 || payload.byteSize() < FIXED_BYTES) {
        // A newer writer may have changed the layout. Declining is always safe: the caller falls
        // back to the number region's own zone maps.
        return null;
      }
      final byte readTagKind = in.readByte();
      final long readMin = in.readLong();
      final long readMax = in.readLong();
      final int readDictSize = in.readInt();
      if (readDictSize < 0 || payload.byteSize() < (long) FIXED_BYTES + (long) readDictSize * BYTES_PER_TAG) {
        return null;
      }
      version = readVersion;
      tagKind = readTagKind;
      valueMin = readMin;
      valueMax = readMax;
      dictSize = readDictSize;
      if (dict == null || dict.length < dictSize) {
        dict = new int[Math.max(4, dictSize)];
      }
      if (tagCount == null || tagCount.length < dictSize) {
        tagCount = new int[Math.max(4, dictSize)];
      }
      if (tagMin == null || tagMin.length < dictSize) {
        tagMin = new long[Math.max(4, dictSize)];
      }
      if (tagMax == null || tagMax.length < dictSize) {
        tagMax = new long[Math.max(4, dictSize)];
      }
      in.readInts(dict, dictSize);
      in.readInts(tagCount, dictSize);
      in.readLongs(tagMin, dictSize);
      in.readLongs(tagMax, dictSize);
      return this;
    }

    /**
     * Parse a {@link #VERSION_V2} payload. Like the fixed-width form, nothing is committed to the
     * reusable fields until the whole payload has been read, so a declined parse cannot leave one
     * page's dictionary beside another page's bounds.
     *
     * @return {@code this}, or {@code null} when the payload is truncated or malformed
     */
    private Header parseV2(final MemorySegment payload) {
      // Invalidated up front: a caller that ignores a null return then prunes against nothing
      // rather than against a mixture of this page's dictionary and the last page's bounds.
      dictSize = 0;
      final long size = payload.byteSize();
      long position = V2_FIXED_BYTES;
      final byte readTagKind = payload.get(ValueLayout.JAVA_BYTE, 1L);
      final int readDictSize;
      try {
        final long declared = VarInt.readUnsigned(payload, position);
        position += VarInt.sizeOfUnsigned(declared);
        if (declared < 0L || declared > Integer.MAX_VALUE) {
          return null;
        }
        readDictSize = (int) declared;
        if (dict == null || dict.length < readDictSize) {
          dict = new int[Math.max(4, readDictSize)];
        }
        if (tagCount == null || tagCount.length < readDictSize) {
          tagCount = new int[Math.max(4, readDictSize)];
        }
        if (tagMin == null || tagMin.length < readDictSize) {
          tagMin = new long[Math.max(4, readDictSize)];
        }
        if (tagMax == null || tagMax.length < readDictSize) {
          tagMax = new long[Math.max(4, readDictSize)];
        }
        int previousTag = 0;
        long readMin = Long.MAX_VALUE;
        long readMax = Long.MIN_VALUE;
        for (int i = 0; i < readDictSize; i++) {
          if (position >= size) {
            return null;
          }
          final long tagDelta = VarInt.readSigned(payload, position);
          position += VarInt.sizeOfSigned(tagDelta);
          previousTag = (int) (previousTag + tagDelta);
          dict[i] = previousTag;
          final long values = VarInt.readUnsigned(payload, position);
          position += VarInt.sizeOfUnsigned(values);
          if (values < 0L || values > Integer.MAX_VALUE) {
            return null;
          }
          tagCount[i] = (int) values;
          final long min = VarInt.readSigned(payload, position);
          position += VarInt.sizeOfSigned(min);
          final long spread = VarInt.readUnsigned(payload, position);
          position += VarInt.sizeOfUnsigned(spread);
          tagMin[i] = min;
          tagMax[i] = min + spread;
          if (min < readMin) {
            readMin = min;
          }
          if (tagMax[i] > readMax) {
            readMax = tagMax[i];
          }
        }
        if (position > size) {
          return null;
        }
        version = VERSION_V2;
        tagKind = readTagKind;
        dictSize = readDictSize;
        valueMin = readDictSize == 0
            ? 0L
            : readMin;
        valueMax = readDictSize == 0
            ? 0L
            : readMax;
        return this;
      } catch (final IllegalArgumentException malformed) {
        // A corrupt varint is a region that cannot be trusted to prune. Declining sends the caller
        // to the number region's own bounds, which is the behaviour for an absent map.
        return null;
      }
    }
  }

  /**
   * Fill a {@link NumberRegion.Header}'s per-tag directory straight from this region.
   *
   * <p>
   * This is the read side of {@link NumberRegion#ENC_PER_TAG_FOR_EXTERNAL}: the summary IS that
   * region's header, so the number region's parse reads it here rather than from a second copy of its
   * own. Written into the target's arrays directly — no intermediate header, no allocation on a parse
   * path that runs once per page.
   *
   * <p>
   * Unlike {@link Header#parseInto} this does not decline: a value region that says its directory is
   * external cannot be read without one, so a malformed or truncated summary is an exception rather
   * than a fallback. The caller decides whether the summary is ABSENT before getting here.
   *
   * @return the directory's {@code tagKind}, for the caller to check against the value region's
   */
  static byte readDirectoryInto(final MemorySegment payload, final NumberRegion.Header target) {
    if (payload == null || payload.byteSize() < V2_FIXED_BYTES) {
      throw new IllegalArgumentException("the number region's external directory is absent or truncated");
    }
    final byte readVersion = payload.get(ValueLayout.JAVA_BYTE, 0L);
    final byte readTagKind = payload.get(ValueLayout.JAVA_BYTE, 1L);
    final int size;
    long position;
    final boolean varint = readVersion == VERSION_V2;
    if (varint) {
      position = V2_FIXED_BYTES;
      final long declared = VarInt.readUnsigned(payload, position);
      position += VarInt.sizeOfUnsigned(declared);
      if (declared < 0L || declared > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("external directory declares dictSize=" + declared);
      }
      size = (int) declared;
    } else if (readVersion == VERSION_V1) {
      if (payload.byteSize() < FIXED_BYTES) {
        throw new IllegalArgumentException("the number region's external directory is truncated");
      }
      size = payload.get(LE.INT, 18L);
      if (size < 0 || payload.byteSize() < (long) FIXED_BYTES + (long) size * BYTES_PER_TAG) {
        throw new IllegalArgumentException("external directory declares dictSize=" + size);
      }
      position = FIXED_BYTES;
    } else {
      throw new IllegalArgumentException("unknown external directory version " + readVersion);
    }

    target.acceptDirectory(size);
    int previousTag = 0;
    int running = 0;
    long globalMin = Long.MAX_VALUE;
    long globalMax = Long.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      final int tag;
      final int values;
      final long min;
      final long max;
      if (varint) {
        final long tagDelta = VarInt.readSigned(payload, position);
        position += VarInt.sizeOfSigned(tagDelta);
        previousTag = (int) (previousTag + tagDelta);
        tag = previousTag;
        final long declaredValues = VarInt.readUnsigned(payload, position);
        position += VarInt.sizeOfUnsigned(declaredValues);
        if (declaredValues < 0L || declaredValues > Integer.MAX_VALUE - running) {
          throw new IllegalArgumentException("external directory tag " + i + " declares " + declaredValues + " values");
        }
        values = (int) declaredValues;
        min = VarInt.readSigned(payload, position);
        position += VarInt.sizeOfSigned(min);
        final long spread = VarInt.readUnsigned(payload, position);
        position += VarInt.sizeOfUnsigned(spread);
        max = min + spread;
      } else {
        // V1 keeps four parallel arrays: dict, tagCount, tagMin, tagMax.
        tag = payload.get(LE.INT, FIXED_BYTES + (long) i * Integer.BYTES);
        values = payload.get(LE.INT, FIXED_BYTES + (long) size * Integer.BYTES + (long) i * Integer.BYTES);
        final long boundsBase = FIXED_BYTES + 2L * size * Integer.BYTES;
        min = payload.get(LE.LONG, boundsBase + (long) i * Long.BYTES);
        max = payload.get(LE.LONG, boundsBase + (long) size * Long.BYTES + (long) i * Long.BYTES);
        if (values < 0 || values > Integer.MAX_VALUE - running) {
          throw new IllegalArgumentException("external directory tag " + i + " declares " + values + " values");
        }
      }
      target.dict[i] = tag;
      target.tagCount[i] = values;
      target.tagStart[i] = running;
      running += values;
      target.tagMin[i] = min;
      target.tagMax[i] = max;
      if (min < globalMin) {
        globalMin = min;
      }
      if (max > globalMax) {
        globalMax = max;
      }
    }
    target.count = running;
    target.valueMin = size == 0
        ? 0L
        : globalMin;
    target.valueMax = size == 0
        ? 0L
        : globalMax;
    return readTagKind;
  }

  /**
   * Local tag id for a tag value, or {@code -1} when this page carries no such tag.
   *
   * <p>
   * The tag value is interpreted per {@link Header#tagKind}, and callers must probe with a key from
   * the matching space: a nameKey against a path-tagged dictionary can collide on an unrelated int,
   * and a collision here would not merely fail to prune — it would prune against the bounds of a
   * different column.
   */
  public static int lookupTag(final Header h, final int tag) {
    if (h == null) {
      // parseInto returns null for an absent or unreadable region, and passing that straight
      // through is the expected mistake; answering "no such tag" is the safe reading of it.
      return -1;
    }
    for (int i = 0; i < h.dictSize; i++) {
      if (h.dict[i] == tag) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Build the payload from a parsed {@link NumberRegion.Header}.
   *
   * @return the payload bytes, or {@code null} when the number region carries no per-tag zone map
   *         (the legacy {@code ENC_PLAIN_LONG} / {@code ENC_BIT_PACKED} kinds) or has no tags, in
   *         which case there is nothing worth writing
   */
  public static byte[] encode(final NumberRegion.Header source) {
    if (!encodable(source)) {
      return null;
    }
    final int encodedLength = encodedSize(source.dictSize);
    final byte[] retainedScratch = ENCODE_SCRATCH.get();
    // The compatibility API accepts headers larger than a page. Keep that behavior without letting
    // one exceptional call pin an unbounded byte array on a long-lived serializer thread.
    final byte[] scratch = retainedScratch.length >= encodedLength
        ? retainedScratch
        : new byte[encodedLength];
    final int written = encodeInto(source, scratch);
    return written == ENCODE_FAILED
        ? null
        : Arrays.copyOf(scratch, written);
  }

  /**
   * Encode into caller-owned reusable storage.
   *
   * <p>
   * The returned prefix is valid only until {@code out} is reused. A retaining caller must copy it
   * first; {@link RegionTable#set(byte, byte[], int)} performs that copy synchronously.
   *
   * @return bytes written, or {@link #ENCODE_FAILED} when the source carries no per-tag zone map
   */
  public static int encodeInto(final NumberRegion.Header source, final byte[] out) {
    if (out == null) {
      throw new NullPointerException("out must be non-null");
    }
    if (!encodable(source)) {
      return ENCODE_FAILED;
    }
    final int dictSize = source.dictSize;
    requireArrayLength("dict", source.dict, dictSize);
    requireArrayLength("tagCount", source.tagCount, dictSize);
    requireArrayLength("tagMin", source.tagMin, dictSize);
    requireArrayLength("tagMax", source.tagMax, dictSize);

    final int encodedLength = encodedSize(dictSize);
    if (out.length < encodedLength) {
      throw new IllegalArgumentException("output too small: " + out.length + " bytes for " + encodedLength);
    }

    // The varint form is elected on measured bytes and only when it is strictly smaller, so a map
    // of scattered 64-bit bounds — where ten-byte varints would cost more than the fixed widths —
    // keeps the fixed form. That also bounds the varint form by the fixed size the buffer was
    // guaranteed for, so the election never needs a larger output than the caller provided.
    final int varintLength = varintEncodedSize(source, dictSize);
    if (varintLength < encodedLength && NumberRegion.perTagWidthEnabled()) {
      return encodeVarint(source, dictSize, out);
    }

    int offset = 0;
    out[offset++] = VERSION_V1;
    out[offset++] = source.tagKind;
    LONG_LE.set(out, offset, source.valueMin);
    offset += Long.BYTES;
    LONG_LE.set(out, offset, source.valueMax);
    offset += Long.BYTES;
    INT_LE.set(out, offset, dictSize);
    offset += Integer.BYTES;
    for (int i = 0; i < dictSize; i++) {
      INT_LE.set(out, offset, source.dict[i]);
      offset += Integer.BYTES;
    }
    for (int i = 0; i < dictSize; i++) {
      INT_LE.set(out, offset, source.tagCount[i]);
      offset += Integer.BYTES;
    }
    for (int i = 0; i < dictSize; i++) {
      LONG_LE.set(out, offset, source.tagMin[i]);
      offset += Long.BYTES;
    }
    for (int i = 0; i < dictSize; i++) {
      LONG_LE.set(out, offset, source.tagMax[i]);
      offset += Long.BYTES;
    }
    return offset;
  }

  /** Exact bytes {@link #encodeVarint} would write for this source. */
  private static int varintEncodedSize(final NumberRegion.Header source, final int dictSize) {
    long bytes = V2_FIXED_BYTES + VarInt.sizeOfUnsigned(dictSize);
    int previousTag = 0;
    for (int i = 0; i < dictSize; i++) {
      bytes += VarInt.sizeOfSigned((long) source.dict[i] - previousTag);
      previousTag = source.dict[i];
      bytes += VarInt.sizeOfUnsigned(source.tagCount[i]);
      bytes += VarInt.sizeOfSigned(source.tagMin[i]);
      bytes += VarInt.sizeOfUnsigned(source.tagMax[i] - source.tagMin[i]);
    }
    // Bounded by the fixed form, which the caller's buffer is already sized for, so this cannot
    // overflow an int on any input the fixed form accepts.
    return (int) bytes;
  }

  /** Write the {@link #VERSION_V2} form; the caller has checked that it fits. */
  private static int encodeVarint(final NumberRegion.Header source, final int dictSize, final byte[] out) {
    int offset = 0;
    out[offset++] = VERSION_V2;
    out[offset++] = source.tagKind;
    offset = VarInt.writeUnsigned(out, offset, dictSize);
    int previousTag = 0;
    for (int i = 0; i < dictSize; i++) {
      offset = VarInt.writeSigned(out, offset, (long) source.dict[i] - previousTag);
      previousTag = source.dict[i];
      offset = VarInt.writeUnsigned(out, offset, source.tagCount[i]);
      offset = VarInt.writeSigned(out, offset, source.tagMin[i]);
      offset = VarInt.writeUnsigned(out, offset, source.tagMax[i] - source.tagMin[i]);
    }
    return offset;
  }

  private static boolean encodable(final NumberRegion.Header source) {
    if (source == null || source.tagMin == null || source.tagMax == null || source.dictSize <= 0) {
      return false;
    }
    return true;
  }

  private static void requireArrayLength(final String name, final int[] values, final int length) {
    if (values == null || values.length < length) {
      throw new IllegalArgumentException(name + " has length " + (values == null
          ? 0
          : values.length) + ", expected at least " + length);
    }
  }

  private static void requireArrayLength(final String name, final long[] values, final int length) {
    if (values == null || values.length < length) {
      throw new IllegalArgumentException(name + " has length " + (values == null
          ? 0
          : values.length) + ", expected at least " + length);
    }
  }

  /** Exact encoded size for {@code dictSize} tags, for sizing assertions and diagnostics. */
  public static int encodedSize(final int dictSize) {
    if (dictSize < 0) {
      throw new IllegalArgumentException("dictSize=" + dictSize);
    }
    return FIXED_BYTES + dictSize * BYTES_PER_TAG;
  }

  private static final ThreadLocal<byte[]> ENCODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[encodedSize(PAGE_MAX_DICT_SIZE)]);
}

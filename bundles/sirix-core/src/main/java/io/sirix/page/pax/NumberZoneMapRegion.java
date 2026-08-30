/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
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

  /** Current wire format version. */
  public static final byte VERSION_V1 = 1;

  /** Fixed bytes before the per-tag arrays. */
  private static final int FIXED_BYTES = 1 + 1 + 8 + 8 + 4;

  /** Bytes each tag contributes: dict + count + min + max. */
  private static final int BYTES_PER_TAG = 4 + 4 + 8 + 8;

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
      if (payload == null || payload.byteSize() < FIXED_BYTES) {
        return null;
      }
      // Everything is read into locals first and only committed to the reusable fields once the
      // payload has fully checked out. Assigning as we go left this scratch holding one page's
      // dictSize beside the previous page's bounds arrays whenever a parse declined — a mixture
      // that reads as valid and prunes against another page's min/max.
      final RegionReader in = new RegionReader(payload);
      final byte readVersion = in.readByte();
      if (readVersion != VERSION_V1) {
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

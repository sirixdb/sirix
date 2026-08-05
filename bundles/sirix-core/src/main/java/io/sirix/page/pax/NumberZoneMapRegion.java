/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The per-tag min/max of {@link NumberRegion}, lifted out of the compressed payload into a region
 * of its own.
 *
 * <h2>The problem it solves</h2>
 *
 * <p>{@link NumberRegion} already stores per-tag zone maps, and a range predicate already uses them
 * to answer a whole page without comparing a single value. But those zone maps live in the number
 * region's <em>header</em>, and the number region is LZ77-compressed on the wire. So reading them
 * meant decompressing the very payload the zone map exists to avoid reading. The pruning was real;
 * it just arrived one step too late to save the expensive part.
 *
 * <p>That is exactly backwards for the case pruning matters most in — a cold or larger-than-memory
 * working set, where the cost of a page is dominated by fetching and decompressing it rather than
 * by scanning it. A predicate that can rule a page out ought to cost a couple of comparisons
 * against header longs and nothing else.
 *
 * <p>So the zone maps get their own region: small, always stored raw, and written first. A scan
 * reads this one and leaves {@link RegionTable#KIND_NUMBER} on the wire, decompressing it only on
 * the pages it could not decide. It is the same trick {@link StringDictSketch} plays for string
 * equality, where a Bloom filter over the dictionary settles most pages without the dictionary
 * being decompressed at all.
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
 * <p>No {@code tagStart}: it addresses into the value bytes, and a caller that has got as far as
 * needing an offset has already decided to materialize the number region, where the real header
 * carries it. This region holds only what a pruning decision needs, because every byte of it is
 * stored uncompressed and is read on pages that turn out not to need it.
 *
 * <p>At 22 bytes of header plus 24 per tag, a page with three numeric fields spends 94 bytes to
 * potentially skip decompressing a payload measured in kilobytes.
 *
 * <h2>Compatibility</h2>
 *
 * <p>Both directions are safe without a format version bump. A reader that predates this region
 * sees a kind ordinal at or above its own {@code KIND_COUNT} and steps over it by its length
 * prefix, which is the same path it takes for any region it was not asked for. A reader that knows
 * this region and meets a page written without one simply finds it absent and falls back to the
 * number region's own zone maps — the behaviour it had before this class existed.
 */
public final class NumberZoneMapRegion {

  /** Current wire format version. */
  public static final byte VERSION_V1 = 1;

  /** Fixed bytes before the per-tag arrays. */
  private static final int FIXED_BYTES = 1 + 1 + 8 + 8 + 4;

  /** Bytes each tag contributes: dict + count + min + max. */
  private static final int BYTES_PER_TAG = 4 + 4 + 8 + 8;

  private NumberZoneMapRegion() {
  }

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
    public int[] dict;      // length >= dictSize
    public int[] tagCount;  // length >= dictSize
    public long[] tagMin;   // length >= dictSize
    public long[] tagMax;   // length >= dictSize

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
      if (readDictSize < 0
          || payload.byteSize() < (long) FIXED_BYTES + (long) readDictSize * BYTES_PER_TAG) {
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
   * <p>The tag value is interpreted per {@link Header#tagKind}, and callers must probe with a key
   * from the matching space: a nameKey against a path-tagged dictionary can collide on an unrelated
   * int, and a collision here would not merely fail to prune — it would prune against the bounds of
   * a different column.
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
    if (source == null || source.tagMin == null || source.tagMax == null || source.dictSize <= 0) {
      return null;
    }
    final int dictSize = source.dictSize;
    final byte[] out = new byte[FIXED_BYTES + dictSize * BYTES_PER_TAG];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(VERSION_V1);
    bb.put(source.tagKind);
    bb.putLong(source.valueMin);
    bb.putLong(source.valueMax);
    bb.putInt(dictSize);
    for (int i = 0; i < dictSize; i++) {
      bb.putInt(source.dict[i]);
    }
    for (int i = 0; i < dictSize; i++) {
      bb.putInt(source.tagCount[i]);
    }
    for (int i = 0; i < dictSize; i++) {
      bb.putLong(source.tagMin[i]);
    }
    for (int i = 0; i < dictSize; i++) {
      bb.putLong(source.tagMax[i]);
    }
    return out;
  }

  /** Exact encoded size for {@code dictSize} tags, for sizing assertions and diagnostics. */
  public static int encodedSize(final int dictSize) {
    if (dictSize < 0) {
      throw new IllegalArgumentException("dictSize=" + dictSize);
    }
    return FIXED_BYTES + dictSize * BYTES_PER_TAG;
  }
}

package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
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
   * Value-bytes encoded via {@link NumberRegionCompact} (FOR+BP with its own
   * embedded header: version + bitWidth + varint count + minValue + body).
   * Outer tag dict + zone maps remain intact. Written when wiring is enabled;
   * reader supports all four encoding kinds for backward compatibility.
   */
  public static final byte ENC_COMPACT_ZM = 4;

  /**
   * Value-bytes encoded via {@link NumberRegionDelta} (delta-of-delta / zig-zag
   * bit-pack). Outer tag dict + zone maps are laid out exactly like
   * {@link #ENC_COMPACT_ZM} (no outer {@code valueBase}/{@code valueBitWidth} —
   * those live in the nested delta header). Chosen automatically when it
   * produces a strictly smaller value region than FOR+BP, which is the case for
   * temporal columns (commit timestamps, valid-time, monotonic ids).
   *
   * <p>Delta decode is sequential, so payloads under this encoding are excluded
   * from the SIMD scan kernels (they fall back to the scalar
   * {@link #decodeValueAt} / {@link #decodeAllValues} loop).
   */
  public static final byte ENC_DELTA_ZM = 5;

  /**
   * Write {@link #ENC_COMPACT_ZM} instead of {@link #ENC_BIT_PACKED_ZM} on the
   * bit-packed path when enabled. Off by default because the compact codec
   * adds ~2-7 bytes/region of framing overhead (version + varint) without a
   * proportional speedup on Sirix's cold-path. Flip to test.
   *
   * <p>Read volatile-per-call so tests can toggle without class reload. The
   * system property fallback lets production JVMs pin a value at startup.
   */
  private static volatile Boolean COMPACT_WRITE_OVERRIDE = null;

  /** Test hook: enable/disable compact-ZM writes without restarting the JVM. */
  public static void setCompactWriteEnabled(final boolean enabled) {
    COMPACT_WRITE_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the system property. */
  public static void clearCompactWriteOverride() {
    COMPACT_WRITE_OVERRIDE = null;
  }

  private static boolean compactWriteEnabled() {
    final Boolean ov = COMPACT_WRITE_OVERRIDE;
    if (ov != null) {
      return ov;
    }
    return Boolean.getBoolean("sirix.numberRegion.compactWrite");
  }

  /**
   * Enable/disable the delta-of-delta ({@link #ENC_DELTA_ZM}) write path. When
   * enabled (the default) the encoder emits delta whenever it yields a strictly
   * smaller value region than FOR+BP; when disabled the encoder never writes
   * delta (readers still decode existing delta payloads). Toggled without a
   * class reload for A/B tests.
   */
  private static volatile Boolean DELTA_WRITE_OVERRIDE = null;

  /** Test hook: force-enable/disable delta-ZM writes without restarting the JVM. */
  public static void setDeltaWriteEnabled(final boolean enabled) {
    DELTA_WRITE_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the default (enabled). */
  public static void clearDeltaWriteOverride() {
    DELTA_WRITE_OVERRIDE = null;
  }

  private static boolean deltaWriteEnabled() {
    final Boolean ov = DELTA_WRITE_OVERRIDE;
    if (ov != null) {
      return ov;
    }
    // Default ON — the size bake-off only picks delta when it actually wins.
    return !Boolean.getBoolean("sirix.numberRegion.deltaWrite.disabled");
  }

  /** Minimum value count before delta is even considered (avoids churn on tiny pages). */
  private static final int MIN_DELTA_COUNT = 3;

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

  /**
   * @return true iff the value bytes are FOR + bit-packed and directly
   *         random-accessible / SIMD-scannable. {@link #ENC_COMPACT_ZM} counts
   *         (bit-packed under an embedded header); {@link #ENC_DELTA_ZM} does
   *         <em>not</em> — delta is sequential and must never be routed through
   *         a FOR unpack loop.
   */
  public static boolean isBitPacked(final byte encodingKind) {
    return encodingKind == ENC_BIT_PACKED
        || encodingKind == ENC_BIT_PACKED_ZM
        || encodingKind == ENC_COMPACT_ZM;
  }

  /** @return true iff {@code encodingKind == ENC_COMPACT_ZM}. */
  public static boolean isCompact(final byte encodingKind) {
    return encodingKind == ENC_COMPACT_ZM;
  }

  /** @return true iff {@code encodingKind == ENC_DELTA_ZM} (delta-of-delta). */
  public static boolean isDelta(final byte encodingKind) {
    return encodingKind == ENC_DELTA_ZM;
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
    /**
     * Frame-of-reference base for bit-packed encodings. Populated from the
     * outer header for {@link #ENC_BIT_PACKED_ZM}; populated from the nested
     * compact header for {@link #ENC_COMPACT_ZM}.
     */
    public long valueBase;
    /**
     * Bits per value for bit-packed encodings. For {@link #ENC_PLAIN_LONG_ZM}
     * this is 64. For {@link #ENC_COMPACT_ZM} this is taken from the nested
     * compact header (so constant-run encodings surface as 0 here too).
     */
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
    /**
     * Nested delta header. Populated only for {@link #ENC_DELTA_ZM}; null
     * otherwise. Carries {@code firstValue}/{@code firstDelta}/{@code bodyOffset}
     * needed to replay the delta prefix sum.
     */
    public NumberRegionDelta.Header deltaHeader;

    public Header parseInto(final byte[] payload) {
      final ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
      encodingKind = bb.get();
      tagKind = bb.get();
      count = bb.getInt();
      valueMin = bb.getLong();
      valueMax = bb.getLong();
      if (encodingKind == ENC_COMPACT_ZM || encodingKind == ENC_DELTA_ZM) {
        // Compact-ZM / Delta-ZM: no outer valueBase/valueBitWidth — those live
        // inside the nested codec header which precedes the body.
        valueBase = 0L;
        valueBitWidth = 0;
      } else {
        valueBase = bb.getLong();
        valueBitWidth = bb.get();
      }
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
      if (encodingKind == ENC_COMPACT_ZM) {
        // Parse the nested compact header. Populate valueBase/valueBitWidth
        // so existing decoders can treat compact-ZM uniformly. Set
        // valueBytesOffset to point at the compact body (not the compact
        // header) — decodeValueAt adjusts via the compact codec's bit
        // arithmetic.
        final int compactHeaderOff = bb.position();
        final MemorySegment segView = MemorySegment.ofArray(payload);
        final NumberRegionCompact.Header compactH = new NumberRegionCompact.Header();
        NumberRegionCompact.readHeader(segView, compactHeaderOff, compactH);
        valueBase = compactH.minValue;
        valueBitWidth = compactH.bitWidth;
        valueBytesOffset = (int) compactH.bodyOffset;
        valueBytesLength = (int) compactH.bodyBytes;
        deltaHeader = null; // defensive: never leave a stale delta header on a reused Header
      } else if (encodingKind == ENC_DELTA_ZM) {
        // Parse the nested delta header. valueBytesOffset points at the delta
        // body; decode goes through NumberRegionDelta, not the FOR unpack path.
        final int deltaHeaderOff = bb.position();
        final MemorySegment segView = MemorySegment.ofArray(payload);
        if (deltaHeader == null) {
          deltaHeader = new NumberRegionDelta.Header();
        }
        NumberRegionDelta.readHeader(segView, deltaHeaderOff, deltaHeader);
        valueBase = 0L;
        valueBitWidth = deltaHeader.bitWidth;
        valueBytesOffset = (int) deltaHeader.bodyOffset;
        valueBytesLength = (int) deltaHeader.bodyBytes;
      } else {
        deltaHeader = null;
        valueBytesOffset = bb.position();
        valueBytesLength = bitsToBytes((long) count * (isBitPacked(encodingKind) ? valueBitWidth : 64));
      }
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

    // Delta-of-delta bake-off. Two conditions must both hold:
    //   1. Structure: the delta-of-delta residual width is *strictly* narrower
    //      than the FOR/plain per-value width. This is what separates a genuine
    //      temporal column (near-constant stride ⇒ tiny residuals) from random
    //      data, where delta-of-delta is as wide or wider. Without it a wide
    //      random column would switch to delta to shave a few header bytes —
    //      a bad trade, since delta forfeits SIMD scans and O(1) random access.
    //   2. Size: the delta value region is actually smaller (guards the small-
    //      count case where the two 8-byte anchors outweigh the width saving).
    // The outer header (dict, tag directory, zone maps) is identical across
    // ENC_BIT_PACKED_ZM/ENC_DELTA_ZM, so only the value regions are compared:
    // FOR+BP writes 9 outer bytes (valueBase + width) plus the packed body;
    // delta writes its self-describing nested payload.
    if (deltaWriteEnabled() && count >= MIN_DELTA_COUNT) {
      final int forBitWidth = bitPacked ? Math.max(1, 64 - Long.numberOfLeadingZeros(spread)) : 64;
      final int deltaBitWidth = NumberRegionDelta.computeBitWidth(sortedValues, count);
      if (deltaBitWidth < forBitWidth) {
        final long forValueRegionBytes = 9L + bitsToBytes((long) count * forBitWidth);
        final long deltaRegionBytes = NumberRegionDelta.headerBytes(count)
            + NumberRegionDelta.bodyBytes(count, deltaBitWidth);
        if (deltaRegionBytes < forValueRegionBytes) {
          return encodeDeltaZM(sortedValues, count, dict, dictSize, tagStart, tagCount,
              tagMin, tagMax, min, max, tagKind);
        }
      }
    }

    if (bitPacked && compactWriteEnabled()) {
      return encodeCompactZM(sortedValues, count, dict, dictSize, tagStart, tagCount,
          tagMin, tagMax, min, max, tagKind);
    }
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

  /**
   * Build an {@link #ENC_COMPACT_ZM} payload. Outer layout is:
   * <pre>
   * byte encodingKind(4), byte tagKind, int count,
   * long valueMin, long valueMax,
   * int dictSize, int[dictSize] dict, int[dictSize] tagStart,
   * int[dictSize] tagCount, long[dictSize] tagMin, long[dictSize] tagMax,
   * NumberRegionCompact payload (its own header: version, bitWidth,
   * varint count, minValue, bit-packed body).
   * </pre>
   *
   * <p>Trade-off vs {@link #ENC_BIT_PACKED_ZM}: saves 8 B (no outer valueBase)
   * + 1 B (no outer valueBitWidth) = 9 B. Adds compact header: 1 (version) +
   * 1 (bitWidth) + varint(count) + 8 (minValue) = ~11 B. Net: ~+2 B per
   * region. Potential win is a cleaner scan decoder that handles constant-run
   * ({@code bitWidth == 0}) branchlessly via the compact API, which matters
   * when value scans dominate cold CPU.
   */
  private static byte[] encodeCompactZM(final long[] sortedValues, final int count,
      final int[] dict, final int dictSize, final int[] tagStart, final int[] tagCount,
      final long[] tagMin, final long[] tagMax, final long min, final long max,
      final byte tagKind) {
    // Pre-size: outer (no valueBase/valueBitWidth) + compact size.
    final int outerHeaderBytes = 1 /* encodingKind */ + 1 /* tagKind */ + 4 /* count */
        + 8 /* valueMin */ + 8 /* valueMax */ + 4 /* dictSize */
        + (4 * dictSize)   // dict
        + (4 * dictSize)   // tagStart
        + (4 * dictSize);  // tagCount
    final int zoneMapBytes = (8 + 8) * dictSize;
    final long compactSize = NumberRegionCompact.maxEncodedSize(sortedValues, count);
    final int totalBytes = outerHeaderBytes + zoneMapBytes + (int) compactSize;
    final byte[] out = new byte[totalBytes];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(ENC_COMPACT_ZM);
    bb.put(tagKind);
    bb.putInt(count);
    bb.putLong(min);
    bb.putLong(max);
    bb.putInt(dictSize);
    for (int i = 0; i < dictSize; i++) bb.putInt(dict[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagStart[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagCount[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMin[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMax[i]);

    final int compactStart = bb.position();
    final MemorySegment view = MemorySegment.ofArray(out);
    final long written = NumberRegionCompact.writeCompact(view, compactStart, sortedValues, count);
    // The compact writer returns actual bytes; if we over-sized the byte[]
    // we must trim. maxEncodedSize is exact for the bit-packed body size so
    // this should always equal compactSize, but be defensive.
    final int actualTotal = compactStart + (int) written;
    if (actualTotal == out.length) {
      return out;
    }
    final byte[] trimmed = new byte[actualTotal];
    System.arraycopy(out, 0, trimmed, 0, actualTotal);
    return trimmed;
  }

  /**
   * Build an {@link #ENC_DELTA_ZM} payload. Outer layout matches
   * {@link #encodeCompactZM} (no outer {@code valueBase}/{@code valueBitWidth});
   * the value region is a {@link NumberRegionDelta} payload with its own
   * header (version, ddBitWidth, varint count, first value, first delta,
   * bit-packed residuals).
   */
  private static byte[] encodeDeltaZM(final long[] sortedValues, final int count,
      final int[] dict, final int dictSize, final int[] tagStart, final int[] tagCount,
      final long[] tagMin, final long[] tagMax, final long min, final long max,
      final byte tagKind) {
    final int outerHeaderBytes = 1 /* encodingKind */ + 1 /* tagKind */ + 4 /* count */
        + 8 /* valueMin */ + 8 /* valueMax */ + 4 /* dictSize */
        + (4 * dictSize)   // dict
        + (4 * dictSize)   // tagStart
        + (4 * dictSize);  // tagCount
    final int zoneMapBytes = (8 + 8) * dictSize;
    final long deltaSize = NumberRegionDelta.maxEncodedSize(sortedValues, count);
    final int totalBytes = outerHeaderBytes + zoneMapBytes + (int) deltaSize;
    final byte[] out = new byte[totalBytes];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(ENC_DELTA_ZM);
    bb.put(tagKind);
    bb.putInt(count);
    bb.putLong(min);
    bb.putLong(max);
    bb.putInt(dictSize);
    for (int i = 0; i < dictSize; i++) bb.putInt(dict[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagStart[i]);
    for (int i = 0; i < dictSize; i++) bb.putInt(tagCount[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMin[i]);
    for (int i = 0; i < dictSize; i++) bb.putLong(tagMax[i]);

    final int deltaStart = bb.position();
    final MemorySegment view = MemorySegment.ofArray(out);
    final long written = NumberRegionDelta.writeDelta(view, deltaStart, sortedValues, count);
    final int actualTotal = deltaStart + (int) written;
    if (actualTotal == out.length) {
      return out;
    }
    final byte[] trimmed = new byte[actualTotal];
    System.arraycopy(out, 0, trimmed, 0, actualTotal);
    return trimmed;
  }

  // ───────────────────────────────────────────────────────────── decoding

  /**
   * Decode the value at {@code index} (absolute within the sorted payload).
   * O(1) for every encoding except {@link #ENC_DELTA_ZM}, where the sequential
   * prefix sum makes it O(index) — scan loops over delta payloads should use
   * {@link #decodeAllValues} instead.
   */
  public static long decodeValueAt(final byte[] payload, final Header h, final int index) {
    if (isDelta(h.encodingKind)) {
      return NumberRegionDelta.readDelta(MemorySegment.ofArray(payload), h.deltaHeader, index);
    }
    if (!isBitPacked(h.encodingKind)) {
      return readLittleEndianLong(payload, h.valueBytesOffset + (index << 3));
    }
    if (h.valueBitWidth == 0) {
      // Constant-run (compact codec) — every value equals valueBase.
      return h.valueBase;
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
    if (isDelta(h.encodingKind)) {
      // Single sequential prefix sum — the fast path for delta payloads.
      NumberRegionDelta.decodeAll(MemorySegment.ofArray(payload), h.deltaHeader, out);
      return;
    }
    if (!isBitPacked(h.encodingKind)) {
      int off = h.valueBytesOffset;
      for (int i = 0; i < count; i++, off += 8) {
        out[i] = readLittleEndianLong(payload, off);
      }
    } else if (h.valueBitWidth == 0) {
      // Constant-run — compact-ZM shortcut.
      final long base = h.valueBase;
      for (int i = 0; i < count; i++) {
        out[i] = base;
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

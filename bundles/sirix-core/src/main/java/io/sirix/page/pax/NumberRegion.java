package io.sirix.page.pax;

import io.sirix.node.LE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * PAX number-region codec. Packs the numeric payload of all {@code OBJECT_NUMBER_VALUE} slots on a
 * {@link io.sirix.page.KeyValueLeafPage} into one contiguous buffer, grouped by parent
 * {@code OBJECT_KEY} {@code nameKey}. Scan operators look up the target field's range via a per-tag
 * directory in the header and iterate only the matching values — no per-entry tag decode, no slot
 * walk, no {@code moveTo}.
 *
 * <h2>Wire format</h2>
 * 
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
 * <h2>Wire format — {@link #ENC_PER_TAG_FOR} (6)</h2>
 *
 * <p>
 * One width for the whole page is the wrong unit: the values are already grouped by tag, and a page
 * of a record-shaped corpus holds an 8-bit flag beside a 64-bit hash. The page-wide frame of
 * reference made the flag cost what the hash costs — and, because the spread of a 64-bit hash
 * exceeds what a bit-packed layout can express at all, it forced every tag on the page back to
 * plain longs. The per-tag layout gives each tag its own frame: its own base, its own width, its
 * own byte-aligned run of packed values.
 *
 * <p>
 * The zone map is not stored beside the frame of reference — it <em>is</em> the frame of reference.
 * A tag's header is {@code (min, spread)}; the zone map reads {@code [min, min + spread]} and the
 * decoder reads {@code width = bits(spread)}. Nothing is written twice.
 *
 * <pre>
 * byte   encodingKind = 6
 * byte   tagKind
 * uvarint dictSize
 * per tag, in tag-id order:
 *   svarint dictDelta          // dict[i] - dict[i-1]; dict[-1] = 0
 *   uvarint tagCount           // values with this tag (tagStart is the running sum)
 *   svarint tagMin             // frame-of-reference base AND the zone-map lower bound
 *   uvarint tagSpread          // tagMax - tagMin, unsigned; width = 0 when it is 0
 * // then, per tag in the same order, BYTE-ALIGNED:
 * //   width w = spread == 0 ? 0 : 64 - numberOfLeadingZeros(spread), rounded up to 64 above 56
 * //   ceil(tagCount × w / 8) bytes of (value - min) packed at w bits, low bits first
 * //   w == 0  writes nothing (every value equals min)
 * //   w == 64 writes the RAW values (base 0), so the plain-long kernels read them unchanged
 * </pre>
 *
 * <p>
 * {@code count}, {@code tagStart}, {@code tagMax}, {@code valueMin} and {@code valueMax} are all
 * derived at parse time, which is why they are absent from the wire.
 *
 * <h2>HFT-grade scan loop</h2> A scan on field {@code F} looks up the tag id (O(dictSize)), reads
 * {@code tagStart[tag]} + {@code tagCount[tag]}, then iterates a tight range of
 * {@link #decodeValueAt(byte[], Header, int)} calls. No conditional per iteration, no tag decode,
 * bit-packing decode reduces to one unaligned 64-bit load + shift + mask.
 */
public final class NumberRegion {

  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  public static final byte ENC_PLAIN_LONG = 0;
  public static final byte ENC_BIT_PACKED = 1;
  /** PLAIN_LONG with per-tag zone maps appended (tagMin[], tagMax[]). */
  public static final byte ENC_PLAIN_LONG_ZM = 2;
  /** BIT_PACKED with per-tag zone maps appended (tagMin[], tagMax[]). */
  public static final byte ENC_BIT_PACKED_ZM = 3;
  /**
   * Value-bytes encoded via {@link NumberRegionCompact} (FOR+BP with its own embedded header: version
   * + bitWidth + varint count + minValue + body). Outer tag dict + zone maps remain intact. Written
   * when wiring is enabled; reader supports all four encoding kinds for backward compatibility.
   */
  public static final byte ENC_COMPACT_ZM = 4;

  /**
   * Value-bytes encoded via {@link NumberRegionDelta} (delta-of-delta / zig-zag bit-pack). Outer tag
   * dict + zone maps are laid out exactly like {@link #ENC_COMPACT_ZM} (no outer
   * {@code valueBase}/{@code valueBitWidth} — those live in the nested delta header). Chosen
   * automatically when it produces a strictly smaller value region than FOR+BP, which is the case for
   * temporal columns (commit timestamps, valid-time, monotonic ids).
   *
   * <p>
   * Delta decode is sequential, so payloads under this encoding are excluded from the SIMD scan
   * kernels (they fall back to the scalar {@link #decodeValueAt} / {@link #decodeAllValues} loop).
   */
  public static final byte ENC_DELTA_ZM = 5;

  /**
   * Per-TAG frame of reference: every tag carries its own {@code (min, spread)} and its values are
   * packed at its own width in a byte-aligned run. The per-tag zone map and the frame of reference
   * are the same two numbers, stored once. See the class comment for the wire layout.
   *
   * <p>
   * Elected per page against the whole-region encodings and written only when it is strictly smaller,
   * so a page the older layouts encode better keeps them.
   */
  public static final byte ENC_PER_TAG_FOR = 6;

  /**
   * {@link #ENC_PER_TAG_FOR} with its per-tag directory stored ONCE, in
   * {@link RegionTable#KIND_NUMBER_ZONEMAP}, rather than a second time here.
   *
   * <p>
   * The zone map was already a copy of this region's per-tag header — the same tag ids, counts,
   * minima and maxima — because a predicate has to read the bounds without decompressing the values
   * they summarise. Once the frame of reference IS the zone map, keeping both is writing the page's
   * directory twice: at 1M it cost 0.38 B/record, 36 % of what the number column and its summary
   * spend together.
   *
   * <p>
   * So the directory lives in the summary, which is small, usually uncompressed, and read first; the
   * value region keeps only its two-byte prefix and the packed values, and derives each tag's base,
   * width and byte offset from the summary. A prune still costs a couple of comparisons against
   * header longs — and now never materializes the value region at all.
   *
   * <p>
   * The coupling is one-directional and declared: decoding these values REQUIRES the zone map, so a
   * reader must request the pair. A writer that cannot publish a zone map falls back to
   * {@link #ENC_PER_TAG_FOR}, which carries its own directory — no page is ever undecodable, only
   * occasionally larger.
   */
  public static final byte ENC_PER_TAG_FOR_EXTERNAL = 7;

  /**
   * Widths above this are rounded up to 64 and stored as raw longs. 57..63 would need a
   * read-modify-write packer and are not vector-unpackable either ({@link BitUnpackSimd#supports}
   * stops at 56), so the ≤ 12 % they could save on the widest columns is not worth a second packing
   * path — at 64 the values ARE plain longs and every plain-long kernel reads them as they lie.
   */
  private static final int MAX_PACKED_BIT_WIDTH = BitUnpackSimd.MAX_BIT_WIDTH;

  /**
   * {@link Header#valueBytesOffset} for a per-tag payload, where no single offset describes the
   * values. A reader that reaches for the page-wide offset on such a payload gets an out-of-bounds
   * access rather than another tag's bytes decoded as this one's.
   */
  public static final int PER_TAG_NO_PAGE_WIDE_OFFSET = -1;

  /**
   * Write {@link #ENC_PER_TAG_FOR} when it is smaller than the whole-region encodings. Off pins the
   * encoder to the pre-per-tag layouts byte for byte.
   */
  private static volatile Boolean PER_TAG_WRITE_OVERRIDE = null;

  /** Test hook: force-enable/disable per-tag FOR writes without restarting the JVM. */
  public static void setPerTagWidthEnabled(final boolean enabled) {
    PER_TAG_WRITE_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the system property. */
  public static void clearPerTagWidthOverride() {
    PER_TAG_WRITE_OVERRIDE = null;
  }

  /** Kill switch {@code -Dsirix.page.numberRegion.perTagWidth=false}; on by default. */
  public static boolean perTagWidthEnabled() {
    final Boolean override = PER_TAG_WRITE_OVERRIDE;
    if (override != null) {
      return override;
    }
    return !"false".equalsIgnoreCase(System.getProperty("sirix.page.numberRegion.perTagWidth"));
  }

  /**
   * Bits needed for a residual of {@code spread}, with the rounding {@link #MAX_PACKED_BIT_WIDTH}
   * describes. {@code spread} is read as unsigned, so a spread that wraps the signed range answers
   * 64.
   */
  public static int widthForSpread(final long spread) {
    if (spread == 0L) {
      return 0;
    }
    final int bits = 64 - Long.numberOfLeadingZeros(spread);
    return bits > MAX_PACKED_BIT_WIDTH
        ? 64
        : bits;
  }

  /** Bytes a tag's byte-aligned packed run occupies. */
  public static int packedBytes(final int valueCount, final int bitWidth) {
    return (int) (((long) valueCount * bitWidth + 7L) >>> 3);
  }

  /**
   * Write {@link #ENC_COMPACT_ZM} instead of {@link #ENC_BIT_PACKED_ZM} on the bit-packed path when
   * enabled. Off by default because the compact codec adds ~2-7 bytes/region of framing overhead
   * (version + varint) without a proportional speedup on Sirix's cold-path. Flip to test.
   *
   * <p>
   * Read volatile-per-call so tests can toggle without class reload. The system property fallback
   * lets production JVMs pin a value at startup.
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
   * Enable/disable the delta-of-delta ({@link #ENC_DELTA_ZM}) write path. When enabled (the default)
   * the encoder emits delta whenever it yields a strictly smaller value region than FOR+BP; when
   * disabled the encoder never writes delta (readers still decode existing delta payloads). Toggled
   * without a class reload for A/B tests.
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
   * {@code tagKind} classifier for the region's tag dictionary. Determines the semantic
   * interpretation of {@link Header#dict}:
   *
   * <ul>
   * <li>{@link #TAG_KIND_NAME} — tags are parent OBJECT_KEY nameKeys (compression-only; not SIMD-safe
   * when the same nameKey sits under multiple pathNodeKeys on one page).</li>
   * <li>{@link #TAG_KIND_PATH_NODE} — tags are parent OBJECT_KEY pathNodeKeys truncated to int.
   * SIMD-safe for path-scoped scans: a successful {@link #lookupTag(Header, int)} implies every value
   * in the tag's range belongs to the exact requested pathNodeKey.</li>
   * </ul>
   */
  public static final byte TAG_KIND_NAME = 0;
  public static final byte TAG_KIND_PATH_NODE = 1;

  /** @return true if the encoding kind includes per-tag zone-map arrays. */
  public static boolean hasZoneMap(final byte encodingKind) {
    return encodingKind >= ENC_PLAIN_LONG_ZM;
  }

  /**
   * @return true iff the value bytes are FOR + bit-packed and directly random-accessible /
   *         SIMD-scannable. {@link #ENC_COMPACT_ZM} counts (bit-packed under an embedded header);
   *         {@link #ENC_DELTA_ZM} does <em>not</em> — delta is sequential and must never be routed
   *         through a FOR unpack loop.
   */
  public static boolean isBitPacked(final byte encodingKind) {
    return encodingKind == ENC_BIT_PACKED || encodingKind == ENC_BIT_PACKED_ZM || encodingKind == ENC_COMPACT_ZM;
  }

  /** @return true iff {@code encodingKind == ENC_COMPACT_ZM}. */
  public static boolean isCompact(final byte encodingKind) {
    return encodingKind == ENC_COMPACT_ZM;
  }

  /** @return true iff {@code encodingKind == ENC_DELTA_ZM} (delta-of-delta). */
  public static boolean isDelta(final byte encodingKind) {
    return encodingKind == ENC_DELTA_ZM;
  }

  /**
   * @return true iff every tag carries its own frame of reference and width
   *         ({@link #ENC_PER_TAG_FOR}). Such a payload has no page-wide base, width or value offset;
   *         it is read through {@link Header#tagWidth}/{@link Header#tagDecodeBase}/
   *         {@link Header#tagValueOffset}, which is why {@link #isBitPacked} answers false for it.
   */
  public static boolean isPerTagFor(final byte encodingKind) {
    return encodingKind == ENC_PER_TAG_FOR || encodingKind == ENC_PER_TAG_FOR_EXTERNAL;
  }

  /**
   * Whether this payload's per-tag directory lives in {@link RegionTable#KIND_NUMBER_ZONEMAP} and
   * must be supplied to {@link Header#parseInto(MemorySegment, MemorySegment)}.
   *
   * <p>
   * Callers that may legitimately hold the value region without its summary — a region-only read
   * whose mask asked for one and not the other — check this and DECLINE, falling back to the record
   * path. Passing a null directory to the parse instead is a programming error and says so.
   */
  public static boolean needsExternalDirectory(final MemorySegment payload) {
    return payload != null && payload.byteSize() > 0
        && payload.get(ValueLayout.JAVA_BYTE, 0L) == ENC_PER_TAG_FOR_EXTERNAL;
  }

  /**
   * Write {@link #ENC_PER_TAG_FOR_EXTERNAL} when the caller also publishes the zone map. Off keeps
   * the directory inside the value region, i.e. {@link #ENC_PER_TAG_FOR}.
   */
  private static volatile Boolean EXTERNAL_HEADER_OVERRIDE = null;

  /** Test hook: force-enable/disable the external directory without restarting the JVM. */
  public static void setExternalHeaderEnabled(final boolean enabled) {
    EXTERNAL_HEADER_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the system property. */
  public static void clearExternalHeaderOverride() {
    EXTERNAL_HEADER_OVERRIDE = null;
  }

  /** Kill switch {@code -Dsirix.page.numberRegion.externalHeader=false}; on by default. */
  public static boolean externalHeaderEnabled() {
    final Boolean override = EXTERNAL_HEADER_OVERRIDE;
    if (override != null) {
      return override;
    }
    return !"false".equalsIgnoreCase(System.getProperty("sirix.page.numberRegion.externalHeader"));
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
     * Frame-of-reference base for bit-packed encodings. Populated from the outer header for
     * {@link #ENC_BIT_PACKED_ZM}; populated from the nested compact header for {@link #ENC_COMPACT_ZM}.
     */
    public long valueBase;
    /**
     * Bits per value for bit-packed encodings. For {@link #ENC_PLAIN_LONG_ZM} this is 64. For
     * {@link #ENC_COMPACT_ZM} this is taken from the nested compact header (so constant-run encodings
     * surface as 0 here too).
     */
    public byte valueBitWidth;
    public int dictSize;
    public int[] dict; // length ≥ dictSize
    public int[] tagStart; // length ≥ dictSize
    public int[] tagCount; // length ≥ dictSize
    /** Per-tag minimum value. Populated only when {@link #hasZoneMap(byte)}; else null. */
    public long[] tagMin;
    /** Per-tag maximum value. Populated only when {@link #hasZoneMap(byte)}; else null. */
    public long[] tagMax;
    /**
     * First byte of the value area, or {@link #PER_TAG_NO_PAGE_WIDE_OFFSET} for
     * {@link #ENC_PER_TAG_FOR}, where each tag has its own.
     */
    public int valueBytesOffset;
    public int valueBytesLength;
    /**
     * Bits per value for each tag; {@code 0} means constant, {@code 64} means raw longs. Populated only
     * for {@link #ENC_PER_TAG_FOR}; else null.
     */
    public byte[] tagWidth;
    /**
     * Frame-of-reference base each tag's residuals are decoded against — {@link #tagMin} except at
     * width 64, where the values are stored raw and the base is 0. Populated only for
     * {@link #ENC_PER_TAG_FOR}; else null.
     */
    public long[] tagDecodeBase;
    /**
     * First byte of each tag's packed run, absolute within the payload. Populated only for
     * {@link #ENC_PER_TAG_FOR}; else null.
     */
    public int[] tagValueOffset;
    /**
     * Nested delta header. Populated only for {@link #ENC_DELTA_ZM}; null otherwise. Carries
     * {@code firstValue}/{@code firstDelta}/{@code bodyOffset} needed to replay the delta prefix sum.
     */
    public NumberRegionDelta.Header deltaHeader;

    /** @return true iff this header describes a per-tag frame-of-reference payload. */
    public boolean isPerTag() {
      return isPerTagFor(encodingKind);
    }

    /**
     * Parse a self-contained payload.
     *
     * @throws IllegalArgumentException when the payload's directory is external — such a region can
     *         only be read together with its zone map, and reading it without one would be a silent
     *         mis-decode rather than a missing region
     */
    public Header parseInto(final MemorySegment payload) {
      return parseInto(payload, null);
    }

    /**
     * Parse, taking the per-tag directory from {@code directoryPayload} when the payload says its
     * directory is external. The directory is ignored for every self-contained encoding, so a caller
     * that holds both may always use this form.
     *
     * @param directoryPayload the page's {@link RegionTable#KIND_NUMBER_ZONEMAP} payload, or
     *        {@code null} when the caller holds none
     */
    public Header parseInto(final MemorySegment payload, final MemorySegment directoryPayload) {
      final RegionReader bb = new RegionReader(payload);
      encodingKind = bb.readByte();
      tagKind = bb.readByte();
      if (encodingKind == ENC_PER_TAG_FOR_EXTERNAL) {
        if (directoryPayload == null) {
          throw new IllegalArgumentException(
              "this number region's per-tag directory is external; read it together with KIND_NUMBER_ZONEMAP "
                  + "(NumberRegion.needsExternalDirectory answers whether a payload needs one)");
        }
        return parsePerTagForExternal(payload, bb.position(), directoryPayload);
      }
      if (encodingKind == ENC_PER_TAG_FOR) {
        return parsePerTagFor(payload, bb.position());
      }
      tagWidth = null;
      tagDecodeBase = null;
      tagValueOffset = null;
      count = bb.readInt();
      valueMin = bb.readLong();
      valueMax = bb.readLong();
      if (encodingKind == ENC_COMPACT_ZM || encodingKind == ENC_DELTA_ZM) {
        // Compact-ZM / Delta-ZM: no outer valueBase/valueBitWidth — those live
        // inside the nested codec header which precedes the body.
        valueBase = 0L;
        valueBitWidth = 0;
      } else {
        valueBase = bb.readLong();
        valueBitWidth = bb.readByte();
      }
      dictSize = bb.readInt();
      if (dict == null || dict.length < dictSize)
        dict = new int[Math.max(4, dictSize)];
      if (tagStart == null || tagStart.length < dictSize)
        tagStart = new int[Math.max(4, dictSize)];
      if (tagCount == null || tagCount.length < dictSize)
        tagCount = new int[Math.max(4, dictSize)];
      for (int i = 0; i < dictSize; i++)
        dict[i] = bb.readInt();
      for (int i = 0; i < dictSize; i++)
        tagStart[i] = bb.readInt();
      for (int i = 0; i < dictSize; i++)
        tagCount[i] = bb.readInt();
      if (hasZoneMap(encodingKind)) {
        if (tagMin == null || tagMin.length < dictSize)
          tagMin = new long[Math.max(4, dictSize)];
        if (tagMax == null || tagMax.length < dictSize)
          tagMax = new long[Math.max(4, dictSize)];
        for (int i = 0; i < dictSize; i++)
          tagMin[i] = bb.readLong();
        for (int i = 0; i < dictSize; i++)
          tagMax[i] = bb.readLong();
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
        final NumberRegionCompact.Header compactH = new NumberRegionCompact.Header();
        NumberRegionCompact.readHeader(payload, compactHeaderOff, compactH);
        valueBase = compactH.minValue;
        valueBitWidth = compactH.bitWidth;
        valueBytesOffset = (int) compactH.bodyOffset;
        valueBytesLength = (int) compactH.bodyBytes;
        deltaHeader = null; // defensive: never leave a stale delta header on a reused Header
      } else if (encodingKind == ENC_DELTA_ZM) {
        // Parse the nested delta header. valueBytesOffset points at the delta
        // body; decode goes through NumberRegionDelta, not the FOR unpack path.
        final int deltaHeaderOff = bb.position();
        if (deltaHeader == null) {
          deltaHeader = new NumberRegionDelta.Header();
        }
        NumberRegionDelta.readHeader(payload, deltaHeaderOff, deltaHeader);
        valueBase = 0L;
        valueBitWidth = deltaHeader.bitWidth;
        valueBytesOffset = (int) deltaHeader.bodyOffset;
        valueBytesLength = (int) deltaHeader.bodyBytes;
      } else {
        deltaHeader = null;
        valueBytesOffset = bb.position();
        valueBytesLength = bitsToBytes((long) count * (isBitPacked(encodingKind)
            ? valueBitWidth
            : 64));
      }
      return this;
    }

    /**
     * Parse a {@link #ENC_PER_TAG_FOR} payload whose two-byte prefix has already been consumed.
     *
     * <p>
     * Everything the older layouts wrote out — {@code count}, {@code tagStart}, {@code tagMax}, the
     * page-global bounds — is reconstructed here from the per-tag {@code (min, spread)} pairs, so a
     * reader of this header sees exactly the fields it saw before and no consumer outside this class
     * learns that the layout changed.
     *
     * @param valuePrefix byte offset just past the {@code encodingKind}/{@code tagKind} prefix
     */
    private Header parsePerTagFor(final MemorySegment payload, final int valuePrefix) {
      deltaHeader = null;
      valueBase = 0L;
      valueBitWidth = 0;
      long position = valuePrefix;
      final long declaredDictSize = VarInt.readUnsigned(payload, position);
      position += VarInt.sizeOfUnsigned(declaredDictSize);
      if (declaredDictSize < 0L || declaredDictSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("per-tag number region declares dictSize=" + declaredDictSize);
      }
      dictSize = (int) declaredDictSize;
      ensureTagCapacity(dictSize);

      int previousTag = 0;
      int running = 0;
      long globalMin = Long.MAX_VALUE;
      long globalMax = Long.MIN_VALUE;
      for (int i = 0; i < dictSize; i++) {
        final long tagDelta = VarInt.readSigned(payload, position);
        position += VarInt.sizeOfSigned(tagDelta);
        previousTag = (int) (previousTag + tagDelta);
        dict[i] = previousTag;

        final long valueCount = VarInt.readUnsigned(payload, position);
        position += VarInt.sizeOfUnsigned(valueCount);
        if (valueCount < 0L || valueCount > Integer.MAX_VALUE - running) {
          throw new IllegalArgumentException(
              "per-tag number region: tag " + i + " declares " + valueCount + " values, which overruns the region");
        }
        tagCount[i] = (int) valueCount;
        tagStart[i] = running;
        running += (int) valueCount;

        final long min = VarInt.readSigned(payload, position);
        position += VarInt.sizeOfSigned(min);
        final long spread = VarInt.readUnsigned(payload, position);
        position += VarInt.sizeOfUnsigned(spread);
        tagMin[i] = min;
        tagMax[i] = min + spread;
        if (min < globalMin) {
          globalMin = min;
        }
        if (tagMax[i] > globalMax) {
          globalMax = tagMax[i];
        }
      }
      count = running;
      valueMin = dictSize == 0
          ? 0L
          : globalMin;
      valueMax = dictSize == 0
          ? 0L
          : globalMax;
      return deriveTagFrames(payload, position);
    }

    /**
     * Parse a {@link #ENC_PER_TAG_FOR_EXTERNAL} payload: the directory comes from the zone map, the
     * values from this payload, and everything else is derived exactly as for the self-contained form.
     */
    private Header parsePerTagForExternal(final MemorySegment payload, final int valuePrefix,
        final MemorySegment directoryPayload) {
      deltaHeader = null;
      valueBase = 0L;
      valueBitWidth = 0;
      final byte directoryTagKind = NumberZoneMapRegion.readDirectoryInto(directoryPayload, this);
      if (directoryTagKind != tagKind) {
        // The two regions disagree about what their tags MEAN — a nameKey read as a pathNodeKey
        // would prune and scan against a different column. Never a shape to work around.
        throw new IllegalArgumentException(
            "number region declares tagKind=" + tagKind + " but its directory declares " + directoryTagKind);
      }
      return deriveTagFrames(payload, valuePrefix);
    }

    /**
     * Turn a filled per-tag directory into decode frames: each tag's width from its spread, its base
     * (the minimum, or zero where the values are stored raw) and its byte-aligned start.
     *
     * @param valuesStart first byte of the packed value area within {@code payload}
     */
    private Header deriveTagFrames(final MemorySegment payload, final long valuesStart) {
      long offset = valuesStart;
      for (int i = 0; i < dictSize; i++) {
        final long spread = tagMax[i] - tagMin[i];
        final int width = widthForSpread(spread);
        tagWidth[i] = (byte) width;
        tagDecodeBase[i] = width == 64
            ? 0L
            : tagMin[i];
        tagValueOffset[i] = (int) offset;
        offset += packedBytes(tagCount[i], width);
      }
      if (offset > payload.byteSize()) {
        throw new IllegalArgumentException(
            "per-tag number region needs " + offset + " bytes but the payload holds " + payload.byteSize());
      }
      valueBytesOffset = PER_TAG_NO_PAGE_WIDE_OFFSET;
      valueBytesLength = (int) (offset - valuesStart);
      return this;
    }

    /**
     * Fill the directory fields from a zone map, for {@link #parsePerTagForExternal}. Package-private
     * so the zone map's parser can write them without a second copy.
     */
    void acceptDirectory(final int size) {
      ensureTagCapacity(size);
      dictSize = size;
    }

    /** Grow every per-tag array to hold {@code size} entries, the per-tag-only ones included. */
    private void ensureTagCapacity(final int size) {
      final int capacity = Math.max(4, size);
      if (dict == null || dict.length < size) {
        dict = new int[capacity];
      }
      if (tagStart == null || tagStart.length < size) {
        tagStart = new int[capacity];
      }
      if (tagCount == null || tagCount.length < size) {
        tagCount = new int[capacity];
      }
      if (tagMin == null || tagMin.length < size) {
        tagMin = new long[capacity];
      }
      if (tagMax == null || tagMax.length < size) {
        tagMax = new long[capacity];
      }
      if (tagWidth == null || tagWidth.length < size) {
        tagWidth = new byte[capacity];
      }
      if (tagDecodeBase == null || tagDecodeBase.length < size) {
        tagDecodeBase = new long[capacity];
      }
      if (tagValueOffset == null || tagValueOffset.length < size) {
        tagValueOffset = new int[capacity];
      }
    }

    /** Per-tag minimum, or the page-global {@link #valueMin} if no per-tag map is present. */
    public long tagMinOrGlobal(final int tag) {
      return tagMin != null
          ? tagMin[tag]
          : valueMin;
    }

    /** Per-tag maximum, or the page-global {@link #valueMax} if no per-tag map is present. */
    public long tagMaxOrGlobal(final int tag) {
      return tagMax != null
          ? tagMax[tag]
          : valueMax;
    }
  }

  // ───────────────────────────────────────────────────────────── encoding

  /** Bytes before the per-tag arrays in the plain/bit-packed outer header. */
  private static final int PLAIN_FIXED_HEADER_BYTES = 1 + 1 + 4 + 8 + 8 + 8 + 1 + 4;

  /** Bytes before the per-tag arrays in the compact/delta outer header. */
  private static final int NESTED_FIXED_HEADER_BYTES = 1 + 1 + 4 + 8 + 8 + 4;

  /** Dict, start, count, minimum and maximum bytes contributed by one distinct tag. */
  private static final int BYTES_PER_TAG = 4 + 4 + 4 + 8 + 8;

  /** Public compatibility calls at or below one leaf page reuse bounded thread-local work storage. */
  private static final int MAX_RETAINED_COMPAT_COUNT = 1024;

  /**
   * Compatibility entry points still promise a new, caller-owned byte array. Their work buffers are
   * nevertheless thread-confined and reused, so the promised result is the only array allocated per
   * call. Production serialization uses its own {@link Encoder} and copies only the valid prefix.
   */
  private static final ThreadLocal<Encoder> COMPAT_ENCODER =
      ThreadLocal.withInitial(() -> new Encoder(MAX_RETAINED_COMPAT_COUNT));

  /**
   * Legacy 3-arg entry point. Encodes with {@link #TAG_KIND_NAME}: dict holds parent OBJECT_KEY
   * nameKeys. Kept for test and callers that don't have pathNodeKey information.
   */
  public static byte[] encode(final long[] values, final int[] parentTags, final int count) {
    return encode(values, parentTags, count, TAG_KIND_NAME);
  }

  /**
   * Encode parallel arrays {@code values[i]} and {@code parentTags[i]} into a tag-sorted payload.
   * {@code tagKind} declares the semantic interpretation of {@code parentTags} so downstream scan
   * operators can decide whether a tag match is safe for path-scoped queries.
   *
   * <p>
   * The arrays may be longer than {@code count}; only the prefix is consumed.
   */
  public static byte[] encode(final long[] values, final int[] parentTags, final int count, final byte tagKind) {
    validateEncodeInput(values, parentTags, count, tagKind);
    // The generic API is not page-size-limited. Do not let one exceptional caller pin eight huge
    // work arrays and its output in a long-lived pool thread; only page-bounded calls use the TL.
    final Encoder encoder = count <= MAX_RETAINED_COMPAT_COUNT
        ? COMPAT_ENCODER.get()
        : new Encoder(count);
    final int encodedLength = encoder.encodeInto(values, parentTags, count, tagKind);
    return Arrays.copyOf(encoder.output(), encodedLength);
  }

  // ───────────────────────────────────────────────────────────── decoding

  /**
   * Thread-confined reusable number-region encoder. One instance may serve any number of pages on one
   * thread, but it must not be shared or entered recursively. The output array is scratch: only
   * {@code [0, encodeInto(...))} is valid, and the next encode overwrites it.
   *
   * <p>
   * The page writer must copy that prefix into page-owned storage before another encode. In
   * particular, a heap {@link MemorySegment} view of {@link #output()} must never be installed
   * through {@link RegionTable#setSegment(byte, MemorySegment)} because later pages would overwrite
   * it.
   */
  public static final class Encoder {
    private int[] dict;
    private int[] localIds;
    private int[] tagCount;
    private int[] tagStart;
    private int[] cursor;
    private long[] sortedValues;
    private long[] tagMin;
    private long[] tagMax;
    /** Per-tag packing width, filled by {@link #measurePerTagFor} and consumed by the writer. */
    private byte[] tagWidths;
    /** Bytes the packed runs occupy, filled by {@link #measurePerTagFor}. */
    private long perTagValueBytes;
    /** The directory of the most recent encode, for {@link #directoryInto}. */
    private int lastCount;
    private int lastDictSize;
    private long lastValueMin;
    private long lastValueMax;
    private byte lastTagKind;
    private byte lastEncodingKind;
    private byte[] output;
    private MemorySegment outputView;
    private int encodedLength;

    public Encoder(final int initialCapacity) {
      if (initialCapacity < 0) {
        throw new IllegalArgumentException("initialCapacity=" + initialCapacity);
      }
      allocate(Math.max(1, initialCapacity));
    }

    /**
     * Encode into this instance's reusable output buffer.
     *
     * @return exact encoded length; only this prefix of {@link #output()} is valid
     */
    public int encodeInto(final long[] values, final int[] parentTags, final int count, final byte tagKind) {
      return encodeInto(values, parentTags, count, tagKind, false);
    }

    /**
     * Encode into this instance's reusable output buffer.
     *
     * @param directoryIsExternal the caller undertakes to publish this region's per-tag directory as
     *        the page's {@link RegionTable#KIND_NUMBER_ZONEMAP}, from {@link #directoryInto}. Only then
     *        may the encoder omit the directory here; a caller that cannot promise it gets the
     *        self-contained form, which every reader can decode alone.
     * @return exact encoded length; only this prefix of {@link #output()} is valid
     */
    public int encodeInto(final long[] values, final int[] parentTags, final int count, final byte tagKind,
        final boolean directoryIsExternal) {
      encodedLength = 0;
      validateEncodeInput(values, parentTags, count, tagKind);
      ensureCapacity(count);

      // A zero dictSize makes every stale dictionary entry unreachable. Only active count ranges are
      // reset below; full-capacity clears would turn a small page into fixed O(1024) memory traffic.
      int dictSize = 0;
      for (int i = 0; i < count; i++) {
        final int tag = parentTags[i];
        int found = -1;
        for (int j = 0; j < dictSize; j++) {
          if (dict[j] == tag) {
            found = j;
            break;
          }
        }
        if (found < 0) {
          found = dictSize;
          dict[dictSize++] = tag;
        }
        localIds[i] = found;
      }

      Arrays.fill(tagCount, 0, dictSize, 0);
      for (int i = 0; i < count; i++) {
        tagCount[localIds[i]]++;
      }
      int running = 0;
      for (int tag = 0; tag < dictSize; tag++) {
        tagStart[tag] = running;
        running += tagCount[tag];
        tagMin[tag] = Long.MAX_VALUE;
        tagMax[tag] = Long.MIN_VALUE;
      }
      System.arraycopy(tagStart, 0, cursor, 0, dictSize);

      for (int i = 0; i < count; i++) {
        final int tag = localIds[i];
        final long value = values[i];
        sortedValues[cursor[tag]++] = value;
        if (value < tagMin[tag]) {
          tagMin[tag] = value;
        }
        if (value > tagMax[tag]) {
          tagMax[tag] = value;
        }
      }

      long min = 0L;
      long max = 0L;
      if (count > 0) {
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
        for (int tag = 0; tag < dictSize; tag++) {
          if (tagMin[tag] < min) {
            min = tagMin[tag];
          }
          if (tagMax[tag] > max) {
            max = tagMax[tag];
          }
        }
      }

      final long spread = count == 0
          ? 0L
          : max - min;
      final boolean bitPacked = count > 0 && spread >= 0 && spread < (1L << BitUnpackSimd.MAX_BIT_WIDTH);

      // Delta-of-delta must win on both residual width and exact encoded bytes because selecting it
      // trades away random access and SIMD scans.
      boolean delta = false;
      if (deltaWriteEnabled() && count >= MIN_DELTA_COUNT) {
        final int forBitWidth = bitPacked
            ? Math.max(1, 64 - Long.numberOfLeadingZeros(spread))
            : 64;
        final int deltaBitWidth = NumberRegionDelta.computeBitWidth(sortedValues, count);
        if (deltaBitWidth < forBitWidth) {
          final long forValueRegionBytes = 9L + bitsToBytes((long) count * forBitWidth);
          final long deltaRegionBytes =
              NumberRegionDelta.headerBytes(count) + NumberRegionDelta.bodyBytes(count, deltaBitWidth);
          delta = deltaRegionBytes < forValueRegionBytes;
        }
      }
      final boolean compact = !delta && bitPacked && compactWriteEnabled();

      // The per-tag layout is elected on measured bytes, never on shape: a page whose tags share one
      // narrow width is encoded no worse by the whole-region form, and a page whose values are one
      // long monotonic run is still delta's. Only a strict win takes the new layout, so the older
      // ones keep every page they already encode better.
      if (count > 0 && perTagWidthEnabled()) {
        final int perTagBytes = measurePerTagFor(count, dictSize);
        // The zone map is written whatever this region elects, so comparing region bytes alone is
        // the honest comparison even when the directory moves into it.
        final boolean external = directoryIsExternal && externalHeaderEnabled();
        final int perTagCandidate = external
            ? checkedEncodedLength(2L + perTagValueBytes)
            : perTagBytes;
        final long incumbentBytes;
        if (delta) {
          incumbentBytes = (long) NESTED_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize
              + NumberRegionDelta.maxEncodedSize(sortedValues, count);
        } else if (compact) {
          incumbentBytes = (long) NESTED_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize
              + NumberRegionCompact.maxEncodedSize(sortedValues, count);
        } else {
          final int width = bitPacked
              ? Math.max(1, 64 - Long.numberOfLeadingZeros(spread))
              : 64;
          incumbentBytes =
              (long) PLAIN_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize + bitsToBytes((long) count * width);
        }
        if (perTagCandidate < incumbentBytes) {
          rememberDirectory(count, dictSize, min, max, tagKind, external
              ? ENC_PER_TAG_FOR_EXTERNAL
              : ENC_PER_TAG_FOR);
          return external
              ? encodePerTagForExternal(dictSize, perTagCandidate, tagKind)
              : encodePerTagFor(count, dictSize, perTagBytes, tagKind);
        }
      }
      rememberDirectory(count, dictSize, min, max, tagKind, delta
          ? ENC_DELTA_ZM
          : (compact
              ? ENC_COMPACT_ZM
              : (bitPacked
                  ? ENC_BIT_PACKED_ZM
                  : ENC_PLAIN_LONG_ZM)));

      if (delta) {
        return encodeNested(ENC_DELTA_ZM, count, dictSize, min, max, tagKind);
      }
      if (compact) {
        return encodeNested(ENC_COMPACT_ZM, count, dictSize, min, max, tagKind);
      }
      return encodePlain(count, dictSize, min, max, spread, bitPacked, tagKind);
    }

    /**
     * Size the per-tag layout and fill {@link #tagWidths} for the writer.
     *
     * <p>
     * One pass over the tags, no pass over the values: the per-tag bounds are already computed, and the
     * width of a tag is a function of its spread alone.
     *
     * @return exact bytes {@link #encodePerTagFor} would write
     */
    private int measurePerTagFor(final int count, final int dictSize) {
      long bytes = 2L + VarInt.sizeOfUnsigned(dictSize);
      perTagValueBytes = 0L;
      int previousTag = 0;
      for (int t = 0; t < dictSize; t++) {
        bytes += VarInt.sizeOfSigned((long) dict[t] - previousTag);
        previousTag = dict[t];
        bytes += VarInt.sizeOfUnsigned(tagCount[t]);
        bytes += VarInt.sizeOfSigned(tagMin[t]);
        final long tagSpread = tagMax[t] - tagMin[t];
        bytes += VarInt.sizeOfUnsigned(tagSpread);
        final int width = widthForSpread(tagSpread);
        tagWidths[t] = (byte) width;
        perTagValueBytes += packedBytes(tagCount[t], width);
      }
      return checkedEncodedLength(bytes + perTagValueBytes);
    }

    /**
     * Record what {@link #directoryInto} will hand the zone-map writer. Kept from the encode rather
     * than re-parsed out of the payload, because the external form no longer has a directory to parse —
     * and re-parsing what we just wrote was never anything but a second copy of it.
     */
    private void rememberDirectory(final int count, final int dictSize, final long min, final long max,
        final byte tagKind, final byte encodingKind) {
      lastCount = count;
      lastDictSize = dictSize;
      lastValueMin = min;
      lastValueMax = max;
      lastTagKind = tagKind;
      lastEncodingKind = encodingKind;
    }

    /**
     * Fill {@code target} with the directory of the region most recently encoded: the tag ids, their
     * value counts and starts, and their bounds.
     *
     * <p>
     * This is the writer's counterpart to a parse. It exists because {@link #ENC_PER_TAG_FOR_EXTERNAL}
     * publishes that directory as the zone map instead of inside the value region, so the writer must
     * hand it over rather than read it back.
     *
     * <p>
     * The decode-side fields are deliberately left cleared: the target describes a directory, not a
     * decodable region, and a stale width or offset from a previous page is exactly the kind of mixture
     * that reads as valid.
     */
    public void directoryInto(final Header target) {
      target.encodingKind = lastEncodingKind;
      target.tagKind = lastTagKind;
      target.count = lastCount;
      target.valueMin = lastValueMin;
      target.valueMax = lastValueMax;
      target.dictSize = lastDictSize;
      target.acceptDirectory(lastDictSize);
      System.arraycopy(dict, 0, target.dict, 0, lastDictSize);
      System.arraycopy(tagStart, 0, target.tagStart, 0, lastDictSize);
      System.arraycopy(tagCount, 0, target.tagCount, 0, lastDictSize);
      System.arraycopy(tagMin, 0, target.tagMin, 0, lastDictSize);
      System.arraycopy(tagMax, 0, target.tagMax, 0, lastDictSize);
      target.valueBase = 0L;
      target.valueBitWidth = 0;
      target.valueBytesOffset = PER_TAG_NO_PAGE_WIDE_OFFSET;
      target.valueBytesLength = 0;
      target.deltaHeader = null;
    }

    /**
     * Write {@link #ENC_PER_TAG_FOR_EXTERNAL}: the two-byte prefix and the packed runs, with the
     * directory left for {@link #directoryInto} to publish as the zone map.
     */
    private int encodePerTagForExternal(final int dictSize, final int totalBytes, final byte tagKind) {
      ensureOutputCapacity(totalBytes);
      int position = 0;
      output[position++] = ENC_PER_TAG_FOR_EXTERNAL;
      output[position++] = tagKind;
      position = writePackedRuns(position, dictSize);
      if (position != totalBytes) {
        throw new IllegalStateException(
            "external per-tag number region size mismatch: measured=" + totalBytes + " written=" + position);
      }
      encodedLength = totalBytes;
      return totalBytes;
    }

    /**
     * Write the per-tag layout. {@link #measurePerTagFor} must have run for this {@code count} /
     * {@code dictSize} — it owns {@link #tagWidths} and the size this writes to.
     */
    private int encodePerTagFor(final int count, final int dictSize, final int totalBytes, final byte tagKind) {
      ensureOutputCapacity(totalBytes);
      int position = 0;
      output[position++] = ENC_PER_TAG_FOR;
      output[position++] = tagKind;
      position = VarInt.writeUnsigned(output, position, dictSize);
      int previousTag = 0;
      for (int t = 0; t < dictSize; t++) {
        position = VarInt.writeSigned(output, position, (long) dict[t] - previousTag);
        previousTag = dict[t];
        position = VarInt.writeUnsigned(output, position, tagCount[t]);
        position = VarInt.writeSigned(output, position, tagMin[t]);
        position = VarInt.writeUnsigned(output, position, tagMax[t] - tagMin[t]);
      }
      position = writePackedRuns(position, dictSize);
      if (position != totalBytes) {
        throw new IllegalStateException(
            "per-tag number region size mismatch: measured=" + totalBytes + " written=" + position);
      }
      encodedLength = totalBytes;
      return totalBytes;
    }

    /** Mutable scratch buffer. The next call to {@link #encodeInto} overwrites it. */
    public byte[] output() {
      return output;
    }

    /** Exact length returned by the most recent successful encode, or zero after a failed attempt. */
    public int encodedLength() {
      return encodedLength;
    }

    private int encodePlain(final int count, final int dictSize, final long min, final long max, final long spread,
        final boolean bitPacked, final byte tagKind) {
      final byte encodingKind = bitPacked
          ? ENC_BIT_PACKED_ZM
          : ENC_PLAIN_LONG_ZM;
      final long valueBase = bitPacked
          ? min
          : 0L;
      final byte valueBitWidth = bitPacked
          ? (byte) Math.max(1, 64 - Long.numberOfLeadingZeros(spread))
          : (byte) 64;
      final int valueBytes = bitsToBytes((long) count * (valueBitWidth & 0xFF));
      final int totalBytes =
          checkedEncodedLength((long) PLAIN_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize + valueBytes);
      ensureOutputCapacity(totalBytes);

      int position = 0;
      output[position++] = encodingKind;
      output[position++] = tagKind;
      putInt(output, position, count);
      position += Integer.BYTES;
      putLong(output, position, min);
      position += Long.BYTES;
      putLong(output, position, max);
      position += Long.BYTES;
      putLong(output, position, valueBase);
      position += Long.BYTES;
      output[position++] = valueBitWidth;
      putInt(output, position, dictSize);
      position += Integer.BYTES;
      position = writeTagMetadata(output, position, dictSize);

      if (!bitPacked) {
        for (int i = 0; i < count; i++) {
          putLong(output, position + (i << 3), sortedValues[i]);
        }
      } else {
        bitPackLongs(output, position, sortedValues, 0, count, valueBase, valueBitWidth & 0xFF);
      }
      encodedLength = totalBytes;
      return totalBytes;
    }

    private int encodeNested(final byte encodingKind, final int count, final int dictSize, final long min,
        final long max, final byte tagKind) {
      final long nestedBytes = encodingKind == ENC_COMPACT_ZM
          ? NumberRegionCompact.maxEncodedSize(sortedValues, count)
          : NumberRegionDelta.maxEncodedSize(sortedValues, count);
      final int totalBytes =
          checkedEncodedLength((long) NESTED_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize + nestedBytes);
      ensureOutputCapacity(totalBytes);

      int position = 0;
      output[position++] = encodingKind;
      output[position++] = tagKind;
      putInt(output, position, count);
      position += Integer.BYTES;
      putLong(output, position, min);
      position += Long.BYTES;
      putLong(output, position, max);
      position += Long.BYTES;
      putInt(output, position, dictSize);
      position += Integer.BYTES;
      position = writeTagMetadata(output, position, dictSize);

      // The 57..63-bit nested writers use read/modify/write words. A reusable buffer may carry bits
      // beyond the logical body from the preceding page, including unused high bits of the last byte;
      // clearing the exact nested range restores the zero-initialized-array invariant of the wire.
      Arrays.fill(output, position, totalBytes, (byte) 0);
      final long written = encodingKind == ENC_COMPACT_ZM
          ? NumberRegionCompact.writeCompact(outputView, position, sortedValues, count)
          : NumberRegionDelta.writeDelta(outputView, position, sortedValues, count);
      if (written != nestedBytes || (long) position + written != totalBytes) {
        throw new IllegalStateException(
            "nested number codec size mismatch: expected=" + nestedBytes + " written=" + written);
      }
      encodedLength = totalBytes;
      return totalBytes;
    }

    /**
     * Write every tag's byte-aligned packed run, in tag order, starting at {@code position}.
     *
     * @return the offset one past the last byte written
     */
    private int writePackedRuns(final int position, final int dictSize) {
      int cursor = position;
      for (int t = 0; t < dictSize; t++) {
        final int width = tagWidths[t] & 0xFF;
        if (width == 0) {
          // Constant tag: the directory's min IS the value, so the run is empty.
          continue;
        }
        final int start = tagStart[t];
        final int n = tagCount[t];
        if (width == 64) {
          // Raw longs, so the plain-long kernels read the run with no frame of reference at all.
          for (int i = 0; i < n; i++) {
            putLong(output, cursor + (i << 3), sortedValues[start + i]);
          }
          cursor += n << 3;
        } else {
          bitPackLongs(output, cursor, sortedValues, start, n, tagMin[t], width);
          cursor += packedBytes(n, width);
        }
      }
      return cursor;
    }

    private int writeTagMetadata(final byte[] target, int position, final int dictSize) {
      for (int i = 0; i < dictSize; i++) {
        putInt(target, position, dict[i]);
        position += Integer.BYTES;
      }
      for (int i = 0; i < dictSize; i++) {
        putInt(target, position, tagStart[i]);
        position += Integer.BYTES;
      }
      for (int i = 0; i < dictSize; i++) {
        putInt(target, position, tagCount[i]);
        position += Integer.BYTES;
      }
      for (int i = 0; i < dictSize; i++) {
        putLong(target, position, tagMin[i]);
        position += Long.BYTES;
      }
      for (int i = 0; i < dictSize; i++) {
        putLong(target, position, tagMax[i]);
        position += Long.BYTES;
      }
      return position;
    }

    private void ensureCapacity(final int count) {
      if (count <= dict.length) {
        return;
      }
      final int doubled = dict.length <= (Integer.MAX_VALUE >>> 1)
          ? dict.length << 1
          : Integer.MAX_VALUE;
      allocate(Math.max(count, doubled));
    }

    private void allocate(final int capacity) {
      dict = new int[capacity];
      localIds = new int[capacity];
      tagCount = new int[capacity];
      tagStart = new int[capacity];
      cursor = new int[capacity];
      sortedValues = new long[capacity];
      tagMin = new long[capacity];
      tagMax = new long[capacity];
      tagWidths = new byte[capacity];
      final int outputCapacity = checkedEncodedLength(64L + 36L * capacity);
      output = new byte[outputCapacity];
      outputView = MemorySegment.ofArray(output);
    }

    private void ensureOutputCapacity(final int required) {
      if (required <= output.length) {
        return;
      }
      final long doubled = Math.min((long) Integer.MAX_VALUE, (long) output.length << 1);
      output = new byte[checkedEncodedLength(Math.max((long) required, doubled))];
      outputView = MemorySegment.ofArray(output);
    }
  }

  /**
   * Decode the value at {@code index} (absolute within the sorted payload). O(1) for every encoding
   * except {@link #ENC_DELTA_ZM}, where the sequential prefix sum makes it O(index) — scan loops over
   * delta payloads should use {@link #decodeAllValues} instead.
   */
  public static long decodeValueAt(final MemorySegment payload, final Header h, final int index) {
    if (h.isPerTag()) {
      final int tag = tagOfIndex(h, index);
      if (tag < 0) {
        throw new IndexOutOfBoundsException("value index " + index + " is outside the region's " + h.count + " values");
      }
      return decodeValueInTag(payload, h, tag, index - h.tagStart[tag]);
    }
    if (isDelta(h.encodingKind)) {
      return NumberRegionDelta.readDelta(payload, h.deltaHeader, index);
    }
    if (!isBitPacked(h.encodingKind)) {
      return readUpToLongLE(payload, (long) h.valueBytesOffset + ((long) index << 3));
    }
    if (h.valueBitWidth == 0) {
      // Constant-run (compact codec) — every value equals valueBase.
      return h.valueBase;
    }
    return h.valueBase + bitUnpackLong(payload, h.valueBytesOffset, h.valueBitWidth, index);
  }

  /**
   * Decode the {@code i}-th value of {@code tag} (relative to {@link Header#tagStart}) from a
   * {@link #ENC_PER_TAG_FOR} payload.
   *
   * <p>
   * This is the shape every scan already has — look the tag up once, then walk its range — so the
   * per-tag layout costs a hot loop nothing: the width, base and byte offset are loaded once outside
   * the loop and the body is the same shift-and-mask it was under one page-wide width.
   */
  public static long decodeValueInTag(final MemorySegment payload, final Header h, final int tag, final int i) {
    final int width = h.tagWidth[tag] & 0xFF;
    if (width == 0) {
      return h.tagMin[tag];
    }
    final int offset = h.tagValueOffset[tag];
    if (width == 64) {
      return readUpToLongLE(payload, (long) offset + ((long) i << 3));
    }
    return h.tagDecodeBase[tag] + bitUnpackLong(payload, offset, width, i);
  }

  /**
   * The tag whose value range contains {@code index}, or {@code -1} when the index is out of range.
   *
   * <p>
   * Binary search over {@code tagStart}, which is ascending by construction. Only the compatibility
   * entry points that take an absolute index need it; every scan door knows its tag already.
   */
  public static int tagOfIndex(final Header h, final int index) {
    if (index < 0 || index >= h.count) {
      return -1;
    }
    final int[] starts = h.tagStart;
    int lo = 0;
    int hi = h.dictSize - 1;
    while (lo < hi) {
      final int mid = (lo + hi + 1) >>> 1;
      if (starts[mid] <= index) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /**
   * Local tag id for a parent tag value, or {@code -1} when absent. O(dictSize). The tag value is
   * interpreted according to {@link Header#tagKind}: nameKey for {@link #TAG_KIND_NAME}, pathNodeKey
   * (int-truncated) for {@link #TAG_KIND_PATH_NODE}.
   */
  public static int lookupTag(final Header h, final int tag) {
    final int[] dict = h.dict;
    if (dict == null)
      return -1;
    for (int i = 0; i < h.dictSize; i++) {
      if (dict[i] == tag)
        return i;
    }
    return -1;
  }

  /** Bulk-decode all values (across all tags) into {@code out}. */
  public static void decodeAllValues(final MemorySegment payload, final Header h, final long[] out) {
    final int count = h.count;
    if (h.isPerTag()) {
      // Tag by tag, so the width, base and offset are hoisted out of the inner loop exactly once.
      for (int tag = 0; tag < h.dictSize; tag++) {
        final int width = h.tagWidth[tag] & 0xFF;
        final int start = h.tagStart[tag];
        final int n = h.tagCount[tag];
        if (width == 0) {
          final long constant = h.tagMin[tag];
          for (int i = 0; i < n; i++) {
            out[start + i] = constant;
          }
        } else if (width == 64) {
          final long offset = h.tagValueOffset[tag];
          for (int i = 0; i < n; i++) {
            out[start + i] = readUpToLongLE(payload, offset + ((long) i << 3));
          }
        } else {
          final int offset = h.tagValueOffset[tag];
          final long base = h.tagDecodeBase[tag];
          for (int i = 0; i < n; i++) {
            out[start + i] = base + bitUnpackLong(payload, offset, width, i);
          }
        }
      }
      return;
    }
    if (isDelta(h.encodingKind)) {
      // Single sequential prefix sum — the fast path for delta payloads.
      NumberRegionDelta.decodeAll(payload, h.deltaHeader, out);
      return;
    }
    if (!isBitPacked(h.encodingKind)) {
      int off = h.valueBytesOffset;
      for (int i = 0; i < count; i++, off += 8) {
        out[i] = readUpToLongLE(payload, off);
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

  private static void validateEncodeInput(final long[] values, final int[] parentTags, final int count,
      final byte tagKind) {
    Objects.requireNonNull(values, "values");
    Objects.requireNonNull(parentTags, "parentTags");
    if (count < 0 || count > values.length || count > parentTags.length) {
      throw new IllegalArgumentException(
          "count=" + count + " values.length=" + values.length + " parentTags.length=" + parentTags.length);
    }
    if (tagKind != TAG_KIND_NAME && tagKind != TAG_KIND_PATH_NODE) {
      throw new IllegalArgumentException("tagKind=" + tagKind);
    }
  }

  private static int checkedEncodedLength(final long length) {
    if (length < 0L || length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("encoded number region is too large: " + length + " bytes");
    }
    return (int) length;
  }

  private static void putInt(final byte[] target, final int offset, final int value) {
    INT_LE.set(target, offset, value);
  }

  private static void putLong(final byte[] target, final int offset, final long value) {
    LONG_LE.set(target, offset, value);
  }

  private static int bitsToBytes(final long bits) {
    return (int) ((bits + 7L) >>> 3);
  }

  /**
   * Pack {@code values[from, from + count)} as {@code bitWidth}-bit residuals of {@code base}, low
   * bits first, starting at {@code out[outOff]}.
   *
   * <p>
   * Every byte of {@code [outOff, outOff + packedBytes(count, bitWidth))} is assigned, the trailing
   * partial byte included, so a reused output buffer cannot leak the previous page's bits onto the
   * wire. Widths above 57 must not come here — {@code v << bitsInBuf} would drop the high bits — and
   * do not: the callers cap at {@link BitUnpackSimd#MAX_BIT_WIDTH} and store wider tags raw.
   */
  private static void bitPackLongs(final byte[] out, final int outOff, final long[] values, final int from,
      final int count, final long base, final int bitWidth) {
    long buf = 0L;
    int bitsInBuf = 0;
    int writePos = outOff;
    final long mask = bitWidth == 64
        ? ~0L
        : (1L << bitWidth) - 1L;
    for (int k = 0; k < count; k++) {
      final int i = from + k;
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

  private static long bitUnpackLong(final MemorySegment data, final int baseOff, final int bitWidth, final int index) {
    final long bitOff = (long) index * bitWidth;
    final int byteOff = (int) (bitOff >>> 3) + baseOff;
    final int bitInByte = (int) (bitOff & 7L);
    final long w0 = readUpToLongLE(data, byteOff);
    final long mask = bitWidth == 64
        ? ~0L
        : (1L << bitWidth) - 1L;
    long v = (w0 >>> bitInByte) & mask;
    final int bitsConsumed = 64 - bitInByte;
    if (bitsConsumed < bitWidth) {
      final int extra = bitWidth - bitsConsumed;
      final long next = readByteUnsigned(data, byteOff + 8);
      v |= (next & ((1L << extra) - 1L)) << bitsConsumed;
    }
    return v;
  }

  private static long readUpToLongLE(final MemorySegment data, final long off) {
    final long avail = data.byteSize() - off;
    if (avail >= Long.BYTES) {
      return data.get(LE.LONG, off);
    }
    long v = 0L;
    for (long i = 0; i < avail; i++) {
      v |= ((long) (data.get(ValueLayout.JAVA_BYTE, off + i) & 0xFF)) << (i << 3);
    }
    return v;
  }

  private static long readByteUnsigned(final MemorySegment data, final long off) {
    return off < data.byteSize()
        ? (data.get(ValueLayout.JAVA_BYTE, off) & 0xFFL)
        : 0L;
  }

}

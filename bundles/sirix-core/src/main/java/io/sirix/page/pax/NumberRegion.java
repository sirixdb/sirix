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

  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

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

    public Header parseInto(final MemorySegment payload) {
      final RegionReader bb = new RegionReader(payload);
      encodingKind = bb.readByte();
      tagKind = bb.readByte();
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
      if (dict == null || dict.length < dictSize) dict = new int[Math.max(4, dictSize)];
      if (tagStart == null || tagStart.length < dictSize) tagStart = new int[Math.max(4, dictSize)];
      if (tagCount == null || tagCount.length < dictSize) tagCount = new int[Math.max(4, dictSize)];
      for (int i = 0; i < dictSize; i++) dict[i] = bb.readInt();
      for (int i = 0; i < dictSize; i++) tagStart[i] = bb.readInt();
      for (int i = 0; i < dictSize; i++) tagCount[i] = bb.readInt();
      if (hasZoneMap(encodingKind)) {
        if (tagMin == null || tagMin.length < dictSize) tagMin = new long[Math.max(4, dictSize)];
        if (tagMax == null || tagMax.length < dictSize) tagMax = new long[Math.max(4, dictSize)];
        for (int i = 0; i < dictSize; i++) tagMin[i] = bb.readLong();
        for (int i = 0; i < dictSize; i++) tagMax[i] = bb.readLong();
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
   * Thread-confined reusable number-region encoder. One instance may serve any number of pages on
   * one thread, but it must not be shared or entered recursively. The output array is scratch: only
   * {@code [0, encodeInto(...))} is valid, and the next encode overwrites it.
   *
   * <p>The page writer must copy that prefix into page-owned storage before another encode. In
   * particular, a heap {@link MemorySegment} view of {@link #output()} must never be installed through
   * {@link RegionTable#setSegment(byte, MemorySegment)} because later pages would overwrite it.
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
    public int encodeInto(final long[] values, final int[] parentTags, final int count,
        final byte tagKind) {
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

      final long spread = count == 0 ? 0L : max - min;
      final boolean bitPacked =
          count > 0 && spread >= 0 && spread < (1L << BitUnpackSimd.MAX_BIT_WIDTH);

      // Delta-of-delta must win on both residual width and exact encoded bytes because selecting it
      // trades away random access and SIMD scans.
      if (deltaWriteEnabled() && count >= MIN_DELTA_COUNT) {
        final int forBitWidth = bitPacked ? Math.max(1, 64 - Long.numberOfLeadingZeros(spread)) : 64;
        final int deltaBitWidth = NumberRegionDelta.computeBitWidth(sortedValues, count);
        if (deltaBitWidth < forBitWidth) {
          final long forValueRegionBytes = 9L + bitsToBytes((long) count * forBitWidth);
          final long deltaRegionBytes =
              NumberRegionDelta.headerBytes(count) + NumberRegionDelta.bodyBytes(count, deltaBitWidth);
          if (deltaRegionBytes < forValueRegionBytes) {
            return encodeNested(ENC_DELTA_ZM, count, dictSize, min, max, tagKind);
          }
        }
      }

      if (bitPacked && compactWriteEnabled()) {
        return encodeNested(ENC_COMPACT_ZM, count, dictSize, min, max, tagKind);
      }
      return encodePlain(count, dictSize, min, max, spread, bitPacked, tagKind);
    }

    /** Mutable scratch buffer. The next call to {@link #encodeInto} overwrites it. */
    public byte[] output() {
      return output;
    }

    /** Exact length returned by the most recent successful encode, or zero after a failed attempt. */
    public int encodedLength() {
      return encodedLength;
    }

    private int encodePlain(final int count, final int dictSize, final long min, final long max,
        final long spread, final boolean bitPacked, final byte tagKind) {
      final byte encodingKind = bitPacked ? ENC_BIT_PACKED_ZM : ENC_PLAIN_LONG_ZM;
      final long valueBase = bitPacked ? min : 0L;
      final byte valueBitWidth = bitPacked
          ? (byte) Math.max(1, 64 - Long.numberOfLeadingZeros(spread))
          : (byte) 64;
      final int valueBytes = bitsToBytes((long) count * (valueBitWidth & 0xFF));
      final int totalBytes = checkedEncodedLength(
          (long) PLAIN_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize + valueBytes);
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
        bitPackLongs(output, position, sortedValues, count, valueBase, valueBitWidth & 0xFF);
      }
      encodedLength = totalBytes;
      return totalBytes;
    }

    private int encodeNested(final byte encodingKind, final int count, final int dictSize,
        final long min, final long max, final byte tagKind) {
      final long nestedBytes = encodingKind == ENC_COMPACT_ZM
          ? NumberRegionCompact.maxEncodedSize(sortedValues, count)
          : NumberRegionDelta.maxEncodedSize(sortedValues, count);
      final int totalBytes = checkedEncodedLength(
          (long) NESTED_FIXED_HEADER_BYTES + (long) BYTES_PER_TAG * dictSize + nestedBytes);
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
      final int doubled = dict.length <= (Integer.MAX_VALUE >>> 1) ? dict.length << 1 : Integer.MAX_VALUE;
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
   * Decode the value at {@code index} (absolute within the sorted payload).
   * O(1) for every encoding except {@link #ENC_DELTA_ZM}, where the sequential
   * prefix sum makes it O(index) — scan loops over delta payloads should use
   * {@link #decodeAllValues} instead.
   */
  public static long decodeValueAt(final MemorySegment payload, final Header h, final int index) {
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
  public static void decodeAllValues(final MemorySegment payload, final Header h, final long[] out) {
    final int count = h.count;
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

  private static long bitUnpackLong(final MemorySegment data, final int baseOff, final int bitWidth,
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
    return off < data.byteSize() ? (data.get(ValueLayout.JAVA_BYTE, off) & 0xFFL) : 0L;
  }

}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import io.sirix.node.LE;

import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * The floating-point counterpart of {@link NumberRegion}: per-page, per-tag columns of
 * double-typed field values with per-tag zone maps — ALP-encoded where the data allows it, which
 * for real-world doubles is almost always.
 *
 * <h2>Why a second numeric region</h2>
 *
 * <p>{@link NumberRegion} is long-only by construction. A field whose value happened to be
 * fractional on some records never entered a column at all, and the completeness oracle (rightly)
 * refused every page holding one. With both regions present, a numeric predicate over a mixed
 * page is the SUM of two kernel passes, and the oracle becomes
 * {@code longCount + doubleCount == anchorSlots}. This is how a typed column store treats it —
 * one column per physical type, each with its own summaries.
 *
 * <h2>ALP</h2>
 *
 * <p>Doubles do not bit-pack, but the doubles people actually store are decimals — prices,
 * measurements, {@code 1990.5} — that are short decimal strings rendered into binary floating
 * point. ALP (Afroozeh &amp; Boncz; the codec DuckDB adopted and BtrBlocks' pseudodecimal refined
 * into) exploits exactly that: encode {@code d} as the integer
 * {@code I = round(d * 10^e / 10^f)} for a per-tag exponent pair, and the column of doubles
 * becomes a column of small integers that FOR+bit-packs like any other. {@code e} scales
 * fractional digits up; {@code f} scales trailing zeros away.
 *
 * <p>Losslessness is not assumed, it is PROVED per value: the encoder decodes each candidate with
 * the exact arithmetic the reader uses and keeps the integer only when the round-trip reproduces
 * the original bits. Values that fail — true irrationals, exotic exponents — are stored verbatim
 * in a small exception list, with a placeholder in the packed stream. A tag where exceptions
 * exceed a quarter of the values falls to {@link #ENC_ALP_RD}, and to {@link #ENC_PLAIN} when even
 * the bit split does not pay; nothing is ever approximated.
 *
 * <p>ALP carries a value by its DOUBLE IMAGE, which is only sound for a value that IS a double —
 * or a decimal that equals its own image. A tag of genuine decimals (prices: {@code 19.99},
 * {@code 100.10}) takes {@link #ENC_DEC} instead and carries the unscaled integers themselves, so
 * a comparison stays in decimal space; the encoder tries that domain FIRST and refuses the tag
 * outright rather than stand a rounded double in for a decimal.
 *
 * <p>The scan benefit is the point of the exercise: decode is {@code I * 10^f / 10^e}, a
 * multiplication by a positive constant, so it is MONOTONIC in {@code I} — and a range predicate
 * over the doubles is therefore a range predicate over the packed INTEGERS. The kernel translates
 * {@code [dlo, dhi]} into integer bounds once per tag and runs the same vectorized bit-packed
 * count the long column uses, never materializing a double. Exceptions are corrected scalar,
 * proportional to their count rather than the column's.
 *
 * <h2>Wire format</h2>
 *
 * <pre>
 * byte             version       // VERSION_V1
 * byte             tagKind       // NumberRegion.TAG_KIND_NAME or TAG_KIND_PATH_NODE
 * int              count
 * int              dictSize
 * int[dictSize]    dict
 * int[dictSize]    tagStart
 * int[dictSize]    tagCount
 * double[dictSize] tagMin        // NaN-free by construction: JSON has no NaN literal
 * double[dictSize] tagMax
 * int[dictSize]    tagEncOffset  // absolute byte offset of each tag's encoding block
 * int[dictSize]    tagPosOffset  // absolute byte offset of the tag's field-ordinal shorts, or
 *                                 // -1 when the ordinals are the IDENTITY (value k is the field's
 *                                 // k-th slot) — true of every all-double field, which makes the
 *                                 // list free exactly where it would have been pure overhead
 * per tag:
 *   byte enc                     // ENC_PLAIN | ENC_ALP | ENC_ALP_RD | ENC_DEC
 *   PLAIN: double[tagCount] values (LE)
 *   ALP:   byte e, byte f, long forBase, byte bitWidth, short exceptionCount,
 *          byte[ceil(tagCount*bitWidth/8)] packed (I - forBase), little-endian bit order,
 *          short[exceptionCount] positions (tag-local, ascending),
 *          long[exceptionCount]  raw IEEE bits
 *   DEC:   the ALP layout byte for byte, with e = the common decimal scale, f = 0 and
 *          exceptionCount REQUIRED to be 0 (an exact-decimal tag rounds nothing, so a block
 *          claiming an exception is corrupt and the page keeps its record path). Only the
 *          MEANING of the integers differs, and the kernels branch on enc to enforce it
 *   RD:    byte rightWidth (48..56), byte dictSize (1..16), byte codeWidth (<= 4),
 *          short exceptionCount,
 *          long[dictSize] left-part dictionary,
 *          byte[ceil(tagCount*codeWidth/8)] packed left-part codes,
 *          byte[ceil(tagCount*rightWidth/8)] packed right parts, little-endian bit order,
 *          short[exceptionCount] positions (tag-local, ascending),
 *          short[exceptionCount] left parts (those whose left part missed the dictionary)
 *  * per tag, after every encoding block:
 *   short[tagCount] fieldOrdinals  // ascending: the value's ordinal among the FIELD's slots on
 *                                  // the page, counting BOTH numeric types — the projection a
 *                                  // versioned merge needs to split one liveness bitmap into a
 *                                  // long-column mask and a double-column mask
 * </pre>
 *
 * <p>A reader meeting a different version declines to parse and the page keeps its record path —
 * absence of this region is a state every caller already handles.
 */
public final class DoubleRegion {

  public static final byte VERSION_V1 = 1;

  /** Verbatim little-endian doubles. */
  public static final byte ENC_PLAIN = 0;

  /** ALP: FOR+bit-packed decimal integers plus a raw-value exception list. */
  public static final byte ENC_ALP = 1;

  /**
   * ALP-RD ("real doubles"): for the values the decimal scheme cannot capture — true reals,
   * embeddings, full-precision noise. Each double's 64 bits split at a per-tag point: the LEFT
   * part (sign, exponent, top mantissa) is dictionary-coded, because real-world reals cluster in
   * magnitude and produce only a handful of distinct left parts; the RIGHT part (low mantissa) is
   * bit-packed verbatim. Losslessness is STRUCTURAL — concatenation reproduces the bits by
   * construction, so unlike ALP nothing needs a round-trip proof; only dictionary membership can
   * fail, and those left parts go to the exception list. Typical yield: 49-52 bits per value
   * instead of 64 where ALP would have surrendered to PLAIN.
   */
  public static final byte ENC_ALP_RD = 2;

  /**
   * EXACT DECIMAL: the tag's values are decimals carried as their own unscaled integers at a common
   * scale, stored in the {@link #ENC_ALP} block layout with {@code e = scale}, {@code f = 0} and no
   * exceptions.
   *
   * <p>A separate kind rather than a flag on ALP, because it changes what a COMPARISON means. An
   * ALP tag's integers are a lossless recoding of doubles, so a double-domain bound may be
   * translated into integer space and answered there. These integers are not doubles at all: they
   * are the decimals themselves, and two distinct decimals can share a double image
   * ({@code 1000.25000000000001} and {@code 1000.25} do). Answering such a tag from a double bound
   * silently puts rows on the wrong side, so the kernels REFUSE a double-bounded query here and
   * the caller keeps its record path. Only a bound supplied in decimal space
   * ({@code DoubleRegionSimd.countDecTagRangeMasked}) can serve it.
   */
  public static final byte ENC_DEC = 3;

  /**
   * The split keeps the left part at 8..16 bits (right 48..56): wide enough that the dictionary
   * captures magnitude clusters, narrow enough that an exception's left part stores as a short —
   * and the right width stays within the bit-unpack kernel's ceiling.
   */
  private static final int MIN_RIGHT_WIDTH = 48;
  private static final int MAX_RIGHT_WIDTH = 56;

  /**
   * Left-part dictionary ceiling; codes fit 4 bits. Past one vector register's worth of entries
   * the kernel gathers with a blend cascade — one {@code selectFrom} per register — so the ceiling
   * is a cost knob, not a hardware limit: 16 doubles the dictionary for one extra shuffle.
   */
  static final int RD_MAX_DICT = 16;

  private static final int FIXED_BYTES = 1 + 1 + 4 + 4;

  private static final int BYTES_PER_TAG = 4 + 4 + 4 + 8 + 8 + 4 + 4;

  /** Exact powers of ten; every entry is exactly representable as a double. */
  static final double[] EXP10 = {
      1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14
  };

  private static final int MAX_EXPONENT = EXP10.length - 1;

  /**
   * Largest decimal scale this column can carry EXACTLY, as {@code I = unscaled} at
   * {@code e = scale, f = 0}.
   *
   * <p>Exposed for the page builder's admission test. A decimal admitted at or below this scale is
   * stored as its own unscaled integer, so both the stored values and a scan's threshold live as
   * integers at the same decimal scale and the comparison the kernel already performs IS a
   * decimal-space comparison — exact, with no double ever standing in for the value.
   */
  public static final int MAX_DECIMAL_SCALE = MAX_EXPONENT;

  /** Exact powers of ten as longs, for rescaling an unscaled decimal without leaving integers. */
  private static final long[] POW10_LONG = {
      1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L,
      1_000_000_000L, 10_000_000_000L, 100_000_000_000L, 1_000_000_000_000L, 10_000_000_000_000L,
      100_000_000_000_000L
  };

  /** {@code |v| <= POW10_LIMIT[k]} iff {@code v * 10^k} cannot overflow a long. Division-free. */
  private static final long[] POW10_LIMIT = new long[POW10_LONG.length];

  static {
    for (int k = 0; k < POW10_LONG.length; k++) {
      POW10_LIMIT[k] = Long.MAX_VALUE / POW10_LONG[k];
    }
  }

  /** {@code 10^e} as an exactly-representable double, for {@code e} within the ALP range. */
  public static double exp10(final int e) {
    return EXP10[e];
  }

  /**
   * Magnitude ceiling for the scaled value before rounding. Past 2^51 the double grid is coarser
   * than the integers, and round-trip verification would be deciding on the wrong side of the
   * rounding anyway.
   */
  private static final double MAX_SCALED = 0x1p51;

  private DoubleRegion() {
    throw new AssertionError("no instances");
  }

  /** The exact decode the encoder verifies against and the reader applies. */
  static double alpDecode(final long encoded, final int e, final int f) {
    return encoded * EXP10[f] / EXP10[e];
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
    /** Per-tag encoding: {@link #ENC_PLAIN} or {@link #ENC_ALP}. */
    public byte[] tagEnc;
    /** PLAIN: absolute byte offset of the tag's doubles. ALP: of the packed integers. */
    public int[] tagDataOffset;
    public byte[] alpE;
    public byte[] alpF;
    public long[] alpForBase;
    public byte[] alpBitWidth;
    /** ALP: raw-double exceptions. ALP-RD: left-part exceptions. Same per-tag bookkeeping. */
    public short[] alpExceptionCount;
    /** Absolute byte offset of the tag's exception positions; payloads follow them. */
    public int[] alpExceptionOffset;
    public byte[] rdRightWidth;
    public byte[] rdCodeWidth;
    public byte[] rdDictSize;
    /** Absolute byte offset of the tag's left-part dictionary (longs). */
    public int[] rdDictOffset;
    /** Absolute byte offset of the packed left codes. */
    public int[] rdCodesOffset;
    /** Absolute byte offset of the tag's field-ordinal shorts. */
    public int[] tagPosOffset;
    /** {@code tagDataOffset} holds the packed RIGHT bits for an RD tag. */

    /**
     * Parse {@code payload} into this instance.
     *
     * @return {@code this}, or {@code null} for an absent, truncated or other-version payload —
     *         each meaning "no double column", never a wrong one
     */
    public Header parseInto(final MemorySegment payload) {
      if (payload == null || payload.byteSize() < FIXED_BYTES) {
        return null;
      }
      final RegionReader in = new RegionReader(payload);
      if (in.readByte() != VERSION_V1) {
        return null;
      }
      final byte readTagKind = in.readByte();
      final int readCount = in.readInt();
      final int readDictSize = in.readInt();
      if (readCount < 0 || readDictSize < 0
          || payload.byteSize() < (long) FIXED_BYTES + (long) readDictSize * BYTES_PER_TAG) {
        return null;
      }
      tagKind = readTagKind;
      count = readCount;
      dictSize = readDictSize;
      ensureTagCapacity(dictSize);
      in.readInts(dict, dictSize);
      in.readInts(tagStart, dictSize);
      in.readInts(tagCount, dictSize);
      for (int i = 0; i < dictSize; i++) {
        tagMin[i] = Double.longBitsToDouble(in.readLong());
      }
      for (int i = 0; i < dictSize; i++) {
        tagMax[i] = Double.longBitsToDouble(in.readLong());
      }
      if (!readTagOffsets(in, payload)) {
        return null;
      }
      for (int t = 0; t < dictSize; t++) {
        if (!parseTagBlock(payload, t)) {
          return null;
        }
      }
      return this;
    }

    /** Grow the per-tag arrays to hold {@code dictSize} tags, reusing them when they already do. */
    private void ensureTagCapacity(final int dictSize) {
      if (dict == null || dict.length < dictSize) {
        final int cap = Math.max(4, dictSize);
        dict = new int[cap];
        tagStart = new int[cap];
        tagCount = new int[cap];
        tagMin = new double[cap];
        tagMax = new double[cap];
        tagEnc = new byte[cap];
        tagDataOffset = new int[cap];
        alpE = new byte[cap];
        alpF = new byte[cap];
        alpForBase = new long[cap];
        alpBitWidth = new byte[cap];
        alpExceptionCount = new short[cap];
        alpExceptionOffset = new int[cap];
        rdRightWidth = new byte[cap];
        rdCodeWidth = new byte[cap];
        rdDictSize = new byte[cap];
        rdDictOffset = new int[cap];
        rdCodesOffset = new int[cap];
        tagPosOffset = new int[cap];
      }
    }

    /** Read the per-tag block and ordinal-vector offsets, bounds-checking each against the page. */
    private boolean readTagOffsets(final RegionReader in, final MemorySegment payload) {
      for (int i = 0; i < dictSize; i++) {
        final int off = in.readInt();
        if (off < 0 || off >= payload.byteSize()) {
          return false;
        }
        tagDataOffset[i] = off;  // provisionally the block offset; adjusted per encoding below
      }
      for (int i = 0; i < dictSize; i++) {
        final int off = in.readInt();
        if (off != -1
            && (off < 0 || payload.byteSize() < (long) off + (long) tagCount[i] * Short.BYTES)) {
          return false;
        }
        tagPosOffset[i] = off;
      }
      return true;
    }

    /** Parse tag {@code t}'s block header and validate every offset it implies. */
    private boolean parseTagBlock(final MemorySegment payload, final int t) {
      final RegionReader block = new RegionReader(payload, tagDataOffset[t]);
      final byte enc = block.readByte();
      tagEnc[t] = enc;
      if (enc == ENC_PLAIN) {
        tagDataOffset[t] = block.position();
        return payload.byteSize()
            >= (long) tagDataOffset[t] + (long) tagCount[t] * Double.BYTES;
      }
      if (enc == ENC_ALP || enc == ENC_DEC) {
        return parseAlpTagBlock(payload, block, t, enc);
      }
      if (enc == ENC_ALP_RD) {
        return parseRdTagBlock(payload, block, t);
      }
      return false;
    }

    /**
     * Parse an ALP or exact-decimal block. Identical layout; only the MEANING of the integers
     * differs, and the kernels branch on {@code tagEnc} to enforce it. Validated by exactly the
     * same bounds either way.
     */
    private boolean parseAlpTagBlock(final MemorySegment payload, final RegionReader block,
        final int t, final byte enc) {
      alpE[t] = block.readByte();
      alpF[t] = block.readByte();
      alpForBase[t] = block.readLong();
      alpBitWidth[t] = block.readByte();
      alpExceptionCount[t] = block.readShort();
      if ((alpE[t] & 0xFF) > MAX_EXPONENT || (alpF[t] & 0xFF) > MAX_EXPONENT
          || (alpBitWidth[t] & 0xFF) > Long.SIZE
          || alpExceptionCount[t] < 0 || alpExceptionCount[t] > tagCount[t]
          || (enc == ENC_DEC && alpExceptionCount[t] != 0)) {
        // An exact-decimal tag rounds nothing, so it can have no exceptions; a block claiming
        // otherwise is corrupt and the page keeps its record path.
        return false;
      }
      tagDataOffset[t] = block.position();
      final long packedBytes = ((long) tagCount[t] * (alpBitWidth[t] & 0xFF) + 7) >>> 3;
      alpExceptionOffset[t] = (int) (tagDataOffset[t] + packedBytes);
      final long need =
          alpExceptionOffset[t] + (long) alpExceptionCount[t] * (Short.BYTES + Long.BYTES);
      return payload.byteSize() >= need
          && exceptionPositionsValid(payload, alpExceptionOffset[t], alpExceptionCount[t],
                                     tagCount[t]);
    }

    /** Parse an ALP-RD block: a dictionary of left parts, packed codes, then the right parts. */
    private boolean parseRdTagBlock(final MemorySegment payload, final RegionReader block,
        final int t) {
      rdRightWidth[t] = block.readByte();
      rdDictSize[t] = block.readByte();
      rdCodeWidth[t] = block.readByte();
      alpExceptionCount[t] = block.readShort();
      final int rw = rdRightWidth[t] & 0xFF;
      final int ds = rdDictSize[t] & 0xFF;
      final int cw = rdCodeWidth[t] & 0xFF;
      if (rw < MIN_RIGHT_WIDTH || rw > MAX_RIGHT_WIDTH || ds < 1 || ds > RD_MAX_DICT
          || cw > 4 || alpExceptionCount[t] < 0 || alpExceptionCount[t] > tagCount[t]) {
        return false;
      }
      rdDictOffset[t] = block.position();
      rdCodesOffset[t] = (int) (rdDictOffset[t] + (long) ds * Long.BYTES);
      final long codeBytes = ((long) tagCount[t] * cw + 7) >>> 3;
      tagDataOffset[t] = (int) (rdCodesOffset[t] + codeBytes);
      final long rightBytes = ((long) tagCount[t] * rw + 7) >>> 3;
      alpExceptionOffset[t] = (int) (tagDataOffset[t] + rightBytes);
      final long need =
          alpExceptionOffset[t] + (long) alpExceptionCount[t] * (Short.BYTES + Short.BYTES);
      return payload.byteSize() >= need
          && exceptionPositionsValid(payload, alpExceptionOffset[t], alpExceptionCount[t],
                                     tagCount[t]);
    }
  }

  /**
   * Advance the per-field ordinal counter and return the CURRENT slot's ordinal.
   *
   * <p>This is the single definition of the wire's ordinal semantics: the ordinal counts the
   * field's numeric slots — BOTH types — in slot order, and both producers (the seal-time writer
   * in {@code PageKind.buildRegionTable} and the reconstruction rebuild in
   * {@code KeyValueLeafPage}) must advance it on every fused number slot, long or double, columned
   * or (Big*) skipped. Two independently-maintained counters drifting is how a versioned merge
   * splits a liveness bitmap between the wrong columns, so the increment lives here and nowhere
   * else.
   */
  public static int nextFieldOrdinal(final Int2IntOpenHashMap counter, final int nameKey) {
    return counter.addTo(nameKey, 1);
  }

  /**
   * The field ordinal of the tag's {@code k}-th value: its position among the FIELD's slots on the
   * page, counting both numeric types. Ascending by construction, because the writer appends
   * values in slot order.
   */
  public static int fieldOrdinalAt(final MemorySegment payload, final Header h, final int t,
      final int k) {
    final int off = h.tagPosOffset[t];
    return off < 0 ? k : payload.get(LE.SHORT, off + (long) k * Short.BYTES) & 0xFFFF;
  }

  /**
   * Exception positions must be strictly ascending and inside the tag, checked ONCE at parse.
   *
   * <p>The masked merge indexes a liveness bitmap sized to the tag with these shorts; an
   * out-of-range position from a corrupt payload would throw out of the kernel instead of taking
   * the decline-to-records path every other corrupt-region shape takes. Validating here keeps the
   * kernels branch-free and the failure mode uniform: unreadable means absent, never an exception.
   */
  private static boolean exceptionPositionsValid(final MemorySegment payload, final long posBase,
      final int exceptions, final int tagCount) {
    int prev = -1;
    for (int x = 0; x < exceptions; x++) {
      final int pos = payload.get(LE.SHORT, posBase + (long) x * Short.BYTES) & 0xFFFF;
      if (pos <= prev || pos >= tagCount) {
        return false;
      }
      prev = pos;
    }
    return true;
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

  /**
   * The value at tag-local index {@code local} of tag {@code t}.
   *
   * <p>Exceptions first — their packed slot holds a placeholder, not a value — then the packed
   * integer through the exact decode. The exception probe is a binary search over the ascending
   * position list, so a scalar walk over an ALP tag stays {@code O(n log x)} for {@code x}
   * exceptions rather than {@code O(n·x)}.
   */
  public static double decodeValueAt(final MemorySegment payload, final Header h, final int t,
      final int local) {
    if (h.tagEnc[t] == ENC_PLAIN) {
      return Double.longBitsToDouble(
          payload.get(LE.LONG, h.tagDataOffset[t] + (long) local * Double.BYTES));
    }
    if (h.tagEnc[t] == ENC_ALP_RD) {
      final int rw = h.rdRightWidth[t] & 0xFF;
      final long right = BitUnpackSimd.decodeAt(payload, h.tagDataOffset[t], rw,
                                                BitUnpackSimd.maskFor(rw), local);
      final long left = rdLeftPartAt(payload, h, t, local);
      return Double.longBitsToDouble((left << rw) | right);
    }
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions > 0) {
      final long posBase = h.alpExceptionOffset[t];
      int lo = 0;
      int hi = exceptions - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int pos = payload.get(LE.SHORT, posBase + (long) mid * Short.BYTES) & 0xFFFF;
        if (pos == local) {
          return Double.longBitsToDouble(payload.get(LE.LONG,
              posBase + (long) exceptions * Short.BYTES + (long) mid * Long.BYTES));
        }
        if (pos < local) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
    }
    final int width = h.alpBitWidth[t] & 0xFF;
    final long packed = width == 0
        ? 0L
        : BitUnpackSimd.decodeAt(payload, h.tagDataOffset[t], width, BitUnpackSimd.maskFor(width),
                                 local);
    return alpDecode(h.alpForBase[t] + packed, h.alpE[t] & 0xFF, h.alpF[t] & 0xFF);
  }

  /**
   * The left part of RD entry {@code local}: the exception list first (its packed code is a
   * placeholder), then the dictionary through the packed code.
   */
  static long rdLeftPartAt(final MemorySegment payload, final Header h, final int t,
      final int local) {
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions > 0) {
      final long posBase = h.alpExceptionOffset[t];
      int lo = 0;
      int hi = exceptions - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int pos = payload.get(LE.SHORT, posBase + (long) mid * Short.BYTES) & 0xFFFF;
        if (pos == local) {
          return payload.get(LE.SHORT,
              posBase + (long) exceptions * Short.BYTES + (long) mid * Short.BYTES) & 0xFFFFL;
        }
        if (pos < local) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
    }
    final int cw = h.rdCodeWidth[t] & 0xFF;
    final long code = cw == 0
        ? 0L
        : BitUnpackSimd.decodeAt(payload, h.rdCodesOffset[t], cw, BitUnpackSimd.maskFor(cw), local);
    return payload.get(LE.LONG, h.rdDictOffset[t] + code * Long.BYTES);
  }

  /**
   * Encode from parallel arrays, regrouping by tag and choosing ALP or PLAIN per tag.
   *
   * @return the payload, or {@code null} when {@code count == 0}
   */
  public static byte[] encode(final double[] values, final int[] tags, final int[] ordinals,
      final int count, final byte tagKind) {
    return encode(values, null, null, tags, ordinals, count, tagKind);
  }

  /**
   * As {@link #encode(double[], int[], int[], int, byte)}, additionally carrying each value's EXACT
   * decimal form when it has one.
   *
   * <p>{@code decScales[i] < 0} means "value {@code i} is a plain double"; otherwise the value is
   * exactly {@code decUnscaled[i] * 10^-decScales[i]} and {@code values[i]} is only its nearest
   * double, used for zone-map bounds. A tag whose values are ALL decimal-sourced is encoded at
   * {@code e = max scale, f = 0} with the unscaled integers themselves, so nothing is rounded and
   * the kernel's integer comparison is a decimal-space comparison.
   *
   * @param decUnscaled exact unscaled values, or {@code null} when no value has a decimal form
   * @param decScales exact scales, or {@code null}; negative marks a plain double
   */
  public static byte[] encode(final double[] values, final long @Nullable [] decUnscaled,
      final int @Nullable [] decScales, final int[] tags, final int[] ordinals, final int count,
      final byte tagKind) {
    if (values == null || tags == null || ordinals == null || count <= 0) {
      return null;
    }
    final int[] dict = new int[count];
    final int dictSize = buildTagDictionary(tags, count, dict);
    final int[] tagStart = new int[dictSize];
    final int[] tagCount = new int[dictSize];
    final double[] tagMin = new double[dictSize];
    final double[] tagMax = new double[dictSize];
    computeTagBounds(values, tags, dict, dictSize, count, tagStart, tagCount, tagMin, tagMax);

    // Values regrouped by tag, page order within each tag; ordinals travel with their values.
    final double[] grouped = new double[count];
    final int[] groupedOrd = new int[count];
    final boolean haveDecimals = decUnscaled != null && decScales != null;
    final long[] groupedUnscaled = haveDecimals ? new long[count] : null;
    final int[] groupedScale = haveDecimals ? new int[count] : null;
    if (!groupValuesByTag(values, decUnscaled, decScales, tags, ordinals, count, dict, dictSize,
                          tagStart, grouped, groupedOrd, groupedUnscaled, groupedScale)) {
      return null;
    }
    if (haveDecimals) {
      widenDecimalSourcedBounds(dictSize, tagStart, tagCount, groupedScale, tagMin, tagMax);
    }

    // Per-tag blocks, encoded before the header so the offsets are known.
    final byte[][] blocks = new byte[dictSize][];
    for (int d = 0; d < dictSize; d++) {
      final byte[] block =
          encodeTag(grouped, tagStart[d], tagCount[d], groupedUnscaled, groupedScale);
      if (block == null) {
        return encodeWithoutTag(values, decUnscaled, decScales, tags, ordinals, count, tagKind,
                                dict[d]);
      }
      blocks[d] = block;
    }

    int size = FIXED_BYTES + dictSize * BYTES_PER_TAG;
    final int[] blockOffset = new int[dictSize];
    for (int d = 0; d < dictSize; d++) {
      blockOffset[d] = size;
      size += blocks[d].length;
    }
    final int[] posOffset = new int[dictSize];
    size = computePositionOffsets(dictSize, tagStart, tagCount, groupedOrd, size, posOffset);
    return serializeDoubleRegion(size, count, tagKind, dictSize, dict, tagStart, tagCount, tagMin,
                                 tagMax, blockOffset, posOffset, blocks, groupedOrd);
  }

  /** Collect the distinct tags in first-occurrence order into {@code dict}; answers their count. */
  private static int buildTagDictionary(final int[] tags, final int count, final int[] dict) {
    int dictSize = 0;
    for (int i = 0; i < count; i++) {
      if (indexOfOrMinus1(dict, dictSize, tags[i]) < 0) {
        dict[dictSize++] = tags[i];
      }
    }
    return dictSize;
  }

  /** Per-tag cardinality, value bounds and start offset, in one pass over the page's values. */
  private static void computeTagBounds(final double[] values, final int[] tags, final int[] dict,
      final int dictSize, final int count, final int[] tagStart, final int[] tagCount,
      final double[] tagMin, final double[] tagMax) {
    for (int d = 0; d < dictSize; d++) {
      tagMin[d] = Double.POSITIVE_INFINITY;
      tagMax[d] = Double.NEGATIVE_INFINITY;
    }
    for (int i = 0; i < count; i++) {
      final int d = indexOfOrMinus1(dict, dictSize, tags[i]);
      tagCount[d]++;
      if (values[i] < tagMin[d]) tagMin[d] = values[i];
      if (values[i] > tagMax[d]) tagMax[d] = values[i];
    }
    int running = 0;
    for (int d = 0; d < dictSize; d++) {
      tagStart[d] = running;
      running += tagCount[d];
    }
  }

  /**
   * Regroup the values by tag, page order within each tag, carrying ordinals and any decimal
   * images along. {@code false} means an ordinal a short cannot hold — the caller's counter is
   * broken and the page keeps its record path.
   */
  private static boolean groupValuesByTag(final double[] values, final long @Nullable [] decUnscaled,
      final int @Nullable [] decScales, final int[] tags, final int[] ordinals, final int count,
      final int[] dict, final int dictSize, final int[] tagStart, final double[] grouped,
      final int[] groupedOrd, final long @Nullable [] groupedUnscaled,
      final int @Nullable [] groupedScale) {
    final int[] cursor = new int[dictSize];
    for (int i = 0; i < count; i++) {
      final int d = indexOfOrMinus1(dict, dictSize, tags[i]);
      if (ordinals[i] < 0 || ordinals[i] > 0xFFFF) {
        return false;
      }
      final int at = tagStart[d] + cursor[d];
      grouped[at] = values[i];
      if (groupedUnscaled != null) {
        groupedUnscaled[at] = decUnscaled[i];
        groupedScale[at] = decScales[i];
      }
      groupedOrd[at] = ordinals[i];
      cursor[d]++;
    }
    return true;
  }

  /**
   * Widen the bounds of every tag holding a decimal-sourced value by one ulp each way.
   *
   * <p>Such a value's bound came from a double division and can sit up to one ulp off the true
   * decimal. Widening only ever makes "all match" and "none match" fire LESS often, so a matching
   * page can never be pruned away.
   */
  private static void widenDecimalSourcedBounds(final int dictSize, final int[] tagStart,
      final int[] tagCount, final int[] groupedScale, final double[] tagMin,
      final double[] tagMax) {
    for (int d = 0; d < dictSize; d++) {
      for (int k = 0; k < tagCount[d]; k++) {
        if (groupedScale[tagStart[d] + k] >= 0) {
          tagMin[d] = Math.nextDown(tagMin[d]);
          tagMax[d] = Math.nextUp(tagMax[d]);
          break;
        }
      }
    }
  }

  /** Lay out the per-tag ordinal vectors after {@code size}; answers the total payload size. */
  private static int computePositionOffsets(final int dictSize, final int[] tagStart,
      final int[] tagCount, final int[] groupedOrd, final int from, final int[] posOffset) {
    int size = from;
    for (int d = 0; d < dictSize; d++) {
      boolean identity = true;
      for (int k = 0; k < tagCount[d] && identity; k++) {
        identity = groupedOrd[tagStart[d] + k] == k;
      }
      if (identity) {
        posOffset[d] = -1;  // an all-double field's ordinals ARE its indices; store nothing
      } else {
        posOffset[d] = size;
        size += tagCount[d] * Short.BYTES;
      }
    }
    return size;
  }

  /** Write the header, the per-tag blocks and the ordinal vectors into one payload. */
  private static byte[] serializeDoubleRegion(final int size, final int count, final byte tagKind,
      final int dictSize, final int[] dict, final int[] tagStart, final int[] tagCount,
      final double[] tagMin, final double[] tagMax, final int[] blockOffset, final int[] posOffset,
      final byte[][] blocks, final int[] groupedOrd) {
    final byte[] out = new byte[size];
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
    for (int d = 0; d < dictSize; d++) bb.putInt(blockOffset[d]);
    for (int d = 0; d < dictSize; d++) bb.putInt(posOffset[d]);
    for (int d = 0; d < dictSize; d++) bb.put(blocks[d]);
    for (int d = 0; d < dictSize; d++) {
      if (posOffset[d] < 0) {
        continue;
      }
      for (int k = 0; k < tagCount[d]; k++) {
        bb.putShort((short) groupedOrd[tagStart[d] + k]);
      }
    }
    return out;
  }

  /**
   * Re-encode with every value of {@code dropTag} removed, so ONE unsound tag costs one FIELD its
   * column rather than costing the page its whole double region.
   *
   * <p>A tag whose values cannot be carried without standing a rounded double in for a decimal must
   * not enter the column in any form a scan would trust — not PLAIN, not ALP, both of which store
   * exactly those images. Omitting it is unambiguous in the wire format precisely because the tag
   * disappears whole: the dict, the per-tag counts and the total are rebuilt from the surviving
   * values, so a reader sees a region that simply never held that field. Its scan then finds no
   * tag, the {@code longCount + doubleCount == anchorSlots} oracle does not add up, and THAT field
   * alone keeps the record path while every other field on the page keeps its column.
   *
   * <p>Field ordinals are numbered per field, so removing one field's values leaves every other
   * field's ordinals — and therefore its encoded block — bit-for-bit what it would have been.
   *
   * <p>Terminates: each call strictly reduces the distinct tag count, and an empty remainder ends it.
   *
   * @return the payload without {@code dropTag}, or {@code null} when nothing else was in the region
   */
  private static byte @Nullable [] encodeWithoutTag(final double[] values,
      final long @Nullable [] decUnscaled, final int @Nullable [] decScales, final int[] tags,
      final int[] ordinals, final int count, final byte tagKind, final int dropTag) {
    int kept = 0;
    for (int i = 0; i < count; i++) {
      if (tags[i] != dropTag) {
        kept++;
      }
    }
    if (kept == 0) {
      return null;
    }
    final double[] keptValues = new double[kept];
    final int[] keptTags = new int[kept];
    final int[] keptOrdinals = new int[kept];
    final boolean haveDecimals = decUnscaled != null && decScales != null;
    final long[] keptUnscaled = haveDecimals ? new long[kept] : null;
    final int[] keptScales = haveDecimals ? new int[kept] : null;
    int w = 0;
    for (int i = 0; i < count; i++) {
      if (tags[i] == dropTag) {
        continue;
      }
      keptValues[w] = values[i];
      keptTags[w] = tags[i];
      keptOrdinals[w] = ordinals[i];
      if (haveDecimals) {
        keptUnscaled[w] = decUnscaled[i];
        keptScales[w] = decScales[i];
      }
      w++;
    }
    return encode(keptValues, keptUnscaled, keptScales, keptTags, keptOrdinals, kept, tagKind);
  }

  /**
   * The common scale for a tag whose values are ALL decimal-sourced, or {@code -1} otherwise.
   *
   * <p>All-or-nothing on purpose: a tag mixing exact decimals with plain doubles has no single
   * integer domain that represents both without rounding one of them, so it takes the ordinary ALP
   * path where the round-trip proof decides value by value.
   */
  private static int exactDecimalExponent(final int @Nullable [] scales, final int start,
      final int n) {
    if (scales == null) {
      return -1;
    }
    int max = 0;
    for (int i = 0; i < n; i++) {
      final int s = scales[start + i];
      if (s < 0) {
        return -1;
      }
      if (s > max) {
        max = s;
      }
    }
    return max;
  }

  /**
   * Whether every decimal-sourced value in a tag is EXACTLY its own double image.
   *
   * <p>Asked only of a tag the exact-decimal domain declined — a tag mixing decimals with plain
   * doubles, or one whose unscaled values overflow the common scale. Such a tag falls back to
   * carrying doubles, and a decimal may only ride along when the double IS the decimal.
   *
   * <p>Off the scan path entirely: one page build, and only for a tag that already holds a decimal
   * the exact domain could not take, so {@link BigDecimal} is affordable here and is the only
   * representation that decides the question without rounding it first.
   */
  private static boolean decimalImagesAreExact(final double[] grouped,
      final long @Nullable [] unscaled, final int @Nullable [] scales, final int start,
      final int n) {
    if (unscaled == null || scales == null) {
      return true;  // no decimal ever entered this region
    }
    for (int i = 0; i < n; i++) {
      final int s = scales[start + i];
      if (s < 0) {
        continue;  // a genuinely double-typed value: its own bits are the truth
      }
      final double d = grouped[start + i];
      if (!Double.isFinite(d)
          || BigDecimal.valueOf(unscaled[start + i], s).compareTo(new BigDecimal(d)) != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Lift every unscaled value to the common scale {@code e}, exactly. {@code false} on overflow,
   * which sends the tag to the ordinary ALP path rather than storing a rounded value.
   */
  private static boolean fillExactDecimalInts(final long[] unscaled, final int[] scales,
      final int start, final int n, final int e, final long[] out) {
    for (int i = 0; i < n; i++) {
      final int lift = e - scales[start + i];
      final long v = unscaled[start + i];
      if (v > POW10_LIMIT[lift] || v < -POW10_LIMIT[lift]) {
        return false;
      }
      out[i] = v * POW10_LONG[lift];
    }
    return true;
  }

  /**
   * One tag's encoding block: exact decimal when the tag is all-decimal, else ALP, else PLAIN.
   *
   * @return {@code null} when the tag cannot be carried without standing a rounded double in for a
   *     decimal, which takes the whole region — and with it the page — back to the record path
   */
  private static byte @Nullable [] encodeTag(final double[] grouped, final int start, final int n,
      final long @Nullable [] groupedUnscaled, final int @Nullable [] groupedScale) {
    // ---- exact decimal domain: no rounding, so no round-trip proof is needed ----
    // The values ARE integers at a common scale, and a scan's threshold is converted into the same
    // domain by countAlp's fix-up, so the packed comparison is a decimal-space comparison. This is
    // the path that makes a column of prices (19.99, 100.10, ...) scannable at all: their double
    // images are inexact, so the proof below would reject every one of them.
    final int decE = exactDecimalExponent(groupedScale, start, n);
    if (decE >= 0) {
      final long[] decInts = new long[n];
      if (fillExactDecimalInts(groupedUnscaled, groupedScale, start, n, decE, decInts)) {
        long lo = Long.MAX_VALUE;
        long hi = Long.MIN_VALUE;
        for (int i = 0; i < n; i++) {
          if (decInts[i] < lo) lo = decInts[i];
          if (decInts[i] > hi) hi = decInts[i];
        }
        final byte[] exact = packAlpBlock(grouped, start, n, decInts, null, 0, decE, 0, lo, hi, ENC_DEC);
        if (exact != null) {
          return exact;
        }
      }
    }
    // Every path below carries a value as its DOUBLE IMAGE — ALP over it, or the raw bits. That is
    // sound for a genuinely double-typed value and for a decimal whose image is the decimal, and
    // unsound for any other decimal: two decimals can share an image, so a threshold answered in
    // double space would put such a row on the wrong side. Refuse rather than round.
    if (!decimalImagesAreExact(grouped, groupedUnscaled, groupedScale, start, n)) {
      return null;
    }
    // ---- choose the exponent pair on a sample, best-hit-count first, smallest scale second ----
    final int sampleStep = Math.max(1, n / 32);
    int bestE = -1;
    int bestF = 0;
    int bestHits = 0;
    for (int e = 0; e <= MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        int hits = 0;
        int sampled = 0;
        for (int i = 0; i < n; i += sampleStep) {
          sampled++;
          if (alpRoundTrips(grouped[start + i], e, f)) {
            hits++;
          }
        }
        if (hits > bestHits) {
          bestHits = hits;
          bestE = e;
          bestF = f;
          if (hits == sampled && e - f == 0) {
            break;
          }
        }
      }
    }
    if (bestHits == 0) {
      return rdOrPlainBlock(grouped, start, n);
    }

    // ---- full pass: every value either round-trips exactly or becomes an exception ----
    final long[] ints = new long[n];
    final boolean[] isException = new boolean[n];
    int exceptions = 0;
    long minInt = Long.MAX_VALUE;
    long maxInt = Long.MIN_VALUE;
    for (int i = 0; i < n; i++) {
      final double d = grouped[start + i];
      final double scaled = d * EXP10[bestE] / EXP10[bestF];
      if (Math.abs(scaled) < MAX_SCALED) {
        final long enc = Math.round(scaled);
        if (Double.doubleToLongBits(alpDecode(enc, bestE, bestF)) == Double.doubleToLongBits(d)) {
          ints[i] = enc;
          if (enc < minInt) minInt = enc;
          if (enc > maxInt) maxInt = enc;
          continue;
        }
      }
      isException[i] = true;
      exceptions++;
    }
    if (exceptions == n || exceptions > n / 4 || exceptions > Short.MAX_VALUE) {
      return rdOrPlainBlock(grouped, start, n);
    }
    final byte[] packed =
        packAlpBlock(grouped, start, n, ints, isException, exceptions, bestE, bestF, minInt,
                     maxInt, ENC_ALP);
    // The integer kernel cannot serve values wider than its bit budget, and an ALP block only it
    // can read is worse than plain doubles.
    return packed != null ? packed : plainBlock(grouped, start, n);
  }

  /**
   * Write one ALP block, shared by the exact-decimal and round-trip-proved paths.
   *
   * <p>{@code isException == null} means every value encoded, which is always so for the exact
   * decimal domain: nothing is rounded there, so nothing can fail a proof. Returns {@code null}
   * when the FOR range needs more bits than the packed kernel reads, leaving the fallback to the
   * caller.
   */
  private static byte @Nullable [] packAlpBlock(final double[] grouped, final int start,
      final int n, final long[] ints, final boolean @Nullable [] isException, final int exceptions,
      final int bestE, final int bestF, final long minInt, final long maxInt, final byte enc) {
    final long range = maxInt - minInt;
    final int width = range == 0 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(range);
    if (width > BitUnpackSimd.MAX_BIT_WIDTH) {
      return null;
    }

    final int packedBytes = (int) (((long) n * width + 7) >>> 3);
    final byte[] block =
        new byte[1 + 1 + 1 + 8 + 1 + 2 + packedBytes + exceptions * (Short.BYTES + Long.BYTES)];
    final ByteBuffer bb = ByteBuffer.wrap(block).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(enc);
    bb.put((byte) bestE);
    bb.put((byte) bestF);
    bb.putLong(minInt);
    bb.put((byte) width);
    bb.putShort((short) exceptions);
    final int packedBase = bb.position();
    if (width > 0) {
      for (int i = 0; i < n; i++) {
        if (isException != null && isException[i]) {
          continue;  // placeholder: packed zero, i.e. forBase — corrected by the reader
        }
        final long v = ints[i] - minInt;
        long bitOffset = (long) i * width;
        int byteIndex = packedBase + (int) (bitOffset >>> 3);
        int shift = (int) (bitOffset & 7L);
        long rest = v;
        int remaining = width;
        while (remaining > 0) {
          block[byteIndex] |= (byte) ((rest & 0xFFL) << shift);
          final int consumed = Math.min(8 - shift, remaining);
          rest >>>= consumed;
          remaining -= consumed;
          byteIndex++;
          shift = 0;
        }
      }
    }
    bb.position(packedBase + packedBytes);
    for (int i = 0; isException != null && i < n; i++) {
      if (isException[i]) {
        bb.putShort((short) i);
      }
    }
    for (int i = 0; isException != null && i < n; i++) {
      if (isException[i]) {
        bb.putLong(Double.doubleToLongBits(grouped[start + i]));
      }
    }
    return block;
  }

  private static boolean alpRoundTrips(final double d, final int e, final int f) {
    final double scaled = d * EXP10[e] / EXP10[f];
    if (!(Math.abs(scaled) < MAX_SCALED)) {
      return false;
    }
    final long enc = Math.round(scaled);
    return Double.doubleToLongBits(alpDecode(enc, e, f)) == Double.doubleToLongBits(d);
  }

  /**
   * One tag's ALP-RD block, or PLAIN when the split does not pay.
   *
   * <p>The split point is chosen by COSTING, not assumed: for each candidate right width the
   * distinct left parts are counted exactly (the tag holds at most a page of values, so a sort
   * beats any hash), the top-{@value #RD_MAX_DICT} by frequency become the dictionary, the rest
   * exceptions — and the total bits are compared across candidates and against plain storage.
   */
  private static byte[] rdOrPlainBlock(final double[] grouped, final int start, final int n) {
    final long[] bits = new long[n];
    for (int i = 0; i < n; i++) {
      bits[i] = Double.doubleToLongBits(grouped[start + i]);
    }
    final RdPlan plan = chooseRdPlan(bits, n);
    return plan == null ? plainBlock(grouped, start, n) : encodeRdBlock(bits, n, plan);
  }

  /** The split point and left-part dictionary an ALP-RD block was costed with. */
  private record RdPlan(int rightWidth, long[] dict) {
  }

  /** Bits a code needs to address {@code dictSize} dictionary entries. */
  private static int codeWidthFor(final int dictSize) {
    return dictSize <= 1 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(dictSize - 1);
  }

  /**
   * Cost every admissible right-width and keep the cheapest, or {@code null} when none beats a
   * plain block.
   */
  private static RdPlan chooseRdPlan(final long[] bits, final int n) {
    int bestRw = -1;
    long bestCost = (long) n * Double.SIZE;  // beat plain or stay plain
    long[] bestDict = null;
    final long[] runValue = new long[n];
    final int[] runCount = new int[n];
    for (int rw = MIN_RIGHT_WIDTH; rw <= MAX_RIGHT_WIDTH; rw++) {
      final long[] sorted = new long[n];
      for (int i = 0; i < n; i++) {
        sorted[i] = bits[i] >>> rw;
      }
      Arrays.sort(sorted);
      final int runs = collectRuns(sorted, n, runValue, runCount);
      final int dictSize = Math.min(RD_MAX_DICT, runs);
      final long[] dict = new long[dictSize];
      final int exceptions = n - takeMostFrequent(runValue, runCount, runs, dict);
      if (exceptions > Short.MAX_VALUE) {
        continue;
      }
      final int cw = codeWidthFor(dictSize);
      final long cost = (long) n * (cw + rw) + (long) exceptions * 2 * Short.SIZE
          + (long) dictSize * Long.SIZE + 6L * Byte.SIZE;
      if (cost < bestCost) {
        bestCost = cost;
        bestRw = rw;
        bestDict = dict;
      }
    }
    return bestRw < 0 ? null : new RdPlan(bestRw, bestDict);
  }

  /** Runs of equal values in the sorted left parts; answers how many there are. */
  private static int collectRuns(final long[] sorted, final int n, final long[] runValue,
      final int[] runCount) {
    int runs = 0;
    for (int i = 0; i < n; ) {
      int j = i;
      while (j < n && sorted[j] == sorted[i]) {
        j++;
      }
      runValue[runs] = sorted[i];
      runCount[runs] = j - i;
      runs++;
      i = j;
    }
    return runs;
  }

  /**
   * Fill {@code dict} with the most frequent run values and answer how many values they cover.
   * Repeated max extraction rather than a sort: k is tiny and runs ≤ n. Consumes {@code runCount}.
   */
  private static int takeMostFrequent(final long[] runValue, final int[] runCount, final int runs,
      final long[] dict) {
    int covered = 0;
    for (int d = 0; d < dict.length; d++) {
      int argmax = -1;
      for (int r = 0; r < runs; r++) {
        if (runCount[r] > 0 && (argmax < 0 || runCount[r] > runCount[argmax])) {
          argmax = r;
        }
      }
      dict[d] = runValue[argmax];
      covered += runCount[argmax];
      runCount[argmax] = 0;
    }
    return covered;
  }

  /** Write the ALP-RD block {@code plan} describes: dictionary, packed codes, right parts. */
  private static byte[] encodeRdBlock(final long[] bits, final int n, final RdPlan plan) {
    final int rw = plan.rightWidth();
    final long[] dict = plan.dict();
    final int dictSize = dict.length;
    final int cw = codeWidthFor(dictSize);
    final int[] codes = new int[n];
    final boolean[] isException = new boolean[n];
    int exceptions = 0;
    for (int i = 0; i < n; i++) {
      final long left = bits[i] >>> rw;
      int code = -1;
      for (int d = 0; d < dictSize; d++) {
        if (dict[d] == left) {
          code = d;
          break;
        }
      }
      if (code < 0) {
        isException[i] = true;
        exceptions++;
      } else {
        codes[i] = code;
      }
    }
    final int codeBytes = (int) (((long) n * cw + 7) >>> 3);
    final int rightBytes = (int) (((long) n * rw + 7) >>> 3);
    final byte[] block = new byte[1 + 1 + 1 + 1 + 2 + dictSize * Long.BYTES + codeBytes
        + rightBytes + exceptions * (Short.BYTES + Short.BYTES)];
    final ByteBuffer bb = ByteBuffer.wrap(block).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(ENC_ALP_RD);
    bb.put((byte) rw);
    bb.put((byte) dictSize);
    bb.put((byte) cw);
    bb.putShort((short) exceptions);
    for (final long d : dict) {
      bb.putLong(d);
    }
    final int codesBase = bb.position();
    final int rightBase = codesBase + codeBytes;
    for (int i = 0; i < n; i++) {
      // Exceptions pack code zero as a placeholder; their left part lives in the exception list.
      if (cw > 0 && !isException[i]) {
        packBits(block, codesBase, i, cw, codes[i]);
      }
      packBits(block, rightBase, i, rw, bits[i] & BitUnpackSimd.maskFor(rw));
    }
    bb.position(rightBase + rightBytes);
    for (int i = 0; i < n; i++) {
      if (isException[i]) {
        bb.putShort((short) i);
      }
    }
    for (int i = 0; i < n; i++) {
      if (isException[i]) {
        bb.putShort((short) (bits[i] >>> rw));
      }
    }
    return block;
  }

  /** OR {@code value}'s low {@code width} bits into the little-endian bit stream at entry {@code i}. */
  private static void packBits(final byte[] block, final int base, final int i, final int width,
      final long value) {
    long bitOffset = (long) i * width;
    int byteIndex = base + (int) (bitOffset >>> 3);
    int shift = (int) (bitOffset & 7L);
    long rest = value;
    int remaining = width;
    while (remaining > 0) {
      block[byteIndex] |= (byte) ((rest & 0xFFL) << shift);
      final int consumed = Math.min(8 - shift, remaining);
      rest >>>= consumed;
      remaining -= consumed;
      byteIndex++;
      shift = 0;
    }
  }

  private static byte[] plainBlock(final double[] grouped, final int start, final int n) {
    final byte[] block = new byte[1 + n * Double.BYTES];
    final ByteBuffer bb = ByteBuffer.wrap(block).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(ENC_PLAIN);
    for (int i = 0; i < n; i++) {
      bb.putDouble(grouped[start + i]);
    }
    return block;
  }

  private static int indexOfOrMinus1(final int[] dict, final int dictSize, final int tag) {
    for (int d = 0; d < dictSize; d++) {
      if (dict[d] == tag) {
        return d;
      }
    }
    return -1;
  }
}

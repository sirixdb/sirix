/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Schema-aware Frame-of-Reference (FOR) + bit-packed codec for {@code long[]}
 * sequences over an off-heap {@link MemorySegment}. Designed as a structural
 * replacement for LZ4 on numeric PAX regions: knowing the payload is a
 * {@code long[]} lets us encode it with ~LZ4 size but near-zero decompress
 * CPU (a right-shift + mask per value).
 *
 * <h2>Wire format</h2>
 * <pre>
 * byte    version     // format version, currently VERSION_V1
 * byte    bitWidth    // 0..64; 0 ⇒ constant-only, no body
 * varint  n           // unsigned: number of values encoded
 * long    minValue    // frame-of-reference base; stored LE
 * byte[]  body        // ceil(n * bitWidth / 8) bytes, bit-packed LE
 * </pre>
 *
 * <p>When {@code bitWidth == 0} every value equals {@code minValue} — the
 * body is omitted. When {@code n == 0} the segment contains only the
 * 10-byte minimal header (1+1+1+8 if the 1-byte varint 0 is used).
 *
 * <h2>Value layout and random access</h2>
 * <p>Each logical value is {@code v[i] = minValue + u[i]} where {@code u[i]}
 * is the unsigned bit-packed width-{@code W}-bits payload at bit offset
 * {@code i*W}. Reads fetch one aligned 64-bit word (two if the value
 * straddles a word boundary at widths > 56), shift, and mask — no branches
 * in the steady state beyond the straddle check.
 *
 * <h2>Why this beats LZ4 on numeric columns</h2>
 * <ul>
 *   <li><b>Size</b>: A narrow range (e.g. 1M salaries, spread ≤ 2^20) packs
 *       to 20 bits × N + 10 B header — matches or beats LZ4 on delta-varint
 *       of the same data.</li>
 *   <li><b>CPU</b>: Scan = 1 word load + shift + mask per value. LZ4 costs
 *       ~16% of cold CPU at 100M scale (see umbra-iter6 memory).</li>
 *   <li><b>Composability</b>: Output is a contiguous range in an existing
 *       MemorySegment — no extra allocation, no decompress buffer.</li>
 * </ul>
 *
 * <h2>HFT-grade constraints</h2>
 * <ul>
 *   <li>Zero allocation on both encode and decode hot paths.</li>
 *   <li>All shifts + masks; no conditional inside bit-unpack for widths ≤ 56.</li>
 *   <li>Accepts both on-heap-backed and native-backed MemorySegments.</li>
 *   <li>Operates on {@code long} offsets throughout — no 2 GiB wrap risk.</li>
 * </ul>
 */
public final class NumberRegionCompact {

  /** Current wire format version. Bumped when the on-disk layout changes. */
  public static final byte VERSION_V1 = 1;

  /** Header bytes when {@code n == 0} and bitWidth == 0. */
  private static final int MIN_HEADER_BYTES = 1 /* version */ + 1 /* bitWidth */ + 1 /* varint n=0 */ + 8 /* minValue */;

  private NumberRegionCompact() {
    // utility class
  }

  // ────────────────────────────────────────────────────────────── size helpers

  /**
   * Exact byte count required to encode {@code values} with FOR+BP.
   *
   * @param values source values
   * @param count number of valid entries (0..values.length)
   * @return byte count the encoding will consume
   */
  public static long maxEncodedSize(final long[] values, final int count) {
    if (values == null) {
      throw new IllegalArgumentException("values");
    }
    if (count < 0 || count > values.length) {
      throw new IllegalArgumentException("count=" + count + " values.length=" + values.length);
    }
    final int bitWidth = computeBitWidth(values, count);
    return headerBytes(count) + bodyBytes(count, bitWidth);
  }

  /**
   * Exact size of the encoded body (bit-packed payload) for {@code count}
   * values at {@code bitWidth} bits. Always 8-byte aligned reads are safe
   * thanks to {@link #readUpToLongLE} inside {@link #readCompact}.
   */
  public static long bodyBytes(final int count, final int bitWidth) {
    if (count < 0) {
      throw new IllegalArgumentException("count=" + count);
    }
    if (bitWidth < 0 || bitWidth > 64) {
      throw new IllegalArgumentException("bitWidth=" + bitWidth);
    }
    if (bitWidth == 0 || count == 0) {
      return 0L;
    }
    final long totalBits = (long) count * (long) bitWidth;
    return (totalBits + 7L) >>> 3;
  }

  /**
   * Bytes the header consumes for {@code n == count}. Varies because the
   * count is varint-encoded (1 byte up to 127, 2 bytes up to 16383, etc.).
   */
  public static long headerBytes(final int count) {
    if (count < 0) {
      throw new IllegalArgumentException("count=" + count);
    }
    // 1 version + 1 bitWidth + varint(count) + 8 minValue
    return 1L + 1L + varintSize(count) + 8L;
  }

  /**
   * Smallest bit width W such that every {@code values[i] - min} fits in
   * unsigned W bits. Returns 0 when {@code min == max} (constant-run
   * shortcut — no body needed). Returns 64 when the span overflows signed
   * range (fallback to plain 64-bit storage).
   *
   * @param values source array
   * @param count number of valid entries
   * @return bit width in {@code [0..64]}
   */
  public static int computeBitWidth(final long[] values, final int count) {
    if (values == null) {
      throw new IllegalArgumentException("values");
    }
    if (count < 0 || count > values.length) {
      throw new IllegalArgumentException("count=" + count + " values.length=" + values.length);
    }
    if (count == 0) {
      return 0;
    }
    long min = values[0];
    long max = values[0];
    for (int i = 1; i < count; i++) {
      final long v = values[i];
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    if (min == max) {
      return 0;
    }
    // spread treated as unsigned so negatives-to-positives still work.
    final long spread = max - min;
    if (spread < 0L) {
      // Overflow (e.g. Long.MIN_VALUE → Long.MAX_VALUE span). Store raw 64-bit.
      return 64;
    }
    // Bits needed for the non-negative span.
    return 64 - Long.numberOfLeadingZeros(spread);
  }

  // ─────────────────────────────────────────────────────────────── encoding

  /**
   * Encode {@code count} values from {@code values[0..count)} into
   * {@code target} starting at {@code offset}. Writes version, bit width,
   * varint count, min-value, and bit-packed body.
   *
   * @param target destination segment (must have ≥ {@link #maxEncodedSize}
   *     bytes available at {@code offset})
   * @param offset byte offset at which to start writing
   * @param values source values; only the {@code count} prefix is read
   * @param count number of values to encode
   * @return total bytes written (header + body)
   */
  public static long writeCompact(final MemorySegment target, final long offset,
      final long[] values, final int count) {
    if (target == null) {
      throw new IllegalArgumentException("target");
    }
    if (values == null) {
      throw new IllegalArgumentException("values");
    }
    if (count < 0 || count > values.length) {
      throw new IllegalArgumentException("count=" + count + " values.length=" + values.length);
    }
    if (offset < 0 || offset + MIN_HEADER_BYTES > target.byteSize()) {
      throw new IllegalArgumentException(
          "offset=" + offset + " byteSize=" + target.byteSize());
    }

    final int bitWidth = computeBitWidth(values, count);
    // For raw 64-bit storage (bitWidth == 64) we don't apply a FOR base —
    // values are stored verbatim and readback skips the +base step. The
    // header's minValue field is forced to 0 so decoders don't double-add.
    final long minValue = (count == 0 || bitWidth == 64) ? 0L : findMin(values, count);
    final long bodyBytes = bodyBytes(count, bitWidth);

    long pos = offset;
    target.set(ValueLayout.JAVA_BYTE, pos, VERSION_V1);
    pos++;
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) bitWidth);
    pos++;
    pos += writeVarintUnsigned(target, pos, count);
    target.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, minValue);
    pos += 8L;
    final long bodyStart = pos;
    if (bodyBytes > 0L) {
      if (pos + bodyBytes > target.byteSize()) {
        throw new IllegalArgumentException(
            "target too small: need " + (pos + bodyBytes - offset) + " bytes at offset " + offset);
      }
      bitPack(target, bodyStart, values, count, minValue, bitWidth);
      pos += bodyBytes;
    }
    return pos - offset;
  }

  // ─────────────────────────────────────────────────────────────── decoding

  /**
   * Parsed header describing a compact payload. Reusable across calls to
   * minimise allocation pressure on scan paths.
   */
  public static final class Header {
    public byte version;
    /** Bit width per value, 0..64. 0 ⇒ constant-run ({@code n} copies of {@link #minValue}). */
    public byte bitWidth;
    /** Number of encoded values. */
    public int count;
    /** Frame-of-reference base. */
    public long minValue;
    /** Absolute byte offset of the bit-packed body (or end of header when no body). */
    public long bodyOffset;
    /** Total bytes consumed by the header. */
    public int headerBytes;
    /** Total bytes consumed by the body. */
    public long bodyBytes;

    /** Clear for reuse. Doesn't null-out anything; every write populates on parse. */
    public Header reset() {
      version = 0;
      bitWidth = 0;
      count = 0;
      minValue = 0L;
      bodyOffset = 0L;
      headerBytes = 0;
      bodyBytes = 0L;
      return this;
    }
  }

  /**
   * Parse the compact header at {@code headerOffset} into {@code out}.
   *
   * @param src source segment
   * @param headerOffset absolute byte offset of the version byte
   * @param out header to populate
   * @return {@code out} for chaining
   */
  public static Header readHeader(final MemorySegment src, final long headerOffset, final Header out) {
    if (src == null || out == null) {
      throw new IllegalArgumentException("src/out");
    }
    out.version = src.get(ValueLayout.JAVA_BYTE, headerOffset);
    if (out.version != VERSION_V1) {
      throw new IllegalStateException("unsupported NumberRegionCompact version=" + out.version);
    }
    out.bitWidth = src.get(ValueLayout.JAVA_BYTE, headerOffset + 1L);
    if (out.bitWidth < 0 || out.bitWidth > 64) {
      throw new IllegalStateException("illegal bitWidth=" + out.bitWidth);
    }
    // Inline varint parse — zero allocation; the encoded key is the low 32
    // bits and the byte count is the high 32 bits of the returned long.
    final long varintPacked = readVarintUnsignedPacked(src, headerOffset + 2L);
    final long countLong = varintPacked & 0xFFFFFFFFL;
    final int varintBytes = (int) (varintPacked >>> 32);
    if (countLong < 0L || countLong > (long) Integer.MAX_VALUE) {
      throw new IllegalStateException("illegal count=" + countLong);
    }
    out.count = (int) countLong;
    out.minValue = src.get(ValueLayout.JAVA_LONG_UNALIGNED, headerOffset + 2L + varintBytes);
    out.headerBytes = 2 + varintBytes + 8;
    out.bodyOffset = headerOffset + out.headerBytes;
    out.bodyBytes = bodyBytes(out.count, out.bitWidth);
    return out;
  }

  /**
   * Random-access read of the value at {@code index}. Right-shift + mask
   * (no virtual calls, no object allocation). Caller supplies a {@code Header}
   * previously populated by {@link #readHeader(MemorySegment, long, Header)}.
   *
   * @param src source segment
   * @param header parsed header
   * @param index 0-based index into the logical sequence
   * @return value stored at {@code index}
   */
  public static long readCompact(final MemorySegment src, final Header header, final int index) {
    if (src == null || header == null) {
      throw new IllegalArgumentException("src/header");
    }
    if (index < 0 || index >= header.count) {
      throw new IndexOutOfBoundsException("index=" + index + " count=" + header.count);
    }
    if (header.bitWidth == 0) {
      return header.minValue;
    }
    return header.minValue + bitUnpack(src, header.bodyOffset, header.bitWidth, index);
  }

  /**
   * Convenience: parses the header each call — slower than the 2-arg
   * overload if you read repeatedly from the same payload. Allocates one
   * {@link Header}, which is only acceptable outside inner scan loops.
   */
  public static long readCompact(final MemorySegment src, final long headerOffset, final int index) {
    final Header h = new Header();
    readHeader(src, headerOffset, h);
    return readCompact(src, h, index);
  }

  /**
   * Bulk decode into a caller-supplied buffer. Writes exactly
   * {@code header.count} values starting at {@code out[0]}.
   *
   * @param src source segment
   * @param header parsed header
   * @param out destination array, {@code out.length >= header.count}
   */
  public static void decodeAll(final MemorySegment src, final Header header, final long[] out) {
    if (src == null || header == null || out == null) {
      throw new IllegalArgumentException("src/header/out");
    }
    final int n = header.count;
    if (out.length < n) {
      throw new IllegalArgumentException("out too small: " + out.length + " < " + n);
    }
    if (n == 0) {
      return;
    }
    final int bitWidth = header.bitWidth;
    final long base = header.minValue;
    if (bitWidth == 0) {
      for (int i = 0; i < n; i++) {
        out[i] = base;
      }
      return;
    }
    final long bodyOff = header.bodyOffset;
    for (int i = 0; i < n; i++) {
      out[i] = base + bitUnpack(src, bodyOff, bitWidth, i);
    }
  }

  /**
   * Convenience bulk-decode that parses the header first. Prefer
   * {@link #decodeAll(MemorySegment, Header, long[])} in hot code.
   */
  public static void decodeAll(final MemorySegment src, final long headerOffset, final long[] out) {
    final Header h = new Header();
    readHeader(src, headerOffset, h);
    decodeAll(src, h, out);
  }

  // ─────────────────────────────────────────────────────── bit-pack / unpack

  /**
   * Bit-pack {@code count} values starting at {@code baseOff}. Uses a fast
   * 8-byte buffered path for widths ≤ 56 (where any single value plus up to
   * 7 stray bits from a previous value still fits in a {@code long}); widths
   * 57..64 use a direct per-value bit-level write that tolerates straddles
   * across 64-bit words.
   *
   * <p>When {@code bitWidth == 64} the values are stored raw and {@code base}
   * is assumed to be zero (set by the caller so reads don't double-add).
   */
  private static void bitPack(final MemorySegment target, final long baseOff, final long[] values,
      final int count, final long base, final int bitWidth) {
    if (bitWidth == 64) {
      // Raw 64-bit path: base is 0 (spread overflow => caller forced min=0).
      long off = baseOff;
      for (int i = 0; i < count; i++) {
        target.set(ValueLayout.JAVA_LONG_UNALIGNED, off, values[i]);
        off += 8L;
      }
      return;
    }
    if (bitWidth <= 56) {
      // Fast path: max(bitsInBuf) before flush = 7 + 56 = 63 bits, fits in long.
      final long mask = (1L << bitWidth) - 1L;
      long buf = 0L;
      int bitsInBuf = 0;
      long writePos = baseOff;
      for (int i = 0; i < count; i++) {
        final long v = (values[i] - base) & mask;
        buf |= v << bitsInBuf;
        bitsInBuf += bitWidth;
        while (bitsInBuf >= 8) {
          target.set(ValueLayout.JAVA_BYTE, writePos, (byte) buf);
          writePos++;
          buf >>>= 8;
          bitsInBuf -= 8;
        }
      }
      if (bitsInBuf > 0) {
        target.set(ValueLayout.JAVA_BYTE, writePos, (byte) buf);
      }
      return;
    }
    // Slow path: widths 57..63. Buffered accumulator would overflow, so we
    // place each value by read-modify-writing the containing 64-bit word(s).
    // We MUST clear any stale bits in the target region so the write is
    // deterministic even when the target segment holds leftover data from a
    // previous use; we do this by masking OUT the destination bit range
    // before OR'ing in the new value.
    final long mask = (1L << bitWidth) - 1L;
    for (int i = 0; i < count; i++) {
      final long v = (values[i] - base) & mask;
      final long bitOff = (long) i * (long) bitWidth;
      final long byteOff = baseOff + (bitOff >>> 3);
      final int bitInByte = (int) (bitOff & 7L);
      final long loWord = readUpToLongLE(target, byteOff);
      final int bitsFromLo = Math.min(bitWidth, 64 - bitInByte);
      final long loMask = bitsFromLo == 64 ? ~0L : (((1L << bitsFromLo) - 1L) << bitInByte);
      final long loShifted = (v & ((1L << bitsFromLo) - 1L)) << bitInByte;
      final long loCombined = (loWord & ~loMask) | loShifted;
      writeBitPackWord(target, byteOff, loCombined);
      if (bitsFromLo < bitWidth) {
        final int remaining = bitWidth - bitsFromLo;
        final long hiMask = (1L << remaining) - 1L;
        final long hiWord = readUpToLongLE(target, byteOff + 8L);
        final long hiValBits = (v >>> bitsFromLo) & hiMask;
        final long hiCombined = (hiWord & ~hiMask) | hiValBits;
        writeBitPackWordPartial(target, byteOff + 8L, hiCombined, (remaining + 7) >>> 3);
      }
    }
  }

  /**
   * Write up to 8 bytes LE; a full 8-byte unaligned store if possible, else
   * byte-by-byte up to the segment end.
   */
  private static void writeBitPackWord(final MemorySegment target, final long off, final long word) {
    final long size = target.byteSize();
    if (off + 8L <= size) {
      target.set(ValueLayout.JAVA_LONG_UNALIGNED, off, word);
      return;
    }
    final long remaining = Math.max(0L, size - off);
    for (long k = 0L; k < remaining; k++) {
      target.set(ValueLayout.JAVA_BYTE, off + k, (byte) (word >>> (k << 3)));
    }
  }

  /** Write up to {@code bytes} bytes of {@code word}, clamped to segment end. */
  private static void writeBitPackWordPartial(final MemorySegment target, final long off,
      final long word, final int bytes) {
    final long size = target.byteSize();
    final long limit = Math.min((long) bytes, Math.max(0L, size - off));
    for (long k = 0L; k < limit; k++) {
      target.set(ValueLayout.JAVA_BYTE, off + k, (byte) (word >>> (k << 3)));
    }
  }

  /**
   * Unsigned bit-unpack. Handles widths 1..64. For widths ≤ 56 a single
   * unaligned 64-bit load suffices. Widths 57..64 may straddle two 64-bit
   * words (the top {@code bitWidth - (64 - bitInByte)} bits live in the
   * next word).
   */
  private static long bitUnpack(final MemorySegment src, final long baseOff, final int bitWidth,
      final int index) {
    if (bitWidth == 64) {
      return src.get(ValueLayout.JAVA_LONG_UNALIGNED, baseOff + ((long) index << 3));
    }
    final long bitOff = (long) index * (long) bitWidth;
    final long byteOff = baseOff + (bitOff >>> 3);
    final int bitInByte = (int) (bitOff & 7L);
    final long mask = (1L << bitWidth) - 1L;
    final long w0 = readUpToLongLE(src, byteOff);
    long v = (w0 >>> bitInByte) & mask;
    final int bitsFromW0 = 64 - bitInByte;
    if (bitsFromW0 < bitWidth) {
      final int extra = bitWidth - bitsFromW0;
      final long w1 = readUpToLongLE(src, byteOff + 8L);
      v |= (w1 & ((1L << extra) - 1L)) << bitsFromW0;
    }
    return v;
  }

  /**
   * Read up to 8 little-endian bytes as a {@code long} even if fewer bytes
   * remain. Bytes past the segment end are treated as zero. Safe because
   * the caller always masks the result to {@code bitWidth} and the valid
   * bits for the last value lie inside the segment.
   */
  private static long readUpToLongLE(final MemorySegment src, final long off) {
    final long size = src.byteSize();
    if (off + 8L <= size) {
      return src.get(ValueLayout.JAVA_LONG_UNALIGNED, off);
    }
    if (off >= size) {
      return 0L;
    }
    long v = 0L;
    final long remaining = size - off;
    for (long k = 0L; k < remaining; k++) {
      v |= ((long) (src.get(ValueLayout.JAVA_BYTE, off + k) & 0xFF)) << (k << 3);
    }
    return v;
  }

  // ───────────────────────────────────────────────────────────────── varint

  /**
   * Writes an unsigned long as a variable-width integer (LEB-128) at
   * {@code offset} and returns the byte count written.
   */
  private static int writeVarintUnsigned(final MemorySegment target, final long offset,
      final long value) {
    long v = value;
    long pos = offset;
    while ((v & ~0x7FL) != 0L) {
      target.set(ValueLayout.JAVA_BYTE, pos, (byte) ((v & 0x7FL) | 0x80L));
      pos++;
      v >>>= 7;
    }
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) v);
    return (int) (pos - offset + 1L);
  }

  /**
   * Zero-alloc varint read: returns {@code (byteCount << 32) | (value & 0xFFFFFFFF)}.
   * Only supports unsigned values ≤ {@link Integer#MAX_VALUE}, enough for our
   * {@code count} field (we already reject larger counts).
   */
  private static long readVarintUnsignedPacked(final MemorySegment src, final long offset) {
    long pos = offset;
    long result = 0L;
    int shift = 0;
    while (true) {
      final byte b = src.get(ValueLayout.JAVA_BYTE, pos);
      pos++;
      result |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        throw new IllegalStateException("varint too long at offset=" + offset);
      }
    }
    final int bytes = (int) (pos - offset);
    return ((long) bytes << 32) | (result & 0xFFFFFFFFL);
  }

  private static int varintSize(final int unsignedValue) {
    if (unsignedValue < 0) {
      return 5;
    }
    if (unsignedValue < 1 << 7) {
      return 1;
    }
    if (unsignedValue < 1 << 14) {
      return 2;
    }
    if (unsignedValue < 1 << 21) {
      return 3;
    }
    if (unsignedValue < 1 << 28) {
      return 4;
    }
    return 5;
  }

  private static long findMin(final long[] values, final int count) {
    long m = values[0];
    for (int i = 1; i < count; i++) {
      if (values[i] < m) {
        m = values[i];
      }
    }
    return m;
  }
}

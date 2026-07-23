/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Delta-of-delta ("DoubleDelta") codec for {@code long[]} sequences over an
 * off-heap {@link MemorySegment}. Sibling of {@link NumberRegionCompact}: where
 * that codec removes a shared frame-of-reference base and bit-packs the
 * residuals, this one removes both the base <em>and the slope</em>, bit-packing
 * only the change in stride between consecutive values.
 *
 * <p>The target workload is temporal columns — commit timestamps, valid-time
 * bounds, monotonic sequence numbers — where values advance by a near-constant
 * stride. For a strictly constant stride (e.g. one tick per row) every
 * delta-of-delta is {@code 0}, the bit width collapses to {@code 0}, and the
 * body disappears entirely: the whole column reduces to {@code firstValue} +
 * {@code firstDelta} regardless of length. This is the same family of codec
 * ClickHouse exposes as {@code DoubleDelta} and Gorilla uses for its timestamp
 * stream.
 *
 * <h2>Wire format</h2>
 * <pre>
 * byte    version      // format version, currently VERSION_V1
 * byte    ddBitWidth   // 0..64; bits per zig-zag(delta-of-delta). 0 ⇒ no body
 * varint  n            // unsigned: number of values encoded
 * long    firstValue   // v[0], stored raw LE (0 when n == 0)
 * long    firstDelta   // v[1]-v[0], stored raw LE signed (0 when n < 2)
 * byte[]  body          // ceil(max(0,n-2) * ddBitWidth / 8) bytes, bit-packed LE
 * </pre>
 *
 * <p>Decode is a running prefix sum: {@code delta += dd; value += delta}. That
 * is inherently sequential, so unlike {@link NumberRegionCompact} random access
 * costs {@code O(index)} ({@link #readDelta}); bulk {@link #decodeAll} is the
 * fast path and is what scan operators use.
 *
 * <h2>Zone maps</h2>
 * <p>This codec deliberately does <em>not</em> compute or store min/max. In the
 * PAX layout the enclosing {@link NumberRegion} header already carries the
 * global and per-tag zone maps (computed from the raw values before any value
 * codec runs), exactly as it does for {@link NumberRegionCompact}. Duplicating
 * them here would waste bytes and risk divergence.
 *
 * <h2>HFT-grade constraints</h2>
 * <ul>
 *   <li>Zero allocation on both encode and decode hot paths.</li>
 *   <li>All shifts + masks; the only branch in the steady state is the
 *       word-straddle check inherited from the bit-packing layer.</li>
 *   <li>Accepts both on-heap-backed and native-backed MemorySegments.</li>
 *   <li>Operates on {@code long} offsets throughout — no 2 GiB wrap risk.</li>
 * </ul>
 */
public final class NumberRegionDelta {

  /** Current wire format version. Bumped when the on-disk layout changes. */
  public static final byte VERSION_V1 = 1;

  private NumberRegionDelta() {
    // utility class
  }

  // ────────────────────────────────────────────────────────────── size helpers

  /**
   * Exact byte count required to encode {@code values} with delta-of-delta.
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
   * Bytes the fixed header consumes for {@code n == count}: the count field is
   * varint-encoded, the two anchor longs are always present.
   */
  public static long headerBytes(final int count) {
    if (count < 0) {
      throw new IllegalArgumentException("count=" + count);
    }
    // 1 version + 1 ddBitWidth + varint(count) + 8 firstValue + 8 firstDelta
    return 1L + 1L + varintSize(count) + 8L + 8L;
  }

  /**
   * Exact size of the bit-packed body for {@code count} values at
   * {@code ddBitWidth} bits. Only {@code max(0, count-2)} residuals are stored
   * (the first two values are the raw anchors).
   */
  public static long bodyBytes(final int count, final int bitWidth) {
    if (count < 0) {
      throw new IllegalArgumentException("count=" + count);
    }
    if (bitWidth < 0 || bitWidth > 64) {
      throw new IllegalArgumentException("bitWidth=" + bitWidth);
    }
    final int residuals = count > 2 ? count - 2 : 0;
    if (bitWidth == 0 || residuals == 0) {
      return 0L;
    }
    final long totalBits = (long) residuals * (long) bitWidth;
    return (totalBits + 7L) >>> 3;
  }

  /**
   * Smallest bit width W such that every zig-zag(delta-of-delta) residual fits
   * in unsigned W bits. Returns 0 when there are fewer than three values, or
   * when every residual is 0 (constant-stride shortcut — no body needed).
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
    if (count <= 2) {
      return 0;
    }
    long prevDelta = values[1] - values[0];
    long maxZigZag = 0L;
    for (int i = 2; i < count; i++) {
      final long delta = values[i] - values[i - 1];
      final long dd = delta - prevDelta;
      prevDelta = delta;
      final long zz = zigZagEncode(dd);
      // Unsigned max: a zig-zag of a large-magnitude dd can set the top bit,
      // making the value "negative" as a signed long. Compare unsigned so the
      // widest residual wins regardless of sign.
      if (Long.compareUnsigned(zz, maxZigZag) > 0) {
        maxZigZag = zz;
      }
    }
    if (maxZigZag == 0L) {
      return 0;
    }
    return 64 - Long.numberOfLeadingZeros(maxZigZag);
  }

  // ─────────────────────────────────────────────────────────────── encoding

  /**
   * Encode {@code count} values from {@code values[0..count)} into
   * {@code target} starting at {@code offset}.
   *
   * @param target destination segment (must have ≥ {@link #maxEncodedSize}
   *     bytes available at {@code offset})
   * @param offset byte offset at which to start writing
   * @param values source values; only the {@code count} prefix is read
   * @param count number of values to encode
   * @return total bytes written (header + body)
   */
  public static long writeDelta(final MemorySegment target, final long offset,
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
    // Guard against the *actual* header size (the count varint may be up to 5
    // bytes), so an undersized target fails with IllegalArgumentException here
    // rather than an IndexOutOfBoundsException from a later store.
    if (offset < 0 || offset + headerBytes(count) > target.byteSize()) {
      throw new IllegalArgumentException(
          "offset=" + offset + " byteSize=" + target.byteSize());
    }

    final int bitWidth = computeBitWidth(values, count);
    final long firstValue = count >= 1 ? values[0] : 0L;
    final long firstDelta = count >= 2 ? values[1] - values[0] : 0L;
    final long bodyBytes = bodyBytes(count, bitWidth);

    long pos = offset;
    target.set(ValueLayout.JAVA_BYTE, pos, VERSION_V1);
    pos++;
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) bitWidth);
    pos++;
    pos += writeVarintUnsigned(target, pos, count);
    target.set(LE.LONG, pos, firstValue);
    pos += 8L;
    target.set(LE.LONG, pos, firstDelta);
    pos += 8L;

    if (bodyBytes > 0L) {
      if (pos + bodyBytes > target.byteSize()) {
        throw new IllegalArgumentException(
            "target too small: need " + (pos + bodyBytes - offset) + " bytes at offset " + offset);
      }
      packResiduals(target, pos, values, count, bitWidth);
      pos += bodyBytes;
    }
    return pos - offset;
  }

  /**
   * Bit-pack the {@code count-2} zig-zag(delta-of-delta) residuals starting at
   * {@code bodyOff}. Recomputes deltas in one pass — no scratch allocation.
   */
  private static void packResiduals(final MemorySegment target, final long bodyOff,
      final long[] values, final int count, final int bitWidth) {
    long prevDelta = values[1] - values[0];
    if (bitWidth == 64) {
      // Every residual is byte-aligned (index*64 bits = index*8 bytes), so a
      // direct 64-bit store per value avoids the straddle logic — and its
      // {@code 1L << 64} masking hazard — entirely.
      long off = bodyOff;
      for (int i = 2; i < count; i++) {
        final long delta = values[i] - values[i - 1];
        target.set(LE.LONG, off, zigZagEncode(delta - prevDelta));
        prevDelta = delta;
        off += 8L;
      }
      return;
    }
    // Widths ≤ 63 ⇒ the low-part slice is at most 63 bits, so every
    // {@code (1L << n) - 1} mask below is well-defined.
    final long mask = (1L << bitWidth) - 1L;
    // Buffered LE bit writer. bitWidth ≤ 56 keeps max(bitsInBuf) = 7 + 56 < 64.
    if (bitWidth <= 56) {
      long buf = 0L;
      int bitsInBuf = 0;
      long writePos = bodyOff;
      for (int i = 2; i < count; i++) {
        final long delta = values[i] - values[i - 1];
        final long zz = zigZagEncode(delta - prevDelta) & mask;
        prevDelta = delta;
        buf |= zz << bitsInBuf;
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
    // Widths 57..63: place each residual by read-modify-writing the containing
    // 64-bit word(s), tolerating straddles across the word boundary.
    int residualIndex = 0;
    for (int i = 2; i < count; i++) {
      final long delta = values[i] - values[i - 1];
      final long zz = zigZagEncode(delta - prevDelta) & mask;
      prevDelta = delta;
      final long bitOff = (long) residualIndex * (long) bitWidth;
      residualIndex++;
      final long byteOff = bodyOff + (bitOff >>> 3);
      final int bitInByte = (int) (bitOff & 7L);
      final int bitsFromLo = Math.min(bitWidth, 64 - bitInByte);
      final long loWord = readUpToLongLE(target, byteOff);
      final long loMask = bitsFromLo == 64 ? ~0L : (((1L << bitsFromLo) - 1L) << bitInByte);
      final long loShifted = (zz & ((1L << bitsFromLo) - 1L)) << bitInByte;
      writeWord(target, byteOff, (loWord & ~loMask) | loShifted);
      if (bitsFromLo < bitWidth) {
        final int remaining = bitWidth - bitsFromLo;
        final long hiMask = (1L << remaining) - 1L;
        final long hiWord = readUpToLongLE(target, byteOff + 8L);
        final long hiValBits = (zz >>> bitsFromLo) & hiMask;
        writeWordPartial(target, byteOff + 8L, (hiWord & ~hiMask) | hiValBits,
            (remaining + 7) >>> 3);
      }
    }
  }

  // ─────────────────────────────────────────────────────────────── decoding

  /**
   * Parsed header describing a delta payload. Reusable across calls to
   * minimise allocation pressure on scan paths.
   */
  public static final class Header {
    public byte version;
    /** Bit width per zig-zag residual, 0..64. 0 ⇒ constant stride (no body). */
    public byte bitWidth;
    /** Number of encoded values. */
    public int count;
    /** First value, {@code v[0]}. */
    public long firstValue;
    /** First delta, {@code v[1]-v[0]}. */
    public long firstDelta;
    /** Absolute byte offset of the bit-packed body (or end of header when no body). */
    public long bodyOffset;
    /** Total bytes consumed by the header. */
    public int headerBytes;
    /** Total bytes consumed by the body. */
    public long bodyBytes;

    /** Clear for reuse. Every field is repopulated on parse. */
    public Header reset() {
      version = 0;
      bitWidth = 0;
      count = 0;
      firstValue = 0L;
      firstDelta = 0L;
      bodyOffset = 0L;
      headerBytes = 0;
      bodyBytes = 0L;
      return this;
    }
  }

  /**
   * Parse the delta header at {@code headerOffset} into {@code out}.
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
      throw new IllegalStateException("unsupported NumberRegionDelta version=" + out.version);
    }
    out.bitWidth = src.get(ValueLayout.JAVA_BYTE, headerOffset + 1L);
    if (out.bitWidth < 0 || out.bitWidth > 64) {
      throw new IllegalStateException("illegal bitWidth=" + out.bitWidth);
    }
    final long varintPacked = readVarintUnsignedPacked(src, headerOffset + 2L);
    final long countLong = varintPacked & 0xFFFFFFFFL;
    final int varintBytes = (int) (varintPacked >>> 32);
    if (countLong < 0L || countLong > (long) Integer.MAX_VALUE) {
      throw new IllegalStateException("illegal count=" + countLong);
    }
    out.count = (int) countLong;
    long pos = headerOffset + 2L + varintBytes;
    out.firstValue = src.get(LE.LONG, pos);
    pos += 8L;
    out.firstDelta = src.get(LE.LONG, pos);
    pos += 8L;
    out.headerBytes = (int) (pos - headerOffset);
    out.bodyOffset = pos;
    out.bodyBytes = bodyBytes(out.count, out.bitWidth);
    return out;
  }

  /**
   * Bulk decode into a caller-supplied buffer. Writes exactly
   * {@code header.count} values starting at {@code out[0]}. This is the fast
   * path — a single sequential prefix-sum over the residuals.
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
    long value = header.firstValue;
    out[0] = value;
    if (n == 1) {
      return;
    }
    long delta = header.firstDelta;
    value += delta;
    out[1] = value;
    if (n == 2) {
      return;
    }
    final int bitWidth = header.bitWidth;
    if (bitWidth == 0) {
      // Constant stride — every delta-of-delta is 0.
      for (int i = 2; i < n; i++) {
        value += delta;
        out[i] = value;
      }
      return;
    }
    final long bodyOff = header.bodyOffset;
    for (int i = 2; i < n; i++) {
      final long dd = zigZagDecode(bitUnpack(src, bodyOff, bitWidth, i - 2));
      delta += dd;
      value += delta;
      out[i] = value;
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

  /**
   * Random-access read of the value at {@code index}. Unlike
   * {@link NumberRegionCompact#readCompact}, delta-of-delta has no closed-form
   * random access: this replays the prefix sum from the start and therefore
   * costs {@code O(index)}. Provided for correctness and point probes; scan
   * loops must use {@link #decodeAll}.
   *
   * @param src source segment
   * @param header parsed header
   * @param index 0-based index into the logical sequence
   * @return value stored at {@code index}
   */
  public static long readDelta(final MemorySegment src, final Header header, final int index) {
    if (src == null || header == null) {
      throw new IllegalArgumentException("src/header");
    }
    if (index < 0 || index >= header.count) {
      throw new IndexOutOfBoundsException("index=" + index + " count=" + header.count);
    }
    final long value = header.firstValue;
    if (index == 0) {
      return value;
    }
    final long firstDelta = header.firstDelta;
    final int bitWidth = header.bitWidth;
    if (bitWidth == 0) {
      // Constant stride: v[i] = firstValue + firstDelta*i. The multiply wraps
      // in two's-complement identically to i repeated additions — O(1).
      return value + firstDelta * index;
    }
    long delta = firstDelta;
    long acc = value + delta;
    final long bodyOff = header.bodyOffset;
    for (int i = 2; i <= index; i++) {
      delta += zigZagDecode(bitUnpack(src, bodyOff, bitWidth, i - 2));
      acc += delta;
    }
    return acc;
  }

  // ───────────────────────────────────────────────────────── zig-zag helpers

  /** Map a signed long to an unsigned long: 0,-1,1,-2,2,… → 0,1,2,3,4,… */
  static long zigZagEncode(final long v) {
    return (v << 1) ^ (v >> 63);
  }

  /** Inverse of {@link #zigZagEncode}. */
  static long zigZagDecode(final long z) {
    return (z >>> 1) ^ -(z & 1L);
  }

  // ─────────────────────────────────────────────────────── bit-pack / unpack

  /**
   * Unsigned bit-unpack of the residual at {@code residualIndex}. Widths ≤ 56
   * need a single unaligned 64-bit load; widths 57..64 may straddle two words.
   */
  private static long bitUnpack(final MemorySegment src, final long baseOff, final int bitWidth,
      final int residualIndex) {
    if (bitWidth == 64) {
      return src.get(LE.LONG, baseOff + ((long) residualIndex << 3));
    }
    final long bitOff = (long) residualIndex * (long) bitWidth;
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

  private static void writeWord(final MemorySegment target, final long off, final long word) {
    final long size = target.byteSize();
    if (off + 8L <= size) {
      target.set(LE.LONG, off, word);
      return;
    }
    final long remaining = Math.max(0L, size - off);
    for (long k = 0L; k < remaining; k++) {
      target.set(ValueLayout.JAVA_BYTE, off + k, (byte) (word >>> (k << 3)));
    }
  }

  private static void writeWordPartial(final MemorySegment target, final long off,
      final long word, final int bytes) {
    final long size = target.byteSize();
    final long limit = Math.min((long) bytes, Math.max(0L, size - off));
    for (long k = 0L; k < limit; k++) {
      target.set(ValueLayout.JAVA_BYTE, off + k, (byte) (word >>> (k << 3)));
    }
  }

  /**
   * Read up to 8 little-endian bytes as a {@code long} even if fewer bytes
   * remain. Bytes past the segment end read as zero; callers mask to bitWidth
   * and the valid bits of the last residual always lie inside the segment.
   */
  private static long readUpToLongLE(final MemorySegment src, final long off) {
    final long size = src.byteSize();
    if (off + 8L <= size) {
      return src.get(LE.LONG, off);
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
}

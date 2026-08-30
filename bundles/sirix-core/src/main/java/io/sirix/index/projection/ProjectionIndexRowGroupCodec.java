/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Shared primitive encoders and decoders for the canonical segmented projection format.
 *
 * <p>This class is deliberately package-private and is <b>not</b> a persisted row-group codec.
 * {@link ProjectionIndexColumnSegmentCodec} owns the only supported row-group persistence format;
 * this helper merely centralizes its little-endian access, frame-of-reference bit packing,
 * dictionaries, presence bitmaps, record keys, and order-label primitives so writers and readers
 * cannot drift.
 */
final class ProjectionIndexRowGroupCodec {

  private ProjectionIndexRowGroupCodec() {}

  /**
   * Byte-array view handles for little-endian loads — HotSpot intrinsifies {@code get} on
   * static-final view handles to a single MOVL/MOVQ. The previous byte-assembly form cost 8 dependent
   * byte loads per long; this load is the hot instruction of the bulk unpacker (one per packed
   * value), so the switch is the unpacker's single biggest win. {@link ProjectionIndexByteScan}
   * measured VarHandle vs MemorySegment vs Unsafe on the cold 100M bench (iter#02) — VarHandle won;
   * this mirrors that choice.
   */
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  /**
   * Whether the little-endian accessors go through a {@link VarHandle} or assemble bytes by hand.
   *
   * <p>
   * A byte-array-view VarHandle folds to one load once C2 has compiled the caller, which is why it
   * wins every warm benchmark — the choice recorded above was made on one. A ONE-SHOT query never
   * gets there: profiled cold, {@code checkAccessModeThenIsDirect}, {@code guard_LI_J},
   * {@code VarForm.getMemberName} and {@code ArrayHandle.index} together were ~29 % of the query.
   * Manual assembly has no access-mode check to elide, so it costs the same interpreted, in C1 and in
   * C2.
   */
  private static final boolean MANUAL_LE = System.getProperty("sirix.projection.manualLE") != null
      ? !"false".equals(System.getProperty("sirix.projection.manualLE"))
      // Default: manual on the JVM (the cold one-shot rationale above), VarHandle under a NATIVE
      // IMAGE — AOT compilation removes the interpreter/C1 access-mode checks the manual form
      // exists to avoid, so manual is a pure 5-8x loss per value there, and the bulk hydrate
      // executes these loads tens of millions of times.
      : System.getProperty("org.graalvm.nativeimage.imagecode") == null;

  static long getLongLE(final byte[] b, final int off) {
    if (MANUAL_LE) {
      return (b[off] & 0xFFL) | (b[off + 1] & 0xFFL) << 8 | (b[off + 2] & 0xFFL) << 16 | (b[off + 3] & 0xFFL) << 24
          | (b[off + 4] & 0xFFL) << 32 | (b[off + 5] & 0xFFL) << 40 | (b[off + 6] & 0xFFL) << 48
          | (b[off + 7] & 0xFFL) << 56;
    }
    return (long) LONG_LE.get(b, off);
  }

  static void encodeRecordKeys(final ByteArrayOutputStream out, final long[] keys, final int rowCount) {
    boolean ascending = true;
    for (int i = 1; i < rowCount; i++) {
      if (keys[i] < keys[i - 1]) {
        ascending = false;
        break;
      }
    }
    if (ascending) {
      long maxDelta = 0;
      for (int i = 1; i < rowCount; i++) {
        final long d = keys[i] - keys[i - 1];
        if (d > maxDelta)
          maxDelta = d;
      }
      final int width = widthOf(maxDelta);
      out.write(0); // key mode 0 = delta-FOR
      putLongLE(out, keys[0]);
      out.write(width);
      final BitWriter bw = new BitWriter(out);
      for (int i = 1; i < rowCount; i++) {
        bw.write(keys[i] - keys[i - 1], width);
      }
      bw.flush();
    } else {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = 0; i < rowCount; i++) {
        if (keys[i] < min)
          min = keys[i];
        if (keys[i] > max)
          max = keys[i];
      }
      final int width = rangeWidth(min, max);
      out.write(1); // key mode 1 = absolute-FOR
      putLongLE(out, min);
      out.write(width);
      final BitWriter bw = new BitWriter(out);
      for (int i = 0; i < rowCount; i++) {
        bw.write(keys[i] - min, width);
      }
      bw.flush();
    }
  }

  static void encodeForBitPacked(final ByteArrayOutputStream out, final long[] values, final int rowCount) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < rowCount; i++) {
      if (values[i] < min)
        min = values[i];
      if (values[i] > max)
        max = values[i];
    }
    final int width = rangeWidth(min, max);
    putLongLE(out, min);
    out.write(width);
    if (width > 0) {
      final BitWriter bw = new BitWriter(out);
      for (int i = 0; i < rowCount; i++) {
        bw.write(values[i] - min, width);
      }
      bw.flush();
    }
  }

  /**
   * {@link #encodeForBitPacked} for NUMERIC_DOUBLE value streams
   * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-6): probes ALP ({@link ProjectionAlpEncoding}) and
   * emits the width-escape wire form when strictly smaller; otherwise falls through to the plain FOR
   * form byte-identically to before — non-decimal data and pre-ALP stores are unaffected.
   * Deterministic either way, so the descriptor-hash no-op carry-forward stays stable.
   */
  static void encodeForBitPackedDouble(final ByteArrayOutputStream out, final long[] values, final int rowCount) {
    final ProjectionAlpEncoding.Encoded alp =
        ProjectionAlpEncoding.tryEncode(values, rowCount, plainForSizeBytes(values, rowCount));
    if (alp == null) {
      encodeForBitPacked(out, values, rowCount);
      return;
    }
    putLongLE(out, 0L); // reserved base slot — the shared decoder reads it unconditionally
    out.write(ProjectionAlpEncoding.WIDTH_ESCAPE_ALP);
    out.write(alp.e());
    out.write(alp.f());
    putIntLE(out, alp.exceptionRows().length);
    encodeForBitPacked(out, alp.digits(), rowCount);
    final int[] exceptionRows = alp.exceptionRows();
    final long[] exceptionBits = alp.exceptionBits();
    for (int i = 0; i < exceptionRows.length; i++) {
      putIntLE(out, exceptionRows[i]);
      putLongLE(out, exceptionBits[i]);
    }
  }

  /** Exact byte size {@link #encodeForBitPacked} would emit — ALP's profitability bar. */
  private static int plainForSizeBytes(final long[] values, final int rowCount) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < rowCount; i++) {
      if (values[i] < min)
        min = values[i];
      if (values[i] > max)
        max = values[i];
    }
    final int width = rangeWidth(min, max);
    return 8 + 1 + ((rowCount * width + 7) >>> 3);
  }

  /** Number of populated (null-terminated) dictionary slots. Shared dict-size authority. */
  static int dictSizeOf(final byte[][] dict) {
    int dictSize = 0;
    while (dictSize < dict.length && dict[dictSize] != null) {
      dictSize++;
    }
    return dictSize;
  }

  /** Range-backed dictionary encoder used by live pages without materialising {@code byte[][]}. */
  static void encodeDictEntries(final ByteArrayOutputStream out, final ProjectionIndexRowGroupPage page,
      final int column) {
    final int dictSize = page.stringDictionarySize(column);
    putIntLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      putIntLE(out, page.stringDictionaryEntryLength(column, i));
    }
    for (int i = 0; i < dictSize; i++) {
      out.write(page.stringDictionaryEntryBacking(column, i), page.stringDictionaryEntryOffset(column, i),
          page.stringDictionaryEntryLength(column, i));
    }
  }

  /** Id-stream encoder when the caller already has the representation-independent live size. */
  static void encodeDictIds(final ByteArrayOutputStream out, final int dictSize, final int[] ids, final int rowCount) {
    final int width = dictSize <= 1
        ? 0
        : widthOf(dictSize - 1L);
    out.write(width);
    if (width > 0) {
      final BitWriter bw = new BitWriter(out);
      for (int i = 0; i < rowCount; i++) {
        bw.write(ids[i], width);
      }
      bw.flush();
    }
  }

  static void encodePresence(final ByteArrayOutputStream out, final long[] bits, final int rowCount) {
    final int words = (rowCount + 63) >>> 6;
    boolean allPresent = true;
    boolean allMissing = true;
    for (int w = 0; w < words; w++) {
      final long expect = expectedFullWord(w, words, rowCount);
      if (bits[w] != expect)
        allPresent = false;
      if (bits[w] != 0L)
        allMissing = false;
    }
    if (allPresent) {
      out.write(0);
    } else if (allMissing) {
      out.write(1);
    } else {
      out.write(2);
      for (int w = 0; w < words; w++) {
        putLongLE(out, bits[w]);
      }
    }
  }

  static long[] decodeRecordKeys(final Cursor in, final int rowCount) {
    final int mode = in.readByte() & 0xFF;
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    final long[] keys = new long[rowCount];
    if (mode == 0) {
      keys[0] = base;
      if (rowCount > 1) {
        unpackInto(in, rowCount - 1, width, 0L, keys, 1);
        for (int i = 1; i < rowCount; i++) {
          keys[i] += keys[i - 1];
        }
      }
    } else if (mode == 1) {
      unpackInto(in, rowCount, width, base, keys, 0);
    } else {
      throw new IllegalStateException("Bad record-key mode " + mode);
    }
    return keys;
  }

  static void encodeOrderLabels(final ByteArrayOutputStream out, final ProjectionIndexRowGroupPage page) {
    final int rowCount = page.getRowCount();
    final byte[] bytes = page.orderLabelBytes();
    final int[] offsets = page.orderLabelOffsets();
    final int length = page.orderLabelLength();
    if (rowCount == 0 && (bytes == null || offsets == null)) {
      putIntLE(out, 0);
      putIntLE(out, 0);
      return;
    }
    ProjectionIndexRowGroupPage.validateOrderLabels(rowCount, bytes, offsets, length);
    putIntLE(out, length);
    for (int row = 0; row <= rowCount; row++) {
      putIntLE(out, offsets[row]);
    }
    out.write(bytes, 0, length);
  }

  static OrderLabels decodeOrderLabels(final Cursor in, final int rowCount) {
    final int length = in.readInt();
    if (length < 0 || length > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
      throw new IllegalStateException("invalid projection Dewey order-label byte length " + length);
    }
    final int[] offsets = new int[ProjectionIndexRowGroupPage.MAX_ROWS + 1];
    for (int row = 0; row <= rowCount; row++) {
      offsets[row] = in.readInt();
    }
    final byte[] bytes = in.readBytes(length);
    ProjectionIndexRowGroupPage.validateOrderLabels(rowCount, bytes, offsets, length);
    return new OrderLabels(bytes, offsets);
  }

  record OrderLabels(byte[] bytes, int[] offsets) {
  }

  /**
   * Inverse of {@link #encodeForBitPacked}/{@link #encodeForBitPackedDouble}: FOR base + width +
   * packed values, where width byte {@link ProjectionAlpEncoding#WIDTH_ESCAPE_ALP} selects the ALP
   * branch (double columns only ever WRITE it, but decode is safe unconditionally — no other encoder
   * emits it). Width bytes 66..255 remain RESERVED escapes for future numeric encodings — rejecting
   * them loudly keeps those additive (old readers fail attributably instead of misparsing packed
   * bits), with no version machinery.
   */
  static long[] decodeForBitPackedColumn(final Cursor in, final int rowCount) {
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    if (width == ProjectionAlpEncoding.WIDTH_ESCAPE_ALP) {
      return ProjectionAlpEncoding.decode(in, rowCount);
    }
    if (width > 64) {
      throw new IllegalStateException("Reserved numeric-encoding escape " + width + " — written by a newer version");
    }
    return unpackFor(in, rowCount, base, width);
  }

  /**
   * Plain-FOR decode with NO escape handling — ALP's digits stream decoder (an escape byte inside an
   * ALP payload is corruption, not nesting).
   */
  static long[] decodePlainForBitPacked(final Cursor in, final int rowCount) {
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    if (width > 64) {
      throw new IllegalStateException("Corrupt nested numeric-encoding escape " + width);
    }
    return unpackFor(in, rowCount, base, width);
  }

  private static long[] unpackFor(final Cursor in, final int rowCount, final long base, final int width) {
    final long[] values = new long[rowCount];
    unpackInto(in, rowCount, width, base, values, 0);
    return values;
  }

  /** Boolean column body: packed words verbatim. */
  static long[] decodeBooleanWords(final Cursor in, final int words) {
    final long[] bits = new long[words];
    for (int w = 0; w < words; w++) {
      bits[w] = in.readLong();
    }
    return bits;
  }

  /** Inverse of {@link #encodeDictEntries}; pads the dict array to the interning floor of 16. */
  /**
   * The dictionary half read as ONE flat run: {@code offsets[i]..offsets[i+1]} bounds entry {@code i}
   * inside the returned buffer, and {@code offsets.length - 1} is the exact entry count.
   *
   * <p>
   * ZERO COPY: the raw wire form already stores the entries concatenated, so the buffer handed back
   * IS the segment and the offsets are absolute into it. That is the whole point of the flat form —
   * {@link #decodeDictEntries} allocates one {@code byte[]} per entry, which on a high-cardinality
   * column (a dictionary nearly as large as the leaf) is the dominant allocation of a column fill.
   *
   * @return the offsets; the entry bytes live in {@code in}'s own buffer
   */
  static int[] decodeFlatDictEntries(final Cursor in) {
    final int dictSize = in.readInt();
    if (dictSize < 0) {
      throw new IllegalStateException("Negative dictionary size " + dictSize);
    }
    final int[] offsets = new int[dictSize + 1];
    // Lengths come first, then the concatenated bytes: one pass turns the lengths into absolute
    // offsets, and the base is wherever the byte run starts (right after the length table).
    int total = 0;
    for (int i = 0; i < dictSize; i++) {
      final int len = in.readInt();
      if (len < 0) {
        throw new IllegalStateException("Negative dictionary entry length " + len + " at " + i);
      }
      total += len;
      offsets[i + 1] = total;
    }
    final int base = in.position();
    if (base + total > in.buffer().length) {
      throw new IllegalStateException("Dictionary run of " + total + " bytes overruns the segment");
    }
    for (int i = 0; i <= dictSize; i++) {
      offsets[i] += base;
    }
    in.skip(total);
    return offsets;
  }

  static byte[][] decodeDictEntries(final Cursor in) {
    final int dictSize = in.readInt();
    final int[] lens = new int[dictSize];
    for (int i = 0; i < dictSize; i++) {
      lens[i] = in.readInt();
    }
    final byte[][] dict = new byte[Math.max(16, dictSize)][];
    for (int i = 0; i < dictSize; i++) {
      dict[i] = in.readBytes(lens[i]);
    }
    return dict;
  }

  /** Inverse of {@link #encodeDictIds}: width byte + packed ids. */
  /**
   * Bit-pack {@code count} non-negative values whose maximum is {@code maxValue}, in the same
   * {@code [width][packed]} shape {@link #encodeDictIds} writes and {@link #decodePackedIds} reads.
   *
   * <p>
   * Exists for the per-row element counts of a STRING_SET column, whose width comes from the largest
   * set on the leaf rather than from a dictionary size. A leaf where every row holds the same number
   * of elements packs those counts to zero bits.
   */
  static void encodePackedIds(final ByteArrayOutputStream out, final int[] values, final int count,
      final int maxValue) {
    final int width = maxValue <= 0
        ? 0
        : widthOf(maxValue);
    out.write(width);
    if (width > 0) {
      final BitWriter bw = new BitWriter(out);
      for (int i = 0; i < count; i++) {
        bw.write(values[i], width);
      }
      bw.flush();
    }
  }

  static int[] decodePackedIds(final Cursor in, final int rowCount) {
    final int width = in.readByte() & 0xFF;
    final int[] ids = new int[rowCount];
    if (width > 0) {
      unpackIntsInto(in, rowCount, width, ids);
    }
    return ids;
  }

  /** Inverse of {@link #encodePresence}, filling {@code bits} in place. */
  static void decodePresenceInto(final Cursor in, final long[] bits, final int presWords, final int rowCount) {
    final int mode = in.readByte() & 0xFF;
    switch (mode) {
      case 0 -> {
        for (int w = 0; w < presWords; w++) {
          bits[w] = expectedFullWord(w, presWords, rowCount);
        }
      }
      case 1 -> {
        /* all-missing — words stay zero */ }
      case 2 -> {
        for (int w = 0; w < presWords; w++) {
          bits[w] = in.readLong();
        }
      }
      default -> throw new IllegalStateException("Bad presence marker " + mode);
    }
  }

  // ==================== helpers ====================

  /** Bits needed to represent {@code maxValue >= 0}; 0 for 0. */
  static int widthOf(final long maxValue) {
    return clampPackWidth(64 - Long.numberOfLeadingZeros(maxValue));
  }

  /** FOR width for [min, max]; 64 when the range overflows a signed long. */
  static int rangeWidth(final long min, final long max) {
    try {
      return widthOf(Math.subtractExact(max, min));
    } catch (final ArithmeticException overflow) {
      return 64;
    }
  }

  /**
   * A byte-at-a-time accumulator holds at most 63 usable bits (a byte shifted by
   * {@code avail > 56} loses its top bits past bit 63), so packed runs are capped at 56 bits;
   * anything wider uses the aligned raw 64-bit path. Wider-than-56-bit ranges are pathological for
   * FOR packing anyway — the raw path costs at most 1 byte/value more.
   */
  static int clampPackWidth(final int width) {
    return width > 56
        ? 64
        : width;
  }

  /** The presence word value of a fully-present leaf at word {@code w}. */
  static long expectedFullWord(final int w, final int words, final int rowCount) {
    return w == words - 1 && (rowCount & 63) != 0
        ? (1L << (rowCount & 63)) - 1
        : -1L;
  }

  /** Little-endian {@code int} into an array at {@code off} — the block writers' primitive. */
  static void putIntLEAt(final byte[] b, final int off, final int v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
    b[off + 2] = (byte) (v >>> 16);
    b[off + 3] = (byte) (v >>> 24);
  }

  /** Little-endian {@code long} into an array at {@code off} — twin of {@link #putIntLEAt}. */
  static void putLongLEAt(final byte[] b, final int off, final long v) {
    putIntLEAt(b, off, (int) v);
    putIntLEAt(b, off + 4, (int) (v >>> 32));
  }

  static void putIntLE(final ByteArrayOutputStream out, final int v) {
    out.write(v);
    out.write(v >>> 8);
    out.write(v >>> 16);
    out.write(v >>> 24);
  }

  static void putLongLE(final ByteArrayOutputStream out, final long v) {
    putIntLE(out, (int) v);
    putIntLE(out, (int) (v >>> 32));
  }

  static int getIntLE(final byte[] b, final int off) {
    if (MANUAL_LE) {
      return (b[off] & 0xFF) | (b[off + 1] & 0xFF) << 8 | (b[off + 2] & 0xFF) << 16 | (b[off + 3] & 0xFF) << 24;
    }
    return (int) INT_LE.get(b, off);
  }

  /** Little-endian byte cursor over a compact payload. */
  static final class Cursor {
    private final byte[] buf;
    private int pos;

    Cursor(final byte[] buf, final int pos) {
      this.buf = buf;
      this.pos = pos;
    }

    byte readByte() {
      return buf[pos++];
    }

    int readInt() {
      final int v = getIntLE(buf, pos);
      pos += 4;
      return v;
    }

    long readLong() {
      final long lo = readInt() & 0xFFFFFFFFL;
      final long hi = readInt() & 0xFFFFFFFFL;
      return lo | (hi << 32);
    }

    byte[] readBytes(final int n) {
      final byte[] out = new byte[n];
      System.arraycopy(buf, pos, out, 0, n);
      pos += n;
      return out;
    }

    /** The backing segment — for readers that decode IN PLACE instead of copying out. */
    byte[] buffer() {
      return buf;
    }

    /** Absolute position of the next unread byte within {@link #buffer()}. */
    int position() {
      return pos;
    }

    /** Advance past {@code n} bytes already consumed straight from {@link #buffer()}. */
    void skip(final int n) {
      pos += n;
    }
  }

  /** LSB-first bit packer emitting whole bytes into the output stream. */
  static final class BitWriter {
    private final ByteArrayOutputStream out;
    private long acc;
    private int used;

    BitWriter(final ByteArrayOutputStream out) {
      this.out = out;
    }

    void write(final long value, final int width) {
      if (width == 0)
        return;
      if (width == 64) {
        flush();
        putLongLE(out, value);
        return;
      }
      final long masked = value & ((1L << width) - 1);
      acc |= masked << used;
      used += width;
      if (used >= 64) {
        putLongLE(out, acc);
        used -= 64;
        acc = used == 0
            ? 0L
            : masked >>> (width - used);
      }
      // Note: when used < 64 the accumulator still holds the partial bits.
    }

    void flush() {
      int remaining = used;
      long rest = acc;
      while (remaining > 0) {
        out.write((int) rest);
        rest >>>= 8;
        remaining -= 8;
      }
      acc = 0L;
      used = 0;
    }
  }

  /**
   * Bulk bit-unpacker — the decode hot path (hydrate assembles ~10k packed runs per projection load;
   * the former per-byte accumulator with its per-byte {@link Cursor} call was the dominant
   * cost). Reads {@code count} {@code width}-bit little-endian values (exactly
   * {@code ceil(count·width / 8)} bytes, adds
   * {@code base}, writes {@code out[0..count)}.
   *
   * <p>
   * Main loop: one unaligned 8-byte window load per value ({@code width + 7 ≤ 64} holds for widths ≤
   * 57 — wider widths and the last few values whose window would over-read the source array take the
   * scalar accumulator path instead).
   *
   * <p>
   * <b>Deliberately scalar — a measured verdict.</b> {@code ProjectionFoldKernelBenchmark} A/B-tested
   * this loop against a {@code BitUnpackSimd}-style vector group unpack (two loads, two permutes,
   * three shifts per 8 values) over the same packed blocks: the windowed scalar loop won 1.7 vs
   * 4.5&nbsp;ns/row. The vector unpacker earns its keep in the PAX kernels by feeding lanes straight
   * into vector consumers; here the destination is a materialized {@code long[]} scratch block, and
   * one out-of-order-friendly load per value beats the permute/shift cascade. Re-run the bench before
   * revisiting.
   */
  static void unpackInto(final Cursor in, final int count, final int width, final long base, final long[] out,
      final int outOff) {
    in.pos = unpackInto(in.buf, in.pos, count, width, base, out, outOff);
  }

  /**
   * Positional core of {@link #unpackInto(Cursor, int, int, long, long[], int)}: unpack {@code count}
   * {@code width}-bit values starting at BYTE-ALIGNED position {@code pos}, returning the byte
   * position after the consumed run. The chunked fold kernels call this per value block — block
   * starts stay byte-aligned because 1024·width bits is a whole number of bytes for every width.
   */
  static int unpackInto(final byte[] src, final int pos, final int count, final int width, final long base,
      final long[] out, final int outOff) {
    if (width == 0) {
      Arrays.fill(out, outOff, outOff + count, base);
      return pos;
    }
    if (width == 64) {
      for (int i = 0; i < count; i++) {
        out[outOff + i] = base + getLongLE(src, pos + (i << 3));
      }
      return pos + (count << 3);
    }
    final int end = pos + ((count * width + 7) >>> 3);
    final long mask = (1L << width) - 1L;
    int i = 0;
    if (width <= 57) {
      long bitPos = 0;
      // Windowed loads must stay inside src: stop where an 8-byte read would over-run.
      final int safeBytes = src.length - 8;
      while (i < count) {
        final int bytePos = pos + (int) (bitPos >>> 3);
        if (bytePos > safeBytes) {
          break;
        }
        out[outOff + i] = base + ((getLongLE(src, bytePos) >>> (bitPos & 7)) & mask);
        bitPos += width;
        i++;
      }
    }
    if (i < count) {
      // Scalar tail (and the width > 57 case): classic accumulator from the exact bit offset.
      long bitPos = (long) i * width;
      int bytePos = pos + (int) (bitPos >>> 3);
      long acc = 0L;
      int avail = 0;
      final int skew = (int) (bitPos & 7);
      if (skew != 0) {
        acc = (src[bytePos++] & 0xFFL) >>> skew;
        avail = 8 - skew;
      }
      while (i < count) {
        while (avail < width) {
          acc |= (long) (src[bytePos++] & 0xFF) << avail;
          avail += 8;
        }
        out[outOff + i] = base + (acc & mask);
        acc >>>= width;
        avail -= width;
        i++;
      }
    }
    return end;
  }

  /** {@link #unpackInto(Cursor, int, int, long, long[], int)} for int outputs (dict ids). */
  static void unpackIntsInto(final Cursor in, final int count, final int width, final int[] out) {
    if (width == 0) {
      Arrays.fill(out, 0, count, 0);
      return;
    }
    final byte[] src = in.buf;
    final int pos = in.pos;
    final int end = pos + ((count * width + 7) >>> 3);
    final long mask = (1L << width) - 1L;
    int i = 0;
    if (width <= 57) {
      long bitPos = 0;
      final int safeBytes = src.length - 8;
      while (i < count) {
        final int bytePos = pos + (int) (bitPos >>> 3);
        if (bytePos > safeBytes) {
          break;
        }
        out[i] = (int) ((getLongLE(src, bytePos) >>> (bitPos & 7)) & mask);
        bitPos += width;
        i++;
      }
    }
    if (i < count) {
      long bitPos = (long) i * width;
      int bytePos = pos + (int) (bitPos >>> 3);
      long acc = 0L;
      int avail = 0;
      final int skew = (int) (bitPos & 7);
      if (skew != 0) {
        acc = (src[bytePos++] & 0xFFL) >>> skew;
        avail = 8 - skew;
      }
      while (i < count) {
        while (avail < width) {
          acc |= (long) (src[bytePos++] & 0xFF) << avail;
          avail += 8;
        }
        out[i] = (int) (acc & mask);
        acc >>>= width;
        avail -= width;
        i++;
      }
    }
    in.pos = end;
  }

}

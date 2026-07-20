/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Storage codec for projection leaves: converts between the flat
 * scan-friendly byte layout the {@link ProjectionIndexByteScan} kernels
 * operate on (the "raw" form — see {@link ProjectionIndexLeafPage}'s class
 * javadoc) and a compact persisted form. The raw form trades space for
 * branch-free fixed-stride access (raw 8-byte numerics, 4-byte dict-ids,
 * full presence words); persisting it verbatim roughly doubles the store.
 * The compact form applies:
 *
 * <ul>
 *   <li><b>record keys</b> — delta + frame-of-reference bit-packing when
 *       ascending (the builder's document-order walk), absolute FOR
 *       otherwise;</li>
 *   <li><b>NUMERIC_LONG columns</b> — frame-of-reference base + minimal
 *       bit-width packing ({@code width == 0} collapses a constant column,
 *       including the all-default body of an all-missing column, to 9
 *       bytes);</li>
 *   <li><b>STRING_DICT columns</b> — dictionary verbatim (tiny), dict-ids
 *       bit-packed to {@code ceil(log2(dictSize))} bits;</li>
 *   <li><b>BOOLEAN columns</b> — packed words verbatim (already 1 bit/row);</li>
 *   <li><b>presence bitmaps</b> — one marker byte for the all-present /
 *       all-missing cases, literal words otherwise.</li>
 * </ul>
 *
 * <p>{@link #decode} reconstructs the raw payload <b>byte-identically</b>
 * (it re-assembles a {@link ProjectionIndexLeafPage} and re-serialises it),
 * so hydrated leaves are indistinguishable from freshly built ones —
 * presence, unrepresentable and integrality provenance included. Encode →
 * decode is a strict identity on any valid raw leaf.
 *
 * <p>Compact payloads are recognised by a leading {@link #COMPACT_MAGIC};
 * {@link #decode} passes non-compact payloads through unchanged (a raw
 * leaf's first int is its row count {@code <= 1024}, which can never
 * collide with the magic).
 */
public final class ProjectionIndexLeafCodec {

  /** Leading magic of a compact payload ("PIXC" little-endian). */
  public static final int COMPACT_MAGIC = 0x43585049;

  /**
   * Version byte written immediately after {@link #COMPACT_MAGIC}. A future layout change bumps
   * this instead of minting a new magic; {@link #decode} fails fast on an unknown value (a
   * version mismatch can only mean a newer writer or corruption — the metadata's own version
   * gate triggers a rebuild before hydration ever reaches an incompatible leaf).
   */
  public static final byte COMPACT_VERSION = 1;

  private ProjectionIndexLeafCodec() {
  }

  /**
   * Record-key zone map {@code [firstRecordKey, lastRecordKey]} of a
   * persisted leaf payload, read from its HEAD bytes without materialising
   * the leaf — the single canonical header-range reader for BOTH persisted
   * forms (compact: keys at offsets 13/21 after magic + version byte; raw serialised:
   * offsets 8/16 — see {@link ProjectionIndexLeafPage#columnCountOf} for the
   * raw header's canonical column reader). Callers may pass just the head
   * chunk of a chunked store; returns {@code null} when the bytes are too
   * short to carry the range (caller falls back to the full payload).
   */
  public static long @Nullable [] recordKeyRange(final byte @Nullable [] head) {
    if (head == null) {
      return null;
    }
    if (head.length >= 4 && getIntLE(head, 0) == COMPACT_MAGIC) {
      if (head.length < 29) {
        return null;
      }
      return new long[] { getLongLE(head, 13), getLongLE(head, 21) };
    }
    if (head.length < 24) {
      return null;
    }
    return new long[] { getLongLE(head, 8), getLongLE(head, 16) };
  }

  static long getLongLE(final byte[] b, final int off) {
    return (getIntLE(b, off) & 0xFFFFFFFFL) | ((long) getIntLE(b, off + 4) << 32);
  }

  // ==================== encode ====================

  /**
   * Encode a raw leaf payload into the compact persisted form.
   *
   * @throws IllegalStateException when {@code rawPayload} is not a valid raw
   *         leaf (propagated from {@link ProjectionIndexLeafPage#deserialize}).
   */
  public static byte[] encode(final byte[] rawPayload) {
    if (rawPayload == null) {
      return null;
    }
    final ProjectionIndexLeafPage page = ProjectionIndexLeafPage.deserialize(rawPayload);
    final int rowCount = page.getRowCount();
    final int columnCount = page.getColumnCount();
    final ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
    putIntLE(out, COMPACT_MAGIC);
    out.write(COMPACT_VERSION);
    putIntLE(out, rowCount);
    putIntLE(out, columnCount);
    putLongLE(out, page.firstRecordKey());
    putLongLE(out, page.lastRecordKey());
    for (int c = 0; c < columnCount; c++) {
      out.write(page.columnKind(c));
    }
    if (rowCount > 0) {
      encodeRecordKeys(out, page.recordKeys(), rowCount);
      for (int c = 0; c < columnCount; c++) {
        putLongLE(out, page.columnMin(c));
        putLongLE(out, page.columnMax(c));
        switch (page.columnKind(c)) {
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG ->
              encodeForBitPacked(out, page.numericColumn(c), rowCount);
          case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
            final long[] bits = page.booleanColumnBits(c);
            final int words = (rowCount + 63) >>> 6;
            for (int w = 0; w < words; w++) {
              putLongLE(out, bits[w]);
            }
          }
          case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT ->
              encodeStringDict(out, page.stringDictionary(c), page.stringDictIdColumn(c), rowCount);
          default -> throw new IllegalStateException("Unknown column kind " + page.columnKind(c));
        }
      }
    }
    // Tail: flags verbatim, presence per column as marker byte or literal words.
    for (int c = 0; c < columnCount; c++) {
      byte flags = page.columnUnrepresentable(c) ? ProjectionIndexLeafPage.COLUMN_FLAG_UNREPRESENTABLE : 0;
      if (page.columnNumericNonIntegral(c)) {
        flags |= ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL;
      }
      out.write(flags);
    }
    if (rowCount > 0) {
      for (int c = 0; c < columnCount; c++) {
        encodePresence(out, page.presenceColumnBits(c), rowCount);
      }
    }
    return out.toByteArray();
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
        if (d > maxDelta) maxDelta = d;
      }
      final int width = widthOf(maxDelta);
      out.write(0);                       // key mode 0 = delta-FOR
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
        if (keys[i] < min) min = keys[i];
        if (keys[i] > max) max = keys[i];
      }
      final int width = rangeWidth(min, max);
      out.write(1);                       // key mode 1 = absolute-FOR
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
      if (values[i] < min) min = values[i];
      if (values[i] > max) max = values[i];
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

  private static void encodeStringDict(final ByteArrayOutputStream out, final byte[][] dict,
      final int[] ids, final int rowCount) {
    int dictSize = 0;
    while (dictSize < dict.length && dict[dictSize] != null) {
      dictSize++;
    }
    putIntLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      putIntLE(out, dict[i].length);
    }
    for (int i = 0; i < dictSize; i++) {
      out.write(dict[i], 0, dict[i].length);
    }
    final int width = dictSize <= 1 ? 0 : widthOf(dictSize - 1L);
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
      if (bits[w] != expect) allPresent = false;
      if (bits[w] != 0L) allMissing = false;
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

  // ==================== decode ====================

  /**
   * Decode a compact payload back to the raw scan form; non-compact
   * payloads (no leading {@link #COMPACT_MAGIC}) pass through unchanged.
   */
  public static byte[] decode(final byte[] payload) {
    if (payload == null || payload.length < 4 || getIntLE(payload, 0) != COMPACT_MAGIC) {
      return payload;
    }
    if (payload.length < 5 || payload[4] != COMPACT_VERSION) {
      throw new IllegalStateException("Unknown compact projection-leaf version "
          + (payload.length < 5 ? "<missing>" : payload[4]) + " (expected " + COMPACT_VERSION
          + ") — written by a newer version or corrupt");
    }
    final Cursor in = new Cursor(payload, 5);
    final int rowCount = in.readInt();
    final int columnCount = in.readInt();
    final long firstRecordKey = in.readLong();
    final long lastRecordKey = in.readLong();
    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = in.readByte();
    }
    final long[] recordKeys = rowCount > 0 ? decodeRecordKeys(in, rowCount) : new long[0];
    final long[] columnMin = new long[columnCount];
    final long[] columnMax = new long[columnCount];
    final long[][] numericCols = new long[columnCount][];
    final long[][] booleanCols = new long[columnCount][];
    final int[][] dictIdCols = new int[columnCount][];
    final byte[][][] dicts = new byte[columnCount][][];
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    if (rowCount > 0) {
      for (int c = 0; c < columnCount; c++) {
        columnMin[c] = in.readLong();
        columnMax[c] = in.readLong();
        switch (kinds[c]) {
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> {
            final long base = in.readLong();
            final int width = in.readByte() & 0xFF;
            final long[] values = new long[rowCount];
            if (width == 0) {
              Arrays.fill(values, base);
            } else {
              final BitReader br = new BitReader(in);
              for (int i = 0; i < rowCount; i++) {
                values[i] = base + br.read(width);
              }
              br.align();
            }
            numericCols[c] = values;
          }
          case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
            final long[] bits = new long[presWords];
            for (int w = 0; w < presWords; w++) {
              bits[w] = in.readLong();
            }
            booleanCols[c] = bits;
          }
          case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
            final int dictSize = in.readInt();
            final int[] lens = new int[dictSize];
            for (int i = 0; i < dictSize; i++) {
              lens[i] = in.readInt();
            }
            final byte[][] dict = new byte[Math.max(16, dictSize)][];
            for (int i = 0; i < dictSize; i++) {
              dict[i] = in.readBytes(lens[i]);
            }
            dicts[c] = dict;
            final int width = in.readByte() & 0xFF;
            final int[] ids = new int[rowCount];
            if (width > 0) {
              final BitReader br = new BitReader(in);
              for (int i = 0; i < rowCount; i++) {
                ids[i] = (int) br.read(width);
              }
              br.align();
            }
            dictIdCols[c] = ids;
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    final boolean[] unrep = new boolean[columnCount];
    final boolean[] nonIntegral = new boolean[columnCount];
    for (int c = 0; c < columnCount; c++) {
      final byte flags = in.readByte();
      unrep[c] = (flags & ProjectionIndexLeafPage.COLUMN_FLAG_UNREPRESENTABLE) != 0;
      nonIntegral[c] = (flags & ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL) != 0;
    }
    final long[][] presence = new long[columnCount][];
    for (int c = 0; c < columnCount; c++) {
      final long[] bits = new long[Math.max(presWords, (ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6)];
      presence[c] = bits;
      if (rowCount == 0) continue;
      final int mode = in.readByte() & 0xFF;
      switch (mode) {
        case 0 -> {
          for (int w = 0; w < presWords; w++) {
            bits[w] = expectedFullWord(w, presWords, rowCount);
          }
        }
        case 1 -> { /* all-missing — words stay zero */ }
        case 2 -> {
          for (int w = 0; w < presWords; w++) {
            bits[w] = in.readLong();
          }
        }
        default -> throw new IllegalStateException("Bad presence marker " + mode);
      }
    }
    final ProjectionIndexLeafPage page = ProjectionIndexLeafPage.reconstruct(kinds, rowCount,
        firstRecordKey, lastRecordKey, recordKeys, columnMin, columnMax,
        numericCols, booleanCols, dictIdCols, dicts, presence, unrep, nonIntegral);
    return page.serialize();
  }

  static long[] decodeRecordKeys(final Cursor in, final int rowCount) {
    final int mode = in.readByte() & 0xFF;
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    final long[] keys = new long[rowCount];
    if (mode == 0) {
      keys[0] = base;
      final BitReader br = new BitReader(in);
      for (int i = 1; i < rowCount; i++) {
        keys[i] = keys[i - 1] + (width == 0 ? 0 : br.read(width));
      }
      br.align();
    } else if (mode == 1) {
      final BitReader br = new BitReader(in);
      for (int i = 0; i < rowCount; i++) {
        keys[i] = base + (width == 0 ? 0 : br.read(width));
      }
      br.align();
    } else {
      throw new IllegalStateException("Bad record-key mode " + mode);
    }
    return keys;
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
   * The byte-at-a-time {@link BitReader} accumulates at most 63 usable bits
   * (a byte shifted by {@code avail > 56} loses its top bits past bit 63),
   * so packed runs are capped at 56 bits; anything wider uses the aligned
   * raw 64-bit path. Wider-than-56-bit ranges are pathological for FOR
   * packing anyway — the raw path costs at most 1 byte/value more.
   */
  static int clampPackWidth(final int width) {
    return width > 56 ? 64 : width;
  }

  /** The presence word value of a fully-present leaf at word {@code w}. */
  static long expectedFullWord(final int w, final int words, final int rowCount) {
    return w == words - 1 && (rowCount & 63) != 0 ? (1L << (rowCount & 63)) - 1 : -1L;
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
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
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
      if (width == 0) return;
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
        acc = used == 0 ? 0L : masked >>> (width - used);
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

  /** LSB-first bit reader mirroring {@link BitWriter}. */
  static final class BitReader {
    private final Cursor in;
    private long acc;
    private int avail;

    BitReader(final Cursor in) {
      this.in = in;
    }

    long read(final int width) {
      if (width == 0) return 0L;
      if (width == 64) {
        if (avail != 0) {
          throw new IllegalStateException("64-bit read on unaligned stream");
        }
        return in.readLong();
      }
      while (avail < width) {
        acc |= (long) (in.readByte() & 0xFF) << avail;
        avail += 8;
      }
      final long v = acc & ((1L << width) - 1);
      acc >>>= width;
      avail -= width;
      return v;
    }

    /** Drop any partial-byte state at the end of a packed run. */
    void align() {
      acc = 0L;
      avail = 0;
    }
  }
}

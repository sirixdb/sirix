/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
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

  /**
   * Byte-array view handles for little-endian loads — HotSpot intrinsifies {@code get} on
   * static-final view handles to a single MOVL/MOVQ. The previous byte-assembly form cost
   * 8 dependent byte loads per long; this load is the hot instruction of the bulk
   * unpacker (one per packed value), so the switch is the unpacker's single biggest win.
   * {@link ProjectionIndexByteScan} measured VarHandle vs MemorySegment vs Unsafe on the
   * cold 100M bench (iter#02) — VarHandle won; this mirrors that choice.
   */
  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  static long getLongLE(final byte[] b, final int off) {
    return (long) LONG_LE.get(b, off);
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
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              encodeForBitPackedDouble(out, page.numericColumn(c), rowCount);
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
      if (page.columnPureDoubleSource(c)) {
        flags |= ProjectionIndexLeafPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE;
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

  /**
   * {@link #encodeForBitPacked} for NUMERIC_DOUBLE value streams
   * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-6): probes ALP
   * ({@link ProjectionAlpEncoding}) and emits the width-escape wire form when strictly
   * smaller; otherwise falls through to the plain FOR form byte-identically to before —
   * non-decimal data and pre-ALP stores are unaffected. Deterministic either way, so the
   * descriptor-hash no-op carry-forward stays stable.
   */
  static void encodeForBitPackedDouble(final ByteArrayOutputStream out, final long[] values,
      final int rowCount) {
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
      if (values[i] < min) min = values[i];
      if (values[i] > max) max = values[i];
    }
    final int width = rangeWidth(min, max);
    return 8 + 1 + ((rowCount * width + 7) >>> 3);
  }

  private static void encodeStringDict(final ByteArrayOutputStream out, final byte[][] dict,
      final int[] ids, final int rowCount) {
    encodeDictEntries(out, dict);
    encodeDictIds(out, dict, ids, rowCount);
  }

  /** Number of populated (null-terminated) dictionary slots. Shared dict-size authority. */
  static int dictSizeOf(final byte[][] dict) {
    int dictSize = 0;
    while (dictSize < dict.length && dict[dictSize] != null) {
      dictSize++;
    }
    return dictSize;
  }

  /** Dictionary half of the string-dict wire form: count, lengths, concatenated bytes. */
  static void encodeDictEntries(final ByteArrayOutputStream out, final byte[][] dict) {
    final int dictSize = dictSizeOf(dict);
    putIntLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      putIntLE(out, dict[i].length);
    }
    for (int i = 0; i < dictSize; i++) {
      out.write(dict[i], 0, dict[i].length);
    }
  }

  /** Id-stream half of the string-dict wire form: width byte, packed ids. */
  static void encodeDictIds(final ByteArrayOutputStream out, final byte[][] dict, final int[] ids,
      final int rowCount) {
    final int dictSize = dictSizeOf(dict);
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
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              numericCols[c] = decodeForBitPackedColumn(in, rowCount);
          case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN ->
              booleanCols[c] = decodeBooleanWords(in, presWords);
          case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
            dicts[c] = decodeDictEntries(in);
            dictIdCols[c] = decodePackedIds(in, rowCount);
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    final byte[] columnFlags = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      columnFlags[c] = in.readByte();
    }
    final long[][] presence = new long[columnCount][];
    for (int c = 0; c < columnCount; c++) {
      final long[] bits = new long[Math.max(presWords, (ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6)];
      presence[c] = bits;
      if (rowCount == 0) continue;
      decodePresenceInto(in, bits, presWords, rowCount);
    }
    final ProjectionIndexLeafPage page = ProjectionIndexLeafPage.reconstruct(kinds, rowCount,
        firstRecordKey, lastRecordKey, recordKeys, columnMin, columnMax,
        numericCols, booleanCols, dictIdCols, dicts, presence, columnFlags);
    return page.serialize();
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

  /**
   * Inverse of {@link #encodeForBitPacked}/{@link #encodeForBitPackedDouble}: FOR base +
   * width + packed values, where width byte {@link ProjectionAlpEncoding#WIDTH_ESCAPE_ALP}
   * selects the ALP branch (double columns only ever WRITE it, but decode is safe
   * unconditionally — no other encoder emits it). Width bytes 66..255 remain RESERVED
   * escapes for future numeric encodings — rejecting them loudly keeps those additive
   * (old readers fail attributably instead of misparsing packed bits), with no version
   * machinery.
   */
  static long[] decodeForBitPackedColumn(final Cursor in, final int rowCount) {
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    if (width == ProjectionAlpEncoding.WIDTH_ESCAPE_ALP) {
      return ProjectionAlpEncoding.decode(in, rowCount);
    }
    if (width > 64) {
      throw new IllegalStateException("Reserved numeric-encoding escape " + width
          + " — written by a newer version");
    }
    return unpackFor(in, rowCount, base, width);
  }

  /**
   * Plain-FOR decode with NO escape handling — ALP's digits stream decoder (an escape byte
   * inside an ALP payload is corruption, not nesting).
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
      case 1 -> { /* all-missing — words stay zero */ }
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
  /**
   * Bulk bit-unpacker — the decode hot path (hydrate assembles ~10k packed runs per
   * projection load; the {@link BitReader} per-byte accumulator with its per-byte
   * {@link Cursor} call was the dominant cost). Reads {@code count} {@code width}-bit
   * little-endian values (exactly {@code ceil(count·width / 8)} bytes, byte-identical
   * consumption to {@link BitReader}), adds {@code base}, writes {@code out[0..count)}.
   *
   * <p>Main loop: one unaligned 8-byte window load per value ({@code width + 7 ≤ 64} holds
   * for widths ≤ 57 — wider widths and the last few values whose window would over-read the
   * source array take the scalar accumulator path instead).
   */
  static void unpackInto(final Cursor in, final int count, final int width, final long base,
      final long[] out, final int outOff) {
    in.pos = unpackInto(in.buf, in.pos, count, width, base, out, outOff);
  }

  /**
   * Positional core of {@link #unpackInto(Cursor, int, int, long, long[], int)}: unpack
   * {@code count} {@code width}-bit values starting at BYTE-ALIGNED position {@code pos},
   * returning the byte position after the consumed run. The chunked fold kernels call this
   * per value block — block starts stay byte-aligned because 1024·width bits is a whole
   * number of bytes for every width.
   */
  static int unpackInto(final byte[] src, final int pos, final int count, final int width,
      final long base, final long[] out, final int outOff) {
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

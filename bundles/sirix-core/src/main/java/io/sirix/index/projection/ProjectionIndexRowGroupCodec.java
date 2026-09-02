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

  /**
   * Leading {@code int32} of the order-label lane when the labels are stored as an arithmetic RUN
   * rather than one length-prefixed blob per row. The legacy form's leading {@code int32} is a byte
   * length in {@code [0, MAX_ORDER_LABEL_BYTES]} and every reader has always rejected a negative one,
   * so a negative marker is a free, additive discriminator — no format-version machinery, and a
   * reader that predates a future mode fails attributably instead of misparsing offsets.
   */
  static final int ORDER_LABEL_MARKER_SYNTHESIZED = -1;

  /** Marker for the front-coded fallback; see {@link #ORDER_LABEL_MARKER_SYNTHESIZED}. */
  static final int ORDER_LABEL_MARKER_FRONT_CODED = -2;

  /**
   * Largest trailing field the synthesized mode derives arithmetically. Capped at 7 so a tail value
   * is always {@code < 2^56}: the delta arithmetic, the FOR width and every bound check then stay in
   * signed {@code long} range with no unsigned compare, and {@link #clampPackWidth} never escalates
   * the delta stream to the raw 64-bit path.
   */
  private static final int MAX_SYNTHESIZED_TAIL_BYTES = 7;

  /**
   * Kill switch for the synthesized/front-coded order-label lane (P3):
   * {@code -Dsirix.projection.orderLabels.synthesized=false} restores the legacy
   * {@code length + int32 offsets + bytes} form BYTE-IDENTICALLY. Decoding accepts every mode
   * unconditionally — a kill switch must never make already-written data unreadable.
   *
   * <p>
   * Package-private and mutable for the same reason {@code verifyDirectAssembly} is: the witness has
   * to encode one fixture both ways inside one JVM. The system property remains the production
   * switch.
   */
  static volatile boolean synthesizedOrderLabels =
      !"false".equals(System.getProperty("sirix.projection.orderLabels.synthesized"));

  /**
   * Order-label lane writer. Three interchangeable wire forms, chosen per leaf by ENCODED SIZE:
   *
   * <pre>
   *   LEGACY        int32 byteLength; int32 offsets[rowCount+1]; byte labels[byteLength]
   *   SYNTHESIZED   int32 -1; byte tailLen; byte deltaWidth; int64 deltaBase;
   *                 int32 anchorCount; byte anchorRowWidth;  packed anchorRows[anchorCount-1];
   *                 byte anchorLenWidth; packed anchorLens[anchorCount];
   *                 byte anchorLabels[sum anchorLens]; packed deltas[rowCount-anchorCount]
   *   FRONT_CODED   int32 -2; int32 byteLength; byte prefixWidth; byte suffixWidth; int32 suffixMin;
   *                 packed prefixLens[rowCount]; packed suffixLens[rowCount]; byte suffixes[...]
   * </pre>
   *
   * <p>
   * SYNTHESIZED is the in-order-append shape: a record's Dewey label is its predecessor's with the
   * last division advanced, and the tiered division encoding turns that into "same byte length, same
   * leading bytes, trailing {@code tailLen}-byte big-endian field advanced by a delta". Rows that
   * break the shape (a division crossing a tier boundary, a carried division, a prepend) become
   * ANCHORS carrying their bytes verbatim; every other row is derived, and the per-row {@code int32}
   * offset lane disappears entirely because a run's lengths are constant. With a constant stride the
   * delta stream packs to zero bits and a 1,024-row leaf costs one label plus ~20 bytes.
   */
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
    if (synthesizedOrderLabels && rowCount > 0) {
      final int legacyBytes = Integer.BYTES + (rowCount + 1) * Integer.BYTES + length;
      int bestTailLen = 0;
      int bestBytes = legacyBytes;
      int candidates = synthesizedTailCandidates(bytes, offsets, rowCount);
      while (candidates != 0) {
        final int tailLen = Integer.numberOfTrailingZeros(candidates);
        candidates &= candidates - 1;
        final int candidate = synthesizedOrderLabelBytes(bytes, offsets, rowCount, tailLen);
        if (candidate < bestBytes) {
          bestBytes = candidate;
          bestTailLen = tailLen;
        }
      }
      final int frontCodedBytes = frontCodedOrderLabelBytes(bytes, offsets, rowCount);
      // Smaller wins; ties go to the simpler reader — legacy over both (bestBytes starts at its
      // size and only a STRICTLY smaller candidate displaces it), and the run over front coding,
      // whose random access has to rebuild a label from its predecessor.
      if (bestTailLen != 0 && bestBytes <= frontCodedBytes) {
        encodeSynthesizedOrderLabels(out, bytes, offsets, rowCount, bestTailLen);
        return;
      }
      if (frontCodedBytes < legacyBytes) {
        encodeFrontCodedOrderLabels(out, bytes, offsets, rowCount, length);
        return;
      }
    }
    putIntLE(out, length);
    for (int row = 0; row <= rowCount; row++) {
      putIntLE(out, offsets[row]);
    }
    out.write(bytes, 0, length);
  }

  /**
   * The tail widths worth costing, as a bit set over {@code 1..MAX_SYNTHESIZED_TAIL_BYTES}.
   *
   * <p>
   * A width {@code k} admits exactly the rows whose differing suffix is at most {@code k} bytes, so
   * the admitted set — and with it the encoded size, since a wider tail spans only bytes that are
   * equal and therefore leaves every delta unchanged — only moves at a width some row actually
   * needs. Costing those widths alone is exact, and on real label runs it is one or two probes
   * instead of seven.
   */
  private static int synthesizedTailCandidates(final byte[] bytes, final int[] offsets, final int rowCount) {
    int candidates = 0;
    for (int row = 1; row < rowCount; row++) {
      final int start = offsets[row];
      final int length = offsets[row + 1] - start;
      if (length != start - offsets[row - 1]) {
        continue;
      }
      final int needed = length - sharedPrefixLength(bytes, offsets, row);
      if (needed >= 1 && needed <= MAX_SYNTHESIZED_TAIL_BYTES) {
        candidates |= 1 << needed;
      }
    }
    return candidates;
  }

  /**
   * Whether row {@code row} is derivable from row {@code row - 1} at this tail width: same byte
   * length, at least {@code tailLen} bytes long, and identical outside the trailing field. The one
   * predicate both the sizing pass and the emitting pass consult, so a plan can never disagree with
   * what is written.
   */
  private static boolean isSynthesizedRunRow(final byte[] bytes, final int[] offsets, final int row,
      final int tailLen) {
    final int previousStart = offsets[row - 1];
    final int start = offsets[row];
    final int length = offsets[row + 1] - start;
    return length == start - previousStart && length >= tailLen
        && equalPrefix(bytes, previousStart, start, length - tailLen);
  }

  /** {@code count} bytes from {@code leftOff} equal to {@code count} bytes from {@code rightOff}. */
  private static boolean equalPrefix(final byte[] bytes, final int leftOff, final int rightOff, final int count) {
    return Arrays.equals(bytes, leftOff, leftOff + count, bytes, rightOff, rightOff + count);
  }

  /** Big-endian unsigned value of the {@code count} bytes at {@code off}; {@code count <= 7}. */
  private static long tailValue(final byte[] bytes, final int off, final int count) {
    long value = 0L;
    for (int index = 0; index < count; index++) {
      value = (value << 8) | (bytes[off + index] & 0xFFL);
    }
    return value;
  }

  /** Exact size {@link #encodeSynthesizedOrderLabels} would emit at this tail width. */
  private static int synthesizedOrderLabelBytes(final byte[] bytes, final int[] offsets, final int rowCount,
      final int tailLen) {
    int anchorCount = 1;
    int anchorByteTotal = offsets[1] - offsets[0];
    int maxAnchorRow = 0;
    int maxAnchorLength = anchorByteTotal;
    long minDelta = Long.MAX_VALUE;
    long maxDelta = Long.MIN_VALUE;
    for (int row = 1; row < rowCount; row++) {
      final int start = offsets[row];
      final int end = offsets[row + 1];
      if (isSynthesizedRunRow(bytes, offsets, row, tailLen)) {
        final long delta =
            tailValue(bytes, end - tailLen, tailLen) - tailValue(bytes, start - tailLen, tailLen);
        if (delta < minDelta) {
          minDelta = delta;
        }
        if (delta > maxDelta) {
          maxDelta = delta;
        }
      } else {
        final int length = end - start;
        anchorCount++;
        anchorByteTotal += length;
        maxAnchorRow = row;
        if (length > maxAnchorLength) {
          maxAnchorLength = length;
        }
      }
    }
    final int deltaCount = rowCount - anchorCount;
    final int deltaWidth = deltaCount == 0
        ? 0
        : rangeWidth(minDelta, maxDelta);
    final int anchorRowWidth = anchorCount == 1
        ? 0
        : widthOf(maxAnchorRow);
    final int anchorLenWidth = widthOf(maxAnchorLength);
    return Integer.BYTES + 1 + 1 + Long.BYTES + Integer.BYTES + 1
        + (((anchorCount - 1) * anchorRowWidth + 7) >>> 3) + 1 + ((anchorCount * anchorLenWidth + 7) >>> 3)
        + anchorByteTotal + ((deltaCount * deltaWidth + 7) >>> 3);
  }

  /**
   * Writer for {@link #ORDER_LABEL_MARKER_SYNTHESIZED}; mirrors {@link #synthesizedOrderLabelBytes}.
   *
   * <p>
   * The run/anchor classification is evaluated ONCE into {@code runBits} and then read four times —
   * the four output streams have to agree on it exactly, and re-deciding per stream would both cost
   * four prefix compares per row and open the door to them disagreeing.
   */
  private static void encodeSynthesizedOrderLabels(final ByteArrayOutputStream out, final byte[] bytes,
      final int[] offsets, final int rowCount, final int tailLen) {
    final long[] runBits = new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6];
    int anchorCount = 1;
    int maxAnchorRow = 0;
    int maxAnchorLength = offsets[1] - offsets[0];
    long minDelta = Long.MAX_VALUE;
    long maxDelta = Long.MIN_VALUE;
    for (int row = 1; row < rowCount; row++) {
      final int start = offsets[row];
      final int end = offsets[row + 1];
      if (isSynthesizedRunRow(bytes, offsets, row, tailLen)) {
        runBits[row >>> 6] |= 1L << (row & 63);
        final long delta =
            tailValue(bytes, end - tailLen, tailLen) - tailValue(bytes, start - tailLen, tailLen);
        if (delta < minDelta) {
          minDelta = delta;
        }
        if (delta > maxDelta) {
          maxDelta = delta;
        }
      } else {
        anchorCount++;
        maxAnchorRow = row;
        final int length = end - start;
        if (length > maxAnchorLength) {
          maxAnchorLength = length;
        }
      }
    }
    final int deltaCount = rowCount - anchorCount;
    final int deltaWidth = deltaCount == 0
        ? 0
        : rangeWidth(minDelta, maxDelta);
    final long deltaBase = deltaCount == 0
        ? 0L
        : minDelta;
    final int anchorRowWidth = anchorCount == 1
        ? 0
        : widthOf(maxAnchorRow);
    final int anchorLenWidth = widthOf(maxAnchorLength);

    putIntLE(out, ORDER_LABEL_MARKER_SYNTHESIZED);
    out.write(tailLen);
    out.write(deltaWidth);
    putLongLE(out, deltaBase);
    putIntLE(out, anchorCount);

    out.write(anchorRowWidth);
    final BitWriter anchorRowWriter = new BitWriter(out);
    for (int row = 1; row < rowCount; row++) {
      if (!isRunRow(runBits, row)) {
        anchorRowWriter.write(row, anchorRowWidth);
      }
    }
    anchorRowWriter.flush();

    out.write(anchorLenWidth);
    final BitWriter anchorLenWriter = new BitWriter(out);
    anchorLenWriter.write(offsets[1] - offsets[0], anchorLenWidth);
    for (int row = 1; row < rowCount; row++) {
      if (!isRunRow(runBits, row)) {
        anchorLenWriter.write(offsets[row + 1] - offsets[row], anchorLenWidth);
      }
    }
    anchorLenWriter.flush();

    out.write(bytes, offsets[0], offsets[1] - offsets[0]);
    for (int row = 1; row < rowCount; row++) {
      if (!isRunRow(runBits, row)) {
        out.write(bytes, offsets[row], offsets[row + 1] - offsets[row]);
      }
    }

    final BitWriter deltaWriter = new BitWriter(out);
    for (int row = 1; row < rowCount; row++) {
      if (isRunRow(runBits, row)) {
        final long delta = tailValue(bytes, offsets[row + 1] - tailLen, tailLen)
            - tailValue(bytes, offsets[row] - tailLen, tailLen);
        deltaWriter.write(delta - deltaBase, deltaWidth);
      }
    }
    deltaWriter.flush();
  }

  private static boolean isRunRow(final long[] runBits, final int row) {
    return (runBits[row >>> 6] & (1L << (row & 63))) != 0L;
  }

  /** Bytes row {@code row} shares with row {@code row - 1}; {@code row >= 1}. */
  private static int sharedPrefixLength(final byte[] bytes, final int[] offsets, final int row) {
    final int previousStart = offsets[row - 1];
    final int start = offsets[row];
    final int limit = Math.min(start - previousStart, offsets[row + 1] - start);
    int shared = 0;
    while (shared < limit && bytes[previousStart + shared] == bytes[start + shared]) {
      shared++;
    }
    return shared;
  }

  /** Exact size {@link #encodeFrontCodedOrderLabels} would emit. */
  private static int frontCodedOrderLabelBytes(final byte[] bytes, final int[] offsets, final int rowCount) {
    int maxPrefix = 0;
    int minSuffix = Integer.MAX_VALUE;
    int maxSuffix = 0;
    int suffixTotal = 0;
    for (int row = 0; row < rowCount; row++) {
      final int prefix = row == 0
          ? 0
          : sharedPrefixLength(bytes, offsets, row);
      final int suffix = offsets[row + 1] - offsets[row] - prefix;
      if (prefix > maxPrefix) {
        maxPrefix = prefix;
      }
      if (suffix < minSuffix) {
        minSuffix = suffix;
      }
      if (suffix > maxSuffix) {
        maxSuffix = suffix;
      }
      suffixTotal += suffix;
    }
    final int prefixWidth = widthOf(maxPrefix);
    final int suffixWidth = rangeWidth(minSuffix, maxSuffix);
    return Integer.BYTES + Integer.BYTES + 1 + 1 + Integer.BYTES + ((rowCount * prefixWidth + 7) >>> 3)
        + ((rowCount * suffixWidth + 7) >>> 3) + suffixTotal;
  }

  /** Writer for {@link #ORDER_LABEL_MARKER_FRONT_CODED}; mirrors {@link #frontCodedOrderLabelBytes}. */
  private static void encodeFrontCodedOrderLabels(final ByteArrayOutputStream out, final byte[] bytes,
      final int[] offsets, final int rowCount, final int length) {
    int maxPrefix = 0;
    int minSuffix = Integer.MAX_VALUE;
    int maxSuffix = 0;
    for (int row = 0; row < rowCount; row++) {
      final int prefix = row == 0
          ? 0
          : sharedPrefixLength(bytes, offsets, row);
      final int suffix = offsets[row + 1] - offsets[row] - prefix;
      if (prefix > maxPrefix) {
        maxPrefix = prefix;
      }
      if (suffix < minSuffix) {
        minSuffix = suffix;
      }
      if (suffix > maxSuffix) {
        maxSuffix = suffix;
      }
    }
    final int prefixWidth = widthOf(maxPrefix);
    final int suffixWidth = rangeWidth(minSuffix, maxSuffix);

    putIntLE(out, ORDER_LABEL_MARKER_FRONT_CODED);
    putIntLE(out, length);
    out.write(prefixWidth);
    out.write(suffixWidth);
    putIntLE(out, minSuffix);

    final BitWriter prefixWriter = new BitWriter(out);
    for (int row = 0; row < rowCount; row++) {
      prefixWriter.write(row == 0
          ? 0
          : sharedPrefixLength(bytes, offsets, row), prefixWidth);
    }
    prefixWriter.flush();

    final BitWriter suffixWriter = new BitWriter(out);
    for (int row = 0; row < rowCount; row++) {
      final int prefix = row == 0
          ? 0
          : sharedPrefixLength(bytes, offsets, row);
      suffixWriter.write((long) (offsets[row + 1] - offsets[row] - prefix) - minSuffix, suffixWidth);
    }
    suffixWriter.flush();

    for (int row = 0; row < rowCount; row++) {
      final int prefix = row == 0
          ? 0
          : sharedPrefixLength(bytes, offsets, row);
      out.write(bytes, offsets[row] + prefix, offsets[row + 1] - offsets[row] - prefix);
    }
  }

  static OrderLabels decodeOrderLabels(final Cursor in, final int rowCount) {
    final OrderLabelLane lane = decodeOrderLabelLane(in, rowCount);
    final byte[] bytes = lane.materializeLabelBytes();
    final int[] offsets = new int[ProjectionIndexRowGroupPage.MAX_ROWS + 1];
    lane.copyOffsetsInto(offsets);
    ProjectionIndexRowGroupPage.validateOrderLabels(rowCount, bytes, offsets, lane.totalBytes());
    return new OrderLabels(bytes, offsets);
  }

  record OrderLabels(byte[] bytes, int[] offsets) {
  }

  /**
   * Decode the order-label lane in place, leaving {@code in} positioned immediately after it. Every
   * mode is validated here — offsets/lengths in range, labels strictly increasing — so corruption is
   * caught once, at fill time, for both the hydrate path and {@code decodeKeysView}.
   */
  static OrderLabelLane decodeOrderLabelLane(final Cursor in, final int rowCount) {
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS) {
      throw new IllegalStateException("invalid projection order-label row count " + rowCount);
    }
    final int marker = in.readInt();
    if (marker >= 0) {
      return decodeFlatOrderLabelLane(in, rowCount, marker);
    }
    return switch (marker) {
      case ORDER_LABEL_MARKER_SYNTHESIZED -> decodeSynthesizedOrderLabelLane(in, rowCount);
      case ORDER_LABEL_MARKER_FRONT_CODED -> decodeFrontCodedOrderLabelLane(in, rowCount);
      default -> throw new IllegalStateException(
          "Reserved projection order-label encoding escape " + marker + " — written by a newer version");
    };
  }

  private static OrderLabelLane decodeFlatOrderLabelLane(final Cursor in, final int rowCount, final int byteLength) {
    if (byteLength > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
      throw new IllegalStateException("invalid projection Dewey order-label byte length " + byteLength);
    }
    final int[] offsets = new int[rowCount + 1];
    for (int row = 0; row <= rowCount; row++) {
      offsets[row] = in.readInt();
    }
    final byte[] source = in.buffer();
    final int bytesOffset = in.position();
    in.skip(byteLength);
    return newValidatedFlatLane(byteLength, source, bytesOffset, offsets, byteLength, rowCount);
  }

  private static OrderLabelLane newValidatedFlatLane(final int marker, final byte[] source, final int bytesOffset,
      final int[] offsets, final int byteLength, final int rowCount) {
    if (bytesOffset < 0 || byteLength < 0 || byteLength > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES
        || bytesOffset + byteLength > source.length || offsets.length != rowCount + 1 || offsets[0] != 0
        || offsets[rowCount] != byteLength) {
      throw new IllegalStateException("invalid projection Dewey order-label lane");
    }
    for (int row = 0; row < rowCount; row++) {
      final int start = offsets[row];
      final int end = offsets[row + 1];
      if (start < 0 || end <= start || end > byteLength) {
        throw new IllegalStateException("invalid projection Dewey order-label offset at row " + row);
      }
      if (row > 0 && ProjectionIndexRowGroupPage.compareOrderLabels(source, bytesOffset + offsets[row - 1],
          bytesOffset + start, source, bytesOffset + start, bytesOffset + end) >= 0) {
        throw new IllegalStateException("projection Dewey order labels are not strictly increasing");
      }
    }
    return new FlatOrderLabels(marker, source, bytesOffset, offsets, byteLength);
  }

  private static OrderLabelLane decodeSynthesizedOrderLabelLane(final Cursor in, final int rowCount) {
    final int tailLen = in.readByte() & 0xFF;
    final int deltaWidth = in.readByte() & 0xFF;
    final long deltaBase = in.readLong();
    final int anchorCount = in.readInt();
    if (rowCount == 0 || tailLen < 1 || tailLen > MAX_SYNTHESIZED_TAIL_BYTES || deltaWidth > 56 || anchorCount < 1
        || anchorCount > rowCount) {
      throw new IllegalStateException("invalid synthesized projection order-label header");
    }
    final int deltaCount = rowCount - anchorCount;
    if (deltaCount > 0 && (deltaBase < 1L || deltaBase >= 1L << (tailLen << 3))) {
      throw new IllegalStateException("synthesized projection order-label stride leaves the tail field");
    }

    final int anchorRowWidth = in.readByte() & 0xFF;
    if (anchorRowWidth > widthOf(ProjectionIndexRowGroupPage.MAX_ROWS)) {
      throw new IllegalStateException("invalid synthesized projection order-label anchor-row width " + anchorRowWidth);
    }
    final int[] anchorRows = new int[anchorCount];
    if (anchorCount > 1) {
      final int[] packedRows = new int[anchorCount - 1];
      unpackIntsInto(in, anchorCount - 1, anchorRowWidth, packedRows);
      System.arraycopy(packedRows, 0, anchorRows, 1, anchorCount - 1);
    }
    int previousAnchorRow = 0;
    for (int anchor = 1; anchor < anchorCount; anchor++) {
      if (anchorRows[anchor] <= previousAnchorRow || anchorRows[anchor] >= rowCount) {
        throw new IllegalStateException("invalid synthesized projection order-label anchor row " + anchorRows[anchor]);
      }
      previousAnchorRow = anchorRows[anchor];
    }

    final int anchorLenWidth = in.readByte() & 0xFF;
    final int[] anchorLengths = new int[anchorCount];
    if (anchorLenWidth == 0 || anchorLenWidth > widthOf(ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES)) {
      throw new IllegalStateException("invalid synthesized projection order-label length width " + anchorLenWidth);
    }
    unpackIntsInto(in, anchorCount, anchorLenWidth, anchorLengths);

    final byte[] source = in.buffer();
    final int[] anchorOffsets = new int[anchorCount];
    final long[] anchorTails = new long[anchorCount];
    long anchorByteTotal = 0L;
    for (int anchor = 0; anchor < anchorCount; anchor++) {
      if (anchorLengths[anchor] < 1) {
        throw new IllegalStateException("invalid synthesized projection order-label length at anchor " + anchor);
      }
      anchorByteTotal += anchorLengths[anchor];
    }
    if (anchorByteTotal > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
      throw new IllegalStateException("synthesized projection order-label anchors exceed the bounded lane");
    }
    final int anchorBytesOffset = in.position();
    if (anchorBytesOffset + anchorByteTotal > source.length) {
      throw new IllegalStateException("truncated synthesized projection order-label anchors");
    }
    int anchorCursor = anchorBytesOffset;
    for (int anchor = 0; anchor < anchorCount; anchor++) {
      anchorOffsets[anchor] = anchorCursor;
      anchorTails[anchor] = anchorLengths[anchor] >= tailLen
          ? tailValue(source, anchorCursor + anchorLengths[anchor] - tailLen, tailLen)
          : 0L;
      anchorCursor += anchorLengths[anchor];
    }
    in.skip((int) anchorByteTotal);

    long[] tails = null;
    if (deltaWidth > 0) {
      final long tailLimit = 1L << (tailLen << 3);
      tails = new long[rowCount];
      final long[] deltas = new long[Math.max(deltaCount, 1)];
      unpackInto(in, deltaCount, deltaWidth, deltaBase, deltas, 0);
      int anchor = 0;
      int delta = 0;
      for (int row = 0; row < rowCount; row++) {
        if (anchor < anchorCount && anchorRows[anchor] == row) {
          tails[row] = anchorTails[anchor];
          anchor++;
          continue;
        }
        // deltaBase >= 1 is checked above and an unpacked value of width <= 56 is non-negative, so
        // every step is >= 1 and the run is strictly increasing by construction; only the field
        // bound can still be violated by a corrupt stream.
        final long tail = tails[row - 1] + deltas[delta++];
        if (tail >= tailLimit) {
          throw new IllegalStateException("synthesized projection order-label deltas leave the tail field");
        }
        tails[row] = tail;
      }
    }

    final SynthesizedOrderLabels lane = new SynthesizedOrderLabels(rowCount, tailLen, deltaBase, tails, source,
        anchorRows, anchorOffsets, anchorLengths, anchorTails);
    lane.validate();
    return lane;
  }

  private static OrderLabelLane decodeFrontCodedOrderLabelLane(final Cursor in, final int rowCount) {
    final int byteLength = in.readInt();
    final int prefixWidth = in.readByte() & 0xFF;
    final int suffixWidth = in.readByte() & 0xFF;
    final int suffixMin = in.readInt();
    // A label cannot be longer than the bounded lane, so no length field can be wider than
    // widthOf(MAX_ORDER_LABEL_BYTES). Bounding the widths HERE is what keeps every length below
    // arithmetic that could overflow the cursor checks in the rebuild loop.
    final int maxLengthWidth = widthOf(ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES);
    if (byteLength < 0 || byteLength > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES
        || prefixWidth > maxLengthWidth || suffixWidth > maxLengthWidth || suffixMin < 0
        || suffixMin > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
      throw new IllegalStateException("invalid front-coded projection order-label header");
    }
    final int[] prefixLengths = new int[rowCount];
    final int[] suffixLengths = new int[rowCount];
    unpackIntsInto(in, rowCount, prefixWidth, prefixLengths);
    unpackIntsInto(in, rowCount, suffixWidth, suffixLengths);
    final byte[] source = in.buffer();
    final int suffixOffset = in.position();
    final byte[] labels = new byte[byteLength];
    final int[] offsets = new int[rowCount + 1];
    int labelCursor = 0;
    int suffixCursor = suffixOffset;
    int previousStart = 0;
    int previousLength = 0;
    for (int row = 0; row < rowCount; row++) {
      final int prefix = prefixLengths[row];
      final int suffix = suffixLengths[row] + suffixMin;
      if (prefix > previousLength || suffix < 1 || (row == 0 && prefix != 0)) {
        throw new IllegalStateException("invalid front-coded projection order-label lengths at row " + row);
      }
      final int length = prefix + suffix;
      if (labelCursor + length > byteLength || suffixCursor + suffix > source.length) {
        throw new IllegalStateException("truncated front-coded projection order-label lane");
      }
      offsets[row] = labelCursor;
      System.arraycopy(labels, previousStart, labels, labelCursor, prefix);
      System.arraycopy(source, suffixCursor, labels, labelCursor + prefix, suffix);
      suffixCursor += suffix;
      previousStart = labelCursor;
      previousLength = length;
      labelCursor += length;
    }
    offsets[rowCount] = labelCursor;
    if (labelCursor != byteLength) {
      throw new IllegalStateException("front-coded projection order-label lane length mismatch");
    }
    in.skip(suffixCursor - suffixOffset);
    return newValidatedFlatLane(ORDER_LABEL_MARKER_FRONT_CODED, labels, 0, offsets, byteLength, rowCount);
  }

  /**
   * Read view over one leaf's order labels, independent of which wire form carried them. Compare and
   * copy are the only per-row operations the consumers need
   * ({@code ProjectionIndexFences#compareFirstOrderLabel}, the change listener's binary search); the
   * synthesized implementation answers both from the run description with NO per-row state.
   */
  sealed interface OrderLabelLane permits FlatOrderLabels, SynthesizedOrderLabels {

    /**
     * The wire marker this lane was decoded from: {@code >= 0} for the legacy offset lane, else
     * {@link #ORDER_LABEL_MARKER_SYNTHESIZED} or {@link #ORDER_LABEL_MARKER_FRONT_CODED}. Diagnostics
     * and the encoder's mode witnesses read it; nothing on the serving path branches on it.
     */
    int marker();

    /** Concatenated length of every label — the legacy lane's {@code byteLength}. */
    int totalBytes();

    /** {@code compareOrderLabels(label(row), other)}. */
    int compareAt(int row, byte[] other);

    /** A fresh copy of row {@code row}'s label. */
    byte[] copyAt(int row);

    /** The concatenated labels, exactly {@link #totalBytes()} long. */
    byte[] materializeLabelBytes();

    /** Fills {@code dst[0..rowCount]} with the label offsets. */
    void copyOffsetsInto(int[] dst);
  }

  /** Labels stored one after another with an explicit offset per row (LEGACY and FRONT_CODED). */
  record FlatOrderLabels(int marker, byte[] source, int bytesOffset, int[] offsets, int totalBytes)
      implements OrderLabelLane {

    @Override
    public int compareAt(final int row, final byte[] other) {
      return ProjectionIndexRowGroupPage.compareOrderLabels(source, bytesOffset + offsets[row],
          bytesOffset + offsets[row + 1], other, 0, other.length);
    }

    @Override
    public byte[] copyAt(final int row) {
      return Arrays.copyOfRange(source, bytesOffset + offsets[row], bytesOffset + offsets[row + 1]);
    }

    @Override
    public byte[] materializeLabelBytes() {
      return Arrays.copyOfRange(source, bytesOffset, bytesOffset + totalBytes);
    }

    @Override
    public void copyOffsetsInto(final int[] dst) {
      System.arraycopy(offsets, 0, dst, 0, offsets.length);
    }
  }

  /**
   * Labels described as arithmetic runs over anchor labels. A row that is not an anchor has its
   * anchor's byte length and leading bytes, and a trailing {@code tailLen}-byte big-endian field
   * derived from the anchor's — either {@code anchorTail + (row - anchorRow) * stride} when the
   * strides are uniform (the in-order-append shape, and then this lane holds NO per-row state at all)
   * or the prefix-summed {@code tails} otherwise.
   */
  static final class SynthesizedOrderLabels implements OrderLabelLane {
    private final int rowCount;
    private final int tailLen;
    private final long stride;
    private final long[] tails;
    private final byte[] source;
    private final int[] anchorRows;
    private final int[] anchorOffsets;
    private final int[] anchorLengths;
    private final long[] anchorTails;
    private final int totalBytes;

    SynthesizedOrderLabels(final int rowCount, final int tailLen, final long stride, final long[] tails,
        final byte[] source, final int[] anchorRows, final int[] anchorOffsets, final int[] anchorLengths,
        final long[] anchorTails) {
      this.rowCount = rowCount;
      this.tailLen = tailLen;
      this.stride = stride;
      this.tails = tails;
      this.source = source;
      this.anchorRows = anchorRows;
      this.anchorOffsets = anchorOffsets;
      this.anchorLengths = anchorLengths;
      this.anchorTails = anchorTails;
      long total = 0L;
      for (int anchor = 0; anchor < anchorRows.length; anchor++) {
        final int runEnd = anchor + 1 < anchorRows.length
            ? anchorRows[anchor + 1]
            : rowCount;
        total += (long) anchorLengths[anchor] * (runEnd - anchorRows[anchor]);
      }
      if (total > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
        throw new IllegalStateException("synthesized projection order-label lane exceeds the bounded lane");
      }
      this.totalBytes = (int) total;
    }

    /**
     * Index of the anchor governing {@code row} — the greatest anchor row {@code <= row}. Anchor
     * counts are single digits on a run-shaped leaf, so the binary search settles in 1-2 probes.
     */
    private int anchorOf(final int row) {
      int low = 0;
      int high = anchorRows.length - 1;
      while (low < high) {
        final int middle = (low + high + 1) >>> 1;
        if (anchorRows[middle] <= row) {
          low = middle;
        } else {
          high = middle - 1;
        }
      }
      return low;
    }

    /** Trailing field value of {@code row}. */
    private long tailAt(final int row, final int anchor) {
      return tails != null
          ? tails[row]
          : anchorTails[anchor] + (long) (row - anchorRows[anchor]) * stride;
    }

    @Override
    public int marker() {
      return ORDER_LABEL_MARKER_SYNTHESIZED;
    }

    @Override
    public int totalBytes() {
      return totalBytes;
    }

    @Override
    public int compareAt(final int row, final byte[] other) {
      return compareToRange(row, other, 0, other.length);
    }

    /** {@code compareOrderLabels(label(row), other[from, to))} without materialising the label. */
    int compareToRange(final int row, final byte[] other, final int from, final int to) {
      final int anchor = anchorOf(row);
      final int length = anchorLengths[anchor];
      final int offset = anchorOffsets[anchor];
      final int otherLength = to - from;
      if (anchorRows[anchor] == row) {
        return ProjectionIndexRowGroupPage.compareOrderLabels(source, offset, offset + length, other, from, to);
      }
      final int common = Math.min(length, otherLength);
      final int prefixLength = length - tailLen;
      int index = 0;
      while (index < common && index < prefixLength) {
        final int left = source[offset + index] & 0xFF;
        final int right = other[from + index] & 0xFF;
        if (left != right) {
          return left - right;
        }
        index++;
      }
      if (index < common) {
        final long tail = tailAt(row, anchor);
        while (index < common) {
          final int left = (int) ((tail >>> ((length - 1 - index) << 3)) & 0xFFL);
          final int right = other[from + index] & 0xFF;
          if (left != right) {
            return left - right;
          }
          index++;
        }
      }
      return Integer.compare(length, otherLength);
    }

    @Override
    public byte[] copyAt(final int row) {
      final int anchor = anchorOf(row);
      final int length = anchorLengths[anchor];
      final byte[] label = new byte[length];
      writeLabel(row, anchor, label, 0);
      return label;
    }

    /** Writes row {@code row}'s label into {@code dst} at {@code dstOff}. */
    private void writeLabel(final int row, final int anchor, final byte[] dst, final int dstOff) {
      final int length = anchorLengths[anchor];
      final int offset = anchorOffsets[anchor];
      if (anchorRows[anchor] == row) {
        System.arraycopy(source, offset, dst, dstOff, length);
        return;
      }
      final int prefixLength = length - tailLen;
      System.arraycopy(source, offset, dst, dstOff, prefixLength);
      final long tail = tailAt(row, anchor);
      for (int index = 0; index < tailLen; index++) {
        dst[dstOff + prefixLength + index] = (byte) (tail >>> ((tailLen - 1 - index) << 3));
      }
    }

    @Override
    public byte[] materializeLabelBytes() {
      final byte[] labels = new byte[totalBytes];
      int cursor = 0;
      int anchor = 0;
      for (int row = 0; row < rowCount; row++) {
        if (anchor + 1 < anchorRows.length && anchorRows[anchor + 1] == row) {
          anchor++;
        }
        writeLabel(row, anchor, labels, cursor);
        cursor += anchorLengths[anchor];
      }
      return labels;
    }

    @Override
    public void copyOffsetsInto(final int[] dst) {
      int cursor = 0;
      int anchor = 0;
      for (int row = 0; row < rowCount; row++) {
        if (anchor + 1 < anchorRows.length && anchorRows[anchor + 1] == row) {
          anchor++;
        }
        dst[row] = cursor;
        cursor += anchorLengths[anchor];
      }
      dst[rowCount] = cursor;
    }

    /**
     * Every guard the flat lane gets, answered from the run description: a derived row must have a
     * tail that fits its field, and every row must be strictly greater than its predecessor. Uniform
     * strides make this O(anchors) — one bound check per run instead of one per row.
     */
    void validate() {
      final long tailLimit = 1L << (tailLen << 3);
      for (int anchor = 0; anchor < anchorRows.length; anchor++) {
        final int anchorRow = anchorRows[anchor];
        final int runEnd = anchor + 1 < anchorRows.length
            ? anchorRows[anchor + 1]
            : rowCount;
        final int span = runEnd - 1 - anchorRow;
        if (span > 0) {
          if (anchorLengths[anchor] < tailLen) {
            throw new IllegalStateException("synthesized projection order-label run is shorter than its tail field");
          }
          // Bound the LAST tail without ever forming the product: an overflowing multiply would wrap
          // past the limit check instead of failing it.
          if (tails == null
              ? stride > (tailLimit - 1L - anchorTails[anchor]) / span
              : tails[runEnd - 1] >= tailLimit) {
            throw new IllegalStateException("synthesized projection order-label run overflows its tail field");
          }
        }
        if (anchorRow > 0) {
          final int offset = anchorOffsets[anchor];
          if (compareToRange(anchorRow - 1, source, offset, offset + anchorLengths[anchor]) >= 0) {
            throw new IllegalStateException("projection Dewey order labels are not strictly increasing");
          }
        }
      }
    }
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
    final long[] values = new long[rowCount];
    decodeForBitPackedColumnInto(in, rowCount, values);
    return values;
  }

  /**
   * {@link #decodeForBitPackedColumn} into a caller-owned array of length {@code rowCount} — the
   * windowed accesses decode a leaf per column per pass and hand the array back on eviction, so the
   * decoder must write EVERY element (FOR width 0 fills the base, ALP overwrites every cell) and may
   * assume nothing about the array's prior contents.
   */
  static void decodeForBitPackedColumnInto(final Cursor in, final int rowCount, final long[] out) {
    if (out.length != rowCount) {
      throw new IllegalArgumentException("values array of " + out.length + " for rowCount " + rowCount);
    }
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    if (width == ProjectionAlpEncoding.WIDTH_ESCAPE_ALP) {
      ProjectionAlpEncoding.decodeInto(in, rowCount, out);
      return;
    }
    if (width > 64) {
      throw new IllegalStateException("Reserved numeric-encoding escape " + width + " — written by a newer version");
    }
    unpackInto(in, rowCount, width, base, out, 0);
  }

  /**
   * Plain-FOR decode with NO escape handling — ALP's digits stream decoder (an escape byte inside an
   * ALP payload is corruption, not nesting).
   */
  static long[] decodePlainForBitPacked(final Cursor in, final int rowCount) {
    final long[] values = new long[rowCount];
    decodePlainForBitPackedInto(in, rowCount, values);
    return values;
  }

  /** {@link #decodePlainForBitPacked} into a caller-owned array of length {@code rowCount}. */
  static void decodePlainForBitPackedInto(final Cursor in, final int rowCount, final long[] out) {
    final long base = in.readLong();
    final int width = in.readByte() & 0xFF;
    if (width > 64) {
      throw new IllegalStateException("Corrupt nested numeric-encoding escape " + width);
    }
    unpackInto(in, rowCount, width, base, out, 0);
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
        // all-missing: a reused (recycled) array carries the previous leaf's words, so zero explicitly
        Arrays.fill(bits, 0, presWords, 0L);
      }
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

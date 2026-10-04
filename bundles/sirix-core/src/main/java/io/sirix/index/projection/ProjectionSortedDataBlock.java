/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Self-contained, byte-addressable columns for keys ending in (present long, record id). The
 * leading encoded fields form an ordered dictionary. Numeric columns use a minimum and unsigned,
 * fixed-byte-width deltas (width zero is a single value). No bit packing or block decompression is
 * involved in a read. Arbitrary key shapes retain the prefix-row scheme.
 *
 * <p>
 * Wire: PSL1, scheme=2, rows:u16, dictionarySize:u16, payloadWords:u8, valueWidth:u8,
 * recordWidth:u8, valueMin:i64, valueMax:i64, recordMin:u64; then (width:u8,min:u64) per payload
 * word, dictionary offsets:u16[size+1], dictionary bytes, optional dictionary ids:u8[rows], value
 * deltas, record deltas, and payload-word delta columns. Numeric metadata and deltas are little
 * endian. Dictionary bytes retain the exact ordered key representation. The min/max header is the
 * chunk's SMA; its sorted value column permits exact positional narrowing by binary search, without
 * a separate PSMA table larger than the chunk itself.
 * </p>
 */
final class ProjectionSortedDataBlock {
  static final byte SCHEME = 2;
  private static final int HEADER = 36;
  private static final ThreadLocal<int[]> DICTIONARY_ROWS =
      ThreadLocal.withInitial(() -> new int[ProjectionSortedLeaf.MAX_ROWS]);
  private final byte[] data;
  private final int count;
  private final int dictionaries;
  private final int words;
  private final int valueWidth;
  private final int recordWidth;
  private final long minimum;
  private final long maximum;
  private final long recordBase;
  private final int dictionaryOffsets;
  private final int dictionaryData;
  private final int ids;
  private final int values;
  private final int records;
  private final int[] payloadOffsets;

  private ProjectionSortedDataBlock(final byte[] data) {
    this.data = data;
    count = u16(data, 5);
    dictionaries = u16(data, 7);
    words = data[9] & 255;
    valueWidth = data[10] & 255;
    recordWidth = data[11] & 255;
    minimum = le(data, 12, 8);
    maximum = le(data, 20, 8);
    recordBase = le(data, 28, 8);
    dictionaryOffsets = HEADER + words * 9;
    dictionaryData = dictionaryOffsets + (dictionaries + 1) * 2;
    ids = dictionaryData + u16(data, dictionaryOffsets + dictionaries * 2);
    values = ids + (dictionaries == 1
        ? 0
        : count);
    records = values + count * valueWidth;
    payloadOffsets = new int[words];
    int at = records + count * recordWidth;
    for (int word = 0; word < words; word++) {
      payloadOffsets[word] = at;
      at += count * (data[HEADER + word * 9] & 255);
    }
  }

  static @Nullable ProjectionSortedDataBlock encode(final byte[][] keys, final byte @Nullable [][] payloads,
      final int from, final int count) {
    // Summaries and directory rows usually have no trailing long/record pair. Reject their
    // shape before allocating column statistics or dictionary staging.
    final byte[] firstKey = keys[from];
    final int firstEnd = firstKey.length - 17;
    if (firstEnd < 0 || firstKey[firstEnd] != ProjectionSortKeyCodec.PRESENT)
      return null;
    final int payloadLength = payloads == null
        ? 0
        : payloads[from].length;
    if (payloadLength > 32 || payloadLength % 8 != 0)
      return null;
    final int words = payloadLength / 8;
    final int[] dictionaryRows = DICTIONARY_ROWS.get();
    final long[] bases = new long[words];
    final long[] maxima = new long[words];
    Arrays.fill(bases, -1L);
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long recordMin = -1L;
    long recordMax = 0;
    int dictionaryCount = 0;
    int dictionaryBytes = 0;
    for (int row = 0; row < count; row++) {
      final byte[] key = keys[from + row];
      final int end = key.length - 17;
      if (end < 0 || key[end] != ProjectionSortKeyCodec.PRESENT
          || payloads != null && payloads[from + row].length != payloadLength)
        return null;
      boolean equal = false;
      if (dictionaryCount > 0) {
        final byte[] previous = keys[from + dictionaryRows[dictionaryCount - 1]];
        final int previousEnd = previous.length - 17;
        final int comparison = Arrays.compareUnsigned(previous, 0, previousEnd, key, 0, end);
        equal = comparison == 0;
        // Prefix-overlapping dictionary strings need the general row representation.
        if (!equal && (comparison > 0
            || Arrays.equals(previous, 0, Math.min(previousEnd, end), key, 0, Math.min(previousEnd, end))))
          return null;
      }
      if (!equal) {
        dictionaryRows[dictionaryCount++] = row;
        dictionaryBytes += end;
      }
      final long value = be(key, end + 1) ^ Long.MIN_VALUE;
      final long record = be(key, end + 9);
      min = Math.min(min, value);
      max = Math.max(max, value);
      if (Long.compareUnsigned(record, recordMin) < 0)
        recordMin = record;
      if (Long.compareUnsigned(record, recordMax) > 0)
        recordMax = record;
      for (int word = 0; word < words; word++) {
        final long v = le(payloads[from + row], word * 8, 8);
        if (Long.compareUnsigned(v, bases[word]) < 0)
          bases[word] = v;
        if (Long.compareUnsigned(v, maxima[word]) > 0)
          maxima[word] = v;
      }
    }
    if (dictionaryBytes > 65535)
      return null;
    final int vw = width(max - min);
    final int rw = width(recordMax - recordMin);
    int length = HEADER + words * 9 + (dictionaryCount + 1) * 2 + dictionaryBytes
        + count * (vw + rw + (dictionaryCount == 1
            ? 0
            : 1));
    for (int word = 0; word < words; word++)
      length += count * width(maxima[word] - bases[word]);
    if (length > ProjectionSortedLeaf.MAX_BYTES)
      return null;
    final byte[] bytes = new byte[length];
    put(bytes, 0, 0x314C5350, 4);
    bytes[4] = SCHEME;
    put(bytes, 5, count, 2);
    put(bytes, 7, dictionaryCount, 2);
    bytes[9] = (byte) words;
    bytes[10] = (byte) vw;
    bytes[11] = (byte) rw;
    put(bytes, 12, min, 8);
    put(bytes, 20, max, 8);
    put(bytes, 28, recordMin, 8);
    for (int word = 0; word < words; word++) {
      bytes[HEADER + word * 9] = (byte) width(maxima[word] - bases[word]);
      put(bytes, HEADER + word * 9 + 1, bases[word], 8);
    }
    final int offsets = HEADER + words * 9;
    final int start = offsets + (dictionaryCount + 1) * 2;
    int at = start;
    for (int entry = 0; entry < dictionaryCount; entry++) {
      put(bytes, offsets + entry * 2, at - start, 2);
      final byte[] key = keys[from + dictionaryRows[entry]];
      System.arraycopy(key, 0, bytes, at, key.length - 17);
      at += key.length - 17;
    }
    put(bytes, offsets + dictionaryCount * 2, at - start, 2);
    final ProjectionSortedDataBlock block = new ProjectionSortedDataBlock(bytes);
    int dictionary = 0;
    for (int row = 0; row < count; row++) {
      final byte[] key = keys[from + row];
      if (dictionaryCount != 1) {
        if (dictionary + 1 < dictionaryCount && dictionaryRows[dictionary + 1] == row)
          dictionary++;
        bytes[block.ids + row] = (byte) dictionary;
      }
      put(bytes, block.values + row * vw, (be(key, key.length - 16) ^ Long.MIN_VALUE) - min, vw);
      put(bytes, block.records + row * rw, be(key, key.length - 8) - recordMin, rw);
      for (int word = 0; word < words; word++) {
        final int w = bytes[HEADER + word * 9] & 255;
        put(bytes, block.payloadOffsets[word] + row * w, le(payloads[from + row], word * 8, 8) - bases[word], w);
      }
    }
    return block;
  }

  static ProjectionSortedDataBlock open(final byte[] bytes) {
    if (bytes.length < HEADER + 4 || bytes.length > ProjectionSortedLeaf.MAX_BYTES)
      throw invalid();
    final int rows = u16(bytes, 5);
    final int dictionaryCount = u16(bytes, 7);
    final int words = bytes[9] & 255;
    if (rows < 1 || rows > ProjectionSortedLeaf.MAX_ROWS || dictionaryCount < 1 || dictionaryCount > rows || words > 4
        || (bytes[10] & 255) > 8 || (bytes[11] & 255) > 8
        || HEADER + words * 9 + (dictionaryCount + 1) * 2 > bytes.length)
      throw invalid();
    final int offsets = HEADER + words * 9;
    final int start = offsets + (dictionaryCount + 1) * 2;
    if (u16(bytes, offsets) != 0)
      throw invalid();
    int priorEnd = 0;
    for (int entry = 0; entry < dictionaryCount; entry++) {
      final int end = u16(bytes, offsets + (entry + 1) * 2);
      if (end < priorEnd || start + end > bytes.length)
        throw invalid();
      if (entry > 0) {
        final int previousStart = u16(bytes, offsets + (entry - 1) * 2);
        if (Arrays.compareUnsigned(bytes, start + previousStart, start + priorEnd, bytes, start + priorEnd,
            start + end) >= 0
            || Arrays.equals(bytes, start + previousStart,
                start + previousStart + Math.min(priorEnd - previousStart, end - priorEnd), bytes, start + priorEnd,
                start + priorEnd + Math.min(priorEnd - previousStart, end - priorEnd)))
          throw invalid();
      }
      priorEnd = end;
    }
    int length = start + priorEnd + rows * ((bytes[10] & 255) + (bytes[11] & 255) + (dictionaryCount == 1
        ? 0
        : 1));
    for (int word = 0; word < words; word++) {
      final int w = bytes[HEADER + word * 9] & 255;
      if (w > 8)
        throw invalid();
      length += rows * w;
    }
    if (length != bytes.length)
      throw invalid();
    final ProjectionSortedDataBlock block = new ProjectionSortedDataBlock(bytes);
    if (block.minimum > block.maximum)
      throw invalid();
    int previousId = -1;
    long previousValue = 0;
    long previousRecord = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int row = 0; row < rows; row++) {
      final int id = block.dictionaryId(row);
      final long value = block.value(row);
      final long record = block.record(row);
      if (id >= dictionaryCount || id < previousId || value < block.minimum || value > block.maximum || id == previousId
          && (value < previousValue || value == previousValue && Long.compareUnsigned(record, previousRecord) <= 0))
        throw invalid();
      min = Math.min(min, value);
      max = Math.max(max, value);
      previousId = id;
      previousValue = value;
      previousRecord = record;
    }
    if (min != block.minimum || max != block.maximum)
      throw invalid();
    return block;
  }

  byte[] bytes() {
    return data;
  }

  int rows() {
    return count;
  }

  boolean hasSingleDictionaryEntry() {
    return dictionaries == 1;
  }

  long minimum() {
    return minimum;
  }

  long maximum() {
    return maximum;
  }

  /** Size and encode a build slice with one statistics pass and unchanged canonical boundaries. */
  ProjectionSortedDataBlock sliceFitting(final int from, final int byteLimit) {
    Objects.checkIndex(from, count);
    if (dictionaries != 1 || byteLimit < 1 || byteLimit > ProjectionSortedLeaf.MAX_BYTES) {
      throw new IllegalArgumentException("single dictionary and valid byte limit required");
    }
    final SliceStats stats = new SliceStats(from);
    final long[] nextBases = new long[words];
    final long[] nextMaxima = new long[words];
    for (int row = from; row < count && stats.appendIfFits(row, byteLimit, nextBases, nextMaxima); row++) {
      // Statistics are published only for accepted rows; a rejected width jump cannot affect bytes.
    }
    if (stats.rows == 0) {
      throw new IllegalArgumentException("sorted projection row exceeds a bounded page");
    }
    return slice(from, stats.rows, stats);
  }

  /** Rebase a selected range's fixed-width columns; source bytes remain immutable. */
  ProjectionSortedDataBlock slice(final int from, final int rows) {
    Objects.checkFromIndexSize(from, rows, count);
    if (dictionaries != 1 || rows == 0)
      throw new IllegalArgumentException("nonempty single dictionary required");
    final SliceStats stats = new SliceStats(from);
    for (int row = from; row < from + rows; row++)
      stats.append(row);
    return slice(from, rows, stats);
  }

  private ProjectionSortedDataBlock slice(final int from, final int rows, final SliceStats stats) {
    final int length = stats.columnBytes();
    if (length > ProjectionSortedLeaf.MAX_BYTES)
      throw new IllegalArgumentException("slice exceeds leaf bound");
    final byte[] bytes = new byte[length];
    put(bytes, 0, 0x314C5350, 4);
    bytes[4] = SCHEME;
    put(bytes, 5, rows, 2);
    put(bytes, 7, 1, 2);
    bytes[9] = (byte) words;
    bytes[10] = (byte) width(stats.lastValue - stats.firstValue);
    bytes[11] = (byte) width(stats.recordMaximum - stats.recordMinimum);
    put(bytes, 12, stats.firstValue, 8);
    put(bytes, 20, stats.lastValue, 8);
    put(bytes, 28, stats.recordMinimum, 8);
    for (int word = 0; word < words; word++) {
      bytes[HEADER + word * 9] = (byte) width(stats.maxima[word] - stats.bases[word]);
      put(bytes, HEADER + word * 9 + 1, stats.bases[word], 8);
    }
    final int offsets = HEADER + words * 9;
    put(bytes, offsets + 2, stats.prefixLength, 2);
    System.arraycopy(data, prefixStart(from), bytes, offsets + 4, stats.prefixLength);
    final ProjectionSortedDataBlock result = new ProjectionSortedDataBlock(bytes);
    copyColumn(data, values, valueWidth, minimum, from, rows, bytes, result.values, result.valueWidth,
        stats.firstValue);
    copyColumn(data, records, recordWidth, recordBase, from, rows, bytes, result.records, result.recordWidth,
        stats.recordMinimum);
    for (int word = 0; word < words; word++) {
      final int metadata = HEADER + word * 9;
      copyColumn(data, payloadOffsets[word], data[metadata] & 255, le(data, metadata + 1, Long.BYTES), from, rows,
          bytes, result.payloadOffsets[word], bytes[metadata] & 255, stats.bases[word]);
    }
    return result;
  }

  int prefixBytes(final int from, final int rows) {
    final int prefix = prefixEnd(from) - prefixStart(from);
    int common = prefix + 1 + Long.numberOfLeadingZeros(value(from) ^ value(from + rows - 1)) / 8;
    if (common == prefix + 9)
      common += Long.numberOfLeadingZeros(record(from) ^ record(from + rows - 1)) / 8;
    return 9 + common + (rows + 1) * 4 + rows * (4 + prefix + 17 - common + words * 8);
  }

  /**
   * Rewrite a single-dictionary chunk directly from its columns. A changed dictionary, payload shape,
   * or a smaller prefix representation falls back to the general row encoder. The choice and bytes
   * are identical to that encoder; no neighboring chunk participates in an edit.
   */
  @Nullable
  ProjectionSortedDataBlock rewrite(final int position, final byte[] key, final byte[] payload, final int delta) {
    if (dictionaries != 1)
      return null;
    final int prefix = prefixEnd(0) - prefixStart(0);
    if (delta >= 0 && (key.length != prefix + 17 || key[prefix] != ProjectionSortKeyCodec.PRESENT
        || payload.length != words * Long.BYTES || !Arrays.equals(data, prefixStart(0), prefixEnd(0), key, 0, prefix)))
      return null;
    final int rows = count + delta;
    final long changedValue = delta < 0
        ? 0
        : be(key, prefix + 1) ^ Long.MIN_VALUE;
    final long changedRecord = delta < 0
        ? 0
        : be(key, prefix + 9);
    final long min = position == 0
        ? delta < 0
            ? value(1)
            : changedValue
        : value(0);
    final long max = position == rows - 1 && delta >= 0
        ? changedValue
        : value(delta < 0 && position == count - 1
            ? count - 2
            : count - 1);
    final long firstRecord = position == 0
        ? delta < 0
            ? record(1)
            : changedRecord
        : record(0);
    final long lastRecord = position == rows - 1 && delta >= 0
        ? changedRecord
        : record(delta < 0 && position == count - 1
            ? count - 2
            : count - 1);
    int common = prefix + 1 + Long.numberOfLeadingZeros(min ^ max) / 8;
    if (common == prefix + 9)
      common += Long.numberOfLeadingZeros(firstRecord ^ lastRecord) / 8;
    final int prefixBytes = 9 + common + (rows + 1) * 4 + rows * (4 + prefix + 17 - common + words * 8);
    if (prefixBytes > ProjectionSortedLeaf.MAX_BYTES)
      return null;

    long recordMin = -1L;
    long recordMax = 0;
    final long[] bases = new long[words];
    final long[] maxima = new long[words];
    Arrays.fill(bases, -1L);
    for (int row = 0; row < rows; row++) {
      final boolean changed = delta >= 0 && row == position;
      final int source = row < position
          ? row
          : row - delta;
      final long record = changed
          ? changedRecord
          : record(source);
      if (Long.compareUnsigned(record, recordMin) < 0)
        recordMin = record;
      if (Long.compareUnsigned(record, recordMax) > 0)
        recordMax = record;
      for (int word = 0; word < words; word++) {
        final long value = changed
            ? le(payload, word * Long.BYTES, Long.BYTES)
            : payloadWord(source, word);
        if (Long.compareUnsigned(value, bases[word]) < 0)
          bases[word] = value;
        if (Long.compareUnsigned(value, maxima[word]) > 0)
          maxima[word] = value;
      }
    }
    final int vw = width(max - min);
    final int rw = width(recordMax - recordMin);
    int length = HEADER + words * 9 + 4 + prefix + rows * (vw + rw);
    for (int word = 0; word < words; word++)
      length += rows * width(maxima[word] - bases[word]);
    if (length >= prefixBytes)
      return null;

    final byte[] bytes = new byte[length];
    put(bytes, 0, 0x314C5350, 4);
    bytes[4] = SCHEME;
    put(bytes, 5, rows, 2);
    put(bytes, 7, 1, 2);
    bytes[9] = (byte) words;
    bytes[10] = (byte) vw;
    bytes[11] = (byte) rw;
    put(bytes, 12, min, 8);
    put(bytes, 20, max, 8);
    put(bytes, 28, recordMin, 8);
    for (int word = 0; word < words; word++) {
      bytes[HEADER + word * 9] = (byte) width(maxima[word] - bases[word]);
      put(bytes, HEADER + word * 9 + 1, bases[word], 8);
    }
    final int offsets = HEADER + words * 9;
    put(bytes, offsets + 2, prefix, 2);
    System.arraycopy(data, prefixStart(0), bytes, offsets + 4, prefix);
    final ProjectionSortedDataBlock result = new ProjectionSortedDataBlock(bytes);
    copyEditedColumn(values, valueWidth, minimum, bytes, result.values, vw, min, position, delta, changedValue);
    copyEditedColumn(records, recordWidth, recordBase, bytes, result.records, rw, recordMin, position, delta,
        changedRecord);
    for (int word = 0; word < words; word++) {
      final int metadata = HEADER + word * 9;
      final long changed = delta < 0
          ? 0
          : le(payload, word * Long.BYTES, Long.BYTES);
      copyEditedColumn(payloadOffsets[word], data[metadata] & 255, le(data, metadata + 1, Long.BYTES), bytes,
          result.payloadOffsets[word], bytes[metadata] & 255, bases[word], position, delta, changed);
    }
    return result;
  }

  /** Copy the unchanged runs around one edit, rebasing only when the canonical column requires it. */
  private void copyEditedColumn(final int sourceOffset, final int sourceWidth, final long sourceBase,
      final byte[] target, final int targetOffset, final int targetWidth, final long targetBase, final int position,
      final int delta, final long changedValue) {
    if (targetWidth == 0)
      return;
    copyColumn(data, sourceOffset, sourceWidth, sourceBase, 0, position, target, targetOffset, targetWidth, targetBase);
    final int sourceAfter = position + (delta > 0
        ? 0
        : 1);
    final int targetAfter = position + (delta < 0
        ? 0
        : 1);
    if (delta >= 0)
      put(target, targetOffset + position * targetWidth, changedValue - targetBase, targetWidth);
    copyColumn(data, sourceOffset, sourceWidth, sourceBase, sourceAfter, count - sourceAfter, target,
        targetOffset + targetAfter * targetWidth, targetWidth, targetBase);
  }

  /**
   * Fixed-width columns are contiguous. Matching bases and widths permit an exact byte copy;
   * otherwise subtract the base difference once per value, with the same unsigned wrap as encoding.
   * Zero-width columns are represented entirely by their already-written header.
   */
  private static void copyColumn(final byte[] source, final int sourceOffset, final int sourceWidth,
      final long sourceBase, final int from, final int rows, final byte[] target, final int targetOffset,
      final int targetWidth, final long targetBase) {
    if (rows == 0 || targetWidth == 0)
      return;
    int sourceAt = sourceOffset + from * sourceWidth;
    if (sourceWidth == targetWidth && sourceBase == targetBase) {
      System.arraycopy(source, sourceAt, target, targetOffset, rows * targetWidth);
      return;
    }
    final long baseDelta = sourceBase - targetBase;
    int targetAt = targetOffset;
    for (int row = 0; row < rows; row++) {
      put(target, targetAt, le(source, sourceAt, sourceWidth) + baseDelta, targetWidth);
      sourceAt += sourceWidth;
      targetAt += targetWidth;
    }
  }

  private final class SliceStats {
    private final int from;
    private final int prefixLength;
    private final long firstValue;
    private final long[] bases = new long[words];
    private final long[] maxima = new long[words];
    private int rows;
    private long lastValue;
    private long recordMinimum = -1L;
    private long recordMaximum;

    SliceStats(final int from) {
      this.from = from;
      prefixLength = prefixEnd(from) - prefixStart(from);
      firstValue = value(from);
      Arrays.fill(bases, -1L);
    }

    void append(final int row) {
      rows++;
      lastValue = value(row);
      final long record = record(row);
      if (Long.compareUnsigned(record, recordMinimum) < 0)
        recordMinimum = record;
      if (Long.compareUnsigned(record, recordMaximum) > 0)
        recordMaximum = record;
      for (int word = 0; word < words; word++) {
        final long v = payloadWord(row, word);
        if (Long.compareUnsigned(v, bases[word]) < 0)
          bases[word] = v;
        if (Long.compareUnsigned(v, maxima[word]) > 0)
          maxima[word] = v;
      }
    }

    boolean appendIfFits(final int row, final int byteLimit, final long[] nextBases, final long[] nextMaxima) {
      final int nextRows = rows + 1;
      final long nextValue = value(row);
      final long nextRecord = record(row);
      final long recordMin = Long.compareUnsigned(nextRecord, recordMinimum) < 0
          ? nextRecord
          : recordMinimum;
      final long recordMax = Long.compareUnsigned(nextRecord, recordMaximum) > 0
          ? nextRecord
          : recordMaximum;
      int widths = width(nextValue - firstValue) + width(recordMax - recordMin);
      for (int word = 0; word < words; word++) {
        final long v = payloadWord(row, word);
        nextBases[word] = Long.compareUnsigned(v, bases[word]) < 0
            ? v
            : bases[word];
        nextMaxima[word] = Long.compareUnsigned(v, maxima[word]) > 0
            ? v
            : maxima[word];
        widths += width(nextMaxima[word] - nextBases[word]);
      }
      final int columnBytes = HEADER + words * 9 + 4 + prefixLength + nextRows * widths;
      int common = prefixLength + 1 + Long.numberOfLeadingZeros(firstValue ^ nextValue) / 8;
      if (common == prefixLength + 9) {
        common += Long.numberOfLeadingZeros(record(from) ^ nextRecord) / 8;
      }
      final int prefixBytes = 9 + common + (nextRows + 1) * 4 + nextRows * (4 + prefixLength + 17 - common + words * 8);
      if (prefixBytes > ProjectionSortedLeaf.MAX_BYTES || rows != 0 && Math.min(prefixBytes, columnBytes) > byteLimit) {
        return false;
      }
      rows = nextRows;
      lastValue = nextValue;
      recordMinimum = recordMin;
      recordMaximum = recordMax;
      System.arraycopy(nextBases, 0, bases, 0, words);
      System.arraycopy(nextMaxima, 0, maxima, 0, words);
      return true;
    }

    int columnBytes() {
      int widths = width(lastValue - firstValue) + width(recordMaximum - recordMinimum);
      for (int word = 0; word < words; word++)
        widths += width(maxima[word] - bases[word]);
      return HEADER + words * 9 + 4 + prefixLength + rows * widths;
    }
  }

  private int dictionaryId(final int row) {
    return dictionaries == 1
        ? 0
        : data[ids + row] & 255;
  }

  private int prefixStart(final int row) {
    return dictionaryData + u16(data, dictionaryOffsets + dictionaryId(row) * 2);
  }

  private int prefixEnd(final int row) {
    return dictionaryData + u16(data, dictionaryOffsets + (dictionaryId(row) + 1) * 2);
  }

  private long value(final int row) {
    return minimum + le(data, values + row * valueWidth, valueWidth);
  }

  private long record(final int row) {
    return recordBase + le(data, records + row * recordWidth, recordWidth);
  }

  int keyLength(final int row) {
    Objects.checkIndex(row, count);
    return prefixEnd(row) - prefixStart(row) + 17;
  }

  void copyKey(final int row, final byte[] target) {
    final int size = keyLength(row);
    Objects.checkFromIndexSize(0, size, target.length);
    final int prefix = size - 17;
    System.arraycopy(data, prefixStart(row), target, 0, prefix);
    target[prefix] = ProjectionSortKeyCodec.PRESENT;
    putBe(target, prefix + 1, value(row) ^ Long.MIN_VALUE);
    putBe(target, prefix + 9, record(row));
  }

  int compareKey(final int row, final byte[] key) {
    Objects.checkIndex(row, count);
    final int start = prefixStart(row);
    final int end = prefixEnd(row);
    final int prefix = end - start;
    final int common = Math.min(prefix, key.length);
    final int comparison = Arrays.compareUnsigned(data, start, start + common, key, 0, common);
    if (comparison != 0)
      return comparison;
    if (key.length <= prefix)
      return 1;
    final int marker = 1 - (key[prefix] & 255);
    if (marker != 0)
      return marker;
    final long value = value(row) ^ Long.MIN_VALUE;
    final long record = record(row);
    final int tail = Math.min(16, key.length - prefix - 1);
    if (tail == 16) {
      final int valueCompare = Long.compareUnsigned(value, be(key, prefix + 1));
      if (valueCompare != 0)
        return valueCompare;
      final int recordCompare = Long.compareUnsigned(record, be(key, prefix + 9));
      return recordCompare != 0
          ? recordCompare
          : Integer.compare(prefix + 17, key.length);
    }
    for (int i = 0; i < tail; i++) {
      final long v = i < 8
          ? value
          : record;
      final int difference = (int) (v >>> ((7 - (i & 7)) * 8) & 255) - (key[prefix + 1 + i] & 255);
      if (difference != 0)
        return difference;
    }
    return 1;
  }

  boolean hasPrefix(final int row, final byte[] prefix, final int length) {
    Objects.checkFromIndexSize(0, length, prefix.length);
    final int size = keyLength(row);
    if (length > size)
      return false;
    final int dictionaryLength = size - 17;
    final int common = Math.min(length, dictionaryLength);
    if (!Arrays.equals(data, prefixStart(row), prefixStart(row) + common, prefix, 0, common))
      return false;
    if (length <= dictionaryLength)
      return true;
    if (prefix[dictionaryLength] != 1)
      return false;
    final long value = value(row) ^ Long.MIN_VALUE;
    final long record = record(row);
    for (int i = 0; i < length - dictionaryLength - 1; i++) {
      final long v = i < 8
          ? value
          : record;
      if ((byte) (v >>> ((7 - (i & 7)) * 8)) != prefix[dictionaryLength + 1 + i])
        return false;
    }
    return true;
  }

  int payloadLength(final int row) {
    Objects.checkIndex(row, count);
    return words * 8;
  }

  private long payloadWord(final int row, final int word) {
    final int w = data[HEADER + word * 9] & 255;
    return le(data, HEADER + word * 9 + 1, 8) + le(data, payloadOffsets[word] + row * w, w);
  }

  void copyPayload(final int row, final byte[] target, final int at) {
    Objects.checkIndex(row, count);
    Objects.checkFromIndexSize(at, words * 8, target.length);
    for (int word = 0; word < words; word++)
      put(target, at + word * 8, payloadWord(row, word), 8);
  }

  int payloadInt(final int row, final int offset) {
    Objects.checkFromIndexSize(offset, 4, payloadLength(row));
    int result = 0;
    for (int i = 0; i < 4; i++)
      result |= (int) (payloadWord(row, (offset + i) / 8) >>> (((offset + i) & 7) * 8) & 255) << (i * 8);
    return result;
  }

  private static int width(final long value) {
    return (64 - Long.numberOfLeadingZeros(value) + 7) / 8;
  }

  private static int u16(final byte[] bytes, final int at) {
    return (bytes[at] & 255) | (bytes[at + 1] & 255) << 8;
  }

  private static long le(final byte[] bytes, final int at, final int width) {
    // Avoid a variable-trip loop for the byte widths used by compact columns and their headers.
    // Plain shifts also stay cheap before C2 compilation; no VarHandle access-mode linkage is needed.
    if (width == 0)
      return 0;
    if (width == 1)
      return bytes[at] & 255L;
    if (width == 2)
      return u16(bytes, at);
    if (width == 3)
      return u16(bytes, at) | (bytes[at + 2] & 255L) << 16;
    if (width == 4)
      return u16(bytes, at) | (long) u16(bytes, at + 2) << 16;
    if (width == 8)
      return (bytes[at] & 255L) | (bytes[at + 1] & 255L) << 8 | (bytes[at + 2] & 255L) << 16
          | (bytes[at + 3] & 255L) << 24 | (bytes[at + 4] & 255L) << 32 | (bytes[at + 5] & 255L) << 40
          | (bytes[at + 6] & 255L) << 48 | (bytes[at + 7] & 255L) << 56;
    long value = 0;
    for (int i = 0; i < width; i++)
      value |= (bytes[at + i] & 255L) << (i * 8);
    return value;
  }

  private static long be(final byte[] bytes, final int at) {
    return Long.reverseBytes(le(bytes, at, Long.BYTES));
  }

  private static void put(final byte[] bytes, final int at, final long value, final int width) {
    if (width == 1) {
      bytes[at] = (byte) value;
      return;
    }
    if (width == 2) {
      bytes[at] = (byte) value;
      bytes[at + 1] = (byte) (value >>> 8);
      return;
    }
    if (width == 8) {
      bytes[at] = (byte) value;
      bytes[at + 1] = (byte) (value >>> 8);
      bytes[at + 2] = (byte) (value >>> 16);
      bytes[at + 3] = (byte) (value >>> 24);
      bytes[at + 4] = (byte) (value >>> 32);
      bytes[at + 5] = (byte) (value >>> 40);
      bytes[at + 6] = (byte) (value >>> 48);
      bytes[at + 7] = (byte) (value >>> 56);
      return;
    }
    for (int i = 0; i < width; i++)
      bytes[at + i] = (byte) (value >>> (i * 8));
  }

  private static void putBe(final byte[] bytes, final int at, final long value) {
    put(bytes, at, Long.reverseBytes(value), Long.BYTES);
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invalid sorted Data Block");
  }
}

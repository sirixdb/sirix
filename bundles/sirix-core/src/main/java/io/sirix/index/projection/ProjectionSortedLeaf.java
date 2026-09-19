/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * One bounded, immutable leaf of a covering projection ordered by encoded field tuples.
 *
 * <p>
 * The common prefix of the first and last key is stored once. Each entry contains the remaining key
 * bytes and an optional covering payload; a primitive offset table permits binary search without
 * decoding preceding rows. A writer replaces this leaf as one copy-on-write unit. Larger key ranges
 * are split into additional leaves instead of increasing the mutation unit.
 * </p>
 *
 * <p>
 * Wire form: {@code magic:i32, version:u8, rows:u16, prefixBytes:u16, commonPrefix,
 * offsets[rows+1]:i32, (keySuffixBytes:u16, payloadBytes:u16, keySuffix, payload)*}. All integers
 * are little-endian except the ordered key bytes themselves. The last offset equals payload length;
 * every entry and offset is checked when the leaf is opened.
 * </p>
 */
final class ProjectionSortedLeaf {

  static final int MAX_ROWS = 256;
  static final int MAX_BYTES = 64 << 10;

  private static final int MAGIC = 0x314C5350; // PSL1
  private static final byte VERSION = 1;
  private static final int HEADER_BYTES = 9;
  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final byte[] bytes;
  private final int rows;
  private final int prefixLength;
  private final int offsetsStart;

  private ProjectionSortedLeaf(final byte[] bytes, final int rows, final int prefixLength) {
    this.bytes = bytes;
    this.rows = rows;
    this.prefixLength = prefixLength;
    this.offsetsStart = HEADER_BYTES + prefixLength;
  }

  /** Sorted keys addressed by position: a packed build run or a merge's staging window. */
  interface KeySource {
    int keyCount();

    byte[] keyBlock(int position);

    int keyOffset(int position);

    int keyLength(int position);
  }

  /**
   * Encode a sorted run of unique row keys. Returns null when the bounded leaf must split.
   * {@code payloads == null} means the keys cover every column needed by this access path.
   */
  static @Nullable ProjectionSortedLeaf encode(final byte[][] keys, final byte @Nullable [][] payloads,
      final int count) {
    return encode(keys, payloads, 0, count);
  }

  /**
   * Encode {@code keys[from, from + count)} with their payloads; null when the bounded leaf must
   * split.
   */
  static @Nullable ProjectionSortedLeaf encode(final byte[][] keys, final byte @Nullable [][] payloads, final int from,
      final int count) {
    Objects.requireNonNull(keys, "keys");
    if (from < 0 || count < 1 || (long) from + count > keys.length) {
      throw new IllegalArgumentException("count must name a nonempty range of keys");
    }
    if (payloads != null && payloads.length < from + count) {
      throw new IllegalArgumentException("payloads must cover every key");
    }
    if (count > MAX_ROWS) {
      return null;
    }
    final byte[] first = Objects.requireNonNull(keys[from], "first key");
    final byte[] last = Objects.requireNonNull(keys[from + count - 1], "last key");
    int prefix = 0;
    while (prefix < first.length && prefix < last.length && first[prefix] == last[prefix]) {
      prefix++;
    }
    if (prefix > 0xFFFF) {
      return null;
    }
    long length = HEADER_BYTES + prefix + ((long) count + 1) * Integer.BYTES;
    for (int i = from; i < from + count; i++) {
      final byte[] key = Objects.requireNonNull(keys[i], "key");
      if (i > from && Arrays.compareUnsigned(keys[i - 1], key) >= 0) {
        throw new IllegalArgumentException("sorted leaf keys must be unique and increasing at row " + (i - from));
      }
      if (key.length < prefix || key.length - prefix > 0xFFFF) {
        return null;
      }
      final byte[] payload = payloads == null
          ? EMPTY_PAYLOAD
          : Objects.requireNonNull(payloads[i], "payload");
      if (payload.length > 0xFFFF) {
        return null;
      }
      length += 2L * Short.BYTES + key.length - prefix + payload.length;
      if (length > MAX_BYTES) {
        return null;
      }
    }
    final byte[] data = new byte[(int) length];
    putInt(data, 0, MAGIC);
    data[4] = VERSION;
    putShort(data, 5, count);
    putShort(data, 7, prefix);
    System.arraycopy(first, 0, data, HEADER_BYTES, prefix);
    final int offsetsStart = HEADER_BYTES + prefix;
    int at = offsetsStart + (count + 1) * Integer.BYTES;
    for (int i = 0; i < count; i++) {
      putInt(data, offsetsStart + i * Integer.BYTES, at);
      final byte[] key = keys[from + i];
      final byte[] payload = payloads == null
          ? EMPTY_PAYLOAD
          : payloads[from + i];
      final int suffixLength = key.length - prefix;
      putShort(data, at, suffixLength);
      putShort(data, at + Short.BYTES, payload.length);
      at += 2 * Short.BYTES;
      System.arraycopy(key, prefix, data, at, suffixLength);
      at += suffixLength;
      System.arraycopy(payload, 0, data, at, payload.length);
      at += payload.length;
    }
    putInt(data, offsetsStart + count * Integer.BYTES, at);
    return new ProjectionSortedLeaf(data, count, prefix);
  }

  /** Initial-build encoding directly from packed sorted keys, with no per-row key arrays. */
  static @Nullable ProjectionSortedLeaf encodeSortedRun(final KeySource run, final int from, final int count) {
    Objects.requireNonNull(run, "run");
    if (from < 0 || count < 1 || (long) from + count > run.keyCount()) {
      throw new IllegalArgumentException("sorted run page range is outside its rows");
    }
    if (count > MAX_ROWS) {
      return null;
    }
    final byte[] firstBlock = run.keyBlock(from);
    final byte[] lastBlock = run.keyBlock(from + count - 1);
    final int firstOffset = run.keyOffset(from);
    final int lastOffset = run.keyOffset(from + count - 1);
    final int firstLength = run.keyLength(from);
    final int lastLength = run.keyLength(from + count - 1);
    int prefix = 0;
    while (prefix < firstLength && prefix < lastLength
        && firstBlock[firstOffset + prefix] == lastBlock[lastOffset + prefix]) {
      prefix++;
    }
    long length = HEADER_BYTES + prefix + ((long) count + 1) * Integer.BYTES;
    for (int i = 0; i < count; i++) {
      length += 2L * Short.BYTES + run.keyLength(from + i) - prefix;
      if (length > MAX_BYTES) {
        return null;
      }
    }
    final byte[] data = new byte[(int) length];
    putInt(data, 0, MAGIC);
    data[4] = VERSION;
    putShort(data, 5, count);
    putShort(data, 7, prefix);
    System.arraycopy(firstBlock, firstOffset, data, HEADER_BYTES, prefix);
    final int offsetsStart = HEADER_BYTES + prefix;
    int at = offsetsStart + (count + 1) * Integer.BYTES;
    for (int i = 0; i < count; i++) {
      final int suffixLength = run.keyLength(from + i) - prefix;
      putInt(data, offsetsStart + i * Integer.BYTES, at);
      putShort(data, at, suffixLength);
      putShort(data, at + Short.BYTES, 0);
      at += 2 * Short.BYTES;
      System.arraycopy(run.keyBlock(from + i), run.keyOffset(from + i) + prefix, data, at, suffixLength);
      at += suffixLength;
    }
    putInt(data, offsetsStart + count * Integer.BYTES, at);
    return new ProjectionSortedLeaf(data, count, prefix);
  }

  /** Open and validate a persisted leaf before it can participate in a result. */
  static ProjectionSortedLeaf open(final byte[] data) {
    Objects.requireNonNull(data, "data");
    if (data.length < HEADER_BYTES + 2 * Integer.BYTES || data.length > MAX_BYTES || getInt(data, 0) != MAGIC
        || data[4] != VERSION) {
      throw new IllegalArgumentException("invalid sorted projection leaf header");
    }
    final int count = getUnsignedShort(data, 5);
    final int prefix = getUnsignedShort(data, 7);
    if (count < 1 || count > MAX_ROWS || HEADER_BYTES + prefix + ((long) count + 1) * Integer.BYTES > data.length) {
      throw new IllegalArgumentException("invalid sorted projection leaf dimensions");
    }
    final ProjectionSortedLeaf leaf = new ProjectionSortedLeaf(data, count, prefix);
    final int dataStart = leaf.offsetsStart + (count + 1) * Integer.BYTES;
    if (leaf.offset(0) != dataStart || leaf.offset(count) != data.length) {
      throw new IllegalArgumentException("sorted projection leaf offsets do not cover its data");
    }
    int previousSuffixStart = -1;
    int previousSuffixEnd = -1;
    for (int i = 0; i < count; i++) {
      final int start = leaf.offset(i);
      final int end = leaf.offset(i + 1);
      if (start < dataStart || end < start + 2 * Short.BYTES || end > data.length) {
        throw new IllegalArgumentException("invalid sorted projection leaf offset at row " + i);
      }
      final int suffixLength = getUnsignedShort(data, start);
      final int payloadLength = getUnsignedShort(data, start + Short.BYTES);
      final int suffixStart = start + 2 * Short.BYTES;
      if ((long) suffixStart + suffixLength + payloadLength != end) {
        throw new IllegalArgumentException("invalid sorted projection leaf entry at row " + i);
      }
      if (i > 0 && Arrays.compareUnsigned(data, previousSuffixStart, previousSuffixEnd, data, suffixStart,
          suffixStart + suffixLength) >= 0) {
        throw new IllegalArgumentException("sorted projection leaf keys are not strictly increasing");
      }
      previousSuffixStart = suffixStart;
      previousSuffixEnd = suffixStart + suffixLength;
    }
    return leaf;
  }

  int rowCount() {
    return rows;
  }

  int commonPrefixLength() {
    return prefixLength;
  }

  /** First row whose key is greater than or equal to {@code searchKey}. */
  int lowerBound(final byte[] searchKey) {
    Objects.requireNonNull(searchKey, "searchKey");
    int low = 0;
    int high = rows;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (compareRowKey(mid, searchKey) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  int compareRowKey(final int row, final byte[] searchKey) {
    Objects.requireNonNull(searchKey, "searchKey");
    final int start = offset(row);
    final int suffixLength = getUnsignedShort(bytes, start);
    final int comparedPrefix = Math.min(searchKey.length, prefixLength);
    final int prefixComparison =
        Arrays.compareUnsigned(bytes, HEADER_BYTES, HEADER_BYTES + prefixLength, searchKey, 0, comparedPrefix);
    if (prefixComparison != 0) {
      return prefixComparison;
    }
    return Arrays.compareUnsigned(bytes, start + 2 * Short.BYTES, start + 2 * Short.BYTES + suffixLength, searchKey,
        prefixLength, searchKey.length);
  }

  byte[] copyKey(final int row) {
    final byte[] key = new byte[keyLength(row)];
    copyKeyTo(row, key);
    return key;
  }

  int keyLength(final int row) {
    return prefixLength + getUnsignedShort(bytes, offset(row));
  }

  void copyKeyTo(final int row, final byte[] target) {
    Objects.requireNonNull(target, "target");
    final int start = offset(row);
    final int suffixLength = getUnsignedShort(bytes, start);
    if (target.length < prefixLength + suffixLength) {
      throw new IllegalArgumentException("sorted key scratch is too short");
    }
    System.arraycopy(bytes, HEADER_BYTES, target, 0, prefixLength);
    System.arraycopy(bytes, start + 2 * Short.BYTES, target, prefixLength, suffixLength);
  }

  boolean keyHasPrefix(final int row, final byte[] prefix, final int length) {
    Objects.requireNonNull(prefix, "prefix");
    Objects.checkFromIndexSize(0, length, prefix.length);
    final int start = offset(row);
    final int suffixLength = getUnsignedShort(bytes, start);
    if (prefixLength + suffixLength < length) {
      return false;
    }
    final int common = Math.min(prefixLength, length);
    return Arrays.equals(bytes, HEADER_BYTES, HEADER_BYTES + common, prefix, 0, common)
        && (length <= prefixLength || Arrays.equals(bytes, start + 2 * Short.BYTES,
            start + 2 * Short.BYTES + length - prefixLength, prefix, prefixLength, length));
  }

  /** First row after the current prefix run, comparing the leaf's shared bytes only once. */
  int firstNonPrefixRowAfter(final int row, final byte[] prefix, final int length) {
    Objects.requireNonNull(prefix, "prefix");
    Objects.checkFromIndexSize(0, length, prefix.length);
    Objects.checkIndex(row, rows);
    if (!keyHasPrefix(row, prefix, length)) {
      throw new IllegalArgumentException("row is not on the requested prefix");
    }
    if (length <= prefixLength) {
      return rows;
    }
    final int suffixPrefixLength = length - prefixLength;
    int low = row + 1;
    // Most sorted groups contain one or two rows. Probe those adjacent rows before searching
    // the rest of the leaf so a singleton never pays for log(rowCount) prefix comparisons.
    if (low == rows || !keyHasPrefix(low, prefix, length)) {
      return low;
    }
    if (++low == rows || !keyHasPrefix(low, prefix, length)) {
      return low;
    }
    low++;
    int high = rows;
    while (low < high) {
      final int middle = low + (high - low >>> 1);
      final int start = offset(middle);
      final int suffixLength = getUnsignedShort(bytes, start);
      if (suffixLength >= suffixPrefixLength && Arrays.equals(bytes, start + 2 * Short.BYTES,
          start + 2 * Short.BYTES + suffixPrefixLength, prefix, prefixLength, length)) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  byte[] copyPayload(final int row) {
    final byte[] payload = new byte[payloadLength(row)];
    copyPayloadTo(row, payload, 0);
    return payload;
  }

  int payloadLength(final int row) {
    return getUnsignedShort(bytes, offset(row) + Short.BYTES);
  }

  /** Copy a covering value into caller-owned scratch; a scan need not allocate per row. */
  void copyPayloadTo(final int row, final byte[] target, final int targetOffset) {
    Objects.requireNonNull(target, "target");
    final int start = offset(row);
    final int suffixLength = getUnsignedShort(bytes, start);
    final int length = getUnsignedShort(bytes, start + Short.BYTES);
    Objects.checkFromIndexSize(targetOffset, length, target.length);
    System.arraycopy(bytes, start + 2 * Short.BYTES + suffixLength, target, targetOffset, length);
  }

  /** Decode a four-byte directory child id without allocating a payload array. */
  int intPayloadAt(final int row) {
    final int start = offset(row);
    final int suffixLength = getUnsignedShort(bytes, start);
    if (getUnsignedShort(bytes, start + Short.BYTES) != Integer.BYTES) {
      throw new IllegalStateException("sorted directory child id must occupy four bytes");
    }
    return getInt(bytes, start + 2 * Short.BYTES + suffixLength);
  }

  /** Immutable bytes owned by this leaf; a storage writer may persist them without copying. */
  byte[] encodedBytes() {
    return bytes;
  }

  /**
   * Insert one distinct row, rewriting only this bounded leaf. Null means it must split first. No
   * stored row is materialized as a {@code byte[]} during the rewrite.
   */
  @Nullable
  ProjectionSortedLeaf withInserted(final byte[] key, final byte[] payload) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(payload, "payload");
    final int position = lowerBound(key);
    if (position < rows && compareRowKey(position, key) == 0) {
      throw new IllegalArgumentException("sorted projection leaf already contains the key");
    }
    if (rows == MAX_ROWS || key.length > 0xFFFF || payload.length > 0xFFFF) {
      return null;
    }
    final byte[] first = position == 0
        ? key
        : copyKey(0);
    final byte[] last = position == rows
        ? key
        : copyKey(rows - 1);
    return rewrite(position, key, payload, first, last, true);
  }

  /**
   * Delete an exact row. Null means the leaf became empty and its directory entry must be removed.
   * Missing keys fail loudly: a persisted row locator that names this leaf cannot silently drift.
   */
  @Nullable
  ProjectionSortedLeaf withRemoved(final byte[] key) {
    Objects.requireNonNull(key, "key");
    final int position = lowerBound(key);
    if (position == rows || compareRowKey(position, key) != 0) {
      throw new IllegalStateException("sorted projection leaf does not contain the key");
    }
    if (rows == 1) {
      return null;
    }
    final byte[] first = copyKey(position == 0
        ? 1
        : 0);
    final byte[] last = copyKey(position == rows - 1
        ? rows - 2
        : rows - 1);
    return rewrite(position, key, EMPTY_PAYLOAD, first, last, false);
  }

  /** Replace one fence or covering value while copying existing entries as contiguous bytes. */
  @Nullable
  ProjectionSortedLeaf withReplaced(final int position, final byte[] key, final byte[] payload) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(payload, "payload");
    if (position < 0 || position >= rows) {
      throw new IndexOutOfBoundsException("row " + position + " outside 0.." + (rows - 1));
    }
    if ((position > 0 && compareRowKey(position - 1, key) >= 0)
        || (position + 1 < rows && compareRowKey(position + 1, key) <= 0)) {
      throw new IllegalArgumentException("replacement key violates sorted leaf order");
    }
    if (key.length > 0xFFFF || payload.length > 0xFFFF) {
      return null;
    }
    final byte[] first = position == 0
        ? key
        : copyKey(0);
    final byte[] last = position == rows - 1
        ? key
        : copyKey(rows - 1);
    int newPrefix = 0;
    while (newPrefix < first.length && newPrefix < last.length && first[newPrefix] == last[newPrefix]) {
      newPrefix++;
    }
    if (newPrefix > 0xFFFF) {
      return null;
    }
    long length = HEADER_BYTES + newPrefix + ((long) rows + 1) * Integer.BYTES;
    for (int i = 0; i < rows; i++) {
      if (i == position) {
        length += 2L * Short.BYTES + key.length - newPrefix + payload.length;
      } else {
        final int start = offset(i);
        length += 2L * Short.BYTES + prefixLength + getUnsignedShort(bytes, start) - newPrefix
            + getUnsignedShort(bytes, start + Short.BYTES);
      }
      if (length > MAX_BYTES) {
        return null;
      }
    }
    final byte[] encoded = new byte[(int) length];
    putInt(encoded, 0, MAGIC);
    encoded[4] = VERSION;
    putShort(encoded, 5, rows);
    putShort(encoded, 7, newPrefix);
    System.arraycopy(first, 0, encoded, HEADER_BYTES, newPrefix);
    final int newOffsetsStart = HEADER_BYTES + newPrefix;
    int at = newOffsetsStart + (rows + 1) * Integer.BYTES;
    for (int i = 0; i < rows; i++) {
      putInt(encoded, newOffsetsStart + i * Integer.BYTES, at);
      at = i == position
          ? writeInsertedRow(encoded, at, key, payload, newPrefix)
          : copyExistingRow(encoded, at, i, newPrefix);
    }
    putInt(encoded, newOffsetsStart + rows * Integer.BYTES, at);
    return new ProjectionSortedLeaf(encoded, rows, newPrefix);
  }

  private @Nullable ProjectionSortedLeaf rewrite(final int position, final byte[] insertedKey,
      final byte[] insertedPayload, final byte[] first, final byte[] last, final boolean insertion) {
    int newPrefix = 0;
    while (newPrefix < first.length && newPrefix < last.length && first[newPrefix] == last[newPrefix]) {
      newPrefix++;
    }
    if (newPrefix > 0xFFFF) {
      return null;
    }
    final int newRows = rows + (insertion
        ? 1
        : -1);
    long length = HEADER_BYTES + newPrefix + ((long) newRows + 1) * Integer.BYTES;
    for (int target = 0; target < newRows; target++) {
      if (insertion && target == position) {
        length += 2L * Short.BYTES + insertedKey.length - newPrefix + insertedPayload.length;
      } else {
        final int source = insertion
            ? (target < position
                ? target
                : target - 1)
            : (target < position
                ? target
                : target + 1);
        final int at = offset(source);
        final int suffixLength = getUnsignedShort(bytes, at);
        final int payloadLength = getUnsignedShort(bytes, at + Short.BYTES);
        length += 2L * Short.BYTES + prefixLength + suffixLength - newPrefix + payloadLength;
      }
      if (length > MAX_BYTES) {
        return null;
      }
    }
    final byte[] encoded = new byte[(int) length];
    putInt(encoded, 0, MAGIC);
    encoded[4] = VERSION;
    putShort(encoded, 5, newRows);
    putShort(encoded, 7, newPrefix);
    System.arraycopy(first, 0, encoded, HEADER_BYTES, newPrefix);
    final int newOffsetsStart = HEADER_BYTES + newPrefix;
    int at = newOffsetsStart + (newRows + 1) * Integer.BYTES;
    for (int target = 0; target < newRows; target++) {
      putInt(encoded, newOffsetsStart + target * Integer.BYTES, at);
      if (insertion && target == position) {
        at = writeInsertedRow(encoded, at, insertedKey, insertedPayload, newPrefix);
      } else {
        final int source = insertion
            ? (target < position
                ? target
                : target - 1)
            : (target < position
                ? target
                : target + 1);
        at = copyExistingRow(encoded, at, source, newPrefix);
      }
    }
    putInt(encoded, newOffsetsStart + newRows * Integer.BYTES, at);
    return new ProjectionSortedLeaf(encoded, newRows, newPrefix);
  }

  private static int writeInsertedRow(final byte[] target, int at, final byte[] key, final byte[] payload,
      final int newPrefix) {
    final int suffixLength = key.length - newPrefix;
    putShort(target, at, suffixLength);
    putShort(target, at + Short.BYTES, payload.length);
    at += 2 * Short.BYTES;
    System.arraycopy(key, newPrefix, target, at, suffixLength);
    at += suffixLength;
    System.arraycopy(payload, 0, target, at, payload.length);
    return at + payload.length;
  }

  private int copyExistingRow(final byte[] target, int at, final int source, final int newPrefix) {
    final int sourceStart = offset(source);
    final int oldSuffixLength = getUnsignedShort(bytes, sourceStart);
    final int payloadLength = getUnsignedShort(bytes, sourceStart + Short.BYTES);
    final int oldSuffixStart = sourceStart + 2 * Short.BYTES;
    final int newSuffixLength = prefixLength + oldSuffixLength - newPrefix;
    putShort(target, at, newSuffixLength);
    putShort(target, at + Short.BYTES, payloadLength);
    at += 2 * Short.BYTES;
    if (newPrefix < prefixLength) {
      final int prefixTail = prefixLength - newPrefix;
      System.arraycopy(bytes, HEADER_BYTES + newPrefix, target, at, prefixTail);
      System.arraycopy(bytes, oldSuffixStart, target, at + prefixTail, oldSuffixLength);
    } else {
      System.arraycopy(bytes, oldSuffixStart + newPrefix - prefixLength, target, at, newSuffixLength);
    }
    at += newSuffixLength;
    System.arraycopy(bytes, oldSuffixStart + oldSuffixLength, target, at, payloadLength);
    return at + payloadLength;
  }

  private int offset(final int index) {
    if (index < 0 || index > rows) {
      throw new IndexOutOfBoundsException("row " + index + " outside 0.." + rows);
    }
    return getInt(bytes, offsetsStart + index * Integer.BYTES);
  }

  private static int getUnsignedShort(final byte[] data, final int at) {
    return (data[at] & 0xFF) | (data[at + 1] & 0xFF) << 8;
  }

  private static int getInt(final byte[] data, final int at) {
    return (data[at] & 0xFF) | (data[at + 1] & 0xFF) << 8 | (data[at + 2] & 0xFF) << 16 | (data[at + 3] & 0xFF) << 24;
  }

  private static void putShort(final byte[] data, final int at, final int value) {
    data[at] = (byte) value;
    data[at + 1] = (byte) (value >>> 8);
  }

  private static void putInt(final byte[] data, final int at, final int value) {
    data[at] = (byte) value;
    data[at + 1] = (byte) (value >>> 8);
    data[at + 2] = (byte) (value >>> 16);
    data[at + 3] = (byte) (value >>> 24);
  }
}

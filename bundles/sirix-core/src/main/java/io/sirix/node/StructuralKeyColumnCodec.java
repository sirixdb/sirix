/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.node;

import io.sirix.settings.Fixed;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Page-level column codec for a single structural-key column (parentKey, firstChildKey,
 * leftSiblingKey, rightSiblingKey) across all slots in a {@link io.sirix.page.KeyValueLeafPage}.
 *
 * <p>
 * Replaces per-slot {@code delta(value, nodeKey)} varint encoding with a column-wide scheme that
 * exploits DFS-order locality: sibling groups share the same {@code parentKey}, so most column
 * values repeat the previous slot.
 *
 * <h2>Encoding</h2>
 * <ul>
 * <li>{@code FLAG_ALL_NULL}: column is entirely {@link Fixed#NULL_NODE_KEY}. Fixed 3 bytes.</li>
 * <li>{@code FLAG_CONSTANT}: all slots hold the same value. 11 bytes regardless of N.</li>
 * <li>{@code FLAG_SEQUENTIAL_PLUS1}: slot i equals {@code base + i}. 11 bytes regardless of N.</li>
 * <li>{@code FLAG_HAS_BITMAP} (general): 1 bit per slot indicates "equal to predictor"; for slots
 * where the bit is zero, a zig-zag varint of {@code delta = value - predictor} follows in an
 * override stream. The predictor is the previous slot's decoded value (or {@code NULL} for slot 0).
 * In DFS insert order this pattern is dense: sibling chains, null-child columns, and repeated
 * sentinel values all collapse to 1 bit.</li>
 * <li>{@code FLAG_NODEKEY_PREDICTED} (general): 2 bits per slot selecting one of four predictors —
 * see below. Requires the caller to supply each slot's node key.</li>
 * </ul>
 *
 * <h2>Why two general formats</h2> The single-bit format's only predictor is "the previous slot's
 * value". That is exactly right for {@code parentKey}, where a sibling group repeats one value for
 * its whole run. It is the wrong shape for the sibling and child columns, whose dominant case is
 * {@code value ==
 * nodeKey + 1} — a value that changes at every slot, so every slot spends an override varint, and
 * interleaved {@code NULL}s (the last child of each group) cost two large overrides each on the way
 * in and out. Measured against the per-slot {@code delta(value, nodeKey)} varints it replaces, that
 * lands at 0.86× on a synthetic right-sibling column: a net loss.
 *
 * <p>
 * {@code FLAG_NODEKEY_PREDICTED} spends 2 bits per slot instead of 1 and buys three predictors with
 * them, so the DFS shapes that defeat the single-bit format cost no override at all:
 * <ul>
 * <li>{@code 0} — the slot is {@link Fixed#NULL_NODE_KEY}. Last child of a sibling group, or the
 * whole column for a page of leaves.</li>
 * <li>{@code 1} — the slot is {@code nodeKey + stride}, with one signed {@code stride} in the
 * column header. {@code +1} covers right siblings and first children in DFS order; {@code -1}
 * covers left siblings.</li>
 * <li>{@code 2} — the slot repeats the previous slot's decoded value (what the single-bit format
 * encodes, kept so a mixed column doesn't have to choose).</li>
 * <li>{@code 3} — an explicit zig-zag varint of {@code value - nodeKey} follows in the override
 * stream. Deltas stay small for forward subtree jumps, so this is normally one byte — and unlike
 * code {@code 2} it leaves the stride alone, so an isolated jump doesn't knock the following run
 * out of code {@code 1}.</li>
 * </ul>
 * The encoder sizes both general formats and emits the smaller, so a column only pays the extra bit
 * per slot when the predictors earn it back.
 *
 * <h2>Random access</h2> Both general formats decode {@code O(slotIndex)} bytes from the override
 * stream, so whole-page reads should use {@link #decodeAll}, which is a single linear pass. For
 * random-access OLTP a per-column index (one entry every 16 slots) can be bolted on top without
 * changing the wire format.
 *
 * <h2>Lane encoding</h2> The two general formats spend their per-slot bits in a fixed-width lane —
 * one bit per slot for {@code FLAG_HAS_BITMAP}, two for {@code FLAG_NODEKEY_PREDICTED}. On a page
 * of records that lane is a handful of long runs: one code repeated for a whole record's fields,
 * then a different one at the boundary. {@code FLAG_LANE_RUN_LENGTH} says the lane is stored as
 * those runs instead — {@code varint runCount}, then {@code (code, varint runLength)} per run — and
 * the encoder emits it only when it comes out smaller.
 *
 * <p>
 * Worth doing even though the body codec already compresses the lane: measured on a 1,024-slot page
 * of 106-field records, the parentKey column's 128-byte bitmap is 27 bytes after LZ77 and 12 bytes
 * as runs then LZ77; the sibling columns' 256-byte code lanes go 23 and 20 down to 12. What LZ77
 * cannot do is turn a 105-bit run into a length, because it matches bytes and the run's byte
 * boundary shifts.
 *
 * <p>
 * The low bits of the format byte are exactly one of the format flags, never a combination; bit 5
 * is the lane's encoding, which is orthogonal to which predictors the format uses.
 */
public final class StructuralKeyColumnCodec {

  public static final int FLAG_ALL_NULL = 0x01;
  public static final int FLAG_CONSTANT = 0x02;
  public static final int FLAG_SEQUENTIAL_PLUS1 = 0x04;
  public static final int FLAG_HAS_BITMAP = 0x08;
  public static final int FLAG_NODEKEY_PREDICTED = 0x10;

  /**
   * Modifier on either general format: the per-slot lane is stored as runs rather than fixed-width
   * bits. Set only when the runs come out smaller than the bits they replace.
   */
  public static final int FLAG_LANE_RUN_LENGTH = 0x20;

  /** The format bits of a tag, with the lane-encoding modifier masked off. */
  private static final int FORMAT_MASK = 0x1F;

  /**
   * Whether a column may store its lane as runs.
   *
   * <p>
   * Kill switch: {@code -Dsirix.structuralColumn.runLengthLane=false} keeps every lane fixed-width,
   * so a column encodes exactly as it did before this form existed. Readers accept both regardless —
   * the tag says which — so a resource can hold columns of each. Not final, so a byte-identity test
   * can flip it after class load.
   */
  public static boolean RUN_LENGTH_LANE_ENABLED =
      !"false".equals(System.getProperty("sirix.structuralColumn.runLengthLane"));

  /** Maximum supported slots per column; fits in an unsigned 16-bit length field. */
  public static final int MAX_SLOTS = 0xFFFF;

  /** {@code FLAG_NODEKEY_PREDICTED} per-slot code: the slot is {@link Fixed#NULL_NODE_KEY}. */
  private static final int CODE_NULL = 0;

  /** {@code FLAG_NODEKEY_PREDICTED} per-slot code: the slot is {@code nodeKey + stride}. */
  private static final int CODE_STRIDE = 1;

  /** {@code FLAG_NODEKEY_PREDICTED} per-slot code: the slot repeats the previous slot's value. */
  private static final int CODE_PREVIOUS = 2;

  /** {@code FLAG_NODEKEY_PREDICTED} per-slot code: an explicit {@code value - nodeKey} varint. */
  private static final int CODE_EXPLICIT = 3;

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();

  private StructuralKeyColumnCodec() {}

  /**
   * Upper bound on the encoded size of an {@code n}-slot column in any format.
   *
   * <p>
   * Lets a caller encode straight into a pre-sized buffer and decide from the returned length whether
   * the column was worth keeping, instead of sizing with {@link #encodedSize} and then encoding —
   * which walks the values twice over and asks both calls to independently pick the same format.
   */
  public static int maxEncodedSize(final int n) {
    // Widest format is the node-key-predicted one: tag + slotCount + a 10-byte stride varint +
    // two bits per slot + a 10-byte override varint per slot.
    return 13 + ((n + 3) >>> 2) + 10 * n;
  }

  /**
   * Compute the encoded size in bytes without materializing the output.
   *
   * @param values one value per slot
   * @return encoded size in bytes
   */
  public static int encodedSize(final long[] values) {
    return encodeByteArray(null, 0, values, values.length, null);
  }

  /**
   * Compute the encoded size over {@code values[0..n)} without materializing the output. Allows
   * pre-sized scratch arrays with {@code length > n}.
   */
  public static int encodedSize(final long[] values, final int n) {
    return encodeByteArray(null, 0, values, n, null);
  }

  /**
   * Compute the encoded size over {@code values[0..n)} with node-key context, so
   * {@code FLAG_NODEKEY_PREDICTED} is considered alongside the other formats.
   *
   * @param values one value per slot
   * @param n number of slots to encode
   * @param nodeKeys node key of each slot, parallel to {@code values}; {@code null} to skip the
   *        node-key-predicted format
   */
  public static int encodedSize(final long[] values, final int n, final long[] nodeKeys) {
    return encodeByteArray(null, 0, values, n, nodeKeys);
  }

  /**
   * Encode the column into a {@code byte[]} at {@code offset}. Pass {@code target == null} to dry-run
   * and only compute the size.
   *
   * @return bytes written
   */
  public static int encodeByteArray(final byte[] target, final int offset, final long[] values) {
    return encodeByteArray(target, offset, values, values.length, null);
  }

  /**
   * Encode the first {@code n} values of {@code values} into {@code target} at {@code offset}. Pass
   * {@code target == null} to dry-run.
   */
  public static int encodeByteArray(final byte[] target, final int offset, final long[] values, final int n) {
    return encodeByteArray(target, offset, values, n, null);
  }

  /**
   * Encode the first {@code n} values of {@code values} into {@code target} at {@code offset}, with
   * the node key of each slot available as predictor context. Pass {@code target == null} to dry-run.
   *
   * <p>
   * Passing {@code nodeKeys} lets the encoder consider {@code FLAG_NODEKEY_PREDICTED}; it is emitted
   * only when it is strictly smaller than {@code FLAG_HAS_BITMAP} would be. The decoder needs the
   * same {@code nodeKeys} to read such a column back, so callers that cannot supply them at read time
   * must pass {@code null} here.
   *
   * @param nodeKeys node key of each slot, parallel to {@code values}; {@code null} to skip the
   *        node-key-predicted format
   * @return bytes written
   */
  public static int encodeByteArray(final byte[] target, final int offset, final long[] values, final int n,
      final long[] nodeKeys) {
    if (nodeKeys != null && nodeKeys.length < n) {
      throw new IllegalArgumentException("nodeKeys too short: " + nodeKeys.length + " < " + n);
    }
    if (n > MAX_SLOTS) {
      throw new IllegalArgumentException("Column too large: " + n + " > " + MAX_SLOTS);
    }
    if (n < 0 || n > values.length) {
      throw new IllegalArgumentException("Invalid n=" + n + " for values.length=" + values.length);
    }

    // 3-byte header: tag (1) + slotCount (2, big-endian).
    if (n == 0) {
      if (target != null) {
        target[offset] = 0;
        writeUnsignedShort(target, offset + 1, 0);
      }
      return 3;
    }

    // Pattern detection: all-null, constant, monotonic +1.
    boolean allNull = true;
    boolean constant = true;
    boolean monotonic = true;
    final long v0 = values[0];
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != NULL)
        allNull = false;
      if (v != v0)
        constant = false;
      if (v != v0 + i)
        monotonic = false;
    }

    if (allNull) {
      if (target != null) {
        target[offset] = FLAG_ALL_NULL;
        writeUnsignedShort(target, offset + 1, n);
      }
      return 3;
    }
    if (constant) {
      if (target != null) {
        target[offset] = FLAG_CONSTANT;
        writeUnsignedShort(target, offset + 1, n);
        writeLong(target, offset + 3, v0);
      }
      return 11;
    }
    if (monotonic) {
      if (target != null) {
        target[offset] = FLAG_SEQUENTIAL_PLUS1;
        writeUnsignedShort(target, offset + 1, n);
        writeLong(target, offset + 3, v0);
      }
      return 11;
    }

    // General: a one-bit-per-slot lane plus an override-varint stream.
    final int bitmapBytes = (n + 7) >>> 3;

    // Dry-run to size the override stream, and in the same pass the run-length form of the lane: a
    // run closes whenever the bit flips, and each costs its code byte plus a length varint.
    int overrideBytes = 0;
    int runLaneBytes = varintSize(0);
    int runCount = 0;
    int runLength = 0;
    int previousBit = -1;
    long predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      final int bit;
      if (v == predictor) {
        bit = 1;
      } else {
        bit = 0;
        overrideBytes += zigzagVarintSize(v - predictor);
      }
      if (bit != previousBit) {
        if (previousBit >= 0) {
          runLaneBytes += 1 + varintSize(runLength);
          runCount++;
        }
        previousBit = bit;
        runLength = 0;
      }
      runLength++;
      predictor = v;
    }
    if (previousBit >= 0) {
      runLaneBytes += 1 + varintSize(runLength);
      runCount++;
    }
    runLaneBytes += varintSize(runCount) - varintSize(0);
    final boolean bitmapRuns = RUN_LENGTH_LANE_ENABLED && runLaneBytes < bitmapBytes;
    final int bitmapLaneBytes = bitmapRuns
        ? runLaneBytes
        : bitmapBytes;

    final int totalBytes = 1 + 2 + bitmapLaneBytes + overrideBytes;

    // The node-key-predicted format costs one more bit per slot and saves an override on every
    // slot that is NULL or sits at nodeKey + stride, so which one wins is a property of the
    // column. Size it and take the smaller; ties go to the cheaper-to-decode single-bit form.
    if (nodeKeys != null) {
      final long stride = pickStride(values, n, nodeKeys);
      final int predictedBytes = sizeNodeKeyPredicted(values, n, nodeKeys, stride);
      if (predictedBytes < totalBytes) {
        return encodeNodeKeyPredicted(target, offset, values, n, nodeKeys, stride, predictedBytes);
      }
    }

    if (target == null) {
      return totalBytes;
    }

    target[offset] = (byte) (bitmapRuns
        ? (FLAG_HAS_BITMAP | FLAG_LANE_RUN_LENGTH)
        : FLAG_HAS_BITMAP);
    writeUnsignedShort(target, offset + 1, n);
    int writePos;
    if (bitmapRuns) {
      writePos = writeVarint(target, offset + 3, runCount);
      int previous = -1;
      int length = 0;
      predictor = NULL;
      for (int i = 0; i < n; i++) {
        final long v = values[i];
        final int bit = v == predictor
            ? 1
            : 0;
        if (bit != previous) {
          if (previous >= 0) {
            target[writePos++] = (byte) previous;
            writePos = writeVarint(target, writePos, length);
          }
          previous = bit;
          length = 0;
        }
        length++;
        predictor = v;
      }
      target[writePos++] = (byte) previous;
      writePos = writeVarint(target, writePos, length);
    } else {
      // zero bitmap
      for (int i = 0; i < bitmapBytes; i++) {
        target[offset + 3 + i] = 0;
      }
      writePos = offset + 3 + bitmapBytes;
    }
    predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v == predictor) {
        if (!bitmapRuns) {
          target[offset + 3 + (i >>> 3)] |= (byte) (1 << (i & 7));
        }
      } else {
        writePos = writeZigzagVarintToBytes(target, writePos, v - predictor);
      }
      predictor = v;
    }
    return totalBytes;
  }

  // ==================== FLAG_NODEKEY_PREDICTED ====================

  /**
   * Choose the column's {@code stride}, the delta from a slot's node key that code
   * {@link #CODE_STRIDE} stands for.
   *
   * <p>
   * Three candidates are counted in one pass: {@code +1} (right siblings and first children in DFS
   * order), {@code -1} (left siblings), and the first non-NULL slot's own delta, which catches a
   * column with some other uniform shape. A wrong guess is not a correctness problem and barely a
   * size one — those slots fall back to {@link #CODE_EXPLICIT}, and the caller still only emits this
   * format when it beats the alternative.
   */
  private static long pickStride(final long[] values, final int n, final long[] nodeKeys) {
    long firstDelta = 1;
    boolean firstSeen = false;
    int countPlus1 = 0;
    int countMinus1 = 0;
    int countFirst = 0;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v == NULL) {
        continue;
      }
      final long d = v - nodeKeys[i];
      if (!firstSeen) {
        firstDelta = d;
        firstSeen = true;
      }
      if (d == 1L)
        countPlus1++;
      if (d == -1L)
        countMinus1++;
      if (d == firstDelta)
        countFirst++;
    }
    long stride = 1L;
    int best = countPlus1;
    if (countMinus1 > best) {
      stride = -1L;
      best = countMinus1;
    }
    if (countFirst > best) {
      stride = firstDelta;
    }
    return stride;
  }

  /**
   * Encoded size of the node-key-predicted format for the given stride, with the lane in whichever
   * encoding comes out smaller.
   */
  private static int sizeNodeKeyPredicted(final long[] values, final int n, final long[] nodeKeys, final long stride) {
    int overrideBytes = 0;
    long previous = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != NULL && v != nodeKeys[i] + stride && v != previous) {
        overrideBytes += zigzagVarintSize(v - nodeKeys[i]);
      }
      previous = v;
    }
    return 3 + zigzagVarintSize(stride) + predictedLaneBytes(values, n, nodeKeys, stride) + overrideBytes;
  }

  /**
   * Bytes the node-key-predicted lane takes, fixed-width or as runs, whichever is smaller. A page of
   * records repeats one code for a whole record's fields, so the run form is usually a fraction of
   * the two bits per slot the fixed one spends.
   */
  private static int predictedLaneBytes(final long[] values, final int n, final long[] nodeKeys, final long stride) {
    if (!RUN_LENGTH_LANE_ENABLED) {
      return codeBytes(n);
    }
    int runLaneBytes = 0;
    int runCount = 0;
    int runLength = 0;
    int previousCode = -1;
    long previous = NULL;
    for (int i = 0; i < n; i++) {
      final int code = predictedCodeAt(values, nodeKeys, stride, i, previous);
      if (code != previousCode) {
        if (previousCode >= 0) {
          runLaneBytes += 1 + varintSize(runLength);
          runCount++;
        }
        previousCode = code;
        runLength = 0;
      }
      runLength++;
      previous = values[i];
    }
    if (previousCode >= 0) {
      runLaneBytes += 1 + varintSize(runLength);
      runCount++;
    }
    runLaneBytes += varintSize(runCount);
    return Math.min(codeBytes(n), runLaneBytes);
  }

  /** The predictor code slot {@code i} takes, given the previous slot's decoded value. */
  private static int predictedCodeAt(final long[] values, final long[] nodeKeys, final long stride, final int i,
      final long previous) {
    final long v = values[i];
    if (v == NULL) {
      return CODE_NULL;
    }
    if (v == nodeKeys[i] + stride) {
      return CODE_STRIDE;
    }
    return v == previous
        ? CODE_PREVIOUS
        : CODE_EXPLICIT;
  }

  /**
   * Emit the node-key-predicted format. {@code totalBytes} is the size {@link #sizeNodeKeyPredicted}
   * already computed for this {@code stride}; a dry-run ({@code target == null}) returns it without
   * writing.
   */
  private static int encodeNodeKeyPredicted(final byte[] target, final int offset, final long[] values, final int n,
      final long[] nodeKeys, final long stride, final int totalBytes) {
    if (target == null) {
      return totalBytes;
    }
    final boolean runs = predictedLaneBytes(values, n, nodeKeys, stride) < codeBytes(n);
    target[offset] = (byte) (runs
        ? (FLAG_NODEKEY_PREDICTED | FLAG_LANE_RUN_LENGTH)
        : FLAG_NODEKEY_PREDICTED);
    writeUnsignedShort(target, offset + 1, n);
    int writePos = writeZigzagVarintToBytes(target, offset + 3, stride);
    final int codesStart = writePos;
    final int codeArrayBytes = runs
        ? 0
        : codeBytes(n);
    if (runs) {
      int runCount = 0;
      int previousCode = -1;
      long walk = NULL;
      for (int i = 0; i < n; i++) {
        final int code = predictedCodeAt(values, nodeKeys, stride, i, walk);
        if (code != previousCode) {
          runCount++;
          previousCode = code;
        }
        walk = values[i];
      }
      writePos = writeVarint(target, writePos, runCount);
      previousCode = -1;
      int runLength = 0;
      walk = NULL;
      for (int i = 0; i < n; i++) {
        final int code = predictedCodeAt(values, nodeKeys, stride, i, walk);
        if (code != previousCode) {
          if (previousCode >= 0) {
            target[writePos++] = (byte) previousCode;
            writePos = writeVarint(target, writePos, runLength);
          }
          previousCode = code;
          runLength = 0;
        }
        runLength++;
        walk = values[i];
      }
      target[writePos++] = (byte) previousCode;
      writePos = writeVarint(target, writePos, runLength);
    } else {
      for (int i = 0; i < codeArrayBytes; i++) {
        target[codesStart + i] = 0;
      }
      writePos = codesStart + codeArrayBytes;
    }

    long previous = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      final int code = predictedCodeAt(values, nodeKeys, stride, i, previous);
      if (code == CODE_EXPLICIT) {
        writePos = writeZigzagVarintToBytes(target, writePos, v - nodeKeys[i]);
      }
      if (!runs) {
        target[codesStart + (i >>> 2)] |= (byte) (code << ((i & 3) << 1));
      }
      previous = v;
    }
    return totalBytes;
  }

  /** Bytes taken by the 2-bits-per-slot code array of a node-key-predicted column. */
  private static int codeBytes(final int n) {
    return (n + 3) >>> 2;
  }

  /** Bytes an unsigned varint takes. */
  private static int varintSize(final int value) {
    int size = 1;
    int remaining = value >>> 7;
    while (remaining != 0) {
      size++;
      remaining >>>= 7;
    }
    return size;
  }

  /** Write an unsigned varint and report the position after it. */
  private static int writeVarint(final byte[] target, final int offset, final int value) {
    int pos = offset;
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      target[pos++] = (byte) ((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    target[pos++] = (byte) remaining;
    return pos;
  }

  /** Read an unsigned varint. */
  private static int readVarint(final byte[] src, final int offset) {
    int value = 0;
    int shift = 0;
    int pos = offset;
    while (true) {
      final byte b = src[pos++];
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
  }

  /**
   * Offset one past a run-length lane, i.e. where its override stream begins.
   *
   * <p>
   * A fixed-width lane's length is a function of the slot count, a run lane's is not — so it is
   * walked once, over at most a few dozen runs, rather than spending bytes on a length prefix.
   */
  private static int runLaneEnd(final byte[] src, final int laneStart) {
    int pos = laneStart;
    int runs = readVarint(src, pos);
    pos += varintSize(runs);
    while (runs-- > 0) {
      pos++; // the run's code
      pos += varintSize(readVarint(src, pos));
    }
    return pos;
  }

  /**
   * Random access decode of a single slot from a byte-array-encoded column. Worst case
   * {@code O(slotIndex)}; amortized O(1) for scan-then-decode.
   *
   * @throws IllegalStateException if the column is {@code FLAG_NODEKEY_PREDICTED}, which needs the
   *         node keys — use {@link #decodeSlot(byte[], int, int, long[])} for those
   */
  public static long decodeSlot(final byte[] src, final int columnOffset, final int slotIndex) {
    return decodeSlot(src, columnOffset, slotIndex, null);
  }

  /**
   * Random access decode of a single slot, with the node keys the {@code FLAG_NODEKEY_PREDICTED}
   * format needs as predictor context.
   *
   * @param nodeKeys node key of each slot, parallel to the encoded column; may be {@code null} for
   *        columns in any of the other formats
   */
  public static long decodeSlot(final byte[] src, final int columnOffset, final int slotIndex, final long[] nodeKeys) {
    final int tag = src[columnOffset] & 0xFF;
    final int n = readUnsignedShort(src, columnOffset + 1);
    if (slotIndex < 0 || slotIndex >= n) {
      throw new IndexOutOfBoundsException("slotIndex " + slotIndex + " out of [0," + n + ")");
    }
    if (tag == FLAG_ALL_NULL) {
      return NULL;
    }
    if (tag == FLAG_CONSTANT) {
      return readLong(src, columnOffset + 3);
    }
    if (tag == FLAG_SEQUENTIAL_PLUS1) {
      return readLong(src, columnOffset + 3) + slotIndex;
    }
    final boolean runLane = (tag & FLAG_LANE_RUN_LENGTH) != 0;
    if ((tag & FORMAT_MASK) == FLAG_HAS_BITMAP) {
      final int laneStart = columnOffset + 3;
      int readPos = runLane
          ? runLaneEnd(src, laneStart)
          : laneStart + ((n + 7) >>> 3);
      int runPos = laneStart;
      int runsLeft = 0;
      int runCode = 0;
      int runRemaining = 0;
      if (runLane) {
        runsLeft = readVarint(src, runPos);
        runPos += varintSize(runsLeft);
      }
      long predictor = NULL;
      for (int i = 0; i <= slotIndex; i++) {
        final int bit;
        if (runLane) {
          if (runRemaining == 0) {
            if (runsLeft-- <= 0) {
              throw new IllegalStateException("run-length lane ended at slot " + i + " of " + n);
            }
            runCode = src[runPos++] & 0xFF;
            runRemaining = readVarint(src, runPos);
            runPos += varintSize(runRemaining);
          }
          runRemaining--;
          bit = runCode;
        } else {
          bit = (src[laneStart + (i >>> 3)] >>> (i & 7)) & 1;
        }
        final long value;
        if (bit == 1) {
          value = predictor;
        } else {
          final long delta = readZigzagVarintFromBytes(src, readPos);
          readPos += zigzagVarintSize(delta);
          value = predictor + delta;
        }
        if (i == slotIndex) {
          return value;
        }
        predictor = value;
      }
    }
    if ((tag & FORMAT_MASK) == FLAG_NODEKEY_PREDICTED) {
      requireNodeKeys(nodeKeys, n);
      final long stride = readZigzagVarintFromBytes(src, columnOffset + 3);
      final int codesStart = columnOffset + 3 + zigzagVarintSize(stride);
      int readPos = runLane
          ? runLaneEnd(src, codesStart)
          : codesStart + codeBytes(n);
      int runPos = codesStart;
      int runsLeft = 0;
      int runCode = 0;
      int runRemaining = 0;
      if (runLane) {
        runsLeft = readVarint(src, runPos);
        runPos += varintSize(runsLeft);
      }
      long previous = NULL;
      for (int i = 0; i <= slotIndex; i++) {
        final int code;
        if (runLane) {
          if (runRemaining == 0) {
            if (runsLeft-- <= 0) {
              throw new IllegalStateException("run-length lane ended at slot " + i + " of " + n);
            }
            runCode = src[runPos++] & 0xFF;
            runRemaining = readVarint(src, runPos);
            runPos += varintSize(runRemaining);
          }
          runRemaining--;
          code = runCode;
        } else {
          code = (src[codesStart + (i >>> 2)] >>> ((i & 3) << 1)) & 3;
        }
        final long value;
        if (code == CODE_NULL) {
          value = NULL;
        } else if (code == CODE_STRIDE) {
          value = nodeKeys[i] + stride;
        } else if (code == CODE_PREVIOUS) {
          value = previous;
        } else {
          final long delta = readZigzagVarintFromBytes(src, readPos);
          readPos += zigzagVarintSize(delta);
          value = nodeKeys[i] + delta;
        }
        if (i == slotIndex) {
          return value;
        }
        previous = value;
      }
    }
    throw new IllegalStateException("Unknown column format tag: 0x" + Integer.toHexString(tag));
  }

  /**
   * Decode every slot of a byte-array-encoded column in a single linear pass.
   *
   * <p>
   * {@link #decodeSlot} restarts the override-stream walk from slot 0 on every call, so decoding a
   * whole column slot by slot costs {@code O(N^2)} varint reads. A page holds up to 1024 slots and a
   * deserialize touches every one of them, which is the only way this codec is consumed on the read
   * path — so the bulk form is what callers should reach for. Random access stays available for the
   * (currently hypothetical) point-lookup caller.
   *
   * @param src encoded column bytes
   * @param columnOffset offset of the column's tag byte within {@code src}
   * @param out destination for the decoded values; must hold at least the column's slot count
   * @return the number of slots decoded, i.e. the slot count in the column header
   * @throws IllegalStateException if the column is {@code FLAG_NODEKEY_PREDICTED}, which needs the
   *         node keys — use {@link #decodeAll(byte[], int, long[], long[])} for those
   */
  public static int decodeAll(final byte[] src, final int columnOffset, final long[] out) {
    return decodeAll(src, columnOffset, out, null);
  }

  /**
   * Decode every slot in a single linear pass, with the node keys the {@code FLAG_NODEKEY_PREDICTED}
   * format needs as predictor context.
   *
   * @param nodeKeys node key of each slot, parallel to {@code out}; may be {@code null} for columns
   *        in any of the other formats
   */
  public static int decodeAll(final byte[] src, final int columnOffset, final long[] out, final long[] nodeKeys) {
    final int tag = src[columnOffset] & 0xFF;
    final int n = readUnsignedShort(src, columnOffset + 1);
    if (out.length < n) {
      throw new IllegalArgumentException("out too small: " + out.length + " < " + n);
    }
    if (n == 0) {
      return 0;
    }
    switch (tag) {
      case FLAG_ALL_NULL -> {
        for (int i = 0; i < n; i++) {
          out[i] = NULL;
        }
      }
      case FLAG_CONSTANT -> {
        final long v = readLong(src, columnOffset + 3);
        for (int i = 0; i < n; i++) {
          out[i] = v;
        }
      }
      case FLAG_SEQUENTIAL_PLUS1 -> {
        final long base = readLong(src, columnOffset + 3);
        for (int i = 0; i < n; i++) {
          out[i] = base + i;
        }
      }
      case FLAG_HAS_BITMAP, FLAG_HAS_BITMAP | FLAG_LANE_RUN_LENGTH -> {
        final int bitmapStart = columnOffset + 3;
        final boolean runLane = (tag & FLAG_LANE_RUN_LENGTH) != 0;
        int readPos = runLane
            ? runLaneEnd(src, bitmapStart)
            : bitmapStart + ((n + 7) >>> 3);
        int runPos = bitmapStart;
        int runsLeft = 0;
        int runCode = 0;
        int runRemaining = 0;
        if (runLane) {
          runsLeft = readVarint(src, runPos);
          runPos += varintSize(runsLeft);
        }
        long predictor = NULL;
        for (int i = 0; i < n; i++) {
          final int bit;
          if (runLane) {
            if (runRemaining == 0) {
              if (runsLeft-- <= 0) {
                throw new IllegalStateException("run-length lane ended at slot " + i + " of " + n);
              }
              runCode = src[runPos++] & 0xFF;
              runRemaining = readVarint(src, runPos);
              runPos += varintSize(runRemaining);
            }
            runRemaining--;
            bit = runCode;
          } else {
            bit = (src[bitmapStart + (i >>> 3)] >>> (i & 7)) & 1;
          }
          if (bit == 0) {
            // Inlined zig-zag varint read: the shared helper would need a second
            // pass over the same bytes to report how far it advanced.
            long zz = 0;
            int shift = 0;
            byte b;
            do {
              b = src[readPos++];
              zz |= ((long) (b & 0x7F)) << shift;
              shift += 7;
            } while ((b & 0x80) != 0);
            predictor += (zz >>> 1) ^ -(zz & 1);
          }
          out[i] = predictor;
        }
      }
      case FLAG_NODEKEY_PREDICTED, FLAG_NODEKEY_PREDICTED | FLAG_LANE_RUN_LENGTH -> {
        requireNodeKeys(nodeKeys, n);
        final boolean runLane = (tag & FLAG_LANE_RUN_LENGTH) != 0;
        final long stride = readZigzagVarintFromBytes(src, columnOffset + 3);
        final int codesStart = columnOffset + 3 + zigzagVarintSize(stride);
        int readPos = runLane
            ? runLaneEnd(src, codesStart)
            : codesStart + codeBytes(n);
        int runPos = codesStart;
        int runsLeft = 0;
        int runCode = 0;
        int runRemaining = 0;
        if (runLane) {
          runsLeft = readVarint(src, runPos);
          runPos += varintSize(runsLeft);
        }
        long previous = NULL;
        for (int i = 0; i < n; i++) {
          final int code;
          if (runLane) {
            if (runRemaining == 0) {
              if (runsLeft-- <= 0) {
                throw new IllegalStateException("run-length lane ended at slot " + i + " of " + n);
              }
              runCode = src[runPos++] & 0xFF;
              runRemaining = readVarint(src, runPos);
              runPos += varintSize(runRemaining);
            }
            runRemaining--;
            code = runCode;
          } else {
            code = (src[codesStart + (i >>> 2)] >>> ((i & 3) << 1)) & 3;
          }
          if (code == CODE_NULL) {
            previous = NULL;
          } else if (code == CODE_STRIDE) {
            previous = nodeKeys[i] + stride;
          } else if (code == CODE_EXPLICIT) {
            // Inlined zig-zag varint read: the shared helper would need a second
            // pass over the same bytes to report how far it advanced.
            long zz = 0;
            int shift = 0;
            byte b;
            do {
              b = src[readPos++];
              zz |= ((long) (b & 0x7F)) << shift;
              shift += 7;
            } while ((b & 0x80) != 0);
            previous = nodeKeys[i] + ((zz >>> 1) ^ -(zz & 1));
          }
          // CODE_PREVIOUS leaves `previous` alone, which is exactly its meaning.
          out[i] = previous;
        }
      }
      default -> throw new IllegalStateException("Unknown column format tag: 0x" + Integer.toHexString(tag));
    }
    return n;
  }

  private static void requireNodeKeys(final long[] nodeKeys, final int n) {
    if (nodeKeys == null) {
      throw new IllegalStateException("FLAG_NODEKEY_PREDICTED column needs node keys, none were supplied");
    }
    if (nodeKeys.length < n) {
      throw new IllegalStateException("nodeKeys too short for column: " + nodeKeys.length + " < " + n);
    }
  }

  // ==================== MemorySegment variants ====================

  /**
   * Encode directly into a {@link MemorySegment}. Same layout as the byte-array variant.
   */
  public static int encode(final MemorySegment target, final long offset, final long[] values) {
    final int n = values.length;
    if (n > MAX_SLOTS) {
      throw new IllegalArgumentException("Column too large: " + n + " > " + MAX_SLOTS);
    }

    if (n == 0) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) 0);
        putUnsignedShort(target, offset + 1, 0);
      }
      return 3;
    }

    boolean allNull = true;
    boolean constant = true;
    boolean monotonic = true;
    final long v0 = values[0];
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != NULL)
        allNull = false;
      if (v != v0)
        constant = false;
      if (v != v0 + i)
        monotonic = false;
    }

    if (allNull) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_ALL_NULL);
        putUnsignedShort(target, offset + 1, n);
      }
      return 3;
    }
    if (constant) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_CONSTANT);
        putUnsignedShort(target, offset + 1, n);
        putLong(target, offset + 3, v0);
      }
      return 11;
    }
    if (monotonic) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_SEQUENTIAL_PLUS1);
        putUnsignedShort(target, offset + 1, n);
        putLong(target, offset + 3, v0);
      }
      return 11;
    }

    final int bitmapBytes = (n + 7) >>> 3;
    int overrideBytes = 0;
    long predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != predictor) {
        overrideBytes += zigzagVarintSize(v - predictor);
      }
      predictor = v;
    }
    final int totalBytes = 1 + 2 + bitmapBytes + overrideBytes;
    if (target == null) {
      return totalBytes;
    }

    target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_HAS_BITMAP);
    putUnsignedShort(target, offset + 1, n);
    for (int i = 0; i < bitmapBytes; i++) {
      target.set(ValueLayout.JAVA_BYTE, offset + 3 + i, (byte) 0);
    }

    long writePos = offset + 3 + bitmapBytes;
    predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v == predictor) {
        final long bmByte = offset + 3 + (i >>> 3);
        final int cur = target.get(ValueLayout.JAVA_BYTE, bmByte) & 0xFF;
        target.set(ValueLayout.JAVA_BYTE, bmByte, (byte) (cur | (1 << (i & 7))));
      } else {
        writePos = writeZigzagVarint(target, writePos, v - predictor);
      }
      predictor = v;
    }
    return totalBytes;
  }

  /** Random-access decode of a single slot from a MemorySegment-encoded column. */
  public static long decodeSlot(final MemorySegment src, final long columnOffset, final int slotIndex) {
    final int tag = src.get(ValueLayout.JAVA_BYTE, columnOffset) & 0xFF;
    final int n = getUnsignedShort(src, columnOffset + 1);
    if (slotIndex < 0 || slotIndex >= n) {
      throw new IndexOutOfBoundsException("slotIndex " + slotIndex + " out of [0," + n + ")");
    }
    if (tag == FLAG_ALL_NULL)
      return NULL;
    if (tag == FLAG_CONSTANT)
      return getLong(src, columnOffset + 3);
    if (tag == FLAG_SEQUENTIAL_PLUS1)
      return getLong(src, columnOffset + 3) + slotIndex;
    if (tag == FLAG_HAS_BITMAP) {
      final int bitmapBytes = (n + 7) >>> 3;
      long readPos = columnOffset + 3 + bitmapBytes;
      long predictor = NULL;
      for (int i = 0; i <= slotIndex; i++) {
        final int bit = (src.get(ValueLayout.JAVA_BYTE, columnOffset + 3 + (i >>> 3)) >>> (i & 7)) & 1;
        final long value;
        if (bit == 1) {
          value = predictor;
        } else {
          final long delta = readZigzagVarint(src, readPos);
          readPos += zigzagVarintSize(delta);
          value = predictor + delta;
        }
        if (i == slotIndex)
          return value;
        predictor = value;
      }
    }
    throw new IllegalStateException("Unknown column format tag: 0x" + Integer.toHexString(tag));
  }

  // ==================== Helpers ====================

  private static int zigzagVarintSize(final long v) {
    final long zz = (v << 1) ^ (v >> 63);
    if (zz == 0)
      return 1;
    // Number of 7-bit groups needed.
    final int bits = 64 - Long.numberOfLeadingZeros(zz);
    return (bits + 6) / 7;
  }

  private static int writeZigzagVarintToBytes(final byte[] target, final int offset, final long v) {
    long zz = (v << 1) ^ (v >> 63);
    int pos = offset;
    while ((zz & ~0x7FL) != 0L) {
      target[pos++] = (byte) (zz | 0x80L);
      zz >>>= 7;
    }
    target[pos++] = (byte) zz;
    return pos;
  }

  private static long readZigzagVarintFromBytes(final byte[] src, final int offset) {
    long zz = 0;
    int shift = 0;
    int pos = offset;
    while (true) {
      final byte b = src[pos++];
      zz |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0)
        break;
      shift += 7;
    }
    return (zz >>> 1) ^ -(zz & 1);
  }

  private static long writeZigzagVarint(final MemorySegment target, final long offset, final long v) {
    long zz = (v << 1) ^ (v >> 63);
    long pos = offset;
    while ((zz & ~0x7FL) != 0L) {
      target.set(ValueLayout.JAVA_BYTE, pos++, (byte) (zz | 0x80L));
      zz >>>= 7;
    }
    target.set(ValueLayout.JAVA_BYTE, pos++, (byte) zz);
    return pos;
  }

  private static long readZigzagVarint(final MemorySegment src, final long offset) {
    long zz = 0;
    int shift = 0;
    long pos = offset;
    while (true) {
      final byte b = src.get(ValueLayout.JAVA_BYTE, pos++);
      zz |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0)
        break;
      shift += 7;
    }
    return (zz >>> 1) ^ -(zz & 1);
  }

  private static void writeUnsignedShort(final byte[] target, final int offset, final int v) {
    target[offset] = (byte) ((v >>> 8) & 0xFF);
    target[offset + 1] = (byte) (v & 0xFF);
  }

  private static int readUnsignedShort(final byte[] src, final int offset) {
    return ((src[offset] & 0xFF) << 8) | (src[offset + 1] & 0xFF);
  }

  private static void writeLong(final byte[] target, final int offset, final long v) {
    for (int i = 0; i < 8; i++) {
      target[offset + i] = (byte) (v >>> (56 - 8 * i));
    }
  }

  private static long readLong(final byte[] src, final int offset) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (src[offset + i] & 0xFF);
    }
    return v;
  }

  private static void putUnsignedShort(final MemorySegment target, final long offset, final int v) {
    target.set(ValueLayout.JAVA_BYTE, offset, (byte) ((v >>> 8) & 0xFF));
    target.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) (v & 0xFF));
  }

  private static int getUnsignedShort(final MemorySegment src, final long offset) {
    return ((src.get(ValueLayout.JAVA_BYTE, offset) & 0xFF) << 8) | (src.get(ValueLayout.JAVA_BYTE, offset + 1) & 0xFF);
  }

  private static void putLong(final MemorySegment target, final long offset, final long v) {
    for (int i = 0; i < 8; i++) {
      target.set(ValueLayout.JAVA_BYTE, offset + i, (byte) (v >>> (56 - 8 * i)));
    }
  }

  private static long getLong(final MemorySegment src, final long offset) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (src.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFF);
    }
    return v;
  }
}

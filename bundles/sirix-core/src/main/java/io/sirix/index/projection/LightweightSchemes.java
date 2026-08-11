/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * One Value and Run-Length Encoding — two of the schemes in BtrBlocks Table 1 that our region and
 * projection encoders did not have.
 *
 * <h2>Why these two first</h2>
 *
 * <p>Both are the paper's cheapest schemes and both hit distributions we actually store. A
 * projection column of per-row set CARDINALITIES is the clearest case: nearly every record carries
 * the same number of elements, so the column is one value with a handful of exceptions, and
 * bit-packing still spends its width on every row. Sorted or clustered scalar columns — a year
 * beside records inserted in year order — are RLE's case.
 *
 * <p>Neither is a general win. RLE stores a length beside every value, so on data without runs it
 * is strictly LARGER than the input, which is why BtrBlocks excludes it from estimation when the
 * average run is below 2 rather than letting the sample discover it. That rule lives in
 * {@link ProjectionSchemePool}.
 *
 * <h2>Scanning, not just decompressing</h2>
 *
 * <p>BtrBlocks optimises DECOMPRESSION; our fast paths try never to decompress at all, evaluating
 * predicates over the encoded bytes. The two aims agree for One Value and disagree for RLE:
 *
 * <ul>
 *   <li>One Value makes scanning strictly cheaper — one comparison decides the whole block, which
 *       is a zone map that happens to also be the data.</li>
 *   <li>RLE makes a positional scan HARDER: the run at index {@code i} is only known by walking the
 *       lengths. A predicate can be evaluated per RUN rather than per value, which is a large win
 *       when runs are long, but it is a different kernel, not the existing one.</li>
 * </ul>
 *
 * <p>So the ratio these buy is not automatically speed, and the selector must not be given RLE for
 * a column whose scan has no run-aware kernel. Recorded here because the paper's headline numbers
 * are decompression-bound and ours are scan-bound, and that difference decides which of the two
 * schemes is worth taking.
 */
public final class LightweightSchemes {

  private LightweightSchemes() {
  }

  // ───────────────────────────────────────────────────────────── One Value

  /**
   * Encoded size of a constant block: the value, and nothing per row.
   *
   * @return bytes, or {@code -1} when the block is not constant
   */
  public static long oneValueBytes(final long[] values, final int count) {
    if (values == null || count <= 0) {
      return -1;
    }
    final long first = values[0];
    for (int i = 1; i < count; i++) {
      if (values[i] != first) {
        return -1;
      }
    }
    return 8;   // just the value
  }

  /** Write a constant block: the single value. */
  public static void encodeOneValue(final ByteArrayOutputStream out, final long value) {
    putLongLE(out, value);
  }

  // ───────────────────────────────────────────────────────────── RLE

  /** Number of runs in {@code values[0..count)}; {@code 0} for an empty block. */
  public static int runCount(final long[] values, final int count) {
    if (values == null || count <= 0) {
      return 0;
    }
    int runs = 1;
    for (int i = 1; i < count; i++) {
      if (values[i] != values[i - 1]) {
        runs++;
      }
    }
    return runs;
  }

  /**
   * Encoded size of the RLE form: a bit-packed value per run plus a bit-packed length per run.
   *
   * <p>Sized the way the encoder writes it rather than as {@code runs * 16}: both arrays are packed
   * to the width their own maximum needs, and on the columns RLE is chosen for — few distinct
   * values, long runs — that is several times smaller than a byte-aligned estimate. Estimating high
   * here would make the selector prefer bit-packing on data where RLE genuinely wins, which is the
   * failure mode the paper's sample-and-measure approach exists to avoid.
   */
  public static long rleBytes(final long[] values, final int count) {
    final int runs = runCount(values, count);
    if (runs <= 0) {
      return -1;
    }
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    int maxRun = 0;
    int current = 1;
    for (int i = 0; i < count; i++) {
      final long v = values[i];
      if (v < minValue) {
        minValue = v;
      }
      if (v > maxValue) {
        maxValue = v;
      }
      if (i > 0 && v == values[i - 1]) {
        current++;
      } else {
        if (current > maxRun) {
          maxRun = current;
        }
        current = 1;
      }
    }
    if (current > maxRun) {
      maxRun = current;
    }
    // Frame of reference on the values, so the width follows the spread rather than the magnitude.
    final long valueRange = maxValue - minValue;
    if (valueRange < 0) {
      return -1;   // spread wider than an unsigned 64-bit delta — see ProjectionSchemePool
    }
    final int valueWidth = widthOf(valueRange);
    final int lengthWidth = widthOf(maxRun);
    final long valueBits = (long) runs * valueWidth;
    final long lengthBits = (long) runs * lengthWidth;
    return 8 /* reference */ + 4 /* runs */ + 2 /* widths */
        + (valueBits + 7) / 8 + (lengthBits + 7) / 8;
  }

  /**
   * Expand an RLE block back into values.
   *
   * @param runValues one value per run
   * @param runLengths one length per run, same order
   * @param out receives the expanded values; must hold their sum
   * @return values written, or {@code -1} if {@code out} is too small
   */
  public static int decodeRle(final long[] runValues, final int[] runLengths, final int runs,
      final long[] out) {
    if (runValues == null || runLengths == null || out == null || runs < 0) {
      return -1;
    }
    int at = 0;
    for (int r = 0; r < runs; r++) {
      final int length = runLengths[r];
      if (length < 0 || length > out.length - at) {
        return -1;
      }
      Arrays.fill(out, at, at + length, runValues[r]);
      at += length;
    }
    return at;
  }

  /** Bits needed for {@code 0..maxValue}; {@code 0} when everything is zero. */
  static int widthOf(final long maxValue) {
    return maxValue <= 0 ? 0 : 64 - Long.numberOfLeadingZeros(maxValue);
  }

  private static void putLongLE(final ByteArrayOutputStream out, final long v) {
    for (int i = 0; i < 8; i++) {
      out.write((int) ((v >>> (i * 8)) & 0xFF));
    }
  }
}

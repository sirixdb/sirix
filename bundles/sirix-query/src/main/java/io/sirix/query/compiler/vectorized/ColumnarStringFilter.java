package io.sirix.query.compiler.vectorized;

import io.sirix.utils.FSSTCompressor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * SIMD-optimized string equality filter for {@link ColumnBatch} DEFERRED_BYTES columns.
 *
 * <p>Key optimization: the search constant is pre-encoded with each page's FSST symbol table
 * so comparison operates on compressed bytes directly — no decompression needed for
 * non-matching rows. This is correct because FSST encoding is deterministic per symbol table:
 * same input always produces the same compressed output when encoded with the same table.</p>
 *
 * <p>For rows where:
 * <ul>
 *   <li>The page has no FSST table (uncompressed): raw UTF-8 byte comparison</li>
 *   <li>The row itself is uncompressed: raw UTF-8 byte comparison</li>
 *   <li>FSST table differs or encoding fails: fall back to decompress-then-compare</li>
 * </ul></p>
 *
 * <p>Updates the batch's selection vector in-place. Chain with other filters
 * for conjunctive predicates.</p>
 */
public final class ColumnarStringFilter {

  private ColumnarStringFilter() {}

  /**
   * Filter a DEFERRED_BYTES column for string equality.
   *
   * @param batch    the batch to filter (selection vector modified in-place)
   * @param col      the column index (must be DEFERRED_BYTES type)
   * @param constant the string constant to compare against
   */
  public static void filterStringEqual(final ColumnBatch batch, final int col,
      final String constant) {
    filterString(batch, col, constant, true);
  }

  /**
   * Filter a DEFERRED_BYTES column for string inequality (not equal).
   *
   * @param batch    the batch to filter
   * @param col      the column index (must be DEFERRED_BYTES type)
   * @param constant the string constant to compare against
   */
  public static void filterStringNotEqual(final ColumnBatch batch, final int col,
      final String constant) {
    filterString(batch, col, constant, false);
  }

  /**
   * Core string filter implementation shared by equality and inequality.
   *
   * @param batch      the batch to filter
   * @param col        column index (DEFERRED_BYTES)
   * @param constant   string constant to compare against
   * @param keepIfEqual if true, keep rows that match; if false, keep rows that don't match
   */
  private static void filterString(final ColumnBatch batch, final int col,
      final String constant, final boolean keepIfEqual) {
    Objects.requireNonNull(batch, "batch");
    Objects.requireNonNull(constant, "constant");

    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    final boolean[] nulls = batch.nullFlags(col);
    final int[] offsets = batch.deferredOffsets(col);
    final int[] lengths = batch.deferredLengths(col);
    final boolean[] isCompressed = batch.deferredCompressed(col);
    final int[] pageIndices = batch.deferredPageIndices(col);
    final List<MemorySegment> pagesList = batch.backingPages(col);
    final List<byte[][]> symbolsByPageList = batch.parsedFsstSymbolsByPage(col);

    final byte[] constantUtf8 = constant.getBytes(StandardCharsets.UTF_8);
    // Hoist MemorySegment wrapper for the raw constant — avoid per-row allocation
    final MemorySegment constantSeg = MemorySegment.ofArray(constantUtf8);

    // Convert to arrays for direct access in hot loop (avoid List.get() virtual dispatch)
    final int pageCount = pagesList.size();
    final MemorySegment[] pages = pagesList.toArray(new MemorySegment[pageCount]);
    final byte[][][] symbolsByPage = symbolsByPageList.toArray(new byte[pageCount][][]);

    // Pre-encode constant with each page's FSST table + wrap as MemorySegment
    final byte[][] encodedByPage = new byte[pageCount][];
    final MemorySegment[] encodedSegByPage = new MemorySegment[pageCount];
    for (int p = 0; p < pageCount; p++) {
      final byte[][] symbols = symbolsByPage[p];
      if (symbols != null && symbols.length > 0) {
        encodedByPage[p] = FSSTCompressor.encode(constantUtf8, symbols);
        encodedSegByPage[p] = MemorySegment.ofArray(encodedByPage[p]);
      }
    }

    int outPos = 0;
    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];

      if (nulls[row]) {
        continue; // null excluded from both equality and inequality
      }

      final int pgIdx = pageIndices[row];
      final MemorySegment page = pages[pgIdx];
      final int offset = offsets[row];
      final int length = lengths[row];

      final boolean isEqual;
      if (isCompressed[row]) {
        final byte[] encoded = encodedByPage[pgIdx];
        if (encoded != null) {
          isEqual = (length == encoded.length)
              && segmentBytesEqual(page, offset, encodedSegByPage[pgIdx], length);
        } else {
          // No FSST table for this page — fall back to decompress-then-compare
          isEqual = decompressAndCompare(page, offset, length,
              symbolsByPage[pgIdx], constantUtf8, constantSeg);
        }
      } else {
        // Uncompressed row: direct UTF-8 byte comparison
        isEqual = (length == constantUtf8.length)
            && segmentBytesEqual(page, offset, constantSeg, length);
      }

      if (isEqual == keepIfEqual) {
        sel[outPos++] = row;
      }
    }

    batch.setSelectionCount(outPos);
  }

  /**
   * Compare bytes in a MemorySegment against a pre-wrapped MemorySegment constant.
   * Uses {@link MemorySegment#mismatch} for SIMD-accelerated comparison.
   */
  private static boolean segmentBytesEqual(final MemorySegment page,
      final int offset, final MemorySegment expected, final int length) {
    final MemorySegment slice = page.asSlice(offset, length);
    return slice.mismatch(expected) == -1;
  }

  /**
   * Decompress FSST bytes and compare against the constant.
   * When no symbol table exists, compares directly via MemorySegment (no allocation).
   */
  private static boolean decompressAndCompare(final MemorySegment page,
      final int offset, final int length, final byte[][] symbols,
      final byte[] constantUtf8, final MemorySegment constantSeg) {
    if (symbols == null || symbols.length == 0) {
      // No compression — compare directly from the page segment
      if (length != constantUtf8.length) {
        return false;
      }
      final MemorySegment slice = page.asSlice(offset, length);
      return slice.mismatch(constantSeg) == -1;
    }

    final byte[] raw = new byte[length];
    MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offset, raw, 0, length);
    final byte[] decoded = FSSTCompressor.decode(raw, symbols);

    if (decoded.length != constantUtf8.length) {
      return false;
    }
    return Arrays.equals(decoded, constantUtf8);
  }
}

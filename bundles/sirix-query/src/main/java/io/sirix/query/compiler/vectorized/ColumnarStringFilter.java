package io.sirix.query.compiler.vectorized;

import io.sirix.page.pax.StringRegion;
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

    // QuestDB-style per-page symbol-table fast path.
    // For each backing page that exposes a StringRegion, decide whether the
    // literal can possibly appear on the page by probing every tag's local
    // dictionary. {@code stringRegionLiteralAbsent[p] == true} means the
    // literal is not present in any tag's per-page dict, so:
    //   - EQ: every row from page p is non-matching (skip without byte-compare)
    //   - NE: every non-null row from page p is matching (keep without byte-compare)
    //
    // Correctness guard: the StringRegion is built only from fused OBJECT_NAMED_STRING
    // slots. If the page also has raw STRING_VALUE rows in the batch, the region
    // does not cover them and the short-circuit would be unsafe. We compare the
    // region's encoded {@code count} against the number of input rows the batch
    // contributes from page p; if they match, every row is fused-and-covered,
    // making the short-circuit safe. Otherwise the page falls back to FSST.
    final boolean[] stringRegionLiteralAbsent = new boolean[pageCount];
    final int[] perPageRowCount = countRowsPerPage(sel, inputCount, pageIndices, nulls, pageCount);
    for (int p = 0; p < pageCount; p++) {
      final StringRegion.Header h = batch.stringRegionHeader(col, p);
      if (h == null) {
        continue;
      }
      final byte[] payload = batch.stringRegionPayload(col, p);
      if (payload == null) {
        continue;
      }
      // Region must cover every row this batch contributes from page p.
      if (h.count != perPageRowCount[p]) {
        continue;
      }
      stringRegionLiteralAbsent[p] = !literalInAnyTag(h, payload, constantUtf8);
    }

    // Pre-encode constant with each page's FSST table + wrap as MemorySegment.
    // FSSTCompressor.encode() prepends a 1-byte header (HEADER_COMPRESSED or HEADER_RAW),
    // but ColumnarPageExtractor extracts payload offsets pointing past the page's own
    // compression header — just the raw compressed bytes. We must strip the FSSTCompressor
    // header byte before comparison.
    final byte[][] encodedByPage = new byte[pageCount][];
    final MemorySegment[] encodedSegByPage = new MemorySegment[pageCount];
    for (int p = 0; p < pageCount; p++) {
      // Skip FSST encode for pages where the StringRegion already proved the literal absent;
      // their rows take the short-circuit branch below without ever touching encodedByPage.
      if (stringRegionLiteralAbsent[p]) {
        continue;
      }
      final byte[][] symbols = symbolsByPage[p];
      if (symbols != null && symbols.length > 0) {
        final byte[] withHeader = FSSTCompressor.encode(constantUtf8, symbols);
        if (withHeader.length > 1 && withHeader[0] == FSSTCompressor.HEADER_COMPRESSED) {
          // Strip the 1-byte header — page data contains only the compressed payload
          final byte[] stripped = new byte[withHeader.length - 1];
          System.arraycopy(withHeader, 1, stripped, 0, stripped.length);
          encodedByPage[p] = stripped;
          encodedSegByPage[p] = MemorySegment.ofArray(stripped);
        } else {
          // Encoding returned raw (HEADER_RAW or short input fallback).
          // On the page, uncompressed rows are compared via the raw UTF-8 path,
          // so null out encodedByPage to force the fallback decompress-then-compare
          // path for compressed rows whose FSST table produced no compression benefit.
          encodedByPage[p] = null;
        }
      }
    }

    int outPos = 0;
    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];

      if (nulls[row]) {
        continue; // null excluded from both equality and inequality
      }

      final int pgIdx = pageIndices[row];

      // QuestDB-style fast path: literal proven absent on this page → answer
      // is statically known without touching the row's bytes.
      if (stringRegionLiteralAbsent[pgIdx]) {
        if (!keepIfEqual) {
          // NE: literal absent → row's value differs from literal → keep
          sel[outPos++] = row;
        }
        // EQ: literal absent → drop
        continue;
      }

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
   * QuestDB-style page-level Bloom probe: does the UTF-8 literal appear in
   * <em>any</em> tag's local dictionary on this StringRegion?
   *
   * <p>Returns {@code true} on the first hit; iterates {@code parentDictSize} tags
   * (≤16 typical) and within each tag the local dict (≤16 typical), all while
   * walking concatenated payload bytes — zero allocations.
   */
  private static boolean literalInAnyTag(final StringRegion.Header h, final byte[] payload,
      final byte[] literal) {
    final int litLen = literal.length;
    final int tagCount = h.parentDictSize;
    for (int t = 0; t < tagCount; t++) {
      if (StringRegion.lookupDictId(payload, h, t, literal, 0, litLen) >= 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Per-page non-null row count over the active selection. Allocates a tiny
   * int[pageCount] (typically 1-8 entries — ~32 bytes); the savings from
   * skipping FSST encode + per-row byte compare on miss-pages dwarf the
   * allocation cost.
   */
  private static int[] countRowsPerPage(final int[] sel, final int inputCount,
      final int[] pageIndices, final boolean[] nulls, final int pageCount) {
    final int[] counts = new int[pageCount];
    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];
      if (!nulls[row]) {
        counts[pageIndices[row]]++;
      }
    }
    return counts;
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
    final byte[] decoded = FSSTCompressor.decodeRaw(raw, symbols);

    if (decoded.length != constantUtf8.length) {
      return false;
    }
    return Arrays.equals(decoded, constantUtf8);
  }
}

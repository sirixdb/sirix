package io.sirix.query.compiler.vectorized;

import io.sirix.utils.FSSTCompressor;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ColumnarStringFilter} — SIMD-optimized string equality filter
 * that operates on DEFERRED_BYTES columns with optional FSST compression.
 */
final class ColumnarStringFilterTest {

  // --- Uncompressed string tests ---

  @Test
  void filterEqualUncompressedStrings() {
    // Set up a batch with 4 uncompressed string values
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"Chicago", "New York", "Chicago", "Boston"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      final int len = values[i].getBytes(StandardCharsets.UTF_8).length;
      batch.setDeferredBytes(0, i, pgIdx, offset, len, false);
      offset += len + 16; // 16-byte gap between values
    }
    batch.setRowCount(4);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, "Chicago");

    assertEquals(2, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
  }

  @Test
  void filterNotEqualUncompressedStrings() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"Chicago", "New York", "Chicago", "Boston"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      final int len = values[i].getBytes(StandardCharsets.UTF_8).length;
      batch.setDeferredBytes(0, i, pgIdx, offset, len, false);
      offset += len + 16;
    }
    batch.setRowCount(4);
    batch.seal();

    ColumnarStringFilter.filterStringNotEqual(batch, 0, "Chicago");

    assertEquals(2, batch.selectionCount());
    assertEquals(1, batch.selectedRow(0));
    assertEquals(3, batch.selectedRow(1));
  }

  // --- FSST-compressed string tests ---

  @Test
  void filterEqualCompressedStrings() {
    // Build a symbol table from sample data and compress strings
    final String[] values = buildLongStrings("Chicago-IL-USA", "New-York-NY-USA", "Chicago-IL-USA", "Boston-MA-USA");
    final byte[][] rawValues = new byte[values.length][];
    for (int i = 0; i < values.length; i++) {
      rawValues[i] = values[i].getBytes(StandardCharsets.UTF_8);
    }

    // Build FSST symbol table from samples
    final List<byte[]> samples = buildSufficientSamples(rawValues);
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // If the symbol table is empty (not enough data for compression), fall back to
    // testing with uncompressed data marked as compressed + null symbols, which
    // exercises the decompressAndCompare fallback path.
    if (symbolTable.length == 0) {
      filterEqualCompressedStrings_fallbackPath(values, rawValues);
      return;
    }

    final byte[][] parsedSymbols = FSSTCompressor.parseSymbolTable(symbolTable);

    // Encode each value (with header), then strip header for page simulation
    final byte[][] compressed = new byte[values.length][];
    for (int i = 0; i < values.length; i++) {
      final byte[] withHeader = FSSTCompressor.encode(rawValues[i], parsedSymbols);
      if (withHeader[0] == FSSTCompressor.HEADER_COMPRESSED) {
        compressed[i] = new byte[withHeader.length - 1];
        System.arraycopy(withHeader, 1, compressed[i], 0, compressed[i].length);
      } else {
        // Not compressed — use raw bytes and mark as uncompressed
        compressed[i] = rawValues[i];
      }
    }

    // Build page with stripped compressed data
    final byte[] pageData = buildPageFromByteArrays(compressed);
    final MemorySegment page = MemorySegment.ofArray(pageData);

    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, page, parsedSymbols);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      final byte[] withHeader = FSSTCompressor.encode(rawValues[i], parsedSymbols);
      final boolean isCompressed = withHeader[0] == FSSTCompressor.HEADER_COMPRESSED;
      batch.setDeferredBytes(0, i, pgIdx, offset, compressed[i].length, isCompressed);
      offset += compressed[i].length + 16;
    }
    batch.setRowCount(values.length);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, values[0]);

    // values[0] and values[2] are both "Chicago-IL-USA..." repeated
    assertEquals(2, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
  }

  /**
   * Fallback test when FSST compression doesn't produce a symbol table
   * (not enough data). Tests the decompressAndCompare path with null symbols.
   */
  private void filterEqualCompressedStrings_fallbackPath(String[] values, byte[][] rawValues) {
    final byte[] pageData = buildPageFromByteArrays(rawValues);
    final MemorySegment page = MemorySegment.ofArray(pageData);

    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    // No symbols — this exercises the fallback in decompressAndCompare
    final int pgIdx = batch.addBackingPage(0, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offset, rawValues[i].length, false);
      offset += rawValues[i].length + 16;
    }
    batch.setRowCount(values.length);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, values[0]);

    assertEquals(2, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
  }

  // --- Length mismatch tests ---

  @Test
  void filterEqualRejectsLengthMismatch() {
    // All values have different lengths from the search constant
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"short", "longer value", "a"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      final int len = values[i].getBytes(StandardCharsets.UTF_8).length;
      batch.setDeferredBytes(0, i, pgIdx, offset, len, false);
      offset += len + 16;
    }
    batch.setRowCount(3);
    batch.seal();

    // Search for something whose length doesn't match any value
    ColumnarStringFilter.filterStringEqual(batch, 0, "no-match-at-all");

    assertEquals(0, batch.selectionCount());
  }

  // --- Null handling ---

  @Test
  void filterEqualSkipsNullValues() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"target", "other"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    // Row 0: "target"
    batch.setDeferredBytes(0, 0, pgIdx, 0,
        "target".getBytes(StandardCharsets.UTF_8).length, false);
    // Row 1: null
    batch.setNull(0, 1);
    // Row 2: "target" (same as row 0)
    batch.setDeferredBytes(0, 2, pgIdx, 0,
        "target".getBytes(StandardCharsets.UTF_8).length, false);
    batch.setRowCount(3);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, "target");

    // Null row should be excluded
    assertEquals(2, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
  }

  @Test
  void filterNotEqualAlsoSkipsNulls() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"target", "other"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    // Row 0: "target"
    batch.setDeferredBytes(0, 0, pgIdx, 0,
        "target".getBytes(StandardCharsets.UTF_8).length, false);
    // Row 1: null
    batch.setNull(0, 1);
    // Row 2: "other"
    final int otherOff = "target".getBytes(StandardCharsets.UTF_8).length + 16;
    batch.setDeferredBytes(0, 2, pgIdx, otherOff,
        "other".getBytes(StandardCharsets.UTF_8).length, false);
    batch.setRowCount(3);
    batch.seal();

    ColumnarStringFilter.filterStringNotEqual(batch, 0, "target");

    // Null excluded from both equality and inequality
    assertEquals(1, batch.selectionCount());
    assertEquals(2, batch.selectedRow(0));
  }

  // --- Empty batch ---

  @Test
  void filterEqualOnEmptyBatch() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    batch.setRowCount(0);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, "anything");

    assertEquals(0, batch.selectionCount());
  }

  // --- Selection vector compaction ---

  @Test
  void selectionVectorCompactionAfterFiltering() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final String[] values = {"alpha", "beta", "alpha", "gamma", "alpha"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(0, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      final int len = values[i].getBytes(StandardCharsets.UTF_8).length;
      batch.setDeferredBytes(0, i, pgIdx, offset, len, false);
      offset += len + 16;
    }
    batch.setRowCount(5);
    batch.seal();

    // First filter: keep "alpha" rows (0, 2, 4)
    ColumnarStringFilter.filterStringEqual(batch, 0, "alpha");
    assertEquals(3, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
    assertEquals(4, batch.selectedRow(2));

    // Second filter on already-compacted selection: filter for "alpha" again (all should pass)
    ColumnarStringFilter.filterStringEqual(batch, 0, "alpha");
    assertEquals(3, batch.selectionCount());

    // Third filter: filter for "beta" on compacted selection (none should pass)
    ColumnarStringFilter.filterStringEqual(batch, 0, "beta");
    assertEquals(0, batch.selectionCount());
  }

  @Test
  void chainedFiltersCompactSelection() {
    // Two-column batch: first filter narrows by INT64, then string filter on narrowed set
    final var batch = new ColumnBatch(8, 2);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setColumnType(1, ColumnType.DEFERRED_BYTES);

    final String[] values = {"Chicago", "New York", "Chicago", "Boston"};
    final byte[] pageData = buildUncompressedPage(values);
    final MemorySegment page = MemorySegment.ofArray(pageData);
    final int pgIdx = batch.addBackingPage(1, page, null);

    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      batch.setLong(0, i, (i + 1) * 100L);
      final int len = values[i].getBytes(StandardCharsets.UTF_8).length;
      batch.setDeferredBytes(1, i, pgIdx, offset, len, false);
      offset += len + 16;
    }
    batch.setRowCount(4);
    batch.seal();

    // Pre-compact: keep rows with ID > 100 (rows 1, 2, 3)
    final int[] sel = batch.selectionVector();
    sel[0] = 1;
    sel[1] = 2;
    sel[2] = 3;
    batch.setSelectionCount(3);

    // Now string filter on the compacted selection
    ColumnarStringFilter.filterStringEqual(batch, 1, "Chicago");

    // Only row 2 has "Chicago" AND was in the selection
    assertEquals(1, batch.selectionCount());
    assertEquals(2, batch.selectedRow(0));
  }

  // --- Helpers ---

  /**
   * Build a page byte array with uncompressed string values laid out with 16-byte gaps.
   */
  private byte[] buildUncompressedPage(String[] values) {
    int totalSize = 0;
    for (String v : values) {
      totalSize += v.getBytes(StandardCharsets.UTF_8).length + 16;
    }
    final byte[] pageData = new byte[totalSize + 64]; // extra padding

    int offset = 0;
    for (String v : values) {
      final byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(bytes, 0, pageData, offset, bytes.length);
      offset += bytes.length + 16;
    }
    return pageData;
  }

  /**
   * Build a page byte array from pre-encoded byte arrays with 16-byte gaps.
   */
  private byte[] buildPageFromByteArrays(byte[][] values) {
    int totalSize = 0;
    for (byte[] v : values) {
      totalSize += v.length + 16;
    }
    final byte[] pageData = new byte[totalSize + 64];

    int offset = 0;
    for (byte[] v : values) {
      System.arraycopy(v, 0, pageData, offset, v.length);
      offset += v.length + 16;
    }
    return pageData;
  }

  /**
   * Build long-enough strings for FSST compression (need >= MIN_COMPRESSION_SIZE = 32 bytes).
   * Pads each value by repeating it to exceed the minimum.
   */
  private String[] buildLongStrings(String... bases) {
    final String[] result = new String[bases.length];
    for (int i = 0; i < bases.length; i++) {
      final StringBuilder sb = new StringBuilder();
      while (sb.length() < 64) {
        sb.append(bases[i]).append('-');
      }
      result[i] = sb.toString();
    }
    return result;
  }

  /**
   * Build sufficient samples for FSST symbol table building.
   * Requires >= MIN_SAMPLES_FOR_TABLE (64) samples with >= MIN_TOTAL_BYTES_FOR_TABLE (4096) bytes.
   */
  private List<byte[]> buildSufficientSamples(byte[][] rawValues) {
    final List<byte[]> samples = new ArrayList<>();
    // Repeat the raw values enough times to meet the minimum requirements
    while (samples.size() < FSSTCompressor.MIN_SAMPLES_FOR_TABLE * 2) {
      for (byte[] v : rawValues) {
        samples.add(v);
      }
    }
    return samples;
  }
}

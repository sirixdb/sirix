package io.sirix.query.compiler.vectorized;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageGuard;
import io.sirix.page.ColumnarPageExtractor;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageScanIterator;
import io.sirix.utils.FSSTCompressor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Multi-page columnar scan that accumulates string-type nodes from sequential
 * {@link KeyValueLeafPage}s into {@link ColumnBatch}es.
 *
 * <p>Drives a {@link PageScanIterator} + {@link ColumnarPageExtractor} to extract
 * STRING_VALUE and OBJECT_STRING_VALUE nodes directly from slotted page MemorySegments.
 * Rows from multiple pages are accumulated into a single batch until a fill threshold
 * is reached, ensuring good SIMD utilization even when individual pages contain few strings.</p>
 *
 * <p>Guard management: all pages contributing to the current batch are guarded (reference-counted)
 * to prevent cache eviction. Guards are released when the next batch is requested or the axis
 * is closed.</p>
 *
 * <h3>Column layout</h3>
 * <ul>
 *   <li>Column 0: {@code INT64} — absolute node key</li>
 *   <li>Column 1: {@code INT64} — parent key (delta-decoded)</li>
 *   <li>Column 2: {@code DEFERRED_BYTES} — string value (late materialization)</li>
 *   <li>Column 3: {@code BYTES} — DeweyID (optional, null if not stored)</li>
 * </ul>
 */
public final class ColumnarScanAxis implements AutoCloseable {

  /** Column indices for the output batch. */
  public static final int COL_NODE_KEY = 0;
  public static final int COL_PARENT_KEY = 1;
  public static final int COL_STRING_VALUE = 2;
  public static final int COL_DEWEY_ID = 3;

  /** Number of columns in the output batch. */
  private static final int COLUMN_COUNT = 4;

  /** Minimum rows before flushing a batch — ensures good SIMD utilization. */
  private static final int BATCH_FILL_THRESHOLD = 512;

  private final PageScanIterator pageIterator;
  private final ColumnarPageExtractor extractor;
  private final boolean extractDeweyIds;

  // Pre-allocated extraction arrays (capacity = DEFAULT_CAPACITY = 1024)
  private final long[] nodeKeys = new long[ColumnBatch.DEFAULT_CAPACITY];
  private final long[] parentKeys = new long[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] payloadOffsets = new int[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] valueLengths = new int[ColumnBatch.DEFAULT_CAPACITY];
  private final boolean[] isCompressed = new boolean[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] deweyIdOffsets = new int[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] deweyIdLengths = new int[ColumnBatch.DEFAULT_CAPACITY];

  // Per-row page index tracking (which page backs each row)
  private final int[] pageIndices = new int[ColumnBatch.DEFAULT_CAPACITY];

  // Multi-page guard tracking for the current batch
  private final List<PageGuard> batchGuards = new ArrayList<>(8);
  private final List<MemorySegment> pageSegments = new ArrayList<>(8);
  private final List<byte[][]> parsedSymbolTables = new ArrayList<>(8);

  // Reusable ColumnBatch
  private final ColumnBatch batch;

  private boolean exhausted;

  /**
   * Create a columnar scan axis.
   *
   * @param reader          the storage engine reader for page resolution
   * @param extractDeweyIds whether to extract DeweyIDs into column 3
   */
  public ColumnarScanAxis(final StorageEngineReader reader, final boolean extractDeweyIds) {
    this.pageIterator = new PageScanIterator(reader);
    this.extractor = new ColumnarPageExtractor();
    this.extractDeweyIds = extractDeweyIds;
    this.exhausted = false;
    this.batch = new ColumnBatch(ColumnBatch.DEFAULT_CAPACITY, COLUMN_COUNT);
    initBatchColumns();
  }

  private void initBatchColumns() {
    batch.setColumnType(COL_NODE_KEY, ColumnType.INT64);
    batch.setColumnType(COL_PARENT_KEY, ColumnType.INT64);
    batch.setColumnType(COL_STRING_VALUE, ColumnType.DEFERRED_BYTES);
    batch.setColumnType(COL_DEWEY_ID, ColumnType.BYTES);
  }

  /**
   * Produce the next {@link ColumnBatch}. Returns {@code null} when the scan is complete.
   *
   * <p>Accumulates string-type rows from multiple pages until either:
   * <ul>
   *   <li>The batch reaches {@link #BATCH_FILL_THRESHOLD} rows (good SIMD utilization)</li>
   *   <li>The batch capacity (1024) is reached</li>
   *   <li>The page scan is exhausted (flush remaining rows)</li>
   * </ul></p>
   *
   * @return a filled batch, or {@code null} if no more rows
   */
  public ColumnBatch nextBatch() {
    if (exhausted) {
      return null;
    }

    releaseBatchGuards();
    batch.reset();

    int writePos = 0;
    int pageIdx = 0;

    while (writePos < BATCH_FILL_THRESHOLD) {
      final KeyValueLeafPage page = pageIterator.nextPage();
      if (page == null) {
        exhausted = true;
        break;
      }

      // Acquire our own guard for this page — survives pageIterator.nextPage()
      page.acquireGuard();
      batchGuards.add(PageGuard.wrapAlreadyGuarded(page));

      final MemorySegment segment = page.getSlottedPage();
      pageSegments.add(segment);

      // Parse FSST symbol table once per page
      final byte[] fsstTable = page.getFsstSymbolTable();
      parsedSymbolTables.add(
          fsstTable != null ? FSSTCompressor.parseSymbolTable(fsstTable) : null);

      final int prevPos = writePos;
      writePos = extractor.extractStringsFromPage(
          page, nodeKeys, parentKeys, payloadOffsets, valueLengths,
          isCompressed, deweyIdOffsets, deweyIdLengths, writePos);

      // Tag rows from this page with the page index
      Arrays.fill(pageIndices, prevPos, writePos, pageIdx);
      pageIdx++;

      if (writePos >= ColumnBatch.DEFAULT_CAPACITY) {
        break; // batch capacity reached
      }
    }

    if (writePos == 0) {
      return null; // scan exhausted with no rows
    }

    populateBatch(writePos);
    batch.setRowCount(writePos);
    batch.seal();
    return batch;
  }

  /**
   * Populate the ColumnBatch from the extraction arrays using bulk copies.
   */
  private void populateBatch(final int rowCount) {
    // Register backing pages for DEFERRED_BYTES column
    for (int p = 0; p < pageSegments.size(); p++) {
      batch.addBackingPage(COL_STRING_VALUE, pageSegments.get(p), parsedSymbolTables.get(p));
    }

    // Bulk copy long columns (nodeKey, parentKey) — single System.arraycopy each
    batch.copyLongColumn(COL_NODE_KEY, nodeKeys, rowCount);
    batch.copyLongColumn(COL_PARENT_KEY, parentKeys, rowCount);

    // Bulk copy deferred bytes metadata
    batch.copyDeferredBytesColumn(COL_STRING_VALUE, pageIndices,
        payloadOffsets, valueLengths, isCompressed, rowCount);

    // DeweyID column — per-row byte[] copy only for surviving rows with DeweyIDs
    if (extractDeweyIds) {
      for (int row = 0; row < rowCount; row++) {
        if (deweyIdOffsets[row] >= 0 && deweyIdLengths[row] > 0) {
          final int pgIdx = pageIndices[row];
          final MemorySegment seg = pageSegments.get(pgIdx);
          final byte[] deweyBytes = new byte[deweyIdLengths[row]];
          MemorySegment.copy(seg, ValueLayout.JAVA_BYTE,
              deweyIdOffsets[row], deweyBytes, 0, deweyIdLengths[row]);
          batch.setBytes(COL_DEWEY_ID, row, deweyBytes);
        } else {
          batch.setNull(COL_DEWEY_ID, row);
        }
      }
    } else {
      // Bulk-null the entire DeweyID column — no per-row iteration needed
      Arrays.fill(batch.nullFlags(COL_DEWEY_ID), 0, rowCount, true);
    }
  }

  private void releaseBatchGuards() {
    for (final PageGuard guard : batchGuards) {
      guard.close();
    }
    batchGuards.clear();
    pageSegments.clear();
    parsedSymbolTables.clear();
  }

  @Override
  public void close() {
    releaseBatchGuards();
    pageIterator.close();
  }
}

package io.sirix.query.compiler.vectorized;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageGuard;
import io.sirix.page.ColumnarPageExtractor;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageScanIterator;
import io.sirix.utils.FSSTCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(ColumnarScanAxis.class);

  /** Column indices for the output batch. */
  public static final int COL_NODE_KEY = 0;
  public static final int COL_PARENT_KEY = 1;
  public static final int COL_STRING_VALUE = 2;
  public static final int COL_DEWEY_ID = 3;
  /** Numeric value column — FLOAT64, populated for NUMBER_VALUE/OBJECT_NUMBER_VALUE nodes. */
  public static final int COL_NUMERIC_VALUE = 4;

  /** Number of columns in the output batch. */
  private static final int COLUMN_COUNT = 5;

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

  // Pre-allocated number extraction arrays
  private final long[] numNodeKeys = new long[ColumnBatch.DEFAULT_CAPACITY];
  private final long[] numParentKeys = new long[ColumnBatch.DEFAULT_CAPACITY];
  private final double[] numValues = new double[ColumnBatch.DEFAULT_CAPACITY];
  private final boolean[] numNulls = new boolean[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] numDeweyIdOffsets = new int[ColumnBatch.DEFAULT_CAPACITY];
  private final int[] numDeweyIdLengths = new int[ColumnBatch.DEFAULT_CAPACITY];

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
    batch.setColumnType(COL_NUMERIC_VALUE, ColumnType.FLOAT64);
  }

  /**
   * Produce the next {@link ColumnBatch}. Returns {@code null} when the scan is complete.
   *
   * <p>Accumulates string-type and number-type rows from multiple pages until either:
   * <ul>
   *   <li>The batch reaches {@link #BATCH_FILL_THRESHOLD} rows (good SIMD utilization)</li>
   *   <li>The batch capacity (1024) is reached</li>
   *   <li>The page scan is exhausted (flush remaining rows)</li>
   * </ul></p>
   *
   * <p>String rows have COL_NUMERIC_VALUE=null; number rows have COL_STRING_VALUE=null.
   * Both share COL_NODE_KEY and COL_PARENT_KEY.</p>
   *
   * @return a filled batch, or {@code null} if no more rows
   */
  public ColumnBatch nextBatch() {
    if (exhausted) {
      return null;
    }

    releaseBatchGuards();
    batch.reset();

    int strWritePos = 0;
    int numWritePos = 0;
    int pageIdx = 0;

    while (strWritePos + numWritePos < BATCH_FILL_THRESHOLD) {
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

      // Extract string nodes
      final int prevStrPos = strWritePos;
      strWritePos = extractor.extractStringsFromPage(
          page, nodeKeys, parentKeys, payloadOffsets, valueLengths,
          isCompressed, deweyIdOffsets, deweyIdLengths, strWritePos);
      Arrays.fill(pageIndices, prevStrPos, strWritePos, pageIdx);

      // Extract number nodes
      numWritePos = extractor.extractNumbersFromPage(
          page, numNodeKeys, numParentKeys, numValues, numNulls,
          numDeweyIdOffsets, numDeweyIdLengths, numWritePos);

      pageIdx++;

      if (strWritePos + numWritePos >= ColumnBatch.DEFAULT_CAPACITY) {
        break;
      }
    }

    // Clamp combined rows to batch capacity. Across multiple pages, the total
    // can exceed DEFAULT_CAPACITY because each extractor respects its own array
    // bounds (1024 each) but the batch can only hold 1024 combined rows.
    // Prioritize string rows (they reference backing page segments); truncate
    // number rows if necessary. Truncated rows are a rare edge case when
    // pages have very high density of mixed string + number nodes.
    if (strWritePos > ColumnBatch.DEFAULT_CAPACITY) {
      LOG.warn("Batch capacity exceeded: {} string rows clamped to {}, {} number rows truncated",
          strWritePos, ColumnBatch.DEFAULT_CAPACITY, numWritePos);
      strWritePos = ColumnBatch.DEFAULT_CAPACITY;
      numWritePos = 0;
    } else if (strWritePos + numWritePos > ColumnBatch.DEFAULT_CAPACITY) {
      final int truncated = strWritePos + numWritePos - ColumnBatch.DEFAULT_CAPACITY;
      LOG.warn("Batch capacity exceeded: truncated {} number rows (kept {} string + {} number)",
          truncated, strWritePos, ColumnBatch.DEFAULT_CAPACITY - strWritePos);
      numWritePos = ColumnBatch.DEFAULT_CAPACITY - strWritePos;
    }

    final int totalRows = strWritePos + numWritePos;
    if (totalRows == 0) {
      return null;
    }

    populateBatch(strWritePos, numWritePos);
    batch.setRowCount(totalRows);
    batch.seal();
    return batch;
  }

  /**
   * Populate the ColumnBatch from both string and number extraction arrays.
   *
   * <p>Layout: rows [0..strCount-1] are string nodes, rows [strCount..total-1] are number nodes.
   * String rows have null in COL_NUMERIC_VALUE; number rows have null in COL_STRING_VALUE.</p>
   */
  private void populateBatch(final int strCount, final int numCount) {
    final int totalRows = strCount + numCount;

    // Register backing pages for DEFERRED_BYTES column
    for (int p = 0; p < pageSegments.size(); p++) {
      batch.addBackingPage(COL_STRING_VALUE, pageSegments.get(p), parsedSymbolTables.get(p));
    }

    // --- String rows [0..strCount-1] ---
    if (strCount > 0) {
      batch.copyLongColumn(COL_NODE_KEY, nodeKeys, strCount);
      batch.copyLongColumn(COL_PARENT_KEY, parentKeys, strCount);
      batch.copyDeferredBytesColumn(COL_STRING_VALUE, pageIndices,
          payloadOffsets, valueLengths, isCompressed, strCount);
      // Null out COL_NUMERIC_VALUE for string rows
      Arrays.fill(batch.nullFlags(COL_NUMERIC_VALUE), 0, strCount, true);
    }

    // --- Number rows [strCount..totalRows-1] ---
    if (numCount > 0) {
      // Append number nodeKeys and parentKeys after string rows
      final long[] batchNodeKeys = batch.longColumn(COL_NODE_KEY);
      System.arraycopy(numNodeKeys, 0, batchNodeKeys, strCount, numCount);
      final long[] batchParentKeys = batch.longColumn(COL_PARENT_KEY);
      System.arraycopy(numParentKeys, 0, batchParentKeys, strCount, numCount);

      // Copy numeric values
      final double[] batchNumValues = batch.doubleColumn(COL_NUMERIC_VALUE);
      System.arraycopy(numValues, 0, batchNumValues, strCount, numCount);
      // Copy null flags for numeric values
      System.arraycopy(numNulls, 0, batch.nullFlags(COL_NUMERIC_VALUE), strCount, numCount);

      // Null out COL_STRING_VALUE for number rows
      Arrays.fill(batch.nullFlags(COL_STRING_VALUE), strCount, totalRows, true);
    }

    // DeweyID column — handle both string and number rows
    if (extractDeweyIds) {
      // String rows
      for (int row = 0; row < strCount; row++) {
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
      // Number rows — no page index tracking, set all to null
      Arrays.fill(batch.nullFlags(COL_DEWEY_ID), strCount, strCount + numCount, true);
    } else {
      Arrays.fill(batch.nullFlags(COL_DEWEY_ID), 0, totalRows, true);
    }
  }

  private void releaseBatchGuards() {
    Throwable firstError = null;
    for (final PageGuard guard : batchGuards) {
      try {
        guard.close();
      } catch (final Throwable t) {
        if (firstError == null) {
          firstError = t;
        } else {
          firstError.addSuppressed(t);
        }
      }
    }
    batchGuards.clear();
    pageSegments.clear();
    parsedSymbolTables.clear();
    if (firstError instanceof RuntimeException re) {
      throw re;
    } else if (firstError != null) {
      throw new RuntimeException("Failed to release page guards", firstError);
    }
  }

  @Override
  public void close() {
    try {
      releaseBatchGuards();
    } finally {
      pageIterator.close();
    }
  }
}

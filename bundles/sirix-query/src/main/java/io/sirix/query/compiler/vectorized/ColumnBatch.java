package io.sirix.query.compiler.vectorized;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.sirix.utils.FSSTCompressor;

/**
 * Fixed-capacity columnar batch with selection vector for vectorized query execution.
 *
 * <p>Stores rows in columnar layout (one array per column) for cache-friendly
 * SIMD processing. A selection vector tracks which rows are "active" after
 * filter operations — only selected rows are materialized downstream.</p>
 *
 * <p>Supports {@link ColumnType#DEFERRED_BYTES} for late materialization:
 * string values remain as offset+length references into backing page
 * MemorySegments until explicitly decoded. This enables SIMD filtering on
 * compressed data without decompression.</p>
 *
 * <p>Design follows the Volcano/MonetDB vectorized execution model:
 * <ul>
 *   <li>Fixed batch capacity (default 1024) for predictable memory usage</li>
 *   <li>Selection vector avoids copying rows during filter operations</li>
 *   <li>Columnar layout enables SIMD comparison via Java Vector API</li>
 *   <li>Null tracking via per-column boolean arrays</li>
 *   <li>Multi-page backing for deferred columns (one batch spans multiple pages)</li>
 * </ul></p>
 */
public final class ColumnBatch {

  /** Default batch capacity — 1024 balances SIMD width and cache pressure. */
  public static final int DEFAULT_CAPACITY = 1024;

  private final int capacity;
  private final int columnCount;
  private final ColumnType[] columnTypes;

  // Columnar storage — only the relevant array is non-null per column
  private final long[][] longColumns;
  private final double[][] doubleColumns;
  private final String[][] stringColumns;
  private final boolean[][] boolColumns;
  private final boolean[][] nullFlags;

  // DEFERRED_BYTES storage (per column)
  private final int[][] deferredOffsets;
  private final int[][] deferredLengths;
  private final boolean[][] deferredCompressed;
  private final int[][] deferredPageIndex;

  // BYTES storage (per column)
  private final byte[][][] bytesColumns;

  // Multi-page backing for DEFERRED_BYTES columns
  private final List<MemorySegment>[] backingPages;
  private final List<byte[][]>[] parsedFsstSymbolsByPage;

  // Cached arrays for materializeAllSelected — avoid per-batch toArray() allocation
  private MemorySegment[] cachedPagesArray;
  private byte[][][] cachedSymbolsArray;

  // Selection vector: indices of active rows after filtering
  private final int[] selectionVector;
  private int selectionCount;

  // Number of rows currently loaded
  private int rowCount;

  /**
   * Create a new column batch.
   *
   * @param capacity   maximum number of rows (must be positive)
   * @param columnCount number of columns (must be positive)
   */
  @SuppressWarnings("unchecked")
  public ColumnBatch(int capacity, int columnCount) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("Capacity must be positive: " + capacity);
    }
    if (columnCount <= 0) {
      throw new IllegalArgumentException("Column count must be positive: " + columnCount);
    }
    this.capacity = capacity;
    this.columnCount = columnCount;
    this.columnTypes = new ColumnType[columnCount];
    this.longColumns = new long[columnCount][];
    this.doubleColumns = new double[columnCount][];
    this.stringColumns = new String[columnCount][];
    this.boolColumns = new boolean[columnCount][];
    this.nullFlags = new boolean[columnCount][];
    this.deferredOffsets = new int[columnCount][];
    this.deferredLengths = new int[columnCount][];
    this.deferredCompressed = new boolean[columnCount][];
    this.deferredPageIndex = new int[columnCount][];
    this.bytesColumns = new byte[columnCount][][];
    this.backingPages = new List[columnCount];
    this.parsedFsstSymbolsByPage = new List[columnCount];
    this.selectionVector = new int[capacity];
    this.selectionCount = 0;
    this.rowCount = 0;
  }

  /**
   * Set the type for a column and allocate its storage.
   *
   * @param col  column index
   * @param type the column type
   */
  public void setColumnType(int col, ColumnType type) {
    checkColumnIndex(col);
    if (type == null) {
      throw new IllegalArgumentException("Column type must not be null");
    }
    columnTypes[col] = type;
    nullFlags[col] = new boolean[capacity];
    switch (type) {
      case INT64 -> longColumns[col] = new long[capacity];
      case FLOAT64 -> doubleColumns[col] = new double[capacity];
      case STRING -> stringColumns[col] = new String[capacity];
      case BOOLEAN -> boolColumns[col] = new boolean[capacity];
      case DEFERRED_BYTES -> {
        deferredOffsets[col] = new int[capacity];
        deferredLengths[col] = new int[capacity];
        deferredCompressed[col] = new boolean[capacity];
        deferredPageIndex[col] = new int[capacity];
        backingPages[col] = new ArrayList<>(8);
        parsedFsstSymbolsByPage[col] = new ArrayList<>(8);
        // Also allocate string column for materialized results
        stringColumns[col] = new String[capacity];
      }
      case BYTES -> bytesColumns[col] = new byte[capacity][];
    }
  }

  /**
   * Get the type of a column.
   *
   * @param col column index
   * @return the column type, or null if not yet set
   */
  public ColumnType getColumnType(int col) {
    checkColumnIndex(col);
    return columnTypes[col];
  }

  // --- Append methods (fill the batch row by row) ---

  public void setLong(int col, int row, long value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    longColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  public void setDouble(int col, int row, double value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    doubleColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  public void setString(int col, int row, String value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    stringColumns[col][row] = value;
    nullFlags[col][row] = (value == null);
  }

  public void setBoolean(int col, int row, boolean value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    boolColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  public void setNull(int col, int row) {
    checkColumnIndex(col);
    checkRowIndex(row);
    nullFlags[col][row] = true;
  }

  // --- DEFERRED_BYTES methods ---

  /**
   * Set a deferred byte reference for a row.
   *
   * @param col        column index (must be DEFERRED_BYTES type)
   * @param row        row index
   * @param pageIdx    index into the backing page table
   * @param offset     absolute byte offset in the page MemorySegment
   * @param length     byte length of the value
   * @param compressed whether the value is FSST-compressed
   */
  public void setDeferredBytes(int col, int row, int pageIdx, int offset, int length, boolean compressed) {
    deferredOffsets[col][row] = offset;
    deferredLengths[col][row] = length;
    deferredCompressed[col][row] = compressed;
    deferredPageIndex[col][row] = pageIdx;
    nullFlags[col][row] = false;
  }

  /**
   * Register a backing page for a DEFERRED_BYTES column.
   *
   * @param col           column index
   * @param page          the page MemorySegment
   * @param parsedSymbols pre-parsed FSST symbol table (null if no compression)
   * @return the page index to use in {@link #setDeferredBytes}
   */
  public int addBackingPage(int col, MemorySegment page, byte[][] parsedSymbols) {
    final int idx = backingPages[col].size();
    backingPages[col].add(page);
    parsedFsstSymbolsByPage[col].add(parsedSymbols);
    return idx;
  }

  /**
   * Materialize a single deferred string value (on-demand decode).
   *
   * @param col column index (must be DEFERRED_BYTES type)
   * @param row row index
   * @return the decoded string, or null if the row is null
   */
  public String materializeDeferredString(int col, int row) {
    if (nullFlags[col][row]) {
      return null;
    }
    // Check if already materialized
    if (stringColumns[col][row] != null) {
      return stringColumns[col][row];
    }
    final int pageIdx = deferredPageIndex[col][row];
    final MemorySegment page = backingPages[col].get(pageIdx);
    final int offset = deferredOffsets[col][row];
    final int length = deferredLengths[col][row];

    final byte[] raw = new byte[length];
    MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offset, raw, 0, length);

    final String result;
    if (deferredCompressed[col][row]) {
      final byte[][] symbols = parsedFsstSymbolsByPage[col].get(pageIdx);
      final byte[] decoded = FSSTCompressor.decode(raw, symbols);
      result = new String(decoded, StandardCharsets.UTF_8);
    } else {
      result = new String(raw, StandardCharsets.UTF_8);
    }
    stringColumns[col][row] = result;
    return result;
  }

  /**
   * Batch-materialize all selected deferred strings.
   * Delegates to {@link FSSTCompressor#batchDecode} for buffer-pooled bulk decoding.
   *
   * @param col column index (must be DEFERRED_BYTES type)
   * @return array of decoded strings indexed by original row position
   */
  public String[] materializeAllSelected(int col) {
    final List<MemorySegment> pages = backingPages[col];
    final List<byte[][]> symbolTables = parsedFsstSymbolsByPage[col];
    final int pageCount = pages.size();

    // Reuse cached arrays — only reallocate when page count exceeds capacity
    if (cachedPagesArray == null || cachedPagesArray.length < pageCount) {
      cachedPagesArray = new MemorySegment[pageCount];
      cachedSymbolsArray = new byte[pageCount][][];
    }
    for (int i = 0; i < pageCount; i++) {
      cachedPagesArray[i] = pages.get(i);
      cachedSymbolsArray[i] = symbolTables.get(i);
    }
    // Clear stale references from prior (larger) batches to allow GC
    for (int i = pageCount; i < cachedPagesArray.length; i++) {
      cachedPagesArray[i] = null;
      cachedSymbolsArray[i] = null;
    }

    FSSTCompressor.batchDecode(
        cachedPagesArray, deferredPageIndex[col],
        deferredOffsets[col], deferredLengths[col], deferredCompressed[col],
        selectionVector, selectionCount,
        cachedSymbolsArray,
        stringColumns[col]);

    return stringColumns[col];
  }

  /**
   * Release all backing page references for a DEFERRED_BYTES column.
   * Call after the batch is fully consumed.
   *
   * @param col column index
   */
  public void clearBackingPages(int col) {
    if (backingPages[col] != null) {
      backingPages[col].clear();
    }
    if (parsedFsstSymbolsByPage[col] != null) {
      parsedFsstSymbolsByPage[col].clear();
    }
  }

  // --- BYTES methods ---

  public void setBytes(int col, int row, byte[] value) {
    bytesColumns[col][row] = value;
    nullFlags[col][row] = (value == null);
  }

  public byte[] getBytes(int col, int row) {
    return bytesColumns[col][row];
  }

  // --- Direct deferred column array access ---

  public int[] deferredOffsets(int col) { return deferredOffsets[col]; }
  public int[] deferredLengths(int col) { return deferredLengths[col]; }
  public boolean[] deferredCompressed(int col) { return deferredCompressed[col]; }
  public int[] deferredPageIndices(int col) { return deferredPageIndex[col]; }
  public List<MemorySegment> backingPages(int col) { return backingPages[col]; }
  public List<byte[][]> parsedFsstSymbolsByPage(int col) { return parsedFsstSymbolsByPage[col]; }

  // --- Read methods ---

  public long getLong(int col, int row) {
    return longColumns[col][row];
  }

  public double getDouble(int col, int row) {
    return doubleColumns[col][row];
  }

  public String getString(int col, int row) {
    return stringColumns[col][row];
  }

  public boolean getBoolean(int col, int row) {
    return boolColumns[col][row];
  }

  public boolean isNull(int col, int row) {
    return nullFlags[col][row];
  }

  // --- Row count management ---

  public void setRowCount(int count) {
    if (count < 0 || count > capacity) {
      throw new IllegalArgumentException("Row count out of range [0, " + capacity + "]: " + count);
    }
    this.rowCount = count;
  }

  public int rowCount() {
    return rowCount;
  }

  /**
   * Seal the batch: initialize the selection vector to select all rows.
   * Call this after filling the batch and before applying filters.
   */
  public void seal() {
    for (int i = 0; i < rowCount; i++) {
      selectionVector[i] = i;
    }
    selectionCount = rowCount;
  }

  // --- Selection vector ---

  public int[] selectionVector() { return selectionVector; }
  public int selectionCount() { return selectionCount; }

  public void setSelectionCount(int count) {
    if (count < 0 || count > rowCount) {
      throw new IllegalArgumentException(
          "Selection count out of range [0, " + rowCount + "]: " + count);
    }
    this.selectionCount = count;
  }

  public int selectedRow(int i) {
    return selectionVector[i];
  }

  // --- Direct column array access for SIMD operators ---

  public long[] longColumn(int col) { return longColumns[col]; }
  public double[] doubleColumn(int col) { return doubleColumns[col]; }
  public boolean[] nullFlags(int col) { return nullFlags[col]; }

  // --- Bulk copy methods (avoid per-row bounds checks) ---

  /**
   * Bulk-copy a long array into a column. Skips per-row bounds checks.
   *
   * @param col column index (must be INT64 type)
   * @param src source array
   * @param len number of elements to copy
   */
  void copyLongColumn(int col, long[] src, int len) {
    System.arraycopy(src, 0, longColumns[col], 0, len);
    Arrays.fill(nullFlags[col], 0, len, false);
  }

  /**
   * Bulk-copy deferred byte references into a column. Skips per-row bounds checks.
   *
   * @param col         column index (must be DEFERRED_BYTES type)
   * @param pageIdx     per-row page indices
   * @param offsets     per-row byte offsets
   * @param lengths     per-row byte lengths
   * @param compressed  per-row compression flags
   * @param len         number of rows to copy
   */
  void copyDeferredBytesColumn(int col, int[] pageIdx, int[] offsets,
      int[] lengths, boolean[] compressed, int len) {
    System.arraycopy(offsets, 0, deferredOffsets[col], 0, len);
    System.arraycopy(lengths, 0, deferredLengths[col], 0, len);
    System.arraycopy(compressed, 0, deferredCompressed[col], 0, len);
    System.arraycopy(pageIdx, 0, deferredPageIndex[col], 0, len);
    Arrays.fill(nullFlags[col], 0, len, false);
  }

  // --- Batch lifecycle ---

  public int capacity() { return capacity; }
  public int columnCount() { return columnCount; }

  /**
   * Reset the batch for reuse — clears row count and selection.
   * Column types and arrays are retained to avoid reallocation.
   */
  public void reset() {
    final int clearLen = rowCount; // only touch populated range
    rowCount = 0;
    selectionCount = 0;
    for (int col = 0; col < columnCount; col++) {
      if (columnTypes[col] == ColumnType.STRING && stringColumns[col] != null) {
        Arrays.fill(stringColumns[col], 0, clearLen, null);
      }
      if (columnTypes[col] == ColumnType.DEFERRED_BYTES) {
        if (stringColumns[col] != null) {
          Arrays.fill(stringColumns[col], 0, clearLen, null);
        }
        clearBackingPages(col);
      }
      if (columnTypes[col] == ColumnType.BYTES && bytesColumns[col] != null) {
        Arrays.fill(bytesColumns[col], 0, clearLen, null);
      }
    }
  }

  private void checkColumnIndex(int col) {
    if (col < 0 || col >= columnCount) {
      throw new IndexOutOfBoundsException("Column index " + col + " out of [0, " + columnCount + ")");
    }
  }

  private void checkRowIndex(int row) {
    if (row < 0 || row >= capacity) {
      throw new IndexOutOfBoundsException("Row index " + row + " out of [0, " + capacity + ")");
    }
  }
}

package io.sirix.query.compiler.vectorized;

import java.util.Arrays;

/**
 * Fixed-capacity columnar batch with selection vector for vectorized query execution.
 *
 * <p>Stores rows in columnar layout (one array per column) for cache-friendly
 * SIMD processing. A selection vector tracks which rows are "active" after
 * filter operations — only selected rows are materialized downstream.</p>
 *
 * <p>Design follows the Volcano/MonetDB vectorized execution model:
 * <ul>
 *   <li>Fixed batch capacity (default 1024) for predictable memory usage</li>
 *   <li>Selection vector avoids copying rows during filter operations</li>
 *   <li>Columnar layout enables SIMD comparison via Java Vector API</li>
 *   <li>Null tracking via per-column boolean arrays</li>
 * </ul></p>
 *
 * <p>Usage pattern:
 * <pre>
 *   var batch = new ColumnBatch(1024, 2);
 *   batch.setColumnType(0, ColumnType.INT64);
 *   batch.setColumnType(1, ColumnType.FLOAT64);
 *   // Fill via appendLong/appendDouble, then seal
 *   batch.seal();
 *   // Apply vectorized filter
 *   selectOperator.filter(batch);
 *   // Iterate selected rows
 *   for (int i = 0; i &lt; batch.selectionCount(); i++) {
 *     int row = batch.selectedRow(i);
 *     long val = batch.getLong(0, row);
 *   }
 * </pre></p>
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

  /**
   * Set a long value at a specific row and column.
   *
   * @param col column index (must be INT64 type)
   * @param row row index
   * @param value the value
   */
  public void setLong(int col, int row, long value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    longColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  /**
   * Set a double value at a specific row and column.
   *
   * @param col column index (must be FLOAT64 type)
   * @param row row index
   * @param value the value
   */
  public void setDouble(int col, int row, double value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    doubleColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  /**
   * Set a string value at a specific row and column.
   *
   * @param col column index (must be STRING type)
   * @param row row index
   * @param value the value (may be null)
   */
  public void setString(int col, int row, String value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    stringColumns[col][row] = value;
    nullFlags[col][row] = (value == null);
  }

  /**
   * Set a boolean value at a specific row and column.
   *
   * @param col column index (must be BOOLEAN type)
   * @param row row index
   * @param value the value
   */
  public void setBoolean(int col, int row, boolean value) {
    checkColumnIndex(col);
    checkRowIndex(row);
    boolColumns[col][row] = value;
    nullFlags[col][row] = false;
  }

  /**
   * Mark a cell as null.
   *
   * @param col column index
   * @param row row index
   */
  public void setNull(int col, int row) {
    checkColumnIndex(col);
    checkRowIndex(row);
    nullFlags[col][row] = true;
  }

  // --- Read methods ---

  /**
   * Get a long value.
   *
   * @param col column index
   * @param row row index
   * @return the long value
   */
  public long getLong(int col, int row) {
    return longColumns[col][row];
  }

  /**
   * Get a double value.
   *
   * @param col column index
   * @param row row index
   * @return the double value
   */
  public double getDouble(int col, int row) {
    return doubleColumns[col][row];
  }

  /**
   * Get a string value.
   *
   * @param col column index
   * @param row row index
   * @return the string value (may be null)
   */
  public String getString(int col, int row) {
    return stringColumns[col][row];
  }

  /**
   * Get a boolean value.
   *
   * @param col column index
   * @param row row index
   * @return the boolean value
   */
  public boolean getBoolean(int col, int row) {
    return boolColumns[col][row];
  }

  /**
   * Check if a cell is null.
   *
   * @param col column index
   * @param row row index
   * @return true if null
   */
  public boolean isNull(int col, int row) {
    return nullFlags[col][row];
  }

  // --- Row count management ---

  /**
   * Set the number of valid rows in this batch.
   *
   * @param count the row count
   */
  public void setRowCount(int count) {
    if (count < 0 || count > capacity) {
      throw new IllegalArgumentException("Row count out of range [0, " + capacity + "]: " + count);
    }
    this.rowCount = count;
  }

  /**
   * @return the number of rows currently in this batch
   */
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

  /**
   * Get the raw selection vector array for direct SIMD manipulation.
   * The first {@link #selectionCount()} entries are valid.
   *
   * @return the selection vector (internal reference — do not cache)
   */
  public int[] selectionVector() {
    return selectionVector;
  }

  /**
   * @return the number of currently selected (active) rows
   */
  public int selectionCount() {
    return selectionCount;
  }

  /**
   * Set the number of selected rows after a filter operation.
   *
   * @param count the new selection count
   */
  public void setSelectionCount(int count) {
    if (count < 0 || count > rowCount) {
      throw new IllegalArgumentException(
          "Selection count out of range [0, " + rowCount + "]: " + count);
    }
    this.selectionCount = count;
  }

  /**
   * Get the row index of the i-th selected row.
   *
   * @param i index into the selection vector
   * @return the actual row index
   */
  public int selectedRow(int i) {
    return selectionVector[i];
  }

  // --- Direct column array access for SIMD operators ---

  /**
   * Get the raw long column array for SIMD processing.
   *
   * @param col column index
   * @return the long array (internal reference)
   */
  public long[] longColumn(int col) {
    return longColumns[col];
  }

  /**
   * Get the raw double column array for SIMD processing.
   *
   * @param col column index
   * @return the double array (internal reference)
   */
  public double[] doubleColumn(int col) {
    return doubleColumns[col];
  }

  /**
   * Get the raw null flags for a column.
   *
   * @param col column index
   * @return the null flag array (internal reference)
   */
  public boolean[] nullFlags(int col) {
    return nullFlags[col];
  }

  // --- Batch lifecycle ---

  /**
   * @return the maximum number of rows this batch can hold
   */
  public int capacity() {
    return capacity;
  }

  /**
   * @return the number of columns
   */
  public int columnCount() {
    return columnCount;
  }

  /**
   * Reset the batch for reuse — clears row count and selection.
   * Column types and arrays are retained to avoid reallocation.
   */
  public void reset() {
    rowCount = 0;
    selectionCount = 0;
    // Clear string references to avoid retaining garbage
    for (int col = 0; col < columnCount; col++) {
      if (columnTypes[col] == ColumnType.STRING && stringColumns[col] != null) {
        Arrays.fill(stringColumns[col], null);
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

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * Primitive-column leaf page for a projection index. Each page holds up to
 * {@link #MAX_ROWS} contiguous record projections under a single declared
 * projection. Column layout is declared by the owning {@link
 * io.sirix.index.IndexDef}: index {@code i} in {@code IndexDef#getProjectionFields()}
 * maps onto column slot {@code i} in this page, and the column's primitive
 * shape is determined by {@code IndexDef#getProjectionFieldTypes().get(i)}.
 *
 * <h2>On-disk shape</h2>
 *
 * The page materialises to a sequence of primitive arrays — no boxed
 * collections, no {@code Object[]}, no per-row allocation on the scan hot
 * path:
 *
 * <pre>
 *   int    rowCount              // number of active rows (0..MAX_ROWS)
 *   int    columnCount           // index-aligned with the owning IndexDef
 *   long[rowCount] recordKeys    // nodeKey of each record projected here
 *   long   firstRecordKey        // zone-map lower bound across recordKeys
 *   long   lastRecordKey         //   upper bound — enables HOT range skip
 *
 *   for each column c in [0, columnCount):
 *     byte kind                  // 0=NUMERIC_LONG, 1=BOOLEAN, 2=STRING_DICT
 *
 *     // NUMERIC_LONG:
 *       long min, max            // per-column zone map
 *       byte valueBitWidth       // 1..64 (64 = no bit-packing)
 *       long valueBase           // frame-of-reference base for bit-packing
 *       byte[]  packedValues     // ceil(rowCount*valueBitWidth/8) bytes
 *
 *     // BOOLEAN:
 *       byte[ceil(rowCount/8)] packedBits
 *
 *     // STRING_DICT:
 *       int       localDictSize
 *       int[localDictSize] stringLengths
 *       byte[]    concatenatedUtf8
 *       byte      dictIdBitWidth // fits log2(localDictSize)
 *       byte[]    packedDictIds  // ceil(rowCount*dictIdBitWidth/8) bytes
 * </pre>
 *
 * <h2>Scan hot-path contract</h2>
 *
 * The reader exposes, per column, zero-allocation primitive accessors used
 * by the fused SIMD scan kernel (lands with the query-route commit):
 *
 * <pre>
 *   long   numericValueAt(int col, int row)   // unpacks one value
 *   void   numericValuesInto(int col, long[] out, int off) // bulk unpack
 *   boolean booleanAt(int col, int row)
 *   void   booleanBitsInto(int col, long[] out) // packed 64-way bitmap
 *   int    stringDictIdAt(int col, int row)
 *   byte[] stringBytes(int col, int dictId, int[] lenOut) // raw dict bytes
 * </pre>
 *
 * The scan passes each column's per-row primitives through SIMD predicate
 * kernels reused from {@link io.sirix.page.pax.NumberRegionSimd} and
 * {@link io.sirix.page.pax.BooleanRegion#countTrue}, producing a 1024-bit
 * mask; conjunctive predicates AND their masks, popcount gives the count.
 * Zero {@code Object} allocations in the inner loop.
 *
 * <h2>Versioning</h2>
 *
 * Subject to the standard Sirix CoW page-reference scheme: a write-time
 * listener appending to the current open leaf triggers a shadow copy on
 * commit, and the parent IndirectPage in the projection's HOT trie gets
 * the updated page reference. Old revisions keep their reference chain
 * unchanged.
 *
 * <p><b>Status</b>: layout contract only. Fields and accessors are
 * declared here for forward plumbing; ser/deser + the HOT page-type
 * registration land in follow-up commits (task #23).
 */
public final class ProjectionIndexLeafPage {

  /**
   * Row capacity per leaf. Sized to match the existing {@code
   * Constants.INP_REFERENCE_COUNT} / batch size so the SIMD predicate
   * kernel operates on fixed-width lanes across projection and PAX
   * scans alike.
   */
  public static final int MAX_ROWS = 1024;

  /**
   * Column-kind bytes written into the page header. Order matches
   * {@code IndexDef.getProjectionFieldTypes()} — INR/LON → NUMERIC_LONG,
   * BOOL → BOOLEAN, STR and friends → STRING_DICT.
   */
  public static final byte COLUMN_KIND_NUMERIC_LONG = 0;
  public static final byte COLUMN_KIND_BOOLEAN = 1;
  public static final byte COLUMN_KIND_STRING_DICT = 2;

  /** Number of populated rows on this page, {@code 0..MAX_ROWS}. */
  private int rowCount;

  /** Column count, matching the owning {@code IndexDef#getProjectionFields().size()}. */
  private final int columnCount;

  /** {@code long[rowCount]} — record nodeKey per row. {@code null} until {@link #ensureCapacity} runs. */
  private long[] recordKeys;

  /** Per-column kind byte from {@link #COLUMN_KIND_NUMERIC_LONG} / …. */
  private final byte[] columnKinds;

  /**
   * Per-column numeric values. Slot {@code c} is valid iff
   * {@code columnKinds[c] == COLUMN_KIND_NUMERIC_LONG}. {@code long[MAX_ROWS]},
   * allocated lazily in {@link #ensureCapacity}.
   */
  private final long[][] numericCols;

  /**
   * Per-column boolean values, 64-way bit-packed (matches
   * {@link io.sirix.page.pax.BooleanRegion} format for the
   * {@code countTrue}/{@code decodeAt} kernels). Slot {@code c} valid iff
   * {@code columnKinds[c] == COLUMN_KIND_BOOLEAN}. Length
   * {@code ceil(MAX_ROWS / 64)}.
   */
  private final long[][] booleanCols;

  /**
   * Per-column dict-id values. Slot {@code c} valid iff
   * {@code columnKinds[c] == COLUMN_KIND_STRING_DICT}. Paired with
   * {@link #stringDicts}. {@code int[MAX_ROWS]}.
   */
  private final int[][] stringDictIdCols;

  /**
   * Per-column local string dictionary. Slot {@code c} holds the byte[]
   * array whose index is the dict-id stored in {@code stringDictIdCols[c]}.
   * Arrays rather than a shared heap so we can stream per-column without
   * cross-column lookup overhead.
   */
  private final byte[][][] stringDicts;

  /**
   * Per-column zone-map min / max. For numeric columns: inclusive value
   * range. For boolean: irrelevant ({@code 0} → has-false-only,
   * {@code 1} → has-true-only, {@code -1} → both). For dict columns:
   * min / max dict-id observed.
   */
  private final long[] columnMin;
  private final long[] columnMax;

  /** Record-key zone map across all rows. Enables whole-leaf skip at query time. */
  private long firstRecordKey;
  private long lastRecordKey;

  /**
   * Initialise an empty page for the declared column shape. The actual
   * per-column primitive arrays are materialised on first
   * {@link #ensureCapacity} call (which writer / reader paths trigger).
   */
  public ProjectionIndexLeafPage(final byte[] columnKinds) {
    this.columnCount = columnKinds.length;
    this.columnKinds = columnKinds.clone();
    this.numericCols = new long[columnCount][];
    this.booleanCols = new long[columnCount][];
    this.stringDictIdCols = new int[columnCount][];
    this.stringDicts = new byte[columnCount][][];
    this.columnMin = new long[columnCount];
    this.columnMax = new long[columnCount];
    for (int c = 0; c < columnCount; c++) {
      columnMin[c] = Long.MAX_VALUE;
      columnMax[c] = Long.MIN_VALUE;
    }
    this.firstRecordKey = Long.MAX_VALUE;
    this.lastRecordKey = Long.MIN_VALUE;
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getColumnCount() {
    return columnCount;
  }

  public byte columnKind(final int column) {
    return columnKinds[column];
  }

  public long firstRecordKey() {
    return firstRecordKey;
  }

  public long lastRecordKey() {
    return lastRecordKey;
  }

  public long columnMin(final int column) {
    return columnMin[column];
  }

  public long columnMax(final int column) {
    return columnMax[column];
  }

  public long[] recordKeys() {
    return recordKeys;
  }

  public long[] numericColumn(final int column) {
    return numericCols[column];
  }

  public long[] booleanColumnBits(final int column) {
    return booleanCols[column];
  }

  public int[] stringDictIdColumn(final int column) {
    return stringDictIdCols[column];
  }

  public byte[][] stringDictionary(final int column) {
    return stringDicts[column];
  }
}

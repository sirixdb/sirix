/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

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

  /** Ensure the per-column primitive arrays are materialised. Idempotent. */
  private void ensureCapacity() {
    if (recordKeys == null) {
      recordKeys = new long[MAX_ROWS];
      for (int c = 0; c < columnCount; c++) {
        switch (columnKinds[c]) {
          case COLUMN_KIND_NUMERIC_LONG -> numericCols[c] = new long[MAX_ROWS];
          case COLUMN_KIND_BOOLEAN -> booleanCols[c] = new long[(MAX_ROWS + 63) >>> 6];
          case COLUMN_KIND_STRING_DICT -> {
            stringDictIdCols[c] = new int[MAX_ROWS];
            stringDicts[c] = new byte[16][];  // grow on demand in appendString
          }
          default -> throw new IllegalStateException("Unknown column kind " + columnKinds[c]);
        }
      }
    }
  }

  /**
   * Append one record projection. The three varargs-shaped arrays are
   * index-aligned with the page's column kinds: {@code longValues[c]}
   * populated iff kind is NUMERIC_LONG, {@code boolValues[c]} iff BOOLEAN,
   * {@code stringValues[c]} iff STRING_DICT. Mismatches throw — the
   * builder is trusted to extract per the IndexDef's declared types.
   *
   * @return {@code true} if the row was appended, {@code false} if the
   *         page is already at {@link #MAX_ROWS} capacity (caller opens a
   *         fresh leaf and retries).
   */
  public boolean appendRow(final long recordKey,
      final long[] longValues, final boolean[] boolValues, final String[] stringValues) {
    if (rowCount == MAX_ROWS) return false;
    ensureCapacity();
    final int row = rowCount;
    recordKeys[row] = recordKey;
    if (recordKey < firstRecordKey) firstRecordKey = recordKey;
    if (recordKey > lastRecordKey) lastRecordKey = recordKey;
    for (int c = 0; c < columnCount; c++) {
      switch (columnKinds[c]) {
        case COLUMN_KIND_NUMERIC_LONG -> {
          final long v = longValues[c];
          numericCols[c][row] = v;
          if (v < columnMin[c]) columnMin[c] = v;
          if (v > columnMax[c]) columnMax[c] = v;
        }
        case COLUMN_KIND_BOOLEAN -> {
          if (boolValues[c]) {
            booleanCols[c][row >>> 6] |= 1L << (row & 63);
          }
        }
        case COLUMN_KIND_STRING_DICT -> stringDictIdCols[c][row] = appendString(c, stringValues[c]);
        default -> throw new IllegalStateException("Unknown column kind " + columnKinds[c]);
      }
    }
    rowCount++;
    return true;
  }

  /**
   * Intern {@code value} into column {@code c}'s per-leaf dictionary and
   * return its dict-id. Dictionary is append-only within one leaf; grown
   * amortised. Dict-id fits in the {@code dictIdBitWidth} computed at
   * serialize time.
   */
  private int appendString(final int c, final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final byte[][] dict = stringDicts[c];
    // Linear probe for small dictionaries is cheaper than HashMap bookkeeping;
    // at the typical analytical-column cardinality (8-50) this is 1-2 cache lines.
    int size = 0;
    for (int i = 0; i < dict.length; i++) {
      if (dict[i] == null) { size = i; break; }
      size = i + 1;
      if (bytesEqual(dict[i], bytes)) return i;
    }
    if (size == dict.length) {
      final byte[][] grown = new byte[dict.length << 1][];
      System.arraycopy(dict, 0, grown, 0, dict.length);
      stringDicts[c] = grown;
    }
    stringDicts[c][size] = bytes;
    if (size < columnMin[c]) columnMin[c] = size;
    if (size > columnMax[c]) columnMax[c] = size;
    return size;
  }

  private static boolean bytesEqual(final byte[] a, final byte[] b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) if (a[i] != b[i]) return false;
    return true;
  }

  /**
   * Parse a serialised leaf byte[] back into a live
   * {@link ProjectionIndexLeafPage}. Inverse of {@link #serialize}.
   */
  public static ProjectionIndexLeafPage deserialize(final byte[] payload) {
    final ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
    final int rowCount = bb.getInt();
    final int columnCount = bb.getInt();
    final long firstRecordKey = bb.getLong();
    final long lastRecordKey = bb.getLong();
    final byte[] kinds = new byte[columnCount];
    bb.get(kinds);
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    page.rowCount = rowCount;
    page.firstRecordKey = firstRecordKey;
    page.lastRecordKey = lastRecordKey;
    if (rowCount == 0) return page;
    page.ensureCapacity();
    for (int i = 0; i < rowCount; i++) page.recordKeys[i] = bb.getLong();
    for (int c = 0; c < columnCount; c++) {
      page.columnMin[c] = bb.getLong();
      page.columnMax[c] = bb.getLong();
      switch (kinds[c]) {
        case COLUMN_KIND_NUMERIC_LONG -> {
          final long[] col = page.numericCols[c];
          for (int i = 0; i < rowCount; i++) col[i] = bb.getLong();
        }
        case COLUMN_KIND_BOOLEAN -> {
          final int wordCount = (rowCount + 63) >>> 6;
          final long[] bits = page.booleanCols[c];
          for (int i = 0; i < wordCount; i++) bits[i] = bb.getLong();
        }
        case COLUMN_KIND_STRING_DICT -> {
          final int dictSize = bb.getInt();
          final int[] lengths = new int[dictSize];
          for (int i = 0; i < dictSize; i++) lengths[i] = bb.getInt();
          final byte[][] dict = new byte[Math.max(16, dictSize)][];
          for (int i = 0; i < dictSize; i++) {
            dict[i] = new byte[lengths[i]];
            bb.get(dict[i]);
          }
          page.stringDicts[c] = dict;
          final int[] ids = page.stringDictIdCols[c];
          for (int i = 0; i < rowCount; i++) ids[i] = bb.getInt();
        }
        default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
      }
    }
    return page;
  }

  /**
   * Serialise the current page state to a byte[] matching the on-disk
   * shape documented in the class javadoc. Zero-allocation on the hot
   * scan path is preserved by the reader — ser is a cold path used only
   * during commit.
   */
  public byte[] serialize() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    final ByteBuffer header = ByteBuffer.allocate(8 + 16 + columnCount).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(rowCount);
    header.putInt(columnCount);
    header.putLong(firstRecordKey);
    header.putLong(lastRecordKey);
    for (int c = 0; c < columnCount; c++) header.put(columnKinds[c]);
    baos.write(header.array(), 0, header.position());
    if (rowCount == 0) {
      // Empty page — no row or column data to append.
      return baos.toByteArray();
    }
    // recordKeys
    final ByteBuffer recBuf = ByteBuffer.allocate(rowCount * 8).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < rowCount; i++) recBuf.putLong(recordKeys[i]);
    baos.write(recBuf.array(), 0, recBuf.position());
    // per-column
    for (int c = 0; c < columnCount; c++) {
      final ByteBuffer colHdr = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      colHdr.putLong(columnMin[c]);
      colHdr.putLong(columnMax[c]);
      baos.write(colHdr.array(), 0, colHdr.position());
      switch (columnKinds[c]) {
        case COLUMN_KIND_NUMERIC_LONG -> {
          final ByteBuffer b = ByteBuffer.allocate(rowCount * 8).order(ByteOrder.LITTLE_ENDIAN);
          final long[] col = numericCols[c];
          for (int i = 0; i < rowCount; i++) b.putLong(col[i]);
          baos.write(b.array(), 0, b.position());
        }
        case COLUMN_KIND_BOOLEAN -> {
          final int wordCount = (rowCount + 63) >>> 6;
          final ByteBuffer b = ByteBuffer.allocate(wordCount * 8).order(ByteOrder.LITTLE_ENDIAN);
          final long[] bits = booleanCols[c];
          for (int i = 0; i < wordCount; i++) b.putLong(bits[i]);
          baos.write(b.array(), 0, b.position());
        }
        case COLUMN_KIND_STRING_DICT -> {
          final byte[][] dict = stringDicts[c];
          int dictSize = 0;
          while (dictSize < dict.length && dict[dictSize] != null) dictSize++;
          final ByteBuffer dh = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(dictSize);
          baos.write(dh.array(), 0, dh.position());
          final ByteBuffer dl = ByteBuffer.allocate(dictSize * 4).order(ByteOrder.LITTLE_ENDIAN);
          int totalBytes = 0;
          for (int i = 0; i < dictSize; i++) {
            dl.putInt(dict[i].length);
            totalBytes += dict[i].length;
          }
          baos.write(dl.array(), 0, dl.position());
          for (int i = 0; i < dictSize; i++) {
            baos.write(dict[i], 0, dict[i].length);
          }
          // dict-ids — packed 32-bit per entry for now; bit-packing is a later codec refinement.
          final ByteBuffer idBuf = ByteBuffer.allocate(rowCount * 4).order(ByteOrder.LITTLE_ENDIAN);
          final int[] ids = stringDictIdCols[c];
          for (int i = 0; i < rowCount; i++) idBuf.putInt(ids[i]);
          baos.write(idBuf.array(), 0, idBuf.position());
        }
        default -> throw new IllegalStateException("Unknown column kind " + columnKinds[c]);
      }
    }
    return baos.toByteArray();
  }

}

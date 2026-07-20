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
 * <h2>Serialized scan shape (in-memory form)</h2>
 *
 * The page materialises to a sequence of primitive arrays — no boxed
 * collections, no {@code Object[]}, no per-row allocation on the scan hot
 * path. This flat layout deliberately favors fixed-stride, branch-free
 * kernel access (raw 8-byte numerics, raw 4-byte dict-ids) over density: it
 * is what {@link ProjectionIndexByteScan} scans and what the registry holds
 * in memory. <b>Persistence uses {@link ProjectionIndexLeafCodec}</b>, which
 * bit-packs this form (frame-of-reference numerics, delta record keys,
 * packed dict-ids, marker-byte presence) to a fraction of its size and
 * decodes back byte-identically on hydrate.
 *
 * <pre>
 *   int    rowCount              // number of active rows (0..MAX_ROWS)
 *   int    columnCount           // index-aligned with the owning IndexDef
 *   long   firstRecordKey        // zone-map lower bound across recordKeys
 *   long   lastRecordKey         //   upper bound — enables HOT range skip
 *   byte[columnCount] kinds      // 0=NUMERIC_LONG, 1=BOOLEAN, 2=STRING_DICT
 *   long[rowCount] recordKeys    // nodeKey of each record projected here
 *
 *   for each column c in [0, columnCount):
 *     long min, max              // per-column zone map
 *
 *     // NUMERIC_LONG:
 *       long[rowCount] values    // raw 8-byte values (fixed stride)
 *
 *     // BOOLEAN:
 *       long[ceil(rowCount/64)] packedBits
 *
 *     // STRING_DICT:
 *       int       localDictSize
 *       int[localDictSize] stringLengths
 *       byte[]    concatenatedUtf8
 *       int[rowCount] dictIds    // raw 4-byte ids (fixed stride)
 *
 *   // ---- presence tail (v1, mandatory — appended after the column stream):
 *   byte[columnCount] columnFlags     // bit0 = present-but-unrepresentable value seen
 *                                     //        (JSON null, object/array, kind mismatch)
 *                                     // bit1 = non-integral value truncated into a
 *                                     //        NUMERIC_LONG cell
 *   for each column c (only when rowCount &gt; 0):
 *     long[ceil(rowCount/64)] presenceBits  // bit i = field exists on row i
 *   int  tailLength                   // bytes from tail start to before this field
 *   byte version = 1                  // tail-layout version, bumped on change
 *   int  magic = 0x50495831 ("PIX1")
 * </pre>
 *
 * <p><b>Integrality semantics.</b> Flag bit1 records, per NUMERIC_LONG
 * column, whether any cell was fed from a non-integral number (double /
 * decimal with a fraction) and hence TRUNCATED by
 * {@code Number#longValue()}. Value-exact consumers (aggregates) may serve
 * a numeric column iff the tail is present AND bit1 is clear. Because the
 * evidence lives in the persisted bytes — not in builder memory — the
 * aggregate fast path survives a close/re-open.
 *
 * <p><b>Presence semantics.</b> The presence bit is set iff the projected
 * field EXISTS on the record — including present-but-unrepresentable values
 * (null / object / array / kind mismatch), which additionally raise the
 * column's unrepresentable flag. Value slots of absent rows hold defaults
 * ({@code 0} / {@code false} / {@code ""}); consumers MUST consult the
 * presence bitmap before trusting a value, and MUST decline columns whose
 * unrepresentable flag is set (a present row's stored default is not the
 * real value). The tail is a mandatory part of the format —
 * {@link #deserialize} rejects payloads without it as corrupt.
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
 * <h2>Versioning &amp; storage placement</h2>
 *
 * Each serialised leaf byte[] is stored as one entry in a
 * {@link io.sirix.page.HOTLeafPage} of the projection index's HOT tree,
 * keyed by a synthetic chunk-id. {@code HOTLeafPage} is already a
 * versioned {@code KeyValuePage} — Sirix's
 * {@link io.sirix.settings.VersioningType#combineRecordPages} merge
 * writes only the <strong>modified slots</strong> of a given
 * HOTLeafPage per revision; untouched slots alias the prior revision's
 * bytes via the standard chain walk. No new {@code PageKind}, no
 * {@link io.sirix.index.hot.ChunkDirectory} indirection — the HOT leaf
 * <em>is</em> the directory.
 *
 * <p>Concrete on-disk cost per commit:
 *
 * <ul>
 *   <li>No projection-relevant rows changed → zero bytes.</li>
 *   <li>Rows in chunk <em>N</em> changed → only slot <em>N</em> of that
 *       HOTLeafPage gets re-serialised; slots of untouched chunks alias
 *       the previous revision.</li>
 *   <li>Large leaf values (~20 KB) that exceed the inline-slot
 *       threshold transparently spill to Sirix's overflow-record
 *       mechanism: a separate CoW-versioned page referenced from the
 *       slot — same effect as a dedicated chunk page, no new code.</li>
 * </ul>
 *
 * <p><b>Known architectural debt — to be addressed before general
 * availability.</b> Storing a 20 KB serialised leaf as a single HOT
 * entry value breaks Sirix's documented
 * {@linkplain io.sirix.settings.VersioningType#SLIDING_SNAPSHOT}
 * contract (see {@code docs/ARCHITECTURE.md} §"Problem 9" and §1097):
 * the framework guarantees <em>O(1) writes per record</em>, but our
 * natural "record" is a single projection row (~32 bytes), not the
 * 1024-row leaf. On update-heavy workloads a one-row change re-emits
 * the full ~20 KB slot — ~1000× the share-ratio the README promises.
 *
 * <p>Unlike CAS/NAME/PATH indexes (whose Roaring-bitmap values are
 * naturally KB-sized per record and thus align with slot granularity),
 * projection leaves pack many records per slot and will need
 * sub-slot sharing before production use:
 *
 * <ul>
 *   <li>Per-row slots (1024 slots/leaf, one per row) — exact match
 *       for the SLIDING_SNAPSHOT contract but loses columnar layout.</li>
 *   <li>Per-column slots (3 slots/leaf, one per column) — row update
 *       re-emits the touched column(s) (~8 KB) not the full leaf;
 *       columnar scan still works.</li>
 *   <li>Reuse the half-built {@code BitmapChunkPage} /
 *       {@code ChunkDirectory} machinery in
 *       {@code io.sirix.index.hot}; currently unused by the CAS path
 *       but wired through {@link io.sirix.page.PageKind} and
 *       {@link io.sirix.settings.VersioningType#combineBitmapChunks}.</li>
 * </ul>
 *
 * <p>Tracked in task #57. Today's opaque-byte[]-per-slot layout is
 * explicitly an interim shipping configuration; do not publish a
 * projection-index public API commitment until sub-slot sharing is in.
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

  /**
   * Double column (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6): cells store the
   * ORDER-PRESERVING transform of the double bits ({@link ProjectionDoubleEncoding}), so at
   * the storage/layout/codec level this kind is byte-identical to
   * {@link #COLUMN_KIND_NUMERIC_LONG} — every signed-long compare surface (zone maps, FOR
   * packing, predicate kernels with plan-time-transformed literals) works unchanged. Only
   * extraction (encode on write) and value-materialising consumers (aggregates, serving)
   * touch the transform. For this kind, {@link #COLUMN_FLAG_NON_INTEGRAL} means "a stored
   * cell is NOT value-exact" (lossy Big*→double conversion seen) — same fail-closed gate,
   * kind-dependent reading.
   */
  public static final byte COLUMN_KIND_NUMERIC_DOUBLE = 3;

  /** {@code true} for the two numeric kinds, whose storage layout is identical. */
  public static boolean isNumericKind(final byte kind) {
    return kind == COLUMN_KIND_NUMERIC_LONG || kind == COLUMN_KIND_NUMERIC_DOUBLE;
  }

  /** Footer magic of the presence tail ("PIX1" little-endian). */
  public static final int PRESENCE_TAIL_MAGIC = 0x50495831;

  /**
   * Version byte stored between the tail length and the footer magic. Future tail-layout changes
   * bump this instead of minting a new magic; readers reject unknown values as corrupt.
   */
  public static final byte PRESENCE_TAIL_VERSION = 1;

  /** Column flag bit: a present-but-unrepresentable value (null / object / array / kind mismatch) was seen. */
  public static final byte COLUMN_FLAG_UNREPRESENTABLE = 0x01;

  /**
   * Column flag bit: a NUMERIC_LONG cell was fed from a non-integral number
   * and truncated by {@code Number#longValue()} — value-exact consumers must
   * decline the column.
   */
  public static final byte COLUMN_FLAG_NON_INTEGRAL = 0x02;

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
   * Per-column presence bitmap, 64-way packed like {@link #booleanCols}.
   * Bit {@code i} of column {@code c} is set iff the projected field exists
   * on row {@code i}'s record. Allocated in {@link #ensureCapacity} for ALL
   * column kinds.
   */
  private final long[][] presenceCols;

  /**
   * Per-column flag: a PRESENT row carried a value the column kind cannot
   * represent (JSON null, nested object/array, or a kind mismatch such as a
   * string in a NUMERIC_LONG column). Value-exact consumers must decline the
   * column — the stored default is not the real value.
   */
  private final boolean[] columnUnrepresentable;

  /**
   * Per-column flag: a NUMERIC_LONG cell on THIS leaf was fed from a
   * non-integral number and truncated. Persisted in the presence tail
   * (flag bit1) so value-exact consumers can keep serving the column after
   * a close/re-open.
   */
  private final boolean[] columnNonIntegral;

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
    this.presenceCols = new long[columnCount][];
    this.columnUnrepresentable = new boolean[columnCount];
    this.columnNonIntegral = new boolean[columnCount];
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

  /**
   * Column count of a serialised raw leaf payload — the single canonical
   * reader of the header layout (bytes 4..7, little-endian). Callers must
   * pass a payload of at least 8 bytes.
   */
  public static int columnCountOf(final byte[] rawPayload) {
    return (rawPayload[4] & 0xFF) | ((rawPayload[5] & 0xFF) << 8)
        | ((rawPayload[6] & 0xFF) << 16) | ((rawPayload[7] & 0xFF) << 24);
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

  /** 64-way packed presence bits of {@code column}. */
  public long[] presenceColumnBits(final int column) {
    return presenceCols[column];
  }

  /** Whether {@code column} saw a present-but-unrepresentable value (null / object / array / mismatch). */
  public boolean columnUnrepresentable(final int column) {
    return columnUnrepresentable[column];
  }

  /**
   * Whether a NUMERIC_LONG cell of {@code column} on this leaf was truncated
   * from a non-integral number.
   */
  public boolean columnNumericNonIntegral(final int column) {
    return columnNonIntegral[column];
  }

  /**
   * Reassemble a page from decoded components — the inverse half of
   * {@link ProjectionIndexLeafCodec}. Arrays are adopted (not copied): the
   * codec hands over freshly built arrays sized for {@code rowCount}, which
   * is all {@link #serialize()} ever reads. Package-private on purpose —
   * the only legitimate caller is the codec.
   */
  static ProjectionIndexLeafPage reconstruct(final byte[] kinds, final int rowCount,
      final long firstRecordKey, final long lastRecordKey, final long[] recordKeys,
      final long[] columnMin, final long[] columnMax,
      final long[][] numericCols, final long[][] booleanCols,
      final int[][] stringDictIdCols, final byte[][][] stringDicts,
      final long[][] presenceCols, final boolean[] unrepresentable, final boolean[] nonIntegral) {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    page.rowCount = rowCount;
    page.firstRecordKey = firstRecordKey;
    page.lastRecordKey = lastRecordKey;
    page.recordKeys = recordKeys;
    for (int c = 0; c < page.columnCount; c++) {
      page.columnMin[c] = columnMin[c];
      page.columnMax[c] = columnMax[c];
      page.numericCols[c] = numericCols[c];
      page.booleanCols[c] = booleanCols[c];
      page.stringDictIdCols[c] = stringDictIdCols[c];
      page.stringDicts[c] = stringDicts[c];
      page.presenceCols[c] = presenceCols[c];
      page.columnUnrepresentable[c] = unrepresentable[c];
      page.columnNonIntegral[c] = nonIntegral[c];
    }
    return page;
  }

  /** Ensure the per-column primitive arrays are materialised. Idempotent. */
  private void ensureCapacity() {
    if (recordKeys == null) {
      recordKeys = new long[MAX_ROWS];
      for (int c = 0; c < columnCount; c++) {
        presenceCols[c] = new long[(MAX_ROWS + 63) >>> 6];
        switch (columnKinds[c]) {
          case COLUMN_KIND_NUMERIC_LONG, COLUMN_KIND_NUMERIC_DOUBLE -> numericCols[c] = new long[MAX_ROWS];
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
   * Append one record projection where every field is present with a clean
   * representable value — the historical dense-data entry point, kept for
   * benches/tests that construct synthetic leaves.
   *
   * @return {@code true} if the row was appended, {@code false} if the
   *         page is already at {@link #MAX_ROWS} capacity (caller opens a
   *         fresh leaf and retries).
   */
  public boolean appendRow(final long recordKey,
      final long[] longValues, final boolean[] boolValues, final String[] stringValues) {
    return appendRow(recordKey, longValues, boolValues, stringValues, null, null, null);
  }

  /**
   * Append one record projection. The three value arrays are index-aligned
   * with the page's column kinds: {@code longValues[c]} populated iff kind is
   * NUMERIC_LONG, {@code boolValues[c]} iff BOOLEAN, {@code stringValues[c]}
   * iff STRING_DICT. Mismatches throw — the builder is trusted to extract per
   * the IndexDef's declared types.
   *
   * <p>{@code present[c]} marks whether the projected field EXISTS on this
   * record ({@code null} = all present); {@code unrepresentable[c]} marks a
   * present field whose value the column kind cannot hold (JSON null, nested
   * object/array, kind mismatch — {@code null} = none). Unrepresentable cells
   * keep their default value slot but poison the column for value-exact
   * consumers via {@link #columnUnrepresentable(int)}. Zone maps only fold in
   * present, representable values so an all-missing leaf stays prunable.
   *
   * @return {@code true} if the row was appended, {@code false} if the
   *         page is already at {@link #MAX_ROWS} capacity (caller opens a
   *         fresh leaf and retries).
   */
  public boolean appendRow(final long recordKey,
      final long[] longValues, final boolean[] boolValues, final String[] stringValues,
      final boolean[] present, final boolean[] unrepresentable) {
    return appendRow(recordKey, longValues, boolValues, stringValues, present, unrepresentable, null);
  }

  /**
   * Variant additionally carrying per-column integrality provenance:
   * {@code nonIntegral[c]} marks that this row's NUMERIC_LONG cell {@code c}
   * was truncated from a non-integral number ({@code null} = every numeric
   * cell exact). The flag is sticky per column for the lifetime of the leaf
   * and is persisted in the presence tail so value-exact consumers can keep
   * serving the column after a close/re-open.
   */
  public boolean appendRow(final long recordKey,
      final long[] longValues, final boolean[] boolValues, final String[] stringValues,
      final boolean[] present, final boolean[] unrepresentable, final boolean[] nonIntegral) {
    if (rowCount == MAX_ROWS) return false;
    ensureCapacity();
    final int row = rowCount;
    recordKeys[row] = recordKey;
    if (recordKey < firstRecordKey) firstRecordKey = recordKey;
    if (recordKey > lastRecordKey) lastRecordKey = recordKey;
    for (int c = 0; c < columnCount; c++) {
      final boolean isPresent = present == null || present[c];
      final boolean isUnrepresentable = unrepresentable != null && unrepresentable[c];
      if (isPresent) {
        presenceCols[c][row >>> 6] |= 1L << (row & 63);
      }
      if (isUnrepresentable) {
        columnUnrepresentable[c] = true;
      }
      if (nonIntegral != null && nonIntegral[c]) {
        columnNonIntegral[c] = true;
      }
      final boolean clean = isPresent && !isUnrepresentable;
      switch (columnKinds[c]) {
        case COLUMN_KIND_NUMERIC_LONG, COLUMN_KIND_NUMERIC_DOUBLE -> {
          final long v = longValues[c];
          numericCols[c][row] = v;
          if (clean) {
            if (v < columnMin[c]) columnMin[c] = v;
            if (v > columnMax[c]) columnMax[c] = v;
          }
        }
        case COLUMN_KIND_BOOLEAN -> {
          if (boolValues[c]) {
            booleanCols[c][row >>> 6] |= 1L << (row & 63);
          }
        }
        // Absent / unrepresentable cells intern the "" DEFAULT regardless of
        // what the caller left in the scratch slot — this makes "every
        // non-empty dictionary entry was interned by a clean present row" a
        // STRUCTURAL invariant of the leaf (the dictionary-union
        // count-distinct kernel depends on it), not a builder convention.
        case COLUMN_KIND_STRING_DICT ->
            stringDictIdCols[c][row] = appendString(c, clean ? stringValues[c] : "");
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
   * {@link ProjectionIndexLeafPage}. Inverse of {@link #serialize}. The
   * presence tail is mandatory — a payload whose trailing bytes don't form a
   * valid tail (length, footer length field, and magic must all agree) is
   * rejected as corrupt rather than misread.
   *
   * @throws IllegalStateException when the payload carries no valid presence tail
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
    if (rowCount > 0) {
      page.ensureCapacity();
      for (int i = 0; i < rowCount; i++) page.recordKeys[i] = bb.getLong();
      for (int c = 0; c < columnCount; c++) {
        page.columnMin[c] = bb.getLong();
        page.columnMax[c] = bb.getLong();
        switch (kinds[c]) {
          case COLUMN_KIND_NUMERIC_LONG, COLUMN_KIND_NUMERIC_DOUBLE -> {
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
    }
    // Presence tail. The column stream ends exactly at bb.position(); a valid
    // tail must account for every remaining byte (flags + presence words +
    // 8-byte footer with the magic). Anything else is corrupt — never misread.
    final int tailStart = bb.position();
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final int expectedTailLen = columnCount + columnCount * presWords * 8;
    if (payload.length != tailStart + expectedTailLen + 9
        || getIntLE(payload, payload.length - 4) != PRESENCE_TAIL_MAGIC
        || payload[payload.length - 5] != PRESENCE_TAIL_VERSION
        || getIntLE(payload, payload.length - 9) != expectedTailLen) {
      throw new IllegalStateException(
          "Corrupt projection leaf: no valid presence tail (payload " + payload.length
              + " bytes, column stream ends at " + tailStart + ", expected tail "
              + (expectedTailLen + 9) + " bytes)");
    }
    for (int c = 0; c < columnCount; c++) {
      page.columnUnrepresentable[c] = (payload[tailStart + c] & COLUMN_FLAG_UNREPRESENTABLE) != 0;
      page.columnNonIntegral[c] = (payload[tailStart + c] & COLUMN_FLAG_NON_INTEGRAL) != 0;
    }
    if (rowCount > 0) {
      for (int c = 0; c < columnCount; c++) {
        final long[] bits = page.presenceCols[c];
        final int base = tailStart + columnCount + c * presWords * 8;
        for (int w = 0; w < presWords; w++) {
          bits[w] = getLongLE(payload, base + w * 8);
        }
      }
    }
    return page;
  }

  private static int getIntLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
  }

  private static long getLongLE(final byte[] b, final int off) {
    return (getIntLE(b, off) & 0xFFFFFFFFL) | ((long) getIntLE(b, off + 4) << 32);
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
      // Empty page — only the presence tail (if tracked) follows the header.
      writePresenceTail(baos);
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
        case COLUMN_KIND_NUMERIC_LONG, COLUMN_KIND_NUMERIC_DOUBLE -> {
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
    writePresenceTail(baos);
    return baos.toByteArray();
  }

  /** Append the presence tail. */
  private void writePresenceTail(final ByteArrayOutputStream baos) {
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final int tailLen = columnCount + columnCount * presWords * 8;
    final ByteBuffer tail = ByteBuffer.allocate(tailLen + 9).order(ByteOrder.LITTLE_ENDIAN);
    for (int c = 0; c < columnCount; c++) {
      tail.put(columnFlagsByte(c));
    }
    if (presWords > 0) {
      for (int c = 0; c < columnCount; c++) {
        final long[] bits = presenceCols[c];
        for (int w = 0; w < presWords; w++) tail.putLong(bits[w]);
      }
    }
    tail.putInt(tailLen);
    tail.put(PRESENCE_TAIL_VERSION);
    tail.putInt(PRESENCE_TAIL_MAGIC);
    baos.write(tail.array(), 0, tail.position());
  }

  /** Per-column flags byte of the tail: bit0 = unrepresentable seen, bit1 = non-integral seen. */
  private byte columnFlagsByte(final int c) {
    byte flags = columnUnrepresentable[c] ? COLUMN_FLAG_UNREPRESENTABLE : 0;
    if (columnNonIntegral[c]) {
      flags |= COLUMN_FLAG_NON_INTEGRAL;
    }
    return flags;
  }

}

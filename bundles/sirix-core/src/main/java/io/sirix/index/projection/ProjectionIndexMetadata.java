/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Self-describing metadata payload persisted alongside projection leaves (slot 0 of the HOT
 * sub-tree): the projection's root path, per-column field paths, column names, and column kinds.
 * {@link #rowGroupCount()} is the live logical leaf cardinality; physical leaf ids and their
 * document order live in {@link ProjectionIndexFences} and need not be contiguous after local
 * split/recycle maintenance. Hydration reads the projection's shape from HERE instead of trusting
 * the caller's argument list — without it, a re-create with a same-arity but different field list
 * would silently install the persisted columns under the wrong names (the exact corruption the
 * column-count guard alone cannot catch).
 *
 * <p>
 * Wire form: {@link #MAGIC} ("PIXM" little-endian), a version byte, a flags byte
 * ({@link #FLAG_STALE}), the leaf count and build revision as little-endian ints, the root path as
 * a length-prefixed UTF-8 string, an int column count, then per column: path (UTF-8,
 * length-prefixed), name (UTF-8, length-prefixed), and one column-kind byte
 * ({@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_LONG} / {@code BOOLEAN} /
 * {@code STRING_DICT}).
 *
 * <p>
 * The per-leaf {@code (firstRecordKey, lastRecordKey)} fences — the incremental maintenance's zone
 * map — used to ride inside this blob, but at scale that made every commit re-persist the whole
 * fence array (~1.5&nbsp;MB at 100k leaves) just because one leaf moved. They now live in their own
 * carry-forward chunks ({@link ProjectionIndexFences}), so this metadata blob stays tiny (shape
 * only) and a commit rewrites only the fence chunks it actually changed.
 *
 * <p>
 * The <b>stale</b> flag is the fail-closed marker used by abandoned builds and legacy listeners
 * that cannot maintain a live snapshot. Ordinary update-time maintenance never installs it. The
 * live leaf count is cross-checked with the fence/order header, whose explicit physical order
 * prevents recycled holes or unrelated higher physical ids from being interpreted as live.
 *
 * <p>
 * {@link #parse} returns {@code null} for payloads without the magic, so hydrate paths can probe
 * slot 0 and fall back to metadata-less handling for stores written by the bench setups (which
 * persist leaves only).
 */
public final class ProjectionIndexMetadata {

  /** Leading magic of a metadata payload ("PIXM" little-endian). */
  public static final int MAGIC = 0x4D585049;

  /** Flags bit0: the projection was invalidated by an update transaction. */
  public static final byte FLAG_STALE = 0x01;

  /**
   * Wire-format version. Version zero stores set-summary capabilities separately from bounded chunks.
   * Unknown versions are declined rather than interpreted with shifted fields.
   */
  private static final byte VERSION = 0;

  private final String rootPath;
  private final String[] fieldPaths;
  private final String[] fieldNames;
  private final byte[] columnKinds;
  private final int rowGroupCount;
  private final int buildRevision;

  private final byte flags;

  /**
   * Per set column, either hydrated row counts or an empty persisted capability marker.
   *
   * <p>
   * Indexed by column, {@code null} for columns without one. The current format serializes only the
   * column keys; the counts live in bounded per-column chunks and are hydrated by the catalog before
   * serving.
   *
   * <p>
   * ONE bounded chunk per summarized column, not one per leaf. This cannot serve a count restricted
   * to a subset of rows, and the caller's gate keeps it from being asked to.
   *
   * <p>
   * Summing per-leaf counts is exact because a record lives in exactly one leaf, and the per-leaf
   * counts already count rows rather than occurrences — a record listing the same value twice
   * contributes one.
   */
  private final Map<Integer, Map<String, Long>> setValueRowCounts;

  /**
   * Per {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} column, the node key of its
   * value dictionary's header record; {@code 0} for every other column.
   *
   * <p>
   * This is the only pointer to the dictionary, and it has to live here rather than be computed from
   * the column's identity: the dictionary's records occupy a run of node keys reserved from a shared
   * counter, so where a column's run starts is a fact about a particular build, not a function of
   * which column it is. See {@code GlobalValueDictionary} for why a computed namespace is not an
   * option.
   *
   * <p>
   * Written as a trailing section so every current metadata record carries an explicit stable anchor
   * for each global column.
   */
  private final long[] valueDictionaryHeaderKeys;

  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, null, null);
  }

  /** As above, carrying the index-wide {@link #setValueRowCounts}. */
  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision,
      final Map<Integer, Map<String, Long>> setValueRowCounts) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, setValueRowCounts,
        null);
  }

  /** As above, carrying the per-column value dictionary header keys. */
  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision,
      final Map<Integer, Map<String, Long>> setValueRowCounts, final long[] valueDictionaryHeaderKeys) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, setValueRowCounts,
        valueDictionaryHeaderKeys);
  }

  private ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision, final byte flags,
      final Map<Integer, Map<String, Long>> setValueRowCounts, final long[] valueDictionaryHeaderKeys) {
    Objects.requireNonNull(rootPath);
    Objects.requireNonNull(fieldPaths);
    Objects.requireNonNull(fieldNames);
    Objects.requireNonNull(columnKinds);
    this.setValueRowCounts = setValueRowCounts;
    this.valueDictionaryHeaderKeys = valueDictionaryHeaderKeys == null
        ? null
        : valueDictionaryHeaderKeys.clone();
    if (fieldPaths.length != fieldNames.length || fieldPaths.length != columnKinds.length) {
      throw new IllegalArgumentException("paths/names/kinds must be index-aligned");
    }
    if (columnKinds.length > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("column count exceeds " + RowGroupDescriptor.MAX_COLUMNS);
    }
    if (valueDictionaryHeaderKeys != null && valueDictionaryHeaderKeys.length != columnKinds.length) {
      throw new IllegalArgumentException("value dictionary anchors must be index-aligned with columns");
    }
    if (valueDictionaryHeaderKeys != null) {
      for (int column = 0; column < valueDictionaryHeaderKeys.length; column++) {
        final long key = valueDictionaryHeaderKeys[column];
        if (key < 0 || (key > 0 && columnKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL)) {
          throw new IllegalArgumentException("invalid value dictionary anchor " + key + " at column " + column);
        }
      }
    }
    for (int column = 0; column < columnKinds.length; column++) {
      if (columnKinds[column] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
          && (valueDictionaryHeaderKeys == null || valueDictionaryHeaderKeys[column] == 0)) {
        throw new IllegalArgumentException("global string column " + column + " requires a dictionary anchor");
      }
    }
    if (setValueRowCounts != null) {
      for (final int column : setValueRowCounts.keySet()) {
        if (column < 0 || column >= columnKinds.length
            || columnKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          throw new IllegalArgumentException("set-summary capability names non-set column " + column);
        }
      }
    }
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException(
          "rowGroupCount out of range [0, " + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + "]: " + rowGroupCount);
    }
    if (buildRevision < 0) {
      throw new IllegalArgumentException("buildRevision must be >= 0, got " + buildRevision);
    }
    this.rootPath = rootPath;
    this.fieldPaths = fieldPaths.clone();
    this.fieldNames = fieldNames.clone();
    this.columnKinds = columnKinds.clone();
    this.rowGroupCount = rowGroupCount;
    this.buildRevision = buildRevision;
    this.flags = flags;
  }

  /** Minimal stale marker the change listener writes over slot 0 on invalidation. */
  public static ProjectionIndexMetadata staleTombstone() {
    return new ProjectionIndexMetadata("", new String[0], new String[0], new byte[0], 0, 0, FLAG_STALE, null, null);
  }


  public String rootPath() {
    return rootPath;
  }

  public String[] fieldPaths() {
    return fieldPaths.clone();
  }

  public String[] fieldNames() {
    return fieldNames.clone();
  }

  /**
   * Per column, its declared path RELATIVE to {@link #rootPath()} — {@code "age"} for {@code /[]/age}
   * under {@code /[]}, {@code "commit/collection"} for {@code /[]/commit/collection},
   * {@code "genres"} for the set column {@code /[]/genres/[]}. This is what a query's deref CHAIN is
   * matched against, so a nested column can never answer a top-level deref of the same trailing name
   * (or the reverse). {@code null} at a slot whose declared path is not relativizable against the
   * root — the match then falls back to the trailing name, i.e. to the historical behavior.
   *
   * @see ProjectionIndexRegistry.Handle#columnOf(String)
   */
  public String[] fieldChains() {
    return relativeFieldChains(rootPath, fieldPaths);
  }

  /**
   * {@link #fieldChains()} over an explicit root and declared paths (the catalog's def-side twin).
   */
  public static String[] relativeFieldChains(final String rootPath, final String[] fieldPaths) {
    final String[] chains = new String[fieldPaths.length];
    for (int i = 0; i < chains.length; i++) {
      chains[i] = relativeFieldChain(rootPath, fieldPaths[i]);
    }
    return chains;
  }

  /**
   * One column's declared path relative to the record root, with trailing array steps stripped (a set
   * column is declared at its array layer but IS its field), or {@code null} when the path does not
   * sit strictly under the root — a shape the creation function rejects, so the fallback exists only
   * so an unexpected declaration degrades to name matching instead of becoming unservable.
   */
  public static String relativeFieldChain(final String rootPath, final String fieldPath) {
    if (rootPath == null || rootPath.isEmpty() || fieldPath == null) {
      return null;
    }
    String path = fieldPath;
    while (path.endsWith("/[]")) {
      path = path.substring(0, path.length() - 3);
    }
    if (path.length() <= rootPath.length() + 1 || !path.startsWith(rootPath) || path.charAt(rootPath.length()) != '/') {
      return null;
    }
    return path.substring(rootPath.length() + 1);
  }

  public byte[] columnKinds() {
    return columnKinds.clone();
  }

  /**
   * Number of live logical row-group leaves. Physical ids/order are persisted separately and may
   * contain recycled holes or live ids greater than this cardinality.
   */
  public int rowGroupCount() {
    return rowGroupCount;
  }

  /**
   * Revision the columns were built over — hydration installs the registry handle with this as its
   * valid-from revision, so time-travel executors bound to earlier revisions refuse it.
   */
  public int buildRevision() {
    return buildRevision;
  }

  /**
   * Whether slot 0 carries the stale tombstone: a dropped definition, an unfinished load-time build,
   * or the corruption valve. Ordinary maintenance fails its transaction instead of setting this.
   */
  public boolean isStale() {
    return (flags & FLAG_STALE) != 0;
  }



  /**
   * Whether this metadata describes exactly the given shape.
   *
   * <p>
   * Column kinds are compared up to the choice of string dictionary. A caller derives the expected
   * kinds from the definition's declared TYPES, which can only ever yield
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT} for a string column — whereas whether
   * the build chose a per-leaf or a resource-wide dictionary is a property of the data it saw,
   * decided while the build ran. Comparing the two byte-for-byte would make every store with a global
   * dictionary look like a shape mismatch and be rebuilt on sight, forever. The shape this check is
   * about is what the projection projects, not how a column encodes its strings.
   */
  public boolean matches(final String otherRootPath, final String[] otherFieldPaths, final byte[] otherColumnKinds) {
    if (!rootPath.equals(otherRootPath) || !Arrays.equals(fieldPaths, otherFieldPaths)
        || columnKinds.length != otherColumnKinds.length) {
      return false;
    }
    for (int c = 0; c < columnKinds.length; c++) {
      if (!sameDeclaredShape(columnKinds[c], otherColumnKinds[c])) {
        return false;
      }
    }
    return true;
  }

  /** Whether two column kinds describe the same declared column, ignoring the dictionary choice. */
  private static boolean sameDeclaredShape(final byte persisted, final byte derived) {
    return persisted == derived || (persisted == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
        && derived == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT);
  }

  public byte[] serialize() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    putIntLE(out, MAGIC);
    out.write(VERSION);
    out.write(flags);
    putIntLE(out, rowGroupCount);
    putIntLE(out, buildRevision);
    putString(out, rootPath);
    putIntLE(out, fieldPaths.length);
    for (int i = 0; i < fieldPaths.length; i++) {
      putString(out, fieldPaths[i]);
      putString(out, fieldNames[i]);
      out.write(columnKinds[i]);
    }
    writeSetValueRowCounts(out);
    writeValueDictionaryHeaderKeys(out);
    return out.toByteArray();
  }

  /**
   * Rows in the index whose set at {@code column} contains {@code value}.
   *
   * @return the count, {@code 0} for a value the index does not hold, or {@code null} when the column
   *         carries no summary and the caller must fall back
   */
  public Long setValueRowCount(final int column, final String value) {
    if (setValueRowCounts == null) {
      return null;
    }
    final Map<String, Long> forColumn = setValueRowCounts.get(column);
    if (forColumn == null || forColumn.isEmpty()) {
      return null;
    }
    // The map lists every value the index holds for this column, so a miss is a real zero rather
    // than an unknown — which is what makes an absent literal answerable in the same O(1).
    final Long count = forColumn.get(value);
    return count == null
        ? Long.valueOf(0)
        : count;
  }

  /**
   * Set-summary capability columns. Persisted metadata carries empty maps; hydrated in-memory
   * metadata may carry their counts.
   */
  public Map<Integer, Map<String, Long>> setValueRowCounts() {
    return setValueRowCounts;
  }

  /** Append the set-summary capability columns. */
  private void writeSetValueRowCounts(final ByteArrayOutputStream out) {
    if (setValueRowCounts == null || setValueRowCounts.isEmpty()) {
      putShortLE(out, 0);
      return;
    }
    putShortLE(out, setValueRowCounts.size());
    for (final int column : setValueRowCounts.keySet()) {
      putShortLE(out, column);
      putShortLE(out, 0);
    }
  }

  private static void putShortLE(final ByteArrayOutputStream out, final int v) {
    out.write(v & 0xFF);
    out.write((v >>> 8) & 0xFF);
  }

  /**
   * Parse a metadata payload; {@code null} when {@code payload} does not carry the metadata magic
   * (e.g. a leaf payload from a metadata-less store).
   *
   * @throws IllegalStateException on a structurally corrupt metadata payload
   */
  public static ProjectionIndexMetadata parse(final byte[] payload) {
    if (payload == null || payload.length < 6 || getIntLE(payload, 0) != MAGIC) {
      return null;
    }
    try {
      final int[] pos = {4};
      final byte version = payload[pos[0]++];
      if (version != VERSION) {
        // Older/newer wire format — treated like "no metadata" so callers decline instead of
        // misparsing bytes at shifted offsets.
        return null;
      }
      final byte flags = payload[pos[0]++];
      final int rowGroupCount = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        throw new IllegalStateException("Implausible projection leaf count " + rowGroupCount);
      }
      final int buildRevision = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (buildRevision < 0) {
        throw new IllegalStateException("Implausible projection build revision " + buildRevision);
      }
      final String rootPath = getString(payload, pos);
      final int n = getIntLE(payload, pos[0]);
      pos[0] += 4;
      // Bound by the SAME cap the write path enforces (RowGroupDescriptor.MAX_COLUMNS), not a
      // separate literal. A reader cap below the writer's opens a window where an index persists
      // successfully and then can never be parsed back: the catalog negative-caches it as unusable
      // and every rebuild re-writes the same unreadable store. This guard exists only to reject an
      // implausible count from a corrupt payload before it drives the allocations below.
      if (n < 0 || n > RowGroupDescriptor.MAX_COLUMNS) {
        throw new IllegalStateException("Implausible projection column count " + n);
      }
      final String[] paths = new String[n];
      final String[] names = new String[n];
      final byte[] kinds = new byte[n];
      for (int i = 0; i < n; i++) {
        paths[i] = getString(payload, pos);
        names[i] = getString(payload, pos);
        kinds[i] = payload[pos[0]++];
      }
      // The counts section follows the per-field section. Its presence is guaranteed by the
      // version byte, which parse() has already checked — a payload from before it existed fails
      // the version test above and is treated as no metadata, so there is no shifted-offset read.
      final int setCountColumns = getShortU(payload, pos);
      if (setCountColumns > n) {
        throw new IllegalStateException(
            "Projection metadata declares " + setCountColumns + " set-summary capabilities for " + n + " columns");
      }
      Map<Integer, Map<String, Long>> counts = null;
      for (int c = 0; c < setCountColumns; c++) {
        final int column = getShortU(payload, pos);
        final int values = getShortU(payload, pos);
        if (column >= n || kinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET || values != 0) {
          throw new IllegalStateException(
              "Projection metadata names an invalid set-summary capability at column " + column);
        }
        if (counts == null) {
          counts = new LinkedHashMap<>(4);
        }
        if (counts.put(column, new LinkedHashMap<>()) != null) {
          throw new IllegalStateException("Projection metadata repeats set-summary column " + column);
        }
      }
      // Every current payload carries the dictionary section, including an explicit zero count.
      long[] dictionaryKeys = null;
      final int remaining = payload.length - pos[0];
      if (remaining < 2) {
        throw new IllegalStateException(
            "Projection metadata has " + remaining + " byte(s), too few for a value dictionary section");
      }
      final int dictionaryColumns = getShortU(payload, pos);
      if (dictionaryColumns > 0) {
        if (dictionaryColumns > n) {
          throw new IllegalStateException(
              "Projection metadata declares " + dictionaryColumns + " value dictionaries for " + n + " columns");
        }
        dictionaryKeys = new long[n];
        for (int i = 0; i < dictionaryColumns; i++) {
          final int column = getShortU(payload, pos);
          final long headerKey = getLongLE(payload, pos[0]);
          pos[0] += 8;
          if (column >= n || headerKey <= 0 || kinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
              || dictionaryKeys[column] != 0) {
            throw new IllegalStateException(
                "Projection metadata names value dictionary " + headerKey + " for column " + column);
          }
          dictionaryKeys[column] = headerKey;
        }
      }
      if (pos[0] != payload.length) {
        throw new IllegalStateException(
            "Projection metadata has " + (payload.length - pos[0]) + " byte(s) past the value dictionary section");
      }
      return new ProjectionIndexMetadata(rootPath, paths, names, kinds, rowGroupCount, buildRevision, flags, counts,
          dictionaryKeys);
    } catch (final IndexOutOfBoundsException truncated) {
      throw new IllegalStateException("Corrupt projection metadata payload", truncated);
    }
  }

  /**
   * The node key of column {@code column}'s value dictionary header, or {@code 0} when the column has
   * none, which is every column that is not
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL}.
   *
   * @param column the column ordinal
   * @return the header node key, or {@code 0}
   */
  public long valueDictionaryHeaderKey(final int column) {
    return valueDictionaryHeaderKeys == null || column < 0 || column >= valueDictionaryHeaderKeys.length
        ? 0L
        : valueDictionaryHeaderKeys[column];
  }

  /**
   * Every column's value dictionary anchor, index-aligned with the columns; {@code null} when the
   * index carries none at all.
   */
  public long @Nullable [] valueDictionaryHeaderKeys() {
    return hasValueDictionaries()
        ? valueDictionaryHeaderKeys.clone()
        : null;
  }

  /** Whether any column of this index carries a global value dictionary. */
  public boolean hasValueDictionaries() {
    if (valueDictionaryHeaderKeys == null) {
      return false;
    }
    for (final long key : valueDictionaryHeaderKeys) {
      if (key != 0L) {
        return true;
      }
    }
    return false;
  }

  /** Append the value dictionary section: one entry per column that has one. */
  private void writeValueDictionaryHeaderKeys(final ByteArrayOutputStream out) {
    int present = 0;
    if (valueDictionaryHeaderKeys != null) {
      for (final long key : valueDictionaryHeaderKeys) {
        if (key != 0L) {
          present++;
        }
      }
    }
    putShortU(out, present);
    if (present == 0) {
      return;
    }
    for (int c = 0; c < valueDictionaryHeaderKeys.length; c++) {
      if (valueDictionaryHeaderKeys[c] != 0L) {
        putShortU(out, c);
        putLongLE(out, valueDictionaryHeaderKeys[c]);
      }
    }
  }

  private static void putLongLE(final ByteArrayOutputStream out, final long value) {
    putIntLE(out, (int) value);
    putIntLE(out, (int) (value >>> 32));
  }

  private static long getLongLE(final byte[] payload, final int off) {
    return (getIntLE(payload, off) & 0xFFFFFFFFL) | ((long) getIntLE(payload, off + 4) << 32);
  }

  private static int getShortU(final byte[] payload, final int[] pos) {
    final int lo = payload[pos[0]++] & 0xFF;
    final int hi = payload[pos[0]++] & 0xFF;
    return lo | (hi << 8);
  }

  private static void putString(final ByteArrayOutputStream out, final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    putIntLE(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static String getString(final byte[] payload, final int[] pos) {
    final int len = getIntLE(payload, pos[0]);
    pos[0] += 4;
    if (len < 0 || pos[0] + len > payload.length) {
      throw new IndexOutOfBoundsException("string length " + len);
    }
    final String value = new String(payload, pos[0], len, StandardCharsets.UTF_8);
    pos[0] += len;
    return value;
  }

  private static void putShortU(final ByteArrayOutputStream out, final int v) {
    out.write(v & 0xFF);
    out.write((v >>> 8) & 0xFF);
  }

  private static void putIntLE(final ByteArrayOutputStream out, final int v) {
    out.write(v);
    out.write(v >>> 8);
    out.write(v >>> 16);
    out.write(v >>> 24);
  }

  private static int getIntLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
  }
}

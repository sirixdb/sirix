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
 * The <b>stale</b> flag is the fail-closed marker used when the virgin-tree initializer cannot
 * finish. Ordinary update-time maintenance never installs it: live trees are maintained only by
 * the incremental change listener. The live leaf count is cross-checked with the fence/order
 * header, whose explicit physical order prevents recycled holes or unrelated higher physical ids
 * from being interpreted as live.
 *
 * <p>
 * {@link #parse} returns {@code null} for payloads without the magic or with an unsupported version.
 * Production hydrate paths treat that result as unusable; there is no metadata-less persisted
 * projection format.
 */
public final class ProjectionIndexMetadata {

  /** Leading magic of a metadata payload ("PIXM" little-endian). */
  public static final int MAGIC = 0x4D585049;

  /** Flags bit0: the projection was invalidated by an update transaction. */
  public static final byte FLAG_STALE = 0x01;

  /**
   * Flags bits 1-3: WHY the projection went stale, as a {@link StaleReason} ordinal.
   *
   * <p>
   * Additive on purpose — the bits were previously always zero, so a tombstone written before this
   * existed parses as {@link StaleReason#UNSPECIFIED} and nothing about the wire form changes. No
   * version bump: readers that only test {@link #FLAG_STALE} are unaffected by bits they ignore.
   * </p>
   *
   * <p>
   * It exists because "stale" and "corrupt" are different claims and the difference was being lost. A
   * projection the writer deliberately retired because it cannot maintain a resource-wide dictionary
   * is a DECISION, and the component that made it is the only one that can explain it; every
   * downstream decline should be able to quote that reason rather than invent one. See the
   * kind-inconsistency class recorded in tasks #45 and #50.
   * </p>
   */
  private static final byte STALE_REASON_MASK = 0x0E;

  private static final int STALE_REASON_SHIFT = 1;

  /**
   * Why a projection was tombstoned. Ordinals are WIRE VALUES in bits 1-3 of the flags byte: append
   * only, never renumber, and at most eight will ever fit.
   */
  public enum StaleReason {
    /** No reason recorded — a tombstone written before reasons existed, or a caller that had none. */
    UNSPECIFIED,
    /**
     * The indexed record set changed and the projection has resource-wide value dictionaries, which
     * commit-time maintenance cannot extend: it holds no dictionary writer, so it can neither mint an
     * id for a new value nor rewrite every leaf to a per-leaf encoding without paying O(corpus) on a
     * single commit. This is a reserved historical wire reason; current dictionary maintenance
     * extends the persistent keyed trie incrementally and never emits it.
     */
    GLOBAL_DICTIONARY_NOT_MAINTAINABLE,
    /** Reserved wire value from the retired rebuild fallback; current maintenance never emits it. */
    MAINTENANCE_FAILED,
    /** Leaf descriptors disagreed with this metadata about a column's encoding. */
    KIND_INCONSISTENT_STORE,
    /**
     * A resource-wide dictionary hit its byte budget mid-build, so the load abandoned the projection
     * rather than the collector abandoning the load. Distinct from
     * {@link #GLOBAL_DICTIONARY_NOT_MAINTAINABLE}: nothing is wrong with the store's shape, the
     * projection simply never finished. Raising the budget or supplying a row-count hint avoids it.
     */
    GLOBAL_DICTIONARY_BUDGET_EXCEEDED,
    /**
     * Reserved wire value. Current maintenance never schedules or performs a whole-index rebuild.
     *
     * <p>
     * A future implementation could use this value to request an explicit external repair without
     * paying O(corpus) on the committing thread. It must not become an implicit second mutation path.
     * </p>
     *
     * <p>
     * It remains declared because the ordinal is a wire value and renumbering later entries would be
     * unsafe. No current writer emits it.
     * </p>
     */
    REBUILD_PENDING;

    /**
     * The remedy for this state, in the form someone can actually run.
     *
     * <p>
     * Not "rebuild required" — a reason that does not name its own fix makes the operator rediscover
     * what the writer already knew. Downstream declines should quote this verbatim rather than
     * paraphrase it.
     * </p>
     */
    public String remedy() {
      return switch (this) {
        case GLOBAL_DICTIONARY_BUDGET_EXCEEDED ->
          "Give the loader an expected-row-count hint, so the election declines the oversized"
              + " column up front and the rest of the projection still builds, or raise"
              + " -Dsirix.projection.globalDict.budgetBytes. Then drop the stale definition, commit,"
              + " and call jn:create-projection-index again; the replacement receives a new tree id.";
        default -> "Drop the unusable projection definition, commit, and call"
            + " jn:create-projection-index again; the replacement receives a new tree id.";
      };
    }
  }

  private static final StaleReason[] STALE_REASONS = StaleReason.values();

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

  /**
   * Where a SEGMENT-scoped dictionary lives: the sealed anchor for one {@code (segment, column)} pair.
   *
   * <p>
   * A page written under a segment-scoped dictionary records its SEGMENT as its anchor, because at
   * page-encode time that segment's dictionary has not been written and has no storage key yet. This
   * is the table that closes the gap, and it is the read side's only route from a page's anchor to a
   * dictionary. The sealed count travels with the key so the reader's "the dictionary must hold at
   * least what the page recorded" rule costs no dictionary read.
   * </p>
   *
   * @param segment the segment the pages name
   * @param column the projected column
   * @param headerKey the dictionary this segment's values were sealed under, always positive
   * @param sealedEntryCount entries it held when sealed
   */
  public record SegmentAnchor(long segment, int column, long headerKey, int sealedEntryCount) {
  }

  /**
   * Segment-scoped dictionary anchors, or {@code null} when this projection has none — which is every
   * projection that is not using segment-scoped dictionaries, including every one written before they
   * existed.
   *
   * <p>
   * Written as a SECOND trailing section, after the per-column dictionary anchors. {@link #parse}
   * treats it as absent when the payload ends at the first section, so a database written before this
   * section existed still reads; a payload that carries it cannot be read by a build that predates it,
   * because the parse deliberately refuses trailing bytes it does not understand rather than
   * interpreting shifted fields.
   * </p>
   */
  private final SegmentAnchor[] segmentAnchors;

  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, null, null, null);
  }

  /** As above, carrying the index-wide {@link #setValueRowCounts}. */
  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision,
      final Map<Integer, Map<String, Long>> setValueRowCounts) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, setValueRowCounts,
        null, null);
  }

  /** As above, carrying the per-column value dictionary header keys. */
  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision,
      final Map<Integer, Map<String, Long>> setValueRowCounts, final long[] valueDictionaryHeaderKeys) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, setValueRowCounts,
        valueDictionaryHeaderKeys, null);
  }

  /** As above, additionally carrying SEGMENT-scoped dictionary anchors. */
  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision,
      final Map<Integer, Map<String, Long>> setValueRowCounts, final long[] valueDictionaryHeaderKeys,
      final SegmentAnchor[] segmentAnchors) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0, setValueRowCounts,
        valueDictionaryHeaderKeys, segmentAnchors);
  }

  private ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths, final String[] fieldNames,
      final byte[] columnKinds, final int rowGroupCount, final int buildRevision, final byte flags,
      final Map<Integer, Map<String, Long>> setValueRowCounts, final long[] valueDictionaryHeaderKeys,
      final SegmentAnchor @org.jspecify.annotations.Nullable [] segmentAnchors) {
    Objects.requireNonNull(rootPath);
    Objects.requireNonNull(fieldPaths);
    Objects.requireNonNull(fieldNames);
    Objects.requireNonNull(columnKinds);
    this.setValueRowCounts = setValueRowCounts;
    this.valueDictionaryHeaderKeys = valueDictionaryHeaderKeys == null
        ? null
        : valueDictionaryHeaderKeys.clone();
    this.segmentAnchors = segmentAnchors == null || segmentAnchors.length == 0
        ? null
        : segmentAnchors.clone();
    validateSegmentAnchors(this.segmentAnchors, columnKinds.length);
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
    return staleTombstone(StaleReason.UNSPECIFIED);
  }

  /**
   * Stale marker carrying WHY, so a later decline can quote the writer instead of guessing.
   *
   * @param reason why the projection is being retired; never {@code null}
   */
  public static ProjectionIndexMetadata staleTombstone(final StaleReason reason) {
    Objects.requireNonNull(reason, "reason");
    final byte flags = (byte) (FLAG_STALE | (reason.ordinal() << STALE_REASON_SHIFT));
    return new ProjectionIndexMetadata("", new String[0], new String[0], new byte[0], 0, 0, flags, null, null,
        null);
  }

  /**
   * Why this projection was tombstoned, or {@link StaleReason#UNSPECIFIED} when it is not stale at
   * all — callers should test {@link #isStale()} first; a reason without staleness means nothing.
   */
  public StaleReason staleReason() {
    final int ordinal = (flags & STALE_REASON_MASK) >> STALE_REASON_SHIFT;
    // A blob written by a newer build could name a reason this one has never heard of. That is not
    // a corrupt payload and must not be treated as one: the projection is still stale, which is the
    // only part the reader acts on.
    return ordinal < STALE_REASONS.length
        ? STALE_REASONS[ordinal]
        : StaleReason.UNSPECIFIED;
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
   * Whether any column is encoded against a resource-wide value dictionary.
   *
   * <p>
   * The question commit-time maintenance has to ask before it touches a leaf. Such a column's rows
   * hold DICTIONARY IDS, and the id space is owned by a writer that exists only during a build — so
   * an incremental patcher can neither mint an id for a value the dictionary has never seen nor
   * re-encode the column without rewriting every leaf. It reads this metadata's OWN kinds rather than
   * a leaf's, deliberately: the metadata is the authority on the store's shape and a leaf descriptor
   * is a falsifiable sample of it, which is the whole lesson of task #45.
   * </p>
   */
  public boolean hasGlobalDictionaryColumn() {
    for (final byte kind : columnKinds) {
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        return true;
      }
    }
    return false;
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
    if (persisted == derived
        || (persisted == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
            && derived == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT)) {
      return true;
    }
    // The temporal kinds' kill switch is a DEPLOYMENT choice, not a shape change: a store built with
    // -Dsirix.projection.temporalKinds=false holds the declared column as a per-leaf string column,
    // and a later reader with the switch back on derives the temporal kind from the same declaration.
    // Rejecting that pairing would make the switch a one-way door — the store would hydrate only under
    // the flag it happened to be built with. Serving reads the STORE's kind, so such a column simply
    // keeps taking the string route it was built for.
    return ProjectionIndexRowGroupPage.isTemporalKind(derived)
        && persisted == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /**
   * A segment anchor must name a real column, a positive header key and a non-negative count, and no
   * {@code (segment, column)} pair may appear twice: two anchors for one pair would let a page resolve
   * against whichever the reader happened to index last.
   */
  private static void validateSegmentAnchors(final SegmentAnchor @org.jspecify.annotations.Nullable [] anchors,
      final int columns) {
    if (anchors == null) {
      return;
    }
    final java.util.Set<Long> seen = new java.util.HashSet<>(anchors.length * 2);
    for (final SegmentAnchor anchor : anchors) {
      Objects.requireNonNull(anchor, "a segment anchor must not be null");
      if (anchor.segment() < 0 || anchor.column() < 0 || anchor.column() >= columns || anchor.headerKey() <= 0
          || anchor.sealedEntryCount() < 0) {
        throw new IllegalArgumentException("invalid segment dictionary anchor " + anchor);
      }
      if (!seen.add((anchor.segment() << 20) | anchor.column())) {
        throw new IllegalArgumentException(
            "segment " + anchor.segment() + " column " + anchor.column() + " is anchored twice");
      }
    }
  }

  /**
   * Segment-scoped dictionary anchors, or {@code null} when this projection has none. A copy: the
   * table is part of the persisted shape and callers must not be able to edit it in place.
   */
  public SegmentAnchor @org.jspecify.annotations.Nullable [] segmentAnchors() {
    return segmentAnchors == null
        ? null
        : segmentAnchors.clone();
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
    writeSegmentAnchors(out);
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
        // Unsupported format version: callers decline instead of interpreting shifted fields.
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
      // The SEGMENT anchor section is OPTIONAL: a payload that ends here was written by a build (or a
      // projection) without segment-scoped dictionaries, and must keep reading. Anything else past it
      // is still refused rather than interpreted as shifted fields.
      SegmentAnchor[] anchors = null;
      if (pos[0] != payload.length) {
        final int anchorCount = getIntLE(payload, pos[0]);
        pos[0] += 4;
        if (anchorCount <= 0) {
          throw new IllegalStateException("Projection metadata declares " + anchorCount + " segment anchors");
        }
        anchors = new SegmentAnchor[anchorCount];
        for (int i = 0; i < anchorCount; i++) {
          final long segment = getLongLE(payload, pos[0]);
          pos[0] += 8;
          final int column = getShortU(payload, pos);
          final long headerKey = getLongLE(payload, pos[0]);
          pos[0] += 8;
          final int sealedEntryCount = getIntLE(payload, pos[0]);
          pos[0] += 4;
          anchors[i] = new SegmentAnchor(segment, column, headerKey, sealedEntryCount);
        }
      }
      if (pos[0] != payload.length) {
        throw new IllegalStateException(
            "Projection metadata has " + (payload.length - pos[0]) + " byte(s) past its trailing sections");
      }
      return new ProjectionIndexMetadata(rootPath, paths, names, kinds, rowGroupCount, buildRevision, flags, counts,
          dictionaryKeys, anchors);
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

  /**
   * The SECOND trailing section, written only when the projection has segment anchors — so a
   * projection without them serializes byte-for-byte as before and stays readable by a build that
   * predates this section.
   */
  private void writeSegmentAnchors(final ByteArrayOutputStream out) {
    if (segmentAnchors == null) {
      return;
    }
    putIntLE(out, segmentAnchors.length);
    for (final SegmentAnchor anchor : segmentAnchors) {
      putLongLE(out, anchor.segment());
      putShortU(out, anchor.column());
      putLongLE(out, anchor.headerKey());
      putIntLE(out, anchor.sealedEntryCount());
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

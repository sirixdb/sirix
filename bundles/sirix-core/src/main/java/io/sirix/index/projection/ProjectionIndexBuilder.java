/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.NamePage;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.jspecify.annotations.Nullable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Walks a JSON resource's current revision and materialises one row per record (= node whose
 * pathNodeKey matches the projection's root path) into {@link ProjectionIndexRowGroupPage}s.
 * Completed leaves are delivered to the internal synchronous sink in append order so the caller can
 * stream them into the HOT backing tree without holding more than one leaf in memory. The public
 * {@code Consumer<byte[]>} surface is preserved by a serialising adapter; internal persistence paths
 * borrow the live page and encode detached column segments directly.
 *
 * <h2>Traversal shape</h2> The builder is driven directly rather than via the node-visitor pattern
 * CAS/PATH/NAME indexes use — projection extraction needs to look at each matching record's
 * descendants (to fetch field values), which is easier expressed as "hit a root-matching node, then
 * navigate" than as a visitor that sees individual leaf values.
 *
 * <h2>HFT-grade hot path</h2> Per-record extraction is delegated to a
 * {@link ProjectionIndexRowExtractor} — the single source of truth shared with the incremental
 * maintenance path ({@link ProjectionIndexChangeListener}) — which owns reusable per-row primitive
 * buffers and allocates nothing per record. The only per-record heap activity is the varint-decoded
 * nodeKey load the rtx already pays for, plus the FSST-decoded UTF-8 byte[] for string fields
 * (deduplicated inside the leaf page's local dictionary, so it only occurs once per distinct string
 * per leaf).
 */
public final class ProjectionIndexBuilder {

  /**
   * Synchronous hand-off of a builder-owned row-group page.
   *
   * <p>The callback borrows the page only for the duration of {@link #accept}. It must neither
   * mutate the page nor retain the page or any array reachable through its accessors. Data that
   * outlives the callback must be copied into independently owned output before the callback
   * returns. In particular, {@link ProjectionIndexColumnSegmentCodec#encodeReferencedOnly(
   * ProjectionIndexRowGroupPage, ProjectionIndexColumnSegmentCodec.EncodeWorkspace)} satisfies
   * this contract: all of its published outputs are detached.
   */
  @FunctionalInterface
  interface BorrowedLeafSink {
    void accept(ProjectionIndexRowGroupPage leaf);
  }

  private final BorrowedLeafSink leafSink;

  /** Shared per-record extraction engine (also used by incremental maintenance). */
  private final ProjectionIndexRowExtractor extractor;

  /**
   * Resolved pathNodeKeys of the projection root (e.g. {@code $doc[]}). Multi-PCR roots — the same
   * path shape under sibling subtrees — are supported: every node whose pathNodeKey is in this set is
   * a record (set) root.
   */
  private final LongSet rootPathNodeKeys;

  /** Strict ancestor pathNodeKeys of every root PCR — guides pruned descent. */
  private final LongSet rootAncestorPathNodeKeys;

  private ProjectionIndexRowGroupPage currentLeaf;

  /**
   * One emitted page whose synchronous borrower returned successfully. A builder needs only one
   * spare: the next full leaf consumes it before another leaf can be emitted.
   */
  private ProjectionIndexRowGroupPage reusableLeaf;

  private long rowsEmitted;
  private long leavesEmitted;

  /**
   * How many leading leaves are held back to measure string-column cardinality on. 64 leaves is
   * 65,536 rows — enough that a column's repetition pattern is visible, small enough that holding
   * them costs a few megabytes and delays nothing measurable.
   */
  private static final int SAMPLE_LEAVES = 64;

  /**
   * Below this many dictionary entries in the sample, keep the per-leaf dictionaries whatever the
   * deduplication factor says. A small dictionary is cheap per leaf, packs into a couple of bits
   * per row, and materialises with no record read at all; the resource-wide machinery cannot repay
   * itself against that.
   */
  private static final int MIN_GLOBAL_DICTIONARY_ENTRIES =
      Integer.getInteger("sirix.projection.globalDict.minEntries", 4096);

  /**
   * The per-leaf deduplication factor a column must fail to reach before it goes global — rows per
   * distinct value within a leaf. At 4, a 1024-row leaf holds at most 256 distinct values and the
   * per-leaf dictionary is genuinely compressing; above that number of distinct values it is
   * mostly storing each string once and adding an id per row on top.
   */
  private static final int MIN_PER_LEAF_DEDUP_FACTOR =
      Integer.getInteger("sirix.projection.globalDict.dedupFactor", 4);

  /** How the per-column choice between per-leaf and resource-wide dictionaries is made. */
  private enum GlobalDictionaryMode {
    /** Measure the sample and decide per column. */
    AUTO,
    /** Force every string column global — for testing both encodings on one corpus. */
    ALWAYS,
    /** Never go global; every string column keeps its per-leaf dictionary. */
    NEVER
  }

  /**
   * {@code -Dsirix.projection.globalDict=auto|always|never}. Overridable so one corpus can be built
   * both ways and the two compared, which is the only way to show the encodings agree.
   *
   * <p><b>Defaults to {@code auto}</b>, now that the executor consumes the kind: a global column's
   * group key runs through the integer kernel, its ids feed a distinct fold directly, predicate
   * literals resolve to ids once per query, and winners reverse-map in one batch. What {@code auto}
   * does NOT do is force the encoding — the per-leaf dedup factor decides per column, so a column
   * with a few dozen repeated labels keeps its per-leaf dictionary and every route it already had.
   *
   * <p>{@code always} is a testing knob and nothing else. It forces the encoding onto columns the
   * heuristic would never choose it for, including low-cardinality ones, which is exactly what makes
   * it useful for a differential — and also means it moves shapes that need STRING VALUES rather
   * than identities (ordering by the key, substring and stringify transforms, composite keys) onto
   * the generic pipeline. Those decline; they do not answer differently.
   */
  /**
   * Ceiling on ONE column's resource-wide dictionary, {@code -Dsirix.projection.globalDict.budgetBytes}.
   *
   * <p>
   * Default {@code min(heap/8, 2 GiB)}. The heap fraction is what keeps it sane on a small JVM; the
   * absolute cap is what keeps several elected columns from summing to the whole heap, since the
   * budget is PER COLUMN and ClickBench elects three fat-string ones at once.
   * </p>
   */
  private static long globalDictionaryBudgetBytes() {
    final String configured = System.getProperty("sirix.projection.globalDict.budgetBytes");
    if (configured != null) {
      final long parsed = Long.parseLong(configured.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException("sirix.projection.globalDict.budgetBytes must be positive: " + parsed);
      }
      return parsed;
    }
    return Math.min(Runtime.getRuntime().maxMemory() / 8, 2L << 30);
  }

  private static GlobalDictionaryMode globalDictionaryMode() {
    // Read per BUILD rather than cached in a static: a differential test has to build the same
    // corpus both ways inside one JVM to show the two encodings answer identically, and a mode
    // frozen at class-load makes that impossible. A build is not a hot path.
    final String configured = System.getProperty("sirix.projection.globalDict", "auto");
    return switch (configured.toLowerCase(java.util.Locale.ROOT)) {
      case "always" -> GlobalDictionaryMode.ALWAYS;
      case "never", "off", "false" -> GlobalDictionaryMode.NEVER;
      default -> GlobalDictionaryMode.AUTO;
    };
  }

  /**
   * Columns the most recent build encoded with a resource-wide dictionary.
   *
   * <p>Test observability, and load-bearing for one thing in particular: a differential that
   * compares the two encodings proves nothing unless the "global" arm actually produced a global
   * column, and nothing else about the outcome reveals whether it did.
   */
  private static final java.util.concurrent.atomic.AtomicInteger GLOBAL_DICTIONARY_COLUMNS =
      new java.util.concurrent.atomic.AtomicInteger();

  /** How many columns the most recent build encoded with a resource-wide dictionary. */
  public static int globalDictionaryColumnsBuilt() {
    return GLOBAL_DICTIONARY_COLUMNS.get();
  }

  /** Leading leaves held back until the per-column dictionary choice is made; null afterwards. */
  private List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(SAMPLE_LEAVES);

  /** Per-column resource-wide dictionaries; null slots are columns that stayed per-leaf. */
  private GlobalValueDictionaryWriter[] globalDictionaries;

  /** {@code -Dsirix.projDiag}: explain election declines, which are otherwise silent by design. */
  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Heap cost of ONE dictionary entry beyond its value bytes.
   *
   * <p>
   * Derived, not guessed: {@code offsets} 4 B + {@code lengths} 4 B + {@code hashes} 8 B = 16 B of
   * index per id, plus the open-addressed table's {@code tableHashes} 8 B + {@code tableIds} 4 B
   * per SLOT, and the table rehashes at half load so it holds two slots per entry — 24 B. Steady
   * state is therefore 40 B; the doubling of every one of those arrays can transiently exceed it,
   * which is what the writer's own runtime cap is for.
   * </p>
   */
  private static final long PER_ENTRY_OVERHEAD_BYTES = 40L;

  /**
   * Expected rows in the corpus, or {@code -1} when nobody could say.
   *
   * <p>
   * The election needs it and cannot derive it: a streaming one-pass build sees the row count only
   * when the stream ends. With it, a column whose dictionary cannot fit is declined before any leaf
   * is written as global. Without it, the writer's runtime cap is the only protection and a corpus
   * large enough will abandon its projection mid-build — correct, non-fatal, and measured as the
   * row path.
   * </p>
   */
  private long expectedRows = -1L;

  /**
   * Tell the build how many records to expect, so the global-dictionary election can decline a
   * column that would not fit. Values {@code <= 0} mean "unknown" and disable the election-time
   * check; the runtime cap still applies.
   *
   * @param rows the expected record count
   */
  public void setExpectedRows(final long rows) {
    this.expectedRows = rows;
  }

  /** Total bytes of the sample's per-leaf dictionary ENTRIES for one column. */
  private long sampledValueBytes(final int column) {
    long bytes = 0;
    for (final ProjectionIndexRowGroupPage leaf : sample) {
      final int size = leaf.stringDictionarySize(column);
      for (int id = 0; id < size; id++) {
        bytes += leaf.stringDictionaryEntryLength(column, id);
      }
    }
    return bytes;
  }

  /**
   * Whether rows are FED to this builder ({@link #appendRecord}) instead of walked by it. A streaming
   * builder never resolves the record-set root, because at the time an incremental build is armed the
   * root path class does not exist yet — the resource is empty.
   */
  private final boolean streaming;

  /**
   * Record-fed builder for the INCREMENTAL (load-time) build: the caller supplies each completed
   * record as it is shredded, and the leaf/sample/global-dictionary machinery is identical to the
   * walking build's, so the two produce the same leaves for the same record sequence.
   *
   * <p>
   * Deliberately skips the root-path resolution the walking constructor insists on: an incremental
   * build is armed on an EMPTY resource, where the root path has no path class yet and every declared
   * field path has none either. Record identity comes from the caller (which resolves it from the
   * change notifications), not from a path match, so nothing here needs the summary except the
   * extractor's field paths — and those are re-resolved per batch through {@link #refreshFieldPaths}.
   */
  public static ProjectionIndexBuilder streaming(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final Consumer<byte[]> leafSink) {
    return new ProjectionIndexBuilder(indexDef, pathSummary, serializingLeafSink(leafSink), true);
  }

  /** Internal zero-copy row-group hand-off used by the synchronous bulk-load writer. */
  static ProjectionIndexBuilder streamingBorrowed(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final BorrowedLeafSink leafSink) {
    return new ProjectionIndexBuilder(indexDef, pathSummary, leafSink, true);
  }

  public ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final Consumer<byte[]> leafSink) {
    this(indexDef, pathSummary, serializingLeafSink(leafSink), false);
  }

  private ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final BorrowedLeafSink leafSink, final boolean streaming) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexBuilder requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    this.leafSink = Objects.requireNonNull(leafSink, "leafSink must not be null");
    this.streaming = streaming;
    if (streaming) {
      this.rootPathNodeKeys = LongSets.EMPTY_SET;
      this.rootAncestorPathNodeKeys = LongSets.EMPTY_SET;
      this.extractor = new ProjectionIndexRowExtractor(indexDef, pathSummary);
      this.currentLeaf = new ProjectionIndexRowGroupPage(extractor.columnKindsRef());
      return;
    }
    final Path<QNm> rootPath = indexDef.getProjectionRootPath();
    final Set<Path<QNm>> rootSet = new HashSet<>();
    rootSet.add(rootPath);
    final Set<Long> rootPcrs = pathSummary.getPCRsForPaths(rootSet);
    if (rootPcrs.isEmpty()) {
      throw new IllegalStateException("Projection root path '" + rootPath + "' did not resolve to any pathNodeKey — "
          + "declare the index after the resource has records matching the root");
    }
    // Multi-PCR roots (identical path shapes under sibling subtrees) are
    // supported: every PCR is a record(-set) root and the pruned descent
    // follows the union of their ancestor chains.
    this.rootPathNodeKeys = new LongOpenHashSet(rootPcrs.size());
    for (final Long pcr : rootPcrs) {
      rootPathNodeKeys.add(pcr.longValue());
    }
    // Compare only PCRs matched by the declared ROOT path. Fail fast when one such root is nested
    // below another (for example, //records/[] over self-nested "records" arrays): the pruned
    // descent stops at the outer match, and the per-record field DFS could otherwise let the inner
    // record's fields overwrite the outer row's columns. Declared COLUMN paths are deliberately not
    // part of this comparison; columns may descend to arbitrary and differing depths below a root.
    // Sibling multi-PCR roots remain supported because they have no ancestor relation.
    assertNoNestedRootPcrs(pathSummary, rootPathNodeKeys, rootPath);
    // Pre-compute the set of pathNodeKeys along every path from docRoot
    // to each root PCR — used to PRUNE the walk to only descend into
    // subtrees that can structurally contain records. For deep nested
    // projections (e.g. /wrapper/records/[]) this turns O(total-nodes)
    // into O(ancestor-depth + records). Reference to a HashSet of longs
    // via fastutil to avoid boxing.
    this.rootAncestorPathNodeKeys = computeAncestorPathNodeKeys(pathSummary, rootPathNodeKeys);

    this.extractor = new ProjectionIndexRowExtractor(indexDef, pathSummary);
    this.currentLeaf = new ProjectionIndexRowGroupPage(extractor.columnKindsRef());
  }

  /** Preserve the public raw-payload API at the borrowed-page ownership boundary. */
  static BorrowedLeafSink serializingLeafSink(final Consumer<byte[]> leafSink) {
    final Consumer<byte[]> checkedSink = Objects.requireNonNull(leafSink, "leafSink must not be null");
    return leaf -> checkedSink.accept(leaf.serialize());
  }

  private static void assertNoNestedRootPcrs(final PathSummaryReader pathSummary, final LongSet rootPathNodeKeys,
      final Path<QNm> rootPath) {
    final long saved = pathSummary.getNodeKey();
    try {
      final LongIterator roots = rootPathNodeKeys.iterator();
      while (roots.hasNext()) {
        final long root = roots.nextLong();
        if (!pathSummary.moveTo(root))
          continue;
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0)
            break;
          if (rootPathNodeKeys.contains(pk)) {
            throw new IllegalStateException("Projection ROOT path '" + rootPath
                + "' resolves to overlapping nested root matches (matched pathNodeKey " + root
                + " lies below matched root pathNodeKey " + pk + "). Only matches of the declared ROOT path are "
                + "compared here; projection COLUMN paths may descend to arbitrary and differing depths below each "
                + "root. Self-nested root matches are not supported; declare a more specific root path");
          }
        }
      }
    } finally {
      pathSummary.moveTo(saved);
    }
  }

  private static LongSet computeAncestorPathNodeKeys(final PathSummaryReader pathSummary,
      final LongSet rootPathNodeKeys) {
    final LongSet ancestors = new LongOpenHashSet();
    final long saved = pathSummary.getNodeKey();
    try {
      final LongIterator roots = rootPathNodeKeys.iterator();
      while (roots.hasNext()) {
        final long root = roots.nextLong();
        if (!pathSummary.moveTo(root))
          continue;
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0)
            break; // document root / no more
          ancestors.add(pk);
        }
      }
    } finally {
      pathSummary.moveTo(saved);
    }
    return ancestors;
  }

  /**
   * Canonical declared-type → column-kind mapping. The SINGLE source of truth — the creation
   * function, the persisted metadata, and the builder must agree, or hydration's shape validation
   * would reject healthy stores.
   */
  /**
   * Column kind for a declared field, using its PATH as well as its type.
   *
   * <p>
   * A field path whose last step is an array layer ({@code /[]/genres/[]}) declares the ELEMENTS of
   * an array-valued field, and a set of strings is what that column holds. Every other kind is
   * scalar, which is why such a field used to be recorded as present-but-unrepresentable and the
   * index could answer nothing about it.
   *
   * <p>
   * Keyed off the path rather than a new {@code Type} constant because {@link Type} is brackit's, and
   * the path already says unambiguously what the user declared — an array step is not expressible any
   * other way.
   */
  public static byte mapTypeToColumnKind(final Type type, final Path<QNm> fieldPath) {
    if (fieldPath != null && isArrayLayerPath(fieldPath) && (type == Type.STR || type == Type.ANY)) {
      return ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
    }
    return mapTypeToColumnKind(type);
  }

  /** Whether the path's LAST step selects an array layer. */
  public static boolean isArrayLayerPath(final Path<QNm> fieldPath) {
    final var steps = fieldPath.steps();
    if (steps.isEmpty()) {
      return false;
    }
    final var axis = steps.get(steps.size() - 1).getAxis();
    return axis == Path.Axis.CHILD_ARRAY || axis == Path.Axis.DESC_ARRAY;
  }

  public static byte mapTypeToColumnKind(final Type type) {
    if (type == Type.BOOL)
      return ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN;
    if (type == Type.INR || type == Type.LON || type == Type.INT) {
      return ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
    }
    if (type == Type.DEC || type == Type.DBL || type == Type.FLO) {
      // Floating/decimal columns store exact doubles (order-preserving transform) instead of
      // silently truncating into longs — docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6. No
      // user-facing definition could carry these types before (the creation function rejected
      // them), so the mapping change breaks no persisted shape.
      return ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE;
    }
    return ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /**
   * Build the projection over the transaction's CURRENT state and persist leaves + metadata into the
   * definition's HOT sub-tree — the shared core of the controller's index creation and the change
   * listener's commit-time full rebuild. All writes ride the given writer; the caller's commit
   * persists them.
   *
   * @param emptyRecordSetAllowed creation fails loudly when the root path resolves to no path class
   *        (declaring an index over a non-existent record set is a caller error), while the
   *        maintenance rebuild persists the truthful EMPTY projection (the record set was removed by
   *        the committing transaction)
   */
  public static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final JsonNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    final ProjectionIndexHOTStorage storage =
        ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
    // Null when slot 0 was a LEGACY chunked payload (priorMetadata reset the sub-tree), but also
    // when the blob is simply unreadable AS METADATA and the sub-tree was left intact: no PIXM
    // magic, or a version byte that is not the one supported version — both of which
    // ProjectionIndexMetadata.parse turns into null rather than a throw.
    final ProjectionIndexMetadata priorMeta = priorMetadata(storage);
    final boolean wasReset = resetSubTreeIfRowGroupsAreUndescribable(storage);
    final boolean live = priorMeta != null && !priorMeta.isStale();
    // Probe ABOVE the declared count even for a live snapshot: a rebuild can follow an incremental
    // patch that wrote fresh row groups and then failed before updating slot 0, so the metadata can
    // under-report what is physically live. Those extras must be tombstoned here or the store
    // rejects them as leaked orphans on every later full read.
    //
    // A reset sub-tree short-circuits to 0: there is provably nothing left to reclaim, whereas
    // probeLiveRowGroupCountFrom TRUSTS the declared count without re-reading those slots, so a live
    // snapshot of 100k row groups would otherwise send finishPersist through 100k tombstone calls
    // against slots that no longer exist. Each is a cheap no-op, but 100k of them is not.
    final int priorRowGroupCount = wasReset
        ? 0
        : live
            ? storage.probeLiveRowGroupCountFrom(priorMeta.rowGroupCount())
            : storage.probeLiveRowGroupCount();
    if (emptyRecordSetAllowed && pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath())).isEmpty()) {
      final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
      final byte[] columnKinds = new byte[fieldTypes.size()];
      for (int i = 0; i < columnKinds.length; i++) {
        columnKinds[i] = mapTypeToColumnKind(fieldTypes.get(i), indexDef.getProjectionFields().get(i));
      }
      finishPersist(indexDef, storage, LongArrayList.of(), LongArrayList.of(), priorRowGroupCount,
          rtx.getRevisionNumber(), columnKinds, null, null, null);
      return;
    }
    // Streaming build (descriptor layout): each leaf is written the moment the builder emits
    // it — one leaf in memory at a time, matching this class's streaming contract instead of
    // buffering all encoded leaves on the heap (~240 MB at the 100 M-row scale). Only the two
    // fence longs per leaf are accumulated for the metadata blob written last.
    final LongArrayList firstKeys = new LongArrayList();
    final LongArrayList lastKeys = new LongArrayList();
    final Map<Integer, List<byte[]>> bloomPerColumn = new HashMap<>();
    final Map<Integer, Map<String, Long>> setValueRowCounts = new LinkedHashMap<>();
    final boolean hasSetColumn = hasStringSetColumn(indexDef);
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(indexDef, pathSummary, leaf -> {
      if (leaf.getRowCount() == 0) {
        throw new IllegalStateException("Projection leaf " + firstKeys.size() + " is empty");
      }
      firstKeys.add(leaf.firstRecordKey());
      lastKeys.add(leaf.lastRecordKey());
      // Accumulate the index-wide per-value ROW counts while the leaf is in hand. Summing the
      // per-leaf counts is exact: a record lives in exactly one leaf, and the per-leaf figures
      // already count rows rather than occurrences.
      if (hasSetColumn) {
        accumulateSetValueRowCounts(leaf, setValueRowCounts);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(leaf, encodeWorkspace);
      accumulateBloomSegments(encoded, firstKeys.size(), bloomPerColumn);
      storage.putRowGroupAsColumnSegmentSlots(firstKeys.size(), encoded);
    }, false);
    builder.build(rtx);
    // Dictionaries are written after the leaves, and only once: the leaves refer to values by id,
    // so nothing can be persisted about a dictionary until every id it will ever mint is known.
    final long[] valueDictionaryHeaderKeys =
        flushValueDictionaries(builder.globalDictionaries(), storageEngineWriter);
    finishPersist(indexDef, storage, firstKeys, lastKeys, priorRowGroupCount, rtx.getRevisionNumber(),
        builder.columnKinds(), setValueRowCounts, bloomPerColumn, valueDictionaryHeaderKeys);
  }

  /**
   * Persist every resource-wide dictionary the build produced and return their header keys.
   *
   * <p>The in-memory intern tables are released as soon as their contents are on the page: they are
   * the largest transient allocation of a build over a high-cardinality column, and nothing needs
   * them once the records exist.
   *
   * @return per-column header node keys, {@code 0} where the column has no dictionary, or
   *         {@code null} when the build produced none at all
   */
  static long @Nullable [] flushValueDictionaries(
      final GlobalValueDictionaryWriter @Nullable [] dictionaries,
      final StorageEngineWriter storageEngineWriter) {
    if (dictionaries == null) {
      return null;
    }
    long[] headerKeys = null;
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(storageEngineWriter);
    for (int c = 0; c < dictionaries.length; c++) {
      final GlobalValueDictionaryWriter dictionary = dictionaries[c];
      if (dictionary == null) {
        continue;
      }
      if (headerKeys == null) {
        headerKeys = new long[dictionaries.length];
      }
      headerKeys[c] = dictionary.flush(namePage, databaseType, storageEngineWriter, storageEngineWriter.getLog());
      dictionary.release();
    }
    return headerKeys;
  }

  /**
   * Collect the leaf's fingerprint segments per column, index-aligned to {@code rowGroupId - 1}.
   * Leaves without one (rowless, non-string columns) stay {@code null} in the list — the block
   * encoder writes them as empty slices, which probe as "no evidence, keep".
   */
  static void accumulateBloomSegments(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded,
      final int rowGroupId, final Map<Integer, List<byte[]>> bloomPerColumn) {
    final int[] ids = encoded.columnSegmentIds();
    for (int i = 0; i < ids.length; i++) {
      final int id = ids[i];
      // Stride ids only: the DICT_HASH region sits above them, and its residues would otherwise
      // read as another column's fingerprint — a WRONG bloom block, which prunes real leaves away.
      if (id > 0 && id < ProjectionIndexColumnSegmentCodec.DICT_HASH_SEGMENT_BASE
          && id % ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN == 0) {
        final int column = id / ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN - 1;
        final List<byte[]> list = bloomPerColumn.computeIfAbsent(column, unused -> new ArrayList<>());
        while (list.size() < rowGroupId - 1) {
          list.add(null);
        }
        list.add(encoded.segments()[i]);
      }
    }
  }

  /**
   * Leaf count of the prior persisted snapshot, for orphan tombstoning. Three cases: live metadata →
   * its declared count; stale tombstone or unreadable-but-descriptor-layout metadata → the tombstone
   * no longer carries the pre-invalidation count, so probe the live descriptor slots (invalidate/drop
   * leave the leaves in place); LEGACY (pre-descriptor chunked) slot-0 payload → the sub-tree cannot
   * be selectively cleared at all — {@code resetTree()} swaps in a fresh empty tree (the §6 migration
   * path) and the prior count is 0.
   */

  private static ProjectionIndexMetadata priorMetadata(final ProjectionIndexHOTStorage storage) {
    final ProjectionIndexMetadata parsed;
    try {
      parsed = ProjectionIndexMetadata.parse(storage.getBlob(0));
    } catch (final IllegalStateException legacyLayout) {
      storage.resetTree(); // pre-descriptor chunked store → swap a fresh empty tree (§6 migration)
      return null;
    }
    if (parsed != null) {
      return parsed;
    }
    // parse() returns NULL rather than throwing for a slot 0 that is simply unreadable AS metadata:
    // no PIXM magic, or a version byte that is not the one supported version. The metadata is gone
    // either way, but the SUB-TREE is not — and a sub-tree written before the descriptor layout was
    // retired holds its row groups at RAW slot ids. Returning null alone
    // sends the rebuild to probeLiveRowGroupCount(), which now probes composite keys only, reports
    // 0, and tombstones nothing — so the rebuild writes composite-keyed row groups straight into a
    // sub-tree that still holds raw-keyed ones. Below 65536 old row groups they leak; at or above
    // it, raw slot 65536 aliases exactly onto composite key (rowGroupId=1, slotKind=0) and every
    // later read throws "mixed storage layouts in one sub-tree", unrepairably.
    //
    // Reset for the same reason the legacy chunked payload does: the sub-tree cannot be selectively
    // cleared when nothing left can say what is in it. A raw-keyed slot 1 is the witness — the one
    // thing a segment-slot store never writes.
    if (storage.hasRawKeyedRowGroup()) {
      storage.resetTree();
    }
    return null;
  }

  /**
   * Reset the sub-tree when its row groups can no longer describe themselves.
   *
   * <p>
   * Separate from {@link #priorMetadata} because it is a different kind of damage: slot 0 may parse
   * perfectly (including as a stale tombstone, the normal pre-rebuild state) while a ROW GROUP's
   * descriptor is unreadable. The write path deliberately overwrites such a descriptor rather than
   * throwing — a rebuild has to make progress over damage, not fail on it — but that leniency cannot
   * reclaim a segment slot whose id is absent from the new descriptor, because nothing left names it.
   * A stranded slot makes every later full read throw "segment N has no descriptor entry", and the
   * next rebuild reads the freshly written descriptor as its prior, so it can never detect the orphan
   * either. Clearing the sub-tree is the only operation that reaches it.
   *
   * @return whether the sub-tree was cleared, which tells the caller there is nothing left to reclaim
   */
  private static boolean resetSubTreeIfRowGroupsAreUndescribable(final ProjectionIndexHOTStorage storage) {
    if (!storage.hasUnreadableRowGroupDescriptor()) {
      return false;
    }
    storage.resetTree();
    return true;
  }

  /** Whether the definition declares at least one {@code COLUMN_KIND_STRING_SET} column. */
  static boolean hasStringSetColumn(final IndexDef indexDef) {
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    for (int i = 0; i < fieldTypes.size(); i++) {
      if (mapTypeToColumnKind(fieldTypes.get(i),
          fieldPaths.get(i)) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        return true;
      }
    }
    return false;
  }

  /**
   * Fold one leaf's per-value ROW counts into the index-wide totals.
   *
   * <p>
   * The raw compatibility form retains its original semantics and delegates to the live-page
   * implementation after deserialisation.
   */
  static void accumulateSetValueRowCounts(final byte[] raw, final Map<Integer, Map<String, Long>> into) {
    accumulateSetValueRowCounts(ProjectionIndexRowGroupPage.deserialize(raw), into);
  }

  /** Fold a borrowed live leaf's per-value row counts without a whole-row-group round trip. */
  static void accumulateSetValueRowCounts(final ProjectionIndexRowGroupPage leaf,
      final Map<Integer, Map<String, Long>> into) {
    final int rowCount = leaf.getRowCount();
    if (rowCount == 0) {
      return;
    }
    for (int c = 0; c < leaf.getColumnCount(); c++) {
      if (leaf.columnKind(c) != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        continue;
      }
      final int dictSize = leaf.stringDictionarySize(c);
      final long[] counts = ProjectionIndexColumnSegmentCodec.valueRowCounts(dictSize, leaf.stringSetCountColumn(c),
          leaf.stringSetIdColumn(c), rowCount);
      if (counts == null) {
        continue;
      }
      final Map<String, Long> forColumn = into.computeIfAbsent(c, k -> new LinkedHashMap<>());
      for (int i = 0; i < counts.length; i++) {
        if (counts[i] > 0) {
          forColumn.merge(new String(leaf.stringDictionaryEntryBacking(c, i), leaf.stringDictionaryEntryOffset(c, i),
              leaf.stringDictionaryEntryLength(c, i), StandardCharsets.UTF_8), counts[i], Long::sum);
        }
      }
    }
  }

  /**
   * Finish a (re)build: tombstone orphaned slots above the new leaf count (real deletes — hygiene,
   * not load-bearing; the metadata's leaf count still bounds every read), write the shape-only
   * metadata blob (shape, build revision) at slot 0, then persist the per-leaf record-key fences as
   * carry-forward chunks ({@link ProjectionIndexFences}).
   */
  static void finishPersist(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final LongArrayList firstKeys, final LongArrayList lastKeys, final int priorRowGroupCount,
      final int buildRevision, final byte[] columnKinds, final Map<Integer, Map<String, Long>> setValueRowCounts,
      final @Nullable Map<Integer, List<byte[]>> bloomPerColumn, final long @Nullable [] valueDictionaryHeaderKeys) {
    final int rowGroupCount = firstKeys.size();
    for (long slot = rowGroupCount + 1; slot <= priorRowGroupCount; slot++) {
      storage.tombstoneRowGroupAsColumnSegmentSlots(slot);
    }
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final String[] paths = new String[fieldPaths.size()];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = fieldPaths.get(i).toString();
    }
    final String rootPath = indexDef.getProjectionRootPath().toString();
    final String[] names = ProjectionIndexChangeListener.trailingFieldNames(indexDef);
    final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(rootPath, paths, names, columnKinds,
        rowGroupCount, buildRevision, setValueRowCounts, valueDictionaryHeaderKeys);
    storage.putBlob(0, metadata.serialize());
    // Fingerprint BLOCKS — the contiguous acceleration over the per-leaf segments just written.
    // A full build is the ONLY writer of blocks; incremental maintenance tombstones them
    // (see ProjectionIndexHOTStorage#removeBloomBlocks), so a block always mirrors a complete
    // build's truth. Tombstone-first covers columns whose block existed but is empty now.
    storage.removeBloomBlocks(columnKinds.length);
    if (bloomPerColumn != null) {
      for (final Map.Entry<Integer, List<byte[]>> e : bloomPerColumn.entrySet()) {
        final List<byte[]> list = e.getValue();
        final byte[][] perLeaf = new byte[rowGroupCount][];
        final int upTo = Math.min(list.size(), rowGroupCount);
        for (int i = 0; i < upTo; i++) {
          perLeaf[i] = list.get(i);
        }
        final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(perLeaf, rowGroupCount);
        if (block != null) {
          storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(e.getKey()), block);
        }
      }
    }
    ProjectionIndexFences.write(storage, rowGroupCount, firstKeys.toLongArray(), lastKeys.toLongArray(),
        priorRowGroupCount);
  }

  /**
   * Walk the resource from the document root, materialising one projection row per node whose
   * pathNodeKey is a projection-root PCR. Flushes any partially-filled trailing leaf on completion.
   */
  public void build(final JsonNodeReadOnlyTrx rtx) {
    if (streaming) {
      throw new IllegalStateException("A streaming projection builder is fed records through appendRecord(); "
          + "it never resolved a record-set root to walk");
    }
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      // Optional: -Dsirix.projection.builder=generic forces the original
      // DescendantAxis walk. Used for A/B verification against the pruned
      // descent.
      final boolean forceGeneric = "generic".equals(System.getProperty("sirix.projection.builder"));
      if (forceGeneric) {
        genericBuild(rtx);
      } else {
        rtx.moveToDocumentRoot();
        if (!tryFastArrayIteration(rtx)) {
          genericBuild(rtx);
        }
      }
      flushCurrentRowGroup();
      // A build shorter than the sample never reaches the decision inside the flush, and a build
      // whose last leaf was empty leaves the flush returning early — either way the buffer must
      // still be decided on and drained, or the whole index would be silently dropped.
      if (sample != null) {
        decideDictionaryKindsAndDrainSample();
      }
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  /**
   * Generic pruned descent: walk from docRoot, descending only into subtrees whose pathNodeKey is an
   * ancestor of any root PCR (pre-computed from PathSummary). When a descendant matches a root
   * pathNodeKey, process it as a record — either as an array whose children are rows, or as a
   * single-record itself.
   *
   * <p>
   * Cost bound: O(ancestor-depth + records + record-field-walk) — independent of the total document
   * node count. Works for arbitrary nesting depths, multiple matching roots across sibling subtrees,
   * and any structured record shape.
   */
  private boolean tryFastArrayIteration(final JsonNodeReadOnlyTrx rtx) {
    final long docRoot = rtx.getNodeKey();
    if (rootPathNodeKeys.isEmpty())
      return false;
    boolean processedAny = descendToRoots(rtx, docRoot);
    rtx.moveTo(docRoot);
    return processedAny;
  }

  /**
   * Recursively descend from {@code parentKey} into children whose pathNodeKey lies on the path to
   * the projection root, processing each root-matching node. Returns true if any record(s) were
   * processed.
   */
  private boolean descendToRoots(final JsonNodeReadOnlyTrx rtx, final long parentKey) {
    rtx.moveTo(parentKey);
    if (!rtx.moveToFirstChild())
      return false;
    boolean any = false;
    do {
      final long pk = getPathNodeKeyAtCursor(rtx);
      if (pk >= 0 && rootPathNodeKeys.contains(pk)) {
        // Match — process as record(s).
        final long matchKey = rtx.getNodeKey();
        final NodeKind matchKind = rtx.getKind();
        // iter#32 fusion: OBJECT_NAMED_ARRAY plays both the OBJECT_KEY and ARRAY role —
        // its children are the array elements directly.
        final boolean arrayLike = matchKind == NodeKind.ARRAY || matchKind == NodeKind.OBJECT_NAMED_ARRAY;
        if (arrayLike) {
          if (rtx.moveToFirstChild()) {
            do {
              final long elementKey = rtx.getNodeKey();
              extractRow(rtx, elementKey);
              rtx.moveTo(elementKey);
            } while (rtx.moveToRightSibling());
          }
        } else {
          extractRow(rtx, matchKey);
        }
        rtx.moveTo(matchKey);
        any = true;
      } else if (pk >= 0 && rootAncestorPathNodeKeys.contains(pk)) {
        // Structural ancestor of the root — descend further.
        final long curKey = rtx.getNodeKey();
        if (descendToRoots(rtx, curKey))
          any = true;
        rtx.moveTo(curKey);
      }
      // else: pathNodeKey is unrelated — prune this subtree entirely.
    } while (rtx.moveToRightSibling());
    return any;
  }

  /** Fallback: original descendant-axis walk from the document root. */
  private void genericBuild(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(rtx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (!isRecordRoot(rtx))
        continue;
      final long matchKey = rtx.getNodeKey();
      final NodeKind matchKind = rtx.getKind();
      final boolean arrayLike = matchKind == NodeKind.ARRAY || matchKind == NodeKind.OBJECT_NAMED_ARRAY;
      if (arrayLike) {
        if (rtx.moveToFirstChild()) {
          do {
            final long elementKey = rtx.getNodeKey();
            extractRow(rtx, elementKey);
            rtx.moveTo(elementKey);
          } while (rtx.moveToRightSibling());
        }
      } else {
        extractRow(rtx, matchKey);
      }
      rtx.moveTo(matchKey);
    }
  }

  /**
   * Re-resolve the extractor's declared field paths against the CURRENT path summary — see
   * {@link ProjectionIndexRowExtractor#refresh}. Called once per extraction batch by the incremental
   * build, whose summary is still growing while it extracts.
   */
  public void refreshFieldPaths(final PathSummaryReader pathSummary) {
    extractor.refresh(pathSummary);
  }

  /**
   * Append one completed record as the next row, in the caller's order. The caller owns record
   * identity and ordering; this method only extracts and packs.
   *
   * @return {@code false} when the record no longer exists (extraction found nothing to read), in
   *         which case no row was appended
   */
  public boolean appendRecord(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    if (!streaming) {
      throw new IllegalStateException("appendRecord() is the streaming builder's entry point; this builder walks");
    }
    if (!extractor.extractInto(rtx, recordKey)) {
      return false;
    }
    if (!extractor.appendTo(currentLeaf, recordKey)) {
      flushCurrentRowGroup();
      currentLeaf = newLeaf();
      extractor.appendTo(currentLeaf, recordKey);
    }
    rowsEmitted++;
    return true;
  }

  /**
   * Drain a streaming build: flush the partially-filled trailing leaf and, when the build never grew
   * past the dictionary sample, make the per-column dictionary decision and release the held-back
   * leaves. Mirrors the tail of {@link #build} exactly, which is what makes the incremental and
   * walking builds produce the same leaf sequence for the same records.
   */
  public void finishStreaming() {
    if (!streaming) {
      throw new IllegalStateException("finishStreaming() is the streaming builder's entry point; this builder walks");
    }
    flushCurrentRowGroup();
    if (sample != null) {
      decideDictionaryKindsAndDrainSample();
    }
  }

  /** @return total rows appended across all emitted leaves. */
  public long rowsEmitted() {
    return rowsEmitted;
  }

  /** Per-column kinds, index-aligned with the projection's declared fields. */
  public byte[] columnKinds() {
    return extractor.columnKinds();
  }

  /** @return number of serialised leaves handed to {@code leafSink}. */
  public long leavesEmitted() {
    return leavesEmitted;
  }

  /** Snapshot of the per-column non-integral flags, index-aligned with fieldNames. */
  public boolean[] numericColumnNonIntegralFlags() {
    return extractor.numericColumnNonIntegralFlags();
  }

  /**
   * True when the current rtx position is a record root under this projection. Matches by pathNodeKey
   * so the check is O(1) — no path walk — and correctly handles both OBJECT- and ARRAY-rooted records
   * (any kind whose pathNodeKey matches the declared root counts).
   */
  private boolean isRecordRoot(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isDocumentRoot())
      return false;
    final long pk = getPathNodeKeyAtCursor(rtx);
    return pk >= 0 && rootPathNodeKeys.contains(pk);
  }

  private static long getPathNodeKeyAtCursor(final JsonNodeReadOnlyTrx rtx) {
    // Only structured-kind nodes carry a pathNodeKey; primitives (value
    // nodes) live under an OBJECT_KEY so they return their parent's key
    // via the rtx node API. We consult the current node's kind and
    // dispatch accordingly. Fused OBJECT_NAMED_* records also carry a
    // pathNodeKey because they play the OBJECT_KEY role structurally.
    final NodeKind kind = rtx.getKind();
    if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY || kind.playsObjectKeyRole()) {
      return rtx.getPathNodeKey();
    }
    return -1L;
  }

  private void extractRow(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    extractor.extractAt(rtx, recordKey);
    if (!extractor.appendTo(currentLeaf, recordKey)) {
      flushCurrentRowGroup();
      currentLeaf = newLeaf();
      extractor.appendTo(currentLeaf, recordKey);
    }
    rowsEmitted++;
  }

  /** The per-column resource-wide dictionaries this build produced; null slots stayed per-leaf. */
  GlobalValueDictionaryWriter @Nullable [] globalDictionaries() {
    return globalDictionaries;
  }

  private ProjectionIndexRowGroupPage newLeaf() {
    final ProjectionIndexRowGroupPage reusable = reusableLeaf;
    if (reusable != null) {
      reusableLeaf = null;
      reusable.setGlobalDictionaries(globalDictionaries);
      return reusable;
    }
    final ProjectionIndexRowGroupPage leaf = new ProjectionIndexRowGroupPage(extractor.columnKindsRef());
    leaf.setGlobalDictionaries(globalDictionaries);
    return leaf;
  }

  private void flushCurrentRowGroup() {
    if (currentLeaf.getRowCount() == 0)
      return;
    if (sample != null) {
      sample.add(currentLeaf);
      if (sample.size() >= SAMPLE_LEAVES) {
        decideDictionaryKindsAndDrainSample();
      }
      return;
    }
    reusableLeaf = emitBorrowedLeafForReuse(currentLeaf, globalDictionaries, leafSink);
    leavesEmitted++;
  }

  /**
   * Borrow one live page synchronously, then reset it only after the callback has returned.
   *
   * <p>The ordering is the ownership proof: if the callback throws, execution never reaches the
   * reset and the failing borrower sees an unchanged page. Package visibility supports focused
   * failure-path coverage without exposing reuse through the public API.
   */
  static ProjectionIndexRowGroupPage emitBorrowedLeafForReuse(final ProjectionIndexRowGroupPage leaf,
      final GlobalValueDictionaryWriter[] dictionaries, final BorrowedLeafSink leafSink) {
    leaf.setGlobalDictionaries(dictionaries);
    leafSink.accept(leaf);
    leaf.resetForBuilderReuse(dictionaries);
    return leaf;
  }

  /**
   * Choose per string column between the per-leaf dictionary it was built with and a resource-wide
   * one, then re-encode the buffered leaves accordingly and let them through.
   *
   * <h2>Why the decision is made here rather than up front</h2>
   *
   * Which of the two shapes wins is a property of the DATA, not of the declared type: the same
   * {@code string} column is a handful of repeated labels in one resource and millions of distinct
   * identifiers in another. It cannot be known before rows are seen, and walking the resource twice
   * to find out would double the build. So the build runs normally, the leading leaves are held
   * back, and the per-leaf dictionaries they already contain ARE the measurement — converting them
   * afterwards costs one intern per dictionary entry, not per row.
   *
   * <h2>The measurement</h2>
   *
   * Not the distinct ratio but the per-leaf DEDUPLICATION FACTOR: sampled rows divided by the total
   * size of the per-leaf dictionaries. That is the quantity the choice actually turns on. A per-leaf
   * dictionary earns its keep by storing a recurring value once per leaf; when a leaf's dictionary
   * is nearly as large as its row count the dictionary stores almost nothing twice, so it has
   * become a second copy of the column plus an id per row. A global distinct ratio would not say
   * this — a column can have a million distinct values resource-wide and still repeat heavily
   * inside a leaf.
   */
  private void decideDictionaryKindsAndDrainSample() {
    final byte[] kinds = extractor.columnKindsRef();
    final GlobalValueDictionaryWriter[] dictionaries = new GlobalValueDictionaryWriter[kinds.length];
    final GlobalDictionaryMode mode = globalDictionaryMode();
    final long budgetBytes = globalDictionaryBudgetBytes();
    // A budget below what an EMPTY dictionary retains cannot promote anything: the writer would
    // refuse its own first value. Treat it as "no global dictionaries" rather than constructing
    // writers that fail on contact — a bound set too low is a decline, like every other bound here.
    final boolean budgetAdmitsAnyDictionary = budgetBytes >= GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES;
    if (!budgetAdmitsAnyDictionary && PROJ_DIAG) {
      System.err.println("[proj] global dictionaries DISABLED: budget " + budgetBytes + " B is below the "
          + GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + " B an empty dictionary already retains");
    }
    if (mode != GlobalDictionaryMode.NEVER && budgetAdmitsAnyDictionary) {
      long sampledRows = 0;
      for (final ProjectionIndexRowGroupPage leaf : sample) {
        sampledRows += leaf.getRowCount();
      }
      for (int c = 0; c < kinds.length; c++) {
        if (kinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          continue;
        }
        long perLeafDictTotal = 0;
        for (final ProjectionIndexRowGroupPage leaf : sample) {
          perLeafDictTotal += leaf.stringDictionarySize(c);
        }
        if (mode != GlobalDictionaryMode.ALWAYS && !isGlobalDictionaryWorthwhile(sampledRows, perLeafDictTotal)) {
          continue;
        }
        // ELECTION-TIME DECLINE. The dedup factor above says a resource-wide dictionary is the
        // better SHAPE; it says nothing about whether the thing will fit. Those are different
        // questions, and at 100M rows the second one is the one that kills the load — so ask it
        // here, before a single leaf is written as global, because that is the only point at which
        // declining one column is cheap. Afterwards its ids are in every leaf already and the only
        // retreat is abandoning the whole projection.
        //
        // Answerable only with a row-count hint: projected bytes need a distinct count, a distinct
        // count needs the corpus size, and a streaming build learns that when the stream ends,
        // thousands of leaves too late. Without a hint the runtime cap in the writer is the only
        // protection and the outcome is a late abandon — the documented contract, not a surprise.
        if (mode != GlobalDictionaryMode.ALWAYS && expectedRows > 0) {
          final long avgValueBytes = perLeafDictTotal == 0
              ? 0
              : sampledValueBytes(c) / perLeafDictTotal;
          final long projected = expectedRows * (avgValueBytes + PER_ENTRY_OVERHEAD_BYTES);
          // Half the budget, because the estimate is the weak link: avg length comes from leading
          // rows, and distinct==rows is an upper bound rather than a measurement (see task #51 —
          // within-leaf uniqueness does not imply global uniqueness). Erring toward the per-leaf
          // dictionary costs some query speed; erring the other way costs the load.
          if (projected > budgetBytes / 2) {
            if (PROJ_DIAG) {
              System.err.println("[proj] global dictionary DECLINED for column " + c + ": projected " + projected
                  + " B (" + expectedRows + " rows x " + (avgValueBytes + PER_ENTRY_OVERHEAD_BYTES)
                  + " B/entry) exceeds half of the " + budgetBytes + " B budget — column stays per-leaf DICT");
            }
            continue;
          }
        }
        dictionaries[c] = new GlobalValueDictionaryWriter(c, budgetBytes);
      }
    }

    // Convert every buffered leaf BEFORE flipping the shared kinds array — the array is one
    // instance shared by the extractor and all of them, so flipping first would make the second
    // leaf reject itself as already global.
    for (int c = 0; c < dictionaries.length; c++) {
      if (dictionaries[c] == null) {
        continue;
      }
      for (final ProjectionIndexRowGroupPage leaf : sample) {
        leaf.convertStringDictColumnToGlobal(c, dictionaries[c]);
      }
      kinds[c] = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
    }

    int globalColumns = 0;
    for (final GlobalValueDictionaryWriter dictionary : dictionaries) {
      if (dictionary != null) {
        globalColumns++;
      }
    }
    GLOBAL_DICTIONARY_COLUMNS.set(globalColumns);
    globalDictionaries = dictionaries;
    reusableLeaf = emitBorrowedSampleForReuse(sample, dictionaries, leafSink);
    leavesEmitted += sample.size();
    // currentLeaf is the last buffered leaf (flushCurrentRowGroup buffered it before calling
    // here), so it has already been converted and drained; extractRow replaces it with one built
    // under the new kinds.
    sample = null;
  }

  /**
   * Publish the dictionary-decided leading sample in order through the borrowed-page boundary.
   * Package visibility exists solely for focused ownership/parity coverage of the 64-page drain.
   */
  static void emitBorrowedSample(final List<ProjectionIndexRowGroupPage> sample,
      final GlobalValueDictionaryWriter[] dictionaries, final BorrowedLeafSink leafSink) {
    for (final ProjectionIndexRowGroupPage leaf : sample) {
      leaf.setGlobalDictionaries(dictionaries);
      leafSink.accept(leaf);
    }
  }

  /**
   * Drain the dictionary-election sample without reusing a page while any sample data is still
   * needed. All 64 leading leaves survive measurement, optional local-to-global conversion and
   * their borrowed callbacks intact; only the final leaf is reset, and only after its callback
   * returns successfully, to seed steady-state reuse.
   *
   * @return the reset final sample leaf, or {@code null} for an empty sample
   */
  static @Nullable ProjectionIndexRowGroupPage emitBorrowedSampleForReuse(
      final List<ProjectionIndexRowGroupPage> sample, final GlobalValueDictionaryWriter[] dictionaries,
      final BorrowedLeafSink leafSink) {
    final int last = sample.size() - 1;
    for (int i = 0; i <= last; i++) {
      final ProjectionIndexRowGroupPage leaf = sample.get(i);
      leaf.setGlobalDictionaries(dictionaries);
      leafSink.accept(leaf);
      if (i == last) {
        leaf.resetForBuilderReuse(dictionaries);
        return leaf;
      }
    }
    return null;
  }

  /**
   * Whether a resource-wide dictionary beats the per-leaf ones for a column, given the sample.
   *
   * @param sampledRows rows in the sample
   * @param perLeafDictTotal summed size of the sample's per-leaf dictionaries for the column
   */
  private static boolean isGlobalDictionaryWorthwhile(final long sampledRows, final long perLeafDictTotal) {
    if (perLeafDictTotal < MIN_GLOBAL_DICTIONARY_ENTRIES || sampledRows <= 0) {
      // Too few values for the machinery to repay itself: a small dictionary is cheap to store per
      // leaf, packs into very few bits per row, and needs no record reads at all to materialise.
      return false;
    }
    return sampledRows < (long) MIN_PER_LEAF_DEDUP_FACTOR * perLeafDictTotal;
  }
}

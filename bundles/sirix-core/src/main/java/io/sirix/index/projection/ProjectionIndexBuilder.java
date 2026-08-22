/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.DatabaseType;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.NamePage;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.jspecify.annotations.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Walks a JSON resource's current revision and materialises one row per record (= node whose
 * pathNodeKey matches the projection's root path) into {@link ProjectionIndexRowGroupPage}s.
 * Completed leaves are delivered to the internal synchronous sink in append order so the caller can
 * stream them into the HOT backing tree without holding more than one leaf in memory. The public
 * {@code Consumer<byte[]>} surface is preserved by a serialising adapter; internal persistence paths
 * borrow the live page and encode detached column segments directly.
 * Stable record keys need not be monotone in document traversal order. The builder greedily keeps
 * every key greater than the last routing-backbone key on that backbone and marks every inversion as
 * a sparse order exception. Exception rows stay in document order and receive exact persistent
 * locators when their bounded row group is emitted.
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
  /** Greatest normal routing-backbone key emitted so far; exceptions never advance it. */
  private long lastNormalRecordKey = Long.MIN_VALUE;
  private @Nullable SirixDeweyID lastOrderLabel;
  private final @Nullable LongFunction<SirixDeweyID> orderLabelResolver;

  /**
   * How many leading leaves are held back to measure string-column cardinality on. Sixteen leaves
   * are a bounded 16,384-row decision drain and match the resource-wide interner's structural entry
   * ceiling. AUTO explicitly declines when exact seeding leaves less than one fully distinct row
   * group's headroom, before any page is converted, so the next small burst of novel values cannot
   * immediately abandon the projection. NEVER mode skips the sample entirely because its result is
   * known before the first row.
   */
  private static final int SAMPLE_LEAVES = 16;

  /** Keep one fully distinct post-election row group below the interner's hard append ceiling. */
  private static final int MIN_GLOBAL_DICTIONARY_HEADROOM = ProjectionIndexRowGroupPage.MAX_ROWS;

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
   * Aggregate ceiling for all resource-wide string dictionaries in one build or maintenance
   * transaction, {@code -Dsirix.projection.globalDict.budgetBytes}.
   *
   * <p>
   * Default {@code min(heap/8, 2 GiB)}. The heap fraction is what keeps it sane on a small JVM; the
   * absolute cap is what keeps several elected columns from summing to the whole heap.  Build-time
   * election shares this aggregate evenly across every string column that could go global (before
   * knowing which candidates AUTO will accept); maintenance shares it across the persisted global
   * columns.  {@link Long#MAX_VALUE} explicitly disables the aggregate check for every share.
   * </p>
   */
  static long globalDictionaryBudgetBytes() {
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
   * Columns reported by the latest successfully published build to complete its diagnostic handoff.
   *
   * <p>Test observability, and load-bearing for one thing in particular: a differential that
   * compares the two encodings proves nothing unless the "global" arm actually produced a global
   * column, and nothing else about the outcome reveals whether it did. Concurrent builds report in
   * atomic handoff order; callers needing a resource-specific answer must inspect its metadata.
   */
  private static final java.util.concurrent.atomic.AtomicInteger GLOBAL_DICTIONARY_COLUMNS =
      new java.util.concurrent.atomic.AtomicInteger();

  /** How many global columns the latest successful build to report its diagnostic produced. */
  public static int globalDictionaryColumnsBuilt() {
    return GLOBAL_DICTIONARY_COLUMNS.get();
  }

  /** Dictionary policy resolved once for this build; system-property changes affect the next build. */
  private final GlobalDictionaryMode globalDictionaryMode;

  /**
   * Leading leaves held back until the per-column dictionary choice is made; null when bypassed or
   * drained.
   */
  private List<ProjectionIndexRowGroupPage> sample;

  /** Per-column resource-wide dictionaries; null slots are columns that stayed per-leaf. */
  private GlobalValueDictionaryWriter[] globalDictionaries;

  /** Encoders attached to live leaves; streaming builds rotate writers behind stable wrappers. */
  private GlobalValueDictionaryEncoder[] globalDictionaryEncoders;

  /** Build-local diagnostic, published only after the outer pipeline makes its metadata visible. */
  private int globalDictionaryColumns;

  /** {@code -Dsirix.projDiag}: explain election declines, which are otherwise silent by design. */
  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Heap cost of ONE dictionary entry beyond its value bytes.
   *
   * <p>
   * Derived, not guessed: {@code offsets} 8 B + {@code lengths} 4 B + primary and secondary hashes
   * 16 B = 28 B of index per id, plus the open-addressed table's {@code tableHashes} 8 B and
   * {@code tableIds} 4 B per SLOT.  The table is half full, so it holds two slots per entry — 24 B.
   * Steady state is therefore 52 B; the writer separately accounts for growth peaks, radix output,
   * fixed arena-chunk slack and the chunk directory.
   * </p>
   */
  private static final long PER_ENTRY_OVERHEAD_BYTES = 52L;

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

  /** Largest sampled per-leaf UTF-8 entry, without copying any dictionary value. */
  private int sampledMaximumValueBytes(final int column) {
    int maximum = 0;
    for (final ProjectionIndexRowGroupPage leaf : sample) {
      final int size = leaf.stringDictionarySize(column);
      for (int id = 0; id < size; id++) {
        maximum = Math.max(maximum, leaf.stringDictionaryEntryLength(column, id));
      }
    }
    return maximum;
  }

  /** Saturating projection: overflow is an over-budget estimate, never an accidental admission. */
  static long projectedGlobalDictionaryBytes(final long rows, final long averageValueBytes) {
    if (rows < 0L || averageValueBytes < 0L) {
      throw new IllegalArgumentException("dictionary projection inputs must not be negative");
    }
    final long bytesPerEntry = projectedGlobalDictionaryBytesPerEntry(averageValueBytes);
    return rows != 0L && bytesPerEntry > Long.MAX_VALUE / rows
        ? Long.MAX_VALUE
        : rows * bytesPerEntry;
  }

  private static long projectedGlobalDictionaryBytesPerEntry(final long averageValueBytes) {
    return averageValueBytes > Long.MAX_VALUE - PER_ENTRY_OVERHEAD_BYTES
        ? Long.MAX_VALUE
        : averageValueBytes + PER_ENTRY_OVERHEAD_BYTES;
  }

  /**
   * Whether rows are FED to this builder ({@link #appendRecord}) instead of walked by it. A streaming
   * builder never resolves the record-set root, because at the time an incremental build is armed the
   * root path class does not exist yet — the resource is empty.
   */
  private final boolean streaming;

  /** Walking XML builds accept element record roots only; scalar/non-element roots fail closed. */
  private final boolean xmlRootsAreElements;

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
    return new ProjectionIndexBuilder(indexDef, pathSummary, serializingLeafSink(leafSink), true, null);
  }

  /** Internal zero-copy row-group hand-off used by the synchronous bulk-load writer. */
  static ProjectionIndexBuilder streamingBorrowed(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final BorrowedLeafSink leafSink) {
    return new ProjectionIndexBuilder(indexDef, pathSummary, leafSink, true, null);
  }

  public ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final Consumer<byte[]> leafSink) {
    this(indexDef, pathSummary, serializingLeafSink(leafSink), false, null);
  }

  private ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final BorrowedLeafSink leafSink, final boolean streaming,
      final @Nullable LongFunction<SirixDeweyID> orderLabelResolver) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexBuilder requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    this.leafSink = Objects.requireNonNull(leafSink, "leafSink must not be null");
    this.globalDictionaryMode = globalDictionaryMode();
    this.streaming = streaming;
    this.orderLabelResolver = orderLabelResolver;
    if (streaming) {
      this.rootPathNodeKeys = LongSets.EMPTY_SET;
      this.rootAncestorPathNodeKeys = LongSets.EMPTY_SET;
      this.xmlRootsAreElements = true;
      this.extractor = new ProjectionIndexRowExtractor(indexDef, pathSummary);
      this.sample = initialDictionarySample();
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
    boolean allRootsAreElements = true;
    for (final Long pcr : rootPcrs) {
      rootPathNodeKeys.add(pcr.longValue());
      final PathNode rootNode = pathSummary.getPathNodeForPathNodeKey(pcr.longValue());
      if (rootNode == null || rootNode.getPathKind() != NodeKind.ELEMENT) {
        allRootsAreElements = false;
      }
    }
    this.xmlRootsAreElements = allRootsAreElements;
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
    this.sample = initialDictionarySample();
    this.currentLeaf = new ProjectionIndexRowGroupPage(extractor.columnKindsRef());
  }

  private List<ProjectionIndexRowGroupPage> initialDictionarySample() {
    if (globalDictionaryMode == GlobalDictionaryMode.NEVER) {
      return null;
    }
    for (final byte kind : extractor.columnKindsRef()) {
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        return new ArrayList<>(SAMPLE_LEAVES);
      }
    }
    return null;
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
   * definition's HOT sub-tree for explicit index creation. All writes ride the given writer; the
   * caller's commit persists them.
   *
   * @param emptyRecordSetAllowed creation fails loudly when the root path resolves to no path class
   *        (declaring an index over a non-existent record set is a caller error), while the
   *        explicit recreation may persist a truthful empty projection
   */
  public static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final JsonNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    buildAndPersist(indexDef, pathSummary, (NodeReadOnlyTrx) rtx, storageEngineWriter,
        emptyRecordSetAllowed);
  }

  public static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final XmlNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    buildAndPersist(indexDef, pathSummary, (NodeReadOnlyTrx) rtx, storageEngineWriter,
        emptyRecordSetAllowed);
  }

  private static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    final ProjectionIndexHOTStorage storage =
        ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
    // Explicit creation/recreation owns this complete index sub-tree. Reset it before emitting V0
    // so stale row groups and sparse negative record locators from an earlier incarnation cannot
    // survive under the new metadata. Ordinary transaction maintenance never takes this path.
    storage.resetTree();
    final ProjectionStructuralOrderDirectory.Accessor structuralOrderDirectory =
        ProjectionStructuralOrderDirectory.open(storage);
    // No document-wide pre-pass: the directory mints a record's order label the first time this
    // build asks for one, so it costs exactly one slot per emitted record and no second walk.
    structuralOrderDirectory.seedRoot(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    final LongFunction<ImmutableNode> documentNodeLookup =
        nodeKey -> storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    final LongFunction<SirixDeweyID> orderLabelResolver =
        recordKey -> structuralOrderDirectory.fullLabel(recordKey, documentNodeLookup,
            ProjectionStructuralOrderDirectory.RelabelSink.SEALED);
    final int priorRowGroupCount = 0;
    final ProjectionSetSummaryChunks.BuildAccumulator setSummaries =
        new ProjectionSetSummaryChunks.BuildAccumulator();
    if (emptyRecordSetAllowed && pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath())).isEmpty()) {
      final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
      final byte[] columnKinds = new byte[fieldTypes.size()];
      for (int i = 0; i < columnKinds.length; i++) {
        columnKinds[i] = mapTypeToColumnKind(fieldTypes.get(i), indexDef.getProjectionFields().get(i));
      }
      try {
        finishPersist(indexDef, storage, LongArrayList.of(), LongArrayList.of(), priorRowGroupCount,
            rtx.getRevisionNumber(), columnKinds, setSummaries, null, null);
        publishGlobalDictionaryColumnsBuilt(0);
      } finally {
        setSummaries.release();
      }
      return;
    }
    // Streaming build (descriptor layout): each leaf is written the moment the builder emits
    // it — one leaf in memory at a time, matching this class's streaming contract instead of
    // buffering all encoded leaves on the heap (~240 MB at the 100 M-row scale). Retained derived
    // state is bounded: at most one 32-leaf fence tail, one 256-leaf Bloom window per
    // string column and only set-summary values that still fit their one persisted summary chunk.
    final ProjectionIndexFences.BuildWriter fenceWriter = new ProjectionIndexFences.BuildWriter();
    final ProjectionBloomChunks.Writer bloomChunks = new ProjectionBloomChunks.Writer();
    final boolean hasSetColumn = hasStringSetColumn(indexDef);
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionRecordLocator.Accessor recordLocator = ProjectionRecordLocator.open(storage);
    final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(indexDef, pathSummary, leaf -> {
      if (leaf.getRowCount() == 0) {
        throw new IllegalStateException("Projection leaf " + fenceWriter.rowGroupCount() + " is empty");
      }
      final int physicalSlot = fenceWriter.rowGroupCount() + 1;
      // Accumulate the index-wide per-value ROW counts while the leaf is in hand. Summing the
      // per-leaf counts is exact: a record lives in exactly one leaf, and the per-leaf figures
      // already count rows rather than occurrences.
      if (hasSetColumn) {
        setSummaries.append(leaf);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(leaf, encodeWorkspace);
      storage.putRowGroupAsColumnSegmentSlots(physicalSlot, encoded);
      fenceWriter.append(storage, leaf.firstRecordKey(), leaf.lastRecordKey());
      persistOrderExceptionLocators(leaf, physicalSlot, recordLocator);
      bloomChunks.append(encoded, physicalSlot, storage);
    }, false, orderLabelResolver);
    try {
      if (rtx instanceof final JsonNodeReadOnlyTrx jsonRtx) {
        builder.build(jsonRtx);
      } else if (rtx instanceof final XmlNodeReadOnlyTrx xmlRtx) {
        builder.build(xmlRtx);
      } else {
        throw new IllegalArgumentException("projection build requires a JSON or XML node transaction");
      }
      final byte[] columnKinds = builder.columnKinds();
      bloomChunks.finishChunks(storage, fenceWriter.rowGroupCount(), columnKinds);
      // Dictionaries are written after the leaves, and only once: the leaves refer to values by id,
      // so nothing can be persisted about a dictionary until every id it will ever mint is known.
      final long[] valueDictionaryHeaderKeys =
          flushValueDictionaries(builder.globalDictionaries(), storageEngineWriter);
      fenceWriter.finish(storage, priorRowGroupCount);
      finishPersistWithStreamingFences(indexDef, storage, fenceWriter.rowGroupCount(), priorRowGroupCount,
          rtx.getRevisionNumber(),
          columnKinds, setSummaries, valueDictionaryHeaderKeys, bloomChunks);
      builder.publishGlobalDictionaryColumnsBuilt();
    } finally {
      try {
        bloomChunks.release();
      } finally {
        try {
          setSummaries.release();
        } finally {
          builder.releaseTransientState();
        }
      }
    }
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
   * Finish an explicit full-build boundary: tombstone orphaned slots above the new leaf count (real
   * deletes — hygiene, not load-bearing; metadata still bounds every read), persist summaries,
   * optional Bloom manifests and per-leaf record-key fences ({@link ProjectionIndexFences}), and only
   * then publish the authoritative live metadata at slot 0. A failure before that final slot-0 write
   * therefore leaves the previous live metadata or the load-time tombstone in force; it never exposes
   * a partially described set of persistent units.
   */
  static void finishPersist(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final LongArrayList firstKeys, final LongArrayList lastKeys, final int priorRowGroupCount,
      final int buildRevision, final byte[] columnKinds,
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries,
      final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
    finishPersist(indexDef, storage, firstKeys.size(), firstKeys, lastKeys, priorRowGroupCount,
        buildRevision, columnKinds, setSummaries, valueDictionaryHeaderKeys, streamingBloomChunks);
  }

  static void finishPersistWithStreamingFences(final IndexDef indexDef,
      final ProjectionIndexHOTStorage storage, final int rowGroupCount,
      final int priorRowGroupCount, final int buildRevision, final byte[] columnKinds,
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries,
      final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
    finishPersist(indexDef, storage, rowGroupCount, null, null, priorRowGroupCount, buildRevision,
        columnKinds, setSummaries, valueDictionaryHeaderKeys, streamingBloomChunks);
  }

  private static void finishPersist(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final int rowGroupCount, final @Nullable LongArrayList firstKeys,
      final @Nullable LongArrayList lastKeys, final int priorRowGroupCount,
      final int buildRevision, final byte[] columnKinds,
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries,
      final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
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
    final Map<Integer, Map<String, Long>> persistedSetSummaries =
        setSummaries.writeAll(storage, columnKinds);
    final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(rootPath, paths, names, columnKinds,
        rowGroupCount, buildRevision, persistedSetSummaries, valueDictionaryHeaderKeys);
    // Fingerprint BLOCKS — the contiguous acceleration over the per-leaf segments just written.
    // Tombstone-first covers columns whose block existed but is empty now.
    storage.removeBloomBlocks(columnKinds.length);
    if (firstKeys != null && lastKeys != null) {
      ProjectionIndexFences.write(storage, rowGroupCount, firstKeys.toLongArray(), lastKeys.toLongArray(),
          priorRowGroupCount);
    }
    if (streamingBloomChunks != null) {
      streamingBloomChunks.publishManifests(storage, rowGroupCount);
    }
    // Slot 0 is the authoritative visibility marker and is therefore published strictly after every
    // row group, summary, fence and optional Bloom manifest it describes.
    storage.putBlob(0, metadata.serialize());
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

  public void build(final XmlNodeReadOnlyTrx rtx) {
    if (streaming) {
      throw new IllegalStateException("A streaming projection builder is fed records through appendRecord(); "
          + "it never resolved a record-set root to walk");
    }
    if (!xmlRootsAreElements) {
      throw new IllegalArgumentException("XML projection record roots must resolve exclusively to element nodes");
    }
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (rtx.getKind() != NodeKind.ELEMENT
            || !rootPathNodeKeys.contains(rtx.getPathNodeKey())) {
          continue;
        }
        final long recordKey = rtx.getNodeKey();
        extractRow(rtx, recordKey);
        rtx.moveTo(recordKey);
      }
      flushCurrentRowGroup();
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
    return appendRecord(rtx, recordKey, nextOrderLabel());
  }

  boolean appendRecord(final JsonNodeReadOnlyTrx rtx, final long recordKey,
      final SirixDeweyID orderLabel) {
    if (!streaming) {
      throw new IllegalStateException("appendRecord() is the streaming builder's entry point; this builder walks");
    }
    final byte[] orderLabelBytes = Objects.requireNonNull(orderLabel, "orderLabel must not be null").toBytes();
    prepareLeafForOrderLabel(orderLabelBytes, "JSON");
    if (!extractor.extractInto(rtx, recordKey)) {
      return false;
    }
    appendExtractedRecord(recordKey, orderLabel, orderLabelBytes, "JSON");
    return true;
  }

  public boolean appendRecord(final XmlNodeReadOnlyTrx rtx, final long recordKey) {
    return appendRecord(rtx, recordKey, nextOrderLabel());
  }

  boolean appendRecord(final XmlNodeReadOnlyTrx rtx, final long recordKey,
      final SirixDeweyID orderLabel) {
    if (!streaming) {
      throw new IllegalStateException("appendRecord() is the streaming builder's entry point; this builder walks");
    }
    final byte[] orderLabelBytes = Objects.requireNonNull(orderLabel, "orderLabel must not be null").toBytes();
    prepareLeafForOrderLabel(orderLabelBytes, "XML");
    if (!extractor.extractInto(rtx, recordKey)) {
      return false;
    }
    appendExtractedRecord(recordKey, orderLabel, orderLabelBytes, "XML");
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

  void publishGlobalDictionaryColumnsBuilt() {
    publishGlobalDictionaryColumnsBuilt(globalDictionaryColumns);
  }

  private static void publishGlobalDictionaryColumnsBuilt(final int columns) {
    GLOBAL_DICTIONARY_COLUMNS.set(columns);
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
    final SirixDeweyID orderLabel = resolveOrderLabel(recordKey);
    final byte[] orderLabelBytes = orderLabel.toBytes();
    prepareLeafForOrderLabel(orderLabelBytes, "JSON");
    extractor.extractAt(rtx, recordKey);
    appendExtractedRecord(recordKey, orderLabel, orderLabelBytes, "JSON");
  }

  private void extractRow(final XmlNodeReadOnlyTrx rtx, final long recordKey) {
    final SirixDeweyID orderLabel = resolveOrderLabel(recordKey);
    final byte[] orderLabelBytes = orderLabel.toBytes();
    prepareLeafForOrderLabel(orderLabelBytes, "XML");
    extractor.extractAt(rtx, recordKey);
    appendExtractedRecord(recordKey, orderLabel, orderLabelBytes, "XML");
  }

  private SirixDeweyID nextOrderLabel() {
    return lastOrderLabel == null
        ? SirixDeweyID.newRootID().getNewChildID()
        : SirixDeweyID.newBetween(lastOrderLabel, null);
  }

  private SirixDeweyID resolveOrderLabel(final long recordKey) {
    final LongFunction<SirixDeweyID> resolver = orderLabelResolver;
    return resolver == null ? nextOrderLabel() : resolver.apply(recordKey);
  }

  private void prepareLeafForOrderLabel(final byte[] orderLabel, final String databaseType) {
    if (!currentLeaf.canAppendOrderLabel(orderLabel)) {
      flushCurrentRowGroup();
      currentLeaf = newLeaf();
      if (!currentLeaf.canAppendOrderLabel(orderLabel)) {
        throw new IllegalStateException("an empty projection row group rejected one " + databaseType
            + " record order label");
      }
    }
  }

  private void appendExtractedRecord(final long recordKey, final SirixDeweyID orderLabel,
      final byte[] orderLabelBytes, final String databaseType) {
    if (lastOrderLabel != null && lastOrderLabel.compareTo(orderLabel) >= 0) {
      throw new IllegalStateException("projection " + databaseType
          + " record order labels are not strictly increasing at record " + recordKey);
    }
    final boolean orderException = isOrderException(recordKey);
    if (!extractor.appendTo(currentLeaf, recordKey, orderException, orderLabelBytes)) {
      throw new IllegalStateException("a preflighted projection row group rejected one " + databaseType + " record");
    }
    lastOrderLabel = orderLabel;
    recordAppended(recordKey, orderException);
  }

  private boolean isOrderException(final long recordKey) {
    if (recordKey < 0) {
      throw new IllegalArgumentException("projection record key must be non-negative: " + recordKey);
    }
    return recordKey <= lastNormalRecordKey;
  }

  private void recordAppended(final long recordKey, final boolean orderException) {
    rowsEmitted++;
    if (!orderException) {
      lastNormalRecordKey = recordKey;
    }
  }

  /** Persist this emitted unit's sparse exact locators without retaining a build-wide exception map. */
  static void persistOrderExceptionLocators(final ProjectionIndexRowGroupPage leaf, final int physicalSlot,
      final ProjectionRecordLocator.Accessor recordLocator) {
    Objects.requireNonNull(leaf, "leaf must not be null");
    Objects.requireNonNull(recordLocator, "recordLocator must not be null");
    if (physicalSlot < 1 || physicalSlot > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("projection physical slot out of range: " + physicalSlot);
    }
    if (!leaf.hasOrderExceptions()) {
      return;
    }
    final long[] recordKeys = leaf.recordKeys();
    final int rowCount = leaf.getRowCount();
    for (int row = 0; row < rowCount; row++) {
      if (leaf.orderExceptionAt(row)) {
        recordLocator.put(recordKeys[row], physicalSlot);
      }
    }
  }

  /** The per-column resource-wide dictionaries this build produced; null slots stayed per-leaf. */
  GlobalValueDictionaryWriter @Nullable [] globalDictionaries() {
    return globalDictionaries;
  }

  void beginStreamingDictionaryEpoch(final StorageEngineWriter storageEngineWriter) {
    if (!streaming) {
      throw new IllegalStateException("dictionary epochs are only available to a streaming builder");
    }
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final GlobalValueDictionaryEncoder[] encoders = globalDictionaryEncoders;
    if (encoders == null) {
      return;
    }
    for (final GlobalValueDictionaryEncoder encoder : encoders) {
      if (encoder instanceof final StreamingGlobalDictionary dictionary) {
        dictionary.bind(storageEngineWriter);
      }
    }
  }

  long @Nullable [] flushStreamingDictionaryGeneration(
      final StorageEngineWriter storageEngineWriter) {
    if (!streaming) {
      throw new IllegalStateException("dictionary generations are only available to a streaming builder");
    }
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final GlobalValueDictionaryEncoder[] encoders = globalDictionaryEncoders;
    if (encoders == null) {
      return null;
    }
    long[] headerKeys = null;
    for (int column = 0; column < encoders.length; column++) {
      GlobalValueDictionaryEncoder encoder = encoders[column];
      if (encoder == null) {
        continue;
      }
      if (encoder instanceof final GlobalValueDictionaryWriter initialGeneration) {
        final StreamingGlobalDictionary streamingDictionary =
            new StreamingGlobalDictionary(column, initialGeneration);
        encoders[column] = streamingDictionary;
        if (globalDictionaries != null) {
          globalDictionaries[column] = null;
        }
        encoder = streamingDictionary;
      }
      if (!(encoder instanceof final StreamingGlobalDictionary dictionary)) {
        throw new IllegalStateException("streaming global dictionary column " + column
            + " has an unsupported encoder " + encoder.getClass().getName());
      }
      dictionary.bind(storageEngineWriter);
      if (headerKeys == null) {
        headerKeys = new long[encoders.length];
      }
      headerKeys[column] = dictionary.flush();
    }
    return headerKeys;
  }

  static final class StreamingGlobalDictionary implements GlobalValueDictionaryEncoder {
    private final int column;
    private final long budgetBytes;
    private final GlobalValueDictionaryWriter.AdmissionPolicy admissionPolicy;
    private final GlobalValueDictionaryHotCache hotValues = new GlobalValueDictionaryHotCache();
    private long headerKey;
    private int baseEntryCount;
    private @Nullable ValueDictionaryHeaderNode baseHeader;
    private @Nullable StorageEngineWriter storageEngineWriter;
    private @Nullable GlobalValueDictionaryWriter additions;
    private long persistentProbeCount;

    StreamingGlobalDictionary(final int column,
        final GlobalValueDictionaryWriter initialGeneration) {
      this.column = column;
      this.budgetBytes = initialGeneration.budgetBytes();
      this.admissionPolicy = initialGeneration.admissionPolicy();
      this.additions = initialGeneration;
    }

    void bind(final StorageEngineWriter writer) {
      Objects.requireNonNull(writer, "writer must not be null");
      if (storageEngineWriter != null && storageEngineWriter != writer) {
        throw new IllegalStateException("global dictionary column " + column
            + " is already bound to another storage epoch");
      }
      storageEngineWriter = writer;
      if (headerKey == 0) {
        return;
      }
      final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, writer);
      if (header == null || !header.isDirectoryComplete()) {
        throw new IllegalStateException("global dictionary column " + column
            + " cannot read its durable generation header " + headerKey);
      }
      baseHeader = header;
      baseEntryCount = header.getEntryCount();
    }

    @Override
    public int intern(final String value) {
      Objects.requireNonNull(value, "value must not be null");
      final int encodedLength = GlobalValueDictionaryEncoder.utf8LengthCapped(value,
          GlobalValueDictionaryWriter.MAX_VALUE_BYTES);
      if (encodedLength > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw new IllegalStateException("global dictionary column " + column
            + " cannot persist a UTF-8 value above "
            + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes");
      }
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      return intern(utf8, 0, utf8.length);
    }

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, source.length);
      final StorageEngineWriter writer = storageEngineWriter;
      if (writer == null) {
        throw new IllegalStateException("global dictionary column " + column
            + " is not bound to a streaming storage epoch");
      }
      if (length > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw new IllegalStateException("global dictionary column " + column
            + " cannot persist a value above " + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes");
      }
      GlobalValueDictionaryWriter generation = additions;
      if (generation != null) {
        final int localId = generation.findId(source, offset, length);
        if (localId > 0) {
          return Math.addExact(baseEntryCount, localId);
        }
      }
      final int hotId = hotValues.find(source, offset, length);
      if (hotId > 0) {
        return hotId;
      }
      if (headerKey != 0) {
        persistentProbeCount++;
        final int existing = GlobalValueDictionary.probe(headerKey, source, offset, length, writer);
        if (existing > 0) {
          hotValues.put(source, offset, length, existing);
          return existing;
        }
        if (existing == GlobalValueDictionary.ID_UNKNOWN) {
          throw new IllegalStateException("global dictionary column " + column
              + " cannot probe generation header " + headerKey);
        }
      }
      if (generation == null) {
        generation = new GlobalValueDictionaryWriter(column, budgetBytes, admissionPolicy);
        additions = generation;
      }
      final int localId = generation.intern(source, offset, length);
      final long globalId = (long) baseEntryCount + localId;
      if (globalId > Integer.MAX_VALUE) {
        throw new IllegalStateException("global dictionary column " + column + " exhausted dictionary ids");
      }
      return (int) globalId;
    }

    long persistentProbeCount() {
      return persistentProbeCount;
    }

    long flush() {
      final StorageEngineWriter writer = storageEngineWriter;
      if (writer == null) {
        throw new IllegalStateException("global dictionary column " + column
            + " is not bound to a streaming storage epoch");
      }
      final GlobalValueDictionaryWriter generation = additions;
      try {
        if (generation == null || generation.entryCount() == 0) {
          if (headerKey == 0) {
            throw new IllegalStateException("global dictionary column " + column
                + " has no values or durable header");
          }
          return headerKey;
        }
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(writer);
        if (headerKey == 0) {
          headerKey = generation.flush(namePage, databaseType, writer, writer.getLog());
        } else {
          final ValueDictionaryHeaderNode header = baseHeader;
          if (header == null) {
            throw new IllegalStateException("global dictionary column " + column
                + " has no base header for append");
          }
          generation.flushAppend(header, namePage, databaseType, writer, writer.getLog());
        }
        return headerKey;
      } finally {
        if (generation != null) {
          generation.release();
          additions = null;
        }
        baseHeader = null;
        storageEngineWriter = null;
      }
    }

    void release() {
      if (additions != null) {
        additions.release();
        additions = null;
      }
      baseHeader = null;
      storageEngineWriter = null;
    }
  }

  /**
   * Release build-only state after a streaming load finishes or aborts.
   *
   * <p>A {@link ProjectionBulkLoad} may remain reachable from a caller even after it has been removed
   * from the ACTIVE registry. Clearing the leading sample, reusable pages and dictionary intern
   * tables here prevents that harmless handle from retaining the build's largest transient
   * allocations. Dictionary release is idempotent, including after a successful flush.</p>
   */
  void releaseTransientState() {
    final GlobalValueDictionaryWriter[] dictionaries = globalDictionaries;
    if (dictionaries != null) {
      for (final GlobalValueDictionaryWriter dictionary : dictionaries) {
        if (dictionary != null) {
          dictionary.release();
        }
      }
      globalDictionaries = null;
    }
    final GlobalValueDictionaryEncoder[] encoders = globalDictionaryEncoders;
    if (encoders != null) {
      for (final GlobalValueDictionaryEncoder encoder : encoders) {
        if (encoder instanceof final StreamingGlobalDictionary dictionary) {
          dictionary.release();
        }
      }
      globalDictionaryEncoders = null;
    }
    if (sample != null) {
      sample.clear();
      sample = null;
    }
    currentLeaf = null;
    reusableLeaf = null;
  }

  private ProjectionIndexRowGroupPage newLeaf() {
    final ProjectionIndexRowGroupPage reusable = reusableLeaf;
    if (reusable != null) {
      reusableLeaf = null;
      reusable.setGlobalDictionaries(globalDictionaryEncoders);
      return reusable;
    }
    final ProjectionIndexRowGroupPage leaf = new ProjectionIndexRowGroupPage(extractor.columnKindsRef());
    leaf.setGlobalDictionaries(globalDictionaryEncoders);
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
    reusableLeaf = emitBorrowedLeafForReuse(currentLeaf, globalDictionaryEncoders, leafSink);
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
      final GlobalValueDictionaryEncoder[] dictionaries, final BorrowedLeafSink leafSink) {
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
    final GlobalDictionaryMode mode = globalDictionaryMode;
    final long totalBudgetBytes = globalDictionaryBudgetBytes();
    int possibleGlobalColumns = 0;
    for (final byte kind : kinds) {
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) possibleGlobalColumns++;
    }
    final long budgetBytes = possibleGlobalColumns == 0 || totalBudgetBytes == Long.MAX_VALUE
        ? totalBudgetBytes
        : totalBudgetBytes / possibleGlobalColumns;
    // A budget below what an EMPTY dictionary retains cannot promote anything: the writer would
    // refuse its own first value. Treat it as "no global dictionaries" rather than constructing
    // writers that fail on contact — a bound set too low is a decline, like every other bound here.
    final boolean budgetAdmitsAnyDictionary = budgetBytes >= GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES;
    if (mode == GlobalDictionaryMode.ALWAYS && possibleGlobalColumns > 0
        && !budgetAdmitsAnyDictionary) {
      throw new IllegalStateException("forced global dictionaries require at least "
          + GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + " B per string column, got "
          + budgetBytes + " B");
    }
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
        final int sampledLargestValueBytes = sampledMaximumValueBytes(c);
        if (sampledLargestValueBytes > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
          final String detail = "sampled UTF-8 value length " + sampledLargestValueBytes
              + " exceeds the safe V0 limit of " + GlobalValueDictionaryWriter.MAX_VALUE_BYTES
              + " bytes";
          if (mode == GlobalDictionaryMode.ALWAYS) {
            throw new IllegalStateException("forced global dictionary for column " + c
                + " cannot be built safely: " + detail);
          }
          if (PROJ_DIAG) {
            System.err.println("[proj] global dictionary DECLINED for column " + c + ": "
                + detail + " — column stays per-leaf DICT");
          }
          continue;
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
          final long bytesPerEntry = projectedGlobalDictionaryBytesPerEntry(avgValueBytes);
          final long projected = projectedGlobalDictionaryBytes(expectedRows, avgValueBytes);
          // Half the budget, because the estimate is the weak link: avg length comes from leading
          // rows, and distinct==rows is an upper bound rather than a measurement (see task #51 —
          // within-leaf uniqueness does not imply global uniqueness). Erring toward the per-leaf
          // dictionary costs some query speed; erring the other way costs the load.
          if (projected > budgetBytes / 2) {
            if (PROJ_DIAG) {
              System.err.println("[proj] global dictionary DECLINED for column " + c + ": projected " + projected
                  + " B (" + expectedRows + " rows x " + bytesPerEntry
                  + " B/entry) exceeds half of the " + budgetBytes + " B budget — column stays per-leaf DICT");
            }
            continue;
          }
        }
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter(c, budgetBytes,
            mode == GlobalDictionaryMode.ALWAYS
                ? GlobalValueDictionaryWriter.AdmissionPolicy.FAIL_CLOSED
                : GlobalValueDictionaryWriter.AdmissionPolicy.DECLINE);
        try {
          // perLeafDictTotal is deliberately NOT an admission bound: the same value is counted once
          // per leaf, so it is not the resource-wide distinct count. Seed the real bounded writer
          // from dictionary ranges instead. It discovers the exact sample distinct set without a
          // second hash table or any per-row allocation, and its structural/budget preflight refuses
          // before a sampled leaf has been converted.
          seedGlobalDictionaryFromSample(sample, c, dictionary);
          if (mode == GlobalDictionaryMode.AUTO
              && !globalDictionarySampleHasHeadroom(dictionary.entryCount())) {
            if (PROJ_DIAG) {
              System.err.println("[proj] global dictionary DECLINED for column " + c + ": exact sample seeding "
                  + "used " + dictionary.entryCount() + " of "
                  + GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND + " safe entries, leaving fewer "
                  + "than one full row group's " + MIN_GLOBAL_DICTIONARY_HEADROOM
                  + "-entry headroom — column stays per-leaf DICT");
            }
            dictionary.release();
            continue;
          }
          dictionaries[c] = dictionary;
        } catch (final GlobalDictionaryBudgetExceededException declined) {
          dictionary.release();
          if (PROJ_DIAG) {
            System.err.println("[proj] global dictionary DECLINED for column " + c + ": "
                + declined.getMessage() + " — column stays per-leaf DICT");
          }
        } catch (final RuntimeException | Error failure) {
          dictionary.release();
          throw failure;
        }
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
    globalDictionaries = dictionaries;
    globalDictionaryEncoders = new GlobalValueDictionaryEncoder[dictionaries.length];
    System.arraycopy(dictionaries, 0, globalDictionaryEncoders, 0, dictionaries.length);
    reusableLeaf = emitBorrowedSampleForReuse(sample, globalDictionaryEncoders, leafSink);
    leavesEmitted += sample.size();
    globalDictionaryColumns = globalColumns;
    // currentLeaf is the last buffered leaf (flushCurrentRowGroup buffered it before calling
    // here), so it has already been converted and drained; extractRow replaces it with one built
    // under the new kinds.
    sample = null;
  }

  /**
   * Intern the exact distinct values represented by the leading per-leaf dictionaries into the
   * bounded resource-wide writer, before any leaf is mutated. Duplicate values shared by multiple
   * leaves perform allocation-free lookups in {@link GlobalValueDictionaryWriter}; a structural or
   * aggregate-budget decline therefore leaves every sampled page in its original local-dictionary
   * representation.
   */
  static void seedGlobalDictionaryFromSample(final List<ProjectionIndexRowGroupPage> sample,
      final int column, final GlobalValueDictionaryWriter dictionary) {
    final long[] referencedLocalIds = new long[(ProjectionIndexRowGroupPage.MAX_ROWS + Long.SIZE - 1) / Long.SIZE];
    for (int leafIndex = 0; leafIndex < sample.size(); leafIndex++) {
      final ProjectionIndexRowGroupPage leaf = sample.get(leafIndex);
      Arrays.fill(referencedLocalIds, 0L);
      final int[] localIds = leaf.stringDictIdColumn(column);
      final long[] presence = leaf.presenceColumnBits(column);
      for (int row = 0; row < leaf.getRowCount(); row++) {
        if ((presence[row >>> 6] & (1L << (row & 63))) != 0) {
          final int localId = localIds[row];
          referencedLocalIds[localId >>> 6] |= 1L << (localId & 63);
        }
      }
      final int dictionarySize = leaf.stringDictionarySize(column);
      for (int localId = 0; localId < dictionarySize; localId++) {
        if ((referencedLocalIds[localId >>> 6] & (1L << (localId & 63))) == 0) {
          continue; // absent cells can leave an unreferenced placeholder in the local dictionary
        }
        final byte[] backing = leaf.stringDictionaryEntryBacking(column, localId);
        dictionary.intern(backing, leaf.stringDictionaryEntryOffset(column, localId),
            leaf.stringDictionaryEntryLength(column, localId));
      }
    }
  }

  static boolean globalDictionarySampleHasHeadroom(final int entryCount) {
    if (entryCount < 0) {
      throw new IllegalArgumentException("entryCount must not be negative");
    }
    return entryCount <= GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND
        - MIN_GLOBAL_DICTIONARY_HEADROOM;
  }

  /**
   * Publish the dictionary-decided leading sample in order through the borrowed-page boundary.
   * Package visibility exists solely for focused ownership/parity coverage of the bounded sample
   * drain.
   */
  static void emitBorrowedSample(final List<ProjectionIndexRowGroupPage> sample,
      final GlobalValueDictionaryEncoder[] dictionaries, final BorrowedLeafSink leafSink) {
    for (final ProjectionIndexRowGroupPage leaf : sample) {
      leaf.setGlobalDictionaries(dictionaries);
      leafSink.accept(leaf);
    }
  }

  /**
   * Drain the dictionary-election sample without reusing a page while any sample data is still
   * needed. All leading leaves survive measurement, optional local-to-global conversion and
   * their borrowed callbacks intact; only the final leaf is reset, and only after its callback
   * returns successfully, to seed steady-state reuse.
   *
   * @return the reset final sample leaf, or {@code null} for an empty sample
   */
  static @Nullable ProjectionIndexRowGroupPage emitBorrowedSampleForReuse(
      final List<ProjectionIndexRowGroupPage> sample, final GlobalValueDictionaryEncoder[] dictionaries,
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

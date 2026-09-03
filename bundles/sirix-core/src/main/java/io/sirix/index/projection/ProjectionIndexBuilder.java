/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.DatabaseType;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.NamePage;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Walks a JSON resource's current revision and materialises one row per record (= node whose
 * pathNodeKey matches the projection's root path) into {@link ProjectionIndexRowGroupPage}s.
 * Completed leaves are delivered to the internal synchronous sink in append order so the caller can
 * stream them into the HOT backing tree without holding more than one leaf in memory. The public
 * {@code Consumer<byte[]>} surface is preserved by a serialising adapter; internal persistence
 * paths borrow the live page and encode detached column segments directly. Stable record keys need
 * not be monotone in document traversal order. The builder greedily keeps every key greater than
 * the last routing-backbone key on that backbone and marks every inversion as a sparse order
 * exception. Exception rows stay in document order and receive exact persistent locators when their
 * bounded row group is emitted.
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
   * <p>
   * The callback borrows the page only for the duration of {@link #accept}. It must neither mutate
   * the page nor retain the page or any array reachable through its accessors. Data that outlives the
   * callback must be copied into independently owned output before the callback returns. In
   * particular,
   * {@link ProjectionIndexColumnSegmentCodec#encode(ProjectionIndexRowGroupPage, ProjectionIndexColumnSegmentCodec.EncodeWorkspace)}
   * satisfies this contract: all of its published outputs are detached.
   */
  @FunctionalInterface
  interface BorrowedLeafSink {
    void accept(ProjectionIndexRowGroupPage leaf);
  }

  private final BorrowedLeafSink leafSink;

  /** Shared per-record extraction engine (also used by incremental maintenance). */
  private final ProjectionIndexRowExtractor extractor;

  /**
   * The trie lane's encode-side resolver, or {@code null} when no prebuilt dictionaries are bound.
   *
   * <p>
   * Held here rather than passed around because THIS is where the (path class -> column) mapping
   * becomes known: the extractor re-resolves it against a still-growing path summary, and the
   * resolver's flush-lane readers need a snapshot of it that they can read without a lock. Every
   * refresh republishes.
   * </p>
   */
  private @Nullable TrieLaneWriteDictionaries trieLaneWriteDictionaries;

  /** Segment-scoped encode dictionaries, or {@code null} when that lane is off. */
  private @Nullable SegmentScopedDictionaries segmentScopedDictionaries;

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

  /** Armed by the explicit build only; the walk toggles it around top-level record-set arrays. */
  private @Nullable InOrderLabelLane inOrderLane;

  /** The document root {@link #tryFastArrayIteration} started from; -1 outside a fast iteration. */
  private long fastIterationDocRoot = -1;

  /**
   * The label source a WALKING build resolves against when the caller supplied none — a heap-backed
   * {@link ProjectionStructuralOrderDirectory}, installed by {@link #build} and dropped when it
   * returns.
   *
   * <p>
   * It exists so the order-label lane is a property of the DOCUMENT and not of the entry point:
   * {@link #buildAndPersist} resolves ancestor-prefixed labels out of the persisted directory, and a
   * builder that emitted its own flat sequence instead would produce leaves that are byte-different
   * for the same records — and, worse, labels that no post-build insert could be placed among,
   * because the change listener mints those with {@code fullLabel} too.
   * </p>
   */
  private @Nullable LongFunction<SirixDeweyID> walkOrderLabelResolver;

  /**
   * How many leading leaves are held back to measure string-column cardinality on. Sixteen leaves are
   * a bounded 16,384-row decision drain and match the resource-wide interner's structural entry
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
   * deduplication factor says. A small dictionary is cheap per leaf, packs into a couple of bits per
   * row, and materialises with no record read at all; the resource-wide machinery cannot repay itself
   * against that.
   */
  private static final int MIN_GLOBAL_DICTIONARY_ENTRIES =
      Integer.getInteger("sirix.projection.globalDict.minEntries", 4096);

  /**
   * The per-leaf deduplication factor a column must fail to reach before it goes global — rows per
   * distinct value within a leaf. At 4, a 1024-row leaf holds at most 256 distinct values and the
   * per-leaf dictionary is genuinely compressing; above that number of distinct values it is mostly
   * storing each string once and adding an id per row on top.
   */
  private static final int MIN_PER_LEAF_DEDUP_FACTOR = Integer.getInteger("sirix.projection.globalDict.dedupFactor", 4);

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
   * <p>
   * <b>Defaults to {@code auto}</b>, now that the executor consumes the kind: a global column's group
   * key runs through the integer kernel, its ids feed a distinct fold directly, predicate literals
   * resolve to ids once per query, and winners reverse-map in one batch. What {@code auto} does NOT
   * do is force the encoding — the per-leaf dedup factor decides per column, so a column with a few
   * dozen repeated labels keeps its per-leaf dictionary and every route it already had.
   *
   * <p>
   * {@code always} is a testing knob and nothing else. It forces the encoding onto columns the
   * heuristic would never choose it for, including low-cardinality ones, which is exactly what makes
   * it useful for a differential — and also means it moves shapes that need STRING VALUES rather than
   * identities (ordering by the key, substring and stringify transforms, composite keys) onto the
   * generic pipeline. Those decline; they do not answer differently.
   */
  /**
   * Aggregate ceiling for all resource-wide string dictionaries in one build or maintenance
   * transaction, {@code -Dsirix.projection.globalDict.budgetBytes}.
   *
   * <p>
   * Default {@code min(heap/8, 2 GiB)}. The heap fraction is what keeps it sane on a small JVM; the
   * absolute cap is what keeps several elected columns from summing to the whole heap. AUTO ranks
   * only worthwhile candidates after its bounded leading sample, reserves at least twice each
   * hinted projection per simultaneously resident dictionary structure (four times for the
   * generation-writer plus probe-front pair used by streaming builds), and admits a deterministic
   * subset whose combined budgets sum to no more than this aggregate. Forced mode shares it evenly
   * across every requested string column; maintenance shares it across the persisted global columns.
   * {@link Long#MAX_VALUE} explicitly disables the aggregate check for every writer.
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
   * <p>
   * Test observability, and load-bearing for one thing in particular: a differential that compares
   * the two encodings proves nothing unless the "global" arm actually produced a global column, and
   * nothing else about the outcome reveals whether it did. Concurrent builds report in atomic handoff
   * order; callers needing a resource-specific answer must inspect its metadata.
   */
  private static final java.util.concurrent.atomic.AtomicInteger GLOBAL_DICTIONARY_COLUMNS =
      new java.util.concurrent.atomic.AtomicInteger();

  /** How many global columns the latest successful build to report its diagnostic produced. */
  public static int globalDictionaryColumnsBuilt() {
    return GLOBAL_DICTIONARY_COLUMNS.get();
  }

  /**
   * Persistent-radix probes the latest reported build's dictionaries issued, same atomic-handoff
   * semantics as {@link #globalDictionaryColumnsBuilt()}. Load-bearing witness for the intern-table
   * retention contract: a bulk load that keeps its tables resident until {@code finish()} creates no
   * generation mid-load, so its interns can never reach the persistent-probe branch and this must
   * report {@code 0}. A non-zero value on a fresh bulk load means the uncached per-value radix-walk
   * regime is back. (Loads into a resource that already carries a durable generation legitimately
   * probe it; those report their real count.)
   */
  private static final AtomicLong PERSISTENT_DICTIONARY_PROBES = new AtomicLong();

  /** Persistent-dictionary probes reported by the latest build's diagnostic handoff. */
  public static long persistentDictionaryProbesReported() {
    return PERSISTENT_DICTIONARY_PROBES.get();
  }

  /**
   * Dictionary policy resolved once for this build; system-property changes affect the next build.
   */
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

  /**
   * Per column, a dictionary that ALREADY holds every value this build will meet, or {@code null}.
   *
   * <p>
   * When present the ordinary election never runs: no sample is buffered, no budget is planned and no
   * streaming dictionary is created, because the question those stages answer — is a resource-wide
   * dictionary worth it, and can its probe front be afforded — was settled by whoever built this one.
   * </p>
   */
  private PrebuiltGlobalDictionary @Nullable [] prebuiltGlobalDictionaries;

  /**
   * Anchors named by {@code -Dsirix.projection.globalDict.prebuilt}, awaiting a writer to read them
   * through. Format {@code column:headerKey[,column:headerKey...]}, column being the index in the
   * definition's field order.
   *
   * <p>
   * A property rather than an API argument because the builder is created several layers below
   * whoever ran the pre-pass, and this is the fresh-build route's only entry point until the
   * promotion gate is re-derived to elect it on measured cardinality.
   * </p>
   */
  private long @Nullable [] pendingPrebuiltAnchors;

  /** {@code -Dsirix.projDiag}: explain election declines, which are otherwise silent by design. */
  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Heap cost of ONE dictionary entry beyond its value bytes.
   *
   * <p>
   * Derived, not guessed: {@code offsets} 8 B + {@code lengths} 4 B + primary and secondary hashes 16
   * B = 28 B of index per id, plus the open-addressed table's {@code tableHashes} 8 B and
   * {@code tableIds} 4 B per SLOT. The table is half full, so it holds two slots per entry — 24 B.
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
   * large enough will abandon its projection mid-build — correct, non-fatal, and measured as the row
   * path.
   * </p>
   */
  private long expectedRows = -1L;

  /**
   * Tell the build how many records to expect, so the global-dictionary election can decline a column
   * that would not fit. Values {@code <= 0} mean "unknown" and disable the election-time check; the
   * runtime cap still applies.
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
   * Plan AUTO's per-writer byte ceilings against one aggregate budget.
   *
   * <p>
   * {@code projectedBytes} and {@code benefitScores} are indexed by projection column;
   * {@code -1} marks a non-candidate. A non-streaming build reserves the current two-times
   * uncertainty margin for its one writer. A streaming build retains both a generation writer and a
   * whole-load probe front, so it reserves four times the projection and later splits that combined
   * envelope evenly: each resident structure keeps the same two-times margin independently. An
   * unhinted candidate passes {@code 0} and therefore reserves the corresponding mandatory minimum;
   * its runtime guard remains the authoritative bound. Candidates are ranked by descending sampled
   * local-dictionary pressure, then ascending mandatory reservation, then column number. The final
   * spare bytes are divided in that same order, so the result is deterministic and the sum of all
   * finite combined ceilings is exactly the configured aggregate whenever at least one candidate
   * fits.
   *
   * <p>
   * This is a once-per-build, column-sized plan. It adds no work or allocation to row ingestion.
   */
  static long[] planAutoGlobalDictionaryBudgets(final long totalBudgetBytes, final long[] projectedBytes,
      final long[] benefitScores, final boolean streaming) {
    if (totalBudgetBytes <= 0L) {
      throw new IllegalArgumentException("totalBudgetBytes must be positive");
    }
    Objects.requireNonNull(projectedBytes, "projectedBytes must not be null");
    Objects.requireNonNull(benefitScores, "benefitScores must not be null");
    if (projectedBytes.length != benefitScores.length) {
      throw new IllegalArgumentException("projectedBytes and benefitScores must be index-aligned");
    }

    final long[] allocations = new long[projectedBytes.length];
    final int[] order = new int[projectedBytes.length];
    int candidateCount = 0;
    for (int column = 0; column < projectedBytes.length; column++) {
      final long projected = projectedBytes[column];
      if (projected < -1L) {
        throw new IllegalArgumentException("projectedBytes[" + column + "] must be -1 or non-negative");
      }
      if (benefitScores[column] < 0L) {
        throw new IllegalArgumentException("benefitScores[" + column + "] must not be negative");
      }
      if (projected >= 0L) {
        order[candidateCount++] = column;
      }
    }
    if (candidateCount == 0) {
      return allocations;
    }
    if (totalBudgetBytes == Long.MAX_VALUE) {
      for (int i = 0; i < candidateCount; i++) {
        allocations[order[i]] = Long.MAX_VALUE;
      }
      return allocations;
    }

    // RowGroupDescriptor permits 13,105 columns. Primitive in-place heapsort keeps this cold
    // decision O(columns log columns), adds no boxed index array/comparator scratch, and has a fixed
    // stack footprint.
    sortAutoDictionaryCandidates(order, candidateCount, projectedBytes, benefitScores, streaming);

    long remaining = totalBudgetBytes;
    int admittedCount = 0;
    for (int i = 0; i < candidateCount; i++) {
      final int column = order[i];
      final long required = conservativeAutoDictionaryBudget(projectedBytes[column], streaming);
      if (required <= remaining) {
        allocations[column] = required;
        order[admittedCount++] = column;
        remaining -= required;
      }
    }
    if (admittedCount == 0) {
      return allocations;
    }

    // Budget ceilings, unlike actual retention, are free to distribute. Giving the selected set
    // the unused aggregate avoids an unnecessary runtime decline while preserving the aggregate
    // proof even if every combined envelope simultaneously reaches its ceiling.
    final long perEnvelopeBonus = remaining / admittedCount;
    long remainder = remaining % admittedCount;
    for (int i = 0; i < admittedCount; i++) {
      final int column = order[i];
      allocations[column] += perEnvelopeBonus;
      if (remainder > 0L) {
        allocations[column]++;
        remainder--;
      }
    }
    return allocations;
  }

  private static void sortAutoDictionaryCandidates(final int[] order, final int length, final long[] projectedBytes,
      final long[] benefitScores, final boolean streaming) {
    for (int root = (length >>> 1) - 1; root >= 0; root--) {
      siftAutoDictionaryCandidateDown(order, root, length, projectedBytes, benefitScores, streaming);
    }
    for (int end = length - 1; end > 0; end--) {
      final int worst = order[0];
      order[0] = order[end];
      order[end] = worst;
      siftAutoDictionaryCandidateDown(order, 0, end, projectedBytes, benefitScores, streaming);
    }
  }

  /** Max-heap by total rank: the worst remaining candidate is moved to the sorted tail. */
  private static void siftAutoDictionaryCandidateDown(final int[] order, int root, final int length,
      final long[] projectedBytes, final long[] benefitScores, final boolean streaming) {
    final int candidate = order[root];
    int child = (root << 1) + 1;
    while (child < length) {
      final int right = child + 1;
      if (right < length && compareAutoDictionaryCandidates(order[child], order[right], projectedBytes,
          benefitScores, streaming) < 0) {
        child = right;
      }
      if (compareAutoDictionaryCandidates(candidate, order[child], projectedBytes, benefitScores, streaming) >= 0) {
        break;
      }
      order[root] = order[child];
      root = child;
      child = (root << 1) + 1;
    }
    order[root] = candidate;
  }

  /** Negative means {@code left} ranks before {@code right}. Every key is total and deterministic. */
  private static int compareAutoDictionaryCandidates(final int left, final int right, final long[] projectedBytes,
      final long[] benefitScores, final boolean streaming) {
    final int benefitOrder = Long.compare(benefitScores[right], benefitScores[left]);
    if (benefitOrder != 0) {
      return benefitOrder;
    }
    final int budgetOrder = Long.compare(conservativeAutoDictionaryBudget(projectedBytes[left], streaming),
        conservativeAutoDictionaryBudget(projectedBytes[right], streaming));
    return budgetOrder != 0
        ? budgetOrder
        : Integer.compare(left, right);
  }

  /**
   * Twice the estimate per simultaneously resident structure, saturating closed; real empty-state
   * lower bounds always win.
   */
  private static long conservativeAutoDictionaryBudget(final long projectedBytes, final boolean streaming) {
    if (projectedBytes < 0L) {
      throw new IllegalArgumentException("projectedBytes must not be negative");
    }
    final int residentStructures = streaming ? 2 : 1;
    final long minimum = saturatedMultiply(GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES, residentStructures);
    final long conservativeProjection = saturatedMultiply(projectedBytes, 2 * residentStructures);
    return Math.max(minimum, conservativeProjection);
  }

  private static long saturatedMultiply(final long value, final int multiplier) {
    if (value < 0L || multiplier <= 0) {
      throw new IllegalArgumentException("saturated multiplication requires non-negative value and positive factor");
    }
    return value > Long.MAX_VALUE / multiplier
        ? Long.MAX_VALUE
        : value * multiplier;
  }

  /**
   * One streaming column's disjoint writer/front cap. {@link Long#MAX_VALUE} deliberately preserves
   * the explicit unbounded setting; every finite combined envelope is divided without overcommit.
   */
  static long streamingGlobalDictionaryComponentBudget(final long combinedBudgetBytes) {
    if (combinedBudgetBytes == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    final long minimumCombined = saturatedMultiply(GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES, 2);
    if (combinedBudgetBytes < minimumCombined) {
      throw new IllegalArgumentException("streaming global dictionary combined budget must be at least "
          + minimumCombined + " B, got " + combinedBudgetBytes);
    }
    return combinedBudgetBytes / 2L;
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
      this.pendingPrebuiltAnchors = configuredPrebuiltAnchors(extractor.columnKindsRef().length);
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
    this.pendingPrebuiltAnchors = configuredPrebuiltAnchors(extractor.columnKindsRef().length);
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
    if (type == Type.DATI || type == Type.DATE) {
      // Declared temporal columns store an epoch in the long lane and reproduce the document's exact
      // text on emission (ProjectionTemporalCodec). Under the kill switch they build and serve as
      // ordinary per-leaf string columns, which is what every such column did before this kind
      // existed — same answers, ~25 B/row instead of ~1.
      if (ProjectionTemporalCodec.temporalKindsEnabled()) {
        return type == Type.DATI
            ? ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP
            : ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;
      }
      return ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
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
   *        (declaring an index over a non-existent record set is a caller error), while the explicit
   *        initializer may persist a truthful empty projection
   */
  public static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final JsonNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    buildAndPersist(indexDef, pathSummary, (NodeReadOnlyTrx) rtx, storageEngineWriter, emptyRecordSetAllowed, null);
  }

  public static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final XmlNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed) {
    buildAndPersist(indexDef, pathSummary, (NodeReadOnlyTrx) rtx, storageEngineWriter, emptyRecordSetAllowed, null);
  }

  /**
   * One-shot fault seam after the document walk, before any derived chunks or metadata publish.
   *
   * <p>
   * At this point the walk's row groups have been ACCUMULATED and encoded — readable through the
   * live bulk accumulator / read-through — but nothing has been finalized: the HOT tree has not been
   * spliced, no bloom chunks are finished, no fence is written and no metadata is published.
   *
   * <p>
   * The hook receives the build's OWN storage rather than nothing, so a caller can witness the
   * builder's state at the seam through the live instance. Constructing a second storage afterwards
   * is not an equivalent observation: a build that failed here leaves the transaction rollback-only,
   * and that constructor legitimately prepares a writable page, so it must — and does — fail closed.
   * Reading through the instance that already exists is the only way to observe the seam without
   * attempting a write the transaction is right to refuse.
   *
   * @param postWalkHook receives the build's storage; called exactly once at the seam
   */
  static void buildAndPersistWithPostWalkHook(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final JsonNodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter,
      final boolean emptyRecordSetAllowed, final Consumer<ProjectionIndexHOTStorage> postWalkHook) {
    buildAndPersist(indexDef, pathSummary, (NodeReadOnlyTrx) rtx, storageEngineWriter, emptyRecordSetAllowed,
        Objects.requireNonNull(postWalkHook));
  }

  private static void buildAndPersist(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx, final StorageEngineWriter storageEngineWriter, final boolean emptyRecordSetAllowed,
      final @Nullable Consumer<ProjectionIndexHOTStorage> postWalkHook) {
    // Every complete build — initial creation or a fallback rebuild — funnels through this one
    // variant, so the tick here makes maintenanceTelemetry().fullRebuilds() non-vacuous: a
    // maintenance window that quietly rebuilt the corpus shows a nonzero count instead of the
    // hard-coded zero it used to report.
    ProjectionIndexChangeListener.recordFullRebuild();
    // Bounded retention: without intermediate flushes every page this build creates stays pinned in
    // the transaction intent log until the caller's single commit — at 100M ClickBench rows that is
    // ~20 GB of live 64 KiB frames, more than the whole off-heap arena on a 32 GB machine. Riding
    // the writer's ordinary async-flush epochs keeps the log at one epoch of pages instead. The
    // rotations are safe even when the transaction holds uncommitted document records this build
    // still has to read: a flushed page of the open revision resolves back from disk through the
    // log's recorded offsets (verified by a 40k-record forced-flush build, checksum-exact), and
    // nothing becomes a visible revision before the caller's commit — rollback still discards
    // every flushed page, exactly as for bulk-import epochs.
    final boolean intermediateFlushEnabled =
        !"false".equals(System.getProperty("sirix.projection.buildIntermediateFlush"));
    final BulkBuildEpoch epoch = new BulkBuildEpoch(storageEngineWriter, indexDef.getID());
    // The bulk builder is an initializer, never a second update strategy. A populated definition is
    // maintained in routed units by ProjectionIndexChangeListener and must not be reset/rebuilt here.
    epoch.storage.requireVirginTreeForInitialBuild();
    // Fresh build on a virgin tree: accumulate every slot write and materialize the tree in one
    // canonical bulk pass (docs/HOT_BULK_BUILD.md §2 — measured 9–14× the per-entry path
    // for the order-label and column-segment shapes). Point reads during the build are served
    // from the accumulator (read-through); side-page attaches are deferred under a byte budget;
    // a capacity trip splices the prefix and falls back to per-entry. The accumulator lives
    // OUTSIDE the transaction-intent log, so it also LOWERS mid-build live-entry retention —
    // epoch rebinds transplant it (BulkBuildEpoch.rebind). The finally-splice either materializes
    // the accumulated prefix or marks the page transaction rollback-only; it never silently drops
    // a spent accumulator into a committable transaction.
    epoch.storage.beginBulkSlotAccumulation();
    Throwable buildFailure = null;
    try {
      // No document-wide pre-pass: the directory mints a record's order label the first time this
      // build asks for one, so it costs exactly one slot per emitted record and no second walk.
      epoch.orderDirectory.seedRoot(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final LongFunction<ImmutableNode> documentNodeLookup =
          nodeKey -> storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      // In-order lane: while the walk iterates the members of a top-level record-set array, labels
      // mint by the pure append algebra with ZERO document lookups. The general fullLabel path mints
      // the FIRST record's label by labelling its ENTIRE maximal unlabelled sibling run — on a fresh
      // 100M build that is a full-corpus sibling walk plus one slot write per record before the first
      // row extracts, and none of it can flush because no leaf boundary has been reached yet. The
      // lane is byte-equivalent for exactly this arrival order (the argument lives on
      // fullLabelForInOrderAppend) and the carry survives epoch rebinds because the caller holds it.
      final InOrderLabelLane inOrderLane = new InOrderLabelLane();
      final LongFunction<SirixDeweyID> orderLabelResolver = recordKey -> {
        if (inOrderLane.containerKey >= 0) {
          final SirixDeweyID fullLabel = epoch.orderDirectory.fullLabelForInOrderAppend(recordKey,
              inOrderLane.containerKey, Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), inOrderLane.previousLocal);
          inOrderLane.previousLocal = epoch.orderDirectory.lastInOrderAppendedLocal();
          return fullLabel;
        }
        return epoch.orderDirectory.fullLabel(recordKey, documentNodeLookup,
            ProjectionStructuralOrderDirectory.RelabelSink.SEALED);
      };
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries =
          new ProjectionSetSummaryChunks.BuildAccumulator();
      if (emptyRecordSetAllowed && pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath())).isEmpty()) {
        final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
        final byte[] columnKinds = new byte[fieldTypes.size()];
        for (int i = 0; i < columnKinds.length; i++) {
          columnKinds[i] = mapTypeToColumnKind(fieldTypes.get(i), indexDef.getProjectionFields().get(i));
        }
        try {
          finishPersist(indexDef, epoch.storage, LongArrayList.of(), LongArrayList.of(), rtx.getRevisionNumber(),
              columnKinds, setSummaries, null, null);
          publishGlobalDictionaryColumnsBuilt(0);
        } finally {
          setSummaries.release();
        }
        return;
      }
      // Streaming build (segment-slot layout): each leaf is written the moment the builder emits
      // it — one leaf in memory at a time, matching this class's streaming contract instead of
      // buffering all encoded leaves on the heap (~240 MB at the 100 M-row scale). Retained derived
      // state is bounded: at most one 32-leaf fence tail, one 256-leaf Bloom window per
      // string column and only set-summary values that still fit their one persisted summary chunk.
      final ProjectionIndexFences.BuildWriter fenceWriter = new ProjectionIndexFences.BuildWriter();
      final ProjectionBloomChunks.Writer bloomChunks = new ProjectionBloomChunks.Writer();
      final boolean hasSetColumn = hasStringSetColumn(indexDef);
      final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
          new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
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
            ProjectionIndexColumnSegmentCodec.encode(leaf, encodeWorkspace);
        epoch.storage.putRowGroupAsColumnSegmentSlots(physicalSlot, encoded);
        fenceWriter.append(epoch.storage, leaf.firstRecordKey(), leaf.lastRecordKey());
        persistOrderExceptionLocators(leaf, physicalSlot, epoch.recordLocator);
        bloomChunks.append(encoded, physicalSlot, epoch.storage);
        // Rotate the async-flush epoch at the same log-entry bound the bulk import uses, then rebind
        // every storage-facing accessor: a flush serializes and releases the pages behind them, so a
        // handle cached across the boundary would touch freed frames. The heap-side writers (fences,
        // Bloom window, set summaries, dictionaries) carry build state and take storage per call, so
        // they cross epochs untouched — the same contract ProjectionBulkLoad established for the
        // load-time rider.
        if (intermediateFlushEnabled && storageEngineWriter.isAsyncFlushLogBoundaryReached()) {
          storageEngineWriter.asyncFlush();
          epoch.rebind(storageEngineWriter, indexDef.getID());
        }
      }, false, orderLabelResolver);
      builder.inOrderLane = inOrderLane;
      // A walking build has no dictionary epoch to defer this to, so it binds here — the one point
      // where the builder and a writer are both in hand before the first leaf.
      builder.bindPrebuiltDictionaries(storageEngineWriter);
      try {
        if (rtx instanceof final JsonNodeReadOnlyTrx jsonRtx) {
          builder.build(jsonRtx);
        } else if (rtx instanceof final XmlNodeReadOnlyTrx xmlRtx) {
          builder.build(xmlRtx);
        } else {
          throw new IllegalArgumentException("projection build requires a JSON or XML node transaction");
        }
        if (postWalkHook != null) {
          postWalkHook.accept(epoch.storage);
        }
        final byte[] columnKinds = builder.columnKinds();
        bloomChunks.finishChunks(epoch.storage, fenceWriter.rowGroupCount(), columnKinds);
        // Dictionaries are written after the leaves, and only once: the leaves refer to values by id,
        // so nothing can be persisted about a dictionary until every id it will ever mint is known.
        final long[] valueDictionaryHeaderKeys = builder.valueDictionaryAnchors(storageEngineWriter);
        // A virgin initializer has no prior fence chunks to retire.
        fenceWriter.finish(epoch.storage);
        finishPersistWithStreamingFences(indexDef, epoch.storage, fenceWriter.rowGroupCount(), rtx.getRevisionNumber(),
            columnKinds, setSummaries, valueDictionaryHeaderKeys, bloomChunks);
        builder.publishGlobalDictionaryColumnsBuilt();
        // Materialize the accumulated tree now (idempotent — the outer finally is the exception
        // backstop) and drain the splice burst through the same epoch rotations the in-loop
        // boundary checks use. The burst lands AFTER the last leaf's boundary check, so without
        // this the bounded-retention contract would measure the whole freshly spliced tree as
        // live. Bounded: the pinned-trie spill drains bottom-up (leaves, then interiors whose
        // children became durable), and the loop stops when the boundary clears or an epoch
        // stops reducing the log (the unspillable top-level anchors stay for the final commit).
        epoch.storage.finalizeBulkSlotAccumulation();
        int previousLive = Integer.MAX_VALUE;
        while (intermediateFlushEnabled && storageEngineWriter.isAsyncFlushLogBoundaryReached()) {
          final int live = storageEngineWriter.getLog().liveEntryCount();
          if (live >= previousLive) {
            break;
          }
          previousLive = live;
          storageEngineWriter.asyncFlush();
        }
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
    } catch (final RuntimeException | Error failure) {
      buildFailure = failure;
      // Explicit creation writes directly into its caller-owned transaction. Once any build phase
      // fails, that caller must not be able to catch the exception and commit a partially populated
      // virgin tree whose metadata or derived chunks were never published. Preserve the build
      // failure as the authoritative commit cause even if poisoning itself also fails.
      try {
        storageEngineWriter.markTransactionRollbackOnly(failure);
      } catch (final RuntimeException | Error poisonFailure) {
        addSuppressedSafely(failure, poisonFailure);
      }
      throw failure;
    } finally {
      // The CURRENT epoch's storage owns the (possibly transplanted) accumulator. If both the build
      // and its fail-closed finalizer fail, retain the build failure as authoritative while the
      // finalizer has already made the page transaction rollback-only.
      try {
        epoch.storage.finalizeBulkSlotAccumulation();
      } catch (final RuntimeException | Error finalizationFailure) {
        if (buildFailure == null) {
          throw finalizationFailure;
        }
        addSuppressedSafely(buildFailure, finalizationFailure);
      }
    }
  }

  private static void addSuppressedSafely(final Throwable failure, final Throwable suppressed) {
    if (failure == suppressed) {
      return;
    }
    try {
      failure.addSuppressed(suppressed);
    } catch (final RuntimeException | Error ignored) {
      // Keep propagating the extraction/build failure that initiated cleanup.
    }
  }

  /**
   * Mutable state of the explicit build's in-order order-label lane, shared between the label
   * resolver (which mints through it while armed) and the record walk (which arms it for exactly the
   * members of a top-level record-set array and disarms it after). The carry is held HERE, not on the
   * directory accessor, so an async-flush epoch rebind cannot lose it.
   */
  static final class InOrderLabelLane {
    /** The container whose members are currently arriving in document order; -1 = disarmed. */
    long containerKey = -1;
    /** The previous record's LOCAL label — the append algebra's carry. */
    @Nullable
    SirixDeweyID previousLocal;
    /**
     * The lane serves at most ONE container per build: a second top-level record-set array would
     * receive the same open-bounds container label as the first, so it takes the general path, whose
     * neighbour-aware mint keeps sibling containers strictly ordered.
     */
    boolean used;
  }

  /**
   * The storage-facing bindings of one async-flush epoch of an explicit bulk build. A flush
   * serializes and releases the pages behind these accessors, so each rotation must re-open them over
   * the same persisted sub-tree — the contract {@link ProjectionBulkLoad} established for the
   * load-time rider ("nothing epoch-scoped is ever cached across a commit"). Re-seeding the root
   * label on rebind is idempotent: the seed only writes when no persisted label exists yet.
   */
  private static final class BulkBuildEpoch {
    private ProjectionIndexHOTStorage storage;
    private ProjectionStructuralOrderDirectory.Accessor orderDirectory;
    private ProjectionRecordLocator.Accessor recordLocator;

    private BulkBuildEpoch(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
      bind(storageEngineWriter, indexNumber);
    }

    private void bind(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
      storage = ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexNumber);
      orderDirectory = ProjectionStructuralOrderDirectory.open(storage);
      recordLocator = ProjectionRecordLocator.open(storage);
    }

    private void rebind(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
      final ProjectionIndexHOTStorage previousStorage = storage;
      bind(storageEngineWriter, indexNumber);
      // Transplant an active bulk-slot accumulation onto the fresh storage BEFORE the re-seed:
      // the accumulator is tree-state-independent (it holds only un-materialized slot writes for
      // the same still-virgin tree), and the re-seed's read must see the accumulated root label
      // through the new storage's read-through rather than minting a second one.
      storage.adoptBulkSlotAccumulation(previousStorage);
      orderDirectory.seedRoot(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    }
  }

  /**
   * Persist every resource-wide dictionary the build produced and return their header keys.
   *
   * <p>
   * The in-memory intern tables are released as soon as their contents are on the page: they are the
   * largest transient allocation of a build over a high-cardinality column, and nothing needs them
   * once the records exist.
   *
   * @return per-column header node keys, {@code 0} where the column has no dictionary, or
   *         {@code null} when the build produced none at all
   */
  static long @Nullable [] flushValueDictionaries(final GlobalValueDictionaryWriter @Nullable [] dictionaries,
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
   * Finish a virgin-tree initialization: persist summaries, optional Bloom manifests and per-leaf
   * record-key fences ({@link ProjectionIndexFences}), and only then publish the authoritative live
   * metadata at slot 0. A failure before that final slot-0 write therefore leaves either no metadata or
   * the load-time tombstone in force; it never exposes a partially described set of persistent units.
   */
  static void finishPersist(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final LongArrayList firstKeys, final LongArrayList lastKeys, final int buildRevision, final byte[] columnKinds,
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries,
      final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
    finishPersist(indexDef, storage, firstKeys.size(), firstKeys, lastKeys, buildRevision, columnKinds, setSummaries,
        valueDictionaryHeaderKeys, streamingBloomChunks);
  }

  static void finishPersistWithStreamingFences(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final int rowGroupCount, final int buildRevision, final byte[] columnKinds,
      final ProjectionSetSummaryChunks.BuildAccumulator setSummaries, final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
    finishPersist(indexDef, storage, rowGroupCount, null, null, buildRevision, columnKinds, setSummaries,
        valueDictionaryHeaderKeys, streamingBloomChunks);
  }

  private static void finishPersist(final IndexDef indexDef, final ProjectionIndexHOTStorage storage,
      final int rowGroupCount, final @Nullable LongArrayList firstKeys, final @Nullable LongArrayList lastKeys,
      final int buildRevision, final byte[] columnKinds, final ProjectionSetSummaryChunks.BuildAccumulator setSummaries,
      final long @Nullable [] valueDictionaryHeaderKeys,
      final ProjectionBloomChunks.@Nullable Writer streamingBloomChunks) {
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final String[] paths = new String[fieldPaths.size()];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = fieldPaths.get(i).toString();
    }
    final String rootPath = indexDef.getProjectionRootPath().toString();
    final String[] names = ProjectionIndexChangeListener.trailingFieldNames(indexDef);
    final Map<Integer, Map<String, Long>> persistedSetSummaries = setSummaries.writeAll(storage, columnKinds);
    final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(rootPath, paths, names, columnKinds,
        rowGroupCount, buildRevision, persistedSetSummaries, valueDictionaryHeaderKeys);
    if (firstKeys != null && lastKeys != null) {
      ProjectionIndexFences.write(storage, rowGroupCount, firstKeys.toLongArray(), lastKeys.toLongArray());
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
    installWalkOrderLabelSource(rtx);
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
      publishInjectedColumnKinds();
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
    installWalkOrderLabelSource(rtx);
    try {
      rtx.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (rtx.getKind() != NodeKind.ELEMENT || !rootPathNodeKeys.contains(rtx.getPathNodeKey())) {
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
    fastIterationDocRoot = docRoot;
    try {
      boolean processedAny = descendToRoots(rtx, docRoot);
      rtx.moveTo(docRoot);
      return processedAny;
    } finally {
      fastIterationDocRoot = -1;
    }
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
          // The in-order label lane covers exactly the shape its contract names: the members of a
          // TOP-LEVEL record-set array, arriving in document order, at most one such container per
          // build. Everything else stays on the neighbour-aware general mint.
          final boolean inOrderEligible =
              inOrderLane != null && !inOrderLane.used && parentKey == fastIterationDocRoot && parentKey >= 0;
          if (inOrderEligible) {
            inOrderLane.used = true;
            inOrderLane.containerKey = matchKey;
            inOrderLane.previousLocal = null;
          }
          try {
            if (rtx.moveToFirstChild()) {
              do {
                final long elementKey = rtx.getNodeKey();
                extractRow(rtx, elementKey);
                rtx.moveTo(elementKey);
              } while (rtx.moveToRightSibling());
            }
          } finally {
            if (inOrderEligible) {
              inOrderLane.containerKey = -1;
            }
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
    publishTrieLaneTags();
  }

  /**
   * Bind the trie lane's encode-side resolver, and publish the tags it already knows about.
   *
   * @param dictionaries the resolver, or {@code null} to leave every record page storing bytes
   */
  /**
   * Install the segment lane's dictionaries so tag publication reaches them too. A path class only
   * exists once its first occurrence has been shredded, so both lanes learn their columns as the load
   * discovers them rather than up front.
   */
  public void setSegmentScopedDictionaries(final @Nullable SegmentScopedDictionaries dictionaries) {
    this.segmentScopedDictionaries = dictionaries;
    publishTrieLaneTags();
  }

  public void setTrieLaneWriteDictionaries(final @Nullable TrieLaneWriteDictionaries dictionaries) {
    this.trieLaneWriteDictionaries = dictionaries;
    publishTrieLaneTags();
  }

  /**
   * Hand the resolver the current (path class -> column) mapping.
   *
   * <p>
   * Called after every refresh, because a field only acquires a path class when its first occurrence
   * is shredded — on ClickBench that is usually record one, but a column whose first non-absent value
   * comes late would otherwise never convert. Publishing a WHOLE new map, which is what the resolver
   * requires: the flush lane reads the reference without a lock, so a map rehashed underneath it
   * would be a data race on fastutil internals.
   * </p>
   *
   * <p>
   * A tag missing from the snapshot costs storage on the pages written before it appears, never
   * correctness — those pages simply keep their bytes, and the anchor they do not write is one no
   * reader will look for.
   * </p>
   */
  private void publishTrieLaneTags() {
    final TrieLaneWriteDictionaries dictionaries = trieLaneWriteDictionaries;
    final SegmentScopedDictionaries segments = segmentScopedDictionaries;
    if (dictionaries == null && segments == null) {
      return;
    }
    final long[] pathClasses = extractor.fieldPcrKeysRef();
    final int[] columns = extractor.fieldPcrColumnsRef();
    final Int2IntMap tags = new Int2IntOpenHashMap(pathClasses.length);
    for (int i = 0; i < pathClasses.length && i < columns.length; i++) {
      final long pathClass = pathClasses[i];
      // String-region tags are ints; a path node key outside that range cannot be one, so it cannot
      // name a page this map has to answer for.
      if (pathClass > 0L && pathClass <= Integer.MAX_VALUE) {
        tags.put((int) pathClass, columns[i]);
      }
    }
    if (dictionaries != null) {
      dictionaries.publishTags(tags);
    }
    if (segments != null) {
      segments.publishTags(tags);
    }
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

  boolean appendRecord(final JsonNodeReadOnlyTrx rtx, final long recordKey, final SirixDeweyID orderLabel) {
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

  boolean appendRecord(final XmlNodeReadOnlyTrx rtx, final long recordKey, final SirixDeweyID orderLabel) {
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
   * A chunk batch bound to this build's CURRENT field resolution. Called on the coordinator at
   * chunk-dispatch time, after the chunk's new paths were resolved into the path summary, so the
   * snapshot contains every path class the chunk's records can carry.
   */
  ProjectionChunkRowBatch newChunkBatch(final PathSummaryReader pathSummary, final int expectedRows,
      final long recordSetKey) {
    if (!streaming) {
      throw new IllegalStateException("chunk batches are only available to a streaming builder");
    }
    extractor.refresh(pathSummary);
    publishTrieLaneTags();
    return new ProjectionChunkRowBatch(extractor.fieldPcrKeysRef(), extractor.fieldPcrColumnsRef(),
        extractor.columnKindsRef(), expectedRows, recordSetKey);
  }

  /**
   * Append one worker-extracted batch row as the next record, in the caller's order — the batch
   * counterpart of {@link #appendRecord(JsonNodeReadOnlyTrx, long, SirixDeweyID)}: same leaf
   * preparation, same extractor buffers, same {@link #appendExtractedRecord} packing, with the rtx
   * navigation replaced by {@link ProjectionIndexRowExtractor#loadRowFromBatch}.
   */
  void appendBatchRow(final ProjectionChunkRowBatch batch, final int row, final long recordKey,
      final SirixDeweyID orderLabel) {
    if (!streaming) {
      throw new IllegalStateException("appendBatchRow() is the streaming builder's entry point; this builder walks");
    }
    final byte[] orderLabelBytes = Objects.requireNonNull(orderLabel, "orderLabel must not be null").toBytes();
    prepareLeafForOrderLabel(orderLabelBytes, "JSON");
    extractor.loadRowFromBatch(batch, row);
    appendExtractedRecord(recordKey, orderLabel, orderLabelBytes, "JSON");
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
    publishInjectedColumnKinds();
  }

  /**
   * Flip the SHARED kinds array for injected columns, once every leaf has been written.
   *
   * <p>
   * Deliberately last rather than at injection time: the extractor reads this array to decide how to
   * build a leaf, and the injected columns must be built with their per-leaf dictionaries so the
   * conversion has something to convert. It must also happen before the caller reads
   * {@code columnKinds()} — which hands out a COPY — because that copy reaches both the Bloom chunk
   * finaliser, which must skip these columns or leave bytes nothing will ever read, and the metadata,
   * which must agree with what every leaf's descriptor now says.
   * </p>
   */
  private void publishInjectedColumnKinds() {
    final PrebuiltGlobalDictionary[] injected = prebuiltGlobalDictionaries;
    if (injected == null) {
      return;
    }
    final byte[] kinds = extractor.columnKindsRef();
    for (int column = 0; column < injected.length; column++) {
      if (injected[column] != null) {
        kinds[column] = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
      }
    }
  }

  void publishGlobalDictionaryColumnsBuilt() {
    publishGlobalDictionaryColumnsBuilt(globalDictionaryColumns);
    // Summed at publish time rather than accumulated at flush time so the figure is idempotent and
    // the post-pass path (raw writers, no persistent probes by construction) reports 0 without a
    // parallel bookkeeping field. Runs before releaseTransientState(), so the encoders are live.
    long probes = 0;
    final GlobalValueDictionaryEncoder[] encoders = globalDictionaryEncoders;
    if (encoders != null) {
      for (final GlobalValueDictionaryEncoder encoder : encoders) {
        if (encoder instanceof final StreamingGlobalDictionary dictionary) {
          probes += dictionary.persistentProbeCount();
        }
      }
    }
    PERSISTENT_DICTIONARY_PROBES.set(probes);
  }

  private static void publishGlobalDictionaryColumnsBuilt(final int columns) {
    GLOBAL_DICTIONARY_COLUMNS.set(columns);
    PERSISTENT_DICTIONARY_PROBES.set(0);
  }

  /** @return total rows appended across all emitted leaves. */
  public long rowsEmitted() {
    return rowsEmitted;
  }

  /** Per-column kinds, index-aligned with the projection's declared fields. */
  public byte[] columnKinds() {
    return extractor.columnKinds();
  }

  /**
   * Declare that the named columns resolve against dictionaries that are already complete.
   *
   * <p>
   * This is the fresh-build half of the rank pass. A streaming build must answer "have I seen this
   * value" as it reads, so it holds a probe front and persists a forward radix that measured
   * <b>1,650 B per entry</b>; a build handed a finished rank-ordered dictionary needs neither, and
   * the same column costs <b>61 B per entry</b>. The pre-pass that produced these dictionaries also
   * knows the true distinct count, which is the number the promotion gate currently guesses at by
   * using {@code rows}.
   * </p>
   *
   * <p>
   * Leaves are still BUILT with their per-leaf dictionaries and converted at flush, so a value is
   * resolved once per per-leaf dictionary entry rather than once per row. That is the affordable
   * shape at this scale; above it the ids should come positionally from the pre-pass instead.
   * </p>
   *
   * @param headerKeysByColumn dictionary header key per column, 0 where the column is not injected
   * @param storageEngineWriter the writer the dictionaries are read through
   */
  public void injectPrebuiltGlobalDictionaries(final long[] headerKeysByColumn,
      final StorageEngineWriter storageEngineWriter) {
    Objects.requireNonNull(headerKeysByColumn, "headerKeysByColumn must not be null");
    final byte[] kinds = extractor.columnKindsRef();
    if (headerKeysByColumn.length != kinds.length) {
      throw new IllegalArgumentException("prebuilt dictionary anchors must be index-aligned with the " + kinds.length
          + " columns, not " + headerKeysByColumn.length);
    }
    if (leavesEmitted != 0) {
      throw new IllegalStateException("prebuilt dictionaries must be injected before the first leaf is emitted");
    }
    final PrebuiltGlobalDictionary[] injected = new PrebuiltGlobalDictionary[kinds.length];
    int count = 0;
    for (int column = 0; column < headerKeysByColumn.length; column++) {
      if (headerKeysByColumn[column] == 0L) {
        continue;
      }
      if (kinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalArgumentException("column " + column + " is kind " + kinds[column]
            + "; only a per-leaf string column can be built against a prebuilt dictionary");
      }
      injected[column] = new PrebuiltGlobalDictionary(column, headerKeysByColumn[column], storageEngineWriter);
      count++;
    }
    if (count == 0) {
      return;
    }
    prebuiltGlobalDictionaries = injected;
    globalDictionaryColumns = count;
    // The sample exists to MEASURE whether to promote. That question is answered, so buffering leaves
    // to ask it again would cost memory and decide nothing.
    if (sample != null) {
      sample.clear();
      sample = null;
    }
  }

  /** Materialise anchors named by the property, once a writer exists to validate them through. */
  void bindPrebuiltDictionaries(final StorageEngineWriter storageEngineWriter) {
    if (pendingPrebuiltAnchors == null) {
      return;
    }
    final long[] anchors = pendingPrebuiltAnchors;
    pendingPrebuiltAnchors = null;
    injectPrebuiltGlobalDictionaries(anchors, storageEngineWriter);
  }

  /**
   * The anchors this build's global columns resolve against.
   *
   * <p>
   * An injected build persisted nothing — the pre-pass owns those dictionaries — so there is nothing
   * to flush and the anchors are simply reported. Any other build flushes what it minted.
   * </p>
   */
  long @Nullable [] valueDictionaryAnchors(final StorageEngineWriter storageEngineWriter) {
    if (prebuiltGlobalDictionaries == null) {
      return flushValueDictionaries(globalDictionaries(), storageEngineWriter);
    }
    return prebuiltAnchors();
  }

  /**
   * Header key per injected column, {@code 0} elsewhere.
   *
   * <p>
   * The anchors are all an injected build has to report: it persisted no dictionary of its own, and
   * without them the metadata constructor refuses a global column — a reader could then scan the id
   * lane but never resolve an id through it.
   * </p>
   */
  private long[] prebuiltAnchors() {
    final PrebuiltGlobalDictionary[] injected = prebuiltGlobalDictionaries;
    final long[] anchors = new long[injected.length];
    for (int column = 0; column < injected.length; column++) {
      if (injected[column] != null) {
        anchors[column] = injected[column].headerKey();
      }
    }
    return anchors;
  }

  /**
   * Parses {@code -Dsirix.projection.globalDict.prebuilt}; {@code null} when unset or empty.
   *
   * <p>
   * Package-private rather than private because the trie lane's encode-side resolver binds the SAME
   * anchors — the record pages and the projection leaves must name one dictionary per column, or a
   * page's ids and the index's ids would come from different rankings. One parser, one set of error
   * messages, one source of truth.
   * </p>
   */
  static long @Nullable [] configuredPrebuiltAnchors(final int columnCount) {
    final String configured = System.getProperty("sirix.projection.globalDict.prebuilt");
    if (configured == null || configured.isBlank()) {
      return null;
    }
    final long[] anchors = new long[columnCount];
    for (final String pair : configured.split(",")) {
      final String trimmed = pair.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      final int colon = trimmed.indexOf(':');
      if (colon <= 0 || colon == trimmed.length() - 1) {
        throw new IllegalArgumentException("sirix.projection.globalDict.prebuilt wants column:headerKey pairs, got '"
            + trimmed + "'");
      }
      final int column = Integer.parseInt(trimmed.substring(0, colon).trim());
      if (column < 0 || column >= columnCount) {
        throw new IllegalArgumentException(
            "sirix.projection.globalDict.prebuilt names column " + column + " of " + columnCount);
      }
      anchors[column] = Long.parseLong(trimmed.substring(colon + 1).trim());
    }
    return anchors;
  }

  /** Convert the injected columns of one finished leaf before it is handed on. */
  private void convertInjectedColumns(final ProjectionIndexRowGroupPage leaf) {
    final PrebuiltGlobalDictionary[] injected = prebuiltGlobalDictionaries;
    for (int column = 0; column < injected.length; column++) {
      if (injected[column] != null) {
        leaf.convertStringDictColumnToGlobal(column, injected[column]);
      }
    }
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
    return nextSequentialOrderLabel(lastOrderLabel);
  }

  /**
   * The next label of a build's own strictly increasing sequence.
   *
   * <p>
   * {@link SirixDeweyID#newBetween(SirixDeweyID, SirixDeweyID)}'s open-ended branch advances the
   * final division by an unchecked {@code int} addition, so a long enough append run wraps it
   * negative and the sequence stops increasing — at which point the monotonicity check in
   * {@link #appendExtractedRecord} aborts an otherwise valid build. A run that long is reachable: the
   * sequence starts at division 17 and steps by the sibling distance, so a single-pass build of a
   * corpus in the hundreds of millions of records crosses it.
   *
   * <p>
   * Detect the wrap from the returned label rather than from the step size — the step is a mutable
   * global — and carry into an additional division instead. The carried label is strictly greater
   * because the previous label's divisions are its prefix, which is also how the persisted lane
   * compares them, and the fresh final division restores the sequence's full range.
   */
  static SirixDeweyID nextSequentialOrderLabel(final @Nullable SirixDeweyID previous) {
    if (previous == null) {
      return SirixDeweyID.newRootID().getNewChildID();
    }
    return ProjectionStructuralOrderDirectory.Accessor.nextAppendLabel(previous);
  }

  private SirixDeweyID resolveOrderLabel(final long recordKey) {
    final LongFunction<SirixDeweyID> resolver = orderLabelResolver != null
        ? orderLabelResolver
        : walkOrderLabelResolver;
    return resolver == null
        ? nextOrderLabel()
        : resolver.apply(recordKey);
  }

  /**
   * Arm the walking build's label source. A caller-supplied resolver wins — {@link #buildAndPersist}
   * installs one over the directory it is about to persist, and mounting a second, heap-backed
   * directory beside it would mint the same nodes twice.
   */
  private void installWalkOrderLabelSource(final NodeReadOnlyTrx rtx) {
    if (orderLabelResolver != null) {
      return;
    }
    final ProjectionStructuralOrderDirectory.Accessor directory = ProjectionStructuralOrderDirectory.inMemory();
    directory.seedRoot(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    final StorageEngineReader reader = rtx.getStorageEngineReader();
    final LongFunction<ImmutableNode> nodeLookup = nodeKey -> reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    walkOrderLabelResolver =
        recordKey -> directory.fullLabel(recordKey, nodeLookup, ProjectionStructuralOrderDirectory.RelabelSink.SEALED);
  }

  private void prepareLeafForOrderLabel(final byte[] orderLabel, final String databaseType) {
    if (!currentLeaf.canAppendOrderLabel(orderLabel)) {
      flushCurrentRowGroup();
      currentLeaf = newLeaf();
      if (!currentLeaf.canAppendOrderLabel(orderLabel)) {
        throw new IllegalStateException(
            "an empty projection row group rejected one " + databaseType + " record order label");
      }
    }
  }

  private void appendExtractedRecord(final long recordKey, final SirixDeweyID orderLabel, final byte[] orderLabelBytes,
      final String databaseType) {
    if (lastOrderLabel != null && lastOrderLabel.compareTo(orderLabel) >= 0) {
      throw new IllegalStateException(
          "projection " + databaseType + " record order labels are not strictly increasing at record " + recordKey);
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

  /**
   * Persist this emitted unit's sparse exact locators without retaining a build-wide exception map.
   */
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
    // Deferred to here because a prebuilt dictionary must be READ to be validated, and the builder
    // has no writer until its first epoch opens.
    bindPrebuiltDictionaries(storageEngineWriter);
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

  long @Nullable [] flushStreamingDictionaryGeneration(final StorageEngineWriter storageEngineWriter) {
    if (!streaming) {
      throw new IllegalStateException("dictionary generations are only available to a streaming builder");
    }
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    if (prebuiltGlobalDictionaries != null) {
      // Nothing to flush: the dictionaries were persisted by the pre-pass and this build only read
      // them, so every epoch reports the same anchors rather than minting a generation.
      return prebuiltAnchors();
    }
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
        final StreamingGlobalDictionary streamingDictionary = new StreamingGlobalDictionary(column, initialGeneration);
        encoders[column] = streamingDictionary;
        if (globalDictionaries != null) {
          globalDictionaries[column] = null;
        }
        encoder = streamingDictionary;
      }
      if (!(encoder instanceof final StreamingGlobalDictionary dictionary)) {
        throw new IllegalStateException("streaming global dictionary column " + column + " has an unsupported encoder "
            + encoder.getClass().getName());
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
    /** One disjoint half of the planner's combined per-column streaming envelope. */
    private final long budgetBytes;
    private final GlobalValueDictionaryWriter.AdmissionPolicy admissionPolicy;
    /**
     * Resident value→id map covering EVERY generation of this load, not just the current epoch's
     * additions. Replaces the 64-slot hot cache AND the per-value persistent-radix probe that used to
     * serve values from released generations — the regime measured at ~85% of load CPU. See
     * {@link GlobalValueDictionaryProbeFront}.
     */
    private final GlobalValueDictionaryProbeFront residentFront;
    /**
     * Front-completeness invariant: {@code true} while every value in every durable generation
     * reachable from {@link #headerKey} is present in {@link #residentFront}. Holds by construction
     * today — (i) with {@code headerKey == 0} there are no durable generations; (ii) the constructor
     * seeds the election writer's entries; (iii) every mint and every probe-resolved id is put into the
     * front before it is returned; (iv) {@code flush()} moves additions to durable without changing the
     * value set; and {@code headerKey} only ever becomes non-zero through our OWN flush. While it
     * holds, a front miss PROVES a value is new, so minting without consulting the persistent radix
     * cannot double-assign an id. Any future path that adopts a durable header this front did not
     * witness (resume-into-existing-dictionary) must clear this flag, which re-arms the guarded probe
     * below.
     */
    private boolean frontCoversDurableGenerations = true;
    private long headerKey;
    private int baseEntryCount;
    private @Nullable ValueDictionaryHeaderNode baseHeader;
    private @Nullable StorageEngineWriter storageEngineWriter;
    private @Nullable GlobalValueDictionaryWriter additions;
    private long persistentProbeCount;

    StreamingGlobalDictionary(final int column, final GlobalValueDictionaryWriter initialGeneration) {
      this.column = column;
      this.budgetBytes = initialGeneration.budgetBytes();
      this.admissionPolicy = initialGeneration.admissionPolicy();
      this.additions = initialGeneration;
      this.residentFront = new GlobalValueDictionaryProbeFront(column, this.budgetBytes);
      // The initial generation already holds entries (election seeded the sample's distincts before
      // this wrapper existed). The front must know them: the wrap happens AT the first flush, so a
      // seeded value's first post-wrap occurrence would otherwise miss the front and walk the
      // persistent radix — one probe per seeded distinct, exactly the cost class this front removes.
      // Ids in the initial generation are the global ids (base 0). Copy directly between the two
      // chunked arenas: valueBytes(id) would allocate up to 16,384 transient byte arrays here.
      final int seededEntries = initialGeneration.entryCount();
      try {
        for (int id = 1; id <= seededEntries; id++) {
          initialGeneration.copyValueToProbeFront(id, residentFront);
        }
      } catch (final RuntimeException | Error failure) {
        residentFront.release();
        throw failure;
      }
    }

    void bind(final StorageEngineWriter writer) {
      Objects.requireNonNull(writer, "writer must not be null");
      if (storageEngineWriter != null && storageEngineWriter != writer) {
        throw new IllegalStateException(
            "global dictionary column " + column + " is already bound to another storage epoch");
      }
      storageEngineWriter = writer;
      if (headerKey == 0) {
        return;
      }
      final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, writer);
      if (header == null || !header.isDirectoryComplete()) {
        throw new IllegalStateException(
            "global dictionary column " + column + " cannot read its durable generation header " + headerKey);
      }
      baseHeader = header;
      baseEntryCount = header.getEntryCount();
    }

    @Override
    public int intern(final String value) {
      Objects.requireNonNull(value, "value must not be null");
      final int encodedLength =
          GlobalValueDictionaryEncoder.utf8LengthCapped(value, GlobalValueDictionaryWriter.MAX_VALUE_BYTES);
      if (encodedLength > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw refuseOversizedValue(encodedLength);
      }
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      return intern(utf8, 0, utf8.length);
    }

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, source.length);
      final StorageEngineWriter writer = storageEngineWriter;
      if (writer == null) {
        throw new IllegalStateException(
            "global dictionary column " + column + " is not bound to a streaming storage epoch");
      }
      if (length > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw refuseOversizedValue(length);
      }
      // The resident front answers for every value this load has seen — whichever generation it
      // went into — in one open-addressing probe. The hashes are computed once and shared with the
      // put below.
      final long hash = GlobalValueDictionary.valueHash(source, offset, length);
      final long secondaryHash = GlobalValueDictionary.secondaryValueHash(source, offset, length);
      final int knownId = residentFront.findId(hash, secondaryHash, source, offset, length);
      if (knownId > 0) {
        return knownId;
      }
      if (headerKey != 0 && !frontCoversDurableGenerations) {
        // Reachable only for a value in a durable generation this front never saw minted — i.e. a
        // generation that predates the front. A fresh bulk load can never get here (see the
        // front-completeness invariant on the flag), which is what dictProbes=0 in the load banner
        // witnesses; a non-zero count on a fresh load means the ~5-page-decode-per-value probe
        // regime is back.
        persistentProbeCount++;
        final int existing = GlobalValueDictionary.probe(headerKey, source, offset, length, writer);
        if (existing > 0) {
          residentFront.put(hash, secondaryHash, source, offset, length, existing);
          return existing;
        }
        if (existing == GlobalValueDictionary.ID_UNKNOWN) {
          throw new IllegalStateException(
              "global dictionary column " + column + " cannot probe generation header " + headerKey);
        }
      }
      GlobalValueDictionaryWriter generation = additions;
      if (generation == null) {
        generation = new GlobalValueDictionaryWriter(column, budgetBytes, admissionPolicy);
        additions = generation;
      }
      final int localId = generation.intern(source, offset, length);
      final long globalId = (long) baseEntryCount + localId;
      if (globalId > Integer.MAX_VALUE) {
        throw new IllegalStateException("global dictionary column " + column + " exhausted dictionary ids");
      }
      residentFront.put(hash, secondaryHash, source, offset, length, (int) globalId);
      return (int) globalId;
    }

    /**
     * A value the V0 layout cannot hold is an ADMISSION decision, not an encoding fault: this
     * dictionary is optional, so under {@link GlobalValueDictionaryWriter.AdmissionPolicy#DECLINE} the
     * build must receive the typed decline, abandon the projection and let the LOAD COMPLETE. Throwing
     * an untyped failure here instead killed a legal ingest over an optional index — the one outcome
     * {@link GlobalDictionaryBudgetExceededException} exists to prevent. A forced dictionary still
     * fails closed, because there is no per-leaf fallback to retreat to.
     */
    private RuntimeException refuseOversizedValue(final int length) {
      final GlobalValueDictionaryWriter generation = additions;
      final long retainedBytes = generation == null
          ? 0L
          : generation.retainedBytes();
      final int admitted = generation == null
          ? baseEntryCount
          : Math.addExact(baseEntryCount, generation.entryCount());
      return GlobalValueDictionaryWriter.oversizedValueRefusal(column, length, retainedBytes, budgetBytes, admitted,
          admissionPolicy);
    }

    long persistentProbeCount() {
      return persistentProbeCount;
    }

    long generationBudgetBytesForTest() {
      return budgetBytes;
    }

    long residentFrontBudgetBytesForTest() {
      return residentFront.budgetBytesForTest();
    }

    long flush() {
      final StorageEngineWriter writer = storageEngineWriter;
      if (writer == null) {
        throw new IllegalStateException(
            "global dictionary column " + column + " is not bound to a streaming storage epoch");
      }
      final GlobalValueDictionaryWriter generation = additions;
      try {
        if (generation == null || generation.entryCount() == 0) {
          if (headerKey == 0) {
            throw new IllegalStateException("global dictionary column " + column + " has no values or durable header");
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
            throw new IllegalStateException("global dictionary column " + column + " has no base header for append");
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
      residentFront.release();
      baseHeader = null;
      storageEngineWriter = null;
    }
  }

  /**
   * Release build-only state after a streaming load finishes or aborts.
   *
   * <p>
   * A {@link ProjectionBulkLoad} may remain reachable from a caller even after it has been removed
   * from the ACTIVE registry. Clearing the leading sample, reusable pages and dictionary intern
   * tables here prevents that harmless handle from retaining the build's largest transient
   * allocations. Dictionary release is idempotent, including after a successful flush.
   * </p>
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
    if (prebuiltGlobalDictionaries != null) {
      convertInjectedColumns(currentLeaf);
      leafSink.accept(currentLeaf);
      // NOT reused: the conversion tore down this page's per-leaf dictionary state and flipped its
      // kinds to GLOBAL, and resetForBuilderReuse restores neither — a reused page would try to
      // intern the next leaf's rows against an encoder it was never given. A fresh page per row
      // group costs one allocation per 1,024 rows, which is not worth a resurrection path.
      reusableLeaf = null;
      leavesEmitted++;
      return;
    }
    reusableLeaf = emitBorrowedLeafForReuse(currentLeaf, globalDictionaryEncoders, leafSink);
    leavesEmitted++;
  }

  /**
   * Borrow one live page synchronously, then reset it only after the callback has returned.
   *
   * <p>
   * The ordering is the ownership proof: if the callback throws, execution never reaches the reset
   * and the failing borrower sees an unchanged page. Package visibility supports focused failure-path
   * coverage without exposing reuse through the public API.
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
   * to find out would double the build. So the build runs normally, the leading leaves are held back,
   * and the per-leaf dictionaries they already contain ARE the measurement — converting them
   * afterwards costs one intern per dictionary entry, not per row.
   *
   * <h2>The measurement</h2>
   *
   * Not the distinct ratio but the per-leaf DEDUPLICATION FACTOR: sampled rows divided by the total
   * size of the per-leaf dictionaries. That is the quantity the choice actually turns on. A per-leaf
   * dictionary earns its keep by storing a recurring value once per leaf; when a leaf's dictionary is
   * nearly as large as its row count the dictionary stores almost nothing twice, so it has become a
   * second copy of the column plus an id per row. A global distinct ratio would not say this — a
   * column can have a million distinct values resource-wide and still repeat heavily inside a leaf.
   */
  private void decideDictionaryKindsAndDrainSample() {
    final byte[] kinds = extractor.columnKindsRef();
    final GlobalValueDictionaryWriter[] dictionaries = new GlobalValueDictionaryWriter[kinds.length];
    final GlobalDictionaryMode mode = globalDictionaryMode;
    final long totalBudgetBytes = globalDictionaryBudgetBytes();
    int possibleGlobalColumns = 0;
    for (final byte kind : kinds) {
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT)
        possibleGlobalColumns++;
    }
    final long forcedColumnBudget = possibleGlobalColumns == 0 || totalBudgetBytes == Long.MAX_VALUE
        ? totalBudgetBytes
        : totalBudgetBytes / possibleGlobalColumns;
    final long minimumColumnBudget = conservativeAutoDictionaryBudget(0L, streaming);
    if (mode == GlobalDictionaryMode.ALWAYS && possibleGlobalColumns > 0
        && forcedColumnBudget < minimumColumnBudget) {
      throw new IllegalStateException("forced " + (streaming ? "streaming " : "")
          + "global dictionaries require at least " + minimumColumnBudget + " B per string column, got "
          + forcedColumnBudget + " B");
    }

    long sampledRows = 0L;
    for (final ProjectionIndexRowGroupPage leaf : sample) {
      sampledRows += leaf.getRowCount();
    }

    long[] autoColumnBudgets = null;
    long[] autoProjectedBytes = null;
    long[] autoPerLeafDictionaryEntries = null;
    if (mode == GlobalDictionaryMode.AUTO) {
      autoProjectedBytes = new long[kinds.length];
      Arrays.fill(autoProjectedBytes, -1L);
      final long[] benefitScores = new long[kinds.length];
      autoPerLeafDictionaryEntries = new long[kinds.length];
      for (int c = 0; c < kinds.length; c++) {
        if (kinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          continue;
        }
        long perLeafDictionaryEntries = 0L;
        for (final ProjectionIndexRowGroupPage leaf : sample) {
          perLeafDictionaryEntries += leaf.stringDictionarySize(c);
        }
        autoPerLeafDictionaryEntries[c] = perLeafDictionaryEntries;
        final int sampledLargestValueBytes = sampledMaximumValueBytes(c);
        if (sampledLargestValueBytes > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
          if (PROJ_DIAG) {
            System.err.println("[proj] global dictionary DECLINED for column " + c + ": sampled UTF-8 value length "
                + sampledLargestValueBytes + " exceeds the safe V0 limit of "
                + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes — column stays per-leaf DICT");
          }
          continue;
        }
        if (!isGlobalDictionaryWorthwhile(sampledRows, perLeafDictionaryEntries)) {
          continue;
        }

        // The number of local dictionary entries is the bounded sample's direct measure of how
        // much per-leaf string work a global id space removes. All columns see the same sampled row
        // count, so it is also a division-free, deterministic benefit ordering.
        benefitScores[c] = perLeafDictionaryEntries;
        if (expectedRows > 0L) {
          final long averageValueBytes = perLeafDictionaryEntries == 0L
              ? 0L
              : sampledValueBytes(c) / perLeafDictionaryEntries;
          // distinct==rows is deliberately the upper bound, while the leading-sample average is
          // still uncertain. The aggregate planner therefore reserves twice this projection for
          // every simultaneously resident structure before admitting the column; it does not weaken
          // the old half-budget safety margin or let the streaming writer/front pair double-spend it.
          autoProjectedBytes[c] = projectedGlobalDictionaryBytes(expectedRows, averageValueBytes);
        } else {
          // With no cardinality hint, reserve the real empty-writer minimum and let the same runtime
          // cap as before fail closed if distinct values grow beyond the assigned aggregate share.
          autoProjectedBytes[c] = 0L;
        }
      }
      autoColumnBudgets =
          planAutoGlobalDictionaryBudgets(totalBudgetBytes, autoProjectedBytes, benefitScores, streaming);
    }

    if (mode != GlobalDictionaryMode.NEVER) {
      for (int c = 0; c < kinds.length; c++) {
        if (kinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          continue;
        }
        final long perLeafDictTotal;
        final long dictionaryBudget;
        if (mode == GlobalDictionaryMode.ALWAYS) {
          long measuredEntries = 0L;
          for (final ProjectionIndexRowGroupPage leaf : sample) {
            measuredEntries += leaf.stringDictionarySize(c);
          }
          perLeafDictTotal = measuredEntries;
          final int sampledLargestValueBytes = sampledMaximumValueBytes(c);
          if (sampledLargestValueBytes > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
            final String detail = "sampled UTF-8 value length " + sampledLargestValueBytes
                + " exceeds the safe V0 limit of " + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes";
            throw new IllegalStateException(
                "forced global dictionary for column " + c + " cannot be built safely: " + detail);
          }
          dictionaryBudget = forcedColumnBudget;
        } else {
          if (autoProjectedBytes[c] < 0L) {
            continue;
          }
          perLeafDictTotal = autoPerLeafDictionaryEntries[c];
          dictionaryBudget = autoColumnBudgets[c];
          if (dictionaryBudget == 0L) {
            if (PROJ_DIAG) {
              final long required = conservativeAutoDictionaryBudget(autoProjectedBytes[c], streaming);
              final String estimate = expectedRows > 0L
                  ? "projected " + autoProjectedBytes[c] + " B, requiring a " + required
                      + " B combined envelope with 2x headroom per resident structure"
                  : "requiring at least the combined empty-state floor of " + required + " B";
              System.err.println("[proj] global dictionary DECLINED for column " + c + ": " + estimate
                  + " was not selected within the " + totalBudgetBytes
                  + " B aggregate budget — column stays per-leaf DICT");
            }
            continue;
          }
        }
        final long writerBudget = streaming
            ? streamingGlobalDictionaryComponentBudget(dictionaryBudget)
            : dictionaryBudget;
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter(c, writerBudget,
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
          if (mode == GlobalDictionaryMode.AUTO && !globalDictionarySampleHasHeadroom(dictionary.entryCount())) {
            if (PROJ_DIAG) {
              System.err.println(
                  "[proj] global dictionary DECLINED for column " + c + ": exact sample seeding " + "used "
                      + dictionary.entryCount() + " of " + GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND
                      + " safe entries, leaving fewer " + "than one full row group's " + MIN_GLOBAL_DICTIONARY_HEADROOM
                      + "-entry headroom — column stays per-leaf DICT");
            }
            dictionary.release();
            continue;
          }
          dictionaries[c] = dictionary;
        } catch (final GlobalDictionaryBudgetExceededException declined) {
          dictionary.release();
          if (PROJ_DIAG) {
            System.err.println("[proj] global dictionary DECLINED for column " + c + ": " + declined.getMessage()
                + " — column stays per-leaf DICT");
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
  static void seedGlobalDictionaryFromSample(final List<ProjectionIndexRowGroupPage> sample, final int column,
      final GlobalValueDictionaryWriter dictionary) {
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
    return entryCount <= GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND - MIN_GLOBAL_DICTIONARY_HEADROOM;
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
   * needed. All leading leaves survive measurement, optional local-to-global conversion and their
   * borrowed callbacks intact; only the final leaf is reset, and only after its callback returns
   * successfully, to seed steady-state reuse.
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

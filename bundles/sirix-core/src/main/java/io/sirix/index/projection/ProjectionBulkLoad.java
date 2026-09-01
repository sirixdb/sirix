/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongFunction;

/**
 * One-pass (load-time) construction of a projection index: the index is declared on an EMPTY
 * resource and then maintained by the shred itself, instead of being derived afterwards by a second
 * full walk of the finished resource.
 *
 * <h2>Why this is not just the incremental maintenance path</h2>
 *
 * {@link ProjectionIndexChangeListener}'s per-leaf patcher maintains an EXISTING snapshot: it
 * re-extracts touched leaves, appends to the tail leaf, and updates derived structures in bounded
 * persistent units. Running it over a bulk load would still repeat touched-unit work at every
 * auto-commit window. This class instead keeps the REAL build machinery
 * ({@link ProjectionIndexBuilder} in its record-fed mode) alive for the whole load, so the leaves,
 * the global-dictionary decision, the fingerprint blocks, the fences and the metadata are produced
 * by exactly the code the post-pass build uses.
 *
 * <h2>Surviving the auto-commit</h2>
 *
 * A bulk load commits every {@code -Dsirix.autoCommit.nodes} nodes, and every one of those commits
 * destroys and rebinds the index listeners and installs a NEW storage-engine writer and path
 * summary ({@code AbstractNodeTrxImpl#reInstantiate}). Build state therefore cannot live in the
 * listener. It lives here, keyed by resource and definition, and the listener hands its CURRENT
 * writer and summary in on each use — nothing epoch-scoped is ever cached across a commit.
 *
 * <h2>What is written when</h2>
 *
 * Full leaves stream into the definition's HOT sub-tree as they fill and ride the auto-commit that
 * follows. Retained derived state is explicit and bounded: one 32-leaf fence tail, one 256-leaf
 * Bloom-reference window per local-string column, and only those set-summary values that still fit
 * their one optional summary chunk. Complete fence and Bloom windows stream to storage eagerly; the
 * partial tails, Bloom manifests, set summaries and live metadata publish at {@link #finish}. Slot
 * 0 holds the {@link ProjectionIndexMetadata#staleTombstone() stale tombstone} for the whole load,
 * so a load that dies half-way leaves a projection every reader SKIPS in favour of the generic
 * pipeline. Writing a truthful-looking {@code rowGroupCount=0} metadata instead would make every
 * query answer from an empty index — zero rows, silently, which is the one outcome worse than being
 * slow.
 */
public final class ProjectionBulkLoad {

  /** Publication seam kept synchronous so a storage failure poisons the owning load immediately. */
  @FunctionalInterface
  interface RowGroupPublisher {
    void publish(ProjectionIndexHOTStorage storage, long rowGroupId,
        ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded);
  }

  private static final RowGroupPublisher DEFAULT_ROW_GROUP_PUBLISHER =
      ProjectionIndexHOTStorage::putRowGroupAsColumnSegmentSlots;

  /**
   * Active bulk loads by {@code resourceKey#defId}. Process-global by the same reasoning as
   * {@link ProjectionIndexRegistry}'s registry: the state has to outlive the objects that reach it
   * (listeners are rebuilt per commit epoch, index controllers per revision), and the resource path
   * is the only durable identity all of them agree on. Each value additionally carries the owning
   * write-transaction object identity, so a different transaction using the same path cannot attach
   * to the load.
   */
  private static final ConcurrentMap<String, ProjectionBulkLoad> ACTIVE = new ConcurrentHashMap<>();

  private final IndexDef indexDef;
  private final String key;
  /**
   * Identity of the one write transaction whose successful intermediate epochs may feed this load.
   */
  private final Object ownerToken;

  /** Owner-confined FSST scratch reused by every row group in this bulk-load stream. */
  private final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
      new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();

  /** The record-fed builder; owns the current leaf, the dictionary sample and the dictionaries. */
  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionBulkLoad.class);

  /**
   * OFF by default, opt in with {@code -Dsirix.projection.trieLane=true}.
   *
   * <p>
   * <b>Default flipped 2026-09-01 because the lane writes DOCUMENT pages that cannot be read back.</b>
   * The 1M gate's converted arm fails a subtree serialization with
   * {@code AssertionError: Type not known} out of {@code deserializeNumber} — a fused NUMBER record's
   * payload type byte is wrong — while the four arms without the lane serialize byte-identically. It
   * reproduces with derived elision off too, so it is the lane's own path, and it appears a few pages
   * in rather than on the first page.
   * </p>
   *
   * <p>
   * On by default was a footgun in its own right: the lane engages for any load that binds prebuilt
   * dictionaries, which is the configuration the pre-pass route exists to create. Nothing opts INTO
   * corruption by accident now.
   * </p>
   *
   * <p>
   * The switch gates BEHAVIOUR only. Every decoder still reads a converted page, so a database
   * written by an earlier run stays readable to the extent it ever was.
   * </p>
   */
  private static final boolean TRIE_LANE_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.trieLane", "false"));

  /** The trie lane's encode-side resolver for this load, or {@code null} when the lane is not bound. */
  private volatile @Nullable TrieLaneWriteDictionaries trieLaneWriteDictionaries;

  /**
   * The writer the lane was installed on, so ABORT can uninstall it too.
   *
   * <p>
   * {@code abort()} takes no writer argument, and the listener calls it DURING the load on a
   * dictionary-budget breach — the load then keeps running and keeps flushing record pages. Without
   * this reference the writer would keep handing the released resolver to every page it creates, so
   * the in-flight gate would be guarding an open-ended stream of calls instead of a closing window.
   * Held only to uninstall; nothing reads it for anything else.
   * </p>
   */
  private volatile @Nullable StorageEngineWriter trieLaneWriter;

  private final ProjectionIndexBuilder builder;

  /** Bounded fence-chunk stream; only its current 32-leaf tail remains on heap. */
  private final ProjectionIndexFences.BuildWriter fenceWriter = new ProjectionIndexFences.BuildWriter();

  /** Bounded 256-row-group fingerprint accumulator; full chunks are persisted eagerly. */
  private final ProjectionBloomChunks.Writer bloomChunks = new ProjectionBloomChunks.Writer();

  /** Bounded index-wide per-value row counts for {@code COLUMN_KIND_STRING_SET} columns. */
  private final ProjectionSetSummaryChunks.BuildAccumulator setSummaries =
      new ProjectionSetSummaryChunks.BuildAccumulator();

  private final boolean hasSetColumn;

  /**
   * Whether the declared root path selects ARRAY ELEMENTS. Then a record root is by construction an
   * array element, and a fused {@code OBJECT_NAMED_*} node — which is an object FIELD and nothing
   * else — can never be one. That is what lets the listener skip the ancestor read for every field
   * notification instead of paying one per node.
   */
  private final boolean arrayElementRoot;

  /**
   * The record currently being shredded, or {@code -1} before the first one. Held back from every
   * batch: an auto-commit can fire in the MIDDLE of a record's subtree, and extracting it there would
   * store a half-built row.
   */
  private long currentRecordKey = -1L;

  /**
   * Records proven complete (a later record has started) and not yet extracted. Bounded by the
   * async-flush storage epoch — roughly 150 records for ClickBench and 525 for Bluesky at the
   * 16,384-node bound — and drained at every commit or storage-only rotation.
   */
  private final LongArrayList completedRecordKeys = new LongArrayList();

  /** Highest record key ever closed — the append-only contract's witness. */
  private long lastClosedRecordKey = Long.MIN_VALUE;

  /** The append-only record-set array currently receiving children. */
  private long activeArrayRootKey = -1L;
  private long activeArrayRootCountedChildren;
  private long expectedArrayRecords;
  private long arrayRootInstanceCount;

  /** Storage of the CURRENT epoch; set on entry to every method that writes. */
  private @Nullable ProjectionIndexHOTStorage storage;

  /**
   * COORDINATOR-FED mode: the parallel importer extracts rows in its build workers and appends them
   * here directly, in document order, so the drain never re-reads a record through the transaction.
   * Engaged by the first {@link #appendCoordinatorRow}; from then on the notification entry points
   * refuse, because the same records must not arrive through two mechanisms.
   */
  private boolean coordinatorFeed;

  /** Epoch-lived order-label lane of the coordinator feed; bound with {@link #storage}. */
  private ProjectionStructuralOrderDirectory.@Nullable Accessor feedDirectory;

  /** The previous record's LOCAL order label — the in-order append lane's carry state. */
  private @Nullable SirixDeweyID feedLastRecordLocal;

  private boolean finished;

  /**
   * {@code -Dsirix.projection.bulkDiag=true} prints how every change notification was classified when
   * the load finishes. A load-time build attributes records from notifications alone, so when it
   * comes out short the only question that matters is WHICH notifications it discarded — and without
   * these counters that question needs a debugger.
   */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projection.bulkDiag");

  private long diagNotifications;
  private long diagSkippedNamedKind;
  private long diagSkippedPathClass;
  private long diagNotUnderRecordSet;
  private long diagUnresolved;
  private long diagObserved;
  private long diagClosed;
  private long diagDrains;
  private long diagExtractFailures;

  /** Whether the per-notification classification counters are being kept. */
  public static boolean diagnosticsEnabled() {
    return DIAG;
  }

  /** Count one classification outcome; {@code outcome} is one of the {@code DIAG_*} constants. */
  public void countClassification(final int outcome) {
    diagNotifications++;
    switch (outcome) {
      case DIAG_SKIPPED_NAMED_KIND -> diagSkippedNamedKind++;
      case DIAG_SKIPPED_PATH_CLASS -> diagSkippedPathClass++;
      case DIAG_NOT_UNDER_RECORD_SET -> diagNotUnderRecordSet++;
      case DIAG_UNRESOLVED -> diagUnresolved++;
      default -> diagObserved++;
    }
  }

  public static final int DIAG_SKIPPED_NAMED_KIND = 0;
  public static final int DIAG_SKIPPED_PATH_CLASS = 1;
  public static final int DIAG_NOT_UNDER_RECORD_SET = 2;
  public static final int DIAG_UNRESOLVED = 3;
  public static final int DIAG_OBSERVED = 4;

  private String diagnostics() {
    if (!DIAG) {
      return " (re-run with -Dsirix.projection.bulkDiag=true for the per-notification classification)";
    }
    return " [notifications=" + diagNotifications + " skippedNamedKind=" + diagSkippedNamedKind + " skippedPathClass="
        + diagSkippedPathClass + " notUnderRecordSet=" + diagNotUnderRecordSet + " unresolved=" + diagUnresolved
        + " observed=" + diagObserved + " closed=" + diagClosed + " drains=" + diagDrains + " extractFailures="
        + diagExtractFailures + " arrayRoots=" + arrayRootInstanceCount + ']';
  }

  private ProjectionBulkLoad(final IndexDef indexDef, final String key, final Object ownerToken,
      final PathSummaryReader pathSummary, final RowGroupPublisher rowGroupPublisher) {
    this.indexDef = indexDef;
    this.key = key;
    this.ownerToken = Objects.requireNonNull(ownerToken, "ownerToken must not be null");
    final RowGroupPublisher checkedPublisher =
        Objects.requireNonNull(rowGroupPublisher, "rowGroupPublisher must not be null");
    this.hasSetColumn = ProjectionIndexBuilder.hasStringSetColumn(indexDef);
    this.arrayElementRoot = ProjectionIndexBuilder.isArrayLayerPath(indexDef.getProjectionRootPath());
    this.builder = ProjectionIndexBuilder.streamingBorrowed(indexDef, pathSummary, leaf -> {
      if (leaf.getRowCount() == 0) {
        throw new IllegalStateException("Projection leaf " + fenceWriter.rowGroupCount() + " is empty");
      }
      final int physicalSlot = fenceWriter.rowGroupCount() + 1;
      if (hasSetColumn) {
        setSummaries.append(leaf);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(leaf, encodeWorkspace);
      final ProjectionIndexHOTStorage currentStorage = currentStorage();
      checkedPublisher.publish(currentStorage, physicalSlot, encoded);
      fenceWriter.append(currentStorage, leaf.firstRecordKey(), leaf.lastRecordKey());
      ProjectionIndexBuilder.persistOrderExceptionLocators(leaf, physicalSlot,
          ProjectionRecordLocator.open(currentStorage));
      bloomChunks.append(encoded, physicalSlot, currentStorage);
    });
  }

  /** Registry key for a definition on a resource. */
  private static String keyOf(final String resourceKey, final int indexDefId) {
    return resourceKey + '#' + indexDefId;
  }

  /**
   * Arm a bulk load for {@code indexDef} on an empty resource and tombstone slot 0 so nothing reads
   * the half-built index. Fails loudly if one is already armed for this definition — two concurrent
   * bulk loads over one projection would interleave their leaf slots.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter, -1L,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /**
   * As above, telling the build how many records to expect so the global-dictionary election can
   * decline a column whose dictionary would not fit. {@code -1} means unknown.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter, final long expectedRows) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter, expectedRows,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /**
   * Arm a load owned by {@code ownerTrx}. Listener epochs may resolve the process-global handoff only
   * when they carry this exact transaction identity; another transaction on the same resource and
   * definition can neither attach to nor advance it.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final NodeReadOnlyTrx ownerTrx, final PathSummaryReader pathSummary,
      final StorageEngineWriter storageEngineWriter, final long expectedRows) {
    return begin(indexDef, resourceKey, ownerTrx, pathSummary, storageEngineWriter, expectedRows,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /**
   * Bind the trie lane for this load: the record pages and the projection leaves name ONE dictionary
   * per column.
   *
   * <p>
   * The dictionaries were committed by the pre-import pre-pass into the still-empty resource, so the
   * revision to read them at is the most recent COMMITTED one — this load's own revision is not
   * committed yet, and the encode-side probes run on flush threads that must see a fixed structure.
   * </p>
   *
   * <p>
   * Silent when no prebuilt anchors are configured, which is every load that is not using the lane:
   * the writer keeps a null resolver and every page stores bytes, exactly as before. A FAILURE to
   * bind, on the other hand, is not silent — a configured anchor that cannot be read means the pages
   * would name a dictionary no reader can resolve, and that must stop the load rather than quietly
   * produce an unconverted one, since the load's whole point in that configuration is the conversion.
   * </p>
   */
  private void bindTrieLane(final StorageEngineWriter storageEngineWriter, final IndexDef indexDef) {
    if (!TRIE_LANE_ENABLED) {
      // The lane's ONE kill switch, and it exists so an A/B can isolate it. The prebuilt dictionaries
      // this binds against are also consumed by the projection index's own build, which converts its
      // leaves independently and is a large storage effect in its own right -- so a "prebuilt on"
      // arm moves two levers at once, and without this switch neither can be attributed. It gates
      // BEHAVIOUR only: pages simply keep their bytes, and every decoder still reads a converted
      // page written by an earlier run.
      return;
    }
    final TrieLaneWriteDictionaries dictionaries =
        TrieLaneWriteDictionaries.bindConfigured(storageEngineWriter.getResourceSession(),
            storageEngineWriter.getResourceSession().getMostRecentRevisionNumber(),
            indexDef.getProjectionFields().size());
    if (dictionaries == null) {
      return;
    }
    this.trieLaneWriteDictionaries = dictionaries;
    this.trieLaneWriter = storageEngineWriter;
    builder.setTrieLaneWriteDictionaries(dictionaries);
    storageEngineWriter.installDocumentStringDictionaries(dictionaries);
  }

  /**
   * Release the trie lane's per-thread snapshot readers.
   *
   * <p>
   * Called from the load's terminal paths only. By then the flush executor has drained, which is the
   * one moment at which no flush thread can still be inside a probe.
   * </p>
   */
  private void releaseTrieLane(final StorageEngineWriter storageEngineWriter) {
    final TrieLaneWriteDictionaries dictionaries = trieLaneWriteDictionaries;
    if (dictionaries == null) {
      return;
    }
    trieLaneWriteDictionaries = null;
    trieLaneWriter = null;
    if (storageEngineWriter != null) {
      // Stop handing the resolver to NEW pages first, then DRAIN, then close. That order is the
      // whole safety argument: a page created after this line carries no resolver and converts
      // nothing, and awaitPendingAsyncFlush returns only once every page already in flight has been
      // serialized -- which is the last moment any flush thread can be inside a probe.
      //
      // On the ABORT path there is no drain to wait for -- the listener aborts mid-load and the
      // import keeps flushing -- which is why uninstalling FIRST matters there: it stops new pages
      // getting the resolver, so the resolver's own in-flight gate has a closing window to wait out
      // rather than an open-ended stream.
      //
      // A loud guarantee beats a counted degrade, but the counter stays as the check on this claim:
      // if the drain is not what I think it is, closedProbeCount comes back non-zero and the
      // converted arm's gate fails with a number instead of quietly under-converting the pages that
      // flushed late. Best-effort inside a finally -- a failure to drain must not mask the failure
      // that brought us here, and the close below has to happen either way.
      storageEngineWriter.installDocumentStringDictionaries(null);
      try {
        storageEngineWriter.awaitPendingAsyncFlush();
      } catch (final RuntimeException drainFailure) {
        LOGGER.warn("trie lane: draining the async flush before releasing the encode-side readers failed; "
            + "closedProbeCount is the check on whether that mattered", drainFailure);
      }
    }
    // stdout as well as the log, and deliberately: these counters ARE the gate's evidence -- the
    // converted arm asserts absent == 0 and afterClose == 0 -- and a load run with the root logger at
    // ERROR would otherwise report a conversion that left no trace of whether it happened. The
    // pre-pass hook's own [prepass-hook] lines set the precedent for load-time gate output.
    System.out.printf("[trie-lane] %s%n", dictionaries.describeCounters());
    LOGGER.info("{}", dictionaries.describeCounters());
    if (dictionaries.closedProbeCount() > 0) {
      // Should be unreachable after the drain above. Logged rather than thrown because by here the
      // pages are already written and refusing changes nothing -- but it is the signal that the
      // lane under-converted and that the arm's storage number is not the lever's number.
      LOGGER.warn("trie lane: {} probes arrived AFTER the lane was released, so late-flushed pages kept "
          + "their bytes; this arm under-converted and its size is not comparable",
          dictionaries.closedProbeCount());
    }
    dictionaries.close();
  }

  /** Internal publication-injected form used by focused storage-failure coverage. */
  static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter, final long expectedRows,
      final RowGroupPublisher rowGroupPublisher) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter, expectedRows,
        rowGroupPublisher);
  }

  /** Owner-bound publication-injected form used by focused lifecycle and storage-failure coverage. */
  static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey, final Object ownerToken,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter, final long expectedRows,
      final RowGroupPublisher rowGroupPublisher) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionBulkLoad requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    final String key = keyOf(resourceKey, indexDef.getID());
    final ProjectionBulkLoad load = new ProjectionBulkLoad(indexDef, key, ownerToken, pathSummary, rowGroupPublisher);
    load.builder.setExpectedRows(expectedRows);
    final ProjectionBulkLoad previous = ACTIVE.putIfAbsent(key, load);
    if (previous != null) {
      throw new IllegalStateException(
          "A projection bulk load is already active for " + key + " — finish or abort it before starting another");
    }
    boolean persistentMutationStarted = false;
    try {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      // The load-time builder is legal only as a virgin-tree initializer. Existing definitions have
      // exactly one update path: incremental listener maintenance.
      storage.requireVirginTreeForInitialBuild();
      persistentMutationStarted = true;
      ProjectionStructuralOrderDirectory.open(storage).seedRoot(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      storage.putBlob(0, ProjectionIndexMetadata.staleTombstone().serialize());
      load.bindTrieLane(storageEngineWriter, indexDef);
      return load;
    } catch (final Throwable failure) {
      if (persistentMutationStarted) {
        poisonOwningTransaction(storageEngineWriter, failure);
      }
      // The registry entry is already visible. A failure to establish the tombstone must retire it
      // before escaping, or a retry sees a phantom "already loading" build with no fail-closed slot.
      load.poisonAfterFailure(failure);
      throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
    }
  }

  /**
   * The bulk load armed for this definition, or {@code null} when the resource is not being loaded.
   */
  public static @Nullable ProjectionBulkLoad active(final String resourceKey, final int indexDefId) {
    return ACTIVE.get(keyOf(resourceKey, indexDefId));
  }

  /** Resolve an armed load only for its exact owning write transaction. */
  public static @Nullable ProjectionBulkLoad active(final String resourceKey, final int indexDefId,
      final NodeReadOnlyTrx ownerTrx) {
    final ProjectionBulkLoad load = ACTIVE.get(keyOf(resourceKey, indexDefId));
    return load != null && load.ownerToken == ownerTrx
        ? load
        : null;
  }

  /** Whether any bulk load is armed at all — one map read on the listener's construction path. */
  public static boolean anyActive() {
    return !ACTIVE.isEmpty();
  }

  /**
   * Drop the load without finishing it; slot 0 keeps the tombstone, so readers stay on the generic
   * path.
   */
  /**
   * Mid-feed abandonment: the unconditional operator notice (same sentence shape as the listener's
   * drain-lane arm, for the same reason — a silent degradation reads as a healthy load), then
   * {@link #abort()}. Subsequent feed calls no-op via {@link #isFinished()}.
   */
  /**
   * Test seam: {@code false} restores the pre-fix coordinator-lane behaviour in which a dictionary
   * budget breach propagated out of the feed and poisoned the whole load. Only
   * {@code CoordinatorFeedBudgetAbandonTest} flips it.
   */
  static volatile boolean ABANDON_ON_FEED_BUDGET_BREACH = true;

  private synchronized void abandonDuringFeed(final GlobalDictionaryBudgetExceededException tooBig,
      final StorageEngineWriter storageEngineWriter) {
    if (finished) {
      return;
    }
    final String breach = tooBig.breachingTerm() == null
        ? "declined an unsafe allocation over " + tooBig.entryCount() + " distinct values (" + tooBig.retainedBytes()
            + " B retained): " + tooBig.admissionDetail()
        : "needed " + tooBig.breachingBytes() + " B (" + tooBig.breachingTerm() + ") over " + tooBig.entryCount()
            + " distinct values, past its " + tooBig.budgetBytes() + " B budget (" + tooBig.retainedBytes()
            + " B retained)";
    System.err.println("[proj] PROJECTION ABANDONED during the load: index " + indexDef.getID() + ", column "
        + tooBig.column() + " " + breach + ". The load completes; the projection is STALE and every query will take"
        + " the generic pipeline. After the load, drop and commit this stale definition before creating a"
        + " replacement in a new projection tree.");
    abort();
    // Same tombstone the listener lane leaves: the machine-readable reason rides the write
    // transaction (invisible until commit, gone on rollback) so an operator or a guard can tell
    // "abandoned for its dictionary budget" from an unspecified failure. Never overwritten later; a
    // replacement goes under a fresh tree id.
    new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID()).putBlob(0,
        ProjectionIndexMetadata.staleTombstone(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED)
                               .serialize());
  }

  public synchronized void abort() {
    if (finished) {
      return;
    }
    // Poison before any cleanup that can itself fail. A concurrent/re-entrant caller can never see
    // a resumable build after publication failed, and repeated listener cleanup is a no-op.
    finished = true;
    ACTIVE.remove(key, this);
    // The lane's per-thread snapshot readers are a resource; an aborted load must free them exactly
    // as it frees the bloom chunks and the summaries. The writer reference is not available here, so
    // only the readers are closed -- the writer's own resolver field dies with the writer.
    releaseTrieLane(trieLaneWriter);
    try {
      bloomChunks.release();
    } finally {
      try {
        setSummaries.release();
      } finally {
        try {
          builder.releaseTransientState();
        } finally {
          storage = null;
        }
      }
    }
  }

  /** Whether the declared root path selects array elements — see {@link #arrayElementRoot}. */
  public boolean isArrayElementRoot() {
    return arrayElementRoot;
  }

  /**
   * Whether the build is closed — {@link #finish} ran, or {@link #abort} did. From this point the
   * projection is an ordinary maintained index and the listener hands itself back to the dirty-set
   * maintenance path; see {@code ProjectionIndexChangeListener#activeBulkLoad}.
   */
  public boolean isFinished() {
    return finished;
  }

  /** The record whose subtree is currently being shredded, or {@code -1}. */
  public long currentRecordKey() {
    return currentRecordKey;
  }

  /** Whether {@code nodeKey} is a known record-set array instance. */
  public boolean isArrayRootInstance(final long nodeKey) {
    return nodeKey == activeArrayRootKey;
  }

  /** Remember an array instance whose elements are records. */
  public void noteArrayRootInstance(final long nodeKey, final NodeReadOnlyTrx rtx) {
    if (nodeKey == activeArrayRootKey) {
      return;
    }
    if (activeArrayRootKey >= 0) {
      try {
        countActiveArrayRootRecords(rtx);
      } catch (final Throwable failure) {
        poisonAfterFailure(failure);
        throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
      }
    }
    activeArrayRootKey = nodeKey;
    activeArrayRootCountedChildren = 0L;
    arrayRootInstanceCount++;
  }

  /**
   * Note that {@code recordKey} is the record the shredder is now writing into. A change of record
   * CLOSES the previous one — node keys are monotone and a shred visits records in document order, so
   * a record can be extracted the moment a later one begins.
   *
   * <p>
   * Fails loudly on a non-monotone record key. This is an explicit append-only contract, not a
   * heuristic: an out-of-order record means the transaction is not a bulk load (an update reached
   * back into an already-extracted record), and continuing would silently drop that record's new
   * state.
   */
  /**
   * A chunk batch bound to this build's current field resolution — the parallel importer requests one
   * per chunk, at dispatch, AFTER the chunk's new paths were resolved into the path summary.
   */
  public ProjectionChunkRowBatch newChunkBatch(final PathSummaryReader pathSummary, final int expectedRows,
      final long recordSetKey) {
    return builder.newChunkBatch(pathSummary, expectedRows, recordSetKey);
  }

  /** Maximum rows whose row-indexed arrays stay at or below the 256 KiB HFT payload ceiling. */
  public int maxHftChunkRows() {
    return ProjectionChunkRowBatch.maxHftChunkRows(builder.columnKinds().length);
  }

  /**
   * Append one worker-extracted row, in document order — the coordinator-fed replacement for the
   * {@link #observeRecord}/{@link #drain} pair: the row's values come from the batch instead of a
   * record re-read, and its order label from the in-order append lane instead of an ancestry walk, so
   * nothing here reads the document. Storage and a dictionary generation are bound lazily per storage
   * epoch; the rotation drain flushes the generation and unbinds, which is what keeps the
   * resource-wide dictionary rotating exactly as it does for the notification-fed load.
   */
  public void appendCoordinatorRow(final StorageEngineWriter storageEngineWriter, final ProjectionChunkRowBatch batch,
      final int row, final long recordKey, final long containerKey, final long documentRootKey) {
    if (finished) {
      return; // abandoned mid-load — the load continues, the projection does not
    }
    if (finished) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key + " was fed record "
          + recordKey + " after its load-time build was finished.");
    }
    if (recordKey <= lastClosedRecordKey) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key
          + " is coordinator-fed in document order, which is append-only: record " + recordKey
          + " arrived after record " + lastClosedRecordKey + " had already been appended.");
    }
    boolean persistentMutationStarted = false;
    try {
      coordinatorFeed = true;
      ProjectionStructuralOrderDirectory.Accessor directory = feedDirectory;
      if (storage == null || directory == null) {
        final ProjectionIndexHOTStorage currentStorage =
            ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
        this.storage = currentStorage;
        directory = ProjectionStructuralOrderDirectory.open(currentStorage);
        this.feedDirectory = directory;
        builder.beginStreamingDictionaryEpoch(storageEngineWriter);
      }
      persistentMutationStarted = true;
      final SirixDeweyID orderLabel =
          directory.fullLabelForInOrderAppend(recordKey, containerKey, documentRootKey, feedLastRecordLocal);
      builder.appendBatchRow(batch, row, recordKey, orderLabel);
      feedLastRecordLocal = directory.lastInOrderAppendedLocal();
      lastClosedRecordKey = recordKey;
      diagObserved++;
      diagClosed++;
    } catch (final GlobalDictionaryBudgetExceededException tooBig) {
      // The ONE recoverable failure of this feed: a resource-wide dictionary hit its allocation
      // bound. The probe front refuses BEFORE mutating, so the build state is consistent — and the
      // designed outcome (the exception says it itself) is to abandon the PROJECTION and let the
      // LOAD complete on the generic pipeline. The listener's drain lane already does exactly this;
      // without this arm the coordinator feed lane poisoned the whole transaction instead, which is
      // how a 100M AUTO load died at 674k distinct URL values with exit 1.
      if (!ABANDON_ON_FEED_BUDGET_BREACH) {
        // Test seam: the pre-fix behaviour, so the regression test can prove this arm is what keeps
        // the load alive.
        poisonAfterFailure(tooBig);
        throw tooBig;
      }
      abandonDuringFeed(tooBig, storageEngineWriter);
    } catch (final Throwable failure) {
      if (persistentMutationStarted) {
        poisonOwningTransaction(storageEngineWriter, failure);
      }
      poisonAfterFailure(failure);
      throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
    }
  }

  public void observeRecord(final long recordKey) {
    if (coordinatorFeed) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key
          + " is coordinator-fed; a change notification for record " + recordKey
          + " would announce the same records through a second mechanism.");
    }
    if (recordKey == currentRecordKey) {
      return;
    }
    if (finished) {
      // The build is closed and its metadata written; a record arriving now would be accepted into a
      // batch nothing will ever drain, and the index would be short that row with no sign of it. The
      // ordinary maintenance path takes over from the next transaction epoch, which is where a
      // post-load insert belongs.
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key + " saw record " + recordKey
          + " after its load-time build was finished. Records written after the "
          + "load's final commit belong to ordinary incremental maintenance, which the listener hands over to "
          + "automatically — reaching this means a listener kept feeding a closed build, and the record would "
          + "have been accepted into a batch nothing will ever drain.");
    }
    if (recordKey <= lastClosedRecordKey) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key
          + " is being built by the LOAD-TIME path, which is append-only: it extracts each record once, when a "
          + "later record begins, so it cannot revisit one. Record " + recordKey + " was touched after record "
          + lastClosedRecordKey + " had already been extracted, which means this transaction is not a "
          + "document-order bulk load (an update reached back into an already-indexed record). Two ways "
          + "forward: (1) load the data with no declared projection and build the index afterwards with "
          + "jn:create-projection-index, which walks the finished resource and handles any write order; or "
          + "(2) keep the load append-only and make these updates AFTER its final commit, where ordinary "
          + "incremental maintenance handles them.");
    }
    closeCurrentRecord();
    currentRecordKey = recordKey;
  }

  /**
   * Close the open record without starting another. {@link #observeRecord} holds the current record
   * back because a shredder's notification arrives mid-subtree: an extraction there would store a
   * half-built row, so the record is closed only when a later one begins.
   */
  private void closeCurrentRecord() {
    if (currentRecordKey >= 0) {
      completedRecordKeys.add(currentRecordKey);
      lastClosedRecordKey = currentRecordKey;
      diagClosed++;
      currentRecordKey = -1L;
    }
  }

  /**
   * Extract every record closed since the last drain into the builder. Runs at pre-commit, where the
   * cursor may be moved: doing it inside a change notification would move the cursor out from under
   * the shredder mid-insert.
   */
  public void drain(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx) {
    if (finished) {
      return;
    }
    long savedNodeKey = -1L;
    boolean restoreCursor = false;
    Throwable primaryFailure = null;
    try {
      countArrayRootRecords(rtx);
      if (coordinatorFeed) {
        // Rows were appended at adoption, straight from the worker batches; the drain's only jobs
        // here are the record-count refresh above and closing the epoch: flush the dictionary
        // generation begun by the epoch's first append — the rotation that lifts the per-append
        // ceiling — and let the finally below unbind the epoch's storage and label lane.
        if (storage != null) {
          builder.flushStreamingDictionaryGeneration(storageEngineWriter);
        }
        return;
      }
      if (completedRecordKeys.isEmpty()) {
        return;
      }
      // Acquisition can allocate/prepare the HOT root and therefore fail. Keep it under the same
      // poison boundary as extraction and publication so no ACTIVE load survives a writer fault.
      final ProjectionIndexHOTStorage currentStorage =
          ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      this.storage = currentStorage;
      final ProjectionStructuralOrderDirectory.Accessor structuralOrderDirectory =
          ProjectionStructuralOrderDirectory.open(currentStorage);
      final LongFunction<ImmutableNode> documentNodeLookup =
          nodeKey -> storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      builder.beginStreamingDictionaryEpoch(storageEngineWriter);
      savedNodeKey = rtx.getNodeKey();
      restoreCursor = true;
      // The summary is still growing: a field whose first occurrence is in this batch has only just
      // acquired a path class, and an extractor that predates it would record the field ABSENT.
      builder.refreshFieldPaths(pathSummary);
      diagDrains++;
      for (int i = 0; i < completedRecordKeys.size(); i++) {
        final long recordKey = completedRecordKeys.getLong(i);
        final SirixDeweyID orderLabel = structuralOrderDirectory.fullLabel(recordKey, documentNodeLookup,
            ProjectionStructuralOrderDirectory.RelabelSink.SEALED);
        if (!appendRecord(rtx, recordKey, orderLabel)) {
          diagExtractFailures++;
        }
      }
      // The per-epoch generation flush is deliberate and must stay: each generation is bounded by
      // the radix append's MAX_DISTINCT_ENTRIES_PER_APPEND ceiling, and rotating bounded writers is
      // the ONLY way a column's dictionary exceeds that ceiling. (Skipping this call and keeping
      // one whole-load writer was tried: the writer's own per-append ceiling aborted every
      // high-cardinality load mid-way.) What must NOT return is the old probe regime — a released
      // generation's values re-resolved through the persistent radix per occurrence, ~85% of load
      // CPU — which the StreamingGlobalDictionary's resident probe front now prevents: it keeps
      // every (value, id) of the whole load resident, so this flush releases only the WRITER, never
      // the ability to probe. dictProbes=0 in the load banner is the witness.
      builder.flushStreamingDictionaryGeneration(storageEngineWriter);
    } catch (final Throwable failure) {
      primaryFailure = failure;
      poisonAfterFailure(failure);
      throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
    } finally {
      Throwable cleanupFailure = null;
      try {
        completedRecordKeys.clear();
        if (restoreCursor && !rtx.moveTo(savedNodeKey)) {
          ((NodeCursor) rtx).moveToDocumentRoot();
        }
      } catch (final Throwable failure) {
        cleanupFailure = failure;
      } finally {
        this.storage = null;
        this.feedDirectory = null;
      }
      if (cleanupFailure != null) {
        if (primaryFailure != null) {
          addSuppressedSafely(primaryFailure, cleanupFailure);
        } else {
          poisonAfterFailure(cleanupFailure);
          throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(cleanupFailure);
        }
      }
    }
  }

  /** Retire a partially published load while preserving the failure that caused the retirement. */
  private void poisonAfterFailure(final Throwable primaryFailure) {
    try {
      abort();
    } catch (final Throwable cleanupFailure) {
      addSuppressedSafely(primaryFailure, cleanupFailure);
    }
  }

  /** Prevent a caller from committing a partially published multi-slot operation. */
  private static void poisonOwningTransaction(final StorageEngineWriter storageEngineWriter,
      final Throwable primaryFailure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(primaryFailure);
    } catch (final RuntimeException | Error poisonFailure) {
      addSuppressedSafely(primaryFailure, poisonFailure);
    }
  }

  private static void addSuppressedSafely(final Throwable primaryFailure, final Throwable secondaryFailure) {
    if (primaryFailure == secondaryFailure) {
      return;
    }
    try {
      primaryFailure.addSuppressed(secondaryFailure);
    } catch (final RuntimeException | Error ignored) {
      // Preserve the authoritative publication failure even when cleanup runs under VM pressure.
    }
  }

  /** Rethrow any callback/extraction failure without widening the public drain signature. */
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T rethrowUnchecked(final Throwable failure) throws T {
    throw (T) failure;
  }

  /**
   * Close the load: extract the last open record, drain the builder, persist the resource-wide
   * dictionaries, and write the metadata, fingerprint blocks and fences through the SAME
   * {@code finishPersist} the post-pass build uses — which is what makes the two indexes equivalent.
   *
   * <p>
   * The record-set shape is validated here rather than at declaration, because at declaration the
   * resource is empty and the root path has no path class yet: a root that turns out to select NESTED
   * record sets would have let an inner record's fields overwrite an outer row's columns, so it fails
   * loudly now, with the tombstone still in place.
   */
  public void finish(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx, final int buildRevision) {
    if (finished) {
      return;
    }
    closeCurrentRecord();
    // EVERYTHING inside the try, the shape and row-count checks included: a build that fails one of
    // them is dead either way, and its registry entry has to be retired regardless or the definition
    // stays "already loading" for the life of the JVM and a retry cannot even arm. Failing before
    // finishPersist leaves slot 0's tombstone in place, which is exactly the state a failed load
    // should leave behind.
    try {
      assertRecordSetShape(pathSummary);
      drain(storageEngineWriter, pathSummary, rtx);
      countArrayRootRecords(rtx);
      assertEveryRecordEmitted();
      this.storage = ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      builder.beginStreamingDictionaryEpoch(storageEngineWriter);
      builder.finishStreaming();
      final byte[] columnKinds = builder.columnKinds();
      bloomChunks.finishChunks(storage, fenceWriter.rowGroupCount(), columnKinds);
      final long[] valueDictionaryHeaderKeys = builder.flushStreamingDictionaryGeneration(storageEngineWriter);
      fenceWriter.finish(storage);
      // A bulk load owns the virgin sub-tree it created itself; publication never replaces prior units.
      ProjectionIndexBuilder.finishPersistWithStreamingFences(indexDef, storage, fenceWriter.rowGroupCount(),
          buildRevision, columnKinds, setSummaries, valueDictionaryHeaderKeys, bloomChunks);
      builder.publishGlobalDictionaryColumnsBuilt();
    } finally {
      // Before every other release, because it is the one that reports: the probe/hit/absent counters
      // are the lane's only evidence of what it actually did, and the converted-arm gate asserts
      // absent == 0. A finish that failed still frees the snapshot readers.
      releaseTrieLane(storageEngineWriter);
      try {
        bloomChunks.release();
      } finally {
        try {
          setSummaries.release();
        } finally {
          try {
            builder.releaseTransientState();
          } finally {
            this.storage = null;
            finished = true;
            ACTIVE.remove(key, this);
          }
        }
      }
    }
  }

  /** Rows appended so far — test and diagnostic observability. */
  public long rowsEmitted() {
    return builder.rowsEmitted();
  }

  /**
   * Prove the load emitted a row for EVERY record, by counting the record-set arrays' children.
   *
   * <p>
   * This is the invariant that makes per-notification record attribution safe to rely on. Attribution
   * decides which node belongs to which record from change notifications alone; if it ever missed a
   * record root, the index would simply be short a row — no exception, no wrong-looking output, just
   * a query that quietly answers over fewer records than the resource holds. An array node already
   * knows its child count, so the check costs one node read per record set and turns that class of
   * bug from silent into loud.
   *
   * <p>
   * Only the array-rooted shape is checkable this way, which is also the only shape whose attribution
   * takes the kind-filtered fast path; a non-array record root is resolved by the full ancestor walk
   * for every notification and has no comparable counter to check against.
   */
  private void assertEveryRecordEmitted() {
    if (arrayRootInstanceCount == 0) {
      return;
    }
    if (expectedArrayRecords != builder.rowsEmitted()) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " on " + key + " emitted " + builder.rowsEmitted() + " rows for "
              + expectedArrayRecords + " records: the load-time build's per-notification "
              + "record attribution missed records, and the index would have answered over fewer records than the "
              + "resource holds. Nothing was published — slot 0 still holds the tombstone, so the resource's data is "
              + "intact and its queries fall back to the generic pipeline. Build the index with "
              + "jn:create-projection-index over the loaded resource, which walks it rather than attributing "
              + "notifications." + diagnostics());
    }
  }

  private void countArrayRootRecords(final NodeReadOnlyTrx rtx) {
    if (activeArrayRootKey >= 0) {
      countActiveArrayRootRecords(rtx);
    }
  }

  private void countActiveArrayRootRecords(final NodeReadOnlyTrx rtx) {
    final long savedNodeKey = rtx.getNodeKey();
    final long children;
    try {
      children = arrayChildCount(rtx, activeArrayRootKey);
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        ((NodeCursor) rtx).moveToDocumentRoot();
      }
    }
    if (children < activeArrayRootCountedChildren) {
      throw new IllegalStateException("Projection bulk load for " + key + " saw record-set array " + activeArrayRootKey
          + " shrink from " + activeArrayRootCountedChildren + " to " + children);
    }
    expectedArrayRecords = Math.addExact(expectedArrayRecords, children - activeArrayRootCountedChildren);
    activeArrayRootCountedChildren = children;
  }

  private long arrayChildCount(final NodeReadOnlyTrx rtx, final long arrayKey) {
    if (!rtx.moveTo(arrayKey)) {
      throw new IllegalStateException("Projection bulk load for " + key + " cannot read record-set array " + arrayKey
          + " during its bounded drain");
    }
    return rtx.getChildCount();
  }

  /**
   * Reject ancestor/descendant overlap only among matches of the declared ROOT path, mirroring the
   * walking builder's up-front guard. Projection COLUMN paths are not compared and may descend to
   * arbitrary and differing depths below a root. This check is reachable only after the resource has
   * data, so the load-time builder runs it at the end of the load.
   */
  private void assertRecordSetShape(final PathSummaryReader pathSummary) {
    final Path<QNm> rootPath = indexDef.getProjectionRootPath();
    final LongSet rootPcrs = pathSummary.getPCRsForPaths(Set.of(rootPath));
    if (rootPcrs.isEmpty()) {
      return; // the record set never materialised — the truthful empty projection
    }
    final LongOpenHashSet roots = new LongOpenHashSet(rootPcrs.size());
    for (final LongIterator it = rootPcrs.iterator(); it.hasNext();) {
      roots.add(it.nextLong());
    }
    final long saved = pathSummary.getNodeKey();
    try {
      for (final LongIterator it = roots.iterator(); it.hasNext();) {
        final long root = it.nextLong();
        if (!pathSummary.moveTo(root)) {
          continue;
        }
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0) {
            break;
          }
          if (roots.contains(pk)) {
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

  private ProjectionIndexHOTStorage currentStorage() {
    final ProjectionIndexHOTStorage current = storage;
    if (current == null) {
      throw new IllegalStateException(
          "Projection bulk load for " + key + " emitted a leaf outside a drain — no storage is bound");
    }
    return current;
  }

  private boolean appendRecord(final NodeReadOnlyTrx rtx, final long recordKey, final SirixDeweyID orderLabel) {
    if (rtx instanceof final JsonNodeReadOnlyTrx jsonRtx) {
      return builder.appendRecord(jsonRtx, recordKey, orderLabel);
    }
    if (rtx instanceof final XmlNodeReadOnlyTrx xmlRtx) {
      return builder.appendRecord(xmlRtx, recordKey, orderLabel);
    }
    throw new IllegalArgumentException("projection bulk load requires a JSON or XML node transaction");
  }

  /** The declared field types, for callers that mirror the definition's column shape. */
  List<Type> fieldTypes() {
    return indexDef.getProjectionFieldTypes();
  }

  /** Column kinds the build settled on — used by tests to compare the two build routes. */
  public byte[] columnKinds() {
    return builder.columnKinds();
  }

  /** Leaf count so far — used by tests to compare the two build routes. */
  public int rowGroupCount() {
    return fenceWriter.rowGroupCount();
  }

  /** Drop every armed load — test isolation only. */
  public static void clearActive() {
    for (final ProjectionBulkLoad load : ACTIVE.values()) {
      load.abort();
    }
    ACTIVE.clear();
  }

  /** Names of the definitions currently loading — diagnostics. */
  public static List<String> activeKeys() {
    return new ArrayList<>(ACTIVE.keySet());
  }
}

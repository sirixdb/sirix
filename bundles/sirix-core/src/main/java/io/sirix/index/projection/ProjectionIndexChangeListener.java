/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.NodeTrx;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Update-time maintenance hook for a projection index, wired through the {@code IndexController}
 * listener lifecycle like the PATH/CAS/NAME listeners — and, like them, maintaining the index
 * INCREMENTALLY.
 *
 * <h2>Two-phase incremental maintenance</h2>
 *
 * <b>Listen phase (per change, hot path):</b> each notification is classified by pathNodeKey
 * against lazily seeded PCR sets; relevant changes resolve the enclosing <em>record</em> (the row's
 * identity) by walking the node's ancestor chain through raw page-layer record reads — no cursor
 * movement, no allocation beyond the dirty-key set — and add the record's nodeKey to a
 * per-transaction dirty set. Nothing is written yet.
 *
 * <b>Apply phase (once, at pre-commit via
 * {@link IndexController#applyPendingIndexMaintenance()}):</b> dirty records are located by the
 * sparse exception locator first and the normal-backbone fences second. Inserts and deletes rebuild
 * their touched leaves; an update that does not change row membership re-extracts only its dirty
 * column across the leaf and patches only that column's immutable segments. Extraction semantics
 * are shared 1:1 with the bulk builder via {@link ProjectionIndexRowExtractor}. Stable node keys
 * identify rows but never determine their order: persisted projection-owned order labels place new
 * and moved rows, while sparse record locators route order exceptions. Slot 0's metadata is
 * rewritten with the updated leaf count and the committing revision as the new build revision,
 * which re-keys the catalog's decoded-leaf cache. All writes ride the write transaction — invisible
 * to concurrent readers until commit, discarded on rollback, and historical revisions keep serving
 * their own immutable snapshots.
 *
 * <h2>Bounded ownership</h2> Ordinary maintenance rewrites only touched row groups, fence chunks,
 * Bloom chunks, set-summary columns, and affected immutable dictionary radix paths. It never scans
 * or rebuilds the complete projection. Structural moves attribute both the old and new enclosing
 * records and splice moved record roots through bounded local row-group edits. Moves wholly within
 * one record only rewrite that record's dirty persistent units.
 *
 * <h2>Hot-path cost</h2> The relevant-PCR sets are seeded LAZILY on the first notification, so
 * write transactions that never touch any node pay nothing beyond object construction at open.
 * After seeding, an irrelevant notification is one {@code LongOpenHashSet#contains}; a relevant one
 * adds an ancestor walk bounded by the record's nesting depth (raw record reads served from the
 * transaction log / page cache).
 */
public final class ProjectionIndexChangeListener implements PathNodeKeyChangeListener {

  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(ProjectionIndexChangeListener.class));

  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");
  private static final AtomicLong HFT_COMMITS = new AtomicLong();
  private static final AtomicLong HFT_DIRTY_RECORDS = new AtomicLong();
  private static final AtomicLong HFT_ROW_GROUPS_READ = new AtomicLong();
  private static final AtomicLong HFT_ROW_GROUPS_WRITTEN = new AtomicLong();
  private static final AtomicLong HFT_DICTIONARY_SEGMENTS = new AtomicLong();
  private static final AtomicLong HFT_FENCE_CHUNKS_READ = new AtomicLong();
  private static final AtomicLong HFT_FENCE_CHUNKS_WRITTEN = new AtomicLong();
  private static final AtomicLong HFT_SET_CHUNKS_READ = new AtomicLong();
  private static final AtomicLong HFT_SET_CHUNKS_WRITTEN = new AtomicLong();
  private static final AtomicLong HFT_BLOOM_ROW_GROUPS_READ = new AtomicLong();
  private static final AtomicLong HFT_BLOOM_CHUNKS_WRITTEN = new AtomicLong();
  private static final AtomicLong HFT_METADATA_READS = new AtomicLong();
  private static final AtomicLong HFT_METADATA_WRITES = new AtomicLong();
  private static final AtomicLong HFT_DICTIONARY_PROBES = new AtomicLong();

  /**
   * Complete projection builds ({@code ProjectionIndexBuilder.buildAndPersist}) since the last
   * telemetry reset. The maintenance contract is INCREMENTAL — inserts, updates, deletes and moves
   * patch row groups in place — so after a reset this counter staying at zero across a maintenance
   * window is the witness that no path quietly fell back to an O(corpus) rebuild. It ticks for
   * initial creations too, which is why the gates reset BEFORE the window they measure.
   */
  private static final AtomicLong HFT_FULL_REBUILDS = new AtomicLong();

  /** Tick one complete projection build; called by the builder, package-visible on purpose. */
  static void recordFullRebuild() {
    HFT_FULL_REBUILDS.incrementAndGet();
  }

  /** Last completed maintenance pass, used by locality tests and operational diagnostics. */
  private static volatile MaintenanceLocality LAST_MAINTENANCE_LOCALITY = MaintenanceLocality.EMPTY;

  private static final int DEEP_WALK_CYCLE_THRESHOLD = 512;

  /** Resolution verdict: change is not under any record — no row affected. */
  private static final long NOT_UNDER_RECORD_SET = -2L;
  /** Resolution verdict: chain unresolvable — fail the owning transaction. */
  private static final long UNRESOLVED = -4L;
  /** Sentinel for "parent key not provided by this listen overload". */
  private static final long PARENT_UNKNOWN = Long.MIN_VALUE;
  /** No projected record exists on one side of a document-order interval. */
  private static final long NO_RECORD_KEY = -1L;

  private static final byte FIRST_ROOT_EVENT_MASK = 0x03;
  private static final byte FIRST_ROOT_INSERT = 0x01;
  private static final byte FIRST_ROOT_DELETE = 0x02;
  private static final byte STRUCTURAL_ENTER = 0x04;
  private static final byte STRUCTURAL_EXIT = 0x08;
  private static final int STRUCTURAL_BATCH_SIZE = 256;
  private static final int STRUCTURAL_BATCH_LABEL_BYTES = ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES;

  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryReader pathSummary;
  private final IndexDef indexDef;

  /**
   * Navigation handle over the owning write transaction's current state, used ONLY in the apply phase
   * (pre-commit re-extraction). Every listener has this handle: projection updates have one
   * incremental maintenance contract and never fall back to whole-index invalidation.
   */
  private final NodeReadOnlyTrx maintenanceTrx;

  private ProjectionStructuralOrderDirectory.@Nullable Accessor structuralOrderDirectory;
  /** Records a bounded rebalance re-spread in this transaction; their persisted rows must follow. */
  private @Nullable LongOpenHashSet orderRelabels;
  private ProjectionStructuralOrderDirectory.@Nullable RelabelSink orderRelabelSink;

  /**
   * The load-time build this transaction is feeding, or {@code null} for ordinary maintenance. When
   * set, every path below is bypassed: the projection is not being MAINTAINED but BUILT, by the real
   * build machinery, and the dirty-set patcher's structures (dirty records, resolution memo, per-leaf
   * rebuilds) are neither needed nor affordable at load scale.
   *
   * <p>
   * Cleared by {@link #activeBulkLoad()} the moment the build finishes — see the HANDOFF note there.
   */
  private @Nullable ProjectionBulkLoad bulkLoad;

  /**
   * Bulk mode only: a fused {@code OBJECT_NAMED_*} node is an object FIELD and can never be an array
   * element, so when the record set is the elements of an array those notifications need no ancestor
   * read at all. The parent-aware primitive notification also makes the record-root classification a
   * set lookup rather than a page-layer self-read in the usual one-array-root load.
   */
  private final boolean bulkSkipsNamedKinds;

  /** Root PCRs of the record set; empty ⇒ conservative mode (everything relevant). */
  private LongOpenHashSet rootPcrs;
  /** Warm cache: pathNodeKeys known to affect this projection. */
  private LongOpenHashSet relevantPcrs;
  /** Warm cache: pathNodeKeys known NOT to affect this projection. */
  private LongOpenHashSet irrelevantPcrs;

  private boolean seeded;

  /** Fail-closed state used only after an explicitly abandoned bulk build. */
  private boolean invalidated;

  private boolean maintenanceFailed;
  /** Record nodeKeys touched by this transaction; lazily allocated. */
  private @Nullable LongOpenHashSet dirtyRecordKeys;
  private @Nullable Long2ObjectOpenHashMap<long[]> dirtyColumnWordsByRecord;
  /**
   * First authoritative root event plus structural membership provenance, keyed by record identity.
   */
  private @Nullable Long2ByteOpenHashMap rootProvenanceByRecord;
  /** One active surgery and the completed deltas retained until apply-time placement. */
  private @Nullable StructuralRange pendingStructuralRecords;
  private @Nullable ArrayList<StructuralDelta> structuralDeltas;
  private @Nullable LongOpenHashSet pendingStructuralDeletes;

  /**
   * Positive resolution memo: nodeKey → enclosing record's nodeKey, for nodes proven to lie INSIDE a
   * record. Bulk subtree mutations notify every descendant; without the memo each notification
   * re-walks the full ancestor chain (O(nodes × depth) raw record reads) — with it, a walk stops at
   * the first memoized ancestor. Only positive verdicts are memoized: a NOT_UNDER node can still have
   * record descendants (it may be an ancestor of the record set), so negative memoization would be
   * wrong. Lazily allocated; writes stop at {@link #MEMO_CAP} entries.
   */
  private @Nullable Long2LongOpenHashMap resolvedRecordMemo;

  /**
   * NodeKeys proven to be ARRAY-LIKE record-set root instances. A record's own walk crosses its root
   * instance every time and can never memoize it in {@link #resolvedRecordMemo} — the record there is
   * the CHILD, not the root — so without this set every record notification pays a second raw record
   * read just to re-derive "the parent is the array". Bounded by {@link #MEMO_CAP}; in the common
   * single-array case it holds exactly one key.
   */
  private @Nullable LongOpenHashSet arrayRootInstances;

  /** Scratch for the walked ancestor chain (memoized on resolution). */
  private long[] walkChain = new long[32];
  private @Nullable LongOpenHashSet deepWalkVisited;
  private @Nullable Long2ObjectOpenHashMap<long[]> columnWordsByPcr;
  private long[] allColumnWords;

  /**
   * Monotone per-listener maintenance epoch: bumped whenever the pending state changes (new dirty
   * record, invalidation) and when an apply pass rewrites leaves. Lets wtx-serving callers cache a
   * decoded handle and revalidate it with one long compare instead of re-decoding per query.
   */
  private long maintenanceEpoch;

  private static final int MEMO_CAP = 1 << 20;
  private static final long MEMO_MISS = Long.MIN_VALUE;

  public ProjectionIndexChangeListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummary, final IndexDef indexDef, final NodeReadOnlyTrx maintenanceTrx) {
    this(storageEngineWriter, pathSummary, indexDef, maintenanceTrx,
        resolveBulkLoadForNewListener(storageEngineWriter, pathSummary, indexDef, maintenanceTrx));
  }

  /**
   * Validate BEFORE resolving, so that delegating to the injecting constructor cannot change what
   * invalid input does.
   *
   * <p>
   * {@code this(...)} must be the first statement, so this argument expression runs AHEAD of the
   * delegated constructor's own checks. Resolving first would make the failure for identical bad
   * input depend on GLOBAL registry state: with no load armed {@link #resolveBulkLoad} returns early
   * and the delegated constructor reports the real problem, while with one armed it dereferences the
   * very arguments that were never checked and throws a different exception from a different place.
   *
   * <p>
   * The checks below are the delegated constructor's, in ITS EXACT ORDER — projection type first
   * (which is also what makes a null {@code indexDef} fail the way it always has), then
   * {@code storageEngineWriter}, {@code pathSummary}, {@code indexDef}, {@code maintenanceTrx}. Order
   * is not cosmetic here: checking {@code maintenanceTrx} early would turn a non-projection
   * {@link IndexDef} combined with a null transaction from an {@link IllegalArgumentException} into a
   * {@link NullPointerException}. Repeating them in the delegated constructor costs nothing.
   */
  private static @Nullable ProjectionBulkLoad resolveBulkLoadForNewListener(
      final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary, final IndexDef indexDef,
      final NodeReadOnlyTrx maintenanceTrx) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexChangeListener requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter");
    Objects.requireNonNull(pathSummary, "pathSummary");
    Objects.requireNonNull(indexDef, "indexDef");
    Objects.requireNonNull(maintenanceTrx, "maintenanceTrx");
    return resolveBulkLoad(indexDef, maintenanceTrx);
  }

  /**
   * Constructor taking the armed load explicitly, so that {@link #bulkLoad} and the FINAL
   * {@link #bulkSkipsNamedKinds} are always derived from the SAME source.
   *
   * <p>
   * The public constructor resolves the load from the global registry and delegates here, so there is
   * no extra work on any production path — the resolution happens exactly once, as before.
   *
   * <p>
   * It exists because the two fields are correlated and only one of them is a reference: a test that
   * replaced {@code bulkLoad} alone (reflectively, as one did) left {@code bulkSkipsNamedKinds}
   * derived from whatever the registry happened to hold, which is a silent skew between two fields
   * that must agree. Injecting the load removes the possibility rather than asserting against it.
   *
   * @param injectedBulkLoad the armed load to route notifications to, or {@code null} for ordinary
   *        maintenance
   */
  ProjectionIndexChangeListener(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary,
      final IndexDef indexDef, final NodeReadOnlyTrx maintenanceTrx,
      final @Nullable ProjectionBulkLoad injectedBulkLoad) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexChangeListener requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    this.storageEngineWriter = Objects.requireNonNull(storageEngineWriter, "storageEngineWriter");
    this.pathSummary = Objects.requireNonNull(pathSummary, "pathSummary");
    this.indexDef = Objects.requireNonNull(indexDef, "indexDef");
    this.maintenanceTrx = Objects.requireNonNull(maintenanceTrx, "maintenanceTrx");
    this.bulkLoad = injectedBulkLoad;
    this.bulkSkipsNamedKinds = injectedBulkLoad != null && injectedBulkLoad.isArrayElementRoot();
  }

  /**
   * The armed load-time build for this definition, if any. Looked up per listener because listeners
   * are rebuilt at every auto-commit while the build spans all of them; the registry read is guarded
   * by a map-emptiness check so a resource with no load in flight pays one field read.
   */
  /**
   * The load-time build to route this notification to, or {@code null} when ordinary maintenance owns
   * the index.
   *
   * <h2>Handoff</h2> A load ends at its final commit: {@link ProjectionBulkLoad#finish} writes the
   * real metadata over the tombstone and retires the registry entry, and from that moment the
   * projection is an ordinary maintained index. Listeners rebound at later transaction epochs
   * therefore find no armed load and take the maintenance path by construction. THIS listener,
   * however, was constructed before the finish and still holds the reference, so it drops it here —
   * the same instance goes on serving the rest of its transaction through the dirty-set patcher,
   * exactly as it would for a projection built any other way. Without this, a post-load insert on the
   * still-open transaction would be handed to a closed build.
   */
  private @Nullable ProjectionBulkLoad activeBulkLoad() {
    final ProjectionBulkLoad load = bulkLoad;
    if (load == null) {
      return null;
    }
    if (load.isFinished()) {
      bulkLoad = null;
      return null;
    }
    return load;
  }

  private static @Nullable ProjectionBulkLoad resolveBulkLoad(final IndexDef indexDef,
      final NodeReadOnlyTrx maintenanceTrx) {
    if (!ProjectionBulkLoad.anyActive()) {
      return null;
    }
    return ProjectionBulkLoad.active(maintenanceTrx.getResourceSession().getResourceConfig().getResource().toString(),
        indexDef.getID(), maintenanceTrx);
  }

  @Override
  public void transactionAborted() {
    final ProjectionBulkLoad load = bulkLoad;
    bulkLoad = null;
    try {
      if (load != null) {
        load.abort();
      }
    } finally {
      // A cached controller can outlive the write transaction. Drop every potentially large,
      // transaction-owned memo/batch now rather than waiting for the controller itself to be evicted.
      dirtyRecordKeys = null;
      dirtyColumnWordsByRecord = null;
      rootProvenanceByRecord = null;
      pendingStructuralRecords = null;
      structuralDeltas = null;
      pendingStructuralDeletes = null;
      structuralOrderDirectory = null;
      orderRelabels = null;
      orderRelabelSink = null;
      resolvedRecordMemo = null;
      deepWalkVisited = null;
      arrayRootInstances = null;
    }
  }

  /** Catalogue id of the definition this listener maintains. */
  public int indexDefId() {
    return indexDef.getID();
  }

  @Override
  public void beforeStructuralChange(final long movedNodeKey) {
    if (activeBulkLoad() != null) {
      throw new IllegalStateException("Subtree move rejected: projection index " + indexDef.getID()
          + " is being built by the running load; load-time projection construction is append-only");
    }
    if (invalidated) {
      return;
    }
    if (!seeded && skipExplicitlyAbandonedProjection()) {
      return;
    }
    if (!seeded) {
      seed();
    }
    if (pendingStructuralRecords != null) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " received nested structural changes");
    }
    applyPendingMaintenance();
    pendingStructuralRecords = collectStructuralRange(movedNodeKey);
  }

  @Override
  public void afterStructuralChange(final long movedNodeKey) {
    try {
      if (invalidated) {
        return;
      }
      final StructuralRange before = pendingStructuralRecords;
      pendingStructuralRecords = null;
      if (before == null) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " did not observe the start of structural change " + movedNodeKey);
      }
      resolvedRecordMemo = null;
      arrayRootInstances = null;
      relabelStructuralOrder(movedNodeKey);
      final StructuralRange after = collectStructuralRange(movedNodeKey);
      removeStructuralExits(before);
      applyStructuralEntries(movedNodeKey, after);
      if (before.enclosingRecordKey() >= 0) {
        markDirty(before.enclosingRecordKey(), allColumnWords);
      }
      if (after.enclosingRecordKey() >= 0 && after.enclosingRecordKey() != before.enclosingRecordKey()) {
        markDirty(after.enclosingRecordKey(), allColumnWords);
      }
      applyPendingMaintenance();
    } catch (final RuntimeException | Error failure) {
      failMaintenance(failure);
      throw failure;
    }
  }

  @Override
  public void structuralChangeAborted(final long movedNodeKey) {
    pendingStructuralRecords = null;
    resolvedRecordMemo = null;
    arrayRootInstances = null;
  }

  private StructuralRange collectStructuralRange(final long movedNodeKey) {
    long enclosingRecordKey = NO_RECORD_KEY;
    long firstContainedRecordKey = NO_RECORD_KEY;
    long lastContainedRecordKey = NO_RECORD_KEY;
    long containedRecordCount = 0L;
    int recordsSinceMemoReset = 0;
    long currentKey = movedNodeKey;
    for (;;) {
      final ImmutableNode node = readNode(currentKey);
      if (!(node instanceof final StructNode structural)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot read moved subtree node " + currentKey);
      }
      final long pathNodeKey = pathNodeKeyOf(node);
      if (pathNodeKey > 0 && !relevantPcrs.contains(pathNodeKey) && !irrelevantPcrs.contains(pathNodeKey)) {
        classifyUnseenPcr(pathNodeKey);
      }
      final long recordKey = resolveRecordKey(currentKey, node.getKind(), node.getParentKey(), pathNodeKey);
      if (recordKey >= 0) {
        if (recordKey != currentKey) {
          enclosingRecordKey = recordKey;
          return new StructuralRange(enclosingRecordKey, NO_RECORD_KEY, NO_RECORD_KEY, 0L);
        }
        if (firstContainedRecordKey < 0) {
          firstContainedRecordKey = recordKey;
        }
        lastContainedRecordKey = recordKey;
        containedRecordCount++;
        if (++recordsSinceMemoReset == STRUCTURAL_BATCH_SIZE) {
          resolvedRecordMemo = null;
          arrayRootInstances = null;
          recordsSinceMemoReset = 0;
        }
        currentKey = nextNodeAfterSubtreeWithin(currentKey, movedNodeKey);
        if (currentKey < 0) {
          return new StructuralRange(enclosingRecordKey, firstContainedRecordKey, lastContainedRecordKey,
              containedRecordCount);
        }
        continue;
      } else if (recordKey == UNRESOLVED) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot attribute moved subtree node " + currentKey);
      }
      if (structural.hasFirstChild()) {
        currentKey = structural.getFirstChildKey();
        continue;
      }
      long walkKey = currentKey;
      StructNode walk = structural;
      while (walkKey != movedNodeKey && !walk.hasRightSibling()) {
        walkKey = walk.getParentKey();
        final ImmutableNode parent = readNode(walkKey);
        if (!(parent instanceof final StructNode parentStructural)) {
          throw new IllegalStateException(
              "Projection index " + indexDef.getID() + " cannot ascend moved subtree from " + currentKey);
        }
        walk = parentStructural;
      }
      if (walkKey == movedNodeKey) {
        return new StructuralRange(enclosingRecordKey, firstContainedRecordKey, lastContainedRecordKey,
            containedRecordCount);
      }
      currentKey = walk.getRightSiblingKey();
    }
  }

  private long nextNodeAfterSubtreeWithin(final long nodeKey, final long boundaryKey) {
    long current = nodeKey;
    for (;;) {
      final ImmutableNode node = readNode(current);
      if (!(node instanceof final StructNode structural)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot navigate structural subtree from " + current);
      }
      if (current == boundaryKey) {
        return NO_RECORD_KEY;
      }
      if (structural.hasRightSibling()) {
        return structural.getRightSiblingKey();
      }
      current = structural.getParentKey();
      if (current < 0 || current == boundaryKey) {
        return NO_RECORD_KEY;
      }
    }
  }

  private record StructuralRange(long enclosingRecordKey, long firstContainedRecordKey, long lastContainedRecordKey,
      long containedRecordCount) {
  }

  private record StructuralSnapshot(LongOpenHashSet contained, LongArrayList containedInDocumentOrder) {
  }

  private record StructuralDelta(StructuralSnapshot before, StructuralSnapshot after,
      Long2ObjectOpenHashMap<byte[]> afterRecordOrderLabels) {
  }

  private @Nullable ProjectionPersistedRecordLookup openPersistedRecordLookup() {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata metadata = readMetadata(storage);
    if (metadata == null) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " has no live metadata; incremental maintenance cannot proceed");
    }
    if (metadata.isStale()) {
      return null;
    }
    return new ProjectionPersistedRecordLookup(storage, ProjectionIndexFences.open(storage, metadata.rowGroupCount()),
        ProjectionRecordLocator.open(storage));
  }

  private void deriveOrdinaryInsertionPositions(final LongOpenHashSet insertions,
      final Long2ObjectOpenHashMap<InsertionPosition> positions) {
    for (final LongIterator iterator = insertions.iterator(); iterator.hasNext();) {
      final long candidate = iterator.nextLong();
      if (positions.containsKey(candidate)) {
        continue;
      }
      final SirixDeweyID label = structuralOrderDirectory().fullLabel(candidate, this::readNode, orderRelabelSink());
      positions.put(candidate, new InsertionPosition(label.toBytes(), false, 0L));
    }
  }

  private void removeStructuralExits(final StructuralRange before) {
    if (before.firstContainedRecordKey() < 0) {
      return;
    }
    ProjectionPersistedRecordLookup lookup = openPersistedRecordLookup();
    if (lookup == null) {
      return;
    }
    final LongArrayList exits = new LongArrayList(STRUCTURAL_BATCH_SIZE);
    int recordsInBatch = 0;
    long recordKey = before.firstContainedRecordKey();
    for (;;) {
      final long location = lookup.find(recordKey);
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot locate moved record " + recordKey);
      }
      final boolean last = recordKey == before.lastContainedRecordKey();
      final long nextRecord = last
          ? NO_RECORD_KEY
          : lookup.nextRecord(location);
      if (!isCurrentRecordRoot(recordKey)) {
        exits.add(recordKey);
      }
      recordsInBatch++;
      if (recordsInBatch == STRUCTURAL_BATCH_SIZE || last) {
        if (!exits.isEmpty()) {
          flushStructuralBatch(exits, null, null);
        }
        exits.clear();
        recordsInBatch = 0;
        resolvedRecordMemo = null;
        arrayRootInstances = null;
        if (!last) {
          lookup = openPersistedRecordLookup();
          if (lookup == null) {
            throw new IllegalStateException(
                "Projection index " + indexDef.getID() + " lost its live metadata during structural maintenance");
          }
        }
      }
      if (last) {
        return;
      }
      if (nextRecord < 0) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot reach the end of its moved record interval");
      }
      recordKey = nextRecord;
    }
  }

  private void applyStructuralEntries(final long movedNodeKey, final StructuralRange after) {
    if (after.firstContainedRecordKey() < 0) {
      return;
    }
    final ProjectionStructuralOrderDirectory.Accessor orderDirectory = structuralOrderDirectory();
    final LongArrayList entries = new LongArrayList(STRUCTURAL_BATCH_SIZE);
    final ObjectArrayList<byte[]> labels = new ObjectArrayList<>(STRUCTURAL_BATCH_SIZE);
    int recordsSinceMemoReset = 0;
    int pendingLabelBytes = 0;
    long recordOrdinal = 0L;
    long currentKey = movedNodeKey;
    for (;;) {
      final ImmutableNode node = readNode(currentKey);
      if (!(node instanceof final StructNode structural)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot read moved subtree node " + currentKey);
      }
      final long pathNodeKey = pathNodeKeyOf(node);
      final long recordKey = resolveRecordKey(currentKey, node.getKind(), node.getParentKey(), pathNodeKey);
      if (recordKey >= 0 && recordKey != currentKey) {
        return;
      }
      if (recordKey == currentKey) {
        final SirixDeweyID orderLabel = orderDirectory.fullLabel(recordKey, this::readNode, orderRelabelSink());
        final byte[] encodedLabel = orderLabel.toBytes();
        if (!entries.isEmpty()
            && Math.addExact(pendingLabelBytes, encodedLabel.length) > STRUCTURAL_BATCH_LABEL_BYTES) {
          flushStructuralBatch(null, entries, labels);
          entries.clear();
          labels.clear();
          pendingLabelBytes = 0;
        }
        recordOrdinal++;
        entries.add(recordKey);
        labels.add(encodedLabel);
        pendingLabelBytes = Math.addExact(pendingLabelBytes, encodedLabel.length);
        if (entries.size() == STRUCTURAL_BATCH_SIZE || pendingLabelBytes >= STRUCTURAL_BATCH_LABEL_BYTES) {
          flushStructuralBatch(null, entries, labels);
          entries.clear();
          labels.clear();
          pendingLabelBytes = 0;
        }
        if (++recordsSinceMemoReset == STRUCTURAL_BATCH_SIZE) {
          resolvedRecordMemo = null;
          arrayRootInstances = null;
          recordsSinceMemoReset = 0;
        }
        currentKey = nextNodeAfterSubtreeWithin(currentKey, movedNodeKey);
        if (currentKey < 0) {
          break;
        }
        continue;
      }
      if (recordKey == UNRESOLVED) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot attribute moved subtree node " + currentKey);
      }
      if (structural.hasFirstChild()) {
        currentKey = structural.getFirstChildKey();
        continue;
      }
      currentKey = nextNodeAfterSubtreeWithin(currentKey, movedNodeKey);
      if (currentKey < 0) {
        break;
      }
    }
    if (!entries.isEmpty()) {
      flushStructuralBatch(null, entries, labels);
    }
    if (recordOrdinal != after.containedRecordCount()) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " structural record count changed during bounded maintenance");
    }
  }

  private void flushStructuralBatch(final @Nullable LongArrayList exits, final @Nullable LongArrayList entries,
      final @Nullable ObjectArrayList<byte[]> entryLabels) {
    final LongOpenHashSet beforeKeys = new LongOpenHashSet(STRUCTURAL_BATCH_SIZE);
    final LongOpenHashSet afterKeys = new LongOpenHashSet(STRUCTURAL_BATCH_SIZE);
    final LongArrayList beforeOrder = new LongArrayList(STRUCTURAL_BATCH_SIZE);
    final LongArrayList afterOrder = new LongArrayList(STRUCTURAL_BATCH_SIZE);
    final Long2ObjectOpenHashMap<byte[]> labels = new Long2ObjectOpenHashMap<>(STRUCTURAL_BATCH_SIZE);
    final ProjectionPersistedRecordLookup lookup = openPersistedRecordLookup();
    if (lookup == null) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " lost its live metadata during structural maintenance");
    }
    if (exits != null) {
      for (int index = 0; index < exits.size(); index++) {
        final long recordKey = exits.getLong(index);
        beforeKeys.add(recordKey);
        beforeOrder.add(recordKey);
      }
    }
    if (entries != null) {
      if (entryLabels == null || entryLabels.size() != entries.size()) {
        throw new IllegalArgumentException("projection structural labels do not match their records");
      }
      for (int index = 0; index < entries.size(); index++) {
        final long recordKey = entries.getLong(index);
        if (lookup.find(recordKey) != ProjectionPersistedRecordLookup.ABSENT) {
          beforeKeys.add(recordKey);
          beforeOrder.add(recordKey);
        }
        afterKeys.add(recordKey);
        afterOrder.add(recordKey);
        labels.put(recordKey, entryLabels.get(index));
      }
    }
    if (beforeKeys.isEmpty() && afterKeys.isEmpty()) {
      return;
    }
    final StructuralSnapshot before = new StructuralSnapshot(beforeKeys, beforeOrder);
    final StructuralSnapshot after = new StructuralSnapshot(afterKeys, afterOrder);
    structuralDeltas = new ArrayList<>(1);
    structuralDeltas.add(new StructuralDelta(before, after, labels));
    for (final LongIterator iterator = beforeKeys.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      recordStructuralProvenance(recordKey, STRUCTURAL_EXIT);
      markDirty(recordKey, allColumnWords);
    }
    for (final LongIterator iterator = afterKeys.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      recordStructuralProvenance(recordKey, STRUCTURAL_ENTER);
      markDirty(recordKey, allColumnWords);
    }
    applyPendingMaintenance();
    resolvedRecordMemo = null;
    arrayRootInstances = null;
  }

  private void recordStructuralProvenance(final long recordKey, final byte flag) {
    if (rootProvenanceByRecord == null) {
      rootProvenanceByRecord = new Long2ByteOpenHashMap();
    }
    rootProvenanceByRecord.put(recordKey, (byte) (rootProvenanceByRecord.get(recordKey) | flag));
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    onChange(type, node.getNodeKey(), node.getKind(), node.getParentKey(), pathNodeKey);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    onChange(type, nodeKey, nodeKind, PARENT_UNKNOWN, pathNodeKey);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long parentKey, final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    onChange(type, nodeKey, nodeKind, parentKey, pathNodeKey);
  }

  private void onChange(final IndexController.ChangeType type, final long nodeKey, final NodeKind kind,
      final long parentKey, final long pathNodeKey) {
    try {
      onChangeInternal(type, nodeKey, kind, parentKey, pathNodeKey);
    } catch (final RuntimeException | Error failure) {
      failMaintenance(failure);
      throw failure;
    }
  }

  private void onChangeInternal(final IndexController.ChangeType type, final long nodeKey, final NodeKind kind,
      final long parentKey, final long pathNodeKey) {
    final ProjectionBulkLoad load = activeBulkLoad();
    if (invalidated) {
      return;
    }
    if (pendingStructuralRecords != null) {
      if (pathNodeKey > 0 && !relevantPcrs.contains(pathNodeKey) && !irrelevantPcrs.contains(pathNodeKey)) {
        classifyUnseenPcr(pathNodeKey);
      }
      return;
    }
    if (load != null) {
      onChangeDuringBulkLoad(load, nodeKey, kind, parentKey, pathNodeKey);
      return;
    }
    if (!seeded && skipExplicitlyAbandonedProjection()) {
      return;
    }
    if (!seeded) {
      seed();
    }
    // NOTE: an EMPTY root-PCR set is fine here — the record set simply has
    // no instances (yet). A change creating one arrives with a new matching
    // path class, which classifyUnseenPcr reseeds as a root.
    if (pathNodeKey > 0 && !relevantPcrs.contains(pathNodeKey)) {
      // Positive knowledge wins: a class can enter the negative cache first and be PROVEN
      // relevant later (an ancestor registered when a younger column class appears), so
      // relevance is consulted before irrelevance.
      if (irrelevantPcrs.contains(pathNodeKey) || !classifyUnseenPcr(pathNodeKey)) {
        return;
      }
    }
    // pathNodeKey <= 0 (kinds without a PCR, e.g. plain OBJECT provenance)
    // falls through — the ancestor walk classifies it exactly.
    final long recordKey = resolveRecordKey(nodeKey, kind, parentKey, pathNodeKey);
    if (recordKey == NOT_UNDER_RECORD_SET) {
      return;
    }
    if (recordKey < 0) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " could not attribute changed node " + nodeKey + " to a record");
    }
    if (recordKey == nodeKey) {
      recordFirstRootEvent(recordKey, type);
      // Structural ordering is PER PROJECTED RECORD. A field of a record — thirteen of every
      // fourteen nodes on the ClickBench corpus — never reaches this point, so the ingestion hot
      // path performs no order-directory read or write for it.
      maintainStructuralOrder(type, nodeKey);
    }
    final long[] changedColumns = pathNodeKey > 0
        ? columnWordsByPcr.get(pathNodeKey)
        : null;
    markDirty(recordKey, changedColumns == null
        ? allColumnWords
        : changedColumns);
  }

  private void recordFirstRootEvent(final long recordKey, final IndexController.ChangeType type) {
    if (rootProvenanceByRecord == null) {
      rootProvenanceByRecord = new Long2ByteOpenHashMap();
    }
    final byte prior = rootProvenanceByRecord.get(recordKey);
    if ((prior & FIRST_ROOT_EVENT_MASK) != 0) {
      return;
    }
    final byte event = type == IndexController.ChangeType.INSERT
        ? FIRST_ROOT_INSERT
        : FIRST_ROOT_DELETE;
    rootProvenanceByRecord.put(recordKey, (byte) (prior | event));
  }

  /**
   * Load-time build attribution: decide which RECORD the changed node belongs to and tell the build,
   * which closes the previous record when the answer changes.
   *
   * <p>
   * Nothing is extracted here. Extraction moves the cursor, and a notification fires in the middle of
   * the shredder's insert — the shredder would resume from wherever the extractor left the cursor.
   * Closed records are extracted at the commit hook instead, which is where cursor movement is
   * already safe.
   *
   * <h2>Cost</h2> The overwhelmingly common notification — a field of the record being written — is
   * rejected before any read when the record set is array-rooted (a fused named node is never an
   * array element). A new record under the known array root is likewise classified without a read
   * because the JSON write transaction supplies its parent key. Legacy primitive callers that omit
   * the parent retain the exact old fallback: one self-read recovers it before the same walk. No memo
   * is kept: at load scale the maintenance path's node→record memo would grow to its million-entry
   * cap and stay there.
   */
  private void onChangeDuringBulkLoad(final ProjectionBulkLoad load, final long nodeKey, final NodeKind kind,
      final long parentKey, final long pathNodeKey) {
    if (!seeded) {
      seed();
    }
    if (!couldBeRecordRootDuringBulkLoad(kind, pathNodeKey)) {
      if (ProjectionBulkLoad.diagnosticsEnabled()) {
        load.countClassification(bulkSkipsNamedKinds && kind.playsObjectKeyRole()
            ? ProjectionBulkLoad.DIAG_SKIPPED_NAMED_KIND
            : ProjectionBulkLoad.DIAG_SKIPPED_PATH_CLASS);
      }
      return;
    }
    final long recordKey = resolveRecordKeyDuringBulkLoad(load, nodeKey, kind, parentKey, pathNodeKey);
    if (ProjectionBulkLoad.diagnosticsEnabled()) {
      load.countClassification(recordKey >= 0
          ? ProjectionBulkLoad.DIAG_OBSERVED
          : recordKey == UNRESOLVED
              ? ProjectionBulkLoad.DIAG_UNRESOLVED
              : ProjectionBulkLoad.DIAG_NOT_UNDER_RECORD_SET);
    }
    if (recordKey < 0) {
      // NOT_UNDER_RECORD_SET (a container above the record set, or the record-set array itself) and
      // UNRESOLVED (a chain that cannot be read) both mean "no record starts here". An unreadable
      // chain cannot silently lose a record: the end-of-load row count would not add up, and the
      // build fails loudly there rather than persisting a short index.
      return;
    }
    load.observeRecord(recordKey);
  }

  /**
   * Cheap pre-filter for {@link #onChangeDuringBulkLoad}: whether this notification could possibly
   * announce a new record root. Two facts do the work — a record root's notification carries the
   * record set's own path class (the enclosing one, which for an array element IS the array's), and
   * when the declared root path ends in an array step the record roots are array ELEMENTS, which the
   * fused {@code OBJECT_NAMED_*} kinds never are.
   *
   * <p>
   * A pathNodeKey of zero or less carries no provenance (kinds without a path class) and is never
   * filtered out — the walk classifies it exactly.
   */
  private boolean couldBeRecordRootDuringBulkLoad(final NodeKind kind, final long pathNodeKey) {
    if (bulkSkipsNamedKinds && kind.playsObjectKeyRole()) {
      return false;
    }
    if (pathNodeKey <= 0) {
      return true;
    }
    if (rootPcrs.contains(pathNodeKey)) {
      return true;
    }
    if (irrelevantPcrs.contains(pathNodeKey)) {
      return false;
    }
    if (relevantPcrs.contains(pathNodeKey)) {
      // A field's own path class: relevant to maintenance, but it can never BE a record root.
      return false;
    }
    // Unseen path class — it may be the record set's, appearing for the first time (on an empty
    // resource EVERY class is unseen, the root's included). Classification must prove the FULL
    // claim before caching: a class that roots no record may still carry a projection column
    // (a field path first materializing mid-load), and caching it as irrelevant here would make
    // the post-load maintenance path drop that field's changes for the rest of the transaction.
    // classifyUnseenPcr registers matching columns, reseeds rootPcrs on a root match, and only
    // caches irrelevance for classes that match neither.
    if (classifyUnseenPcr(pathNodeKey)) {
      return rootPcrs.contains(pathNodeKey);
    }
    return false;
  }

  /**
   * Ancestor walk for the load-time build: climb until the open record, a known record-set array
   * instance, or a node at a record-set root path class is crossed. {@code parentKey} is normally
   * supplied by the writer; {@link #PARENT_UNKNOWN} preserves the original self-read fallback for
   * legacy primitive callers.
   *
   * @return the record's nodeKey, or {@link #NOT_UNDER_RECORD_SET} / {@link #UNRESOLVED}
   */
  private long resolveRecordKeyDuringBulkLoad(final ProjectionBulkLoad load, final long nodeKey, final NodeKind kind,
      long parentKey, final long pathNodeKey) {
    final boolean atRootPcr = pathNodeKey > 0 && rootPcrs.contains(pathNodeKey);
    if (atRootPcr && isArrayLike(kind)) {
      // The record SET's array instance itself — it has no row, but remembering it is what lets every
      // one of its elements be recognised as a record in one set lookup, and what makes the
      // end-of-load row count checkable.
      load.noteArrayRootInstance(nodeKey, maintenanceTrx);
      return NOT_UNDER_RECORD_SET;
    }
    if (atRootPcr && maintenanceTrx instanceof XmlNodeReadOnlyTrx && kind == NodeKind.ELEMENT) {
      final ImmutableNode self = readNode(nodeKey);
      if (self != null && rootPcrs.contains(pathNodeKeyOf(self))) {
        return nodeKey;
      }
    }
    if (parentKey == PARENT_UNKNOWN) {
      final ImmutableNode self = readNode(nodeKey);
      if (self == null) {
        return UNRESOLVED;
      }
      parentKey = self.getParentKey();
    }
    final long openRecordKey = load.currentRecordKey();
    long childKey = nodeKey;
    int chainLength = 0;
    resetDeepWalkVisited();
    for (;;) {
      if (chainLength == walkChain.length) {
        walkChain = Arrays.copyOf(walkChain, Math.multiplyExact(walkChain.length, 2));
      }
      walkChain[chainLength++] = childKey;
      if (hasAncestorCycle(chainLength, parentKey)) {
        return UNRESOLVED;
      }
      if (parentKey <= 0) {
        // Nothing encloses this node. It is the record itself only when it stands at the root path —
        // a single-record root, whose parent lies outside the record set.
        return atRootPcr
            ? nodeKey
            : NOT_UNDER_RECORD_SET;
      }
      if (parentKey == openRecordKey) {
        return openRecordKey; // the common case: a field of the record being shredded
      }
      if (load.isArrayRootInstance(parentKey)) {
        return childKey;
      }
      final ImmutableNode parent = readNode(parentKey);
      if (parent == null) {
        return UNRESOLVED;
      }
      final long parentPcr = pathNodeKeyOf(parent);
      if (parentPcr > 0 && rootPcrs.contains(parentPcr)) {
        if (isArrayLike(parent.getKind())) {
          load.noteArrayRootInstance(parentKey, maintenanceTrx);
          return childKey;
        }
        return parentKey; // a non-array root IS the record
      }
      childKey = parentKey;
      parentKey = parent.getParentKey();
    }
  }

  /**
   * One-time PCR-set seeding, deferred to the first notification so transactions that never write pay
   * nothing. Seeds the root PCRs, their ancestors (structural deletes of an enclosing container drop
   * the whole record set), and the field PCRs. Primitive iteration throughout — no boxing.
   */
  private void seed() {
    seeded = true;
    rootPcrs = new LongOpenHashSet();
    relevantPcrs = new LongOpenHashSet();
    irrelevantPcrs = new LongOpenHashSet();
    columnWordsByPcr = new Long2ObjectOpenHashMap<>();
    final int columnCount = indexDef.getProjectionFields().size();
    allColumnWords = new long[(columnCount + Long.SIZE - 1) / Long.SIZE];
    Arrays.fill(allColumnWords, -1L);
    if (columnCount % Long.SIZE != 0) {
      allColumnWords[allColumnWords.length - 1] = (1L << (columnCount % Long.SIZE)) - 1L;
    }
    final LongSet roots = pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath()));
    // OUTERMOST FIRST. registerRootPcr detects an overlapping nested root by walking UP from the
    // new root to an already-registered one, which is exact for the incremental path (a path class
    // created now can never be the ancestor of one that already exists) but order-sensitive for a
    // batch. The set iterates in hash order, so a nested root seeded before its ancestor would see
    // an empty rootPcrs above it and the ancestor would never look down — the guard would fail OPEN
    // on exactly the overlap it exists to reject. Sorting by path level restores the precondition:
    // an ancestor has a strictly smaller level, so it is always registered first.
    final long[] orderedRoots = roots.toLongArray();
    if (orderedRoots.length > 1) {
      final long savedNodeKey = pathSummary.getNodeKey();
      final int[] levels = new int[orderedRoots.length];
      try {
        for (int index = 0; index < orderedRoots.length; index++) {
          if (!pathSummary.moveTo(orderedRoots[index])) {
            throw new IllegalStateException(
                "Projection index " + indexDef.getID() + " cannot resolve record-set path node " + orderedRoots[index]);
          }
          levels[index] = pathSummary.getLevel();
        }
      } finally {
        pathSummary.moveTo(savedNodeKey);
      }
      sortRootsByLevelAscending(orderedRoots, levels);
    }
    for (final long pcr : orderedRoots) {
      registerRootPcr(pcr);
    }
    int column = 0;
    for (final Path<QNm> fieldPath : indexDef.getProjectionFields()) {
      final LongSet fieldPcrs = pathSummary.getPCRsForPaths(Set.of(fieldPath));
      for (final LongIterator it = fieldPcrs.iterator(); it.hasNext();) {
        registerColumnPcrAndAncestors(it.nextLong(), column);
      }
      column++;
    }
  }

  private void registerColumnPcrAndAncestors(final long pathNodeKey, final int column) {
    long current = pathNodeKey;
    while (current > 0) {
      relevantPcrs.add(current);
      // A class classified before this column's path existed may sit in the negative cache
      // (e.g. /a seen before /a/b when /a/b is a projection field); relevance discovered later
      // must beat that verdict, or onChange keeps dropping the ancestor's notifications.
      irrelevantPcrs.remove(current);
      long[] words = columnWordsByPcr.get(current);
      if (words == null) {
        words = new long[allColumnWords.length];
        columnWordsByPcr.put(current, words);
      }
      words[column >>> 6] |= 1L << (column & 63);
      final PathNode node = pathSummary.getPathNodeForPathNodeKey(current);
      if (node == null) {
        break;
      }
      current = node.getParentKey();
    }
  }

  /**
   * Classify an unseen PCR (e.g. a brand-new field path created by this transaction) by whether its
   * ancestor chain crosses a record-set root, and cache the verdict so the hot path stays a single
   * set lookup. The new PCR may itself BE a new record-set root — an exact-path record set
   * re-appearing after removal, or a descendant pattern widening to another matching subtree — which
   * the seeded root PCRs cannot know: the root path is re-checked against the new path class and a
   * match RESEEDS the root set, so the new roots' records attribute normally. Stable node keys prove
   * identity only, not document position; apply-time maintenance derives position from the current
   * document neighbors and stores non-backbone rows through the sparse exception locator.
   */
  private boolean classifyUnseenPcr(final long pathNodeKey) {
    registerMatchingColumns(pathNodeKey);
    boolean relevant = columnWordsByPcr.containsKey(pathNodeKey);
    if (matchesRootPath(pathNodeKey)) {
      registerRootPcr(pathNodeKey);
      columnWordsByPcr.put(pathNodeKey, allColumnWords.clone());
      return true;
    }
    if (relevant) {
      relevantPcrs.add(pathNodeKey);
    } else {
      irrelevantPcrs.add(pathNodeKey);
    }
    return relevant;
  }

  /**
   * Stable insertion sort of the seeded root PCRs by ascending path level, keeping the parallel level
   * array in step. Root sets hold a handful of entries, so this beats a comparator-driven sort
   * outright: two primitive arrays, no boxing, no allocation, and it runs once per seeding.
   */
  private static void sortRootsByLevelAscending(final long[] roots, final int[] levels) {
    for (int index = 1; index < roots.length; index++) {
      final long root = roots[index];
      final int level = levels[index];
      int scan = index - 1;
      while (scan >= 0 && levels[scan] > level) {
        roots[scan + 1] = roots[scan];
        levels[scan + 1] = levels[scan];
        scan--;
      }
      roots[scan + 1] = root;
      levels[scan + 1] = level;
    }
  }

  private void registerRootPcr(final long pathNodeKey) {
    if (!rootPcrs.contains(pathNodeKey)) {
      final long savedNodeKey = pathSummary.getNodeKey();
      try {
        if (!pathSummary.moveTo(pathNodeKey)) {
          throw new IllegalStateException(
              "Projection index " + indexDef.getID() + " cannot resolve record-set path node " + pathNodeKey);
        }
        while (pathSummary.moveToParent()) {
          final long ancestor = pathSummary.getNodeKey();
          if (ancestor <= 0) {
            break;
          }
          if (rootPcrs.contains(ancestor)) {
            throw new IllegalStateException("Projection ROOT path '" + indexDef.getProjectionRootPath()
                + "' resolves to overlapping nested root matches (matched pathNodeKey " + pathNodeKey
                + " lies below matched root pathNodeKey " + ancestor + ")");
          }
        }
      } finally {
        pathSummary.moveTo(savedNodeKey);
      }
    }
    rootPcrs.add(pathNodeKey);
    relevantPcrs.add(pathNodeKey);
    PathNode node = pathSummary.getPathNodeForPathNodeKey(pathNodeKey);
    while (node != null) {
      final long parentKey = node.getParentKey();
      if (parentKey <= 0) {
        return;
      }
      relevantPcrs.add(parentKey);
      node = pathSummary.getPathNodeForPathNodeKey(parentKey);
    }
  }

  private void registerMatchingColumns(final long pathNodeKey) {
    final long savedNodeKey = pathSummary.getNodeKey();
    try {
      if (!pathSummary.moveTo(pathNodeKey)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot resolve path node " + pathNodeKey);
      }
      final Path<QNm> path = pathSummary.getPath();
      if (path == null) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot reconstruct path node " + pathNodeKey);
      }
      final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
      for (int column = 0; column < fieldPaths.size(); column++) {
        if (fieldPaths.get(column).matches(path)) {
          registerColumnPcrAndAncestors(pathNodeKey, column);
        }
      }
    } finally {
      pathSummary.moveTo(savedNodeKey);
    }
  }

  /**
   * Whether the (unseen) path class {@code pathNodeKey} matches the definition's root path (exact or
   * descendant pattern — {@code matches} covers both). Cursor-neutral; an unreadable or failing
   * reconstruction cannot be proven irrelevant, so it fails the owning transaction.
   */
  private boolean matchesRootPath(final long pathNodeKey) {
    final long savedNodeKey = pathSummary.getNodeKey();
    try {
      if (!pathSummary.moveTo(pathNodeKey)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot resolve path node " + pathNodeKey);
      }
      final Path<QNm> path = pathSummary.getPath();
      if (path == null) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " cannot reconstruct path node " + pathNodeKey);
      }
      return indexDef.getProjectionRootPath().matches(path);
    } finally {
      pathSummary.moveTo(savedNodeKey);
    }
  }

  /**
   * Resolve the RECORD a changed node belongs to: walk the ancestor chain (raw record reads — no
   * cursor movement) until a node at a record-set root PCR is crossed. Delete notifications fire
   * post-order BEFORE physical removal, so a deleted node's ancestors are still readable at listen
   * time.
   *
   * @return the record's nodeKey, or one of the negative verdicts ({@link #NOT_UNDER_RECORD_SET} /
   *         {@link #UNRESOLVED})
   */
  private long resolveRecordKey(final long nodeKey, final NodeKind kind, long parentKey, final long pathNodeKey) {
    final boolean atRootPcr = pathNodeKey > 0 && rootPcrs.contains(pathNodeKey);
    if (atRootPcr && isArrayLike(kind)) {
      // The record SET's array instance itself appeared or disappeared —
      // no row of its own. Its records are attributed individually: an
      // insert notifies the array BEFORE its records (pre-order), a
      // delete notifies the records BEFORE the array (post-order), so
      // every row lands in the dirty set through its own notification.
      return NOT_UNDER_RECORD_SET;
    }
    if (atRootPcr && maintenanceTrx instanceof XmlNodeReadOnlyTrx && kind == NodeKind.ELEMENT) {
      final ImmutableNode self = readNode(nodeKey);
      if (self != null && rootPcrs.contains(pathNodeKeyOf(self))) {
        if (resolvedRecordMemo == null) {
          resolvedRecordMemo = new Long2LongOpenHashMap();
          resolvedRecordMemo.defaultReturnValue(MEMO_MISS);
        }
        if (resolvedRecordMemo.size() < MEMO_CAP) {
          resolvedRecordMemo.put(nodeKey, nodeKey);
        }
        return nodeKey;
      }
    }
    // NOTE: `atRootPcr` alone must NOT be read as "this node IS the record". An INSERT notifies each
    // new node TWICE, and the first notification carries the pathNodeKey the cursor stood on — the
    // enclosing record's, i.e. the ROOT pcr — because the field's own path node does not exist yet:
    //
    // node=6004 kind=OBJECT_NAMED_BOOLEAN pcr=1(root) then node=6004 ... pcr=3(/[]/active)
    //
    // Taking the shortcut on that first notification made a FIELD its own record, and the extractor
    // then built an ALL-MISSING row for it — one surplus row per field, indistinguishable downstream
    // from a record whose indexed fields are genuinely absent, and so a phantom null group in every
    // group-by. The ancestor walk is authoritative because it reads the stored parent chain rather
    // than a notification-supplied pcr; the shortcut survives only for the case the walk cannot
    // express — a single-record root, whose parent lies OUTSIDE the record set.
    if (resolvedRecordMemo == null) {
      resolvedRecordMemo = new Long2LongOpenHashMap();
      resolvedRecordMemo.defaultReturnValue(MEMO_MISS);
    }
    final long selfMemo = resolvedRecordMemo.get(nodeKey);
    if (selfMemo != MEMO_MISS) {
      return selfMemo;
    }
    long childKey = nodeKey;
    if (parentKey == PARENT_UNKNOWN) {
      final ImmutableNode self = readNode(nodeKey);
      if (self == null) {
        return UNRESOLVED;
      }
      parentKey = self.getParentKey();
    }
    int chainLength = 0;
    walkChain[chainLength++] = nodeKey;
    resetDeepWalkVisited();
    for (;;) {
      if (hasAncestorCycle(chainLength, parentKey)) {
        return UNRESOLVED;
      }
      if (parentKey <= 0) {
        if (atRootPcr) {
          // Nothing encloses this node and it stands at the root path itself: a single-record root
          // (a fused object at the root path) IS the record. This is the ONLY reading of the
          // notified pcr the walk cannot reach on its own, and it is safe precisely because the walk
          // already proved there is no enclosing record — a field misreported at the root pcr always
          // resolves above instead.
          if (resolvedRecordMemo.size() < MEMO_CAP) {
            resolvedRecordMemo.put(nodeKey, nodeKey);
          }
          return nodeKey;
        }
        // Reached the document root without crossing a record-set root:
        // the change cannot affect any indexed row. (Deleting a container
        // ABOVE the record set is covered by the post-order notifications
        // of the record-set nodes themselves.) NOT memoized: a node outside
        // every record can still be an ancestor OF the record set, and its
        // descendants' walks must not inherit this verdict.
        return NOT_UNDER_RECORD_SET;
      }
      if (arrayRootInstances != null && arrayRootInstances.contains(parentKey)) {
        // Known array-like root instance — the record is the element we came from.
        memoizeChain(chainLength, childKey);
        return childKey;
      }
      final long ancestorMemo = resolvedRecordMemo.get(parentKey);
      if (ancestorMemo != MEMO_MISS) {
        // The parent is proven inside record R — so is the whole chain.
        memoizeChain(chainLength, ancestorMemo);
        return ancestorMemo;
      }
      final ImmutableNode parent = readNode(parentKey);
      if (parent == null) {
        return UNRESOLVED;
      }
      final long parentPcr = pathNodeKeyOf(parent);
      if (parentPcr > 0 && rootPcrs.contains(parentPcr)) {
        // Crossed the root: under an array-like root the record is the
        // element we came from; a non-array root IS the record.
        final boolean arrayRoot = isArrayLike(parent.getKind());
        final long recordKey = arrayRoot
            ? childKey
            : parentKey;
        if (arrayRoot) {
          if (arrayRootInstances == null) {
            arrayRootInstances = new LongOpenHashSet();
          }
          if (arrayRootInstances.size() < MEMO_CAP) {
            arrayRootInstances.add(parentKey);
          }
        }
        memoizeChain(chainLength, recordKey);
        if (recordKey == parentKey && resolvedRecordMemo.size() < MEMO_CAP) {
          resolvedRecordMemo.put(recordKey, recordKey);
        }
        return recordKey;
      }
      childKey = parentKey;
      if (chainLength == walkChain.length) {
        walkChain = Arrays.copyOf(walkChain, Math.multiplyExact(walkChain.length, 2));
      }
      walkChain[chainLength++] = parentKey;
      parentKey = parent.getParentKey();
    }
  }

  private boolean hasAncestorCycle(final int chainLength, final long parentKey) {
    if (parentKey <= 0 || chainLength < DEEP_WALK_CYCLE_THRESHOLD) {
      return false;
    }
    if (chainLength == DEEP_WALK_CYCLE_THRESHOLD) {
      if (deepWalkVisited == null) {
        deepWalkVisited = new LongOpenHashSet(chainLength * 2);
      }
      for (int i = 0; i < chainLength; i++) {
        if (!deepWalkVisited.add(walkChain[i])) {
          return true;
        }
      }
    }
    return !deepWalkVisited.add(parentKey);
  }

  private void resetDeepWalkVisited() {
    if (deepWalkVisited != null) {
      deepWalkVisited.clear();
    }
  }

  /** Memoize every walked chain node as lying inside {@code recordKey}. */
  private void memoizeChain(final int chainLength, final long recordKey) {
    if (resolvedRecordMemo.size() >= MEMO_CAP) {
      return;
    }
    for (int i = 0; i < chainLength; i++) {
      resolvedRecordMemo.put(walkChain[i], recordKey);
    }
  }

  private static boolean isArrayLike(final NodeKind kind) {
    return kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_NAMED_ARRAY;
  }

  /**
   * PathNodeKey of a raw record, mirroring the rtx dispatch: only name-carrying and array nodes have
   * one.
   */
  private static long pathNodeKeyOf(final ImmutableNode node) {
    if (node instanceof final NameNode nameNode) {
      return nameNode.getPathNodeKey();
    }
    if (node instanceof final ArrayNode arrayNode) {
      return arrayNode.getPathNodeKey();
    }
    return -1L;
  }

  private @Nullable ImmutableNode readNode(final long nodeKey) {
    final DataRecord record = storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    return record instanceof final ImmutableNode node
        ? node
        : null;
  }

  private ProjectionStructuralOrderDirectory.Accessor structuralOrderDirectory() {
    ProjectionStructuralOrderDirectory.Accessor directory = structuralOrderDirectory;
    if (directory == null) {
      directory =
          ProjectionStructuralOrderDirectory.open(new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID()));
      structuralOrderDirectory = directory;
    }
    return directory;
  }

  /**
   * Retire or un-retire a record's order slot. Labels themselves are NOT minted here: the directory
   * mints one lazily the first time a record's row label is needed, which keeps this notification —
   * the ingestion hot path — free of document reads and HOT-trie writes.
   */
  /**
   * The ONE sink every order-label mint in this listener goes through, so no path can be left with
   * its bounded rebalance unarmed. A re-spread sibling is remembered here and folded into the next
   * apply pass as a move, which is what rewrites its persisted row-group order label.
   */
  private ProjectionStructuralOrderDirectory.RelabelSink orderRelabelSink() {
    ProjectionStructuralOrderDirectory.RelabelSink sink = orderRelabelSink;
    if (sink == null) {
      sink = new ProjectionStructuralOrderDirectory.RelabelSink() {
        @Override
        public boolean canRelabel(final long nodeKey) {
          // Only a record carries its own row: a labelled CONTAINER is a shared prefix of every
          // record below it and must never be re-spread.
          return isCurrentRecordRoot(nodeKey);
        }

        @Override
        public void relabelled(final long nodeKey, final SirixDeweyID localLabel) {
          LongOpenHashSet relabels = orderRelabels;
          if (relabels == null) {
            relabels = new LongOpenHashSet();
            orderRelabels = relabels;
          }
          relabels.add(nodeKey);
        }
      };
      orderRelabelSink = sink;
    }
    return sink;
  }

  private void maintainStructuralOrder(final IndexController.ChangeType type, final long nodeKey) {
    if (type == IndexController.ChangeType.DELETE) {
      stageStructuralDelete(nodeKey);
      return;
    }
    cancelStructuralDelete(nodeKey);
    drainStructuralDeleteCandidates(false);
  }

  private void stageStructuralDelete(final long nodeKey) {
    drainStructuralDeleteCandidates(false);
    LongOpenHashSet candidates = pendingStructuralDeletes;
    if (candidates == null) {
      candidates = new LongOpenHashSet(STRUCTURAL_BATCH_SIZE);
      pendingStructuralDeletes = candidates;
    }
    if (!candidates.contains(nodeKey) && candidates.size() == STRUCTURAL_BATCH_SIZE) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " structural delete staging exceeded its bounded capacity");
    }
    candidates.add(nodeKey);
  }

  private void cancelStructuralDelete(final long nodeKey) {
    final LongOpenHashSet candidates = pendingStructuralDeletes;
    if (candidates == null) {
      return;
    }
    candidates.remove(nodeKey);
    if (candidates.isEmpty()) {
      pendingStructuralDeletes = null;
    }
  }

  private void drainStructuralDeleteCandidates(final boolean discardLiveCandidates) {
    final LongOpenHashSet candidates = pendingStructuralDeletes;
    if (candidates == null) {
      return;
    }
    for (final LongIterator iterator = candidates.iterator(); iterator.hasNext();) {
      final long nodeKey = iterator.nextLong();
      final DataRecord record = storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      if (record == null) {
        structuralOrderDirectory().remove(nodeKey);
        iterator.remove();
      } else if (!(record instanceof ImmutableNode)) {
        throw new IllegalStateException("Projection index " + indexDef.getID()
            + " found a non-node document record while retiring structural order for " + nodeKey);
      } else if (discardLiveCandidates) {
        iterator.remove();
      }
    }
    if (candidates.isEmpty()) {
      pendingStructuralDeletes = null;
    }
  }

  private void relabelStructuralOrder(final long nodeKey) {
    final ImmutableNode node = readNode(nodeKey);
    if (!(node instanceof StructNode)) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " cannot relabel moved structural node " + nodeKey);
    }
    structuralOrderDirectory().relabelDisplaced(nodeKey, this::readNode, orderRelabelSink());
  }

  private void markDirty(final long recordKey, final long[] changedColumnWords) {
    if (dirtyRecordKeys == null) {
      dirtyRecordKeys = new LongOpenHashSet();
      dirtyColumnWordsByRecord = new Long2ObjectOpenHashMap<>();
    }
    boolean changed = dirtyRecordKeys.add(recordKey);
    long[] recordWords = dirtyColumnWordsByRecord.get(recordKey);
    if (recordWords == null) {
      recordWords = changedColumnWords.clone();
      dirtyColumnWordsByRecord.put(recordKey, recordWords);
      changed = true;
    } else {
      for (int word = 0; word < recordWords.length; word++) {
        final long merged = recordWords[word] | changedColumnWords[word];
        changed |= merged != recordWords[word];
        recordWords[word] = merged;
      }
    }
    if (changed) {
      maintenanceEpoch++;
    }
  }

  /**
   * Monotone epoch of this listener's maintenance state — changes whenever the pending dirty set
   * changes, the projection is invalidated, or an apply pass rewrites leaves. Wtx-serving callers
   * cache decoded handles against it: equal epoch (same listener instance) ⇒ the persisted leaves are
   * byte-identical to when the handle was decoded.
   */
  public long maintenanceEpoch() {
    return maintenanceEpoch;
  }

  public record MaintenanceTelemetry(long commits, long dirtyRecords, long rowGroupsRead, long rowGroupsWritten,
      long dictionarySegments, long fenceChunksRead, long fenceChunksWritten, long setChunksRead, long setChunksWritten,
      long bloomRowGroupsRead, long bloomChunksWritten, long metadataReads, long metadataWrites, long dictionaryProbes,
      long storageReads, long storageWrites, long allocatorAllocations, long allocatorReleases, long tilReads,
      long tilWrites, long nativeAllocations, long nativeReleases, long asyncSubmissions, long asyncCompletions,
      long bytesRead, long bytesWritten, long fullRebuilds) {

    public long operations() {
      return Math.addExact(
          Math.addExact(Math.addExact(Math.addExact(storageReads, storageWrites),
              Math.addExact(allocatorAllocations, allocatorReleases)), Math.addExact(tilReads, tilWrites)),
          Math.addExact(Math.addExact(nativeAllocations, nativeReleases),
              Math.addExact(asyncSubmissions, asyncCompletions)));
    }
  }

  /**
   * Logical projection I/O for the most recently completed maintenance pass. Segment counts are
   * independent of cache hits: they describe which persistent units the algorithm requested, making a
   * one-cell locality assertion deterministic across hot and cold test runs.
   */
  public record MaintenanceLocality(int fullRowGroupsRead, int descriptorsRead, int keySegmentsRead,
      int bodySegmentsRead, int dictionarySegmentsRead, int columnSegmentsEncoded, int columnSegmentsWritten,
      int descriptorsWritten, int rowGroupsColumnPatched, int documentNeighborNodesRead) {
    private static final MaintenanceLocality EMPTY = new MaintenanceLocality(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  /** Snapshot of the latest completed pass; immutable and safe to retain. */
  public static MaintenanceLocality lastMaintenanceLocality() {
    return LAST_MAINTENANCE_LOCALITY;
  }

  private static final class MaintenanceLocalityAccumulator {
    private int fullRowGroupsRead;
    private int descriptorsRead;
    private int keySegmentsRead;
    private int bodySegmentsRead;
    private int dictionarySegmentsRead;
    private int columnSegmentsEncoded;
    private int columnSegmentsWritten;
    private int descriptorsWritten;
    private int rowGroupsColumnPatched;
    private int documentNeighborNodesRead;

    private MaintenanceLocality snapshot() {
      return new MaintenanceLocality(fullRowGroupsRead, descriptorsRead, keySegmentsRead, bodySegmentsRead,
          dictionarySegmentsRead, columnSegmentsEncoded, columnSegmentsWritten, descriptorsWritten,
          rowGroupsColumnPatched, documentNeighborNodesRead);
    }
  }

  public static MaintenanceTelemetry maintenanceTelemetry() {
    return maintenanceTelemetry(HftBoundaryTelemetry.snapshot());
  }

  public static MaintenanceTelemetry maintenanceTelemetry(final HftBoundaryTelemetry.Snapshot boundaries) {
    Objects.requireNonNull(boundaries);
    return new MaintenanceTelemetry(HFT_COMMITS.get(), HFT_DIRTY_RECORDS.get(), HFT_ROW_GROUPS_READ.get(),
        HFT_ROW_GROUPS_WRITTEN.get(), HFT_DICTIONARY_SEGMENTS.get(), HFT_FENCE_CHUNKS_READ.get(),
        HFT_FENCE_CHUNKS_WRITTEN.get(), HFT_SET_CHUNKS_READ.get(), HFT_SET_CHUNKS_WRITTEN.get(),
        HFT_BLOOM_ROW_GROUPS_READ.get(), HFT_BLOOM_CHUNKS_WRITTEN.get(), HFT_METADATA_READS.get(),
        HFT_METADATA_WRITES.get(), HFT_DICTIONARY_PROBES.get(), boundaries.storageReads(), boundaries.storageWrites(),
        boundaries.allocatorAllocations(), boundaries.allocatorReleases(), boundaries.tilReads(),
        boundaries.tilWrites(), boundaries.nativeAllocations(), boundaries.nativeReleases(),
        boundaries.asyncSubmissions(), boundaries.asyncCompletions(), boundaries.bytesRead(), boundaries.bytesWritten(),
        HFT_FULL_REBUILDS.get());
  }

  public static void resetMaintenanceTelemetry() {
    LAST_MAINTENANCE_LOCALITY = MaintenanceLocality.EMPTY;
    HFT_COMMITS.set(0L);
    HFT_DIRTY_RECORDS.set(0L);
    HFT_ROW_GROUPS_READ.set(0L);
    HFT_ROW_GROUPS_WRITTEN.set(0L);
    HFT_DICTIONARY_SEGMENTS.set(0L);
    HFT_FENCE_CHUNKS_READ.set(0L);
    HFT_FENCE_CHUNKS_WRITTEN.set(0L);
    HFT_SET_CHUNKS_READ.set(0L);
    HFT_SET_CHUNKS_WRITTEN.set(0L);
    HFT_BLOOM_ROW_GROUPS_READ.set(0L);
    HFT_BLOOM_CHUNKS_WRITTEN.set(0L);
    HFT_METADATA_READS.set(0L);
    HFT_METADATA_WRITES.set(0L);
    HFT_DICTIONARY_PROBES.set(0L);
    HFT_FULL_REBUILDS.set(0L);
    HftBoundaryTelemetry.reset();
  }

  public static void printMaintenanceTelemetry() {
    printMaintenanceTelemetry(HftBoundaryTelemetry.snapshot());
  }

  public static void printMaintenanceTelemetry(final HftBoundaryTelemetry.Snapshot boundaries) {
    final MaintenanceTelemetry telemetry = maintenanceTelemetry(boundaries);
    System.out.printf(
        "# HFT_PROJECTION_MAINTENANCE commits=%d dirtyRecords=%d rowGroupsRead=%d "
            + "rowGroupsWritten=%d dictionarySegments=%d fenceChunksRead=%d fenceChunksWritten=%d "
            + "setChunksRead=%d setChunksWritten=%d bloomRowGroupsRead=%d bloomChunksWritten=%d "
            + "metadataReads=%d metadataWrites=%d dictionaryProbes=%d storageReads=%d storageWrites=%d "
            + "allocatorAllocations=%d allocatorReleases=%d tilReads=%d tilWrites=%d nativeAllocations=%d "
            + "nativeReleases=%d asyncSubmissions=%d asyncCompletions=%d operations=%d bytesRead=%d bytesWritten=%d "
            + "fullRebuilds=%d%n",
        telemetry.commits(), telemetry.dirtyRecords(), telemetry.rowGroupsRead(), telemetry.rowGroupsWritten(),
        telemetry.dictionarySegments(), telemetry.fenceChunksRead(), telemetry.fenceChunksWritten(),
        telemetry.setChunksRead(), telemetry.setChunksWritten(), telemetry.bloomRowGroupsRead(),
        telemetry.bloomChunksWritten(), telemetry.metadataReads(), telemetry.metadataWrites(),
        telemetry.dictionaryProbes(), telemetry.storageReads(), telemetry.storageWrites(),
        telemetry.allocatorAllocations(), telemetry.allocatorReleases(), telemetry.tilReads(), telemetry.tilWrites(),
        telemetry.nativeAllocations(), telemetry.nativeReleases(), telemetry.asyncSubmissions(),
        telemetry.asyncCompletions(), telemetry.operations(), telemetry.bytesRead(), telemetry.bytesWritten(),
        telemetry.fullRebuilds());
  }

  /**
   * Apply the collected changes to the persisted projection. Invoked once per commit through the
   * uniform {@link ChangeListener} lifecycle ({@link IndexController#applyPendingIndexMaintenance()})
   * BEFORE page serialization, so all writes ride the committing transaction. Any attribution or
   * persistent-unit inconsistency fails before publication.
   */
  @Override
  public void beforeCommit() {
    beforeCommit(false);
  }

  @Override
  public void beforeCommit(final boolean finalCommit) {
    awaitDurablePredecessor();
    final ProjectionBulkLoad load = activeBulkLoad();
    if (load != null) {
      // Intermediate commits drain the records closed so far into the build (full leaves are already
      // in the sub-tree and ride this commit); the final commit additionally writes the dictionaries,
      // the fingerprint blocks, the fences and the metadata that replaces the tombstone, after which
      // activeBulkLoad() hands this listener back to ordinary maintenance.
      try {
        drainStructuralDeleteCandidates(true);
        if (finalCommit) {
          load.finish(storageEngineWriter, pathSummary, maintenanceTrx, maintenanceTrx.getRevisionNumber());
        } else {
          load.drain(storageEngineWriter, pathSummary, maintenanceTrx);
        }
      } catch (final GlobalDictionaryBudgetExceededException tooBig) {
        abandonForOversizedDictionary(load, tooBig);
      } catch (final RuntimeException | Error failure) {
        failMaintenance(failure);
        throw failure;
      }
      return;
    }
    applyPendingMaintenance();
  }

  /**
   * A resource-wide dictionary hit its bound: give up the projection, keep the load.
   *
   * <p>
   * The alternative is what this exists to prevent — the dictionary's arena doubling until the
   * collector owns every core and the load stops producing rows while still looking alive, which is
   * how a 100M ClickBench load spent two hours before it was killed. Abandoning is cheap and already
   * modelled: {@link ProjectionBulkLoad#abort()} drops the build without finalizing, so slot 0 keeps
   * the stale tombstone it has carried since the load began, and
   * {@link #invalidate(ProjectionIndexMetadata.StaleReason)} makes every later notification a no-op.
   * Both matter — without the invalidate the listener would fall back to the dirty-set patcher and
   * buffer a record key for every remaining row of the corpus, trading one unbounded structure for
   * another.
   * </p>
   */
  private void abandonForOversizedDictionary(final ProjectionBulkLoad load,
      final GlobalDictionaryBudgetExceededException tooBig) {
    // UNCONDITIONAL, not just LOGGER.warn (task #55). The shipped logback pins the root logger to
    // ERROR, so the warning below is invisible on a default deployment — and this is the one event
    // that turns a successful-looking load into a useless one. A silent degradation that the
    // loader then reports as "projection built" is how a 26-hour run gets measured on the wrong
    // code path. Anything a guard or an operator must see cannot travel by a suppressible channel.
    //
    // The quantity quoted is the one the guard WEIGHED, not merely the one it retains. Every
    // byte-budget check compares retention plus a reservation, so a line reading "retained X B ...
    // past its Y B budget" printed X < Y on every real breach and read as a misfiring guard; worse,
    // it left the operator no number to raise the budget to. A structural ceiling weighs no bytes at
    // all, so that case states the ceiling instead of inventing a comparison.
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
    LOGGER.warn("Projection index " + indexDef.getID() + " ABANDONED during the load. " + tooBig.getMessage() + " "
        + ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED.remedy(), tooBig);
    load.abort();
    invalidate(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED);
  }

  @Override
  public void beforePageFlush() {
    awaitDurablePredecessor();
    final ProjectionBulkLoad load = activeBulkLoad();
    if (load != null) {
      // Last chance to read these records: the flush hands their pages to the writer and the
      // revision they belong to is still uncommitted, so after it they can be read from nowhere.
      //
      // CURSOR SAFETY: this fires from the node-count check at the TOP of an insert, which is mid
      // shred and not at a record boundary — the insert goes on to read the cursor immediately
      // afterwards. drain() therefore saves the cursor and restores it before returning, and
      // ProjectionLoadTimeBuildEquivalenceTest#theShredSurvivesDrainsFiredMidRecord pins that by
      // reading every shredded record back through the row path.
      try {
        drainStructuralDeleteCandidates(true);
        load.drain(storageEngineWriter, pathSummary, maintenanceTrx);
      } catch (final GlobalDictionaryBudgetExceededException tooBig) {
        abandonForOversizedDictionary(load, tooBig);
      } catch (final RuntimeException | Error failure) {
        failMaintenance(failure);
        throw failure;
      }
      return;
    }
    applyPendingMaintenance();
  }

  private void awaitDurablePredecessor() {
    if (maintenanceTrx instanceof final NodeTrx nodeTrx) {
      nodeTrx.awaitPendingAsyncCommit();
    }
  }

  private void applyPendingMaintenance() {
    if (maintenanceFailed) {
      throw new IllegalStateException("Projection index " + indexDef.getID()
          + " maintenance previously failed; rollback is required before another commit");
    }
    try {
      drainStructuralDeleteCandidates(true);
    } catch (final RuntimeException | Error failure) {
      failMaintenance(failure);
      throw failure;
    }
    if (pendingStructuralRecords != null) {
      final IllegalStateException failure = new IllegalStateException("Projection index " + indexDef.getID()
          + " cannot publish maintenance during an incomplete structural change; rollback is required");
      failMaintenance(failure);
      throw failure;
    }
    LongOpenHashSet dirty = dirtyRecordKeys;
    Long2ObjectOpenHashMap<long[]> dirtyColumnWords = dirtyColumnWordsByRecord;
    final Long2ByteOpenHashMap rootProvenance = rootProvenanceByRecord;
    final ArrayList<StructuralDelta> completedStructuralDeltas = structuralDeltas;
    dirtyRecordKeys = null;
    dirtyColumnWordsByRecord = null;
    rootProvenanceByRecord = null;
    structuralDeltas = null;
    if (invalidated) {
      return;
    }
    // A rebalance raised by a subtree move re-spreads siblings BEFORE this pass starts and outside
    // any dirty-marking path, so the relabels have to be taken over here — ahead of the "nothing to
    // do" exit — or a move-only commit would leave those records' persisted order labels stale while
    // the directory already holds the new ones.
    if (dirty == null && orderRelabels != null && !orderRelabels.isEmpty()) {
      dirty = new LongOpenHashSet();
      dirtyColumnWords = new Long2ObjectOpenHashMap<>();
    }
    final LongOpenHashSet carriedRelabels = dirty == null
        ? LongOpenHashSet.of()
        : drainOrderRelabels(dirty, dirtyColumnWords);
    if (dirty == null || dirty.isEmpty()) {
      return;
    }
    try {
      final LongOpenHashSet relabelled =
          mintRecordOrderLabels(dirty, dirtyColumnWords, rootProvenance, carriedRelabels);
      if (!applyIncremental(dirty, dirtyColumnWords, rootProvenance, completedStructuralDeltas, relabelled)) {
        throw new IllegalStateException(
            "Projection index " + indexDef.getID() + " incremental maintenance found inconsistent persistent units");
      }
      maintenanceEpoch++;
    } catch (final RuntimeException | Error failure) {
      failMaintenance(failure);
      throw failure;
    }
  }

  /**
   * Mint the order labels of the records this pass will insert, BEFORE the apply pass reads any of
   * them. Doing it here rather than in the middle of the apply is what lets the directory's bounded
   * rebalance report the sibling records it re-spread: they join the dirty set as moves, so their
   * persisted row-group order labels are rewritten in the very same pass. Work stays incremental — a
   * rebalance touches a fixed-size sibling window and the row groups those records live in, never the
   * whole index, bitmap or trie.
   *
   * @return the already-persisted records a rebalance re-labelled; empty when none did
   */
  private LongOpenHashSet mintRecordOrderLabels(final LongOpenHashSet dirty,
      final Long2ObjectOpenHashMap<long[]> dirtyColumnWords, final @Nullable Long2ByteOpenHashMap rootProvenance,
      final LongOpenHashSet carriedRelabels) {
    if (rootProvenance == null || rootProvenance.isEmpty()) {
      return union(carriedRelabels, drainOrderRelabels(dirty, dirtyColumnWords));
    }
    final ProjectionStructuralOrderDirectory.Accessor directory = structuralOrderDirectory();
    final ProjectionStructuralOrderDirectory.RelabelSink sink = orderRelabelSink();
    final long[] candidates = dirty.toLongArray();
    LongArrays.quickSort(candidates);
    final LongOpenHashSet pending = new LongOpenHashSet(candidates.length);
    for (final long recordKey : candidates) {
      if ((rootProvenance.get(recordKey) & FIRST_ROOT_EVENT_MASK) == FIRST_ROOT_INSERT
          && isCurrentRecordRoot(recordKey)) {
        pending.add(recordKey);
      }
    }
    // Mint each run of newly inserted siblings LEFT to RIGHT. Minting is order-independent for
    // correctness, but not for cost: taken in an arbitrary order, every mint would have to skip its
    // not-yet-labelled neighbours to reach a labelled one, which is quadratic in the size of a
    // prepend-shaped batch. Walking a run from its head keeps every probe one sibling long.
    final LongOpenHashSet minted = new LongOpenHashSet(pending.size());
    for (final long recordKey : candidates) {
      if (!pending.contains(recordKey) || minted.contains(recordKey)) {
        continue;
      }
      long head = recordKey;
      for (;;) {
        final long left = leftSiblingKeyOf(head);
        if (left < 0 || !pending.contains(left) || minted.contains(left)) {
          break;
        }
        head = left;
      }
      for (long cursor = head; cursor >= 0 && pending.contains(cursor) && !minted.contains(cursor); cursor =
          rightSiblingKeyOf(cursor)) {
        directory.fullLabel(cursor, this::readNode, sink);
        minted.add(cursor);
      }
    }
    return union(carriedRelabels, drainOrderRelabels(dirty, dirtyColumnWords));
  }

  private static LongOpenHashSet union(final LongOpenHashSet left, final LongOpenHashSet right) {
    if (left.isEmpty()) {
      return right;
    }
    if (right.isEmpty()) {
      return left;
    }
    left.addAll(right);
    return left;
  }

  /**
   * Take over the records any mint in this transaction re-spread — the pre-pass above, but equally a
   * subtree move's {@code relabelDisplaced} or a structural entry's own mint, which both run before
   * this apply — and make sure each one is dirty so the apply rewrites its persisted row.
   */
  private LongOpenHashSet drainOrderRelabels(final LongOpenHashSet dirty,
      final Long2ObjectOpenHashMap<long[]> dirtyColumnWords) {
    final LongOpenHashSet relabels = orderRelabels;
    orderRelabels = null;
    if (relabels == null || relabels.isEmpty()) {
      return LongOpenHashSet.of();
    }
    for (final LongIterator iterator = relabels.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      if (dirty.add(recordKey)) {
        dirtyColumnWords.put(recordKey, allColumnWords.clone());
      }
    }
    return relabels;
  }

  private long leftSiblingKeyOf(final long nodeKey) {
    return readNode(nodeKey) instanceof final StructNode structural
        ? structural.getLeftSiblingKey()
        : NO_RECORD_KEY;
  }

  private long rightSiblingKeyOf(final long nodeKey) {
    return readNode(nodeKey) instanceof final StructNode structural
        ? structural.getRightSiblingKey()
        : NO_RECORD_KEY;
  }

  private void failMaintenance(final Throwable failure) {
    maintenanceFailed = true;
    if (maintenanceTrx instanceof final NodeTrx nodeTrx) {
      nodeTrx.markRollbackOnly(failure);
    }
  }

  /**
   * Resolve the persisted maintenance state before the first ordinary notification. A stale marker is
   * written only when a load was explicitly abandoned, so that definition remains a transaction-
   * local no-op without touching row groups or structural-order side slots. Absence is not a mode: a
   * catalogued definition without metadata is inconsistent and must fail the owning transaction.
   */
  private boolean skipExplicitlyAbandonedProjection() {
    // Read through the writer's reader view. Constructing writable HOT storage here would CoW the
    // projection container on the first (possibly irrelevant) document notification.
    final ProjectionIndexMetadata metadata = readMetadata();
    if (metadata == null) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " has no live metadata; incremental maintenance cannot proceed");
    }
    if (!metadata.isStale()) {
      return false;
    }
    invalidated = true;
    return true;
  }

  /**
   * Patch the persisted leaves for the given dirty records.
   *
   * @return {@code true} when the persisted state is consistent afterwards (including "nothing to
   *         maintain"); {@code false} when an inconsistency was discovered
   */
  private boolean applyIncremental(final LongOpenHashSet dirty, final Long2ObjectOpenHashMap<long[]> dirtyColumnWords,
      final @Nullable Long2ByteOpenHashMap rootProvenance,
      final @Nullable ArrayList<StructuralDelta> completedStructuralDeltas, final LongOpenHashSet relabelled) {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata meta = readMetadata(storage);
    if (meta == null) {
      throw new IllegalStateException(
          "Projection index " + indexDef.getID() + " has no live metadata; incremental maintenance cannot proceed");
    }
    if (meta.isStale()) {
      // An explicitly abandoned load has no live snapshot to maintain. It remains unusable until its
      // definition is dropped and a replacement is initialized under a fresh tree id.
      return true;
    }
    // The only persisted layout is segment-slot: slotKind 0 owns the zone-map descriptor and each
    // column segment has its own adjacent slot. Every maintenance read/write below stays in that
    // layout; an inline-in-descriptor payload is rejected as unsupported rather than migrated.
    // Shape guard: the persisted snapshot must describe exactly this definition, or patching would
    // splice rows into foreign columns.
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    final String[] defPaths = new String[fieldPaths.size()];
    final byte[] defKinds = new byte[fieldTypes.size()];
    for (int i = 0; i < defPaths.length; i++) {
      defPaths[i] = fieldPaths.get(i).toString();
      defKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(fieldTypes.get(i), fieldPaths.get(i));
    }
    if (!meta.matches(indexDef.getProjectionRootPath().toString(), defPaths, defKinds)) {
      return false;
    }

    final byte[] persistedKinds = meta.columnKinds();
    final MaintenanceGlobalDictionary[] globalDictionaries = createMaintenanceGlobalDictionaries(meta);
    final ProjectionSetSummaryChunks.Accessor setValueRowCounts =
        ProjectionSetSummaryChunks.open(storage, meta.setValueRowCounts());
    try {
      return applyIncremental(dirty, dirtyColumnWords, rootProvenance, completedStructuralDeltas, relabelled, storage,
          meta, persistedKinds, globalDictionaries, setValueRowCounts);
    } finally {
      releaseMaintenanceGlobalDictionaries(globalDictionaries);
    }
  }

  private boolean applyIncremental(final LongOpenHashSet dirty, final Long2ObjectOpenHashMap<long[]> dirtyColumnWords,
      final @Nullable Long2ByteOpenHashMap rootProvenance,
      final @Nullable ArrayList<StructuralDelta> completedStructuralDeltas, final LongOpenHashSet relabelled,
      final ProjectionIndexHOTStorage storage, final ProjectionIndexMetadata meta, final byte[] persistedKinds,
      final MaintenanceGlobalDictionary @Nullable [] globalDictionaries,
      final ProjectionSetSummaryChunks.Accessor setValueRowCounts) {
    final NodeReadOnlyTrx rtx = maintenanceTrx;
    final int priorRowGroupCount = meta.rowGroupCount();
    final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, priorRowGroupCount);
    final ProjectionRecordLocator.Accessor locator = ProjectionRecordLocator.open(storage);
    final ProjectionPersistedRecordLookup persistedLookup =
        new ProjectionPersistedRecordLookup(storage, fences, locator);
    final MaintenanceLocalityAccumulator locality = new MaintenanceLocalityAccumulator();
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final Long2LongOpenHashMap locationByRecord = new Long2LongOpenHashMap();
    locationByRecord.defaultReturnValue(ProjectionPersistedRecordLookup.ABSENT);
    final LongOpenHashSet removals = new LongOpenHashSet();
    final LongOpenHashSet insertions = new LongOpenHashSet();
    final LongOpenHashSet movedRecords = new LongOpenHashSet();
    final Long2ObjectOpenHashMap<long[]> changedColumnsBySlot = new Long2ObjectOpenHashMap<>();
    final LongOpenHashSet structuralEntriesWithoutPriorMembership =
        validateStructuralProvenance(completedStructuralDeltas, rootProvenance);

    for (final LongIterator iterator = dirty.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      final long location = persistedLookup.find(recordKey);
      locationByRecord.put(recordKey, location);
      final boolean persisted = location != ProjectionPersistedRecordLookup.ABSENT;
      final boolean current = isCurrentRecordRoot(recordKey);
      final byte provenance = rootProvenance == null
          ? 0
          : rootProvenance.get(recordKey);
      final byte firstEvent = (byte) (provenance & FIRST_ROOT_EVENT_MASK);
      final boolean created = firstEvent == FIRST_ROOT_INSERT;
      final boolean deleted = firstEvent == FIRST_ROOT_DELETE;
      final boolean structuralEnter = (provenance & STRUCTURAL_ENTER) != 0;
      final boolean structuralExit = (provenance & STRUCTURAL_EXIT) != 0;
      final boolean provenStructuralCreation = firstEvent == 0 && structuralEntriesWithoutPriorMembership != null
          && structuralEntriesWithoutPriorMembership.contains(recordKey);

      if (!persisted) {
        if (current) {
          if (!created && !provenStructuralCreation) {
            return false; // a missing locator/fence row is never inferred to be a new record
          }
          insertions.add(recordKey);
        } else {
          if (!created && !provenStructuralCreation) {
            // FIRST_DELETE proves this was pre-existing, so its missing persistent row is corruption;
            // mere field dirtiness has the same fail-closed answer. Only a first INSERT or an
            // explicitly observed outside→inside entry can prove an absent→absent net no-op.
            return false;
          }
        }
        continue;
      }

      // This maintenance pass observed the identity as newly entering the record set, yet a row for
      // it already exists. A prior apply clears both event and structural state, so this cannot be a
      // legitimate retry; accepting it would preserve or manufacture a duplicate identity.
      if (created || provenStructuralCreation) {
        return false;
      }

      if (!current) {
        if (!deleted && !structuralExit) {
          return false;
        }
        removals.add(recordKey);
        continue;
      }

      if (structuralEnter || structuralExit || relabelled.contains(recordKey)) {
        // A re-spread record keeps its identity and its columns but moves in LABEL space: the only
        // way its persisted row picks up the new order label is to leave its row group and re-enter
        // at the position the fresh label names.
        removals.add(recordKey);
        insertions.add(recordKey);
        movedRecords.add(recordKey);
      } else {
        final int slot = ProjectionPersistedRecordLookup.slot(location);
        mergeChangedColumns(changedColumnsBySlot, slot, dirtyColumnWords.get(recordKey));
      }
    }

    final Long2LongOpenHashMap predecessorByInsertion = new Long2LongOpenHashMap();
    predecessorByInsertion.defaultReturnValue(Long.MIN_VALUE);
    final Long2ObjectOpenHashMap<InsertionPosition> positionByInsertion = new Long2ObjectOpenHashMap<>();
    final long[] insertionOrder = insertions.toLongArray();
    long structuralSequence = 0L;
    if (completedStructuralDeltas != null) {
      for (final StructuralDelta delta : completedStructuralDeltas) {
        final StructuralSnapshot after = delta.after();
        for (int index = 0; index < after.containedInDocumentOrder().size(); index++) {
          final long recordKey = after.containedInDocumentOrder().getLong(index);
          if (insertions.contains(recordKey)) {
            final byte[] orderLabel = delta.afterRecordOrderLabels().get(recordKey);
            if (orderLabel == null) {
              throw new IllegalStateException("projection structural record has no translated order label");
            }
            positionByInsertion.put(recordKey, new InsertionPosition(orderLabel, true, structuralSequence++));
          }
        }
      }
    }
    deriveOrdinaryInsertionPositions(insertions, positionByInsertion);
    if (orderRelabels != null && !orderRelabels.isEmpty()) {
      // Every insertion's label is minted before this pass starts, so a rebalance here would be
      // re-spreading rows this pass has already sized up. Fail closed rather than persist a leaf
      // whose neighbours' order labels moved out from under it.
      return false;
    }
    LongArrays.quickSort(insertionOrder, (left, right) -> compareInsertionPositions(positionByInsertion.get(left), left,
        positionByInsertion.get(right), right));
    long previousInsertion = NO_RECORD_KEY;
    for (final long recordKey : insertionOrder) {
      final InsertionPosition position = positionByInsertion.get(recordKey);
      long predecessor =
          persistedPredecessor(position.orderLabel(), persistedLookup, fences, removals, locationByRecord);
      if (previousInsertion != NO_RECORD_KEY) {
        if (predecessor == NO_RECORD_KEY) {
          predecessor = previousInsertion;
        } else {
          final long location = locationByRecord.get(predecessor);
          final ProjectionIndexColumnSegmentCodec.KeysView view =
              persistedLookup.keys(ProjectionPersistedRecordLookup.slot(location)).view();
          final byte[] previousLabel = positionByInsertion.get(previousInsertion).orderLabel();
          if (view.compareOrderLabelAt(ProjectionPersistedRecordLookup.row(location), previousLabel) < 0) {
            predecessor = previousInsertion;
          }
        }
      }
      predecessorByInsertion.put(recordKey, predecessor);
      previousInsertion = recordKey;
    }

    final Long2LongOpenHashMap nextInsertion = new Long2LongOpenHashMap();
    nextInsertion.defaultReturnValue(Long.MIN_VALUE);
    final LongArrayList insertionStarts = new LongArrayList();
    for (final LongIterator iterator = insertions.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      final long predecessor = predecessorByInsertion.get(recordKey);
      if (predecessor == Long.MIN_VALUE) {
        return false;
      }
      if (nextInsertion.putIfAbsent(predecessor, recordKey) != Long.MIN_VALUE) {
        return false; // two projected records cannot have the same immediate predecessor
      }
      if (!insertions.contains(predecessor)) {
        insertionStarts.add(recordKey);
      }
    }

    final Int2ObjectOpenHashMap<LeafEdit> edits = new Int2ObjectOpenHashMap<>();
    final IntArrayList editOrder = new IntArrayList();
    final IntOpenHashSet membershipSlots = new IntOpenHashSet();
    int rowGroupsRead = 0;
    for (final LongIterator iterator = removals.iterator(); iterator.hasNext();) {
      final long recordKey = iterator.nextLong();
      final long location = locationByRecord.get(recordKey);
      final int slot = ProjectionPersistedRecordLookup.slot(location);
      final LeafEdit edit = loadLeafEdit(storage, fences, slot, true, persistedKinds, edits, editOrder, locality);
      rowGroupsRead += edit.markReadOnce();
      final int row = edit.indexOf(recordKey);
      if (row < 0 || edit.orderExceptions.getBoolean(row) != ProjectionPersistedRecordLookup.orderException(location)) {
        return false;
      }
      edit.removeKeyAt(row);
      membershipSlots.add(slot);
      if (ProjectionPersistedRecordLookup.orderException(location) && !insertions.contains(recordKey)) {
        locator.remove(recordKey);
      }
    }

    final LongOpenHashSet insertedOrMoved = new LongOpenHashSet(insertions);
    final LongOpenHashSet insertedVisited = new LongOpenHashSet();
    // Captured BEFORE the loop mutates the fences: bootstrapping a document base publishes a
    // still-unwritten leaf as the document head, so a later persisted walk would demand the
    // descriptor this commit has not written yet (and that a retired base no longer has). An
    // empty persisted order has no successor to any position by construction.
    final boolean emptyPersistedOrder = fences.liveRowGroupCount() == 0;
    for (int startIndex = 0; startIndex < insertionStarts.size(); startIndex++) {
      final LongArrayList chain = new LongArrayList();
      long recordKey = insertionStarts.getLong(startIndex);
      while (recordKey != Long.MIN_VALUE) {
        if (!insertions.contains(recordKey) || !insertedVisited.add(recordKey)) {
          return false;
        }
        chain.add(recordKey);
        final long next = nextInsertion.get(recordKey);
        recordKey = next;
      }

      final long stablePredecessor = predecessorByInsertion.get(chain.getLong(0));
      final int targetSlot;
      final int insertionOffset;
      final LeafEdit target;
      if (stablePredecessor == NO_RECORD_KEY) {
        if (fences.liveRowGroupCount() == 0) {
          targetSlot = fences.bootstrapDocumentBase(chain.getLong(0));
          target = loadLeafEdit(storage, fences, targetSlot, false, persistedKinds, edits, editOrder, locality);
        } else {
          targetSlot = fences.documentHead();
          target = loadLeafEdit(storage, fences, targetSlot, true, persistedKinds, edits, editOrder, locality);
          rowGroupsRead += target.markReadOnce();
        }
        insertionOffset = 0;
      } else {
        long predecessorLocation = locationByRecord.get(stablePredecessor);
        if (predecessorLocation == ProjectionPersistedRecordLookup.ABSENT
            && !locationByRecord.containsKey(stablePredecessor)) {
          predecessorLocation = persistedLookup.find(stablePredecessor);
          locationByRecord.put(stablePredecessor, predecessorLocation);
        }
        if (predecessorLocation == ProjectionPersistedRecordLookup.ABSENT) {
          return false;
        }
        targetSlot = ProjectionPersistedRecordLookup.slot(predecessorLocation);
        target = loadLeafEdit(storage, fences, targetSlot, true, persistedKinds, edits, editOrder, locality);
        rowGroupsRead += target.markReadOnce();
        final int predecessorRow = target.indexOf(stablePredecessor);
        if (predecessorRow < 0) {
          return false;
        }
        insertionOffset = predecessorRow + 1;
      }

      final boolean documentTail = emptyPersistedOrder
          || persistedSuccessor(stablePredecessor, persistedLookup, removals, locationByRecord) == NO_RECORD_KEY;
      int offset = insertionOffset;
      for (int chainIndex = 0; chainIndex < chain.size(); chainIndex++) {
        final long key = chain.getLong(chainIndex);
        if (target.containsKey(key)) {
          return false;
        }
        boolean orderException = true;
        if (!movedRecords.contains(key) && documentTail && fences.canExtendLastBaseFrom(targetSlot, key)) {
          fences.extendLastBaseUpper(key);
          orderException = false;
        }
        target.insertKeyAt(offset, key, orderException, positionByInsertion.get(key).orderLabel());
        offset++;
      }
      membershipSlots.add(targetSlot);
    }
    if (insertedVisited.size() != insertions.size()) {
      return false; // cycle or a chain with no stable/document-head start
    }

    final long savedNodeKey = rtx.getNodeKey();
    final LongOpenHashSet changedLeafSlots = new LongOpenHashSet();
    final Long2ObjectOpenHashMap<long[]> changedColumnsByLeaf = new Long2ObjectOpenHashMap<>();
    // Lazily allocated only for membership maintenance, then reused across every bounded
    // (<= MAX_ROWS) output group. Column-only commits retain their zero-allocation fast path.
    @Nullable
    LongOpenHashSet rewrittenRecordKeys = null;
    try {
      final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(indexDef, pathSummary);
      final int[] orderedEditSlots = editOrder.toIntArray();
      IntArrays.quickSort(orderedEditSlots,
          (leftSlot, rightSlot) -> compareLeafEditsDescending(edits.get(leftSlot), edits.get(rightSlot)));
      for (final int editSlot : orderedEditSlots) {
        final LeafEdit edit = edits.get(editSlot);
        if (!edit.keysChanged) {
          continue;
        }
        if (rewrittenRecordKeys == null) {
          rewrittenRecordKeys = new LongOpenHashSet(ProjectionIndexRowGroupPage.MAX_ROWS);
        }
        if (edit.oldPage != null) {
          adjustSetValueRowCounts(setValueRowCounts, edit.oldPage, -1L, allColumnWords);
        }
        final int rows = edit.recordKeys.size();
        if (rows == 0) {
          final int successorSlot = fences.next(edit.slot);
          final ProjectionIndexFences.DocumentPosition positionAfterSlot;
          if (successorSlot == 0) {
            positionAfterSlot = fences.documentTailPosition();
          } else {
            final ProjectionIndexColumnSegmentCodec.KeysView successorKeys = persistedLookup.keys(successorSlot).view();
            positionAfterSlot = fences.documentPosition(successorKeys.copyOrderLabelAt(0), persistedLookup);
          }
          if (fences.canRecycle(edit.slot)) {
            fences.recycle(edit.slot, positionAfterSlot);
          } else {
            fences.retireEmptyBase(edit.slot, positionAfterSlot);
          }
          storage.tombstoneRowGroupAsColumnSegmentSlots(edit.slot);
          persistedLookup.invalidate(edit.slot);
          changedLeafSlots.add(edit.slot);
          changedColumnsByLeaf.put(edit.slot, allColumnWords.clone());
          continue;
        }

        final int plannedGroups =
            Math.max(1, (rows + ProjectionIndexRowGroupPage.MAX_ROWS - 1) / ProjectionIndexRowGroupPage.MAX_ROWS);
        final int smallerGroupSize = rows / plannedGroups;
        final int largerGroups = rows % plannedGroups;
        int from = 0;
        int previousSlot = edit.slot;
        int group = 0;
        while (from < rows) {
          final int targetGroupSize = group < plannedGroups
              ? smallerGroupSize + (group < largerGroups
                  ? 1
                  : 0)
              : Math.min(ProjectionIndexRowGroupPage.MAX_ROWS, rows - from);
          final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(persistedKinds);
          page.setGlobalDictionaries(globalDictionaries);
          int appendedRows = 0;
          while (from < rows && appendedRows < targetGroupSize) {
            final long key = edit.recordKeys.getLong(from);
            final byte[] orderLabel = edit.orderLabels.get(from);
            if (!page.canAppendOrderLabel(orderLabel)) {
              if (appendedRows == 0) {
                return false;
              }
              break;
            }
            if (!extractInto(extractor, rtx, key)) {
              return false;
            }
            final boolean appended = extractor.appendTo(page, key, edit.orderExceptions.getBoolean(from), orderLabel);
            if (!appended) {
              throw new IllegalStateException("a preflighted projection maintenance row was rejected");
            }
            from++;
            appendedRows++;
          }
          final int physicalSlot = group == 0
              ? edit.slot
              : fences.allocateSlot();
          updateExceptionLocators(locator, page, physicalSlot, edit.slot, insertedOrMoved);
          validateRewrittenRowGroup(page, physicalSlot, locator, rewrittenRecordKeys);
          writeRowGroup(storage, physicalSlot, page, fences, changedLeafSlots, changedColumnsByLeaf, allColumnWords,
              true, group == 0 && edit.priorExists, encodeWorkspace);
          persistedLookup.invalidate(physicalSlot);
          adjustSetValueRowCounts(setValueRowCounts, page, 1L, allColumnWords);
          if (group > 0) {
            final ProjectionIndexFences.DocumentPosition position =
                fences.documentPosition(page.copyOrderLabelAt(0), persistedLookup);
            fences.linkAfter(previousSlot, physicalSlot, position);
          }
          previousSlot = physicalSlot;
          group++;
        }
      }

      validateTouchedNormalBounds(fences, changedLeafSlots);

      for (final LongIterator iterator = changedColumnsBySlot.keySet().iterator(); iterator.hasNext();) {
        final int slot = Math.toIntExact(iterator.nextLong());
        if (membershipSlots.contains(slot)) {
          continue; // membership rewrite already refreshed every column exactly once
        }
        final ProjectionPersistedRecordLookup.Keys cachedKeys = persistedLookup.keys(slot);
        if (!applyColumnOnlyUpdate(storage, slot, changedColumnsBySlot.get(slot), persistedKinds, globalDictionaries,
            setValueRowCounts, extractor, rtx, cachedKeys, changedLeafSlots, changedColumnsByLeaf, locality,
            encodeWorkspace)) {
          return false;
        }
      }

      locality.descriptorsRead = persistedLookup.descriptorsRead();
      locality.keySegmentsRead = persistedLookup.keySegmentsRead();

      final int newRowGroupCount = fences.liveRowGroupCount();
      if (changedLeafSlots.isEmpty()) {
        recordMaintenanceTelemetry(dirty.size(), rowGroupsRead, 0, 0, fences, setValueRowCounts,
            new ProjectionBloomChunks.RewriteStats(0, 0, 0L, 0L));
        return true;
      }

      final int dictionarySegments = pendingMaintenanceDictionarySegments(globalDictionaries);
      final long[] valueDictionaryHeaderKeys = flushMaintenanceGlobalDictionaries(meta, globalDictionaries);
      final Map<Integer, Map<String, Long>> persistedSetSummaries = setValueRowCounts.flush(persistedKinds);
      // The SEGMENT anchor table is carried forward, not rebuilt: it describes where each already
      // sealed segment's dictionary lives, and maintenance neither seals nor moves one. Dropping it
      // here — which an earlier version of this call did, simply by not passing it — erases the only
      // route from a converted page's anchor to its dictionary, and every such page becomes
      // unresolvable on the very next read ("cannot resolve id N against it").
      final ProjectionIndexMetadata refreshed = new ProjectionIndexMetadata(meta.rootPath(), meta.fieldPaths(),
          meta.fieldNames(), persistedKinds, newRowGroupCount, rtx.getRevisionNumber(), persistedSetSummaries,
          valueDictionaryHeaderKeys, meta.segmentAnchors());
      fences.flush(newRowGroupCount);
      final ProjectionBloomChunks.RewriteStats bloomStats =
          ProjectionBloomChunks.rewriteTouchedChunks(storage, persistedKinds, newRowGroupCount,
              fences.physicalRowGroupCount(), changedColumnsByLeaf, newRowGroupCount != priorRowGroupCount);
      // Slot 0 is the authoritative visibility marker. Publish it only after every row group,
      // locator, dictionary, summary, fence and Bloom unit it describes has been written. A failure
      // anywhere above therefore leaves the previous metadata in force and the transaction
      // rollback-only; it cannot expose a partially described projection snapshot.
      storage.putBlob(0, refreshed.serialize());
      if (HFT_TELEMETRY_ENABLED) {
        HFT_METADATA_WRITES.incrementAndGet();
      }
      recordMaintenanceTelemetry(dirty.size(), rowGroupsRead, changedLeafSlots.size(), dictionarySegments, fences,
          setValueRowCounts, bloomStats);
      return true;
    } finally {
      LAST_MAINTENANCE_LOCALITY = locality.snapshot();
      if (!rtx.moveTo(savedNodeKey)) {
        ((NodeCursor) rtx).moveToDocumentRoot();
      }
    }
  }

  private static @Nullable LongOpenHashSet validateStructuralProvenance(
      final @Nullable ArrayList<StructuralDelta> completedStructuralDeltas,
      final @Nullable Long2ByteOpenHashMap rootProvenance) {
    if (completedStructuralDeltas == null) {
      return null;
    }
    if (rootProvenance == null) {
      throw new IllegalStateException("projection structural deltas have no root provenance");
    }
    final LongOpenHashSet observedMembership = new LongOpenHashSet();
    final LongOpenHashSet entriesWithoutPriorMembership = new LongOpenHashSet();
    for (final StructuralDelta delta : completedStructuralDeltas) {
      validateStructuralSnapshot(delta.before());
      validateStructuralSnapshot(delta.after());
      for (final LongIterator iterator = delta.before().contained().iterator(); iterator.hasNext();) {
        final long key = iterator.nextLong();
        observedMembership.add(key);
        if ((rootProvenance.get(key) & STRUCTURAL_EXIT) == 0) {
          throw new IllegalStateException("projection structural exit provenance missing for record " + key);
        }
      }
      for (final LongIterator iterator = delta.after().contained().iterator(); iterator.hasNext();) {
        final long key = iterator.nextLong();
        if (observedMembership.add(key)) {
          entriesWithoutPriorMembership.add(key);
        }
        if ((rootProvenance.get(key) & STRUCTURAL_ENTER) == 0) {
          throw new IllegalStateException("projection structural enter provenance missing for record " + key);
        }
      }
    }
    return entriesWithoutPriorMembership;
  }

  private record InsertionPosition(byte[] orderLabel, boolean structural, long sequence) {
  }

  private static int compareInsertionPositions(final InsertionPosition left, final long leftKey,
      final InsertionPosition right, final long rightKey) {
    final byte[] leftLabel = left.orderLabel();
    final byte[] rightLabel = right.orderLabel();
    final int labelComparison = compareOrderLabels(leftLabel, rightLabel);
    if (labelComparison != 0) {
      return labelComparison;
    }
    if (left.structural() != right.structural()) {
      return left.structural()
          ? -1
          : 1;
    }
    if (left.structural()) {
      final int comparison = Long.compare(left.sequence(), right.sequence());
      if (comparison != 0) {
        return comparison;
      }
    }
    return Long.compare(leftKey, rightKey);
  }

  private static int compareOrderLabels(final byte[] left, final byte[] right) {
    return ProjectionIndexRowGroupPage.compareOrderLabels(left, 0, left.length, right, 0, right.length);
  }

  private static long persistedPredecessor(final byte[] label, final ProjectionPersistedRecordLookup persistedLookup,
      final ProjectionIndexFences.Accessor fences, final LongOpenHashSet removals,
      final Long2LongOpenHashMap locationByRecord) {
    if (fences.liveRowGroupCount() == 0) {
      return NO_RECORD_KEY;
    }
    int slot = fences.documentPosition(label, persistedLookup).predecessors()[0];
    int skippedRemoved = 0;
    while (slot != 0) {
      final ProjectionIndexColumnSegmentCodec.KeysView keys = persistedLookup.keys(slot).view();
      int low = 0;
      int high = keys.recordKeys().length;
      while (low < high) {
        final int middle = (low + high) >>> 1;
        if (keys.compareOrderLabelAt(middle, label) < 0) {
          low = middle + 1;
        } else {
          high = middle;
        }
      }
      for (int row = low - 1; row >= 0; row--) {
        final long recordKey = keys.recordKeys()[row];
        if (removals.contains(recordKey)) {
          if (++skippedRemoved > removals.size()) {
            throw new IllegalStateException("projection predecessor removal walk did not advance");
          }
          continue;
        }
        final long location = persistedLookup.find(recordKey);
        if (location == ProjectionPersistedRecordLookup.ABSENT) {
          throw new IllegalStateException("projection persisted predecessor " + recordKey + " is not locatable");
        }
        locationByRecord.put(recordKey, location);
        return recordKey;
      }
      slot = fences.previous(slot);
    }
    return NO_RECORD_KEY;
  }

  private static long persistedSuccessor(final long stablePredecessor,
      final ProjectionPersistedRecordLookup persistedLookup, final LongOpenHashSet removals,
      final Long2LongOpenHashMap locationByRecord) {
    long candidate;
    if (stablePredecessor == NO_RECORD_KEY) {
      candidate = persistedLookup.firstRecord();
    } else {
      long location = locationByRecord.get(stablePredecessor);
      if (location == ProjectionPersistedRecordLookup.ABSENT && !locationByRecord.containsKey(stablePredecessor)) {
        location = persistedLookup.find(stablePredecessor);
        locationByRecord.put(stablePredecessor, location);
      }
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        throw new IllegalStateException(
            "projection insertion predecessor " + stablePredecessor + " has no persisted row");
      }
      candidate = persistedLookup.nextRecord(location);
    }
    int skipped = 0;
    while (candidate != NO_RECORD_KEY && removals.contains(candidate)) {
      long location = locationByRecord.get(candidate);
      if (location == ProjectionPersistedRecordLookup.ABSENT && !locationByRecord.containsKey(candidate)) {
        location = persistedLookup.find(candidate);
        locationByRecord.put(candidate, location);
      }
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        throw new IllegalStateException("projection removed record " + candidate + " has no persisted row");
      }
      candidate = persistedLookup.nextRecord(location);
      if (++skipped > removals.size()) {
        throw new IllegalStateException("projection persisted successor walk did not advance");
      }
    }
    return candidate;
  }

  private static void validateStructuralSnapshot(final StructuralSnapshot snapshot) {
    if (snapshot.contained().size() != snapshot.containedInDocumentOrder().size()) {
      throw new IllegalStateException("projection structural snapshot has inconsistent positional anchors");
    }
  }

  private static LeafEdit loadLeafEdit(final ProjectionIndexHOTStorage storage,
      final ProjectionIndexFences.Accessor fences, final int slot, final boolean priorExists,
      final byte[] persistedKinds, final Int2ObjectOpenHashMap<LeafEdit> edits, final IntArrayList editOrder,
      final MaintenanceLocalityAccumulator locality) {
    final LeafEdit cached = edits.get(slot);
    if (cached != null) {
      if (cached.priorExists != priorExists) {
        throw new IllegalStateException("projection physical leaf " + slot + " changed existence within one edit");
      }
      return cached;
    }
    final ProjectionIndexRowGroupPage oldPage;
    if (priorExists) {
      final byte[] raw = storage.getRowGroupFromColumnSegmentSlots(slot);
      if (raw == null) {
        throw new IllegalStateException("projection physical leaf " + slot + " has no row-group payload");
      }
      locality.fullRowGroupsRead++;
      oldPage = ProjectionIndexRowGroupPage.deserialize(raw);
      if (oldPage.getColumnCount() != persistedKinds.length || oldPage.firstRecordKey() != fences.first(slot)
          || oldPage.lastRecordKey() != fences.last(slot)) {
        throw new IllegalStateException("projection row-group/fence mirror mismatch at physical leaf " + slot);
      }
      for (int column = 0; column < persistedKinds.length; column++) {
        if (oldPage.columnKind(column) != persistedKinds[column]) {
          throw new IllegalStateException("projection column kind drift at physical leaf " + slot);
        }
      }
    } else {
      oldPage = null;
    }
    final int rows = oldPage == null
        ? 0
        : oldPage.getRowCount();
    final LongArrayList keys = new LongArrayList(rows);
    final BooleanArrayList exceptions = new BooleanArrayList(rows);
    final ObjectArrayList<byte[]> orderLabels = new ObjectArrayList<>(rows);
    for (int row = 0; row < rows; row++) {
      keys.add(oldPage.recordKeys()[row]);
      exceptions.add(oldPage.orderExceptionAt(row));
      orderLabels.add(oldPage.copyOrderLabelAt(row));
    }
    final LeafEdit loaded = new LeafEdit(slot, priorExists, oldPage, keys, exceptions, orderLabels);
    edits.put(slot, loaded);
    editOrder.add(slot);
    return loaded;
  }

  private static int compareLeafEditsDescending(final LeafEdit left, final LeafEdit right) {
    final byte[] leftLabel = left.oldFirstOrderLabel;
    final byte[] rightLabel = right.oldFirstOrderLabel;
    if (leftLabel == null) {
      return rightLabel == null
          ? Integer.compare(right.slot, left.slot)
          : 1;
    }
    if (rightLabel == null) {
      return -1;
    }
    final int byLabel = compareOrderLabels(rightLabel, leftLabel);
    return byLabel != 0
        ? byLabel
        : Integer.compare(right.slot, left.slot);
  }

  private static void updateExceptionLocators(final ProjectionRecordLocator.Accessor locator,
      final ProjectionIndexRowGroupPage page, final int physicalSlot, final int originalSlot,
      final LongOpenHashSet insertedOrMoved) {
    for (int row = 0; row < page.getRowCount(); row++) {
      if (!page.orderExceptionAt(row)) {
        continue;
      }
      final long recordKey = page.recordKeys()[row];
      if (physicalSlot != originalSlot || insertedOrMoved.contains(recordKey)) {
        locator.put(recordKey, physicalSlot);
      }
    }
  }

  /**
   * Validate the two identity invariants local to one rewritten physical row group before its
   * metadata can be published. The caller reuses {@code uniqueRecordKeys} across groups, so the check
   * is linear, primitive-only, and bounded by the 1024-row page capacity.
   *
   * <p>
   * An exception locator stores the physical slot rather than a row ordinal. Once keys are unique
   * within the page, {@code locator -> physicalSlot -> exact KEYS scan} resolves to exactly the row
   * inspected here. Checking every exception bit, rather than only inserted or relocated rows,
   * catches a missing or misdirected locator on an unchanged exceptional row carried through a
   * membership rewrite.
   * </p>
   */
  static void validateRewrittenRowGroup(final ProjectionIndexRowGroupPage page, final int physicalSlot,
      final ProjectionRecordLocator.Accessor locator, final LongOpenHashSet uniqueRecordKeys) {
    uniqueRecordKeys.clear();
    final long[] recordKeys = page.recordKeys();
    for (int row = 0; row < page.getRowCount(); row++) {
      final long recordKey = recordKeys[row];
      if (!uniqueRecordKeys.add(recordKey)) {
        throw new IllegalStateException(
            "projection record " + recordKey + " occurs more than once in rewritten physical leaf " + physicalSlot);
      }
      if (page.orderExceptionAt(row)) {
        final int locatedSlot = locator.find(recordKey);
        if (locatedSlot != physicalSlot) {
          throw new IllegalStateException("projection exception locator " + recordKey + " resolves to physical leaf "
              + locatedSlot + " instead of rewritten leaf " + physicalSlot + " row " + row);
        }
      }
    }
  }

  private static void validateTouchedNormalBounds(final ProjectionIndexFences.Accessor fences,
      final LongOpenHashSet changedLeafSlots) {
    for (final LongIterator iterator = changedLeafSlots.iterator(); iterator.hasNext();) {
      final int slot = Math.toIntExact(iterator.nextLong());
      if (!fences.isLivePhysicalSlot(slot)) {
        continue;
      }
      fences.validateTouchedNormalBounds(slot);
    }
  }

  private static final class LeafEdit {
    private final int slot;
    private final boolean priorExists;
    private final @Nullable ProjectionIndexRowGroupPage oldPage;
    private final byte @Nullable [] oldFirstOrderLabel;
    private final LongArrayList recordKeys;
    private final BooleanArrayList orderExceptions;
    private final ObjectArrayList<byte[]> orderLabels;
    private boolean keysChanged;
    private boolean readCounted;
    /**
     * Lazily built membership index over {@link #recordKeys}, kept in sync by
     * {@link #insertKeyAt}/{@link #removeKeyAt}. Without it, N records appended at the document tail
     * form ONE insertion chain into ONE edit and the per-element presence probe rescans the growing
     * list — N(N−1)/2 comparisons inside beforeCommit().
     */
    private @Nullable LongOpenHashSet keySet;

    private LeafEdit(final int slot, final boolean priorExists, final @Nullable ProjectionIndexRowGroupPage oldPage,
        final LongArrayList recordKeys, final BooleanArrayList orderExceptions,
        final ObjectArrayList<byte[]> orderLabels) {
      this.slot = slot;
      this.priorExists = priorExists;
      this.oldPage = oldPage;
      oldFirstOrderLabel = oldPage == null || oldPage.getRowCount() == 0
          ? null
          : oldPage.copyOrderLabelAt(0);
      this.recordKeys = recordKeys;
      this.orderExceptions = orderExceptions;
      this.orderLabels = orderLabels;
    }

    private LongOpenHashSet keySet() {
      LongOpenHashSet set = keySet;
      if (set == null) {
        set = new LongOpenHashSet(Math.max(16, recordKeys.size()));
        for (int row = 0; row < recordKeys.size(); row++) {
          set.add(recordKeys.getLong(row));
        }
        keySet = set;
      }
      return set;
    }

    private boolean containsKey(final long recordKey) {
      return keySet().contains(recordKey);
    }

    private int indexOf(final long recordKey) {
      if (keySet != null && !keySet.contains(recordKey)) {
        return -1; // an absent key answers in O(1); the scan below only runs for present keys
      }
      for (int row = 0; row < recordKeys.size(); row++) {
        if (recordKeys.getLong(row) == recordKey) {
          return row;
        }
      }
      return -1;
    }

    private void insertKeyAt(final int row, final long recordKey, final boolean orderException,
        final byte[] orderLabel) {
      recordKeys.add(row, recordKey);
      orderExceptions.add(row, orderException);
      orderLabels.add(row, orderLabel);
      keysChanged = true;
      if (keySet != null) {
        keySet.add(recordKey);
      }
    }

    private void removeKeyAt(final int row) {
      final long removed = recordKeys.removeLong(row);
      orderExceptions.removeBoolean(row);
      orderLabels.remove(row);
      keysChanged = true;
      if (keySet != null) {
        keySet.remove(removed);
      }
    }

    private int markReadOnce() {
      if (oldPage == null || readCounted) {
        return 0;
      }
      readCounted = true;
      return 1;
    }
  }


  /**
   * Update-only path: preserve KEYS and every untouched column segment, rebuilding only the selected
   * columns from masked extraction. Re-extracting the selected columns across the leaf is necessary
   * with the V0 aggregate provenance flags: a one-cell replacement may remove the only
   * unrepresentable/non-integral/non-double-source witness, and V0 stores no per-row witness bits.
   */
  private static boolean applyColumnOnlyUpdate(final ProjectionIndexHOTStorage storage, final int slot,
      final long[] changedColumnWords, final byte[] persistedKinds,
      final MaintenanceGlobalDictionary @Nullable [] globalDictionaries,
      final ProjectionSetSummaryChunks.Accessor setValueRowCounts, final ProjectionIndexRowExtractor extractor,
      final NodeReadOnlyTrx rtx, final ProjectionPersistedRecordLookup.Keys cachedKeys,
      final LongOpenHashSet changedLeafSlots, final Long2ObjectOpenHashMap<long[]> changedColumnsByLeaf,
      final MaintenanceLocalityAccumulator locality,
      final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace) {
    final byte[] descriptor = cachedKeys.descriptor();
    if (descriptor == null || RowGroupDescriptor.columnCount(descriptor) != persistedKinds.length) {
      return false;
    }
    final long[] recordKeys = cachedKeys.view().recordKeys();
    if (recordKeys.length != RowGroupDescriptor.rowCount(descriptor)) {
      return false;
    }

    final ProjectionIndexRowGroupPage[] rebuiltColumns = new ProjectionIndexRowGroupPage[persistedKinds.length];
    final ProjectionColumnStore.ColumnSlice[] priorSlices =
        new ProjectionColumnStore.ColumnSlice[persistedKinds.length];
    int selectedColumns = 0;
    for (int column = 0; column < persistedKinds.length; column++) {
      if (!columnSelected(changedColumnWords, column)) {
        continue;
      }
      selectedColumns++;
      final byte[] body = storage.getVerifiedColumnSegment(slot, descriptor,
          ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column),
          ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY);
      if (body == null) {
        return false;
      }
      locality.bodySegmentsRead++;
      final byte kind = persistedKinds[column];
      final ProjectionColumnStore.ColumnSlice priorSlice;
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        final byte[] dictionary = storage.getVerifiedColumnSegment(slot, descriptor,
            ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(column),
            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT);
        if (dictionary == null) {
          return false;
        }
        locality.dictionarySegmentsRead++;
        priorSlice = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            ? ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, body, dictionary, column)
            : ProjectionIndexColumnSegmentCodec.decodeStringSetSlice(descriptor, body, dictionary, column);
      } else {
        priorSlice = ProjectionIndexColumnSegmentCodec.decodeBodySlice(descriptor, body, column);
      }
      if (priorSlice.rowCount() != recordKeys.length) {
        return false;
      }
      priorSlices[column] = priorSlice;
      final ProjectionIndexRowGroupPage rebuilt = new ProjectionIndexRowGroupPage(new byte[] {kind});
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        if (globalDictionaries == null || globalDictionaries[column] == null) {
          return false;
        }
        rebuilt.setGlobalDictionaries(new GlobalValueDictionaryEncoder[] {globalDictionaries[column]});
      }
      rebuiltColumns[column] = rebuilt;
    }
    if (selectedColumns == 0) {
      return true;
    }

    for (final long recordKey : recordKeys) {
      if (!extractInto(extractor, rtx, recordKey, changedColumnWords)) {
        return false;
      }
      for (int column = 0; column < persistedKinds.length; column++) {
        final ProjectionIndexRowGroupPage rebuilt = rebuiltColumns[column];
        if (rebuilt != null && !extractor.appendColumnTo(rebuilt, recordKey, column)) {
          return false;
        }
      }
    }

    final ProjectionIndexColumnSegmentCodec.EncodedColumn[] encodedColumns =
        new ProjectionIndexColumnSegmentCodec.EncodedColumn[selectedColumns];
    int encodedColumnCount = 0;
    for (int column = 0; column < persistedKinds.length; column++) {
      final ProjectionIndexRowGroupPage rebuilt = rebuiltColumns[column];
      if (rebuilt == null) {
        continue;
      }
      final ProjectionIndexColumnSegmentCodec.EncodedColumn encoded =
          ProjectionIndexColumnSegmentCodec.encodeColumn(rebuilt, column, encodeWorkspace);
      encodedColumns[encodedColumnCount++] = encoded;
      locality.columnSegmentsEncoded += encoded.columnSegmentIds().length;
    }
    if (encodedColumnCount != selectedColumns) {
      throw new IllegalStateException(
          "encoded " + encodedColumnCount + " projection columns, expected " + selectedColumns);
    }
    final long[] actuallyChanged = new long[(persistedKinds.length + Long.SIZE - 1) >>> 6];
    final byte[] patchedDescriptor = ProjectionIndexColumnSegmentCodec.spliceColumns(descriptor, encodedColumns,
        encodedColumnCount, actuallyChanged);
    final ProjectionIndexHOTStorage.ColumnPatchResult patch = storage.putColumnPatches(slot, descriptor,
        patchedDescriptor, encodedColumns, encodedColumnCount, actuallyChanged);
    if (patch.changed()) {
      locality.columnSegmentsWritten += patch.segmentsWritten() + patch.segmentsTombstoned();
      locality.descriptorsWritten++;
      for (int encodedIndex = 0; encodedIndex < encodedColumnCount; encodedIndex++) {
        final int column = encodedColumns[encodedIndex].column();
        if (persistedKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET
            || !columnSelected(actuallyChanged, column)) {
          continue;
        }
        adjustSetValueRowCounts(setValueRowCounts, column, priorSlices[column], -1L);
        adjustSetValueRowCounts(setValueRowCounts, column, rebuiltColumns[column], 1L);
      }
      changedLeafSlots.add(slot);
      mergeChangedColumns(changedColumnsByLeaf, slot, actuallyChanged);
      locality.rowGroupsColumnPatched++;
    }
    return true;
  }

  private boolean isCurrentRecordRoot(final long recordKey) {
    final ImmutableNode record = readNode(recordKey);
    if (record == null) {
      return false;
    }
    final long resolved = resolveRecordKey(recordKey, record.getKind(), record.getParentKey(), pathNodeKeyOf(record));
    if (resolved == UNRESOLVED) {
      throw new IllegalStateException("Projection index " + indexDef.getID()
          + " cannot determine final record-set membership for node " + recordKey);
    }
    return resolved == recordKey;
  }

  private static boolean extractInto(final ProjectionIndexRowExtractor extractor, final NodeReadOnlyTrx rtx,
      final long recordKey) {
    return extractInto(extractor, rtx, recordKey, null);
  }

  private static boolean extractInto(final ProjectionIndexRowExtractor extractor, final NodeReadOnlyTrx rtx,
      final long recordKey, final long @Nullable [] selectedColumns) {
    if (rtx instanceof final JsonNodeReadOnlyTrx jsonRtx) {
      return extractor.extractInto(jsonRtx, recordKey, selectedColumns);
    }
    if (rtx instanceof final XmlNodeReadOnlyTrx xmlRtx) {
      return extractor.extractInto(xmlRtx, recordKey, selectedColumns);
    }
    throw new IllegalArgumentException("projection maintenance requires a JSON or XML node transaction");
  }

  private MaintenanceGlobalDictionary @Nullable [] createMaintenanceGlobalDictionaries(
      final ProjectionIndexMetadata meta) {
    final byte[] kinds = meta.columnKinds();
    int globalColumnCount = 0;
    for (final byte kind : kinds) {
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL)
        globalColumnCount++;
    }
    final long totalBudget = ProjectionIndexBuilder.globalDictionaryBudgetBytes();
    final long columnBudget = globalColumnCount == 0 || totalBudget == Long.MAX_VALUE
        ? totalBudget
        : totalBudget / globalColumnCount;
    if (globalColumnCount > 0 && columnBudget < GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES) {
      throw new IllegalStateException(
          "global dictionary transaction budget cannot retain " + globalColumnCount + " empty dictionaries");
    }
    MaintenanceGlobalDictionary[] dictionaries = null;
    try {
      for (int c = 0; c < kinds.length; c++) {
        if (kinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          continue;
        }
        final long headerKey = meta.valueDictionaryHeaderKey(c);
        if (headerKey <= 0) {
          throw new IllegalStateException("global projection column " + c + " has no value dictionary anchor");
        }
        if (dictionaries == null) {
          dictionaries = new MaintenanceGlobalDictionary[kinds.length];
        }
        dictionaries[c] = new MaintenanceGlobalDictionary(c, headerKey, storageEngineWriter, columnBudget);
      }
      return dictionaries;
    } catch (final RuntimeException failure) {
      releaseMaintenanceGlobalDictionaries(dictionaries);
      throw failure;
    }
  }

  private long @Nullable [] flushMaintenanceGlobalDictionaries(final ProjectionIndexMetadata meta,
      final MaintenanceGlobalDictionary @Nullable [] dictionaries) {
    final long[] current = meta.valueDictionaryHeaderKeys();
    if (dictionaries == null) {
      return current == null
          ? null
          : current.clone();
    }
    final long[] refreshed = current == null
        ? new long[dictionaries.length]
        : current.clone();
    for (int c = 0; c < dictionaries.length; c++) {
      final MaintenanceGlobalDictionary dictionary = dictionaries[c];
      if (dictionary != null) {
        refreshed[c] = dictionary.flush();
      }
    }
    return refreshed;
  }

  private static void releaseMaintenanceGlobalDictionaries(
      final MaintenanceGlobalDictionary @Nullable [] dictionaries) {
    if (dictionaries == null) {
      return;
    }
    for (final MaintenanceGlobalDictionary dictionary : dictionaries) {
      if (dictionary != null) {
        dictionary.release();
      }
    }
  }

  private static int pendingMaintenanceDictionarySegments(final MaintenanceGlobalDictionary @Nullable [] dictionaries) {
    if (dictionaries == null) {
      return 0;
    }
    int count = 0;
    for (final MaintenanceGlobalDictionary dictionary : dictionaries) {
      if (dictionary != null && dictionary.hasAdditions()) {
        count++;
      }
    }
    return count;
  }

  private static long pendingMaintenanceDictionaryBytes(final MaintenanceGlobalDictionary @Nullable [] dictionaries) {
    if (dictionaries == null) {
      return 0L;
    }
    long bytes = 0L;
    for (final MaintenanceGlobalDictionary dictionary : dictionaries) {
      if (dictionary != null) {
        bytes = Math.addExact(bytes, dictionary.pendingBytes());
      }
    }
    return bytes;
  }

  private static void recordMaintenanceTelemetry(final int dirtyRecords, final int rowGroupsRead,
      final int rowGroupsWritten, final int dictionarySegments, final ProjectionIndexFences.Accessor fences,
      final ProjectionSetSummaryChunks.Accessor setSummaries, final ProjectionBloomChunks.RewriteStats bloomStats) {
    if (!HFT_TELEMETRY_ENABLED) {
      return;
    }
    HFT_COMMITS.incrementAndGet();
    HFT_DIRTY_RECORDS.addAndGet(dirtyRecords);
    HFT_ROW_GROUPS_READ.addAndGet(rowGroupsRead);
    HFT_ROW_GROUPS_WRITTEN.addAndGet(rowGroupsWritten);
    HFT_DICTIONARY_SEGMENTS.addAndGet(dictionarySegments);
    HFT_FENCE_CHUNKS_READ.addAndGet(fences.chunksRead());
    HFT_FENCE_CHUNKS_WRITTEN.addAndGet(fences.chunksWritten());
    HFT_SET_CHUNKS_READ.addAndGet(setSummaries.chunksRead());
    HFT_SET_CHUNKS_WRITTEN.addAndGet(setSummaries.chunksWritten());
    HFT_BLOOM_ROW_GROUPS_READ.addAndGet(bloomStats.rowGroupsRead());
    HFT_BLOOM_CHUNKS_WRITTEN.addAndGet(bloomStats.chunksWritten());
  }

  private static void adjustSetValueRowCounts(final ProjectionSetSummaryChunks.Accessor totals,
      final ProjectionIndexRowGroupPage leaf, final long sign, final long[] changedColumnWords) {
    final Map<Integer, Map<String, Long>> leafCounts = new LinkedHashMap<>();
    ProjectionIndexBuilder.accumulateSetValueRowCounts(leaf, leafCounts);
    for (final Map.Entry<Integer, Map<String, Long>> column : leafCounts.entrySet()) {
      if (columnSelected(changedColumnWords, column.getKey())) {
        totals.adjust(column.getKey(), column.getValue(), sign);
      }
    }
  }

  private static void adjustSetValueRowCounts(final ProjectionSetSummaryChunks.Accessor totals,
      final int persistedColumn, final ProjectionColumnStore.ColumnSlice prior, final long sign) {
    final int[] countsByRow = prior.setCounts();
    final int[] ids = prior.stringDictIds();
    final byte[] dictionary = prior.dictBytes();
    final int[] offsets = prior.dictOffsets();
    if (countsByRow == null || ids == null || dictionary == null || offsets == null) {
      throw new IllegalStateException("decoded string-set column is incomplete");
    }
    final long[] counts =
        ProjectionIndexColumnSegmentCodec.valueRowCounts(prior.dictSize(), countsByRow, ids, prior.rowCount());
    if (counts == null) {
      return;
    }
    final Map<String, Long> values = new LinkedHashMap<>();
    for (int id = 0; id < counts.length; id++) {
      if (counts[id] > 0) {
        values.put(new String(dictionary, offsets[id], offsets[id + 1] - offsets[id], StandardCharsets.UTF_8),
            counts[id]);
      }
    }
    totals.adjust(persistedColumn, values, sign);
  }

  private static void adjustSetValueRowCounts(final ProjectionSetSummaryChunks.Accessor totals,
      final int persistedColumn, final ProjectionIndexRowGroupPage rebuiltColumn, final long sign) {
    final Map<Integer, Map<String, Long>> counts = new LinkedHashMap<>();
    ProjectionIndexBuilder.accumulateSetValueRowCounts(rebuiltColumn, counts);
    final Map<String, Long> values = counts.get(0);
    if (values != null) {
      totals.adjust(persistedColumn, values, sign);
    }
  }

  private static void mergeChangedColumns(final Long2ObjectOpenHashMap<long[]> changedColumnsBySlot, final long slot,
      final long[] changedColumnWords) {
    Objects.requireNonNull(changedColumnWords, "changed column mask is required");
    final long[] existing = changedColumnsBySlot.get(slot);
    if (existing == null) {
      changedColumnsBySlot.put(slot, changedColumnWords.clone());
      return;
    }
    for (int word = 0; word < existing.length; word++) {
      existing[word] |= changedColumnWords[word];
    }
  }

  private static boolean columnSelected(final long[] changedColumnWords, final int column) {
    final int word = column >>> 6;
    return word < changedColumnWords.length && (changedColumnWords[word] & (1L << (column & 63))) != 0L;
  }

  static final class MaintenanceGlobalDictionary implements GlobalValueDictionaryEncoder {

    private final int column;
    private final long headerKey;
    private final StorageEngineWriter storageEngineWriter;
    private final ValueDictionaryHeaderNode baseHeader;
    private final int baseEntryCount;
    private final long budgetBytes;
    private final GlobalValueDictionaryHotCache hotValues = new GlobalValueDictionaryHotCache();
    private @Nullable GlobalValueDictionaryWriter additions;
    private long persistentProbeCount;

    MaintenanceGlobalDictionary(final int column, final long headerKey, final StorageEngineWriter storageEngineWriter,
        final long budgetBytes) {
      final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, storageEngineWriter);
      if (header == null || !header.isDirectoryComplete()) {
        throw new IllegalStateException("global projection column " + column + " has an unreadable value dictionary");
      }
      this.column = column;
      this.headerKey = headerKey;
      this.storageEngineWriter = storageEngineWriter;
      this.baseHeader = header;
      this.baseEntryCount = header.getEntryCount();
      this.budgetBytes = budgetBytes;
    }

    @Override
    public int intern(final String value) {
      Objects.requireNonNull(value, "value must not be null");
      final int encodedLength =
          GlobalValueDictionaryEncoder.utf8LengthCapped(value, GlobalValueDictionaryWriter.MAX_VALUE_BYTES);
      if (encodedLength > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw new IllegalStateException(
            "global projection column " + column + " cannot materialise a UTF-8 value above the safe V0 limit of "
                + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes");
      }
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      return intern(utf8, 0, utf8.length);
    }

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, source.length);
      if (length > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        throw new IllegalStateException(
            "global projection column " + column + " cannot persist a value above the safe V0 limit of "
                + GlobalValueDictionaryWriter.MAX_VALUE_BYTES + " bytes");
      }
      if (additions != null) {
        final int localId = additions.findId(source, offset, length);
        if (localId > 0) {
          return Math.addExact(baseEntryCount, localId);
        }
      }
      final int hotId = hotValues.find(source, offset, length);
      if (hotId > 0) {
        return hotId;
      }
      persistentProbeCount++;
      final int existing = GlobalValueDictionary.probe(headerKey, source, offset, length, storageEngineWriter);
      if (HFT_TELEMETRY_ENABLED) {
        HFT_DICTIONARY_PROBES.incrementAndGet();
      }
      if (existing > 0) {
        hotValues.put(source, offset, length, existing);
        return existing;
      }
      if (existing == GlobalValueDictionary.ID_UNKNOWN) {
        throw new IllegalStateException("global projection column " + column + " cannot probe its value dictionary");
      }
      if (additions == null) {
        additions = new GlobalValueDictionaryWriter(column, budgetBytes,
            GlobalValueDictionaryWriter.AdmissionPolicy.FAIL_CLOSED);
      }
      final int localId = additions.intern(source, offset, length);
      final long id = (long) baseEntryCount + localId;
      if (id > Integer.MAX_VALUE) {
        throw new IllegalStateException("global projection column " + column + " exhausted dictionary ids");
      }
      return (int) id;
    }

    long persistentProbeCount() {
      return persistentProbeCount;
    }

    private long flush() {
      if (additions == null || additions.entryCount() == 0) {
        return headerKey;
      }
      return additions.flushAppend(baseHeader,
          storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage()),
          GlobalValueDictionary.databaseTypeOf(storageEngineWriter), storageEngineWriter, storageEngineWriter.getLog());
    }

    private boolean hasAdditions() {
      return additions != null && additions.entryCount() > 0;
    }

    private long pendingBytes() {
      return additions == null
          ? 0L
          : additions.logicalPersistedBytes();
    }

    void release() {
      if (additions != null) {
        additions.release();
        additions = null;
      }
    }
  }

  /**
   * Write a row group and fold its record-key range into the fence arrays. The segment-slot write
   * encodes once and lets {@link ProjectionIndexHOTStorage#putRowGroupAsColumnSegmentSlots} do the
   * per-segment carry-forward, so an unchanged column segment stays a true no-op (its slot value and
   * overflow page carry forward untouched) and segments that vanished from the rebuilt row group are
   * tombstoned.
   */
  private static long writeRowGroup(final ProjectionIndexHOTStorage storage, final long slot,
      final ProjectionIndexRowGroupPage leaf, final ProjectionIndexFences.Accessor fences,
      final LongOpenHashSet changedLeafSlots, final Long2ObjectOpenHashMap<long[]> changedColumnsByLeaf,
      final long[] changedColumnWords, final boolean keysChanged, final boolean priorSlotExists,
      final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace) {
    // The page mirror and fence metadata are one invariant. Rowless and exception-only leaves carry
    // MAX/MIN sentinels; retaining a deleted key here would manufacture a normal fence for no row.
    final long firstRecordKey = leaf.firstRecordKey();
    final long lastRecordKey = leaf.lastRecordKey();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(leaf, encodeWorkspace);
    final boolean changed = priorSlotExists
        ? storage.putRowGroupAsColumnSegmentSlots(slot, encoded, changedColumnWords, keysChanged)
        : storage.putRowGroupAsColumnSegmentSlots(slot, encoded);
    if (!changed) {
      return 0L;
    }
    if (keysChanged || !priorSlotExists) {
      fences.set((int) slot, firstRecordKey, lastRecordKey);
    }
    changedLeafSlots.add(slot);
    changedColumnsByLeaf.put(slot, changedColumnWords.clone());
    long bytes = encoded.descriptor().length;
    for (final byte[] segment : encoded.segments()) {
      bytes += segment.length;
    }
    return bytes;
  }

  /**
   * Mark the projection stale for a known, explicit load-time admission failure. Ordinary mutation
   * maintenance never calls this: unattributable or inconsistent changes fail the owning transaction
   * instead of silently switching to a second maintenance mode.
   *
   * @param reason what the writer decided and on what grounds; never {@code null}
   */
  private void invalidate(final ProjectionIndexMetadata.StaleReason reason) {
    Objects.requireNonNull(reason, "reason");
    invalidated = true;
    dirtyRecordKeys = null;
    dirtyColumnWordsByRecord = null;
    maintenanceEpoch++;
    // The tombstone rides the write transaction: invisible to readers of
    // committed revisions until commit, discarded entirely on rollback.
    // Nothing else is needed — query-side consumers discover projections
    // through the revision-scoped catalog and pages, so post-tombstone
    // revisions see the stale marker and fall back while earlier revisions
    // keep their own immutable snapshot.
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    // The partial build remains isolated behind this stale marker. It is never overwritten; an
    // operator drops the definition and creates a replacement under a fresh tree id.
    storage.putBlob(0, ProjectionIndexMetadata.staleTombstone(reason).serialize());
  }

  /**
   * Metadata blob of the definition's sub-tree, or {@code null} only when slot 0 is absent. Any
   * non-metadata or unsupported-version payload fails the owning transaction.
   */
  private @Nullable ProjectionIndexMetadata readMetadata(final ProjectionIndexHOTStorage storage) {
    return parseMetadata(storage.getBlob(0));
  }

  /** Read-only writer-visible slot-0 probe for first-notification state classification. */
  private @Nullable ProjectionIndexMetadata readMetadata() {
    return parseMetadata(ProjectionIndexHOTStorage.readBlob(storageEngineWriter, indexDef.getID(), 0L));
  }

  private @Nullable ProjectionIndexMetadata parseMetadata(final byte @Nullable [] payload) {
    if (HFT_TELEMETRY_ENABLED && payload != null) {
      HFT_METADATA_READS.incrementAndGet();
    }
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(payload);
    if (metadata == null && payload != null) {
      throw new IllegalStateException("unsupported or non-metadata projection payload for index " + indexDef.getID());
    }
    return metadata;
  }

  /** Trailing object-key step of each projected field path — the registry column names. */
  public static String[] trailingFieldNames(final IndexDef indexDef) {
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final String[] names = new String[fieldPaths.size()];
    for (int i = 0; i < names.length; i++) {
      String path = fieldPaths.get(i).toString();
      // A SET column is declared at the array layer (`/[]/genres/[]`) but is the column `genres`:
      // its name is the field the elements belong to. Taking the literal last step would name it
      // "[]", which no query ever asks for — the column would be present and permanently
      // unreachable, and the query would fall back to the records without saying why.
      while (path.endsWith("/[]")) {
        path = path.substring(0, path.length() - 3);
      }
      final int slash = path.lastIndexOf('/');
      names[i] = slash < 0
          ? path
          : path.substring(slash + 1);
    }
    return names;
  }
}

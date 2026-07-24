/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Update-time maintenance hook for a projection index, wired through the
 * {@code IndexController} listener lifecycle like the PATH/CAS/NAME
 * listeners — and, like them, maintaining the index INCREMENTALLY.
 *
 * <h2>Two-phase incremental maintenance</h2>
 *
 * <b>Listen phase (per change, hot path):</b> each notification is
 * classified by pathNodeKey against lazily seeded PCR sets; relevant
 * changes resolve the enclosing <em>record</em> (the row's identity) by
 * walking the node's ancestor chain through raw page-layer record reads —
 * no cursor movement, no allocation beyond the dirty-key set — and add the
 * record's nodeKey to a per-transaction dirty set. Nothing is written yet.
 *
 * <b>Apply phase (once, at pre-commit via
 * {@link IndexController#applyPendingIndexMaintenance()}):</b> dirty
 * records are located in their leaves through the per-leaf record-key zone
 * maps, each touched leaf is rebuilt by re-extracting ALL its rows from the
 * transaction's current state (deleted records simply drop out, updated
 * records pick up their new values — extraction semantics are shared 1:1
 * with the bulk builder via {@link ProjectionIndexRowExtractor}), and new
 * records (node keys are monotonically increasing, so inserts always sort
 * after every indexed key) are appended to the tail leaf / fresh leaves.
 * Slot 0's metadata is rewritten with the updated leaf count and the
 * committing revision as the new build revision, which re-keys the
 * catalog's decoded-leaf cache. All writes ride the write transaction —
 * invisible to concurrent readers until commit, discarded on rollback, and
 * historical revisions keep serving their own immutable snapshots.
 *
 * <h2>Always maintained — the rebuild fallback</h2>
 * Like the PATH/CAS/NAME listeners, this listener keeps its index EXACTLY
 * maintained across every operation — there is no invalidation ladder.
 * Changes the incremental patch cannot attribute provably-correctly degrade
 * to an automatic FULL REBUILD inside the same commit (the shared
 * {@link ProjectionIndexBuilder#buildAndPersist} core re-extracts the whole
 * record set from the transaction's current state — always correct, cost
 * bounded by the index size, no manual re-creation involved):
 * <ul>
 *   <li>subtree moves ({@link #structuralChange()} — the moved nodes fire
 *       no per-node notifications, so attribution is impossible);</li>
 *   <li>an unresolvable ancestor chain (record read failure mid-walk);</li>
 *   <li>more than {@code -Dsirix.projection.maxIncrementalRecords} dirty
 *       records in one transaction (patching approaches rebuild cost);</li>
 *   <li>any inconsistency discovered while patching (a dirty record that
 *       cannot be located in any leaf, a missing leaf payload, foreign
 *       metadata under this definition's sub-tree).</li>
 * </ul>
 * Cheap exact cases never rebuild: record-set array instances appearing or
 * disappearing are no-ops (their records notify individually — pre-order on
 * insert, post-order on delete), and a NEW path class matching the root
 * path (an exact-path record set re-appearing, or a descendant pattern
 * widening to another subtree) reseeds the root-PCR set so subsequent
 * notifications attribute normally.
 *
 * <p>The {@link ProjectionIndexMetadata#staleTombstone() stale tombstone}
 * survives only as a CORRUPTION VALVE: if the apply phase <em>and</em> the
 * rebuild both fail with an unexpected exception, slot 0 is overwritten so
 * readers fall back to the always-correct generic pipeline until a manual
 * {@code jn:create-projection-index} re-run — the projection analogue of any
 * other index family surfacing an unexpected page-layer failure.
 *
 * <h2>Hot-path cost</h2>
 * The relevant-PCR sets are seeded LAZILY on the first notification, so
 * write transactions that never touch any node pay nothing beyond object
 * construction at open. After seeding, an irrelevant notification is one
 * {@code LongOpenHashSet#contains}; a relevant one adds an ancestor walk
 * bounded by the record's nesting depth (raw record reads served from the
 * transaction log / page cache).
 */
public final class ProjectionIndexChangeListener implements PathNodeKeyChangeListener {

  private static final LogWrapper LOGGER =
      new LogWrapper(LoggerFactory.getLogger(ProjectionIndexChangeListener.class));

  /**
   * Dirty-record ceiling per transaction. Beyond this, per-leaf patching
   * approaches full-rebuild cost, so the listener degrades to the
   * commit-time full rebuild.
   */
  private static final int MAX_INCREMENTAL_RECORDS =
      Integer.getInteger("sirix.projection.maxIncrementalRecords", 100_000);

  /** Hard bound on the record ancestor walk — malformed chains rebuild. */
  private static final int MAX_ANCESTOR_WALK = 512;

  /** Resolution verdict: change is not under any record — no row affected. */
  private static final long NOT_UNDER_RECORD_SET = -2L;
  /** Resolution verdict: chain unresolvable — degrade to the full rebuild. */
  private static final long UNRESOLVED = -4L;
  /** Sentinel for "parent key not provided by this listen overload". */
  private static final long PARENT_UNKNOWN = Long.MIN_VALUE;

  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryReader pathSummary;
  private final IndexDef indexDef;

  /**
   * Navigation handle over the owning write transaction's current state,
   * used ONLY in the apply phase (pre-commit re-extraction). {@code null}
   * degrades to the legacy invalidation-only contract: the first relevant
   * change tombstones the projection.
   */
  private final @Nullable JsonNodeReadOnlyTrx maintenanceTrx;

  /** Root PCRs of the record set; empty ⇒ conservative mode (everything relevant). */
  private LongOpenHashSet rootPcrs;
  /** Warm cache: pathNodeKeys known to affect this projection. */
  private LongOpenHashSet relevantPcrs;
  /** Warm cache: pathNodeKeys known NOT to affect this projection. */
  private LongOpenHashSet irrelevantPcrs;

  private boolean seeded;

  /**
   * Corruption valve ONLY — set when both the incremental apply and the
   * full rebuild failed unexpectedly (or, in legacy invalidation-only mode
   * without a maintenance transaction, on the first relevant change).
   */
  private boolean invalidated;

  /**
   * The pending state cannot be patched incrementally (subtree move,
   * over-ceiling transaction, unresolvable chain) — {@link #beforeCommit()}
   * performs a full rebuild instead. Dirty-key collection stops while set:
   * the rebuild re-extracts everything anyway.
   */
  private boolean rebuildPending;

  /** Record nodeKeys touched by this transaction; lazily allocated. */
  private @Nullable LongOpenHashSet dirtyRecordKeys;

  /**
   * Positive resolution memo: nodeKey → enclosing record's nodeKey, for
   * nodes proven to lie INSIDE a record. Bulk subtree mutations notify
   * every descendant; without the memo each notification re-walks the full
   * ancestor chain (O(nodes × depth) raw record reads) — with it, a walk
   * stops at the first memoized ancestor. Only positive verdicts are
   * memoized: a NOT_UNDER node can still have record descendants (it may be
   * an ancestor of the record set), so negative memoization would be wrong.
   * Lazily allocated; writes stop at {@link #MEMO_CAP} entries.
   */
  private @Nullable Long2LongOpenHashMap resolvedRecordMemo;

  /** Scratch for the walked ancestor chain (memoized on resolution). */
  private long[] walkChain = new long[32];

  /**
   * Monotone per-listener maintenance epoch: bumped whenever the pending
   * state changes (new dirty record, invalidation) and when an apply pass
   * rewrites leaves. Lets wtx-serving callers cache a decoded handle and
   * revalidate it with one long compare instead of re-decoding per query.
   */
  private long maintenanceEpoch;

  private static final int MEMO_CAP = 1 << 20;
  private static final long MEMO_MISS = Long.MIN_VALUE;

  public ProjectionIndexChangeListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummary, final IndexDef indexDef,
      final @Nullable JsonNodeReadOnlyTrx maintenanceTrx) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexChangeListener requires an IndexType.PROJECTION IndexDef; got "
              + indexDef.getType());
    }
    this.storageEngineWriter = storageEngineWriter;
    this.pathSummary = pathSummary;
    this.indexDef = indexDef;
    this.maintenanceTrx = maintenanceTrx;
  }

  /** Catalogue id of the definition this listener maintains. */
  public int indexDefId() {
    return indexDef.getID();
  }

  /**
   * Subtree MOVES cannot be attributed incrementally: a record moved OUT of
   * the record set still exists (so re-extraction would keep its row), and
   * moved plain containers/value elements fire no per-node notifications at
   * all. Degrade to the commit-time full rebuild — the index stays exactly
   * maintained, like the other index families.
   */
  @Override
  public void structuralChange() {
    scheduleRebuild();
  }

  /**
   * Downgrade from incremental patching to a full rebuild at commit. In
   * legacy invalidation-only mode (no maintenance transaction) a rebuild is
   * impossible — tombstone as before.
   */
  private void scheduleRebuild() {
    if (invalidated || rebuildPending) {
      return;
    }
    if (maintenanceTrx == null) {
      invalidate();
      return;
    }
    rebuildPending = true;
    dirtyRecordKeys = null;
    maintenanceEpoch++;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node,
      final long pathNodeKey) {
    onChange(node.getNodeKey(), node.getKind(), node.getParentKey(), pathNodeKey);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey,
      final NodeKind nodeKind, final long pathNodeKey, final @Nullable QNm name,
      final @Nullable Str value) {
    onChange(nodeKey, nodeKind, PARENT_UNKNOWN, pathNodeKey);
  }

  private void onChange(final long nodeKey, final NodeKind kind, final long parentKey,
      final long pathNodeKey) {
    if (invalidated || rebuildPending) {
      return; // the rebuild re-extracts everything — attribution is moot
    }
    if (!seeded) {
      seed();
    }
    if (maintenanceTrx == null) {
      // Legacy invalidation-only mode: first relevant change tombstones.
      // An unresolvable record set makes every change potentially relevant.
      if (rootPcrs.isEmpty() || isRelevantForInvalidation(pathNodeKey)) {
        invalidate();
      }
      return;
    }
    // NOTE: an EMPTY root-PCR set is fine here — the record set simply has
    // no instances (yet). A change creating one arrives with a new matching
    // path class, which classifyUnseenPcr reseeds as a root.
    if (pathNodeKey > 0) {
      if (irrelevantPcrs.contains(pathNodeKey)) {
        return;
      }
      if (!relevantPcrs.contains(pathNodeKey) && !classifyUnseenPcr(pathNodeKey)) {
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
      scheduleRebuild();
      return;
    }
    markDirty(recordKey);
  }

  /**
   * One-time PCR-set seeding, deferred to the first notification so
   * transactions that never write pay nothing. Seeds the root PCRs, their
   * ancestors (structural deletes of an enclosing container drop the whole
   * record set), and the field PCRs. Primitive iteration throughout — no
   * boxing.
   */
  private void seed() {
    seeded = true;
    rootPcrs = new LongOpenHashSet();
    relevantPcrs = new LongOpenHashSet();
    irrelevantPcrs = new LongOpenHashSet();
    final LongSet roots =
        pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath()));
    for (final LongIterator it = roots.iterator(); it.hasNext(); ) {
      final long pcr = it.nextLong();
      rootPcrs.add(pcr);
      relevantPcrs.add(pcr);
      PathNode node = pathSummary.getPathNodeForPathNodeKey(pcr);
      while (node != null) {
        final long parentKey = node.getParentKey();
        if (parentKey <= 0) {
          break;
        }
        relevantPcrs.add(parentKey);
        node = pathSummary.getPathNodeForPathNodeKey(parentKey);
      }
    }
    for (final Path<QNm> fieldPath : indexDef.getProjectionFields()) {
      final LongSet fieldPcrs = pathSummary.getPCRsForPaths(Set.of(fieldPath));
      for (final LongIterator it = fieldPcrs.iterator(); it.hasNext(); ) {
        relevantPcrs.add(it.nextLong());
      }
    }
  }

  /** Legacy invalidation-mode relevance check (conservative on unknowns). */
  private boolean isRelevantForInvalidation(final long pathNodeKey) {
    // Unknown provenance (document-root replacement, no pathNodeKey) —
    // fail safe and invalidate.
    if (pathNodeKey <= 0) {
      return true;
    }
    if (relevantPcrs.contains(pathNodeKey)) {
      return true;
    }
    if (irrelevantPcrs.contains(pathNodeKey)) {
      return false;
    }
    return classifyUnseenPcr(pathNodeKey);
  }

  /**
   * Classify an unseen PCR (e.g. a brand-new field path created by this
   * transaction) by whether its ancestor chain crosses a record-set root,
   * and cache the verdict so the hot path stays a single set lookup. The
   * new PCR may itself BE a new record-set root — an exact-path record set
   * re-appearing after removal, or a descendant pattern widening to another
   * matching subtree — which the seeded root PCRs cannot know: the root
   * path is re-checked against the new path class and a match RESEEDS the
   * root set, so the new roots' records attribute normally (their node
   * keys are fresh, hence pure appends at apply time).
   */
  private boolean classifyUnseenPcr(final long pathNodeKey) {
    boolean relevant = false;
    PathNode node = pathSummary.getPathNodeForPathNodeKey(pathNodeKey);
    while (node != null) {
      final long parentKey = node.getParentKey();
      if (parentKey <= 0) {
        break;
      }
      if (rootPcrs.contains(parentKey)) {
        relevant = true;
        break;
      }
      node = pathSummary.getPathNodeForPathNodeKey(parentKey);
    }
    if (!relevant && matchesRootPath(pathNodeKey)) {
      rootPcrs.add(pathNodeKey);
      relevantPcrs.add(pathNodeKey);
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
   * Whether the (unseen) path class {@code pathNodeKey} matches the
   * definition's root path (exact or descendant pattern — {@code matches}
   * covers both). Cursor-neutral; an unreadable or failing reconstruction
   * cannot be proven irrelevant, so it degrades to the full rebuild and
   * reports no match.
   */
  private boolean matchesRootPath(final long pathNodeKey) {
    final long savedNodeKey = pathSummary.getNodeKey();
    try {
      if (!pathSummary.moveTo(pathNodeKey)) {
        scheduleRebuild();
        return false;
      }
      final Path<QNm> path = pathSummary.getPath();
      if (path == null) {
        scheduleRebuild();
        return false;
      }
      return indexDef.getProjectionRootPath().matches(path);
    } catch (final RuntimeException e) {
      scheduleRebuild();
      return false;
    } finally {
      pathSummary.moveTo(savedNodeKey);
    }
  }

  /**
   * Resolve the RECORD a changed node belongs to: walk the ancestor chain
   * (raw record reads — no cursor movement) until a node at a record-set
   * root PCR is crossed. Delete notifications fire post-order BEFORE
   * physical removal, so a deleted node's ancestors are still readable at
   * listen time.
   *
   * @return the record's nodeKey, or one of the negative verdicts
   *         ({@link #NOT_UNDER_RECORD_SET} / {@link #UNRESOLVED})
   */
  private long resolveRecordKey(final long nodeKey, final NodeKind kind, long parentKey,
      final long pathNodeKey) {
    if (pathNodeKey > 0 && rootPcrs.contains(pathNodeKey)) {
      if (isArrayLike(kind)) {
        // The record SET's array instance itself appeared or disappeared —
        // no row of its own. Its records are attributed individually: an
        // insert notifies the array BEFORE its records (pre-order), a
        // delete notifies the records BEFORE the array (post-order), so
        // every row lands in the dirty set through its own notification.
        return NOT_UNDER_RECORD_SET;
      }
      // A single-record root (fused object at the root path) IS the record.
      return nodeKey;
    }
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
    for (int depth = 0; depth < MAX_ANCESTOR_WALK; depth++) {
      if (parentKey <= 0) {
        // Reached the document root without crossing a record-set root:
        // the change cannot affect any indexed row. (Deleting a container
        // ABOVE the record set is covered by the post-order notifications
        // of the record-set nodes themselves.) NOT memoized: a node outside
        // every record can still be an ancestor OF the record set, and its
        // descendants' walks must not inherit this verdict.
        return NOT_UNDER_RECORD_SET;
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
        final long recordKey = isArrayLike(parent.getKind()) ? childKey : parentKey;
        memoizeChain(chainLength, recordKey);
        if (recordKey == parentKey && resolvedRecordMemo.size() < MEMO_CAP) {
          resolvedRecordMemo.put(recordKey, recordKey);
        }
        return recordKey;
      }
      childKey = parentKey;
      if (chainLength == walkChain.length) {
        walkChain = Arrays.copyOf(walkChain, walkChain.length * 2);
      }
      walkChain[chainLength++] = parentKey;
      parentKey = parent.getParentKey();
    }
    return UNRESOLVED;
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

  /** PathNodeKey of a raw record, mirroring the rtx dispatch: only name-carrying and array nodes have one. */
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
    try {
      final DataRecord record = storageEngineWriter.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      return record instanceof final ImmutableNode node ? node : null;
    } catch (final RuntimeException e) {
      return null;
    }
  }

  private void markDirty(final long recordKey) {
    if (dirtyRecordKeys == null) {
      dirtyRecordKeys = new LongOpenHashSet();
    }
    if (dirtyRecordKeys.add(recordKey)) {
      maintenanceEpoch++;
    }
    if (dirtyRecordKeys.size() > MAX_INCREMENTAL_RECORDS) {
      // Patching this many leaves approaches rebuild cost — rebuild once
      // at commit instead of tracking further keys.
      scheduleRebuild();
    }
  }

  /**
   * Monotone epoch of this listener's maintenance state — changes whenever
   * the pending dirty set changes, the projection is invalidated, or an
   * apply pass rewrites leaves. Wtx-serving callers cache decoded handles
   * against it: equal epoch (same listener instance) ⇒ the persisted leaves
   * are byte-identical to when the handle was decoded.
   */
  public long maintenanceEpoch() {
    return maintenanceEpoch;
  }

  /**
   * Apply the collected changes to the persisted projection. Invoked once
   * per commit through the uniform {@link ChangeListener} lifecycle
   * ({@link IndexController#applyPendingIndexMaintenance()})
   * BEFORE page serialization, so all writes ride the committing
   * transaction. Incremental patching that discovers an inconsistency
   * degrades to the full rebuild; only an unexpected failure of BOTH paths
   * tombstones (corruption valve) — a stale-but-honest projection beats a
   * wrong one.
   */
  @Override
  public void beforeCommit() {
    final LongOpenHashSet dirty = dirtyRecordKeys;
    dirtyRecordKeys = null;
    if (invalidated || maintenanceTrx == null) {
      return;
    }
    if (!rebuildPending && (dirty == null || dirty.isEmpty())) {
      return;
    }
    try {
      boolean patched = false;
      if (!rebuildPending) {
        try {
          patched = applyIncremental(dirty);
        } catch (final RuntimeException incrementalFailure) {
          // The incremental patch is the OPTIONAL fast path: a throw here must degrade to the
          // rebuild, exactly as a `false` return does, so that only a failure of BOTH paths reaches
          // the tombstone valve (the contract this method's javadoc states). Rebuilding over a
          // partially-written patch is sound: the rebuild re-extracts every record and tombstones
          // orphans. A `false` return already established this for phase-1 writes (slots within the
          // declared count); a THROW can additionally leave fresh row groups ABOVE that count, which
          // is why buildAndPersist probes upward from the declared count rather than trusting it.
          LOGGER.warn("Incremental projection maintenance failed for index " + indexDef.getID()
              + " — degrading to a full rebuild", incrementalFailure);
          patched = false;
        }
      }
      if (!patched) {
        rebuildFully();
      }
      rebuildPending = false;
      // Leaves (possibly) rewritten — cached decodes are stale.
      maintenanceEpoch++;
    } catch (final RuntimeException e) {
      LOGGER.warn("Projection maintenance (incremental and rebuild) failed for index "
          + indexDef.getID() + " — falling back to invalidation", e);
      invalidate();
    }
  }

  /**
   * Full commit-time rebuild over the transaction's current state — the
   * exact-maintenance fallback for changes the incremental patch cannot
   * attribute. Reuses the creation path's build core; a record set with no
   * remaining instances persists as the truthful EMPTY projection. A
   * pre-existing tombstone (corruption valve fired in an earlier commit)
   * is respected — only {@code jn:create-projection-index} resurrects.
   */
  private void rebuildFully() {
    final JsonNodeReadOnlyTrx rtx = maintenanceTrx;
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata meta = readMetadata(storage);
    if (meta == null || meta.isStale()) {
      // No live snapshot to maintain (bench/legacy store, or a valve
      // tombstone from an earlier commit) — nothing to rebuild.
      return;
    }
    final long savedNodeKey = rtx.getNodeKey();
    try {
      ProjectionIndexBuilder.buildAndPersist(indexDef, pathSummary, rtx, storageEngineWriter,
          true);
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        rtx.moveToDocumentRoot();
      }
    }
  }

  /**
   * Patch the persisted leaves for the given dirty records.
   *
   * @return {@code true} when the persisted state is consistent afterwards
   *         (including "nothing to maintain"); {@code false} when an
   *         inconsistency was discovered and the caller must run the full
   *         rebuild instead
   */
  private boolean applyIncremental(final LongOpenHashSet dirty) {
    final JsonNodeReadOnlyTrx rtx = maintenanceTrx;
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata meta = readMetadata(storage);
    if (meta == null || meta.isStale()) {
      // No live snapshot to maintain: a metadata-less (bench/legacy) store,
      // a valve tombstone from an earlier commit, or a def whose build
      // never ran. Nothing to do.
      return true;
    }
    // Layout: both storage layouts are patched in place. Every row-group read/write below is
    // dispatched on this flag — a segment-slot store keys its descriptor at slotKind 0 of the
    // composite key and each column segment at its own slot, so it must NOT go through the
    // descriptor layout's raw-slot I/O. The flag is also carried into the refreshed metadata at the
    // end (it is sticky per store); dropping it there would silently reinterpret every slot.
    final boolean columnSegmentSlotLayout = meta.isColumnSegmentSlotLayout();
    // Shape guard: the persisted snapshot must describe exactly this
    // definition, or patching would splice rows into foreign columns —
    // the rebuild replaces the foreign payloads with this definition's.
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    final String[] defPaths = new String[fieldPaths.size()];
    final byte[] defKinds = new byte[fieldTypes.size()];
    for (int i = 0; i < defPaths.length; i++) {
      defPaths[i] = fieldPaths.get(i).toString();
      defKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(fieldTypes.get(i));
    }
    if (!meta.matches(indexDef.getProjectionRootPath().toString(), defPaths, defKinds)) {
      return false;
    }

    final int rowGroupCount = meta.rowGroupCount();
    // Per-leaf record-key zone maps (slots 1..rowGroupCount), read from the
    // carry-forward fence chunks instead of probing every leaf's head chunk
    // (O(rowGroupCount) HOT descents). A missing/short chunk means the persisted
    // zone map is inconsistent — rebuild rather than trust a partial map.
    // Empty leaves carry the degenerate (MAX_VALUE, MIN_VALUE) range and never
    // match. Non-final: appends grow the arrays.
    final long[][] fences = ProjectionIndexFences.read(storage, rowGroupCount);
    if (fences == null) {
      return false;
    }
    long[] firsts = new long[rowGroupCount + 1];
    long[] lasts = new long[rowGroupCount + 1];
    long globalMaxLast = Long.MIN_VALUE;
    for (int slot = 1; slot <= rowGroupCount; slot++) {
      firsts[slot] = fences[0][slot - 1];
      lasts[slot] = fences[1][slot - 1];
      if (lasts[slot] > globalMaxLast) {
        globalMaxLast = lasts[slot];
      }
    }

    final long[] keys = dirty.toLongArray();
    Arrays.sort(keys);
    // Node keys are monotonically increasing, so records created by this
    // transaction always sort AFTER every indexed key: keys beyond the
    // global max are appends, the rest must live in exactly one leaf.
    int appendFrom = keys.length;
    while (appendFrom > 0 && keys[appendFrom - 1] > globalMaxLast) {
      appendFrom--;
    }

    // Locate the touched leaves. Build-order leaves have ascending,
    // non-overlapping ranges (document order over monotone keys) — a
    // two-pointer merge locates every in-place key in one pass. Should a
    // store ever violate the ascending invariant, fall back to a per-key
    // scan over the in-memory zone maps (bounded, else invalidate).
    final LongArrayList touchedSlots = new LongArrayList();
    final boolean ascending = zoneMapsAscending(firsts, lasts, rowGroupCount);
    if (ascending) {
      int slot = 1;
      long lastTouched = -1;
      for (int i = 0; i < appendFrom; i++) {
        final long k = keys[i];
        while (slot <= rowGroupCount && lasts[slot] < k) {
          slot++;
        }
        if (slot > rowGroupCount || firsts[slot] > k) {
          // In a gap although the key predates the snapshot's max: the
          // record was never indexed — inconsistent, rebuild.
          return false;
        }
        if (slot != lastTouched) {
          touchedSlots.add(slot);
          lastTouched = slot;
        }
      }
    } else {
      if ((long) appendFrom * rowGroupCount > 50_000_000L) {
        return false; // quadratic scan too expensive — rebuild instead
      }
      final LongOpenHashSet touched = new LongOpenHashSet();
      for (int i = 0; i < appendFrom; i++) {
        final long k = keys[i];
        boolean found = false;
        for (int slot = 1; slot <= rowGroupCount; slot++) {
          if (firsts[slot] <= k && k <= lasts[slot]) {
            if (touched.add(slot)) {
              touchedSlots.add(slot);
            }
            found = true;
          }
        }
        if (!found) {
          return false;
        }
      }
      touchedSlots.sort(null);
    }

    final long savedNodeKey = rtx.getNodeKey();
    try {
      final ProjectionIndexRowExtractor extractor =
          new ProjectionIndexRowExtractor(indexDef, pathSummary);
      final LongOpenHashSet located = new LongOpenHashSet();
      int newRowGroupCount = rowGroupCount;
      ProjectionIndexRowGroupPage tail = null;
      long tailSlot = -1;

      // Phase 1 — rebuild each touched leaf by re-extraction: every row is
      // re-read from the transaction's current state, so updates pick up
      // their new values and deleted records drop out.
      for (int t = 0; t < touchedSlots.size(); t++) {
        final long slot = touchedSlots.getLong(t);
        final byte[] raw = columnSegmentSlotLayout
            ? storage.getRowGroupFromColumnSegmentSlots(slot)
            : storage.getRowGroup(slot);
        if (raw == null) {
          return false; // declared leaf missing — inconsistent, rebuild
        }
        final ProjectionIndexRowGroupPage old = ProjectionIndexRowGroupPage.deserialize(raw);
        final ProjectionIndexRowGroupPage rebuilt = new ProjectionIndexRowGroupPage(defKinds);
        final long[] recordKeys = old.recordKeys();
        final int rowCount = old.getRowCount();
        for (int i = 0; i < rowCount; i++) {
          final long recordKey = recordKeys[i];
          if (dirty.contains(recordKey)) {
            located.add(recordKey);
          }
          if (!extractor.extractInto(rtx, recordKey)) {
            continue; // record deleted — drop the row
          }
          extractor.appendTo(rebuilt, recordKey); // capacity: <= old rowCount
        }
        if (slot == rowGroupCount) {
          // Defer the tail leaf's write — appends may still land on it.
          tail = rebuilt;
          tailSlot = slot;
        } else {
          writeRowGroup(storage, slot, rebuilt, firsts, lasts, columnSegmentSlotLayout);
        }
      }

      // Every pre-existing dirty key must have been located in its leaf;
      // an unlocated one means the snapshot never indexed a record it
      // should have — rebuild. (Keys of records both created and deleted
      // by this transaction are in the append partition and are skipped
      // harmlessly there.)
      for (int i = 0; i < appendFrom; i++) {
        if (!located.contains(keys[i])) {
          return false;
        }
      }

      // Phase 2 — append new records to the tail leaf, spilling into fresh
      // leaves. Append keys are ascending and greater than every indexed
      // key, preserving the ascending leaf-range invariant.
      if (appendFrom < keys.length) {
        if (tail == null) {
          if (rowGroupCount == 0) {
            tail = new ProjectionIndexRowGroupPage(defKinds);
            tailSlot = 1;
            newRowGroupCount = 1;
            firsts = Arrays.copyOf(firsts, 2);
            lasts = Arrays.copyOf(lasts, 2);
          } else {
            final byte[] rawTail = columnSegmentSlotLayout
                ? storage.getRowGroupFromColumnSegmentSlots(rowGroupCount)
                : storage.getRowGroup(rowGroupCount);
            if (rawTail == null) {
              return false; // declared tail leaf missing — rebuild
            }
            tail = ProjectionIndexRowGroupPage.deserialize(rawTail);
            tailSlot = rowGroupCount;
          }
        }
        for (int i = appendFrom; i < keys.length; i++) {
          final long recordKey = keys[i];
          if (!extractor.extractInto(rtx, recordKey)) {
            continue; // created and deleted within this transaction
          }
          if (!extractor.appendTo(tail, recordKey)) {
            writeRowGroup(storage, tailSlot, tail, firsts, lasts, columnSegmentSlotLayout);
            newRowGroupCount++;
            if (newRowGroupCount + 1 > firsts.length) {
              // A fresh slot — grow the fence arrays before its write.
              firsts = Arrays.copyOf(firsts, Math.max(newRowGroupCount + 1, firsts.length * 2));
              lasts = Arrays.copyOf(lasts, firsts.length);
            }
            tail = new ProjectionIndexRowGroupPage(defKinds);
            tailSlot = newRowGroupCount;
            extractor.appendTo(tail, recordKey);
          }
        }
      }
      if (tail != null) {
        writeRowGroup(storage, tailSlot, tail, firsts, lasts, columnSegmentSlotLayout);
      }

      // Refresh the metadata: the committing revision becomes the new build
      // revision (re-keying the catalog's decoded-leaf cache). The updated
      // per-leaf fences go to their carry-forward chunks — only the chunks
      // whose leaves actually moved re-persist; the rest are byte-identical
      // no-ops. Maintenance only ever grows rowGroupCount (shrinks go through the
      // full rebuild), so no fence chunk is orphaned here.
      // The layout flag MUST be re-stamped: it is sticky per store, and the public constructor
      // defaults it to the descriptor layout — dropping it here would make every later read
      // reinterpret this store's slot keys under the wrong layout.
      ProjectionIndexMetadata refreshed = new ProjectionIndexMetadata(meta.rootPath(),
          meta.fieldPaths(), meta.fieldNames(), meta.columnKinds(), newRowGroupCount,
          rtx.getRevisionNumber());
      if (columnSegmentSlotLayout) {
        refreshed = refreshed.withColumnSegmentSlotLayout();
      }
      storage.putBlob(0, refreshed.serialize());
      final long[] fenceFirsts = new long[newRowGroupCount];
      final long[] fenceLasts = new long[newRowGroupCount];
      System.arraycopy(firsts, 1, fenceFirsts, 0, newRowGroupCount);
      System.arraycopy(lasts, 1, fenceLasts, 0, newRowGroupCount);
      ProjectionIndexFences.write(storage, newRowGroupCount, fenceFirsts, fenceLasts, rowGroupCount);
      return true;
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        rtx.moveToDocumentRoot();
      }
    }
  }

  /**
   * Write a row group and fold its record-key range into the fence arrays, through whichever
   * storage layout this store uses. The segment-slot write encodes once and lets
   * {@link ProjectionIndexHOTStorage#putRowGroupAsColumnSegmentSlots} do the per-segment
   * carry-forward, so an unchanged column segment stays a true no-op (its slot value and overflow
   * page carry forward untouched) and segments that vanished from the rebuilt row group are
   * tombstoned — the same CoW sharing the descriptor layout gets from {@code putRowGroup}.
   */
  private static void writeRowGroup(final ProjectionIndexHOTStorage storage, final long slot,
      final ProjectionIndexRowGroupPage leaf, final long[] firsts, final long[] lasts,
      final boolean columnSegmentSlotLayout) {
    firsts[(int) slot] = leaf.firstRecordKey();
    lasts[(int) slot] = leaf.lastRecordKey();
    final byte[] raw = leaf.serialize();
    if (columnSegmentSlotLayout) {
      storage.putRowGroupAsColumnSegmentSlots(slot, ProjectionIndexColumnSegmentCodec.encode(raw));
    } else {
      storage.putRowGroup(slot, raw);
    }
  }

  /** Whether the non-empty leaf ranges are ascending and non-overlapping. */
  private static boolean zoneMapsAscending(final long[] firsts, final long[] lasts,
      final int rowGroupCount) {
    long prevLast = Long.MIN_VALUE;
    for (int slot = 1; slot <= rowGroupCount; slot++) {
      if (firsts[slot] > lasts[slot]) {
        continue; // empty leaf — degenerate range
      }
      if (firsts[slot] <= prevLast) {
        return false;
      }
      prevLast = lasts[slot];
    }
    return true;
  }

  /**
   * Corruption valve (and the legacy invalidation-only contract): overwrite
   * slot 0 with the stale tombstone so every reader falls back to the
   * generic pipeline until a manual {@code jn:create-projection-index}
   * re-run. Regular maintenance never calls this — unattributable changes
   * degrade to {@link #rebuildFully()} instead.
   */
  private void invalidate() {
    invalidated = true;
    rebuildPending = false;
    dirtyRecordKeys = null;
    maintenanceEpoch++;
    // The tombstone rides the write transaction: invisible to readers of
    // committed revisions until commit, discarded entirely on rollback.
    // Nothing else is needed — query-side consumers discover projections
    // through the revision-scoped catalog and pages, so post-tombstone
    // revisions see the stale marker and fall back while earlier revisions
    // keep their own immutable snapshot.
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    // Carry the tombstoned store's layout into the marker: the row-group slots survive the
    // tombstone, so a later rebuild must write them back under the SAME layout (see
    // ProjectionIndexMetadata#staleTombstone(boolean)).
    storage.putBlob(0, ProjectionIndexMetadata.staleTombstone(tombstoneLayout(storage)).serialize());
  }

  /**
   * The layout to stamp into a stale tombstone. Prefers the store's own metadata, but falls back to
   * a structural probe of the slot keys when slot 0 is unreadable.
   *
   * <p>{@link #readMetadata} reports "absent" and "corrupt" identically as {@code null}, and this is
   * precisely the path a corrupt slot 0 takes to reach the valve — so trusting {@code null} to mean
   * "descriptor layout" would drop the sticky flag exactly when the store is damaged, sending the
   * next rebuild to the wrong layout and mixing raw-keyed with composite-keyed row groups beyond
   * recovery. The probe reads the surviving row-group slots, which the tombstone does not disturb.</p>
   */
  private boolean tombstoneLayout(final ProjectionIndexHOTStorage storage) {
    final ProjectionIndexMetadata priorMeta = readMetadata(storage);
    return priorMeta != null ? priorMeta.isColumnSegmentSlotLayout()
        : storage.probeColumnSegmentSlotLayout();
  }

  /**
   * Metadata blob of the definition's sub-tree, or {@code null} for absent, legacy-layout, or
   * corrupt slot-0 payloads — every one of which means "no live snapshot to maintain" and
   * degrades to the rebuild/no-op ladder rather than throwing mid-commit.
   *
   * <p>Catches every {@link RuntimeException}, not just {@link IllegalStateException}: the read
   * descends real pages, so it can also raise {@code SirixIOException}. {@link #invalidate()} calls
   * this to recover the store's layout, and the corruption valve must never itself throw — escaping
   * from there would fail the user's commit in exactly the scenario the valve exists to survive
   * (and, in invalidation-only mode, would abort the transaction from the {@code listen} hot path).
   */
  private @Nullable ProjectionIndexMetadata readMetadata(final ProjectionIndexHOTStorage storage) {
    try {
      return ProjectionIndexMetadata.parse(storage.getBlob(0));
    } catch (final RuntimeException legacyCorruptOrIoFailure) {
      return null;
    }
  }

  /** Trailing object-key step of each projected field path — the registry column names. */
  public static String[] trailingFieldNames(final IndexDef indexDef) {
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final String[] names = new String[fieldPaths.size()];
    for (int i = 0; i < names.length; i++) {
      final String path = fieldPaths.get(i).toString();
      final int slash = path.lastIndexOf('/');
      names[i] = slash < 0 ? path : path.substring(slash + 1);
    }
    return names;
  }
}

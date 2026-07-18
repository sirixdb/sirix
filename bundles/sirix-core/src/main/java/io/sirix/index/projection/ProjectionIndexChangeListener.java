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
 * <h2>Fallback ladder</h2>
 * Anything the incremental path cannot handle provably-correctly falls back
 * to the pre-existing invalidation contract — overwriting slot 0 with a
 * {@link ProjectionIndexMetadata#staleTombstone() stale tombstone} so
 * post-commit readers use the generic pipeline until the projection is
 * re-created:
 * <ul>
 *   <li>structural changes to a record SET itself (an array at the root
 *       path inserted/removed);</li>
 *   <li>an unresolvable ancestor chain (record read failure mid-walk);</li>
 *   <li>an unresolvable record-set root path (no PCRs);</li>
 *   <li>more than {@code -Dsirix.projection.maxIncrementalRecords} dirty
 *       records in one transaction (a rebuild is cheaper);</li>
 *   <li>a dirty record that should be indexed but cannot be located in any
 *       leaf (inconsistent snapshot);</li>
 *   <li>any unexpected failure during the apply phase.</li>
 * </ul>
 * The tombstone is written EAGERLY at fallback time (not deferred to
 * pre-commit), so correctness never depends on the apply phase running.
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
   * approaches full-rebuild cost, so the listener falls back to the
   * tombstone (the next {@code jn:create-projection-index} rebuilds).
   */
  private static final int MAX_INCREMENTAL_RECORDS =
      Integer.getInteger("sirix.projection.maxIncrementalRecords", 100_000);

  /** Hard bound on the record ancestor walk — malformed chains fail closed. */
  private static final int MAX_ANCESTOR_WALK = 512;

  /** Resolution verdict: change is not under any record — no row affected. */
  private static final long NOT_UNDER_RECORD_SET = -2L;
  /** Resolution verdict: structural change to a record SET — invalidate. */
  private static final long STRUCTURAL_CHANGE = -3L;
  /** Resolution verdict: chain unresolvable — invalidate (fail closed). */
  private static final long UNRESOLVED = -4L;
  /** Sentinel for "parent key not provided by this listen overload". */
  private static final long PARENT_UNKNOWN = Long.MIN_VALUE;

  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryReader pathSummary;
  private final IndexDef indexDef;

  /**
   * Whether the record-set root is a descendant pattern ({@code //...}).
   * Such projections aggregate over EVERY matching subtree, so a brand-new
   * path class matching the pattern (a whole new record-set container
   * appearing mid-transaction) silently widens the record set — the
   * persisted rows no longer cover the definition and the projection must
   * be invalidated (see {@link #classifyUnseenPcr}).
   */
  private final boolean descendantRootPattern;

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
  private boolean invalidated;

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
    this.descendantRootPattern = indexDef.getProjectionRootPath().toString().contains("//");
  }

  /** Catalogue id of the definition this listener maintains. */
  public int indexDefId() {
    return indexDef.getID();
  }

  /**
   * Subtree MOVES cannot be attributed incrementally: a record moved OUT of
   * the record set still exists (so re-extraction would keep its row), and
   * moved plain containers/value elements fire no per-node notifications at
   * all. Fail closed — tombstone, rebuild on the next create.
   */
  @Override
  public void structuralChange() {
    if (!invalidated) {
      invalidate();
    }
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
    if (invalidated) {
      return;
    }
    if (!seeded) {
      seed();
    }
    // Unresolvable record set — every change is potentially relevant and no
    // record identity can be established: fail safe.
    if (rootPcrs.isEmpty()) {
      invalidate();
      return;
    }
    if (maintenanceTrx == null) {
      // Legacy invalidation-only mode: first relevant change tombstones.
      if (isRelevantForInvalidation(pathNodeKey)) {
        invalidate();
      }
      return;
    }
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
      invalidate();
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
   * and cache the verdict so the hot path stays a single set lookup. For
   * descendant-pattern roots the new PCR may itself BE a new record-set
   * root (a second matching subtree appearing mid-transaction) — the
   * seeded root PCRs cannot know it, so the pattern is re-checked against
   * the new path class and a match invalidates (the persisted rows no
   * longer cover the widened record set).
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
    if (!relevant && descendantRootPattern && matchesRootPattern(pathNodeKey)) {
      invalidate();
      return false;
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
   * definition's descendant root pattern. Cursor-neutral; unreadable or
   * failing reconstructions count as a match — fail closed into
   * invalidation.
   */
  private boolean matchesRootPattern(final long pathNodeKey) {
    final long savedNodeKey = pathSummary.getNodeKey();
    try {
      if (!pathSummary.moveTo(pathNodeKey)) {
        return true;
      }
      final Path<QNm> path = pathSummary.getPath();
      return path == null || indexDef.getProjectionRootPath().matches(path);
    } catch (final RuntimeException e) {
      return true;
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
   *         ({@link #NOT_UNDER_RECORD_SET} / {@link #STRUCTURAL_CHANGE} /
   *         {@link #UNRESOLVED})
   */
  private long resolveRecordKey(final long nodeKey, final NodeKind kind, long parentKey,
      final long pathNodeKey) {
    if (pathNodeKey > 0 && rootPcrs.contains(pathNodeKey)) {
      if (isArrayLike(kind)) {
        // The record SET itself (its array node) was inserted/removed —
        // structural; rows cannot be attributed individually.
        return STRUCTURAL_CHANGE;
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
      // Patching this many leaves approaches rebuild cost — invalidate.
      invalidate();
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
   * transaction. Any failure degrades to the tombstone — a stale-but-honest
   * projection beats a wrong one.
   */
  @Override
  public void beforeCommit() {
    final LongOpenHashSet dirty = dirtyRecordKeys;
    dirtyRecordKeys = null;
    if (invalidated || dirty == null || dirty.isEmpty() || maintenanceTrx == null) {
      return;
    }
    try {
      applyIncremental(dirty);
      // Leaves (possibly) rewritten — cached decodes are stale.
      maintenanceEpoch++;
    } catch (final RuntimeException e) {
      LOGGER.warn("Incremental projection maintenance failed for index "
          + indexDef.getID() + " — falling back to invalidation", e);
      invalidate();
    }
  }

  private void applyIncremental(final LongOpenHashSet dirty) {
    final JsonNodeReadOnlyTrx rtx = maintenanceTrx;
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(storage.get(0));
    if (meta == null || meta.isStale()) {
      // No live snapshot to maintain: a metadata-less (bench/legacy) store,
      // a projection already invalidated in an earlier commit, or a def
      // whose build never ran. Nothing to do.
      return;
    }
    // Shape guard: the persisted snapshot must describe exactly this
    // definition, or patching would splice rows into foreign columns.
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    final String[] defPaths = new String[fieldPaths.size()];
    final byte[] defKinds = new byte[fieldTypes.size()];
    for (int i = 0; i < defPaths.length; i++) {
      defPaths[i] = fieldPaths.get(i).toString();
      defKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(fieldTypes.get(i));
    }
    if (!meta.matches(indexDef.getProjectionRootPath().toString(), defPaths, defKinds)) {
      invalidate();
      return;
    }

    final int leafCount = meta.leafCount();
    // Per-leaf record-key zone maps (slots 1..leafCount), read straight from
    // the metadata's persisted fences — ONE slot-0 read per commit instead
    // of probing every leaf's head chunk (O(leafCount) HOT descents). Empty
    // leaves carry the degenerate (MAX_VALUE, MIN_VALUE) range and never
    // match. Non-final: appends grow the arrays.
    long[] firsts = new long[leafCount + 1];
    long[] lasts = new long[leafCount + 1];
    long globalMaxLast = Long.MIN_VALUE;
    for (int slot = 1; slot <= leafCount; slot++) {
      firsts[slot] = meta.leafFirstRecordKey(slot - 1);
      lasts[slot] = meta.leafLastRecordKey(slot - 1);
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
    final boolean ascending = zoneMapsAscending(firsts, lasts, leafCount);
    if (ascending) {
      int slot = 1;
      long lastTouched = -1;
      for (int i = 0; i < appendFrom; i++) {
        final long k = keys[i];
        while (slot <= leafCount && lasts[slot] < k) {
          slot++;
        }
        if (slot > leafCount || firsts[slot] > k) {
          // In a gap although the key predates the snapshot's max: the
          // record was never indexed — inconsistent, fail closed.
          invalidate();
          return;
        }
        if (slot != lastTouched) {
          touchedSlots.add(slot);
          lastTouched = slot;
        }
      }
    } else {
      if ((long) appendFrom * leafCount > 50_000_000L) {
        invalidate();
        return;
      }
      final LongOpenHashSet touched = new LongOpenHashSet();
      for (int i = 0; i < appendFrom; i++) {
        final long k = keys[i];
        boolean found = false;
        for (int slot = 1; slot <= leafCount; slot++) {
          if (firsts[slot] <= k && k <= lasts[slot]) {
            if (touched.add(slot)) {
              touchedSlots.add(slot);
            }
            found = true;
          }
        }
        if (!found) {
          invalidate();
          return;
        }
      }
      touchedSlots.sort(null);
    }

    final long savedNodeKey = rtx.getNodeKey();
    try {
      final ProjectionIndexRowExtractor extractor =
          new ProjectionIndexRowExtractor(indexDef, pathSummary);
      final LongOpenHashSet located = new LongOpenHashSet();
      int newLeafCount = leafCount;
      ProjectionIndexLeafPage tail = null;
      long tailSlot = -1;

      // Phase 1 — rebuild each touched leaf by re-extraction: every row is
      // re-read from the transaction's current state, so updates pick up
      // their new values and deleted records drop out.
      for (int t = 0; t < touchedSlots.size(); t++) {
        final long slot = touchedSlots.getLong(t);
        final byte[] encoded = storage.get(slot);
        if (encoded == null) {
          invalidate();
          return;
        }
        final ProjectionIndexLeafPage old =
            ProjectionIndexLeafPage.deserialize(ProjectionIndexLeafCodec.decode(encoded));
        final ProjectionIndexLeafPage rebuilt = new ProjectionIndexLeafPage(defKinds);
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
        if (slot == leafCount) {
          // Defer the tail leaf's write — appends may still land on it.
          tail = rebuilt;
          tailSlot = slot;
        } else {
          writeLeaf(storage, slot, rebuilt, firsts, lasts);
        }
      }

      // Every pre-existing dirty key must have been located in its leaf;
      // an unlocated one means the snapshot never indexed a record it
      // should have — fail closed. (Keys of records both created and
      // deleted by this transaction are in the append partition and are
      // skipped harmlessly there.)
      for (int i = 0; i < appendFrom; i++) {
        if (!located.contains(keys[i])) {
          invalidate();
          return;
        }
      }

      // Phase 2 — append new records to the tail leaf, spilling into fresh
      // leaves. Append keys are ascending and greater than every indexed
      // key, preserving the ascending leaf-range invariant.
      if (appendFrom < keys.length) {
        if (tail == null) {
          if (leafCount == 0) {
            tail = new ProjectionIndexLeafPage(defKinds);
            tailSlot = 1;
            newLeafCount = 1;
            firsts = Arrays.copyOf(firsts, 2);
            lasts = Arrays.copyOf(lasts, 2);
          } else {
            final byte[] encoded = storage.get(leafCount);
            if (encoded == null) {
              invalidate();
              return;
            }
            tail = ProjectionIndexLeafPage.deserialize(ProjectionIndexLeafCodec.decode(encoded));
            tailSlot = leafCount;
          }
        }
        for (int i = appendFrom; i < keys.length; i++) {
          final long recordKey = keys[i];
          if (!extractor.extractInto(rtx, recordKey)) {
            continue; // created and deleted within this transaction
          }
          if (!extractor.appendTo(tail, recordKey)) {
            writeLeaf(storage, tailSlot, tail, firsts, lasts);
            newLeafCount++;
            if (newLeafCount + 1 > firsts.length) {
              // A fresh slot — grow the fence arrays before its write.
              firsts = Arrays.copyOf(firsts, Math.max(newLeafCount + 1, firsts.length * 2));
              lasts = Arrays.copyOf(lasts, firsts.length);
            }
            tail = new ProjectionIndexLeafPage(defKinds);
            tailSlot = newLeafCount;
            extractor.appendTo(tail, recordKey);
          }
        }
      }
      if (tail != null) {
        writeLeaf(storage, tailSlot, tail, firsts, lasts);
      }

      // Refresh the metadata: the committing revision becomes the new build
      // revision (re-keying the catalog's decoded-leaf cache), and the
      // updated per-leaf fences ride along for the next commit's location.
      final long[] fenceFirsts = new long[newLeafCount];
      final long[] fenceLasts = new long[newLeafCount];
      System.arraycopy(firsts, 1, fenceFirsts, 0, newLeafCount);
      System.arraycopy(lasts, 1, fenceLasts, 0, newLeafCount);
      storage.put(0, new ProjectionIndexMetadata(meta.rootPath(), meta.fieldPaths(),
          meta.fieldNames(), meta.columnKinds(), newLeafCount,
          rtx.getRevisionNumber(), fenceFirsts, fenceLasts).serialize());
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        rtx.moveToDocumentRoot();
      }
    }
  }

  /** Write a leaf and fold its record-key range into the fence arrays. */
  private static void writeLeaf(final ProjectionIndexHOTStorage storage, final long slot,
      final ProjectionIndexLeafPage leaf, final long[] firsts, final long[] lasts) {
    firsts[(int) slot] = leaf.firstRecordKey();
    lasts[(int) slot] = leaf.lastRecordKey();
    storage.put(slot, ProjectionIndexLeafCodec.encode(leaf.serialize()));
  }

  /** Whether the non-empty leaf ranges are ascending and non-overlapping. */
  private static boolean zoneMapsAscending(final long[] firsts, final long[] lasts,
      final int leafCount) {
    long prevLast = Long.MIN_VALUE;
    for (int slot = 1; slot <= leafCount; slot++) {
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

  private void invalidate() {
    invalidated = true;
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
    storage.put(0, ProjectionIndexMetadata.staleTombstone().serialize());
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

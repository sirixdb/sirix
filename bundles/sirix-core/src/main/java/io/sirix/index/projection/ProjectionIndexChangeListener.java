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
   * and cache the verdict so the hot path stays a single set lookup.
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
    if (relevant) {
      relevantPcrs.add(pathNodeKey);
    } else {
      irrelevantPcrs.add(pathNodeKey);
    }
    return relevant;
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
    long childKey = nodeKey;
    if (parentKey == PARENT_UNKNOWN) {
      final ImmutableNode self = readNode(nodeKey);
      if (self == null) {
        return UNRESOLVED;
      }
      parentKey = self.getParentKey();
    }
    for (int depth = 0; depth < MAX_ANCESTOR_WALK; depth++) {
      if (parentKey <= 0) {
        // Reached the document root without crossing a record-set root:
        // the change cannot affect any indexed row. (Deleting a container
        // ABOVE the record set is covered by the post-order notifications
        // of the record-set nodes themselves.)
        return NOT_UNDER_RECORD_SET;
      }
      final ImmutableNode parent = readNode(parentKey);
      if (parent == null) {
        return UNRESOLVED;
      }
      final long parentPcr = pathNodeKeyOf(parent);
      if (parentPcr > 0 && rootPcrs.contains(parentPcr)) {
        // Crossed the root: under an array-like root the record is the
        // element we came from; a non-array root IS the record.
        return isArrayLike(parent.getKind()) ? childKey : parentKey;
      }
      childKey = parentKey;
      parentKey = parent.getParentKey();
    }
    return UNRESOLVED;
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
    dirtyRecordKeys.add(recordKey);
    if (dirtyRecordKeys.size() > MAX_INCREMENTAL_RECORDS) {
      // Patching this many leaves approaches rebuild cost — invalidate.
      invalidate();
    }
  }

  /**
   * Apply the collected changes to the persisted projection. Invoked once
   * per commit from {@link IndexController#applyPendingIndexMaintenance()}
   * BEFORE page serialization, so all writes ride the committing
   * transaction. Any failure degrades to the tombstone — a stale-but-honest
   * projection beats a wrong one.
   */
  public void applyPending() {
    final LongOpenHashSet dirty = dirtyRecordKeys;
    dirtyRecordKeys = null;
    if (invalidated || dirty == null || dirty.isEmpty() || maintenanceTrx == null) {
      return;
    }
    try {
      applyIncremental(dirty);
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
    // Per-leaf record-key zone maps (slots 1..leafCount). Empty leaves carry
    // the degenerate (MAX_VALUE, MIN_VALUE) range and never match.
    final long[] firsts = new long[leafCount + 1];
    final long[] lasts = new long[leafCount + 1];
    long globalMaxLast = Long.MIN_VALUE;
    for (int slot = 1; slot <= leafCount; slot++) {
      if (!readZoneMap(storage, slot, firsts, lasts)) {
        invalidate();
        return;
      }
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
          writeLeaf(storage, slot, rebuilt);
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
            writeLeaf(storage, tailSlot, tail);
            newLeafCount++;
            tail = new ProjectionIndexLeafPage(defKinds);
            tailSlot = newLeafCount;
            extractor.appendTo(tail, recordKey);
          }
        }
      }
      if (tail != null) {
        writeLeaf(storage, tailSlot, tail);
      }

      // Refresh the metadata: the committing revision becomes the new build
      // revision, which re-keys the catalog's decoded-leaf cache so readers
      // of the new revision decode the patched leaves.
      storage.put(0, new ProjectionIndexMetadata(meta.rootPath(), meta.fieldPaths(),
          meta.fieldNames(), meta.columnKinds(), newLeafCount,
          rtx.getRevisionNumber()).serialize());
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        rtx.moveToDocumentRoot();
      }
    }
  }

  private static void writeLeaf(final ProjectionIndexHOTStorage storage, final long slot,
      final ProjectionIndexLeafPage leaf) {
    storage.put(slot, ProjectionIndexLeafCodec.encode(leaf.serialize()));
  }

  /**
   * Read leaf {@code slot}'s record-key zone map from its head chunk without
   * materialising the full payload (falling back to the full read when a
   * tiny configured chunk size splits the header). Handles both the compact
   * codec form and the raw serialised form.
   */
  private static boolean readZoneMap(final ProjectionIndexHOTStorage storage, final int slot,
      final long[] firsts, final long[] lasts) {
    byte[] head = storage.getChunk(slot, 0);
    if (head != null && head.length < 28) {
      head = storage.get(slot);
    }
    if (head == null || head.length < 24) {
      return false;
    }
    if (getIntLE(head, 0) == ProjectionIndexLeafCodec.COMPACT_MAGIC) {
      if (head.length < 28) {
        return false;
      }
      firsts[slot] = getLongLE(head, 12);
      lasts[slot] = getLongLE(head, 20);
    } else {
      // Raw leaf payload: firstRecordKey/lastRecordKey at offsets 8/16.
      firsts[slot] = getLongLE(head, 8);
      lasts[slot] = getLongLE(head, 16);
    }
    return true;
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

  private static int getIntLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16)
        | ((b[off + 3] & 0xFF) << 24);
  }

  private static long getLongLE(final byte[] b, final int off) {
    return (getIntLE(b, off) & 0xFFFFFFFFL) | ((long) getIntLE(b, off + 4) << 32);
  }

  private void invalidate() {
    invalidated = true;
    dirtyRecordKeys = null;
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

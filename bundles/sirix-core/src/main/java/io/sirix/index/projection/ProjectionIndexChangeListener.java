/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Update-time maintenance hook for a projection index, wired through the
 * {@code IndexController} listener lifecycle like the PATH/CAS/NAME
 * listeners. Projections are bulk-built columnar snapshots — incrementally
 * splicing arbitrary inserts/updates/deletes into 1024-row bit-packed
 * leaves is future work — so the maintenance contract is
 * <b>invalidation</b>: the first change that touches the projection's
 * record set overwrites the persisted metadata slot with a
 * {@link ProjectionIndexMetadata#staleTombstone() stale tombstone}. The
 * tombstone rides the write transaction, so it inherits full
 * transactionality from the page layer — invisible to concurrent readers
 * until commit, discarded on rollback. Query-side consumers
 * ({@link ProjectionIndexCatalog}) read projections through the
 * revision-scoped catalog and pages, exactly like the PATH/CAS/NAME scan
 * paths, so revisions committed after the tombstone simply see a stale
 * projection and fall back to the generic pipeline while earlier revisions
 * keep serving their own (still-correct) snapshot. The catalogued
 * {@link IndexDef} stays in place — re-running
 * {@code jn:create-projection-index} with the same shape rebuilds under the
 * same id.
 *
 * <h2>Hot-path cost</h2>
 * The relevant-PCR sets are seeded LAZILY on the first notification (not in
 * the constructor), so write transactions that never touch any node pay
 * nothing beyond object construction at open. After seeding, each
 * notification is one {@code LongOpenHashSet#contains}; unseen pathNodeKeys
 * (brand-new paths created by the transaction) are classified once by an
 * ancestor walk and cached. After the first relevant change the listener is
 * a no-op for the rest of the transaction.
 */
public final class ProjectionIndexChangeListener implements PathNodeKeyChangeListener {

  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryReader pathSummary;
  private final IndexDef indexDef;

  /** Root PCRs of the record set; empty ⇒ conservative mode (everything relevant). */
  private LongOpenHashSet rootPcrs;
  /** Warm cache: pathNodeKeys known to affect this projection. */
  private LongOpenHashSet relevantPcrs;
  /** Warm cache: pathNodeKeys known NOT to affect this projection. */
  private LongOpenHashSet irrelevantPcrs;

  private boolean seeded;
  private boolean invalidated;

  public ProjectionIndexChangeListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummary, final IndexDef indexDef) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexChangeListener requires an IndexType.PROJECTION IndexDef; got "
              + indexDef.getType());
    }
    this.storageEngineWriter = storageEngineWriter;
    this.pathSummary = pathSummary;
    this.indexDef = indexDef;
  }

  /** Catalogue id of the definition this listener maintains. */
  public int indexDefId() {
    return indexDef.getID();
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node,
      final long pathNodeKey) {
    onChange(pathNodeKey);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey,
      final NodeKind nodeKind, final long pathNodeKey, final @Nullable QNm name,
      final @Nullable Str value) {
    onChange(pathNodeKey);
  }

  private void onChange(final long pathNodeKey) {
    if (invalidated) {
      return;
    }
    if (!seeded) {
      seed();
    }
    if (isRelevant(pathNodeKey)) {
      invalidate();
    }
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

  private boolean isRelevant(final long pathNodeKey) {
    // Unknown provenance (document-root replacement, no pathNodeKey) or an
    // unresolvable record set — fail safe and invalidate.
    if (pathNodeKey <= 0 || rootPcrs.isEmpty()) {
      return true;
    }
    if (relevantPcrs.contains(pathNodeKey)) {
      return true;
    }
    if (irrelevantPcrs.contains(pathNodeKey)) {
      return false;
    }
    // Unseen PCR (e.g. a brand-new field path created by this transaction) —
    // classify by whether its ancestor chain crosses a record-set root, and
    // cache the verdict so the hot path stays a single set lookup.
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

  private void invalidate() {
    invalidated = true;
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

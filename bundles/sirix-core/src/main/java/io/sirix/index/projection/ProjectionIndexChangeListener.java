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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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
 * record set
 * <ul>
 *   <li>uninstalls the projection from the in-memory
 *       {@link ProjectionIndexRegistry} (queries immediately fall back to
 *       the generic scan pipeline, which is always correct), and</li>
 *   <li>overwrites the persisted metadata slot with a
 *       {@link ProjectionIndexMetadata#staleTombstone() stale tombstone},
 *       so a later hydrate in any session refuses the outdated columns and
 *       rebuilds instead.</li>
 * </ul>
 * The catalogued {@link IndexDef} stays in place — re-running
 * {@code jn:create-projection-index} with the same shape rebuilds under the
 * same id.
 *
 * <h2>Hot-path cost</h2>
 * One {@code LongOpenHashSet#contains} per notification once warm: the
 * listener seeds the relevant-PCR set with the root PCRs, their ancestors
 * (deleting an enclosing container drops the record set) and the field
 * PCRs, and lazily classifies unseen pathNodeKeys by walking their
 * path-summary ancestor chain once, caching the verdict. After the first
 * relevant change the listener is a no-op for the rest of the transaction.
 */
public final class ProjectionIndexChangeListener implements PathNodeKeyChangeListener {

  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryReader pathSummary;
  private final IndexDef indexDef;
  private final String resourceKey;
  private final String[] fieldNames;

  /** Root PCRs of the record set; empty ⇒ conservative mode (everything relevant). */
  private final LongOpenHashSet rootPcrs;
  /** Warm cache: pathNodeKeys known to affect this projection. */
  private final LongOpenHashSet relevantPcrs;
  /** Warm cache: pathNodeKeys known NOT to affect this projection. */
  private final LongOpenHashSet irrelevantPcrs;

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
    this.resourceKey =
        storageEngineWriter.getResourceSession().getResourceConfig().getResource().toString();
    this.fieldNames = trailingFieldNames(indexDef);
    this.rootPcrs = new LongOpenHashSet();
    this.relevantPcrs = new LongOpenHashSet();
    this.irrelevantPcrs = new LongOpenHashSet();
    for (final Long pcr : pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath()))) {
      rootPcrs.add(pcr.longValue());
      relevantPcrs.add(pcr.longValue());
    }
    // Ancestors: structural deletes of an enclosing container drop the
    // whole record set — they must invalidate too.
    final long[] roots = rootPcrs.toLongArray();
    for (final long root : roots) {
      PathNode node = pathSummary.getPathNodeForPathNodeKey(root);
      while (node != null) {
        final long parentKey = node.getParentKey();
        if (parentKey <= 0) {
          break;
        }
        relevantPcrs.add(parentKey);
        node = pathSummary.getPathNodeForPathNodeKey(parentKey);
      }
    }
    for (final var fieldPath : indexDef.getProjectionFields()) {
      for (final Long pcr : pathSummary.getPCRsForPaths(Set.of(fieldPath))) {
        relevantPcrs.add(pcr.longValue());
      }
    }
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
    if (isRelevant(pathNodeKey)) {
      invalidate();
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
    ProjectionIndexRegistry.uninstallWildcard(resourceKey, fieldNames);
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

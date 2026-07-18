/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Revision-scoped, catalog-driven access to projection indexes — the
 * projection analogue of how PATH/CAS/NAME scans reach their trees: the
 * resource's {@code Indexes} catalog (of the queried revision) says WHICH
 * projections exist, and the revision's {@code ProjectionIndexPage} sub-tree
 * says WHAT they contain. Because both are versioned by the page layer's
 * copy-on-write, every hard problem is solved by construction:
 * <ul>
 *   <li><b>Revision correctness / time travel</b> — a definition only
 *       exists in catalogs from its commit revision onward, and each
 *       revision's sub-tree is immutable, so an executor bound to revision
 *       R can only ever see projection data that was current at R.</li>
 *   <li><b>Transaction isolation</b> — uncommitted builds and tombstones
 *       ride the write transaction's log and are invisible to readers;
 *       rollback discards them with no compensation logic.</li>
 *   <li><b>Reopen</b> — a fresh process discovers persisted projections on
 *       first query, no re-bootstrap call needed (same as the other index
 *       families).</li>
 * </ul>
 *
 * <p>The only in-memory state is a bounded DECODE CACHE: compact persisted
 * leaves are decoded to the flat scan form once per (resource, definition,
 * revision) and reused across queries. A projection that cannot be served
 * (stale tombstone, shape drift, truncation) caches a negative entry so the
 * probe cost is paid once. Entries are weighed by payload bytes; bound via
 * {@code -Dsirix.projection.cacheBytes} (default 8 GiB).
 *
 * <p>The static {@link ProjectionIndexRegistry} remains as bench/test
 * wiring for stores without catalogued definitions — production lookups go
 * through here first.
 */
public final class ProjectionIndexCatalog {

  private record Key(String resourceKey, int indexDefId, int revision) {
  }

  /** Negative entry: probed and not usable at this (def, revision). */
  private static final ProjectionIndexRegistry.Handle NOT_USABLE =
      new ProjectionIndexRegistry.Handle(new String[0], List.of());

  private static final long CACHE_BYTES =
      Long.parseLong(System.getProperty("sirix.projection.cacheBytes",
          String.valueOf(8L << 30)));

  private static final Cache<Key, ProjectionIndexRegistry.Handle> CACHE = Caffeine.newBuilder()
      .maximumWeight(CACHE_BYTES)
      .<Key, ProjectionIndexRegistry.Handle>weigher((key, handle) -> {
        long bytes = 64;
        for (final byte[] payload : handle.leafPayloads()) {
          bytes += payload == null ? 0 : payload.length;
        }
        return (int) Math.min(Integer.MAX_VALUE, bytes);
      })
      .build();

  private ProjectionIndexCatalog() {
  }

  /** Whether the catalog of {@code revision} holds any projection definition. */
  public static boolean hasProjections(final JsonResourceSession session, final int revision) {
    final JsonIndexController controller = session.getRtxIndexController(revision);
    return controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) > 0;
  }

  /** Whether any catalogued projection of {@code revision} carries {@code field} as a column. */
  public static boolean anyDefCoversField(final JsonResourceSession session, final int revision,
      final String field) {
    final JsonIndexController controller = session.getRtxIndexController(revision);
    for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
      if (!def.isProjectionIndex()) {
        continue;
      }
      for (final String name : ProjectionIndexChangeListener.trailingFieldNames(def)) {
        if (name.equals(field)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Select and load the projection serving {@code requiredFields} at
   * {@code revision}: the catalogued definition with the FEWEST columns
   * covering every required field wins (narrower projections scan less per
   * row). When covering definitions exist over DIFFERENT record-set roots,
   * selection is ambiguous (the caller's source path is not matched against
   * roots yet) and the lookup fails closed to the generic pipeline.
   *
   * @return a usable handle, or {@code null}
   */
  public static ProjectionIndexRegistry.Handle lookupCovering(final JsonResourceSession session,
      final int revision, final String[] requiredFields) {
    final JsonIndexController controller = session.getRtxIndexController(revision);
    IndexDef best = null;
    String[] bestNames = null;
    for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
      if (!def.isProjectionIndex()) {
        continue;
      }
      final String[] names = ProjectionIndexChangeListener.trailingFieldNames(def);
      if (!coversAll(names, requiredFields)) {
        continue;
      }
      if (best != null && !Objects.equals(best.getProjectionRootPath().toString(),
          def.getProjectionRootPath().toString())) {
        return null;
      }
      if (best == null || names.length < bestNames.length) {
        best = def;
        bestNames = names;
      }
    }
    return best == null ? null : load(session, revision, best);
  }

  /**
   * Load (decode-cached) the projection payloads of {@code def} as
   * persisted at {@code revision}. Fail-soft: a stale tombstone, a shape
   * that no longer matches the definition, truncation, or an older wire
   * format all yield {@code null} — the caller falls back to the generic
   * pipeline (or rebuilds, on the creation path).
   */
  public static ProjectionIndexRegistry.Handle load(final JsonResourceSession session,
      final int revision, final IndexDef def) {
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final ProjectionIndexRegistry.Handle handle =
        CACHE.get(new Key(resourceKey, def.getID(), revision),
            key -> loadFromStorage(session, revision, def));
    return handle == NOT_USABLE ? null : handle;
  }

  private static ProjectionIndexRegistry.Handle loadFromStorage(final JsonResourceSession session,
      final int revision, final IndexDef def) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), def.getID());
      if (persisted.isEmpty()) {
        return NOT_USABLE;
      }
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(persisted.get(0));
      if (metadata == null || metadata.isStale()) {
        return NOT_USABLE;
      }
      // Belt and braces: the persisted shape must match the catalogued
      // definition (guards id-reuse over leftover sub-trees after drops).
      final List<Path<QNm>> fieldPaths = def.getProjectionFields();
      final String[] defPaths = new String[fieldPaths.size()];
      for (int i = 0; i < defPaths.length; i++) {
        defPaths[i] = fieldPaths.get(i).toString();
      }
      final byte[] defKinds = new byte[def.getProjectionFieldTypes().size()];
      for (int i = 0; i < defKinds.length; i++) {
        defKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(def.getProjectionFieldTypes().get(i));
      }
      if (!metadata.matches(def.getProjectionRootPath().toString(), defPaths, defKinds)) {
        return NOT_USABLE;
      }
      final int leafCount = metadata.leafCount();
      if (persisted.size() < leafCount + 1) {
        return NOT_USABLE;
      }
      final List<byte[]> decoded = new ArrayList<>(leafCount);
      for (int i = 1; i <= leafCount; i++) {
        decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
      }
      return new ProjectionIndexRegistry.Handle(metadata.rootPath(), metadata.buildRevision(),
          metadata.fieldNames(), decoded, null);
    } catch (final RuntimeException e) {
      // Corrupt payloads must never break the query pipeline — the generic
      // scan path is always correct.
      return NOT_USABLE;
    }
  }

  private static boolean coversAll(final String[] names, final String[] requiredFields) {
    outer:
    for (final String required : requiredFields) {
      for (final String name : names) {
        if (name.equals(required)) {
          continue outer;
        }
      }
      return false;
    }
    return true;
  }

  /** Drop all cached decodes — for test isolation. */
  public static void clearCache() {
    CACHE.invalidateAll();
  }
}

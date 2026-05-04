/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * Bench helper for ScaleBenchMain: builds an (age, active, dept) projection
 * index on the current revision and registers it wildcard in
 * {@link ProjectionIndexRegistry}. Lives in its own file so
 * {@link io.brackit.query.util.path.Path} can be imported without colliding
 * with {@link java.nio.file.Path} that ScaleBenchMain uses extensively.
 */
final class ScaleBenchProjectionSetup {

  private static final String[] FIELD_NAMES = {"age", "active", "dept"};

  private ScaleBenchProjectionSetup() {
  }

  /** IndexDef.getID() for the bench projection — one sub-tree per resource. */
  private static final int INDEX_NUMBER = 0;

  static int installWildcard(final JsonResourceSession session) {
    // Fast path: projection has been persisted previously — hydrate the
    // in-memory registry from the HOT sub-tree. Avoids the 24-minute rebuild
    // the non-persistent path pays at 100 M (shred → close → reopen).
    //
    // iter#13: -Dsirix.projection.inMemoryReencode=true re-serialises each
    // persisted leaf through the current serializer. Intended for perf
    // A/B of a new leaf wire format (e.g. NUMERIC_LONG_FOR_BP upgrade)
    // without paying a 24-min disk reshred + HOT-put loop — the re-encoded
    // leaves are installed in-memory only, persisted bytes stay untouched.
    // Adds ~{millis-per-leaf × leaf-count} to the cold start, in return
    // for query-phase format benefits.
    //
    // iter#13: -Dsirix.projection.forceRebuild=true bypasses the fast path
    // even when a persisted projection exists, re-walks the current
    // revision and streams new leaves through HOT storage. Use this to
    // permanently migrate an existing DB to a new wire format.
    final boolean forceRebuild = Boolean.getBoolean("sirix.projection.forceRebuild");
    final boolean inMemoryReencode = Boolean.getBoolean("sirix.projection.inMemoryReencode");
    // iter#13: -Dsirix.projection.repersistReencoded=true upgrades the
    // persisted leaves in-place by re-serialising them through the current
    // serializer and writing them back to HOT storage. Executes ONCE per
    // DB; next cold run reads the new wire format directly (no reencode
    // overhead). Intended for migrating an existing DB to FOR-BP without
    // paying a full resource reshred.
    final boolean repersistReencoded = Boolean.getBoolean("sirix.projection.repersistReencoded");
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final int revision = session.getMostRecentRevisionNumber();
    if (!forceRebuild) {
      try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
        final List<byte[]> persisted =
            ProjectionIndexHOTStorage.readAll(probeRtx.getStorageEngineReader(), INDEX_NUMBER);
        if (!persisted.isEmpty()) {
          final List<byte[]> reencoded = (inMemoryReencode || repersistReencoded)
              ? reencodeLeaves(persisted)
              : persisted;
          if (repersistReencoded) {
            // Repersist the re-encoded leaves back to HOT storage under a
            // single write trx. Next cold run will skip the reencode step
            // because the on-disk bytes will already be in the new format.
            final long t0 = System.nanoTime();
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              final ProjectionIndexHOTStorage storage =
                  new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
              for (int i = 0; i < reencoded.size(); i++) {
                storage.put(i, reencoded.get(i));
              }
              wtx.commit();
            }
            System.out.printf("# Projection repersisted: %,d leaves in %,d ms%n",
                reencoded.size(), (System.nanoTime() - t0) / 1_000_000L);
          }
          ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, reencoded);
          return reencoded.size();
        }
      }
    }

    // Slow path: index is not yet persisted. Build it from the current
    // revision and stream each leaf into HOT storage in one write trx.
    // Requires the resource to have been shredded with
    // {@code buildPathSummary=true} — without it, {@code openPathSummary}
    // returns an empty / unusable summary and {@link ProjectionIndexBuilder}
    // either silently builds an empty index or NPEs deep in path resolution.
    // Surface that mis-config up front with a clear message instead of a
    // cryptic exit 1 several seconds later.
    if (!session.getResourceConfig().withPathSummary) {
      throw new IllegalStateException(
          "ProjectionIndexSetup: no persisted projection index was found and the "
              + "resource was shredded without a PathSummary — building it now is impossible. "
              + "Re-shred with -DbuildPathSummary=true -Dprojection=true, or reuse an "
              + "existing DB that already has a persisted projection (see -Dsirix.db=<path>).");
    }

    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final Path<QNm> agePath = Path.parse("/[]/age", PathParser.Type.JSON);
    final Path<QNm> activePath = Path.parse("/[]/active", PathParser.Type.JSON);
    final Path<QNm> deptPath = Path.parse("/[]/dept", PathParser.Type.JSON);
    final IndexDef def = IndexDefs.createProjectionIdxDef(
        rootPath,
        List.of(agePath, activePath, deptPath),
        List.of(Type.LON, Type.BOOL, Type.STR),
        INDEX_NUMBER,
        IndexDef.DbType.JSON);

    final List<byte[]> leaves = new ArrayList<>();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);
    }

    // Persist under a single write trx. Putting leaves outside the trx would
    // require setting up a StorageEngineWriter by hand — the node trx gives
    // us one for free plus handles commit.
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < leaves.size(); i++) {
        storage.put(i, leaves.get(i));
      }
      wtx.commit();
    }

    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves);
    return leaves.size();
  }

  /**
   * iter#13 helper — re-serialise every persisted leaf through the
   * current serializer so any wire-format upgrades (e.g. the
   * NUMERIC_LONG_FOR_BP column-kind introduced in iter#13) take effect
   * in-memory without touching the on-disk HOT sub-tree. Each leaf is
   * deserialised, its in-memory columns normalised, then re-serialised.
   *
   * <p>Cost: one pass over all leaves (~100K at 100M scale). Uses a
   * right-sized {@code ArrayList} to avoid growth copies — matches the
   * HFT discipline of the rest of this class.
   */
  private static List<byte[]> reencodeLeaves(final List<byte[]> persisted) {
    final List<byte[]> out = new ArrayList<>(persisted.size());
    for (final byte[] payload : persisted) {
      if (payload == null) {
        out.add(null);
        continue;
      }
      final ProjectionIndexLeafPage leaf = ProjectionIndexLeafPage.deserialize(payload);
      out.add(leaf.serialize());
    }
    return out;
  }
}

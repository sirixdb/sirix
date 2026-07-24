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
import io.sirix.index.projection.ProjectionIndexFences;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexRowGroupCodec;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bench helper for ScaleBenchMain: builds an (age, active, dept) projection
 * index on the current revision and registers it wildcard in
 * {@link ProjectionIndexRegistry}. Lives in its own file so
 * {@link io.brackit.query.util.path.Path} can be imported without colliding
 * with {@link java.nio.file.Path} that ScaleBenchMain uses extensively.
 */
final class ScaleBenchProjectionSetup {

  private static final String[] FIELD_NAMES = {"age", "active", "dept", "city", "amount", "score"};

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
        // Descriptor layout: metadata is the slot-0 blob — read FIRST and unconditionally (a
        // catalogued EMPTY projection has metadata with zero leaves and must hydrate, not
        // rebuild-and-clobber the catalogued definition's metadata); leaves assemble directly
        // to the raw scan form (no decode step). Legacy/corrupt payloads degrade to a rebuild.
        // A STALE tombstone (update-transaction invalidation) falls through to the rebuild
        // path below.
        ProjectionIndexMetadata parsedMetadata = null;
        List<byte[]> compact = new ArrayList<>();
        try {
          parsedMetadata = ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(
              probeRtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
          compact = ProjectionIndexHOTStorage.readAllRowGroups(probeRtx.getStorageEngineReader(),
              INDEX_NUMBER);
        } catch (final IllegalStateException incompatibleLayout) {
          System.out.println("# Persisted projection unreadable (" + incompatibleLayout.getMessage()
              + ") — rebuilding");
          parsedMetadata = null;
          compact = new ArrayList<>();
        }
        final ProjectionIndexMetadata metadata = parsedMetadata;
        final boolean stale = metadata != null && metadata.isStale();
        if (stale) {
          System.out.println("# Persisted projection is stale (invalidated by updates) — rebuilding");
        }
        if ((parsedMetadata != null || !compact.isEmpty()) && !stale) {
          if (metadata != null && compact.size() < metadata.rowGroupCount()) {
            // Same contract as ProjectionIndexCatalog: a truncated store is
            // corrupt — refuse loudly instead of benchmarking partial data.
            throw new IllegalStateException("Persisted projection declares " + metadata.rowGroupCount()
                + " leaves but only " + compact.size()
                + " are stored — rebuild with -Dsirix.projection.forceRebuild=true.");
          }
          final int leafEnd = metadata == null ? compact.size() : metadata.rowGroupCount();
          // Leaves are already in the flat scan form (assembled from segments).
          final List<byte[]> persisted = new ArrayList<>(leafEnd);
          for (int i = 0; i < leafEnd; i++) {
            persisted.add(compact.get(i));
          }
          // Guard the shape: hydrating leaves with a different column count
          // under the bench's static field list would mislabel every column.
          if (!persisted.isEmpty()) {
            final byte[] first = persisted.get(0);
            final int persistedColumns =
                first == null || first.length < 8 ? -1 : ProjectionIndexRowGroupPage.columnCountOf(first);
            if (persistedColumns != FIELD_NAMES.length) {
              throw new IllegalStateException("Persisted projection has " + persistedColumns
                  + " columns but the bench expects " + FIELD_NAMES.length + " "
                  + Arrays.toString(FIELD_NAMES)
                  + " — rebuild it with -Dsirix.projection.forceRebuild=true.");
            }
          }
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
                storage.putRowGroup(i + 1, reencoded.get(i));
              }
              wtx.commit();
            }
            System.out.printf("# Projection repersisted: %,d leaves in %,d ms%n",
                reencoded.size(), (System.nanoTime() - t0) / 1_000_000L);
          }
          // No builder flags on this path — the Handle lazily re-derives the
          // NUMERIC_LONG integrality evidence from the leaves' persisted
          // presence tails, so aggregate fast paths survive close/re-open.
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
    final Path<QNm> cityPath = Path.parse("/[]/city", PathParser.Type.JSON);
    final Path<QNm> amountPath = Path.parse("/[]/amount", PathParser.Type.JSON);
    // score is typically non-integral — its column exists to exercise the
    // builder's integrality flags (value-exact consumers must decline it).
    final Path<QNm> scorePath = Path.parse("/[]/score", PathParser.Type.JSON);
    final List<Path<QNm>> projectedFieldPaths =
        List.of(agePath, activePath, deptPath, cityPath, amountPath, scorePath);
    final IndexDef def = IndexDefs.createProjectionIdxDef(
        rootPath,
        projectedFieldPaths,
        List.of(Type.LON, Type.BOOL, Type.STR, Type.STR, Type.LON, Type.LON),
        INDEX_NUMBER,
        IndexDef.DbType.JSON);

    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }

    // Persist under a single write trx. Putting leaves outside the trx would
    // require setting up a StorageEngineWriter by hand — the node trx gives
    // us one for free plus handles commit.
    // -Dsirix.projection.persist=false skips persistence (in-memory registry
    // only). Historically this was REQUIRED when force-rebuilding a wider
    // column set over an already-persisted projection — the in-place HOT
    // overwrite of larger leaves tripped a chunk-split use-after-close bug.
    // That bug is fixed (grown leaves split + replace correctly; guarded by
    // ProjectionPersistForceRebuildTest and the sirix-core
    // ProjectionIndexHOTStorageGrowingPayloadTest), so the flag is now just
    // an optional fast-iteration knob.
    if (!Boolean.parseBoolean(System.getProperty("sirix.projection.persist", "true"))) {
      ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves,
          builder.numericColumnNonIntegralFlags());
      return leaves.size();
    }
    long rawBytes = 0;
    long compactBytes = 0;
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      // Metadata at slot 0, leaves at 1..N — the SAME layout the controller
      // persists, so slot arithmetic never aliases across rebuilds (a
      // metadata-less rebuild over a metadata store would leave one stale
      // leaf remnant even at unchanged leaf count) and hydrate is bounded
      // by the declared leaf count.
      final String[] fieldPathStrings = new String[projectedFieldPaths.size()];
      for (int i = 0; i < fieldPathStrings.length; i++) {
        fieldPathStrings[i] = projectedFieldPaths.get(i).toString();
      }
      // Per-leaf record-key fences — the maintenance zone map, now persisted as
      // carry-forward chunks (ProjectionIndexFences) rather than inside slot 0.
      final long[] leafFirstKeys = new long[leaves.size()];
      final long[] leafLastKeys = new long[leaves.size()];
      for (int i = 0; i < leaves.size(); i++) {
        final long[] range = ProjectionIndexRowGroupCodec.recordKeyRange(leaves.get(i));
        if (range == null) {
          throw new IllegalStateException("Serialised projection leaf " + i + " carries no header");
        }
        leafFirstKeys[i] = range[0];
        leafLastKeys[i] = range[1];
      }
      final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(rootPath.toString(),
          fieldPathStrings, FIELD_NAMES, builder.columnKinds(), leaves.size(),
          wtx.getRevisionNumber());
      storage.putBlob(0, metadata.serialize());
      ProjectionIndexFences.write(storage, leaves.size(), leafFirstKeys, leafLastKeys, 0);
      for (int i = 0; i < leaves.size(); i++) {
        // Persist in the segmented compact form (per-column FOR/bit-packed segments behind a
        // descriptor) — the flat scan form stays in-memory only; hydrate assembles losslessly.
        final byte[] raw = leaves.get(i);
        final var encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
        rawBytes += raw.length;
        compactBytes += encoded.descriptor().length;
        for (final byte[] segment : encoded.segments()) {
          compactBytes += segment.length;
        }
        storage.putEncodedRowGroup(i + 1, encoded);
      }
      wtx.commit();
    }
    System.out.printf("# Projection persisted: %,d leaves, raw %,d bytes -> compact %,d bytes (%.1f%%)%n",
        leaves.size(), rawBytes, compactBytes, rawBytes == 0 ? 0.0 : 100.0 * compactBytes / rawBytes);

    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves, builder.numericColumnNonIntegralFlags());
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
      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(payload);
      out.add(leaf.serialize());
    }
    return out;
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * Empirical hit-rate measurement for Direction 1 (sub-insert into affected on C2
 * collision) versus its I8-safety fallback (scoped rebuild at insertDepth). Runs the
 * interleaved-insert-delete workload that triggered the historical ~103 C2 firings per
 * the Stage 3a measurement (commit {@code c8ac969af}) and reports the counter pair
 * {@link AbstractHOTIndexWriter#DIRECTION_ONE_SUBINSERT} /
 * {@link AbstractHOTIndexWriter#DIRECTION_ONE_FALLBACK}.
 *
 * <p>Purpose: validate that the I8-safety pre-check is selective -- some C2 firings
 * resolve via sub-insert (cheap), others fall back to a scoped rebuild (expensive but
 * canonical). Both modes are correct; the ratio measures how much work iteration 2
 * saved over iteration 1's always-rebuild fallback.
 *
 * <p>Not a correctness gate -- the canary tests cover that. This probe only reports.
 */
final class Direction1HitRateProbe {

  @TempDir
  Path tempDir;

  @Test
  void measureC2HitRateOnInterleavedWorkload() throws IOException {
    final long subInsertBefore = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get();
    final long fallbackBefore = AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get();
    final long offPathOkBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get();
    final long offPathFallbackBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get();
    final long escAvoidedBefore = AbstractHOTIndexWriter.REBUILD_HEIGHT_ESCALATION_AVOIDED.get();
    final long propI7FbBefore = AbstractHOTIndexWriter.REBUILD_PROPAGATION_I7_FALLBACK.get();
    final long rebuildCallsBefore = AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get();

    final int entriesPerRev = 1_000;
    final int totalRevs = 10;
    final long seed = 0xDEADBEEFL;
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("d1-hitrate");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
          .maxNumberOfRevisionsToRestore(5).build());

      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);

        // Rev 1: bootstrap
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildArray(entriesPerRev, i -> i)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();

        // Revs 2..totalRevs: remove + insert with overlapping ranges (triggers C2 firings)
        for (int rev = 2; rev <= totalRevs; rev++) {
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          final int offset = (rev - 1) * (entriesPerRev / 2);
          final int[] values = new int[entriesPerRev];
          for (int i = 0; i < entriesPerRev; i++) {
            values[i] = offset + rng.nextInt(entriesPerRev * 2);
          }
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildArray(entriesPerRev, i -> values[i])),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    final long subInserts = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get() - subInsertBefore;
    final long fallbacks = AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get() - fallbackBefore;
    final long offPathOk = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get() - offPathOkBefore;
    final long offPathFallback = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get()
        - offPathFallbackBefore;
    final long escAvoided = AbstractHOTIndexWriter.REBUILD_HEIGHT_ESCALATION_AVOIDED.get()
        - escAvoidedBefore;
    final long propI7Fb = AbstractHOTIndexWriter.REBUILD_PROPAGATION_I7_FALLBACK.get()
        - propI7FbBefore;
    final long rebuildCalls = AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get()
        - rebuildCallsBefore;
    final long totalC2 = subInserts + fallbacks;
    final long totalIssueB = offPathOk + offPathFallback;

    System.err.println("=== Direction 1 + Issue B Hit Rate ===");
    System.err.println("  C2 firings (total)   : " + totalC2);
    System.err.println("    -> sub-insert (D1) : " + subInserts
        + (totalC2 > 0 ? String.format(" (%.1f%%)", 100.0 * subInserts / totalC2) : ""));
    System.err.println("    -> scoped rebuild  : " + fallbacks
        + (totalC2 > 0 ? String.format(" (%.1f%%)", 100.0 * fallbacks / totalC2) : ""));
    System.err.println("  Issue B firings      : " + totalIssueB);
    System.err.println("    -> incremental     : " + offPathOk
        + (totalIssueB > 0 ? String.format(" (%.1f%%)", 100.0 * offPathOk / totalIssueB) : ""));
    System.err.println("    -> whole rebuild   : " + offPathFallback
        + (totalIssueB > 0 ? String.format(" (%.1f%%)", 100.0 * offPathFallback / totalIssueB) : ""));
    System.err.println("  rebuildSubtree calls  : " + rebuildCalls
        + " (each scoped to insertDepth, non-escalating since Stage 3c)");
    System.err.println("  Stage 3c propagation  : " + escAvoided
        + " ancestors re-encoded in place (escalations avoided)");
    System.err.println("  Stage 3c I7 fallbacks : " + propI7Fb
        + " (defensive scoped rebuild on partial-update collision)");
    System.err.println("=======================================");
  }

  private static String buildArray(int n, java.util.function.IntUnaryOperator gen) {
    final StringBuilder sb = new StringBuilder(n * 16);
    sb.append("{\"k\":[");
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"v\":").append(gen.applyAsInt(i)).append('}');
    }
    sb.append("]}");
    return sb.toString();
  }
}

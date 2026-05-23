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
 * Scale validation for the residual Stage 4b iter-3 rebuild-subtree floor.
 *
 * <p>Today's verdict ({@code HOT_ROUTING_ENCODING_REWRITE.md} Phase 2) declares the 4
 * residual {@code rebuildSubtree(insertDepth)} firings on the SLIDING_SNAPSHOT canary
 * "structurally necessary" -- bounded per canary, eliminating them requires the routing
 * encoding rewrite. The Stage 3c memo measured the floor on a single 10-revision /
 * 1k-keys-per-rev workload only. This probe runs the same workload SHAPE at three
 * scales to settle whether the residual is bounded by revisions (= O(K) for fixed
 * revisions) or grows per insert (= O(N)). The numbers determine whether the floor is
 * a true structural bound or merely "small because the canary is small".
 *
 * <p>Scales:
 * <ul>
 *   <li>baseline -- 1k keys/rev x 10 revs (matches {@link Direction1HitRateProbe})</li>
 *   <li>more revisions -- 1k keys/rev x 50 revs (5x revisions, keys-per-rev fixed)</li>
 *   <li>more keys per revision -- 5k keys/rev x 10 revs (5x keys, revisions fixed)</li>
 * </ul>
 *
 * <p>Expected interpretation:
 * <ul>
 *   <li>If rebuilds scale ~linearly with revisions and stay flat with keys/rev, the
 *       floor is O(revisions) and is exactly what the verdict claims.</li>
 *   <li>If rebuilds scale with keys/rev too, the floor is O(N) -- in that case the
 *       routing rewrite is forced by perf, not just elegance.</li>
 * </ul>
 *
 * <p>Not a correctness gate. Reports only.
 */
final class Direction1ScaleValidationProbe {

  @TempDir
  Path tempDir;

  @Test
  void measureRebuildScalingAcrossWorkloadShapes() throws IOException {
    System.err.println();
    System.err.println("=== Direction 1 Scale Validation -- Stage 4b iter-3 floor ===");
    System.err.println();
    System.err.printf("%-30s %10s %10s %10s %10s %12s %12s %12s%n",
        "shape", "inserts", "revs", "C2-total", "C2-D1", "C2-fbk", "rebuilds",
        "rebuilds/rev");
    System.err.println("-".repeat(120));

    runOne("baseline",      1_000, 10);
    runOne("more revisions", 1_000, 50);
    runOne("more keys/rev",  5_000, 10);

    System.err.println("-".repeat(120));
    System.err.println("Interpretation:");
    System.err.println("  - rebuilds/rev approximately constant across all three shapes ==>");
    System.err.println("    the residual scales with revisions, not inserts (verdict confirmed).");
    System.err.println("  - rebuilds growing with keys/rev ==> O(N) floor, forces routing rewrite.");
    System.err.println();
  }

  private void runOne(String label, int entriesPerRev, int totalRevs) throws IOException {
    final long subInsertBefore = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get();
    final long fallbackBefore = AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get();
    final long rebuildCallsBefore = AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get();

    final long seed = 0xDEADBEEFL;
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("d1-scale-" + label.replace(' ', '_'));
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

        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildArray(entriesPerRev, i -> i)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();

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
    final long rebuildCalls = AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get() - rebuildCallsBefore;
    final long totalC2 = subInserts + fallbacks;
    final long inserts = (long) entriesPerRev * totalRevs;
    final double rebuildsPerRev = (double) rebuildCalls / totalRevs;

    System.err.printf("%-30s %10d %10d %10d %10d %12d %12d %12.3f%n",
        label, inserts, totalRevs, totalC2, subInserts, fallbacks, rebuildCalls, rebuildsPerRev);
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

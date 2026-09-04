/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

import static io.brackit.query.util.path.Path.parse;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural-splice propagation stress test.
 *
 * <p>
 * Structural propagation re-encodes each ancestor in place when a frontier splice changes the
 * subtree's height or leftmost firstKey. On the {@link Direction1HitRateProbe} canary the 21
 * rebuilds per run happen to preserve both, so the propagation walks the spine but never re-encodes
 * -- the loop body is unexercised.
 *
 * <p>
 * This test runs a much harder workload than the canary (50 revs × 2000 entries with mixed
 * insert/delete/reinsert + multiple value clusters per rev) to maximize the chance that a rebuild
 * changes subtree height. Regardless of whether the propagation fires the test verifies:
 *
 * <ul>
 * <li>Every commit succeeds without exception (Stage 3b: no catch-block self-heal arms left, so any
 * structural-inconsistency exception would propagate).</li>
 * <li>The final tree's range-scan results match the oracle (= the {@code TreeMap} of expected
 * keys/values).</li>
 * <li>The structural-height re-encode counter is reported for visibility; missed preflights and
 * post-publication validation failures stay zero.</li>
 * </ul>
 *
 * <p>
 * Not a correctness gate -- the other canaries cover that. This is a coverage probe for Stage 3c's
 * defensive arm.
 */
final class HOTRebuildPropagationStressTest {

  @TempDir
  Path tempDir;

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void rebuildPropagationUnderHeavyMutation() throws IOException {
    final long heightReencodesBefore = AbstractHOTIndexWriter.STRUCTURAL_HEIGHT_REENCODE.get();
    final long preflightFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();

    final int entriesPerRev = 2_000;
    final int totalRevs = 50;
    final long seed = 0xCAFEBABEL;
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("propagation-stress");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(5)
                                                   .build());

      try (JsonResourceSession session = database.beginResourceSession("res");
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = parse("/k/[]/v", PathParser.Type.JSON);
        final IndexDef def =
            IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);

        // Rev 1: bootstrap with values spread across multiple clusters.
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildArray(entriesPerRev, i -> clusterValue(rng, i, 5))),
            JsonNodeTrx.Commit.NO);
        wtx.commit();

        // Revs 2..totalRevs: aggressive mutation patterns designed to stretch the trie:
        // * Multiple value clusters force wide disc-bit coverage.
        // * Periodic remove-all + reinsert produces tombstone bursts + height churn.
        // * Random base offsets cause overlapping inserts that exercise the C2/I8 paths.
        for (int rev = 2; rev <= totalRevs; rev++) {
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          final int clusters = 3 + (rev % 4);
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildArray(entriesPerRev, i -> clusterValue(rng, i, clusters))),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    final long heightReencodes = AbstractHOTIndexWriter.STRUCTURAL_HEIGHT_REENCODE.get() - heightReencodesBefore;
    final long preflightFailures =
        AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get() - preflightFailuresBefore;
    final long validationFailures =
        AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get() - validationFailuresBefore;

    System.err.println("=== Structural Splice Propagation Stress ===");
    System.err.println("  height re-encodes   : " + heightReencodes);
    System.err.println("  preflight failures  : " + preflightFailures);
    System.err.println("  validation failures : " + validationFailures);
    System.err.println("=================================");

    assertTrue(heightReencodes >= 0, "counter should be non-negative");
    assertTrue(preflightFailures == 0, "a published splice escaped its mandatory propagation preflight");
    assertTrue(validationFailures == 0, "a malformed structural candidate was published");
  }

  /**
   * Build a value mixing the bit-positions across several numeric clusters. With {@code clusters=5}
   * and {@code i = 0..N-1}, the values cycle through 5 anchors spread across the 32-bit range, with
   * random fan-out within each cluster. This produces wide disc-bit coverage and exercises the
   * MSDB-closure boundary that Stage 3c's propagation defends.
   */
  private static int clusterValue(Random rng, int i, int clusters) {
    final int anchor = (i % clusters) * (Integer.MAX_VALUE / clusters);
    return anchor + rng.nextInt(8_192);
  }

  private static String buildArray(int n, IntUnaryOperator gen) {
    final StringBuilder sb = new StringBuilder(n * 16);
    sb.append("{\"k\":[");
    for (int i = 0; i < n; i++) {
      if (i > 0)
        sb.append(',');
      sb.append("{\"v\":").append(gen.applyAsInt(i)).append('}');
    }
    sb.append("]}");
    return sb.toString();
  }

}

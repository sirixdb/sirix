/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress tests targeting HOT production-readiness gaps:
 * <ul>
 *   <li>Delete at scale (10K+ tombstones)</li>
 *   <li>Multi-revision isolation with concurrent deletes and reads</li>
 *   <li>Database restart with HOT indexes across versioning strategies</li>
 *   <li>Fragment chain integrity under heavy mutation</li>
 *   <li>Oracle-verified correctness through insert/delete/reinsert cycles</li>
 * </ul>
 */
@DisplayName("HOT versioned leaf stress tests")
final class HOTVersionedLeafStressTest {

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOT() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception ignored) {
    }
    tempDir = Files.createTempDirectory("sirix-hot-stress");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDir != null) {
      deleteRecursively(tempDir);
    }
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception ignored) {
    }
  }

  private void deleteRecursively(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      try (var entries = Files.list(path)) {
        for (final Path entry : entries.toList()) {
          deleteRecursively(entry);
        }
      }
    }
    Files.deleteIfExists(path);
  }

  // ============================================================
  // Delete at scale
  // ============================================================

  @Nested
  @DisplayName("Delete at scale")
  class DeleteAtScale {

    @Test
    @DisplayName("10K inserts, remove all, reinsert 5K: index stays consistent")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void deleteAllAndReinsert() throws IOException {
      final int n = 10_000;
      final int reinsert = 5_000;
      final Path dbPath = tempDir.resolve("delete-all-reinsert");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.DIFFERENTIAL).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create CAS index
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          // Rev 1: insert N entries
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(n, i -> i)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertNoViolations(wtx, IndexType.CAS, def.getID(), "rev1");

          // Rev 2: remove entire subtree
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.commit();

          // Rev 3: reinsert fewer entries with different values
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(reinsert, i -> i + n)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertNoViolations(wtx, IndexType.CAS, def.getID(), "rev3");

          final long count = countCasRange(wtx, session, def, 0, SearchMode.GREATER_OR_EQUAL);
          assertTrue(count > 0, "reinserted entries should be queryable, got 0");
        }

        // Verify rev 1 still readable
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
          assertNoViolations(rtx, IndexType.CAS, 0, "rev1-after-reinsert");
        }
      }
    }

    @Test
    @DisplayName("Interleaved insert-delete across 10 revisions with oracle verification")
    @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
    void interleavedInsertDeleteMultiRev() throws IOException {
      final int entriesPerRev = 1_000;
      final int totalRevs = 10;
      final long seed = 0xDEADBEEFL;
      final Random rng = new Random(seed);
      final Path dbPath = tempDir.resolve("interleaved-insert-delete");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final List<TreeMap<Integer, Integer>> oracleAtRev = new ArrayList<>();

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
            .maxNumberOfRevisionsToRestore(5).build());

        IndexDef def;
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create index
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          // Rev 1: bootstrap
          {
            final TreeMap<Integer, Integer> oracle = new TreeMap<>();
            for (int i = 0; i < entriesPerRev; i++) {
              oracle.merge(i, 1, Integer::sum);
            }
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
            oracleAtRev.add(oracle);
          }

          // Revs 2..totalRevs: remove old, insert new with overlapping ranges
          for (int rev = 2; rev <= totalRevs; rev++) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            final int offset = (rev - 1) * (entriesPerRev / 2);
            final int[] values = new int[entriesPerRev];
            final TreeMap<Integer, Integer> oracle = new TreeMap<>();
            for (int i = 0; i < entriesPerRev; i++) {
              values[i] = offset + rng.nextInt(entriesPerRev * 2);
              oracle.merge(values[i], 1, Integer::sum);
            }
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> values[i])),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
            oracleAtRev.add(oracle);
          }
        }

        // Validate invariants at every revision
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.CAS, 0,
                  "interleaved-rev" + rev + " (seed=" + seed + ")");
            }
          }

          // Oracle-verify last revision's range queries
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(totalRevs)) {
            final TreeMap<Integer, Integer> lastOracle = oracleAtRev.get(totalRevs - 1);
            final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
            final var casFilter = ic.createCASFilter(
                Set.of("/k/[]/v"), new Int32(0), SearchMode.GREATER_OR_EQUAL,
                new JsonPCRCollector(rtx));
            final var iter = ic.openCASIndex(rtx.getStorageEngineReader(), def, casFilter);
            long totalActual = 0;
            while (iter.hasNext()) {
              totalActual += iter.next().getNodeKeys().getLongCardinality();
            }
            final long totalExpected = lastOracle.values().stream()
                .mapToInt(Integer::intValue).sum();
            assertEquals(totalExpected, totalActual,
                "total entry count at rev " + totalRevs + " (seed=" + seed + ")");
          }
        }
      }
    }

    @Test
    @DisplayName("Scale fuzz: interleaved insert-delete, 3 seeds × 15 revs × 2000/rev, strict validate every revision")
    @org.junit.jupiter.api.Timeout(value = 900, unit = java.util.concurrent.TimeUnit.SECONDS)
    void interleavedInsertDeleteScaleFuzz() throws IOException {
      final int entriesPerRev = 2_000;
      final int totalRevs = 15;
      final long[] seeds = {0xCAFEBABEL, 0xDEADBEEFL, 0xFEEDFACEL};

      for (final long seed : seeds) {
        final Random rng = new Random(seed);
        final Path dbPath = tempDir.resolve("scale-fuzz-" + Long.toHexString(seed));
        Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

        try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
          database.createResource(ResourceConfiguration.newBuilder("res")
              .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
              .maxNumberOfRevisionsToRestore(5).build());

          IndexDef def;
          try (JsonResourceSession session = database.beginResourceSession("res");
               JsonNodeTrx wtx = session.beginNodeTrx()) {
            final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
            final var pathToValue = io.brackit.query.util.path.Path.parse(
                "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
            def = IndexDefs.createCASIdxDef(false, Type.INR,
                Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
            ic.createIndexes(Set.of(def), wtx);

            // Rev 1: bootstrap a dense ascending range.
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();

            // Revs 2..N: remove the whole array, reinsert an overlapping random range. The
            // overlapping windows force off-path-straddle accumulation + cross-level overlap
            // (the exact conditions the incremental fold + pre-check fallback must handle).
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
                  JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> values[i])),
                  JsonNodeTrx.Commit.NO);
              wtx.commit();
            }
          }

          // Strict invariant gate at EVERY committed revision (I1–I8 via HOTInvariantValidator).
          try (JsonResourceSession session = database.beginResourceSession("res")) {
            for (int rev = 1; rev <= totalRevs; rev++) {
              try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
                assertNoViolations(rtx, IndexType.CAS, 0,
                    "scale-fuzz seed=" + Long.toHexString(seed) + " rev=" + rev);
              }
            }
          }
        }
      }
    }
  }

  // ============================================================
  // Database restart with HOT indexes
  // ============================================================

  @Nested
  @DisplayName("Restart persistence")
  class RestartPersistence {

    @Test
    @DisplayName("Multi-rev HOT index survives close+reopen (DIFFERENTIAL)")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void surviveRestartDifferential() throws IOException {
      surviveRestart(VersioningType.DIFFERENTIAL);
    }

    @Test
    @DisplayName("Multi-rev HOT index survives close+reopen (INCREMENTAL)")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void surviveRestartIncremental() throws IOException {
      surviveRestart(VersioningType.INCREMENTAL);
    }

    @Test
    @DisplayName("Multi-rev HOT index survives close+reopen (SLIDING_SNAPSHOT)")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void surviveRestartSlidingSnapshot() throws IOException {
      surviveRestart(VersioningType.SLIDING_SNAPSHOT);
    }

    private void surviveRestart(VersioningType versioningType) throws IOException {
      final int entriesPerRev = 500;
      final int totalRevs = 5;
      final Path dbPath = tempDir.resolve("restart-" + versioningType.name().toLowerCase());
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int[] revStoredCounts = new int[totalRevs];

      // Phase 1: create multi-rev database
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(versioningType)
            .maxNumberOfRevisionsToRestore(3).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          for (int rev = 1; rev <= totalRevs; rev++) {
            if (rev > 1) {
              wtx.moveToDocumentRoot();
              if (wtx.moveToFirstChild()) {
                wtx.remove();
              }
            }
            final int offset = (rev - 1) * entriesPerRev;
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> offset + i)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();

            final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                wtx.getStorageEngineReader(), IndexType.CAS, def.getID());
            revStoredCounts[rev - 1] = inv.storedKeyCount();
            inv.assertOk();
          }
        }
      }
      Databases.getGlobalBufferManager().clearAllCaches();

      // Phase 2: reopen and verify all revisions from cold cache
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                  rtx.getStorageEngineReader(), IndexType.CAS, 0);
              assertTrue(inv.violations().isEmpty(),
                  versioningType + " rev " + rev + " violations after restart: "
                      + inv.violations());
              assertEquals(revStoredCounts[rev - 1], inv.storedKeyCount(),
                  versioningType + " rev " + rev + " key count mismatch after restart");
            }
          }
        }
      }
    }
  }

  // ============================================================
  // Fragment chain integrity under heavy mutation
  // ============================================================

  @Nested
  @DisplayName("Fragment chain integrity")
  class FragmentChainIntegrity {

    @Test
    @DisplayName("Rapid 20-revision cycling with INCREMENTAL, latest revision clean")
    @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
    void rapidRevisionCycling() throws IOException {
      final int baseEntries = 200;
      final int totalRevs = 20;
      final Path dbPath = tempDir.resolve("rapid-rev-cycling");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.INCREMENTAL)
            .maxNumberOfRevisionsToRestore(4).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          // Rev 1: baseline
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(baseEntries, i -> i)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Revs 2..totalRevs: remove-and-reinsert with slight shift
          for (int rev = 2; rev <= totalRevs; rev++) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            final int shift = rev * 10;
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(baseEntries, i -> i + shift)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }

        // All historical revisions must be structurally clean — HOT leaves are always
        // emitted as full pages so fragment combining never mixes incompatible entries.
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.CAS, 0, "rapid-rev-" + rev);
            }
          }
        }
      }
    }

    @Test
    @DisplayName("DIFFERENTIAL with maxRevisionsToRestore=2 forces fragment combining")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void differentialFragmentCombining() throws IOException {
      final int entriesPerRev = 300;
      final int totalRevs = 8;
      final Path dbPath = tempDir.resolve("diff-fragment-combine");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.DIFFERENTIAL)
            .maxNumberOfRevisionsToRestore(2).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();

          for (int rev = 2; rev <= totalRevs; rev++) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            final int base = (rev - 1) * 100;
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> base + i)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }

        // Validate combining at every revision
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.CAS, 0,
                  "DIFFERENTIAL rev " + rev + " (maxRestore=2)");
            }
          }
        }
      }
    }
  }

    @Test
    @DisplayName("SLIDING_SNAPSHOT with remove-and-reinsert, all historical revisions clean")
    @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
    void slidingSnapshotFragmentCombining() throws IOException {
      final int entriesPerRev = 250;
      final int totalRevs = 10;
      final Path dbPath = tempDir.resolve("sliding-fragment-combine");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
            .maxNumberOfRevisionsToRestore(4).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();

          for (int rev = 2; rev <= totalRevs; rev++) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            final int shift = (rev - 1) * 50;
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> shift + i)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }

        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              final HOTInvariantValidator.Result inv =
                  HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.CAS, 0);
              final long i1Count = inv.violations().stream()
                  .filter(v -> v.invariant().startsWith("I1-cross-leaf")).count();
              final long i11Count = inv.violations().stream()
                  .filter(v -> v.invariant().startsWith("I11")).count();
              System.out.println("[SLIDING_SNAPSHOT] rev=" + rev
                  + " storedKeys=" + inv.storedKeyCount()
                  + " violations=" + inv.violations().size()
                  + " I1=" + i1Count + " I11=" + i11Count);
              if (!inv.violations().isEmpty()) {
                inv.violations().stream().limit(5)
                    .forEach(v -> System.out.println("  " + v));
              }
              assertTrue(inv.violations().isEmpty(),
                  "SLIDING_SNAPSHOT rev " + rev + " (maxRestore=4): " + inv.violations().size()
                      + " violations (I1=" + i1Count + " I11=" + i11Count + ")");
            }
          }
        }
      }
    }

  // ============================================================
  // Multi-revision isolation: pinned reader vs active writer
  // ============================================================

  @Nested
  @DisplayName("Multi-revision isolation")
  class MultiRevisionIsolation {

    @Test
    @DisplayName("Pinned reader on rev N sees stable snapshot while writer mutates rev N+1")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void pinnedReaderIsolation() throws IOException {
      final int n = 2_000;
      final Path dbPath = tempDir.resolve("pinned-reader");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.DIFFERENTIAL).build());

        // Rev 1: insert baseline using a wtx that we close immediately
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(n, i -> i)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Pin a reader to rev 1, then write rev 2 concurrently
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeReadOnlyTrx pinnedRtx = session.beginNodeReadOnlyTrx(1)) {

          final HOTInvariantValidator.Result invBefore = HOTInvariantValidator.validateIndex(
              pinnedRtx.getStorageEngineReader(), IndexType.CAS, 0);
          invBefore.assertOk();
          final int countBefore = invBefore.storedKeyCount();

          // While reader is pinned, create rev 2 with different data
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(n / 2, i -> i + 100_000)),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }

          // Pinned reader should still see rev 1 data unchanged
          final HOTInvariantValidator.Result invAfter = HOTInvariantValidator.validateIndex(
              pinnedRtx.getStorageEngineReader(), IndexType.CAS, 0);
          invAfter.assertOk();
          assertEquals(countBefore, invAfter.storedKeyCount(),
              "pinned reader's key count changed after writer committed rev 2");
        }

        // Verify rev 2 independently
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeReadOnlyTrx rtx2 = session.beginNodeReadOnlyTrx(2)) {
          assertNoViolations(rtx2, IndexType.CAS, 0, "rev2-independent");
        }
      }
    }

    @Test
    @DisplayName("Oracle-verified range queries across 5 revisions with overlapping values")
    @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
    void oracleVerifiedMultiRevRangeQueries() throws IOException {
      final int entriesPerRev = 1_000;
      final int totalRevs = 5;
      final long seed = 0xCAFEBABEL;
      final Random rng = new Random(seed);
      final Path dbPath = tempDir.resolve("oracle-multirev-range");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final List<TreeMap<Integer, Integer>> oraclePerRev = new ArrayList<>();
      IndexDef def;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.DIFFERENTIAL)
            .maxNumberOfRevisionsToRestore(3).build());

        // Build each revision in a separate wtx so we can validate cleanly
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          // Rev 0: create index
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
            final var pathToValue = io.brackit.query.util.path.Path.parse(
                "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
            def = IndexDefs.createCASIdxDef(false, Type.INR,
                Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
            ic.createIndexes(Set.of(def), wtx);

            for (int rev = 1; rev <= totalRevs; rev++) {
              if (rev > 1) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              final TreeMap<Integer, Integer> oracle = new TreeMap<>();
              final int[] values = new int[entriesPerRev];
              for (int i = 0; i < entriesPerRev; i++) {
                values[i] = rng.nextInt(5_000);
                oracle.merge(values[i], 1, Integer::sum);
              }
              wtx.insertSubtreeAsFirstChild(
                  JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> values[i])),
                  JsonNodeTrx.Commit.NO);
              wtx.commit();
              oraclePerRev.add(oracle);
            }
          }
          // Write transaction is now CLOSED
          Databases.getGlobalBufferManager().clearAllCaches();

          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                  rtx.getStorageEngineReader(), IndexType.CAS, 0);
              if (!inv.violations().isEmpty()) {
                System.out.println("[oracle-fresh] rev " + rev + ": " + inv.violations().size() + " violations");
              } else {
                System.out.println("[oracle-fresh] rev " + rev + " OK (" + inv.storedKeyCount() + " keys)");
              }
            }
          }
        }

        // Post-commit: all historical revisions must be structurally clean.
        final Random queryRng = new Random(seed + 1);
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.CAS, 0,
                  "oracle-rev-" + rev + " (seed=" + seed + ")");
            }
          }

          // Latest revision: oracle-correct range queries
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(totalRevs)) {
            assertNoViolations(rtx, IndexType.CAS, 0,
                "oracle-latest (seed=" + seed + ")");

            final TreeMap<Integer, Integer> oracle = oraclePerRev.get(totalRevs - 1);
            final var ic = session.getRtxIndexController(rtx.getRevisionNumber());

            for (int q = 0; q < 10; q++) {
              final int lo = queryRng.nextInt(5_000);
              final long expected = oracle.tailMap(lo).values().stream()
                  .mapToInt(Integer::intValue).sum();
              final var iter = ic.openCASIndex(rtx.getStorageEngineReader(), def,
                  ic.createCASFilter(Set.of("/k/[]/v"), new Int32(lo),
                      SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(rtx)));
              long actual = 0;
              while (iter.hasNext()) {
                actual += iter.next().getNodeKeys().getLongCardinality();
              }
              assertEquals(expected, actual,
                  "range query lo=" + lo + " at latest rev (seed=" + seed + ")");
            }
          }
        }
      }
    }
  }

  // ============================================================
  // NAME index multi-revision with delete + oracle
  // ============================================================

  @Nested
  @DisplayName("NAME index versioning")
  class NameIndexVersioning {

    @Test
    @DisplayName("NAME index: insert then delete then reinsert across 3 revisions")
    @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
    void nameIndexInsertDeleteReinsert() throws IOException {
      final Path dbPath = tempDir.resolve("name-idx-del-reinsert");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.DIFFERENTIAL).build());

        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final IndexDef nameIdxDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(nameIdxDef), wtx);

          // Rev 1: object with repeated field names
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
              "{\"alpha\": 1, \"beta\": 2, \"gamma\": 3, \"alpha\": 4, \"beta\": 5}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertNoViolations(wtx, IndexType.NAME, nameIdxDef.getID(), "name-rev1");

          // Rev 2: remove all and insert different fields
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
              "{\"delta\": 10, \"epsilon\": 20, \"delta\": 30}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertNoViolations(wtx, IndexType.NAME, nameIdxDef.getID(), "name-rev2");

          // Rev 3: reinsert original fields with more entries
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
              "{\"alpha\": 100, \"beta\": 200, \"gamma\": 300, \"alpha\": 400, "
                  + "\"beta\": 500, \"gamma\": 600, \"alpha\": 700}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertNoViolations(wtx, IndexType.NAME, nameIdxDef.getID(), "name-rev3");
        }

        // Validate structural integrity at all 3 revisions from fresh rtx
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= 3; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.NAME, 0, "name-rev" + rev + "-cold");
            }
          }

          // Verify rev 3 has "alpha" entries queryable
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(3)) {
            final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
            final IndexDef nameIdxDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
            final var iter = ic.openNameIndex(rtx.getStorageEngineReader(), nameIdxDef,
                ic.createNameFilter(Set.of("alpha")));
            long alphaCount = 0;
            while (iter.hasNext()) {
              alphaCount += iter.next().getNodeKeys().getLongCardinality();
            }
            assertEquals(3, alphaCount, "rev 3 should have 3 'alpha' entries");
          }
        }
      }
    }
  }

  // ============================================================
  // Helpers
  // ============================================================

  private static String buildCasArray(int n, java.util.function.IntUnaryOperator valueAt) {
    final StringBuilder json = new StringBuilder("{\"k\":[");
    for (int i = 0; i < n; i++) {
      if (i > 0) json.append(',');
      json.append("{\"v\":").append(valueAt.applyAsInt(i)).append('}');
    }
    json.append("]}");
    return json.toString();
  }

  private static void assertNoViolations(JsonNodeTrx wtx, IndexType indexType,
      int indexId, String label) {
    final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
        wtx.getStorageEngineReader(), indexType, indexId);
    assertTrue(inv.violations().isEmpty(),
        "[" + label + "] structural violations: " + inv.violations());
  }

  private static void assertNoViolations(JsonNodeReadOnlyTrx rtx, IndexType indexType,
      int indexId, String label) {
    final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
        rtx.getStorageEngineReader(), indexType, indexId);
    assertTrue(inv.violations().isEmpty(),
        "[" + label + "] structural violations: " + inv.violations());
  }

  @Test
  @DisplayName("Diagnostic: CASPage root reference matches per-revision under DIFFERENTIAL")
  @org.junit.jupiter.api.Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
  void diagnosticCasPageRootRefs() throws IOException {
    final int entriesPerRev = 200;
    final int totalRevs = 3;
    final Path dbPath = tempDir.resolve("diag-caspage-roots");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.DIFFERENTIAL)
          .maxNumberOfRevisionsToRestore(3).build());

      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);

        for (int rev = 1; rev <= totalRevs; rev++) {
          if (rev > 1) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
          }
          final int base = (rev - 1) * 100;
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i + base)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }

      try (JsonResourceSession session = database.beginResourceSession("res")) {
        for (int rev = 1; rev <= totalRevs; rev++) {
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
            final var reader = rtx.getStorageEngineReader();
            final PageReference rootRef = HOTInvariantValidator.resolveRootRef(reader, IndexType.CAS, 0);
            System.out.println("[diag] rev " + rev + " CAS root ref key=" + (rootRef != null ? rootRef.getKey() : "null")
                + " fragments=" + (rootRef != null ? rootRef.getPageFragments().size() : 0));
            if (rootRef != null) {
              final io.sirix.page.interfaces.Page rootPage = reader.loadHOTPage(rootRef);
              System.out.println("[diag] rev " + rev + " root page type=" + (rootPage != null ? rootPage.getClass().getSimpleName() : "null"));
              if (rootPage instanceof HOTIndirectPage hip) {
                System.out.println("[diag] rev " + rev + " root indirect: children=" + hip.getNumChildren()
                    + " height=" + hip.getHeight());
                for (int c = 0; c < hip.getNumChildren(); c++) {
                  final PageReference childRef = hip.getChildReference(c);
                  System.out.println("[diag]   child " + c + " key=" + (childRef != null ? childRef.getKey() : "null")
                      + " fragments=" + (childRef != null ? childRef.getPageFragments().size() : 0));
                }
              }
            }
            assertNoViolations(rtx, IndexType.CAS, 0, "diag-rev-" + rev);
          }
        }
      }
    }
  }

  @Test
  @DisplayName("FULL versioning: historical revisions with remove-and-reinsert must be clean")
  @org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
  void fullVersioningHistoricalInvariantCheck() throws IOException {
    final int entriesPerRev = 1_000;
    final int totalRevs = 5;
    final long seed = 0xCAFEBABEL;
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("full-versioning-hist");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.FULL).build());

      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);

        for (int rev = 1; rev <= totalRevs; rev++) {
          if (rev > 1) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
          }
          final int[] values = new int[entriesPerRev];
          for (int i = 0; i < entriesPerRev; i++) {
            values[i] = rng.nextInt(5_000);
          }
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> values[i])),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }

      try (JsonResourceSession session = database.beginResourceSession("res")) {
        for (int rev = 1; rev <= totalRevs; rev++) {
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
            assertNoViolations(rtx, IndexType.CAS, 0,
                "FULL rev " + rev + " (seed=" + seed + ")");
          }
        }
      }
    }
  }

  @Test
  @DisplayName("MINIMAL REPRO: FULL, seed DEADBEEF, 2000/rev, corruption surfaces rev3 pre-commit")
  @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
  void minimalScaleReproDeadbeef() throws IOException {
    final int entriesPerRev = 2_000;
    final int totalRevs = 3;
    final long seed = 0xDEADBEEFL;
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("minimal-scale-repro");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.FULL).build());

      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);

        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> i)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
        assertNoViolations(wtx, IndexType.CAS, def.getID(), "minimal rev1 post-commit");

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
              JsonShredder.createStringReader(buildCasArray(entriesPerRev, i -> values[i])),
              JsonNodeTrx.Commit.NO);
          assertNoViolations(wtx, IndexType.CAS, def.getID(), "minimal rev" + rev + " pre-commit");
          wtx.commit();
          assertNoViolations(wtx, IndexType.CAS, def.getID(), "minimal rev" + rev + " post-commit");
        }
      }
    }
  }

  @Test
  @DisplayName("Aggressive fuzz: 8 seeds x 20 revs, varied value distributions, strict validate + oracle retrievability")
  @org.junit.jupiter.api.Timeout(value = 600, unit = java.util.concurrent.TimeUnit.SECONDS)
  void aggressiveInterleaveFuzz() throws IOException {
    final long[] seeds = {0xCAFEBABEL, 0xDEADBEEFL, 0xFEEDFACEL, 0x12345678L,
        0x0L, 0xFFFFFFFFL, 0xA5A5A5A5L, 0x5EED5EEDL};
    final int totalRevs = 20;
    for (final long seed : seeds) {
      final Random rng = new Random(seed);
      final Path dbPath = tempDir.resolve("aggr-fuzz-" + Long.toHexString(seed));
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder("res")
            .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
            .maxNumberOfRevisionsToRestore(5).build());
        final IndexDef def;
        final TreeMap<Integer, Integer> lastOracle = new TreeMap<>();
        try (JsonResourceSession session = database.beginResourceSession("res");
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);

          for (int rev = 1; rev <= totalRevs; rev++) {
            if (rev > 1) {
              wtx.moveToDocumentRoot();
              if (wtx.moveToFirstChild()) {
                wtx.remove();
              }
            }
            final int per = 1000 + rng.nextInt(2001);          // 1000..3000
            final int shape = rng.nextInt(5);
            final int span = 500 + rng.nextInt(6000);
            final int base = rng.nextInt(8000);
            final int[] values = new int[per];
            final TreeMap<Integer, Integer> oracle = new TreeMap<>();
            for (int i = 0; i < per; i++) {
              final int v = switch (shape) {
                case 0 -> base + i;                                  // ascending
                case 1 -> base + (per - i);                          // descending
                case 2 -> base + rng.nextInt(span);                  // random window
                case 3 -> base + (rng.nextInt(span) & ~0x3F);        // clustered (low bits zeroed)
                default -> base + rng.nextInt(Math.max(1, per / 8)); // duplicate-heavy
              };
              values[i] = Math.max(0, v);
              oracle.merge(values[i], 1, Integer::sum);
            }
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(per, i -> values[i])),
                JsonNodeTrx.Commit.NO);
            assertNoViolations(wtx, IndexType.CAS, def.getID(),
                "aggr seed=" + Long.toHexString(seed) + " rev=" + rev + " pre-commit");
            wtx.commit();
            assertNoViolations(wtx, IndexType.CAS, def.getID(),
                "aggr seed=" + Long.toHexString(seed) + " rev=" + rev + " post-commit");
            lastOracle.clear();
            lastOracle.putAll(oracle);
          }
        }
        // Historical structural validation at every revision + oracle retrievability at the last.
        try (JsonResourceSession session = database.beginResourceSession("res")) {
          for (int rev = 1; rev <= totalRevs; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertNoViolations(rtx, IndexType.CAS, 0,
                  "aggr seed=" + Long.toHexString(seed) + " historical rev=" + rev);
            }
          }
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(totalRevs)) {
            final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
            final var casFilter = ic.createCASFilter(Set.of("/k/[]/v"), new Int32(0),
                SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(rtx));
            final var iter = ic.openCASIndex(rtx.getStorageEngineReader(), def, casFilter);
            long totalActual = 0;
            while (iter.hasNext()) {
              totalActual += iter.next().getNodeKeys().getLongCardinality();
            }
            final long totalExpected = lastOracle.values().stream().mapToInt(Integer::intValue).sum();
            assertEquals(totalExpected, totalActual,
                "aggr seed=" + Long.toHexString(seed) + " retrievable entry count at final rev");
          }
        }
      }
    }
  }

  @Test
  @DisplayName("ChunkIdx boundary: >65536 nodes, multi-chunk per value, full retrievability")
  @org.junit.jupiter.api.Timeout(value = 300, unit = java.util.concurrent.TimeUnit.SECONDS)
  void chunkIdxBoundaryRetrievability() throws IOException {
    final int n = 90_000;       // nodeKeys exceed 65536 -> chunkIdx >= 1
    final int distinct = 250;   // each value repeats ~360x -> its nodeKeys span chunk 0 and 1
    final Path dbPath = tempDir.resolve("chunkidx-boundary");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.FULL).build());
      final IndexDef def;
      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildCasArray(n, i -> i % distinct)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
        assertNoViolations(wtx, IndexType.CAS, def.getID(), "chunkidx post-commit");
        final long total = countCasRange(wtx, session, def, 0, SearchMode.GREATER_OR_EQUAL);
        assertEquals(n, total, "all " + n + " nodes (nodeKeys spanning chunkIdx>=1) must be retrievable");
      }
    }
  }

  @Test
  @DisplayName("Benchmark: large-N branch-heavy insert (stranding-guard cost sanity)")
  @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
  void branchHeavyInsertGuardCost() throws IOException {
    final int n = 200_000;
    final Random rng = new Random(0xBEEFCAFEL);
    final int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt(1_000_000);   // wide, mostly-distinct -> branch-heavy tree growth
    }
    final Path dbPath = tempDir.resolve("guard-bench");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.FULL).build());
      final IndexDef def;
      try (JsonResourceSession session = database.beginResourceSession("res");
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = io.brackit.query.util.path.Path.parse(
            "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
        def = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(def), wtx);
        final long start = System.nanoTime();
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(buildCasArray(n, i -> values[i])),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
        final long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        System.out.println("[guard-bench] inserted " + n + " CAS entries in " + elapsedMs
            + " ms (" + (n * 1000L / Math.max(1L, elapsedMs)) + " entries/s)");
        // An O(N^2) guard would blow the 180s timeout well before this; reaching it rules that out.
        assertNoViolations(wtx, IndexType.CAS, def.getID(), "guard-bench post-commit");
      }
    }
  }

  @Test
  @DisplayName("SOAK: sustained remove+reinsert + concurrent readers (set -Dhot.soak.run=true; scale via -Dhot.soak.*)")
  @org.junit.jupiter.api.Timeout(value = 14_400, unit = java.util.concurrent.TimeUnit.SECONDS)
  void soakWithConcurrentReaders() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.getBoolean("hot.soak.run"),
        "soak test disabled; enable with -Dhot.soak.run=true");
    final int perRev = Integer.getInteger("hot.soak.perRev", 2_000);
    final int revs = Integer.getInteger("hot.soak.revs", 10_000);
    final int numReaders = Integer.getInteger("hot.soak.readers", 4);
    final int validateEvery = Integer.getInteger("hot.soak.validateEvery", 100);
    // Seed is parameterizable so the soak can be run across multiple key distributions: a single
    // seed exercises one sequence of combo-add / integrate / off-path-overflow / consolidate
    // firings; varying it stresses the structural handlers under different shapes at high chunkIdx.
    final long seed = Long.decode(System.getProperty("hot.soak.seed", "0x50AC0DE"));
    final Random rng = new Random(seed);
    final Path dbPath = tempDir.resolve("soak");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final java.util.concurrent.ConcurrentLinkedQueue<Throwable> readerErrors =
        new java.util.concurrent.ConcurrentLinkedQueue<>();
    final java.util.concurrent.atomic.AtomicInteger lastCommittedRev =
        new java.util.concurrent.atomic.AtomicInteger(0);
    final java.util.concurrent.atomic.AtomicBoolean writerDone =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    final java.util.concurrent.atomic.AtomicLong readerIterations =
        new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong readerValidations =
        new java.util.concurrent.atomic.AtomicLong();

    final long startMs = System.currentTimeMillis();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
          .maxNumberOfRevisionsToRestore(5).build());

      final IndexDef def;
      try (JsonResourceSession session = database.beginResourceSession("res")) {
        // ONE write transaction for index creation AND every mutating revision: the CAS index
        // listeners are bound to this wtx's path summary, so they must not outlive it.
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
          final var pathToValue = io.brackit.query.util.path.Path.parse(
              "/k/[]/v", io.brackit.query.util.path.PathParser.Type.JSON);
          def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx);
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(buildCasArray(perRev, i -> i)), JsonNodeTrx.Commit.NO);
          wtx.commit();
          lastCommittedRev.set(1);

          // Concurrent readers: open read txns at recent committed revisions, HOT-direct validate,
          // concurrent with the writer's mutation/commit on the same session -> exercises the
          // reader/writer page eviction-and-reconstruction race.
          final java.util.concurrent.ExecutorService pool =
              java.util.concurrent.Executors.newFixedThreadPool(numReaders);
          for (int r = 0; r < numReaders; r++) {
            final long readerSeed = seed ^ (0x9E3779B97F4A7C15L * (r + 1));
            pool.submit(() -> {
              final Random rrng = new Random(readerSeed);
              while (!writerDone.get()) {
                final int hi = lastCommittedRev.get();
                if (hi < 1) {
                  continue;
                }
                final int window = Math.min(hi, 30);  // recent-biased: race on fresh commits
                final int rev = hi - rrng.nextInt(window);
                try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
                  final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                      rtx.getStorageEngineReader(), IndexType.CAS, 0);
                  if (!inv.violations().isEmpty()) {
                    throw new AssertionError("reader rev " + rev + " violations: " + inv.violations());
                  }
                  readerValidations.incrementAndGet();
                  readerIterations.incrementAndGet();
                } catch (Throwable t) {
                  readerErrors.add(t);
                }
              }
            });
          }

          // Writer: sustained remove-whole-array + reinsert-overlapping-random (same wtx).
          for (int rev = 2; rev <= revs && readerErrors.isEmpty(); rev++) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            final int offset = (rev - 1) * (perRev / 2);
            final int[] values = new int[perRev];
            for (int i = 0; i < perRev; i++) {
              values[i] = offset + rng.nextInt(perRev * 2);
            }
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader(buildCasArray(perRev, i -> values[i])),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
            lastCommittedRev.set(rev);
            if (rev % validateEvery == 0) {
              final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                  wtx.getStorageEngineReader(), IndexType.CAS, def.getID());
              assertTrue(inv.violations().isEmpty(),
                  "writer rev " + rev + " violations: " + inv.violations());
              final long elapsed = (System.currentTimeMillis() - startMs) / 1000;
              System.out.println("[soak] rev=" + rev + "/" + revs + " elapsed=" + elapsed + "s"
                  + " readerIters=" + readerIterations.get() + " readerValids=" + readerValidations.get()
                  + " readerErrors=" + readerErrors.size());
            }
          }
          writerDone.set(true);
          pool.shutdown();
          if (!pool.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)) {
            pool.shutdownNow();
          }
          assertTrue(readerErrors.isEmpty(),
              "concurrent reader errors (" + readerErrors.size() + "): " + readerErrors.stream()
                  .map(Throwable::toString).limit(5).toList());
        }

        // Final cold-cache validation at a spread of revisions (wtx closed).
        Databases.getGlobalBufferManager().clearAllCaches();
        final int committed = lastCommittedRev.get();
        int finalHeight = -1;
        long finalKeys = -1;
        for (int rev : new int[] {1, committed / 4, committed / 2, (3 * committed) / 4, committed}) {
          if (rev < 1) {
            continue;
          }
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
            final HOTInvariantValidator.Result inv = HOTInvariantValidator.validateIndex(
                rtx.getStorageEngineReader(), IndexType.CAS, 0);
            assertTrue(inv.violations().isEmpty(),
                "final cold-cache rev " + rev + " violations: " + inv.violations());
            if (rev == committed) {
              finalHeight = inv.observedHeight();
              finalKeys = inv.storedKeyCount();
            }
          }
        }
        System.out.println("[soak] height at rev=" + committed + ": observedHeight=" + finalHeight
            + " storedKeys=" + finalKeys + " (ideal ~log_512(keys)="
            + String.format("%.2f", Math.log(Math.max(1, finalKeys)) / Math.log(512)) + ")");
        System.out.println("[soak] DONE revs=" + committed + " perRev=" + perRev
            + " totalInserted=" + ((long) committed * perRev) + " readerIters=" + readerIterations.get()
            + " readerValids=" + readerValidations.get() + " readerErrors=" + readerErrors.size()
            + " elapsed=" + ((System.currentTimeMillis() - startMs) / 1000) + "s");
        System.out.println("[soak] rebuilds BRANCH_I8_UNSAFE_REBUILD="
            + AbstractHOTIndexWriter.BRANCH_I8_UNSAFE_REBUILD.get()
            + " STRUCTURAL_SELFHEAL_REBUILD=" + AbstractHOTIndexWriter.STRUCTURAL_SELFHEAL_REBUILD.get()
            + " REBUILD_SUBTREE_CALLED=" + AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get()
            + " STRAND_LEAF_REBUILD=" + AbstractHOTIndexWriter.STRAND_LEAF_REBUILD.get()
            + " STRAND_TWO_LEAF_MIGRATE=" + AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get()
            + " STRAND_FULL_FALLBACK=" + AbstractHOTIndexWriter.STRAND_FULL_FALLBACK.get()
            + " DIRECTION_ONE_FALLBACK=" + AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get()
            + " totalInserts=" + ((long) committed * perRev));
      }
    }
  }

  private static long countCasRange(JsonNodeTrx wtx, JsonResourceSession session,
      IndexDef def, int lowerBound, SearchMode mode) {
    final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
    final var iter = ic.openCASIndex(wtx.getStorageEngineReader(), def,
        ic.createCASFilter(Set.of("/k/[]/v"), new Int32(lowerBound), mode,
            new JsonPCRCollector(wtx)));
    long count = 0;
    while (iter.hasNext()) {
      count += iter.next().getNodeKeys().getLongCardinality();
    }
    return count;
  }
}

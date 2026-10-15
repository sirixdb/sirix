package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A committed writer must not traverse the next epoch's path summary until it is used. */
@Isolated
final class SuccessorPathSummaryWorkBudgetTest {
  private static final WorkCapture PATH_WORK = WorkCapture.of(EngineWorkCounters.REPLAY_PATH_STEPS);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void commitAndCloseDoNotBuildAnUnusedSuccessorSummary(final VersioningType versioning) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"a\":1,\"b\":2}]"), JsonNodeTrx.Commit.NO);
        PATH_WORK.run(trx::commit)
                 .assertZero(EngineWorkCounters.REPLAY_PATH_STEPS,
                     "commit eagerly traversed a successor path summary that no caller has requested");
        assertEquals(2, trx.getRevisionNumber());
        PATH_WORK.run(trx::getPathSummary)
                 .assertAtLeast(EngineWorkCounters.REPLAY_PATH_STEPS, 1,
                     "first use must load the successor summary, proving that path traversal is counted");
        PATH_WORK.run(trx::getPathSummary)
                 .assertZero(EngineWorkCounters.REPLAY_PATH_STEPS,
                     "a second access rebuilt the same successor path summary");
        assertEquals(1, trx.getPathSummary().findPathsByLocalName("a").size());
        assertEquals(1, trx.getPathSummary().findPathsByLocalName("b").size());
        PATH_WORK.run(trx::commit)
                 .assertZero(EngineWorkCounters.REPLAY_PATH_STEPS,
                     "an empty commit traversed its unused successor summary");
        PATH_WORK.run(trx::close)
                 .assertZero(EngineWorkCounters.REPLAY_PATH_STEPS, "close materialized an unused path summary");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void firstEditAfterCommitAndRevertKeepsNamesStatisticsAndHistoryExact(final VersioningType versioning) {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .buildPathSummary(true)
                                                   .buildPathStatistics(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"a\":1}]"), JsonNodeTrx.Commit.NO);
        trx.moveToDocumentRoot();
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToFirstChild());
        final long field = trx.getNodeKey();
        trx.commit();
        assertTrue(trx.moveTo(field));
        // This mutation, not an explicit summary access, must trigger initialization.
        trx.setObjectKeyName("b");
        trx.setNumberValue(2);
        trx.commit();
        trx.revertTo(1);
        assertTrue(trx.moveTo(field));
        trx.setNumberValue(3);
        trx.commit();
        // The predecessor summary may be unopened when rollback installs another writer.
        trx.rollback();
        assertSummary(trx.getPathSummary(), "a", "b", 3);
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      for (int revision = 1; revision <= 3; revision++) {
        try (final PathSummaryReader reader = session.openPathSummary(revision)) {
          assertSummary(reader, revision == 2
              ? "b"
              : "a",
              revision == 2
                  ? "a"
                  : "b",
              revision);
        }
      }
    }
  }

  private static void assertSummary(final PathSummaryReader reader, final String present, final String absent,
      final long value) {
    assertFalse(reader.containsLocalName(absent));
    final var paths = reader.findPathsByLocalName(present);
    assertEquals(1, paths.size());
    final var path = paths.getFirst();
    assertEquals(1, path.getReferences());
    assertEquals(1, path.getStatsValueCount());
    assertTrue(path.isStatsSumTrustworthy());
    assertEquals(value, path.getStatsSum());
  }
}

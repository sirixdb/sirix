package io.sirix.index.aggregate;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseNotifications;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.changelog.TemporalAggregateSummaryFolds;
import io.sirix.io.StorageType;
import io.sirix.utils.CommitTimestamps;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest.assertSum;
import static io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest.declareSimple;
import static io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest.setNumber;
import static io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest.state;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/** TXN-310/612: derived resources use their own clock and retain source timestamps in mappings. */
final class DerivedIndexMaintainerTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(30)
  void historyOwnerCanTruncateWhileDerivedCleanupWaits(final boolean databaseClose) throws Exception {
    final var path = directory.toPath().resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        writer.commit();
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        final DatabaseNotifications notifications = ((AbstractResourceSession<?, ?>) session).getNotifications();
        final CountDownLatch quiescent = new CountDownLatch(1);
        final FutureTask<Void> cleanup = new FutureTask<>(() -> {
          try (var pause = notifications.retire()) {
            quiescent.countDown();
            if (databaseClose) {
              database.close();
            } else {
              resource.close();
            }
          }
          return null;
        });
        final Thread cleaner = new Thread(cleanup);
        resource.lockHistory();
        try {
          cleaner.start();
          assertTrue(quiescent.await(10, TimeUnit.SECONDS));
          writer.truncateTo(1);
          assertEquals(1, session.getMostRecentRevisionNumber());
        } finally {
          resource.unlockHistory();
        }
        cleanup.get(10, TimeUnit.SECONDS);
        cleaner.join(10000);
        assertFalse(cleaner.isAlive());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  @SuppressWarnings("ReturnValueIgnored") // Mockito records the Instant.now invocation for static stubbing.
  void backwardsClockKeepsDerivedMappingsValidAndMaintenanceUsable(final StorageType storage) {
    final Instant first = Instant.parse("2025-03-04T05:06:07.123456Z");
    final Instant backwards = first.minusSeconds(1);
    final Instant second = Instant.parse("2025-03-04T05:06:07.123457Z");
    final Instant third = Instant.parse("2025-03-04T05:06:07.123458Z");
    final Instant last = Instant.parse("2025-03-04T05:06:07.123487Z");
    final var path = directory.toPath().resolve("backwards-clock");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(storage)
                                                   .validTimePaths("lo", "hi")
                                                   .customCommitTimestamps(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        try (final MockedStatic<Instant> clock = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
          clock.when(Instant::now).thenReturn(first);
          writer.commit(null, first);
          resource.drain();
          assertSum(session, 1, 10);
          clock.when(Instant::now).thenReturn(backwards);
          setNumber(writer, 20);
          writer.commit(null, backwards);
          try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
            assertEquals(second, reader.getRevisionTimestamp());
            assertTrue(reader.getRevisionTimestamp().isAfter(Instant.now()));
          }
          assertTrue(resource.maintainNextBatch(1));
          resource.drain();
          assertClockMapping(session, 2, second);
          assertSum(session, 2, 20);
          assertNull(resource.failure());
          assertEquals(-1, resource.poisonedRevision());
          assertThrows(IllegalStateException.class, () -> writer.commit(null, first));
          assertEquals(2, session.getMostRecentRevisionNumber());
          boolean folded = false;
          for (int revision = 3; revision <= 32; revision++) {
            setNumber(writer, revision * 10L);
            writer.commit(null, backwards);
            assertTrue(resource.maintainNextBatch(1));
            final int published;
            try (StorageEngineReader derived = resource.openSummaryReader()) {
              published = derived.getRevisionNumber();
            }
            resource.drain();
            try (StorageEngineReader derived = resource.openSummaryReader()) {
              folded |= derived.getRevisionNumber() > published;
            }
            assertEquals(revision, resource.maintainedRevision());
          }
          assertTrue(folded, "Sustained updates must allow an independent aggregate-fold commit");
          assertClockMapping(session, 3, third);
          assertClockMapping(session, 32, last);
          assertSum(session, 32, 320);
          assertNull(resource.failure());
          assertEquals(-1, resource.poisonedRevision());
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("records")) {
      state(session).pause();
      assertClockMapping(session, 2, second);
      assertClockMapping(session, 3, third);
      assertClockMapping(session, 32, last);
      assertSum(session, 32, 320);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("ReturnValueIgnored") // Mockito records the Instant.now invocation for static stubbing.
  void summaryFoldsUseTheirOwnTimelineAfterClockRegression(final boolean withBase) {
    final Instant first = Instant.parse("2025-03-04T05:06:07.123456Z");
    final Instant backwards = first.minusSeconds(1);
    final var path = directory.toPath().resolve("summary-clock");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .customCommitTimestamps(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        try (final MockedStatic<Instant> clock = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
          clock.when(Instant::now).thenReturn(first);
          writer.commit(null, first);
          if (withBase) {
            resource.drain();
          }
          for (int revision = 2; revision <= 18; revision++) {
            setNumber(writer, revision);
            writer.commit(null, first);
            if (withBase && revision == 2) {
              resource.drain();
            }
          }
          assertTrue(resource.foldNextSummaryBatch());
          final int published;
          final Instant previous;
          try (StorageEngineReader derived = resource.openSummaryReader()) {
            published = derived.getRevisionNumber();
            previous = CommitTimestamps.toInstant(derived.getActualRevisionRootPage().getRevisionTimestampMicros());
            if (!withBase) {
              assertEquals(first, previous);
            }
          }
          clock.when(Instant::now).thenReturn(backwards);
          assertTrue(resource.foldNextSummaryBatch());
          try (StorageEngineReader derived = resource.openSummaryReader();
              StorageEngineReader source = session.createStorageEngineReader()) {
            assertEquals(published + 1, derived.getRevisionNumber());
            assertEquals(previous.plusNanos(1_000),
                CommitTimestamps.toInstant(derived.getActualRevisionRootPage().getRevisionTimestampMicros()));
            assertEquals(withBase
                ? 18
                : 16, TemporalAggregateSummaryFolds.frontier(derived, source));
          }
          assertEquals(withBase
              ? 2
              : 0, resource.maintainedRevision());
          if (withBase) {
            assertClockMapping(session, 2, Instant.parse("2025-03-04T05:06:07.123457Z"));
          }
          resource.drain();
          assertEquals(18, resource.maintainedRevision());
          assertClockMapping(session, 18, Instant.parse("2025-03-04T05:06:07.123473Z"));
          assertSum(session, 18, 18);
          assertNull(resource.failure());
          assertEquals(-1, resource.poisonedRevision());
        }
      }
    }
  }

  private static void assertClockMapping(final JsonResourceSession session, final int revision,
      final Instant expectedTimestamp) {
    try (StorageEngineReader source = session.createStorageEngineReader(revision)) {
      assertEquals(expectedTimestamp,
          CommitTimestamps.toInstant(source.getActualRevisionRootPage().getRevisionTimestampMicros()));
      final StorageEngineReader derived = source.getDerivedIndexReader();
      assertNotNull(derived);
      final DerivedRevisionMapping mapping = DerivedRevisionMapping.read(derived, revision);
      assertNotNull(mapping);
      assertEquals(CommitTimestamps.toEpochMicros(expectedTimestamp), mapping.timestamp());
      assertTrue(mapping.matches(source));
    }
  }

}

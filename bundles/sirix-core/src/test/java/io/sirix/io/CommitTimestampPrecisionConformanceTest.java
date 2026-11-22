package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.utils.CommitTimestamps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import io.sirix.access.trx.node.AfterCommitState;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the stored commit timestamp under VER-201, VER-202/203 and TXN-310: the stored unit
 * is epoch microseconds, so commits inside one millisecond stay distinct and ordered, a tie or a
 * backwards clock step is adjusted to the predecessor plus one microsecond, open-by-time resolves
 * at microsecond precision, and a supplied sub-millisecond instant round trips unchanged.
 */
@Isolated
final class CommitTimestampPrecisionConformanceTest {

  private static final String RESOURCE = "history";

  /** Two instants inside one millisecond; a millisecond store cannot tell them apart. */
  private static final Instant FIRST = Instant.parse("2021-03-04T05:06:07.000100Z");
  private static final Instant SECOND = Instant.parse("2021-03-04T05:06:07.000200Z");

  @TempDir
  private Path directory;

  @Test
  void twoCommitsInsideOneMillisecondKeepDistinctOrderedTimestamps() {
    // Fixture proof: both instants truncate to the same millisecond, so this really is the case a
    // millisecond store collapses.
    assertEquals(FIRST.toEpochMilli(), SECOND.toEpochMilli(), "fixture must commit inside one millisecond");
    assertNotEquals(CommitTimestamps.toEpochMicros(FIRST), CommitTimestamps.toEpochMicros(SECOND));

    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        writer.commit(null, SECOND);
      }
      assertEquals(FIRST, commitTimestampOf(session, 1));
      assertEquals(SECOND, commitTimestampOf(session, 2));
      assertArrayEquals(new long[] {CommitTimestamps.toEpochMicros(SECOND), CommitTimestamps.toEpochMicros(FIRST)},
          session.getHistoryTimestampsMicros());
      assertEquals(100L, CommitTimestamps.toEpochMicros(SECOND) - CommitTimestamps.toEpochMicros(FIRST));
      // Each one is separately addressable by its own microsecond.
      assertEquals(1, session.getRevisionNumber(FIRST));
      assertEquals(2, session.getRevisionNumber(SECOND));
    }
  }

  @Test
  void aTieAndABackwardsClockStepAreAdjustedToOneMicrosecondAfterThePredecessor() {
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        // A tie: the same instant again.
        writer.commit(null, FIRST);
        // A backwards step: a minute earlier than the predecessor.
        writer.commit(null, FIRST.minusSeconds(60));
      }
      final long first = CommitTimestamps.toEpochMicros(FIRST);
      assertEquals(first, CommitTimestamps.toEpochMicros(commitTimestampOf(session, 1)));
      assertEquals(first + 1, CommitTimestamps.toEpochMicros(commitTimestampOf(session, 2)));
      assertEquals(first + 2, CommitTimestamps.toEpochMicros(commitTimestampOf(session, 3)));
      assertArrayEquals(new long[] {first + 2, first + 1, first}, session.getHistoryTimestampsMicros());
      // No tie survives, so each adjusted revision has its own microsecond to be found by.
      assertEquals(1, session.getRevisionNumber(FIRST));
      assertEquals(2, session.getRevisionNumber(CommitTimestamps.toInstant(first + 1)));
      assertEquals(3, session.getRevisionNumber(CommitTimestamps.toInstant(first + 2)));
      // The adjusted revisions are not placed at the supplied backwards instant.
      assertEquals(0, session.getRevisionNumber(FIRST.minusSeconds(60)));
    }
  }

  @Test
  void openByTimeSelectsTheRevisionOfAnExactMicrosecond() {
    final Instant third = FIRST.plusNanos(2_000L);
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        writer.commit(null, FIRST.plusNanos(1_000L));
        writer.commit(null, third);
      }
      assertEquals(1, session.getRevisionNumber(FIRST));
      assertEquals(2, session.getRevisionNumber(FIRST.plusNanos(1_000L)));
      assertEquals(3, session.getRevisionNumber(third));
      // Sub-microsecond digits are truncated toward the past, so they never select a later revision.
      assertEquals(1, session.getRevisionNumber(FIRST.plusNanos(999L)));
      assertEquals(2, session.getRevisionNumber(FIRST.plusNanos(1_999L)));
      // One microsecond before the first commit is before this timeline's content.
      assertEquals(0, session.getRevisionNumber(FIRST.minusNanos(1_000L)));
      // The ceiling overload agrees with the unbounded one at and below its ceiling.
      assertEquals(2, session.getRevisionNumber(third, 2));
      assertEquals(3, session.getRevisionNumber(third, 3));
      assertEquals(1, session.getRevisionNumber(FIRST, 3));
      try (final var reader = session.beginNodeReadOnlyTrx(FIRST.plusNanos(1_000L))) {
        assertEquals(2, reader.getRevisionNumber());
        assertEquals(FIRST.plusNanos(1_000L), reader.getRevisionTimestamp());
      }
    }
  }

  /** TXN-310: reverting content does not rewind the timeline's timestamp predecessor. */
  @ParameterizedTest
  @CsvSource({"JSON,FILE_CHANNEL", "JSON,MEMORY_MAPPED", "XML,FILE_CHANNEL", "XML,MEMORY_MAPPED"})
  void revertUsesTheLatestCommittedTimestamp(final DatabaseType type, final StorageType storage) throws Exception {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(directory);
    if (type == DatabaseType.JSON) {
      Databases.createJsonDatabase(configuration);
    } else {
      Databases.createXmlDatabase(configuration);
    }
    final Instant expected = Instant.parse("2021-03-04T05:06:07.000201Z");
    try (final Database<?> database = type == DatabaseType.JSON
        ? Databases.openJsonDatabase(directory)
        : Databases.openXmlDatabase(directory)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(storage).customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        writer.commit(null, SECOND);
        writer.revertTo(1);
        writer.commit(null, FIRST.minusSeconds(1));
        try (final var reader = session.beginNodeReadOnlyTrx(3)) {
          assertEquals(expected, reader.getRevisionTimestamp());
        }
        assertEquals(3, session.getRevisionNumber(expected));
        assertArrayEquals(new long[] {1_614_834_367_000_201L, 1_614_834_367_000_200L, 1_614_834_367_000_100L},
            session.getHistoryTimestampsMicros());
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<?> database = type == DatabaseType.JSON
        ? Databases.openJsonDatabase(directory)
        : Databases.openXmlDatabase(directory);
        final var session = database.beginResourceSession(RESOURCE);
        final var reader = session.beginNodeReadOnlyTrx(3)) {
      assertEquals(expected, reader.getRevisionTimestamp());
      assertEquals(SECOND, session.getHistory().get(1).getRevisionTimestamp());
      assertEquals(3, session.getRevisionNumber(expected));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"2021-03-04T05:06:07Z", "2021-03-04T05:06:07.123Z", "2021-03-04T05:06:07.123456Z"})
  void suppliedInstantRoundTripsAcrossColdReopen(final String text) {
    final Instant supplied = Instant.parse(text);
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit("micros", supplied);
      }
      assertEquals(supplied, commitTimestampOf(session, 1));

    }
    // Cold reopen: the value now comes back from the revision root page and the revision directory
    // rather than from any cache.
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession(RESOURCE)) {
      assertEquals(supplied, commitTimestampOf(session, 1));
      final List<RevisionInfo> history = session.getHistory();
      assertEquals(supplied, history.getFirst().getRevisionTimestamp());
      assertArrayEquals(new long[] {CommitTimestamps.toEpochMicros(supplied)}, session.getHistoryTimestampsMicros());
      assertEquals(1, session.getRevisionNumber(supplied));
      assertEquals(0, session.getRevisionNumber(supplied.minusNanos(1_000L)));
    }
  }

  @Test
  void subMicrosecondDigitsOfASuppliedInstantAreTruncatedTowardThePast() {
    final Instant supplied = Instant.parse("2021-03-04T05:06:07.123456789Z");
    final Instant stored = Instant.parse("2021-03-04T05:06:07.123456Z");
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, supplied);
      }
      assertEquals(stored, commitTimestampOf(session, 1));
      assertTrue(commitTimestampOf(session, 1).isBefore(supplied), "truncation must not move the commit forward");
      assertEquals(1, session.getRevisionNumber(stored));
      assertEquals(1, session.getRevisionNumber(supplied));
    }
  }

  @Test
  void wallClockCommitsStrictlyIncreaseAndKeepMicrosecondPrecision() {
    try (final var database = createResource(false); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        for (int commit = 0; commit < 32; commit++) {
          writer.commit();
        }
      }
      final long[] newestFirst = session.getHistoryTimestampsMicros();
      assertEquals(32, newestFirst.length);
      for (int index = 1; index < newestFirst.length; index++) {
        assertTrue(newestFirst[index] < newestFirst[index - 1], "commit timestamps must strictly increase, but "
            + newestFirst[index] + " follows " + newestFirst[index - 1]);
      }
      if (platformClockHasSubMillisecondResolution()) {
        boolean anySubMillisecond = false;
        for (final long micros : newestFirst) {
          anySubMillisecond |= micros % 1_000L != 0L;
        }
        assertTrue(anySubMillisecond, "a sub-millisecond clock must reach the stored commit timestamp");
      }
    }
  }

  @Test
  void aCommitTimestampOutsideTheRepresentableRangeIsRejectedWithoutConsumingARevision() {
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        assertThrows(IllegalArgumentException.class, () -> writer.commit(null, Instant.MAX));
        assertEquals(1, session.getMostRecentRevisionNumber(), "a rejected timestamp must not append a revision");
        // The writer stays usable: the rejection happens before any commit work.
        writer.commit(null, SECOND);
      }
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertEquals(SECOND, commitTimestampOf(session, 2));
    }
  }

  @Test
  void aSuppliedTimestampIsRejectedWhenCustomTimestampsAreDisabled() {
    try (final var database = createResource(false);
        final var session = database.beginResourceSession(RESOURCE);
        final var writer = session.beginNodeTrx()) {
      assertThrows(IllegalStateException.class, () -> writer.commit(null, FIRST));
      assertEquals(0, session.getMostRecentRevisionNumber());
    }
  }

  @ParameterizedTest
  @CsvSource({"false,2021-03-04T05:06:07.000150Z", "true,2021-03-04T05:06:07.000150Z", "false,2021-03-04T05:07:07Z",
      "true,2021-03-04T05:07:07Z"})
  void retryUsesItsTimestampAfterSyncOrAsyncPhaseOneFailure(final boolean phaseOne, final String failedText) {
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
        writer.getStorageEngineWriter().installEncodePassCompleteListener(() -> {
          throw new IllegalStateException("before publication");
        });
        final Instant failed = Instant.parse(failedText);
        assertThrows(IllegalStateException.class, () -> {
          if (phaseOne) {
            writer.getStorageEngineWriter().commitWritePages(null, failed, true);
          } else {
            writer.commit(null, failed);
          }
        });
        assertEquals(1, session.getMostRecentRevisionNumber());
        writer.getStorageEngineWriter().installEncodePassCompleteListener(null);
        writer.commit(null, SECOND);
      }
      assertEquals(SECOND, commitTimestampOf(session, 2));
      assertArrayEquals(new long[] {CommitTimestamps.toEpochMicros(SECOND), CommitTimestamps.toEpochMicros(FIRST)},
          session.getHistoryTimestampsMicros());
      assertEquals(2, session.getRevisionNumber(SECOND));
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession(RESOURCE)) {
      assertEquals(SECOND, commitTimestampOf(session, 2));
      assertArrayEquals(new long[] {CommitTimestamps.toEpochMicros(SECOND), CommitTimestamps.toEpochMicros(FIRST)},
          session.getHistoryTimestampsMicros());
    }
  }

  @Test
  void retryAfterAutomaticAsyncPhaseOneFailureUsesExplicitTimestamp() {
    try (final var database = createResource(true); final var session = database.beginResourceSession(RESOURCE)) {
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST);
      }
      try (final var writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
        writer.getStorageEngineWriter().installEncodePassCompleteListener(() -> {
          throw new IllegalStateException("before publication");
        });
        writer.insertArrayAsFirstChild();
        assertThrows(IllegalStateException.class, () -> writer.insertStringValueAsFirstChild("retry"));
        assertEquals(1, session.getMostRecentRevisionNumber());
        writer.getStorageEngineWriter().installEncodePassCompleteListener(null);
        writer.commit(null, SECOND);
      }
      assertEquals(SECOND, commitTimestampOf(session, 2));
    }
  }

  private Database<JsonResourceSession> createResource(final boolean customCommitTimestamps) {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    final var database = Databases.openJsonDatabase(directory);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .customCommitTimestamps(customCommitTimestamps)
                                                 .build());
    return database;
  }

  private static Instant commitTimestampOf(final JsonResourceSession session, final int revision) {
    try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
      return reader.getRevisionTimestamp();
    }
  }

  /**
   * Whether this platform's clock resolves below a millisecond. A clock that ticks in whole
   * milliseconds cannot show microseconds in a stored timestamp, and that is a property of the
   * machine rather than of the engine.
   */
  private static boolean platformClockHasSubMillisecondResolution() {
    for (int sample = 0; sample < 1_000; sample++) {
      if (CommitTimestamps.nowEpochMicros() % 1_000L != 0L) {
        return true;
      }
    }
    return false;
  }
}

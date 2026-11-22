package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.BytesOut;
import io.sirix.utils.CommitTimestamps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@Isolated
final class CommitTimestampLateFailureTest {
  private static final Instant PREDECESSOR = Instant.parse("2021-03-04T05:06:07.000100Z");
  private static final Instant RETRY = Instant.parse("2021-03-04T05:06:07.000300Z");

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"FILE_CHANNEL,false,2021-03-04T05:06:07.000200Z,1", "FILE_CHANNEL,true,2021-03-04T05:06:07.000200Z,1",
      "MEMORY_MAPPED,false,2021-03-04T05:06:07.000200Z,1", "MEMORY_MAPPED,true,2021-03-04T05:06:07.000200Z,1",
      "FILE_CHANNEL,false,2021-03-04T05:06:07.000400Z,1", "FILE_CHANNEL,true,2021-03-04T05:06:07.000400Z,1",
      "MEMORY_MAPPED,false,2021-03-04T05:06:07.000400Z,1", "MEMORY_MAPPED,true,2021-03-04T05:06:07.000400Z,1",
      "FILE_CHANNEL,false,2021-03-04T05:06:07.000200Z,70", "FILE_CHANNEL,true,2021-03-04T05:06:07.000400Z,70",
      "MEMORY_MAPPED,false,2021-03-04T05:06:07.000200Z,70", "MEMORY_MAPPED,true,2021-03-04T05:06:07.000400Z,70"})
  void retryReplacesLateFailedRevisionInWarmIndex(final StorageType storage, final boolean phaseOne,
      final String failedText, final int committedRevisions) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(
          ResourceConfiguration.newBuilder("res").storageType(storage).customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          for (int revision = 1; revision <= committedRevisions; revision++) {
            writer.commit(null, PREDECESSOR.minusSeconds(committedRevisions - revision));
          }
          failFinalPhaseOneFlushOnce(writer.getStorageEngineWriter());
          final Instant failed = Instant.parse(failedText);
          final var thrown = assertThrows(IllegalStateException.class, () -> {
            if (phaseOne) {
              writer.getStorageEngineWriter().commitWritePages(null, failed, true);
            } else {
              writer.commit(null, failed);
            }
          });
          assertEquals("late phase-one flush failure", thrown.getMessage());
          assertEquals(committedRevisions, session.getMostRecentRevisionNumber());
          writer.commit(null, RETRY);
          assertTimeline(session, committedRevisions);
          assertTrue(session.getRevisionNumber(failed) <= session.getMostRecentRevisionNumber());
          assertEquals(committedRevisions + 1, session.getRevisionNumber(RETRY.plusSeconds(1)));
        }
        assertTimeline(session, committedRevisions);
      }
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession("res")) {
      assertTimeline(session, committedRevisions);
    }
  }

  @Test
  void automaticAsyncLateFailureRetriesAgainstCommittedPredecessor() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.commit(null, PREDECESSOR);
        }
        try (final var writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          failFinalPhaseOneFlushOnce(writer.getStorageEngineWriter());
          writer.insertArrayAsFirstChild();
          final var thrown =
              assertThrows(IllegalStateException.class, () -> writer.insertStringValueAsFirstChild("retry"));
          assertEquals("late phase-one flush failure", thrown.getMessage());
          assertEquals(1, session.getMostRecentRevisionNumber());
          writer.commit(null, RETRY);
          assertTimeline(session, 1);
        }
      }
    }
  }

  private static void assertTimeline(final JsonResourceSession session, final int committedRevisions) {
    final int head = committedRevisions + 1;
    assertEquals(head, session.getMostRecentRevisionNumber());
    assertEquals(head, session.getRevisionNumber(RETRY));
    assertEquals(head, session.getRevisionNumber(RETRY, head));
    assertEquals(committedRevisions, session.getRevisionNumber(PREDECESSOR));
    assertEquals(committedRevisions, session.getRevisionNumber(RETRY, committedRevisions));
    final long[] expected = new long[head];
    expected[0] = CommitTimestamps.toEpochMicros(RETRY);
    for (int index = 1; index < head; index++) {
      expected[index] = CommitTimestamps.toEpochMicros(PREDECESSOR.minusSeconds(index - 1));
    }
    assertArrayEquals(expected, session.getHistoryTimestampsMicros());
    final var history = session.getHistory();
    assertEquals(head, history.size());
    assertEquals(RETRY, history.getFirst().getRevisionTimestamp());
    try (final var reader = session.beginNodeReadOnlyTrx(head)) {
      assertEquals(RETRY, reader.getRevisionTimestamp());
    }
  }

  private static void failFinalPhaseOneFlushOnce(final StorageEngineWriter storageWriter) throws Exception {
    final Field field = storageWriter.getClass().getDeclaredField("storagePageReaderWriter");
    field.setAccessible(true);
    final Writer delegate = (Writer) field.get(storageWriter);
    final Writer failing = mock(Writer.class, delegatesTo(delegate));
    final var armed = new AtomicBoolean(true);
    doAnswer(invocation -> {
      if (armed.getAndSet(false)) {
        throw new IllegalStateException("late phase-one flush failure");
      }
      final BytesOut<?> buffer = invocation.getArgument(0);
      delegate.flushBufferedWrites(buffer);
      return null;
    }).when(failing).flushBufferedWrites(any());
    field.set(storageWriter, failing);
  }
}

package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated
final class BoundedRevisionLookupTest {

  @TempDir
  private Path directory;

  @Test
  void boundedLookupSelectsLastTieInImmutablePrefixBeforeAndAfterReopen() {
    final Instant firstCommit = Instant.parse("2018-05-01T00:00:00Z");
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("history")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .build());
      try (final var session = database.beginResourceSession("history");
          final var writer = session.beginNodeTrx()) {
        for (int revision = 1; revision <= 130; revision++) {
          writer.commit(null, firstCommit.plusMillis((revision - 1) / 2));
        }
        assertFloors(session, firstCommit);
      }
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession("history")) {
      assertFloors(session, firstCommit);
      assertThrows(NullPointerException.class, () -> session.getRevisionNumber(null, 5));
      assertThrows(IllegalArgumentException.class, () -> session.getRevisionNumber(firstCommit, -1));
      assertThrows(IllegalArgumentException.class, () -> session.getRevisionNumber(firstCommit, 131));
    }
  }

  private static void assertFloors(final JsonResourceSession session, final Instant firstCommit) {
    final int[] ceilings = {0, 1, 2, 5, 63, 64, 65, 127, 130};
    for (final int ceiling : ceilings) {
      for (int offset = -1; offset <= 66; offset++) {
        final int expected = offset < 0
            ? 0
            : Math.min(ceiling, 2 * offset + 2);
        final Instant timestamp = firstCommit.plusMillis(offset);
        assertEquals(expected, session.getRevisionNumber(timestamp, ceiling));
        assertEquals(expected, session.getRevisionNumber(timestamp.plusNanos(500_000), ceiling));
      }
    }
  }
}

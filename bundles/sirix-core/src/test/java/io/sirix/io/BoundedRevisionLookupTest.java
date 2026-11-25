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
  @SuppressWarnings("NullAway") // Deliberately pass null to verify the public argument guard.
  void boundedLookupSeparatesMicrosecondNeighboursBeforeAndAfterReopen() {
    final Instant firstCommit = Instant.parse("2018-05-01T00:00:00Z");
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("history")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .build());
      try (final var session = database.beginResourceSession("history"); final var writer = session.beginNodeTrx()) {
        // Two commits per millisecond: the second of each pair supplies the same instant as the
        // first, which TXN-310 adjusts to its predecessor plus one microsecond. Odd revisions
        // therefore land on millisecond k = (revision - 1) / 2 and even revisions one microsecond
        // later, which milliseconds could not represent.
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
        final Instant timestamp = firstCommit.plusMillis(offset);
        if (offset < 0) {
          // Before every commit of this resource: the bootstrap revision.
          assertEquals(0, session.getRevisionNumber(timestamp, ceiling));
          assertEquals(0, session.getRevisionNumber(timestamp.plusNanos(500_000), ceiling));
          continue;
        }
        // On the millisecond itself only the odd revision of the pair qualifies; its even
        // neighbour commits one microsecond later. Half a millisecond on, both qualify. A
        // millisecond-precision store collapsed the pair and could answer either.
        final int onTheMillisecond = offset <= 64
            ? 2 * offset + 1
            : 130;
        final int halfAMillisecondLater = offset <= 64
            ? 2 * offset + 2
            : 130;
        assertEquals(Math.min(ceiling, onTheMillisecond), session.getRevisionNumber(timestamp, ceiling));
        assertEquals(Math.min(ceiling, halfAMillisecondLater),
            session.getRevisionNumber(timestamp.plusNanos(500_000), ceiling));
        // Unbounded lookup must agree with the ceiling form above its own result.
        assertEquals(onTheMillisecond, session.getRevisionNumber(timestamp));
        assertEquals(halfAMillisecondLater, session.getRevisionNumber(timestamp.plusNanos(500_000)));
      }
    }
  }
}

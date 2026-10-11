package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBCollectionImpl;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Isolated
final class ValidTimeRangeCacheTest {
  @TempDir
  Path directory;

  @Test
  void immutableKeysAreFencedByEveryRangeAndRevisionComponent() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final JsonIndexController controller = new JsonIndexController();
    final long[] keys = {3, 7};
    ValidTimeRangeCache.put(session, controller, 1, 2, 3, 4, 5, 3, keys);
    keys[0] = 100;
    assertArrayEquals(new long[] {3, 7}, ValidTimeRangeCache.get(session, controller, 1, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(mock(JsonResourceSession.class), controller, 1, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, new JsonIndexController(), 1, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, controller, 2, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, controller, 1, 3, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, controller, 1, 2, 4, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, controller, 1, 2, 3, 3, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, controller, 1, 2, 3, 4, 6, 3));
    for (int mode = 0; mode < 3; mode++) {
      assertNull(ValidTimeRangeCache.get(session, controller, 1, 2, 3, 4, 5, mode));
    }
    ValidTimeRangeCache.put(session, controller, 2, 2, 3, 4, 5, 3, new long[] {8});
    assertNull(ValidTimeRangeCache.get(session, controller, 1, 2, 3, 4, 5, 3));
    assertArrayEquals(new long[] {8}, ValidTimeRangeCache.get(session, controller, 2, 2, 3, 4, 5, 3));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void reusedRevisionDoesNotServeRangeKeysCachedBeforeTruncation(final VersioningType versioning) {
    final Path databasePath = directory.resolve("ranges");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("intervals")
                                                   .validTimePaths("vf", "vt")
                                                   .versioningApproach(versioning)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .storeDiffs(false)
                                                   .build());
      final JsonDBCollection collection = new JsonDBCollectionImpl("ranges", database);
      try (final JsonResourceSession session = database.beginResourceSession("intervals")) {
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"vf":"2024-01-01T00:00:00Z","vt":"2024-10-01T00:00:00Z"}]
              """), JsonNodeTrx.Commit.NO);
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "ranges");
          writer.commit();
          assertRangeSize(session, collection, 1);
          setEndpoints(writer, "2025-01-01T00:00:00Z", "2025-10-01T00:00:00Z");
          writer.commit();
          assertEquals(2, session.getMostRecentRevisionNumber());
          assertRangeSize(session, collection, 0);
          assertRangeSize(session, collection, 0);
          writer.truncateTo(1);
        }
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(2, writer.getRevisionNumber());
          setEndpoints(writer, "2024-01-01T00:00:00Z", "2024-12-01T00:00:00Z");
          writer.commit();
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        assertRangeSize(session, collection, 1);
        assertRangeSize(session, collection, 1);
      }
    }
  }

  private static void assertRangeSize(final JsonResourceSession session, final JsonDBCollection collection,
      final int expected) {
    try (final var reader = session.beginNodeReadOnlyTrx()) {
      final JsonDBArray source = new JsonDBArray(reader, collection);
      final DateTime low = new DateTime("2024-04-01T00:00:00Z");
      final DateTime high = new DateTime("2024-07-01T00:00:00Z");
      final Sequence matches = ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> high, () -> low,
          session.getResourceConfig().getValidTimeConfig(), true, true);
      assertNotNull(matches);
      assertEquals(expected, matches.size().intValue());
    }
  }

  private static void setEndpoints(final JsonNodeTrx writer, final String from, final String to) {
    writer.moveToDocumentRoot();
    assertTrue(writer.moveToFirstChild());
    assertTrue(writer.moveToFirstChild());
    assertTrue(writer.moveToFirstChild());
    assertEquals("vf", writer.getName().getLocalName());
    writer.setStringValue(from);
    assertTrue(writer.moveToRightSibling());
    assertEquals("vt", writer.getName().getLocalName());
    writer.setStringValue(to);
  }
}

package io.sirix.access.trx.node.json;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonIdentityHistoryTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void coldHistoryPreservesExactSourceEventsAndSuffixBoundary(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    try (final var database = JsonIdentityImportTest.create(sourcePath, versioning, hash, dewey);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"x\":1},0]"), JsonNodeTrx.Commit.NO);
      writer.moveTo(1);
      writer.insertStringValueAsLastChild("transient");
      writer.remove();
      writer.commit();
      writer.moveTo(3);
      writer.setNumberValue(2);
      writer.setNumberValue(3);
      writer.commit();
      writer.moveTo(2);
      writer.remove();
      writer.commit();
      writer.revertTo(1);
      writer.commit();
      writer.moveTo(3);
      writer.setObjectKeyName("renamed");
      writer.commit();
      writer.commit();
    }
    for (final int start : new int[] {1, 3, 4}) {
      final Path targetPath = directory.resolve("target-" + start);
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = JsonIdentityImportTest.create(targetPath, versioning, hash, dewey);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var writer = target.beginNodeTrx()) {
        for (int revision = start; revision <= source.getMostRecentRevisionNumber(); revision++) {
          final int destination = revision - start + 1;
          try (final var reader = source.beginNodeReadOnlyTrx(revision)) {
            if (revision == start) {
              ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader);
            } else {
              try (final var base = source.beginNodeReadOnlyTrx(revision - 1)) {
                ((InternalJsonNodeTrx) writer).importRevision(
                    JsonIdentityDeltaReader.between(base, reader, destination), reader);
              }
            }
          }
        }
      }
      Databases.clearGlobalCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = Databases.openJsonDatabase(targetPath);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var boundary = source.beginNodeReadOnlyTrx(start)) {
        for (int revision = start; revision <= source.getMostRecentRevisionNumber(); revision++) {
          try (final var original = source.beginNodeReadOnlyTrx(revision);
              final var copied = target.beginNodeReadOnlyTrx(revision - start + 1)) {
            assertSnapshot(original, copied, start - 1);
            assertHistories(original, copied, boundary, start);
          }
        }
      }
    }
  }

  private static void assertHistories(final JsonNodeReadOnlyTrx original, final JsonNodeReadOnlyTrx copied,
      final JsonNodeReadOnlyTrx boundary, final int start) {
    final var source = original.getStorageEngineReader();
    final var target = copied.getStorageEngineReader();
    final long frontier = source.getActualRevisionRootPage().getMaxNodeKeyInRecordToRevisionsIndex();
    assertEquals(frontier, target.getActualRevisionRootPage().getMaxNodeKeyInRecordToRevisionsIndex());
    // This fixture intentionally has a tiny bounded frontier. Production must enumerate pages.
    assertTrue(frontier < 100);
    for (long key = 1; key <= frontier; key++) {
      final RevisionReferencesNode expected = source.getRecord(key, IndexType.RECORD_TO_REVISIONS, 0);
      final RevisionReferencesNode actual = target.getRecord(key, IndexType.RECORD_TO_REVISIONS, 0);
      if (expected == null) {
        assertNull(actual, "unexpected history for identity " + key);
        continue;
      }
      final var events = new IntArrayList();
      boolean boundaryAdded = Arrays.stream(expected.getRevisions()).anyMatch(revision -> revision == start);
      for (final int revision : expected.getRevisions()) {
        if (revision >= start) {
          events.add(revision - start + 1);
        } else if (start > 1 && !boundaryAdded && boundary.moveTo(key)) {
          events.add(1);
          boundaryAdded = true;
        }
      }
      if (events.isEmpty()) {
        assertNull(actual, "history outside copied suffix for identity " + key);
      } else {
        assertNotNull(actual, "missing history for identity " + key);
        assertArrayEquals(events.toIntArray(), actual.getRevisions(), "history for identity " + key);
      }
    }
  }
}

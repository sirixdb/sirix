package io.sirix.service.json.shredder;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplayRecord;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonIdentityCopyIntegrationTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(
                     version -> Stream.of(false, true)
                                      .flatMap(
                                          dewey -> Stream.of(0L, 1L).map(root -> Arguments.of(version, dewey, root))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void copiesAnExactHistorySuffixFromDocumentOrTopLevelValue(final VersioningType version, final boolean dewey,
      final long root) {
    try (final var sourceDb = create(directory.resolve("source"), version, dewey);
        final var targetDb = create(directory.resolve("target"), version, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
        writer.commit();
        assertTrue(writer.moveTo(1));
        writer.insertNumberValueAsLastChild(1);
        writer.commit();
        assertTrue(writer.moveTo(2));
        writer.setNumberValue(7);
        writer.commit();
        writer.remove();
        writer.commit();
      }
      try (final var reader = source.beginNodeReadOnlyTrx(2); final var writer = target.beginNodeTrx()) {
        assertTrue(reader.moveTo(root));
        new JsonResourceCopy.Builder(writer, reader, InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent()
                                                                                   .build()
                                                                                   .call();
        assertEquals(root, reader.getNodeKey(), "history copy must preserve its supplied source cursor");
      }
      assertEquals(3, target.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 3; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision + 1);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          JsonReplayGraphValidator.validate(copied);
          assertEquals(original.getMaxNodeKey(), copied.getMaxNodeKey());
          final var expected = JsonReplaySnapshotOracle.snapshot(original);
          final var actual = JsonReplaySnapshotOracle.snapshot(copied);
          final var originalRoot = expected.remove(0);
          final var copiedRoot = actual.remove(0);
          assertEquals(originalRoot.firstChild(), copiedRoot.firstChild());
          assertEquals(originalRoot.lastChild(), copiedRoot.lastChild());
          assertEquals(originalRoot.descendantCount(), copiedRoot.descendantCount());
          assertEquals(originalRoot.hash(), copiedRoot.hash());
          assertEquals(expected.keySet(), actual.keySet());
          for (final var record : expected.values()) {
            assertEquals(mapped(record), actual.get(record.key()));
          }
        }
      }
    }
  }

  @Test
  void refusesPartialHistoryAndDirtyDestinationsBeforeImport() {
    try (final var sourceDb = create(directory.resolve("source"), VersioningType.FULL, false);
        final var targetDb = create(directory.resolve("target"), VersioningType.FULL, false);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[[0]]"), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
      try (final var reader = source.beginNodeReadOnlyTrx(); final var writer = target.beginNodeTrx()) {
        assertTrue(reader.moveTo(2));
        assertThrows(IllegalArgumentException.class, () -> new JsonResourceCopy.Builder(writer, reader,
            InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent().build().call());
        assertEquals(0, writer.getMaxNodeKey());
        assertEquals(0, target.getMostRecentRevisionNumber());
        reader.moveToDocumentRoot();
        writer.insertArrayAsFirstChild();
        writer.moveToDocumentRoot();
        assertThrows(IllegalArgumentException.class, () -> new JsonResourceCopy.Builder(writer, reader,
            InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent().build().call());
        assertTrue(writer.moveTo(1));
        assertTrue(writer.isArray());
        writer.rollback();
      }
    }
  }

  private static JsonReplayRecord mapped(final JsonReplayRecord record) {
    final int previous = record.previousRevision() <= 1
        ? -1
        : record.previousRevision() - 1;
    final int modified = record.lastModifiedRevision() < 0
        ? record.lastModifiedRevision()
        : Math.max(1, record.lastModifiedRevision() - 1);
    return new JsonReplayRecord(record.key(), record.kind(), record.parent(), record.left(), record.right(),
        record.firstChild(), record.lastChild(), record.childCount(), record.descendantCount(), record.pathKey(),
        record.nameKey(), previous, modified, record.hash(), record.deweyID(), record.name(), record.stringValue(),
        record.numberValue(), record.booleanValue());
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType version,
      final boolean dewey) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(version)
                                                 .hashKind(HashType.ROLLING)
                                                 .useDeweyIDs(dewey)
                                                 .build());
    return database;
  }
}

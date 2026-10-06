package io.sirix.service.json.replay;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSets;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonReplayTransitionValidatorTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectsDetachedReciprocalSiblingCycleWithoutCountsOrDewey(final VersioningType versioning) {
    rejectsAndRetries(versioning, "[0,0,0,0]", false, records -> {
      replace(records, 1, 0, -1, -1, 2, 2);
      replace(records, 2, 1, -1, -1, -1, -1);
      replace(records, 3, 1, 5, 4, -1, -1);
      replace(records, 4, 1, 3, 5, -1, -1);
      replace(records, 5, 1, 4, 3, -1, -1);
    }, "Unreachable changed replay child");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectsDetachedParentCycleWithoutCountsOrDewey(final VersioningType versioning) {
    rejectsAndRetries(versioning, "[[[]]]", false, records -> {
      replace(records, 0, -1, -1, -1, -1, -1);
      replace(records, 1, 3, -1, -1, 2, 2);
      replace(records, 2, 1, -1, -1, 3, 3);
      replace(records, 3, 2, -1, -1, 1, 1);
    }, "Replay parent cycle");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectsMissingLeftSiblingDeweyIDAndRetries(final VersioningType versioning) {
    rejectsAndRetries(versioning, "[0,0]", true, records -> {
      final var left = requireNonNull(records.get(2));
      assertNotNull(left.deweyID());
      records.put(left.key(),
          new JsonReplayRecord(left.key(), left.kind(), left.parent(), left.left(), left.right(), left.firstChild(),
              left.lastChild(), left.childCount(), left.descendantCount(), left.pathKey(), left.nameKey(),
              left.previousRevision(), left.lastModifiedRevision(), left.hash(), null, left.name(), left.stringValue(),
              left.numberValue(), left.booleanValue()));
    }, "Invalid replay Dewey boundary at 3");
  }

  private void rejectsAndRetries(final VersioningType versioning, final String json, final boolean dewey,
      final Consumer<Long2ObjectMap<JsonReplayRecord>> corrupt, final String diagnostic) {
    try (final var sourceDb = create(directory.resolve("source"), versioning, dewey);
        final var targetDb = create(directory.resolve("target"), versioning, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        writer.commit();
        writer.commit();
      }
      try (final var first = source.beginNodeReadOnlyTrx(1);
          final var second = source.beginNodeReadOnlyTrx(2);
          final var writer = target.beginNodeTrx()) {
        final var importer = (InternalJsonNodeTrx) writer;
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
        final var valid = JsonIdentityDeltaReader.between(first, second, 2);
        final var records = new Long2ObjectOpenHashMap<>(JsonIdentityDeltaReader.snapshot(second, 1).puts());
        corrupt.accept(records);
        final var invalid = new JsonIdentityDelta(valid.manifest(), records, LongSets.emptySet());
        final var failure = assertThrows(IllegalStateException.class, () -> importer.importRevision(invalid, second));
        final String message = requireNonNull(failure.getMessage());
        assertTrue(message.contains(diagnostic), message);
        assertEquals(1, target.getMostRecentRevisionNumber());
        JsonReplayGraphValidator.validate(writer);
        importer.importRevision(valid, second);
        assertEquals(2, target.getMostRecentRevisionNumber());
        JsonReplayGraphValidator.validate(writer);
      }
    }
  }

  private static void replace(final Long2ObjectMap<JsonReplayRecord> records, final long key, final long parent,
      final long left, final long right, final long first, final long last) {
    final var old = records.get(key);
    records.put(key,
        new JsonReplayRecord(key, old.kind(), parent, left, right, first, last, old.childCount(), old.descendantCount(),
            old.pathKey(), old.nameKey(), old.previousRevision(), old.lastModifiedRevision(), old.hash(), old.deweyID(),
            old.name(), old.stringValue(), old.numberValue(), old.booleanValue()));
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType versioning,
      final boolean dewey) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(versioning)
                                                 .hashKind(HashType.NONE)
                                                 .storeChildCount(false)
                                                 .useDeweyIDs(dewey)
                                                 .build());
    return database;
  }
}

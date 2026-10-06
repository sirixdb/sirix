package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonHashingWorkProbe;
import io.sirix.access.trx.node.json.JsonStructuralHashInvariantTest;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.replay.JsonReplayRecord;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonStructuralHashingWorkBudgetTest {
  @TempDir
  Path directory;

  private enum Mutation {
    FIRST, RIGHT, RENAME_ARRAY, RENAME_OBJECT
  }

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(
                     versioning -> Stream.of(16, 4096)
                                         .flatMap(
                                             size -> Stream.of(Mutation.values())
                                                           .map(mutation -> Arguments.of(versioning, size, mutation))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void boundaryHashingDoesNotPrepareOrCaptureUnchangedDescendants(final VersioningType versioning, final int size,
      final Mutation mutation) throws Exception {
    final Path path = directory.resolve("source");
    final String values = "[" + "0,".repeat(size - 1) + "0]";
    final String input = switch (mutation) {
      case FIRST -> "[0," + values + "]";
      case RIGHT -> "[" + values + ",0]";
      case RENAME_ARRAY -> "{\"original\":" + values + "}";
      case RENAME_OBJECT -> "{\"original\":{\"values\":" + values + "}}";
    };
    final Long2ObjectOpenHashMap<JsonReplayRecord> initialRecords;
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .hashKind(HashType.ROLLING)
                                                   .useDeweyIDs(false)
                                                   .buildPathSummary(false)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(input), JsonNodeTrx.Commit.NO);
        writer.commit();
        initialRecords = JsonReplaySnapshotOracle.snapshot(writer);
      }
    }
    Databases.clearGlobalCaches();
    long moved;
    long lastDescendant;
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      if (mutation == Mutation.FIRST) {
        assertTrue(writer.moveToRightSibling());
      }
      moved = writer.getNodeKey();
      lastDescendant = mutation == Mutation.RIGHT
          ? writer.getMaxNodeKey() - 1
          : writer.getMaxNodeKey();
      try (final var probe = new JsonHashingWorkProbe(writer, writer.getMaxNodeKey())) {
        final var reads = WorkCounter.alwaysOn("hashRecordReads", "hash-maintenance record lookup", probe::reads);
        final var preparations =
            WorkCounter.alwaysOn("hashPreparations", "document record prepared for hashing", probe::preparations);
        final var writes = WorkCounter.alwaysOn("hashWrites", "document record persisted by hashing", probe::writes);
        final var pages =
            WorkCounter.alwaysOn("hashWrittenPages", "distinct document page written by hashing", probe::writtenPages);
        final var captured =
            WorkCounter.alwaysOn("hashCapturedKeys", "retained hash input records", probe::capturedKeys);
        final var scratch = WorkCounter.alwaysOn("hashScratchGrowth", "primitive scratch backing-array bytes grown",
            probe::scratchBytes);
        final var work = WorkCapture.of(reads, preparations, writes, pages, captured, scratch).run(() -> {
          switch (mutation) {
            case FIRST -> {
              assertTrue(writer.moveTo(1));
              writer.moveSubtreeToFirstChild(moved);
            }
            case RIGHT -> {
              assertTrue(writer.moveTo(lastDescendant + 1));
              writer.moveSubtreeToRightSibling(moved);
            }
            case RENAME_ARRAY, RENAME_OBJECT -> {
              assertTrue(writer.moveTo(moved));
              writer.setObjectKeyName("renamed");
            }
          }
        });
        work.assertBetween(reads, 5, 32, "boundary hashing must not read the unchanged subtree")
            .assertBetween(preparations, 2, 6, "boundary hashing must not prepare descendant records")
            .assertBetween(writes, 2, 6, "boundary hashing must not rewrite descendant records")
            .assertBetween(pages, 1, 2, "unchanged descendant pages must not be promoted for hash writes")
            .assertBetween(captured, 2, 6, "scratch state must contain only boundary records and ancestors")
            .assertZero(scratch, "unchanged subtree size must not grow hash scratch arrays");
        assertTrue(probe.scratchBytes() > 0 && probe.scratchBytes() <= 1800, "live bounded scratch payload");
        assertEquals(0, probe.preparedBetween(moved + 1, lastDescendant), "unchanged descendants prepared");
      }
      final var after = JsonReplaySnapshotOracle.snapshot(writer);
      for (long key = moved + 1; key <= lastDescendant; key++) {
        assertEquals(initialRecords.get(key), after.get(key), "unchanged descendant identity/hash/metadata " + key);
      }
      JsonStructuralHashInvariantTest.assertGraph(writer, HashType.ROLLING);
      writer.commit();
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource");
        final var before = session.beginNodeReadOnlyTrx(1);
        final var after = session.beginNodeReadOnlyTrx(2)) {
      JsonStructuralHashInvariantTest.assertGraph(before, HashType.ROLLING);
      JsonStructuralHashInvariantTest.assertGraph(after, HashType.ROLLING);
      final var oldRecords = JsonReplaySnapshotOracle.snapshot(before);
      final var newRecords = JsonReplaySnapshotOracle.snapshot(after);
      for (long key = moved + 1; key <= lastDescendant; key++) {
        assertEquals(oldRecords.get(key), newRecords.get(key), "persisted unchanged descendant " + key);
      }
      assertTrue(after.moveTo(moved));
      switch (mutation) {
        case FIRST -> assertEquals(-1, after.getLeftSiblingKey());
        case RIGHT -> assertEquals(-1, after.getRightSiblingKey());
        case RENAME_ARRAY, RENAME_OBJECT -> assertEquals("renamed", after.getName().getLocalName());
      }
    }
  }
}

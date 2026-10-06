package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.budget.WorkCapture;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static io.sirix.budget.EngineWorkCounters.REPLAY;
import static io.sirix.budget.EngineWorkCounters.REPLAY_PROJECTION_LABEL_BYTES;
import static io.sirix.budget.EngineWorkCounters.REPLAY_PROJECTION_ROWS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_RECORD_VISITS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ProjectionIdentityImportWorkBudgetTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(version -> Stream.of(16, 4096)
                                           .flatMap(rows -> Stream.of(false, true)
                                                                  .map(prepend -> Arguments.of(version, rows, prepend))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void emptyBoundaryChangesRetainRowsAndLabels(final VersioningType version, final int rows, final boolean prepend)
      throws Exception {
    final Path sourcePath = directory.resolve("source");
    try (final var database = create(sourcePath, version);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      final StringBuilder json = new StringBuilder(rows * 20).append("[{\"rows\":[");
      for (int row = 0; row < rows; row++) {
        if (row != 0) {
          json.append(',');
        }
        json.append("{\"score\":").append(row).append('}');
      }
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append("]}]").toString()),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      insertGroup(writer, prepend, "{}");
      final long empty = writer.getNodeKey();
      writer.commit();
      assertTrue(writer.moveTo(empty));
      writer.remove();
      writer.commit();
      insertGroup(writer, prepend, "{\"rows\":[{\"score\":-1}]}");
      writer.commit();
    }
    clearCaches();
    final Path targetPath = directory.resolve("target");
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, version);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      final var controller = (JsonIndexController) target.getWtxIndexController(1);
      controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
      final var importer = (InternalJsonNodeTrx) writer;
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
      }
      for (int revision = 2; revision <= 4; revision++) {
        try (final var before = source.beginNodeReadOnlyTrx(revision - 1);
            final var after = source.beginNodeReadOnlyTrx(revision)) {
          final var delta = JsonIdentityDeltaReader.between(before, after, revision);
          final var work = WorkCapture.of(REPLAY).run(() -> importer.importRevision(delta, after));
          if (revision < 4) {
            work.assertZero(REPLAY_PROJECTION_ROWS, "empty neighbors must not remove or reinsert retained rows")
                .assertZero(REPLAY_PROJECTION_LABEL_BYTES, "empty neighbors must not allocate retained row labels")
                .assertBetween(REPLAY_RECORD_VISITS, 1, 1000, "boundary import must not visit the unchanged subtree");
          } else {
            work.assertExactly(REPLAY_PROJECTION_ROWS, 1, "a new indexed group must insert exactly its one row")
                .assertBetween(REPLAY_PROJECTION_LABEL_BYTES, 1, 128, "the label allocation counter must be live");
          }
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      final List<byte[]> initial;
      final long[] offsets;
      try (final var reader = target.beginNodeReadOnlyTrx(1)) {
        initial = payloads(reader);
        offsets = segmentOffsets(reader, initial.size());
        if (rows == 4096) {
          assertTrue(offsets[1] > 0, "the large fixture must own a referenced numeric segment");
        }
      }
      for (int revision = 1; revision <= 4; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          JsonReplayGraphValidator.validate(copied);
          final var expected = JsonReplaySnapshotOracle.snapshot(original);
          final var actual = JsonReplaySnapshotOracle.snapshot(copied);
          expected.remove(0);
          actual.remove(0);
          assertEquals(expected, actual);
          final List<byte[]> payloads = payloads(copied);
          assertProjection(original, copied, payloads);
          if (revision < 4) {
            assertEquals(initial.size(), payloads.size());
            for (int group = 0; group < initial.size(); group++) {
              assertArrayEquals(initial.get(group), payloads.get(group), "retained row data and labels");
            }
            assertArrayEquals(offsets, segmentOffsets(copied, payloads.size()),
                "retained key and numeric segments must not be rewritten");
          }
        }
      }
    }
  }

  private static List<byte[]> payloads(final JsonNodeReadOnlyTrx reader) {
    final var session = reader.getResourceSession();
    final int revision = reader.getRevisionNumber();
    final var handle = ProjectionIndexCatalog.load(session, revision, projection());
    assertNotNull(handle);
    final List<byte[]> payloads = new ArrayList<>(handle.rowGroupCount());
    for (final byte[] payload : handle.rowGroupPayloads(
        ProjectionIndexCatalog.rowGroupMaterializer(session, revision, 0, handle.rowGroupCount()))) {
      payloads.add(payload);
    }
    return payloads;
  }

  private static long[] segmentOffsets(final JsonNodeReadOnlyTrx reader, final int groups) {
    final var storage = reader.getStorageEngineReader();
    final var layout = ProjectionIndexHOTStorage.readSlotLayout(storage, 0);
    final long[] offsets = new long[groups * 2];
    for (int group = 0; group < groups; group++) {
      offsets[group * 2] = ProjectionIndexHOTStorage.segmentPageOffset(storage, 0,
          layout.segmentSlot(group + 1, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()), 0);
      offsets[group * 2 + 1] = ProjectionIndexHOTStorage.segmentPageOffset(storage, 0,
          layout.segmentSlot(group + 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)), 0);
    }
    return offsets;
  }

  private static void assertProjection(final JsonNodeReadOnlyTrx original, final JsonNodeReadOnlyTrx copied,
      final List<byte[]> payloads) {
    final LongArrayList expectedKeys = new LongArrayList();
    final LongArrayList expectedValues = new LongArrayList();
    original.moveToDocumentRoot();
    final var axis = new DescendantAxis(original);
    while (axis.hasNext()) {
      axis.nextLong();
      if (original.getKind() == NodeKind.OBJECT_NAMED_NUMBER) {
        expectedKeys.add(original.getParentKey());
        expectedValues.add(original.getNumberValue().longValue());
      }
    }
    final LongArrayList keys = new LongArrayList(expectedKeys.size());
    final LongArrayList values = new LongArrayList(expectedValues.size());
    byte[] previous = null;
    for (final byte[] payload : payloads) {
      final var page = ProjectionIndexRowGroupPage.deserialize(payload);
      for (int row = 0; row < page.getRowCount(); row++) {
        keys.add(page.recordKeys()[row]);
        values.add(page.numericColumn(0)[row]);
        previous = ProjectionIdentityEpochOracle.assertOrder(page, row, previous);
      }
    }
    assertEquals(expectedKeys, keys);
    assertEquals(expectedValues, values);
    ProjectionIdentityEpochOracle.assertSortedRows(copied, 0, expectedKeys, expectedValues);
  }

  private static void insertGroup(final JsonNodeTrx writer, final boolean prepend, final String json) {
    assertTrue(writer.moveTo(1));
    if (prepend) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
    } else {
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
    }
  }

  private static IndexDef projection() {
    return IndexDefs.createProjectionIdxDef(parse("/[]/rows/[]", PathParser.Type.JSON),
        List.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(List.of(0)));
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType version) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final var database = Databases.openJsonDatabase(path);
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                          .storageType(StorageType.FILE_CHANNEL)
                                                          .versioningApproach(version)
                                                          .hashKind(HashType.ROLLING)
                                                          .useDeweyIDs(false)
                                                          .buildPathStatistics(true)
                                                          .build()));
    return database;
  }
}

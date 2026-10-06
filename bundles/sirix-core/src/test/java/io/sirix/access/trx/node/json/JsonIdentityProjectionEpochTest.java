package io.sirix.access.trx.node.json;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Iterator;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.index.projection.ProjectionIdentityEpochOracle;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
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
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertPaths;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.create;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonIdentityProjectionEpochTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    JsonNodeTrxImpl.replayTestHook = null;
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  static Stream<Arguments> batchConfigurations() {
    return configurations().flatMap(configuration -> Stream.of(false, true).map(emptied -> {
      final Object[] values = configuration.get();
      return Arguments.of(values[0], values[1], values[2], emptied);
    }));
  }

  @ParameterizedTest
  @MethodSource("batchConfigurations")
  void batchRotationsAndMixedPermutationsKeepOrderedAnchors(final VersioningType versioning, final HashType hash,
      final boolean dewey, final boolean emptied) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
          [{"rows":[{"score":10}]},{"rows":[{"score":20}]},
           {"rows":[{"score":30}]},{"rows":[{"score":40}]}]
          """), JsonNodeTrx.Commit.NO);
      session.getWtxIndexController(1).createIndexes(Set.of(
          IndexDefs.createPathIdxDef(Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON),
          IndexDefs.createCASIdxDef(false, Type.LON, Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON), projection()), writer);
      final long[] groups = new long[4];
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      for (int group = 0; group < groups.length; group++) {
        groups[group] = writer.getNodeKey();
        if (group < groups.length - 1) {
          assertTrue(writer.moveToRightSibling());
        }
      }
      writer.commit();
      if (emptied) {
        toggleRows(writer, groups[0], 10);
        toggleRows(writer, groups[3], 40);
        writer.commit();
      }
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(groups[2]);
      assertTrue(writer.moveTo(groups[2]));
      writer.moveSubtreeToRightSibling(groups[3]);
      writer.commit();
      final List<int[]> permutations = new ArrayList<>(24);
      for (int first = 0; first < 4; first++) {
        for (int second = 0; second < 4; second++) {
          if (second == first) {
            continue;
          }
          for (int third = 0; third < 4; third++) {
            if (third != first && third != second) {
              permutations.add(new int[] {first, second, third, 6 - first - second - third});
            }
          }
        }
      }
      Collections.shuffle(permutations, new Random(0x5eed));
      long extra = -1;
      for (int step = 0; step < permutations.size(); step++) {
        switch (step % 6) {
          case 0 -> toggleRows(writer, groups[0], 10);
          case 1 -> {
            assertTrue(writer.moveTo(groups[3]));
            assertTrue(writer.moveToFirstChild());
            writer.setObjectKeyName(writer.getName().getLocalName().equals("rows")
                ? "archive"
                : "rows");
          }
          case 2 -> {
            assertTrue(writer.moveTo(1));
            writer.insertObjectAsLastChild();
            extra = writer.getNodeKey();
            assertTrue(writer.moveTo(1));
            writer.moveSubtreeToFirstChild(extra);
          }
          case 3 -> {
            assertTrue(writer.moveTo(extra));
            writer.remove();
            extra = -1;
          }
          case 4 -> toggleRows(writer, groups[3], 40);
          case 5 -> {
            assertTrue(writer.moveTo(groups[2]));
            assertTrue(writer.moveToFirstChild());
            if (writer.moveToFirstChild()) {
              final long row = writer.getNodeKey();
              assertTrue(writer.moveTo(groups[1]));
              assertTrue(writer.moveToFirstChild());
              writer.moveSubtreeToFirstChild(row);
            }
          }
          default -> throw new AssertionError();
        }
        final int[] order = permutations.get(step);
        for (int position = order.length - 1; position >= 0; position--) {
          assertTrue(writer.moveTo(1));
          if (writer.getFirstChildKey() != groups[order[position]]) {
            writer.moveSubtreeToFirstChild(groups[order[position]]);
          }
        }
        writer.commit();
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      final var controller = (JsonIndexController) target.getWtxIndexController(1);
      controller.createIndexes(Set.of(
          IndexDefs.createPathIdxDef(Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON),
          IndexDefs.createCASIdxDef(false, Type.LON, Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON)), writer);
      controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
      final var importer = (InternalJsonNodeTrx) writer;
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
      }
      for (int revision = 2; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var before = source.beginNodeReadOnlyTrx(revision - 1);
            final var after = source.beginNodeReadOnlyTrx(revision)) {
          final var delta = JsonIdentityDeltaReader.between(before, after, revision);
          JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
            if (phase.equals("derived-state-finalized")) {
              throw new IllegalStateException("injected batch failure");
            }
          };
          assertEquals("injected batch failure",
              assertThrows(IllegalStateException.class, () -> importer.importRevision(delta, after)).getMessage());
          JsonNodeTrxImpl.replayTestHook = null;
          assertSnapshot(before, writer, 0);
          assertEquals(revision - 1, target.getMostRecentRevisionNumber());
          try (final var committed = target.beginNodeReadOnlyTrx(revision - 1)) {
            assertProjection(committed);
          }
          importer.importRevision(delta, after);
          try (final var committed = target.beginNodeReadOnlyTrx(revision)) {
            assertSnapshot(after, committed, 0);
            assertProjection(committed);
          }
          assertPaths(source, revision, target, revision);
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(original, copied, 0);
          assertProjection(original);
          assertProjection(copied);
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  private static void toggleRows(final JsonNodeTrx writer, final long group, final int score) {
    assertTrue(writer.moveTo(group));
    assertTrue(writer.moveToFirstChild());
    final long rows = writer.getNodeKey();
    if (writer.moveToFirstChild()) {
      writer.remove();
    } else {
      assertTrue(writer.moveTo(rows));
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"score\":" + score + "}"), JsonNodeTrx.Commit.NO);
    }
  }

  private static IndexDef projection() {
    return IndexDefs.createProjectionIdxDef(parse("/[]/rows/[]", PathParser.Type.JSON),
        List.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(List.of(0)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void boundaryChainsMovesAndReparentingSurviveRollback(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "[{\"rows\":[{\"score\":1},{\"score\":2}]},{\"rows\":[{\"score\":3},{\"score\":4}]}]"),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      final long firstGroup = writer.getNodeKey();
      assertTrue(writer.moveToFirstChild());
      assertTrue(writer.moveToFirstChild());
      final long firstRow = writer.getNodeKey();
      assertTrue(writer.moveTo(firstGroup));
      assertTrue(writer.moveToRightSibling());
      final long secondGroup = writer.getNodeKey();
      assertTrue(writer.moveToFirstChild());
      final long secondRows = writer.getNodeKey();
      final LongArrayList emptyGroups = new LongArrayList(6);
      for (int revision = 2; revision <= 4; revision++) {
        for (int empty = 0; empty < 2; empty++) {
          if (revision == 4) {
            assertTrue(writer.moveTo(firstGroup));
            writer.insertObjectAsRightSibling();
          } else {
            assertTrue(writer.moveTo(1));
            if (revision == 2) {
              writer.insertObjectAsLastChild();
            } else {
              writer.insertObjectAsFirstChild();
            }
          }
          emptyGroups.add(writer.getNodeKey());
        }
        writer.commit();
      }
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(emptyGroups.getLong(0));
      writer.commit();
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToLastChild());
      writer.moveSubtreeToRightSibling(emptyGroups.getLong(0));
      writer.commit();
      assertTrue(writer.moveTo(emptyGroups.getLong(0)));
      writer.insertObjectRecordAsFirstChild("rows", ArrayValue.INSTANCE);
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"score\":9}"), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(emptyGroups.getLong(0));
      writer.commit();
      for (final long key : emptyGroups) {
        assertTrue(writer.moveTo(key));
        writer.remove();
      }
      writer.commit();
      assertTrue(writer.moveTo(secondRows));
      writer.moveSubtreeToFirstChild(firstRow);
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(secondGroup);
      writer.commit();
      writer.revertTo(1);
      writer.commit();
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      final var controller = (JsonIndexController) target.getWtxIndexController(1);
      controller.createIndexes(Set.of(
          IndexDefs.createPathIdxDef(Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON),
          IndexDefs.createCASIdxDef(false, Type.LON, Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON)), writer);
      controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
      final var importer = (InternalJsonNodeTrx) writer;
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
      }
      for (int revision = 2; revision <= 12; revision++) {
        try (final var before = source.beginNodeReadOnlyTrx(revision - 1);
            final var after = source.beginNodeReadOnlyTrx(revision)) {
          final var delta = JsonIdentityDeltaReader.between(before, after, revision);
          JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
            if (phase.equals("derived-state-finalized")) {
              throw new IllegalStateException("injected projection failure");
            }
          };
          assertEquals("injected projection failure",
              assertThrows(IllegalStateException.class, () -> importer.importRevision(delta, after)).getMessage());
          JsonNodeTrxImpl.replayTestHook = null;
          assertEquals(revision - 1, target.getMostRecentRevisionNumber());
          assertSnapshot(before, writer, 0);
          try (final var committed = target.beginNodeReadOnlyTrx(revision - 1)) {
            assertProjection(committed);
          }
          importer.importRevision(delta, after);
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 12; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(original, copied, 0);
          assertProjection(copied);
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void containerPermutationsAndRestorationCrossMaintenanceBatches(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      final StringBuilder json = new StringBuilder(8000).append('[');
      for (int group = 0; group < 2; group++) {
        if (group != 0) {
          json.append(',');
        }
        json.append("{\"rows\":[");
        for (int row = 0; row < 180; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"score\":").append(group * 180 + row).append('}');
        }
        json.append("]}");
      }
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      final long firstGroup = writer.getNodeKey();
      assertTrue(writer.moveToRightSibling());
      final long secondGroup = writer.getNodeKey();
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(secondGroup);
      writer.commit();
      assertTrue(writer.moveTo(firstGroup));
      writer.remove();
      writer.commit();
      assertTrue(writer.moveTo(secondGroup));
      assertTrue(writer.moveToFirstChild());
      final long rows = writer.getNodeKey();
      writer.setObjectKeyName("archive");
      writer.commit();
      assertTrue(writer.moveTo(rows));
      writer.setObjectKeyName("rows");
      writer.commit();
      writer.revertTo(1);
      writer.commit();
      writer.commit();
    }
    for (final int start : new int[] {1, 2}) {
      final Path targetPath = directory.resolve("target-" + start);
      clearCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = create(targetPath, versioning, hash, dewey);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var writer = target.beginNodeTrx()) {
        final var controller = (JsonIndexController) target.getWtxIndexController(1);
        controller.createIndexes(Set.of(
            IndexDefs.createPathIdxDef(Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
                IndexDef.DbType.JSON),
            IndexDefs.createCASIdxDef(false, Type.LON, Set.of(parse("/[]/rows/[]/score", PathParser.Type.JSON)), 0,
                IndexDef.DbType.JSON)),
            writer);
        controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
        for (int revision = start; revision <= 7; revision++) {
          try (final var reader = source.beginNodeReadOnlyTrx(revision);
              final var base = source.beginNodeReadOnlyTrx(revision == start
                  ? 0
                  : revision - 1)) {
            final var delta = revision == start
                ? JsonIdentityDeltaReader.snapshot(reader, 1)
                : JsonIdentityDeltaReader.between(base, reader, revision - start + 1);
            ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
          }
        }
      }
      clearCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = Databases.openJsonDatabase(targetPath);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource")) {
        for (int revision = start; revision <= 7; revision++) {
          final int destinationRevision = revision - start + 1;
          try (final var original = source.beginNodeReadOnlyTrx(revision);
              final var copied = target.beginNodeReadOnlyTrx(destinationRevision)) {
            assertSnapshot(original, copied, start - 1);
            assertProjection(copied);
          }
          assertPaths(source, revision, target, destinationRevision);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void emptyDocumentBootstrapAndRecordSetRestoration(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.commit();
      writer.insertArrayAsFirstChild();
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"Aa\":7}"), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.remove();
      writer.commit();
      writer.revertTo(3);
      writer.commit();
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      JsonIdentityIndexOracle.declare(writer);
      for (int revision = 1; revision <= 5; revision++) {
        try (final var reader = source.beginNodeReadOnlyTrx(revision);
            final var base = source.beginNodeReadOnlyTrx(revision - 1)) {
          final var delta = revision == 1
              ? JsonIdentityDeltaReader.snapshot(reader, 1)
              : JsonIdentityDeltaReader.between(base, reader, revision);
          ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 5; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(original, copied, 0);
          JsonIdentityIndexOracle.assertIndexes(original, copied);
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  private static void assertProjection(final JsonNodeReadOnlyTrx reader) {
    final LongSet expectedFields = new LongOpenHashSet();
    final LongArrayList expectedKeys = new LongArrayList();
    final LongArrayList expectedValues = new LongArrayList();
    reader.moveToDocumentRoot();
    final var axis = new DescendantAxis(reader);
    while (axis.hasNext()) {
      axis.nextLong();
      if (reader.getKind() == NodeKind.OBJECT_NAMED_NUMBER && reader.getName().getLocalName().equals("score")) {
        final long field = reader.getNodeKey();
        final long record = reader.getParentKey();
        final long value = reader.getNumberValue().longValue();
        assertTrue(reader.moveToParent());
        assertTrue(reader.moveToParent());
        if (reader.getKind() == NodeKind.OBJECT_NAMED_ARRAY && reader.getName().getLocalName().equals("rows")) {
          expectedFields.add(field);
          expectedKeys.add(record);
          expectedValues.add(value);
        }
        assertTrue(reader.moveTo(field));
      }
    }
    final var session = reader.getResourceSession();
    final int revision = reader.getRevisionNumber();
    final var controller = session.getRtxIndexController(revision);
    assertEquals(expectedFields,
        collect(controller.openPathIndex(reader.getStorageEngineReader(),
            controller.getIndexes().getIndexDef(0, IndexType.PATH), controller.createPathFilter(Set.of(), reader))),
        "filtered PATH memberships after a container rename");
    assertEquals(expectedFields,
        collect(controller.openCASIndex(reader.getStorageEngineReader(),
            controller.getIndexes().getIndexDef(0, IndexType.CAS),
            controller.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(reader)))),
        "filtered CAS memberships after a container rename");
    final var handle = ProjectionIndexCatalog.load(session, revision, projection());
    assertNotNull(handle);
    final LongArrayList keys = new LongArrayList();
    final LongArrayList values = new LongArrayList();
    byte[] previousLabel = null;
    for (final byte[] payload : handle.rowGroupPayloads(
        ProjectionIndexCatalog.rowGroupMaterializer(session, revision, 0, handle.rowGroupCount()))) {
      final var page = ProjectionIndexRowGroupPage.deserialize(payload);
      for (int row = 0; row < page.getRowCount(); row++) {
        keys.add(page.recordKeys()[row]);
        values.add(page.numericColumn(0)[row]);
        previousLabel = ProjectionIdentityEpochOracle.assertOrder(page, row, previousLabel);
      }
    }
    ProjectionIdentityEpochOracle.assertSortedRows(reader, 0, expectedKeys, expectedValues);
    assertEquals(expectedKeys, keys);
    assertEquals(expectedValues, values);
  }

  private static LongSet collect(final Iterator<NodeReferences> references) {
    final LongSet keys = new LongOpenHashSet();
    while (references.hasNext()) {
      final var iterator = references.next().getNodeKeys().getLongIterator();
      while (iterator.hasNext()) {
        keys.add(iterator.next());
      }
    }
    return keys;
  }

}

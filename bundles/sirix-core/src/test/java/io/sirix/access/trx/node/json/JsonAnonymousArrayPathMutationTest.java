package io.sirix.access.trx.node.json;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.create;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonAnonymousArrayPathMutationTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    JsonNodeTrxImpl.replayTestHook = null;
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations().flatMap(configuration -> Stream.of(false, true).map(shared -> {
      final Object[] values = configuration.get();
      return Arguments.of(values[0], values[1], values[2], shared);
    }));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void nestedArrayRenamesAndMovesKeepColdSourceAndCopiedPathMemberships(final VersioningType versioning,
      final HashType hash, final boolean dewey, final boolean shared) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(shared
          ? "[{\"rows\":[[1]]},{\"rows\":[[2]]}]"
          : "[{\"rows\":[[1]]},{\"spare\":[[2]]}]"), JsonNodeTrx.Commit.NO);
      declarePathIndex(writer);
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      final long firstObject = writer.getNodeKey();
      assertTrue(writer.moveToFirstChild());
      final long renamedField = writer.getNodeKey();
      assertTrue(writer.moveToFirstChild());
      final long movedArray = writer.getNodeKey();
      assertTrue(writer.moveTo(firstObject));
      assertTrue(writer.moveToRightSibling());
      assertTrue(writer.moveToFirstChild());
      final long destination = writer.getNodeKey();
      assertTrue(writer.moveToFirstChild());
      final long otherArray = writer.getNodeKey();
      writer.commit();
      assertTrue(writer.moveTo(renamedField));
      writer.setObjectKeyName("archive");
      JsonStructuralHashInvariantTest.assertGraph(writer, hash);
      writer.rollback();
      assertTrue(writer.moveTo(renamedField));
      assertEquals("rows", writer.getName().getLocalName());
      writer.setObjectKeyName("archive");
      writer.commit();
      assertTrue(writer.moveTo(destination));
      writer.moveSubtreeToFirstChild(movedArray);
      JsonStructuralHashInvariantTest.assertGraph(writer, hash);
      writer.rollback();
      assertTrue(writer.moveTo(destination));
      writer.moveSubtreeToFirstChild(movedArray);
      writer.commit();
      assertTrue(writer.moveTo(otherArray));
      writer.moveSubtreeToRightSibling(movedArray);
      writer.commit();
      assertTrue(writer.moveTo(renamedField));
      writer.insertArrayAsFirstChild();
      final long placeholder = writer.getNodeKey();
      writer.commit();
      assertTrue(writer.moveTo(placeholder));
      writer.moveSubtreeToRightSibling(movedArray);
      writer.commit();
      assertTrue(writer.moveTo(movedArray));
      writer.moveSubtreeToLeftSibling(otherArray);
      writer.commit();
      assertTrue(writer.moveTo(placeholder));
      writer.moveSubtreeToLeftSibling(movedArray);
      writer.commit();
      assertTrue(writer.moveTo(renamedField));
      writer.setObjectKeyName("final");
      writer.commit();
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      declarePathIndex(writer);
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
              throw new IllegalStateException("array path retry");
            }
          };
          assertThrows(IllegalStateException.class, () -> importer.importRevision(delta, after));
          JsonNodeTrxImpl.replayTestHook = null;
          assertSnapshot(before, writer, 0);
          importer.importRevision(delta, after);
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      final Set<String> paths = new HashSet<>();
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision)) {
          paths.addAll(documentPaths(original).keySet());
        }
      }
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(original, copied, 0);
          JsonStructuralHashInvariantTest.assertGraph(original, hash);
          JsonStructuralHashInvariantTest.assertGraph(copied, hash);
          final var expected = documentPaths(original);
          assertPathIndex(original, expected, paths);
          assertPathIndex(copied, expected, paths);
        }
      }
    }
  }

  private static void declarePathIndex(final JsonNodeTrx writer) {
    writer.getResourceSession()
          .getWtxIndexController(writer.getRevisionNumber())
          .createIndexes(Set.of(IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON)), writer);
  }

  private static Map<String, LongSet> documentPaths(final JsonNodeReadOnlyTrx reader) {
    final Map<String, LongSet> paths = new HashMap<>();
    walk(reader, 0, "", paths);
    return paths;
  }

  private static void walk(final JsonNodeReadOnlyTrx reader, final long key, final String parentPath,
      final Map<String, LongSet> paths) {
    assertTrue(reader.moveTo(key));
    final NodeKind kind = reader.getKind();
    String path = parentPath;
    if (kind.playsObjectKeyRole()) {
      path += "/" + reader.getName().getLocalName();
      paths.computeIfAbsent(path, ignored -> new LongOpenHashSet()).add(key);
    }
    if (kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_NAMED_ARRAY) {
      path += "/[]";
      paths.computeIfAbsent(path, ignored -> new LongOpenHashSet()).add(key);
    }
    long child = reader.getFirstChildKey();
    while (child >= 0) {
      walk(reader, child, path, paths);
      assertTrue(reader.moveTo(child));
      child = reader.getRightSiblingKey();
    }
  }

  private static void assertPathIndex(final JsonNodeReadOnlyTrx reader, final Map<String, LongSet> expected,
      final Set<String> allPaths) {
    final var controller = reader.getResourceSession().getRtxIndexController(reader.getRevisionNumber());
    final var definition = controller.getIndexes().getIndexDef(0, IndexType.PATH);
    final LongSet allKeys = new LongOpenHashSet();
    for (final var keys : expected.values()) {
      allKeys.addAll(keys);
    }
    assertEquals(allKeys, collect(controller.openPathIndex(reader.getStorageEngineReader(), definition,
        controller.createPathFilter(Set.of(), reader))), "all PATH identities");
    for (final String path : allPaths) {
      assertEquals(expected.getOrDefault(path, new LongOpenHashSet()),
          collect(controller.openPathIndex(reader.getStorageEngineReader(), definition,
              controller.createPathFilter(Set.of(path), reader))),
          "PATH " + path + " revision " + reader.getRevisionNumber());
    }
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

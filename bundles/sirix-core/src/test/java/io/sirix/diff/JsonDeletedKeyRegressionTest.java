package io.sirix.diff;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonResourceCopy;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static io.sirix.diff.DiffTestHelper.assertJsonCopyStructure;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Persistent identity must survive deletion followed by explicit-key recreation or revert. */
final class JsonDeletedKeyRegressionTest {
  private static final String RESOURCE = "deleted-key";
  private static final String DOCUMENT = "[{\"x\":\"live\",\"keep\":7}]";

  @TempDir
  File directory;

  private static Stream<Arguments> storageModes() {
    return Stream.of(VersioningType.values())
                 .flatMap(type -> Stream.of(Arguments.of(type, false), Arguments.of(type, true)));
  }

  private static Stream<Arguments> replayModes() {
    return Stream.of(VersioningType.values())
                 .flatMap(type -> Stream.of(Arguments.of(type, false, false), Arguments.of(type, false, true),
                     Arguments.of(type, true, false), Arguments.of(type, true, true)));
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  void explicitCopyRecreatesDeletedSlots(final VersioningType versioning, final boolean deweyIDs) throws Exception {
    final long[] recreatedHashes;
    try (final var sourceDb = createDatabase("source", versioning, deweyIDs);
        final var source = sourceDb.beginResourceSession(RESOURCE);
        final var targetDb = createDatabase("target", versioning, deweyIDs);
        final var target = targetDb.beginResourceSession(RESOURCE)) {
      seed(source, DOCUMENT);
      try (final var reader = source.beginNodeReadOnlyTrx(1); final var writer = target.beginNodeTrx()) {
        final Set<Path<QNm>> paths = Set.of(Path.parse("/[]/x", PathParser.Type.JSON));
        target.getWtxIndexController(writer.getRevisionNumber())
              .createIndexes(Set.of(IndexDefs.createPathIdxDef(paths, 0, IndexDef.DbType.JSON),
                  IndexDefs.createCASIdxDef(false, Type.STR, paths, 0, IndexDef.DbType.JSON),
                  IndexDefs.createNameIdxDef(1, IndexDef.DbType.JSON)), writer);
        copyNodes(reader, writer);
        writer.commit();
        assertLiveCopy(reader, writer, target.getWtxIndexController(writer.getRevisionNumber()));
        assertTrue(writer.moveTo(1));
        writer.remove();
        assertFalse(writer.moveTo(1));
        assertIndexKeys(writer, target.getWtxIndexController(writer.getRevisionNumber()), Set.of());
        copyNodes(reader, writer);
        assertLiveCopy(reader, writer, target.getWtxIndexController(writer.getRevisionNumber()));
        recreatedHashes = captureHashes(writer);
        writer.commit();
        assertLiveCopy(reader, writer, target.getWtxIndexController(writer.getRevisionNumber()));
        assertHashes(recreatedHashes, writer);
      }
      assertEquals(DOCUMENT, serialize(target, 2));
      try (final var reader = source.beginNodeReadOnlyTrx(1); final var committed = target.beginNodeReadOnlyTrx(2)) {
        assertLiveCopy(reader, committed, target.getRtxIndexController(2));
        assertHashes(recreatedHashes, committed);
      }
    }
    // Close both databases and reopen with fresh resource sessions and empty page caches.
    Databases.getGlobalBufferManager().clearAllCaches();
    try (final var sourceDb = Databases.openJsonDatabase(directory.toPath().resolve("source"));
        final var source = sourceDb.beginResourceSession(RESOURCE);
        final var targetDb = Databases.openJsonDatabase(directory.toPath().resolve("target"));
        final var target = targetDb.beginResourceSession(RESOURCE);
        final var reader = source.beginNodeReadOnlyTrx(1);
        final var reopened = target.beginNodeReadOnlyTrx(2)) {
      assertEquals(DOCUMENT, serialize(target, 2));
      assertLiveCopy(reader, reopened, target.getRtxIndexController(2));
      assertHashes(recreatedHashes, reopened);
      assertEquals(DOCUMENT, serialize(target, 1), "recreation must leave the earlier revision immutable");
    }
  }

  @ParameterizedTest
  @MethodSource("replayModes")
  void revertRestoresDeletedKeyDuringRevisionCopy(final VersioningType versioning, final boolean deweyIDs,
      final boolean removeSidecars) throws Exception {
    try (final var sourceDb = createDatabase("source", versioning, deweyIDs);
        final var source = sourceDb.beginResourceSession(RESOURCE)) {
      seed(source, "[0,1]");
      try (final var writer = source.beginNodeTrx()) {
        assertTrue(writer.moveTo(3));
        writer.remove();
        writer.commit();
        writer.revertTo(1);
        writer.commit();
      }
      assertEquals("[0,1]", serialize(source, 1));
      assertEquals("[0]", serialize(source, 2));
      assertEquals("[0,1]", serialize(source, 3));
      if (removeSidecars) {
        deleteSidecars(source);
      }
      assertCopiedRevisions(source, versioning, deweyIDs);
    }
    Databases.getGlobalBufferManager().clearAllCaches();
    try (final var sourceDb = Databases.openJsonDatabase(directory.toPath().resolve("source"));
        final var source = sourceDb.beginResourceSession(RESOURCE);
        final var targetDb = Databases.openJsonDatabase(directory.toPath().resolve("target"));
        final var target = targetDb.beginResourceSession(RESOURCE)) {
      assertRevisions(source, target);
    }
  }

  @Disabled("R16: later-created object parents require the separate typed identity replay/import redesign")
  @ParameterizedTest
  @MethodSource("replayModes")
  void laterCreatedObjectParentRequiresReplayRedesign(final VersioningType versioning, final boolean deweyIDs,
      final boolean removeSidecars) throws Exception {
    try (final var sourceDb = createDatabase("source", versioning, deweyIDs);
        final var source = sourceDb.beginResourceSession(RESOURCE)) {
      seed(source, "[0]");
      try (final var writer = source.beginNodeTrx()) {
        assertTrue(writer.moveTo(1));
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[{\"x\":1},{}]"), JsonNodeTrx.Commit.NO,
            JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
        assertTrue(writer.moveTo(5));
        writer.moveSubtreeToFirstChild(4);
        assertTrue(writer.moveTo(3));
        writer.remove();
        writer.commit();
      }
      assertEquals("[0,{\"x\":1}]", serialize(source, 2));
      if (removeSidecars) {
        deleteSidecars(source);
      }
      assertCopiedRevisions(source, versioning, deweyIDs);
    }
  }

  private Database<JsonResourceSession> createDatabase(final String name, final VersioningType versioning,
      final boolean deweyIDs) {
    final var path = directory.toPath().resolve(name);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .hashKind(HashType.ROLLING)
                                                 .versioningApproach(versioning)
                                                 .maxNumberOfRevisionsToRestore(4)
                                                 .useDeweyIDs(deweyIDs)
                                                 .buildPathSummary(true)
                                                 .build());
    return database;
  }

  private static void seed(final JsonResourceSession session, final String json) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      writer.commit();
    }
  }

  private static void copyNodes(final JsonNodeReadOnlyTrx source, final JsonNodeTrx target) {
    source.moveToDocumentRoot();
    target.moveToDocumentRoot();
    final var nodes = new DescendantAxis(source);
    while (nodes.hasNext()) {
      nodes.nextLong();
      final InsertPosition position;
      if (source.hasLeftSibling()) {
        assertTrue(target.moveTo(source.getLeftSiblingKey()));
        position = InsertPosition.AS_RIGHT_SIBLING;
      } else {
        assertTrue(target.moveTo(source.getParentKey()));
        position = InsertPosition.AS_FIRST_CHILD;
      }
      target.copyNodeWithKey(source, position);
      assertEquals(source.getNodeKey(), target.getNodeKey());
    }
  }

  private static void assertLiveCopy(final JsonNodeReadOnlyTrx source, final JsonNodeReadOnlyTrx target,
      final JsonIndexController controller) {
    assertStructure(source, target);
    assertTrue(target.moveTo(3));
    assertEquals("live", target.getValue());
    assertEquals("x", target.getName().getLocalName());
    assertIndexKeys(target, controller, Set.of(3L));
  }

  private static long[] captureHashes(final JsonNodeReadOnlyTrx reader) {
    final long anchor = reader.getNodeKey();
    final long[] hashes = new long[Math.toIntExact(reader.getMaxNodeKey()) + 1];
    reader.moveToDocumentRoot();
    final var nodes = new DescendantAxis(reader, IncludeSelf.YES);
    while (nodes.hasNext()) {
      hashes[Math.toIntExact(nodes.nextLong())] = reader.getHash();
    }
    assertTrue(reader.moveTo(anchor));
    return hashes;
  }

  private static void assertHashes(final long[] expected, final JsonNodeReadOnlyTrx reader) {
    final long[] actual = captureHashes(reader);
    assertArrayEquals(expected, actual, "the recreated record hashes must survive commit and cold reopen");
  }

  private static void assertIndexKeys(final JsonNodeReadOnlyTrx reader, final JsonIndexController controller,
      final Set<Long> expected) {
    final var indexes = controller.getIndexes();
    assertEquals(expected,
        keys(controller.openPathIndex(reader.getStorageEngineReader(), indexes.getIndexDef(0, IndexType.PATH), null)));
    assertEquals(expected, keys(controller.openCASIndex(reader.getStorageEngineReader(),
        indexes.getIndexDef(0, IndexType.CAS), (CASFilter) null)));
    assertEquals(expected,
        keys(controller.openNameIndex(reader.getStorageEngineReader(),
            indexes.getIndexDef(IndexDefs.createNameIdxDef(1, IndexDef.DbType.JSON).getID(), IndexType.NAME),
            controller.createNameFilter(Set.of("x")))));
  }

  private static Set<Long> keys(final Iterator<NodeReferences> postings) {
    final Set<Long> keys = new HashSet<>();
    while (postings.hasNext()) {
      final var iterator = postings.next().nodeKeyIterator();
      while (iterator.hasNext()) {
        assertTrue(keys.add(iterator.next()), "index must not contain duplicate node keys");
      }
    }
    return keys;
  }

  private void assertCopiedRevisions(final JsonResourceSession source, final VersioningType versioning,
      final boolean deweyIDs) throws Exception {
    try (final var database = createDatabase("target", versioning, deweyIDs);
        final var target = database.beginResourceSession(RESOURCE);
        final var reader = source.beginNodeReadOnlyTrx(1);
        final var writer = target.beginNodeTrx()) {
      new JsonResourceCopy.Builder(writer, reader, InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent()
                                                                                 .build()
                                                                                 .call();
      assertRevisions(source, target);
    }
  }

  private static void assertRevisions(final JsonResourceSession source, final JsonResourceSession target)
      throws Exception {
    assertEquals(source.getMostRecentRevisionNumber(), target.getMostRecentRevisionNumber());
    for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
      assertEquals(serialize(source, revision), serialize(target, revision));
      try (final var reader = source.beginNodeReadOnlyTrx(revision);
          final var copied = target.beginNodeReadOnlyTrx(revision)) {
        assertStructure(reader, copied);
        for (long key = 0; key <= reader.getMaxNodeKey(); key++) {
          assertEquals(reader.moveTo(key), copied.moveTo(key), "live and deleted keys must match: " + key);
        }
      }
    }
  }

  private static void assertStructure(final JsonNodeReadOnlyTrx source, final JsonNodeReadOnlyTrx copy) {
    assertJsonCopyStructure(source, copy);
    source.moveToDocumentRoot();
    final var nodes = new DescendantAxis(source, IncludeSelf.YES);
    while (nodes.hasNext()) {
      assertTrue(copy.moveTo(nodes.nextLong()));
      assertEquals(source.getDescendantCount(), copy.getDescendantCount());
      assertEquals(source.getDeweyID(), copy.getDeweyID());
    }
  }

  private static void deleteSidecars(final JsonResourceSession session) throws Exception {
    final var diffDirectory = session.getResourceConfig().resourcePath.resolve(
        ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());
    try (final var sidecars = Files.list(diffDirectory)) {
      for (final var sidecar : sidecars.toList()) {
        Files.delete(sidecar);
      }
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    final var output = new StringWriter();
    JsonSerializer.newBuilder(session, output, revision).build().call();
    return output.toString();
  }
}

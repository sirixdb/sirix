package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.io.StorageType;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.summary.PathStats;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.Bytes;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonIdentityImportTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearHook() {
    JsonNodeTrxImpl.replayTestHook = null;
  }

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values()).flatMap(versioning -> Stream.of(HashType.values())
        .flatMap(hash -> Stream.of(false, true).map(dewey -> Arguments.of(versioning, hash, dewey))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void snapshotPreservesLaterParentAndSparseFrontier(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var source = database.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
        writer.commit();
        assertTrue(writer.moveTo(1));
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[{\"x\":1},{}]"),
            JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
        assertTrue(writer.moveTo(5));
        writer.moveSubtreeToFirstChild(4);
        assertTrue(writer.moveTo(3));
        writer.remove();
        writer.getStorageEngineReader().getActualRevisionRootPage().setMaxNodeKeyInDocumentIndex(1_000_000);
        writer.commit();
      }
      try (final var targetDb = create(targetPath, versioning, hash, dewey);
          final var target = targetDb.beginResourceSession("resource");
          final var reader = source.beginNodeReadOnlyTrx(2);
          final var writer = target.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
          if (phase.equals("links-installed")) {
            assertSnapshot(reader, transaction, 1);
            // Check the immutable source separately before the importer validates its staged graph.
            JsonReplayGraphValidator.validate(reader);
          }
        };
        ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader);
        JsonNodeTrxImpl.replayTestHook = null;
        assertEquals(1, target.getMostRecentRevisionNumber(), "staging must never auto-publish a revision");
      }
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var original = source.beginNodeReadOnlyTrx(2);
        final var copy = target.beginNodeReadOnlyTrx(1)) {
      assertEquals("[0,{\"x\":1}]", serialize(target, 1));
      assertSnapshot(original, copy, 1);
      assertFalse(copy.moveTo(3), "transient parent must remain absent");
      assertTrue(copy.moveTo(4));
      assertEquals(5, copy.getParentKey());
      assertEquals(1_000_000, copy.getMaxNodeKey());
      assertPaths(source, 2, target, 1);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void snapshotPreservesAllPayloadsAndSparseNameBindings(final VersioningType versioning) throws Exception {
    final String overflow = "🧪x".repeat(256);
    for (final boolean dewey : new boolean[] {false, true}) {
      final Path sourcePath = directory.resolve("values-source-" + dewey);
      final Path targetPath = directory.resolve("values-target-" + dewey);
      try (final var sourceDb = create(sourcePath, versioning, HashType.ROLLING, dewey);
          final var targetDb = create(targetPath, versioning, HashType.ROLLING, dewey);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource")) {
        try (final var writer = source.beginNodeTrx()) {
          final String json = "[{\"Aa\":-17,\"BB\":1.000,\"array\":[null,true,false,\"\"],"
              + "\"object\":{\"unicode\":\"" + overflow + "\"},\"nil\":null,\"boolean\":true},"
              + "\"" + overflow + "\",3.14159,null,false]";
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          assertTrue(writer.moveTo(3));
          assertEquals("Aa", writer.getName().getLocalName());
          writer.remove();
          writer.commit();
        }
        try (final var reader = source.beginNodeReadOnlyTrx(1); final var writer = target.beginNodeTrx()) {
          ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader);
        }
      }
      Databases.clearGlobalCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = Databases.openJsonDatabase(targetPath);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var original = source.beginNodeReadOnlyTrx(1);
          final var copy = target.beginNodeReadOnlyTrx(1)) {
        assertSnapshot(original, copy, 0);
        assertEquals(serialize(source, 1), serialize(target, 1));
        assertTrue(original.moveTo(4));
        assertTrue(copy.moveTo(4));
        assertEquals(original.getNameKey(), copy.getNameKey());
        assertEquals("BB", copy.getName().getLocalName());
        assertFalse(copy.moveTo(3));
        assertPaths(source, 1, target, 1);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void snapshotRebuildsPrimitiveIndexesAfterColdReopen(final VersioningType versioning) {
    final Path sourcePath = directory.resolve("indexed-source");
    final Path targetPath = directory.resolve("indexed-target");
    try (final var sourceDb = create(sourcePath, versioning, HashType.ROLLING, true);
        final var targetDb = create(targetPath, versioning, HashType.ROLLING, true);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        source.getWtxIndexController(writer.getRevisionNumber()).createIndexes(primitiveIndexes(), writer);
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "[{\"dept\":\"A\",\"tags\":[\"a\",\"b\"]},{\"dept\":\"B\",\"tags\":[\"b\"]}]"), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
      try (final var reader = source.beginNodeReadOnlyTrx(1); final var writer = target.beginNodeTrx()) {
        target.getWtxIndexController(writer.getRevisionNumber()).createIndexes(primitiveIndexes(), writer);
        ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader);
      }
    }
    Databases.clearGlobalCaches();
    for (final Path path : new Path[] {sourcePath, targetPath}) {
      try (final var database = Databases.openJsonDatabase(path);
          final var session = database.beginResourceSession("resource");
          final var reader = session.beginNodeReadOnlyTrx(1)) {
        final JsonIndexController controller = session.getRtxIndexController(1);
        for (int definition = 0; definition < 2; definition++) {
          final var pathDefinition = controller.getIndexes().getIndexDef(definition, IndexType.PATH);
          assertEquals(LongSet.of(4, 9), collect(controller.openPathIndex(reader.getStorageEngineReader(), pathDefinition,
              controller.createPathFilter(Set.of("/[]/tags"), reader))));
          final var casDefinition = controller.getIndexes().getIndexDef(definition, IndexType.CAS);
          assertEquals(LongSet.of(6, 10), collect(controller.openCASIndex(reader.getStorageEngineReader(), casDefinition,
              controller.createCASFilter(Set.of("/[]/tags/[]"), new Str("b"), SearchMode.EQUAL, new JsonPCRCollector(reader)))));
        }
        final int nameId = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID();
        final var nameDefinition = controller.getIndexes().getIndexDef(nameId, IndexType.NAME);
        assertEquals(LongSet.of(3, 8), collect(controller.openNameIndex(reader.getStorageEngineReader(), nameDefinition,
            controller.createNameFilter(Set.of("dept")))));
      }
    }
  }

  private static Set<IndexDef> primitiveIndexes() {
    return Set.of(IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON),
        IndexDefs.createPathIdxDef(Set.of(parse("/[]/tags", PathParser.Type.JSON)), 1, IndexDef.DbType.JSON),
        IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse("/[]/tags/[]", PathParser.Type.JSON)), 1, IndexDef.DbType.JSON));
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

  @ParameterizedTest
  @ValueSource(strings = {"identities-staged", "links-installed", "derived-state-finalized", "before-publish"})
  void failureCannotPublishAnyStagingPhase(final String failurePhase) {
    try (final var sourceDb = create(directory.resolve("source"), VersioningType.SLIDING_SNAPSHOT, HashType.ROLLING, true);
        final var targetDb = create(directory.resolve("target"), VersioningType.SLIDING_SNAPSHOT, HashType.ROLLING, true);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"Aa\":1,\"BB\":2},null,true,\"value\"]"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
      }
      try (final var reader = source.beginNodeReadOnlyTrx(1);
          final var writer = target.beginNodeTrx(1)) {
        final var delta = JsonIdentityDeltaReader.snapshot(reader, 1);
        JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
          assertEquals(0, target.getMostRecentRevisionNumber());
          if (phase.equals(failurePhase)) {
            throw new IllegalStateException("injected " + phase);
          }
        };
        final var failure = assertThrows(IllegalStateException.class,
            () -> ((InternalJsonNodeTrx) writer).importRevision(delta, reader));
        assertEquals("injected " + failurePhase, failure.getMessage());
        assertEquals(0, target.getMostRecentRevisionNumber());
        assertEquals(0, writer.getMaxNodeKey());
        assertFalse(writer.moveTo(1));
        writer.moveToDocumentRoot();
        assertFalse(writer.hasFirstChild());
        JsonNodeTrxImpl.replayTestHook = null;
        ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
        try (final var copy = target.beginNodeReadOnlyTrx(1)) {
          assertSnapshot(reader, copy, 0);
        }
        assertPaths(source, 1, target, 1);
      }
    }
  }

  @Test
  void dirtyDestinationIsRejectedWithoutDiscardingItsChanges() {
    try (final var sourceDb = create(directory.resolve("source"), VersioningType.FULL, HashType.NONE, false);
        final var targetDb = create(directory.resolve("target"), VersioningType.FULL, HashType.NONE, false);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx()) {
        writer.insertArrayAsFirstChild();
        writer.commit();
      }
      try (final var reader = source.beginNodeReadOnlyTrx(1); final var writer = target.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild("keep me");
        assertThrows(IllegalStateException.class,
            () -> ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader));
        assertEquals("keep me", writer.getValue());
        writer.rollback();
      }
    }
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType versioning,
      final HashType hash, final boolean dewey) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL)
        .versioningApproach(versioning).hashKind(hash).useDeweyIDs(dewey).buildPathStatistics(true).build());
    return database;
  }

  /** Independent complete source traversal; does not reuse replay records or their equality logic. */
  private static void assertSnapshot(final JsonNodeReadOnlyTrx source, final JsonNodeReadOnlyTrx target,
      final int revisionOffset) {
    source.moveToDocumentRoot();
    target.moveToDocumentRoot();
    assertEquals(source.getMaxNodeKey(), target.getMaxNodeKey());
    final var expected = new DescendantAxis(source, IncludeSelf.YES);
    final var actual = new DescendantAxis(target, IncludeSelf.YES);
    while (expected.hasNext()) {
      final long key = expected.nextLong();
      assertTrue(actual.hasNext(), "missing identity " + key);
      assertEquals(key, actual.nextLong());
      assertEquals(source.getKind(), target.getKind());
      assertEquals(source.getName(), target.getName());
      assertEquals(source.getPathNodeKey(), target.getPathNodeKey());
      if (source.getKind().playsObjectKeyRole()) {
        assertEquals(source.getNameKey(), target.getNameKey());
      }
      assertEquals(source.getParentKey(), target.getParentKey());
      assertEquals(source.getFirstChildKey(), target.getFirstChildKey());
      assertEquals(source.getLastChildKey(), target.getLastChildKey());
      assertEquals(source.getLeftSiblingKey(), target.getLeftSiblingKey());
      assertEquals(source.getRightSiblingKey(), target.getRightSiblingKey());
      assertEquals(source.getChildCount(), target.getChildCount());
      assertEquals(source.getDescendantCount(), target.getDescendantCount());
      assertEquals(source.getDeweyID(), target.getDeweyID());
      assertEquals(source.getHash(), target.getHash());
      if (key != 0) {
        final int previous = source.getPreviousRevisionNumber();
        final int modified = source.getNode().getLastModifiedRevisionNumber();
        assertEquals(previous < 0 ? previous : (previous - revisionOffset > 0 ? previous - revisionOffset : -1), target.getPreviousRevisionNumber());
        assertEquals(modified < 0 ? modified : Math.max(1, modified - revisionOffset), target.getNode().getLastModifiedRevisionNumber());
      }
      final NodeKind kind = source.getKind();
      switch (kind) {
        case STRING_VALUE, OBJECT_NAMED_STRING -> assertEquals(source.getValue(), target.getValue());
        case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> assertEquals(source.getNumberValue(), target.getNumberValue());
        case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> assertEquals(source.getBooleanValue(), target.getBooleanValue());
        default -> { }
      }
    }
    assertFalse(actual.hasNext(), "unexpected copied identity");
  }

  private static void assertPaths(final JsonResourceSession source, final int sourceRevision,
      final JsonResourceSession target, final int targetRevision) {
    try (final var expected = source.openPathSummary(sourceRevision);
        final var actual = target.openPathSummary(targetRevision)) {
      assertEquals(expected.getMaxNodeKey(), actual.getMaxNodeKey());
      final var expectedAxis = new DescendantAxis(expected);
      final var actualAxis = new DescendantAxis(actual);
      while (expectedAxis.hasNext()) {
        assertTrue(actualAxis.hasNext());
        assertEquals(expectedAxis.nextLong(), actualAxis.nextLong());
        assertEquals(expected.getPath(), actual.getPath());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getPathKind(), actual.getPathKind());
        assertEquals(expected.getParentKey(), actual.getParentKey());
        assertEquals(expected.getFirstChildKey(), actual.getFirstChildKey());
        assertEquals(expected.getLastChildKey(), actual.getLastChildKey());
        assertEquals(expected.getLeftSiblingKey(), actual.getLeftSiblingKey());
        assertEquals(expected.getRightSiblingKey(), actual.getRightSiblingKey());
        assertEquals(expected.getChildCount(), actual.getChildCount());
        assertEquals(expected.getDescendantCount(), actual.getDescendantCount());
        assertEquals(expected.getPathNode().getLevel(), actual.getPathNode().getLevel());
        assertEquals(expected.getPathNode().getLocalNameKey(), actual.getPathNode().getLocalNameKey());
        assertEquals(expected.getPathNode().getReferences(), actual.getPathNode().getReferences());
        assertStats(expected.getPathNode().getStats(), actual.getPathNode().getStats());
      }
      assertFalse(actualAxis.hasNext());
    }
  }

  private static void assertStats(final PathStats expected, final PathStats actual) {
    if (expected == null || actual == null) {
      assertEquals(expected, actual);
      return;
    }
    assertEquals(expected.count, actual.count);
    assertEquals(expected.nullCount, actual.nullCount);
    assertEquals(expected.sum, actual.sum);
    assertEquals(expected.sumHi, actual.sumHi);
    assertEquals(expected.sumFraction, actual.sumFraction);
    assertEquals(expected.min, actual.min);
    assertEquals(expected.max, actual.max);
    assertEquals(expected.minDirty, actual.minDirty);
    assertEquals(expected.maxDirty, actual.maxDirty);
    assertEquals(expected.sumDirty, actual.sumDirty);
    assertEquals(expected.countDirty, actual.countDirty);
    assertEquals(expected.doubleTyped, actual.doubleTyped);
    assertArrayEquals(expected.minBytes, actual.minBytes);
    assertArrayEquals(expected.maxBytes, actual.maxBytes);
    assertArrayEquals(expected.hll == null ? null : expected.hll.serialize(),
        actual.hll == null ? null : actual.hll.serialize());
    // The serialized comparison also checks the package-private page-presence bitmap.
    try (final var expectedBytes = Bytes.elasticHeapByteBuffer(); final var actualBytes = Bytes.elasticHeapByteBuffer()) {
      expected.writeTo(expectedBytes);
      actual.writeTo(actualBytes);
      assertArrayEquals(expectedBytes.toByteArray(), actualBytes.toByteArray());
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    final StringWriter output = new StringWriter();
    new JsonSerializer.Builder(session, output, revision).build().call();
    return output.toString();
  }
}

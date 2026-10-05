package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.interfaces.StructNode;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct source-only oracle; deliberately has no dependency on identity replay. */
@Isolated
final class JsonStructuralHashInvariantTest {
  private static final long PRIME = 77081L;

  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(
                     versioning -> Stream.of(HashType.values())
                                         .flatMap(hash -> Stream.of(false, true)
                                                                .map(dewey -> Arguments.of(versioning, hash, dewey))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void laterParentMoveAndRemovalPreserveCommittedCountsAndHashes(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    final Path path = directory.resolve("r16");
    try (final var database = create(path, versioning, hash, dewey, true);
        final var session = database.beginResourceSession("resource")) {
      try (final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
        assertGraph(writer, hash);
        writer.commit();
        assertTrue(writer.moveTo(1));
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[{\"x\":1},{}]"), JsonNodeTrx.Commit.NO,
            JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
        assertGraph(writer, hash);
        assertTrue(writer.moveTo(5));
        writer.moveSubtreeToFirstChild(4);
        assertGraph(writer, hash);
        assertTrue(writer.moveTo(3));
        writer.remove();
        assertGraph(writer, hash);
        writer.commit();
      }
    }
    assertReopened(path, hash, "[0,{\"x\":1}]");
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void everyInsertedForestRootContributesAtEveryPosition(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    for (final InsertPosition position : InsertPosition.values()) {
      for (final boolean diffs : new boolean[] {false, true}) {
        final Path path = directory.resolve(position + "-" + diffs);
        try (final var database = create(path, versioning, hash, dewey, diffs);
            final var session = database.beginResourceSession("resource");
            final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,9]"), JsonNodeTrx.Commit.NO);
          writer.commit();
          final var input = JsonShredder.createStringReader("[1,{\"nested\":[2,3]},[]]");
          switch (position) {
            case AS_FIRST_CHILD -> {
              assertTrue(writer.moveTo(1));
              writer.insertSubtreeAsFirstChild(input, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
                  JsonNodeTrx.SkipRootToken.YES);
            }
            case AS_LAST_CHILD -> {
              assertTrue(writer.moveTo(1));
              writer.insertSubtreeAsLastChild(input, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
                  JsonNodeTrx.SkipRootToken.YES);
            }
            case AS_LEFT_SIBLING -> {
              assertTrue(writer.moveTo(3));
              writer.insertSubtreeAsLeftSibling(input, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
                  JsonNodeTrx.SkipRootToken.YES);
            }
            case AS_RIGHT_SIBLING -> {
              assertTrue(writer.moveTo(2));
              writer.insertSubtreeAsRightSibling(input, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
                  JsonNodeTrx.SkipRootToken.YES);
            }
          }
          assertGraph(writer, hash);
          writer.commit();
        }
        final String expected = switch (position) {
          case AS_FIRST_CHILD -> "[1,{\"nested\":[2,3]},[],0,9]";
          case AS_LAST_CHILD -> "[0,9,1,{\"nested\":[2,3]},[]]";
          case AS_LEFT_SIBLING, AS_RIGHT_SIBLING -> "[0,1,{\"nested\":[2,3]},[],9]";
        };
        assertReopened(path, hash, expected);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"jackson", "item"})
  void siblingForestPreservesInputOrderInEveryShredder(final String inputKind) throws Exception {
    final Path path = directory.resolve(inputKind);
    try (final var database = create(path, VersioningType.SLIDING_SNAPSHOT, HashType.ROLLING, true, true);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,9]"), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(3));
      if (inputKind.equals("jackson")) {
        try (final var parser = JacksonJsonShredder.createStringParser("[1,{\"nested\":[2,3]},[]]")) {
          writer.insertSubtreeAsLeftSibling(parser, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
              JsonNodeTrx.SkipRootToken.YES);
        }
      } else {
        final var object = new ArrayObject(new QNm[] {new QNm("nested")},
            new Sequence[] {new DArray(List.of(new Int32(2), new Int32(3)))});
        final var input = new DArray(List.of(new Int32(1), object, new DArray(List.of())));
        writer.insertSubtreeAsLeftSibling(input, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
            JsonNodeTrx.SkipRootToken.YES);
      }
      assertGraph(writer, HashType.ROLLING);
      writer.commit();
    }
    assertReopened(path, HashType.ROLLING, "[0,1,{\"nested\":[2,3]},[],9]");
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void intermediateAutoCommitRevisionsAlreadyHaveValidHashes(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    for (final boolean repair : new boolean[] {false, true}) {
      final Path path = directory.resolve("auto-" + repair);
      try (final var database = create(path, versioning, hash, dewey, true, repair);
          final var session = database.beginResourceSession("resource");
          final var writer = session.beginNodeTrx(3, AfterCommitState.KEEP_OPEN)) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,{\"x\":[1,2]},3,4]"),
            JsonNodeTrx.Commit.NO);
        assertGraph(writer, hash);
        writer.commit();
      }
      assertReopened(path, hash, "[0,{\"x\":[1,2]},3,4]");
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void renamingSharedContainerPathsPreservesSubtreeHashes(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    final String[] inputs = {"[{\"parent\":[{\"child\":1},2]},{\"parent\":[{\"child\":3},4]}]",
        "[{\"parent\":{\"nested\":[1,2]}},{\"parent\":{\"nested\":[3,4]}}]"};
    for (int input = 0; input < inputs.length; input++) {
      final Path path = directory.resolve("rename-" + input);
      try (final var database = create(path, versioning, hash, dewey, true);
          final var session = database.beginResourceSession("resource");
          final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(inputs[input]), JsonNodeTrx.Commit.NO);
        assertGraph(writer, hash);
        writer.commit();
        assertTrue(writer.moveTo(1));
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        writer.setObjectKeyName("renamed");
        assertGraph(writer, hash);
        writer.commit();
      }
      assertReopened(path, hash, inputs[input].replaceFirst("\"parent\"", "\"renamed\""));
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void partialForestFailureCannotPublishUnrepairedState(final VersioningType versioning, final HashType hash,
      final boolean dewey) throws Exception {
    final Path path = directory.resolve("failed-forest");
    try (final var database = create(path, versioning, hash, dewey, true);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      final long oldFrontier = writer.getMaxNodeKey();
      assertThrows(RuntimeException.class,
          () -> writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[1,2,{\"broken\":]"),
              JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES));
      assertTrue(writer.getMaxNodeKey() > oldFrontier, "the failure must follow a partial insert");
      assertThrows(SirixUsageException.class, writer::commit);
      assertEquals(1, session.getMostRecentRevisionNumber());
      writer.rollback();
      assertGraph(writer, hash);
      assertTrue(writer.moveTo(1));
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[7]"), JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      assertGraph(writer, hash);
      writer.commit();
    }
    assertReopened(path, hash, "[0,7]");
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType versioning,
      final HashType hash, final boolean dewey, final boolean diffs) {
    return create(path, versioning, hash, dewey, diffs, false);
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType versioning,
      final HashType hash, final boolean dewey, final boolean diffs, final boolean repair) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(versioning)
                                                 .hashKind(hash)
                                                 .useDeweyIDs(dewey)
                                                 .storeDiffs(diffs)
                                                 .repairBulkInsertHashes(repair)
                                                 .build());
    return database;
  }

  private static void assertReopened(final Path path, final HashType hash, final String expected) throws Exception {
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource")) {
      final int latest = session.getMostRecentRevisionNumber();
      for (int revision = 1; revision <= latest; revision++) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          assertGraph(reader, hash);
        }
      }
      final StringWriter output = new StringWriter();
      new JsonSerializer.Builder(session, output, latest).build().call();
      assertEquals(expected, output.toString());
    }
  }

  static void assertGraph(final JsonNodeReadOnlyTrx reader, final HashType hash) {
    try (final var bytes = Bytes.elasticHeapByteBuffer()) {
      validate(reader, 0, -1, hash, bytes, new LongOpenHashSet());
    }
  }

  private static long validate(final JsonNodeReadOnlyTrx reader, final long key, final long parent, final HashType hash,
      final BytesOut<?> bytes, final LongSet seen) {
    assertTrue(seen.add(key), "cycle or duplicate key " + key);
    final StructNode node = reader.getStorageEngineReader().getRecord(key, IndexType.DOCUMENT, -1);
    assertEquals(parent, node.getParentKey(), "parent at " + key);
    long descendants = 0;
    long children = 0;
    long previous = -1;
    long combinedHash = node.computeHash(bytes);
    long child = node.getFirstChildKey();
    while (child != -1) {
      final StructNode childNode = reader.getStorageEngineReader().getRecord(child, IndexType.DOCUMENT, -1);
      assertEquals(previous, childNode.getLeftSiblingKey(), "left link at " + child);
      descendants += 1 + validate(reader, child, key, hash, bytes, seen);
      combinedHash = hash == HashType.ROLLING
          ? combinedHash + childNode.getHash() * PRIME
          : combinedHash * PRIME + childNode.getHash();
      children++;
      previous = child;
      child = childNode.getRightSiblingKey();
    }
    assertEquals(previous, node.getLastChildKey(), "last child at " + key);
    assertEquals(children, node.getChildCount(), "child count at " + key);
    if (hash != HashType.NONE) {
      assertEquals(descendants, node.getDescendantCount(), "descendant count at " + key);
      assertEquals(combinedHash, node.getHash(), "canonical hash at " + key);
    }
    // NONE does not maintain aggregate hashes/counts; leaf getHash() can still compute
    // a local payload hash lazily. Links and physical child counts remain mandatory.
    return descendants;
  }
}

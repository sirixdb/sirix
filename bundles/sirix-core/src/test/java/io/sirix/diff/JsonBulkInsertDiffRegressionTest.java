package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonResourceCopy;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonBulkInsertDiffRegressionTest {
  @BeforeEach
  @AfterEach
  void cleanUp() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1000, 5})
  void bulkDiffDeclaresCommittedStartingRevision(final int threshold) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (
          final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session);
        try (final var wtx = session.beginNodeTrx(threshold)) {
          assertTrue(wtx.moveTo(array));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[1,2,3,4,5,6,7,8,9,10,11,12]"),
              JsonNodeTrx.Commit.NO);
          final InsertTuple insertedRoot = tuple(wtx);
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[]");
          final int revision = wtx.getRevisionNumber();
          wtx.commit();
          // Read the actual emitted sidecar even when its filename is mislabeled.
          final JsonObject diff;
          try (final var files = Files.list(diffDirectory(session))) {
            final Path sidecar =
                files.filter(path -> path.getFileName().toString().endsWith("toRev" + revision + ".json"))
                     .findFirst()
                     .orElseThrow();
            diff = JsonParser.parseString(Files.readString(sidecar)).getAsJsonObject();
          }
          assertEquals(1, diff.get("old-revision").getAsInt());
          assertEquals(revision, diff.get("new-revision").getAsInt());
          assertTrue(Files.exists(diffDirectory(session).resolve("diffFromRev1toRev" + revision + ".json")));
          assertEquals(1, diff.getAsJsonArray("diffs").size(), "ordinary subtree stays compact");
          assertEquals(Set.of(insertedRoot), insertedTuples(diff));
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsLastChild(42);
          final InsertTuple nextInsert = tuple(wtx);
          wtx.commit();
          assertEquals(Set.of(nextInsert), insertedTuples(readDiff(session, revision, revision + 1)));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_FLUSH"})
  void multipleBulkCallsRetainEarlierTuplesAcrossCommitBoundaries(final AfterCommitState afterCommitState)
      throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session);
        try (final var wtx = session.beginNodeTrx(5, afterCommitState)) {
          final Set<InsertTuple> roots = new HashSet<>();
          for (int call = 0; call < 2; call++) {
            assertTrue(wtx.moveTo(array));
            wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[1,2,3,4,5,6,7,8,9,10,11,12]"),
                JsonNodeTrx.Commit.NO);
            roots.add(tuple(wtx));
          }
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[]");
          final int revision = wtx.getRevisionNumber();
          if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH) {
            assertEquals(2, revision, "storage-only flushes must not publish logical revisions");
          } else {
            assertTrue(revision > 2, "the second call must cross logical commit boundaries");
          }
          wtx.commit();
          assertEquals(2, roots.size());
          assertEquals(roots, insertedTuples(readDiff(session, 1, revision)));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void abortResetsThePendingBulkRevisionBase(final boolean revert) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session);
        try (final var wtx = session.beginNodeTrx(5)) {
          assertTrue(wtx.moveTo(array));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[1,2,3,4,5,6,7,8,9,10,11,12]"),
              JsonNodeTrx.Commit.NO);
          assertTrue(wtx.getRevisionNumber() > 2);
          if (revert) {
            wtx.revertTo(1);
          } else {
            wtx.rollback();
          }
          final int revision = wtx.getRevisionNumber();
          assertTrue(wtx.moveTo(array));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[42]"), JsonNodeTrx.Commit.NO);
          final InsertTuple root = tuple(wtx);
          wtx.commit();
          assertEquals(Set.of(root), insertedTuples(readDiff(session, revision - 1, revision)));
        }
      }
    }
  }

  @Test
  void autoCommittingFirstLoadStillSuppressesSidecars() throws Exception {
    try (final var database =
        JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(false));
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx(5)) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3,4,5,6,7,8,9,10,11,12]"),
          JsonNodeTrx.Commit.NO);
      final long array = wtx.getNodeKey();
      assertTrue(wtx.getRevisionNumber() > 1);
      insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[]");
      wtx.commit();
      try (final var files = Files.list(diffDirectory(session))) {
        assertFalse(files.findAny().isPresent());
      }
      final int revision = wtx.getRevisionNumber();
      assertTrue(wtx.moveTo(array));
      wtx.insertNumberValueAsLastChild(42);
      final InsertTuple insert = tuple(wtx);
      wtx.commit();
      assertEquals(Set.of(insert), insertedTuples(readDiff(session, revision - 1, revision)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "true", "\"text\"", "null", "{\"nested\":2}", "[2]"})
  void gsonSkippedObjectRootHonorsChildPlacement(final String firstValue) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean fusedParent : new boolean[] {false, true}) {
        for (final InsertPosition position : new InsertPosition[] {InsertPosition.AS_FIRST_CHILD,
            InsertPosition.AS_LAST_CHILD}) {
          JsonTestHelper.deleteEverything();
          try (final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
              final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
            final long root = seed(session, fusedParent
                ? "{\"holder\":{\"existing\":0}}"
                : "{\"existing\":0}");
            try (final var wtx = session.beginNodeTrx()) {
              assertTrue(wtx.moveTo(root));
              if (fusedParent) {
                assertTrue(wtx.moveToFirstChild());
              }
              final long object = wtx.getNodeKey();
              final long previousMaxNodeKey = wtx.getMaxNodeKey();
              final String fields = "\"a\":" + firstValue + ",\"b\":2";
              if (position == InsertPosition.AS_LAST_CHILD) {
                wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{" + fields + "}"), JsonNodeTrx.Commit.NO);
              } else {
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{" + fields + "}"), JsonNodeTrx.Commit.NO);
              }
              wtx.commit();
              final List<String> names = new ArrayList<>();
              final Set<InsertTuple> roots = new HashSet<>();
              try (final var rtx = session.beginNodeReadOnlyTrx()) {
                assertTrue(rtx.moveTo(object));
                assertTrue(rtx.moveToFirstChild());
                do {
                  names.add(rtx.getName().getLocalName());
                  if (rtx.getNodeKey() > previousMaxNodeKey) {
                    roots.add(tuple(rtx));
                  }
                } while (rtx.moveToRightSibling());
              }
              assertEquals(position == InsertPosition.AS_LAST_CHILD
                  ? List.of("existing", "a", "b")
                  : List.of("a", "b", "existing"), names);
              final String expectedObject = position == InsertPosition.AS_LAST_CHILD
                  ? "{\"existing\":0," + fields + "}"
                  : "{" + fields + ",\"existing\":0}";
              assertEquals(fusedParent
                  ? "{\"holder\":" + expectedObject + "}"
                  : expectedObject, serialize(session, 2));
              assertEquals(2, roots.size());
              assertEquals(roots, insertedTuples(readDiff(session, 1, 2)));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(InsertPosition.class)
  void insertedSiblingDependenciesReplayAcrossRevisions(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2,3,4,5,6,7,8,9,10,11]");
        try (final var wtx = session.beginNodeTrx()) {
          assertEquals(13, wtx.getMaxNodeKey());
          assertTrue(wtx.moveTo(array));
          if (position == InsertPosition.AS_LEFT_SIBLING || position == InsertPosition.AS_RIGHT_SIBLING) {
            assertTrue(wtx.moveToFirstChild());
          }
          insertSkipped(wtx, position, "[100,101,102]");
          wtx.commit();
          assertReplayableInserts(readDiff(session, 1, 2), 13, 3);
          assertTrue(wtx.moveTo(14));
          wtx.setNumberValue(1000);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_RIGHT_SIBLING"})
  void laterInsertAnchorsCannotBeFixedBySortingKeysAlone(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2,3,4,5,6,7,8,9,10,11]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          if (position == InsertPosition.AS_RIGHT_SIBLING) {
            assertTrue(wtx.moveToFirstChild());
          }
          final long anchor = wtx.getNodeKey();
          for (int value = 100; value <= 102; value++) {
            assertTrue(wtx.moveTo(anchor));
            insertSkipped(wtx, position, "[" + value + "]");
          }
          assertTrue(wtx.moveTo(14));
          assertEquals(15, wtx.getLeftSiblingKey(), "an older insert depends on a later-created sibling");
          wtx.commit();
          assertReplayableInserts(readDiff(session, 1, 2), 13, 3);
          assertTrue(wtx.moveTo(14));
          wtx.setNumberValue(1000);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  private static void assertReplayableInserts(final JsonObject diff, final long previousMaxNodeKey,
      final int insertCount) {
    final Set<Long> available = new HashSet<>();
    for (long nodeKey = 0; nodeKey <= previousMaxNodeKey; nodeKey++) {
      available.add(nodeKey);
    }
    final var operations = diff.getAsJsonArray("diffs");
    assertEquals(insertCount, operations.size());
    for (final var operation : operations) {
      final JsonObject insert = operation.getAsJsonObject().getAsJsonObject("insert");
      assertTrue(available.contains(insert.get("insertPositionNodeKey").getAsLong()),
          "the insertion anchor must already exist when the operation is replayed");
      assertTrue(available.add(insert.get("nodeKey").getAsLong()));
    }
  }

  private static void assertCopiedRevisions(final JsonResourceSession source, final boolean deweyIDs) throws Exception {
    try (final var database =
        JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH2.getFile(), config(deweyIDs));
        final var destination = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = source.beginNodeReadOnlyTrx(1);
        final var wtx = destination.beginNodeTrx()) {
      new JsonResourceCopy.Builder(wtx, rtx, InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent()
                                                                          .build()
                                                                          .call();
      assertEquals(source.getMostRecentRevisionNumber(), destination.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        assertEquals(serialize(source, revision), serialize(destination, revision));
      }
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    try (final var writer = new StringWriter()) {
      JsonSerializer.newBuilder(session, writer, revision).build().call();
      return writer.toString();
    }
  }

  private static JsonObject readDiff(final JsonResourceSession session, final int oldRevision, final int newRevision)
      throws Exception {
    return JsonDiffSidecar.read(diffDirectory(session).resolve("diffFromRev" + oldRevision + "toRev" + newRevision + ".json"),
        JsonTestHelper.RESOURCE, oldRevision, newRevision, session.getResourceConfig().areDeweyIDsStored);
  }

  private static void insertSkipped(final JsonNodeTrx wtx, final InsertPosition position, final String json) {
    final var reader = JsonShredder.createStringReader(json);
    switch (position) {
      case AS_FIRST_CHILD -> wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      case AS_LAST_CHILD -> wtx.insertSubtreeAsLastChild(reader, JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      case AS_LEFT_SIBLING -> wtx.insertSubtreeAsLeftSibling(reader, JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      case AS_RIGHT_SIBLING -> wtx.insertSubtreeAsRightSibling(reader, JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
    }
  }

  @ParameterizedTest
  @EnumSource(InsertPosition.class)
  void skippedRootRecordsEveryInsertedSiblingExactlyOnce(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (
          final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session);
        try (final var wtx = session.beginNodeTrx()) {
          final long previousMaxNodeKey = wtx.getMaxNodeKey();
          assertTrue(wtx.moveTo(array));
          if (position == InsertPosition.AS_LEFT_SIBLING || position == InsertPosition.AS_RIGHT_SIBLING) {
            assertTrue(wtx.moveToFirstChild());
          }
          final var reader = JsonShredder.createStringReader("[[1,2],{\"nested\":[3,4]},5]");
          switch (position) {
            case AS_FIRST_CHILD -> wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO,
                JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
            case AS_LAST_CHILD -> wtx.insertSubtreeAsLastChild(reader, JsonNodeTrx.Commit.NO,
                JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
            case AS_LEFT_SIBLING -> wtx.insertSubtreeAsLeftSibling(reader, JsonNodeTrx.Commit.NO,
                JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
            case AS_RIGHT_SIBLING -> wtx.insertSubtreeAsRightSibling(reader, JsonNodeTrx.Commit.NO,
                JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
          }
          final int revision = wtx.getRevisionNumber();
          wtx.commit();
          final Set<InsertTuple> expected = new HashSet<>();
          try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
            assertTrue(rtx.moveTo(array));
            assertTrue(rtx.moveToFirstChild());
            do {
              if (rtx.getNodeKey() > previousMaxNodeKey) {
                expected.add(tuple(rtx));
              }
            } while (rtx.moveToRightSibling());
          }
          assertEquals(3, expected.size(), "three sibling subtrees must actually be stored");
          final JsonObject diff =
              JsonParser.parseString(Files.readString(diffDirectory(session).resolve("diffFromRev1toRev2.json")))
                        .getAsJsonObject();
          assertEquals(expected, insertedTuples(diff),
              "diff must contain all sibling roots and no descendants or existing nodes");
        }
      }
    }
  }

  private record InsertTuple(long nodeKey, long anchor, String position) {
  }

  private static Set<InsertTuple> insertedTuples(final JsonObject diff) {
    final Set<InsertTuple> tuples = new HashSet<>();
    for (final var entry : diff.getAsJsonArray("diffs")) {
      final JsonObject insert = entry.getAsJsonObject().getAsJsonObject("insert");
      assertTrue(tuples.add(new InsertTuple(insert.get("nodeKey").getAsLong(),
          insert.get("insertPositionNodeKey").getAsLong(), insert.get("insertPosition").getAsString())),
          "each inserted sibling must occur exactly once");
    }
    return tuples;
  }

  private static InsertTuple tuple(final JsonNodeReadOnlyTrx rtx) {
    return new InsertTuple(rtx.getNodeKey(), rtx.hasLeftSibling()
        ? rtx.getLeftSiblingKey()
        : rtx.getParentKey(),
        rtx.hasLeftSibling()
            ? "asRightSibling"
            : "asFirstChild");
  }

  private static ResourceConfiguration config(final boolean deweyIDs) {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .storageType(StorageType.FILE_CHANNEL)
                                .useDeweyIDs(deweyIDs)
                                .build();
  }

  private static Path diffDirectory(final JsonResourceSession session) {
    return session.getResourceConfig()
                  .getResource()
                  .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());
  }

  private static long seed(final JsonResourceSession session) throws Exception {
    return seed(session, "[0,99]");
  }

  private static long seed(final JsonResourceSession session, final String json) throws Exception {
    try (final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      final long array = wtx.getNodeKey();
      wtx.commit();
      try (final var files = Files.list(diffDirectory(session))) {
        assertFalse(files.findAny().isPresent(), "fresh-resource first commit must not emit sidecars");
      }
      return array;
    }
  }
}

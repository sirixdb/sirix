package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.BasicJsonDiff;
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

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void bulkInsertAndRetainedMoveReplayWithStableNodeIdentity(final InsertPosition movePosition) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final InsertPosition bulkPosition : InsertPosition.values()) {
        JsonTestHelper.deleteEverything();
        try (final var database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
            final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
          final long array = seed(session, "[0,1,2]");
          final List<Integer> expected = new ArrayList<>(List.of(0, 1, 2));
          final int movedValue = switch (movePosition) {
            case AS_FIRST_CHILD -> 2;
            case AS_LEFT_SIBLING -> 1;
            case AS_RIGHT_SIBLING -> bulkPosition == InsertPosition.AS_FIRST_CHILD
                || bulkPosition == InsertPosition.AS_LEFT_SIBLING
                    ? 2
                    : 0;
            default -> throw new AssertionError();
          };
          final long movedKey = movedValue + 2;
          try (final var wtx = session.beginNodeTrx()) {
            assertTrue(wtx.moveTo(array));
            if (bulkPosition == InsertPosition.AS_LEFT_SIBLING || bulkPosition == InsertPosition.AS_RIGHT_SIBLING) {
              assertTrue(wtx.moveToFirstChild());
            }
            insertSkipped(wtx, bulkPosition, "[3]");
            switch (bulkPosition) {
              case AS_FIRST_CHILD, AS_LEFT_SIBLING -> expected.add(0, 3);
              case AS_LAST_CHILD -> expected.add(3);
              case AS_RIGHT_SIBLING -> expected.add(1, 3);
            }
            expected.remove(Integer.valueOf(movedValue));
            switch (movePosition) {
              case AS_FIRST_CHILD -> {
                assertTrue(wtx.moveTo(array));
                wtx.moveSubtreeToFirstChild(movedKey);
                expected.add(0, movedValue);
              }
              case AS_LEFT_SIBLING -> {
                assertTrue(wtx.moveTo(5));
                wtx.moveSubtreeToLeftSibling(movedKey);
                expected.add(expected.indexOf(3), movedValue);
              }
              case AS_RIGHT_SIBLING -> {
                assertTrue(wtx.moveTo(5));
                wtx.moveSubtreeToRightSibling(movedKey);
                expected.add(expected.indexOf(3) + 1, movedValue);
              }
              default -> throw new AssertionError();
            }
            wtx.commit();
            assertEquals(JsonParser.parseString(expected.toString()), JsonParser.parseString(serialize(session, 2)));
            try (final var previousRevision = session.beginNodeReadOnlyTrx(1)) {
              assertTrue(JsonDiffSidecar.retainedNodeKeys(readDiff(session, 1, 2).getAsJsonArray("diffs"), previousRevision)
                                      .contains(movedKey));
            }
            assertTrue(wtx.moveTo(5));
            wtx.setNumberValue(30);
            wtx.commit();
            expected.set(expected.indexOf(3), 30);
            assertEquals(JsonParser.parseString(expected.toString()), JsonParser.parseString(serialize(session, 3)));
            assertTrue(wtx.moveTo(movedKey));
            wtx.setNumberValue(100 + movedValue);
            wtx.commit();
            expected.set(expected.indexOf(movedValue), 100 + movedValue);
            assertEquals(JsonParser.parseString(expected.toString()), JsonParser.parseString(serialize(session, 4)));
          }
          assertCopiedRevisions(session, deweyIDs);
        }
      }
    }
  }

  @Test
  void retainedMovesReplayInDependencyOrderWithoutAllocatingKeys() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(3);
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("[2,3,1,0]", serialize(session, 2));
          assertTrue(wtx.moveTo(5));
          wtx.setNumberValue(30);
          wtx.commit();
          assertTrue(wtx.moveTo(2));
          wtx.setNumberValue(100);
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(101);
          wtx.commit();
          assertEquals("[2,30,101,100]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void retainedMoveThatReturnsToItsOriginalPositionKeepsTheReplayCursorOnItsKey() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(2);
          assertTrue(wtx.moveTo(array));
          wtx.moveSubtreeToFirstChild(2);
          wtx.setNumberValue(7);
          wtx.commit();
          assertEquals("[7,1,2,3]", serialize(session, 2));
          assertTrue(wtx.moveTo(5));
          wtx.setNumberValue(30);
          wtx.commit();
          assertEquals("[7,1,2,30]", serialize(session, 3));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void retainedContainerMovePreservesDescendantIdentity() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[[0],1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          assertTrue(wtx.moveTo(6));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("[1,2,3,[0]]", serialize(session, 2));
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(30);
          wtx.commit();
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[1,2,30,[100]]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retainedMoveUsesCurrentWriterAfterAbort(final boolean revert) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          wtx.commit();
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(2);
          if (revert) {
            wtx.revertTo(2);
          } else {
            wtx.rollback();
          }
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("[1,2,3,0]", serialize(session, 3));
          assertTrue(wtx.moveTo(5));
          wtx.setNumberValue(30);
          wtx.commit();
          assertEquals("[1,2,30,0]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void retainedNamedMoveReplaysChangedNameAndInlineValue() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long object = seed(session, "{\"existing\":0,\"other\":1}");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(object));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"added\":3}"), JsonNodeTrx.Commit.NO);
          assertTrue(wtx.moveTo(2));
          wtx.setObjectKeyName("renamed");
          wtx.setNumberValue(7);
          assertTrue(wtx.moveTo(4));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("{\"other\":1,\"added\":3,\"renamed\":7}", serialize(session, 2));
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(30);
          wtx.commit();
          assertTrue(wtx.moveTo(2));
          wtx.setNumberValue(70);
          wtx.commit();
          assertEquals("{\"other\":1,\"added\":30,\"renamed\":70}", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
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
        try (final var sourceRevision = source.beginNodeReadOnlyTrx(revision);
            final var copiedRevision = destination.beginNodeReadOnlyTrx(revision)) {
          assertEquals(sourceRevision.getMaxNodeKey(), copiedRevision.getMaxNodeKey(),
              "a retained move must not allocate new destination keys");
          final var sourceNodes = new DescendantAxis(sourceRevision, IncludeSelf.YES);
          final var copiedNodes = new DescendantAxis(copiedRevision, IncludeSelf.YES);
          while (sourceNodes.hasNext()) {
            assertTrue(copiedNodes.hasNext());
            assertEquals(sourceNodes.nextLong(), copiedNodes.nextLong());
            assertEquals(sourceRevision.getKind(), copiedRevision.getKind());
            assertEquals(sourceRevision.getParentKey(), copiedRevision.getParentKey());
            assertEquals(sourceRevision.getLeftSiblingKey(), copiedRevision.getLeftSiblingKey());
            assertEquals(sourceRevision.getRightSiblingKey(), copiedRevision.getRightSiblingKey());
          }
          assertFalse(copiedNodes.hasNext());
        }
      }
    }
  }

  @Test
  void compactNewRootSkipsRetainedSubtreesBeforeDeletingTheirOldParent() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[{\"field\":[0]},{\"existing\":1}]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[{\"left\":{\"nested\":1},\"right\":2}]");
          assertEquals(10, wtx.getMaxNodeKey());
          assertTrue(wtx.moveTo(10));
          wtx.moveSubtreeToLeftSibling(3);
          assertTrue(wtx.moveTo(2));
          wtx.remove();
          wtx.commit();
          final Set<Long> insertedKeys = new HashSet<>();
          for (final var operation : readDiff(session, 1, 2).getAsJsonArray("diffs")) {
            final JsonObject object = operation.getAsJsonObject();
            if (object.has("insert")) {
              assertTrue(insertedKeys.add(object.getAsJsonObject("insert").get("nodeKey").getAsLong()));
            }
          }
          assertEquals(Set.of(3L, 7L), insertedKeys, "the new subtree remains one compact root tuple");
          assertEquals("[{\"existing\":1},{\"left\":{\"nested\":1},\"field\":[0],\"right\":2}]",
              serialize(session, 2));
          assertTrue(wtx.moveTo(10));
          wtx.setNumberValue(20);
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          wtx.commit();
          assertTrue(wtx.moveTo(9));
          wtx.setNumberValue(30);
          wtx.commit();
          assertEquals("[{\"existing\":1},{\"left\":{\"nested\":30},\"field\":[100],\"right\":20}]",
              serialize(session, 5));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void retainedMoveWaitsForTheMoveOfItsAnchorsAncestor() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[[[0]],1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[2]");
          assertTrue(wtx.moveTo(array));
          wtx.moveSubtreeToFirstChild(3);
          assertTrue(wtx.moveTo(4));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("[[0,[]],1,2]", serialize(session, 2));
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          wtx.commit();
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(20);
          wtx.commit();
          assertEquals("[[100,[]],1,20]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[0]", "{\"value\":0}"})
  void retainedOnlyChildDoesNotAddADestinationDescent(final String retained) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[" + retained + ",1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[[[],9]]");
          assertTrue(wtx.moveTo(6));
          wtx.moveSubtreeToFirstChild(2);
          wtx.commit();
          assertEquals("[1,[[" + retained + "],9]]", serialize(session, 2));
          assertTrue(wtx.moveTo(7));
          wtx.setNumberValue(90);
          wtx.commit();
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[1,[[" + retained.replace("0", "100") + "],90]]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[0]", "{\"value\":0}"})
  void retainedNamedOnlyChildDoesNotAddADestinationDescent(final String retained) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[{\"field\":" + retained + "},1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[{\"holder\":{},\"next\":9}]");
          assertTrue(wtx.moveTo(7));
          wtx.moveSubtreeToFirstChild(3);
          wtx.commit();
          assertEquals("[{},1,{\"holder\":{\"field\":" + retained + "},\"next\":9}]", serialize(session, 2));
          assertTrue(wtx.moveTo(8));
          wtx.setNumberValue(90);
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[{},1,{\"holder\":{\"field\":" + retained.replace("0", "100") + "},\"next\":90}]",
              serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[0]", "{\"value\":0}"})
  void recomputedRetainedFragmentIncludesDescendantEdits(final String retained) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[" + retained + ",1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          assertTrue(wtx.moveTo(6));
          wtx.moveSubtreeToRightSibling(2);
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[1,2,3," + retained.replace("0", "100") + "]", serialize(session, 2));
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(30);
          wtx.commit();
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[1,2,30," + retained.replace("0", "200") + "]", serialize(session, 4));
        }
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void recomputedNewFragmentFindsRetainedRootsAndTheirEdits() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[[0],1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[[[],9]]");
          assertTrue(wtx.moveTo(6));
          wtx.moveSubtreeToFirstChild(2);
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[1,[[[100]],9]]", serialize(session, 2));
          assertTrue(wtx.moveTo(7));
          wtx.setNumberValue(90);
          wtx.commit();
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[1,[[[200]],90]]", serialize(session, 4));
        }
        assertRecomputedRetainsNode(session, 2);
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void recomputedNamedFragmentPreservesNameAndDescendantEdits() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[{\"field\":[0]},1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[{\"holder\":{},\"next\":9}]");
          assertTrue(wtx.moveTo(7));
          wtx.moveSubtreeToFirstChild(3);
          wtx.setObjectKeyName("renamed");
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[{},1,{\"holder\":{\"renamed\":[100]},\"next\":9}]", serialize(session, 2));
          assertTrue(wtx.moveTo(8));
          wtx.setNumberValue(90);
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[{},1,{\"holder\":{\"renamed\":[200]},\"next\":90}]", serialize(session, 4));
        }
        assertRecomputedRetainsNode(session, 3);
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  private static void assertRecomputedRetainsNode(final JsonResourceSession source, final long nodeKey) {
    final String databaseName = source.getResourceConfig().getResource().getParent().getParent().getFileName().toString();
    final JsonObject diff = JsonParser
                                    .parseString(new BasicJsonDiff(databaseName).generateDiff(source, 1, 2, 0, 0, false))
                                    .getAsJsonObject();
    try (final var previousRevision = source.beginNodeReadOnlyTrx(1)) {
      assertTrue(JsonDiffSidecar.retainedNodeKeys(diff.getAsJsonArray("diffs"), previousRevision).contains(nodeKey),
          diff.toString());
    }
  }

  @Test
  void recomputedReplacementMovesRetainedChildrenBeforeRemovingTheirParent() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[[[0],1]]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(2));
          wtx.insertArrayAsLeftSibling();
          assertEquals(6, wtx.getNodeKey());
          wtx.moveSubtreeToFirstChild(3);
          assertTrue(wtx.moveTo(3));
          wtx.insertNumberValueAsRightSibling(9);
          assertEquals(7, wtx.getNodeKey());
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          assertTrue(wtx.moveTo(2));
          wtx.remove();
          wtx.commit();
          assertEquals("[[[100],9]]", serialize(session, 2));
          assertTrue(wtx.moveTo(7));
          wtx.setNumberValue(90);
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[[[200],90]]", serialize(session, 4));
        }
        assertRecomputedRetainsNode(session, 3);
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void recomputedRetainedFragmentIncludesDescendantInsertionsAndRemovals() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[[0,1],2,3]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[4]");
          assertTrue(wtx.moveTo(7));
          wtx.moveSubtreeToRightSibling(2);
          assertTrue(wtx.moveTo(3));
          wtx.remove();
          assertTrue(wtx.moveTo(2));
          wtx.insertNumberValueAsLastChild(100);
          wtx.commit();
          assertEquals("[2,3,4,[1,100]]", serialize(session, 2));
          assertTrue(wtx.moveTo(7));
          wtx.setNumberValue(40);
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(101);
          assertTrue(wtx.moveTo(8));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[2,3,40,[101,200]]", serialize(session, 4));
        }
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void pendingInsertLookupSurvivesMovesOfItsAncestor() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[[],[],[]]");
          assertTrue(wtx.moveTo(3));
          wtx.moveSubtreeToFirstChild(4);
          assertTrue(wtx.moveTo(array));
          wtx.moveSubtreeToFirstChild(3);
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(4);
          wtx.commit();
          assertEquals("[[],0,[],[]]", serialize(session, 2));
          assertEquals(3, readDiff(session, 1, 2).getAsJsonArray("diffs").size());
          assertTrue(wtx.moveTo(4));
          wtx.insertNumberValueAsFirstChild(100);
          wtx.commit();
          assertEquals("[[],0,[],[100]]", serialize(session, 3));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {32, 128})
  void bulkSiblingReorderingKeepsOnePendingInsertPerKey(final int count) throws Exception {
    final List<Integer> input = new ArrayList<>(count);
    for (int value = 0; value < count; value++) {
      input.add(value);
    }
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[-1]");
        final List<Integer> expected = new ArrayList<>(count + 1);
        expected.add(-1);
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, input.toString());
          for (int value = 0; value < count; value++) {
            assertTrue(wtx.moveTo(array));
            wtx.moveSubtreeToFirstChild(value + 3);
            expected.add(0, value);
          }
          wtx.commit();
          assertEquals(JsonParser.parseString(expected.toString()), JsonParser.parseString(serialize(session, 2)));
          assertEquals(count, insertedTuples(readDiff(session, 1, 2)).size());
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(1000);
          wtx.commit();
          expected.set(count - 1, 1000);
          assertEquals(JsonParser.parseString(expected.toString()), JsonParser.parseString(serialize(session, 3)));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void recomputedDiffRetainsMovedKeysAcrossLaterUpdates() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0,1,2]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[3]");
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
          assertEquals("[1,2,3,0]", serialize(session, 2));
          assertTrue(wtx.moveTo(5));
          wtx.setNumberValue(30);
          wtx.commit();
          assertTrue(wtx.moveTo(2));
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals("[1,2,30,100]", serialize(session, 4));
        }
        Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        assertCopiedRevisions(session, deweyIDs);
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

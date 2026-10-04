package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.json.IngestArrayPositionProbe;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
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
import org.junit.jupiter.params.provider.CsvSource;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @ParameterizedTest
  @CsvSource({"reordered,false", "reordered,true", "removed,false", "removed,true", "replaced,false",
      "replaced,true", "reparented,false", "reparented,true", "replaced_root,false", "replaced_root,true",
      "empty,false", "empty,true", "empty_repeat,false", "empty_repeat,true"})
  void initialRevisionCopyPreservesEditedAllocationIdentity(final String scenario, final boolean recompute)
      throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final String initial = switch (scenario) {
          case "reordered" -> "[0,1,2]";
          case "removed" -> "[0,1,2,3]";
          case "replaced" -> "{\"a\":\"old\",\"b\":1}";
          case "reparented" -> "[{\"x\":0},{}]";
          case "replaced_root" -> "[0,1]";
          case "empty", "empty_repeat" -> "[0]";
          default -> throw new AssertionError(scenario);
        };
        final List<String> expected = new ArrayList<>();
        try (final var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initial), JsonNodeTrx.Commit.NO);
          switch (scenario) {
            case "reordered" -> {
              assertTrue(wtx.moveTo(4));
              wtx.moveSubtreeToRightSibling(2);
              assertChildLinks(wtx, 1, 3, 4, 2);
              expected.add("[1,2,0]");
            }
            case "removed" -> {
              assertTrue(wtx.moveTo(3));
              wtx.remove();
              assertTrue(wtx.moveTo(5));
              wtx.remove();
              assertChildLinks(wtx, 1, 2, 4);
              assertEquals(5, wtx.getMaxNodeKey());
              expected.add("[0,2]");
            }
            case "replaced" -> {
              assertTrue(wtx.moveTo(2));
              wtx.replaceObjectRecordValue(new NumberValue(7));
              assertEquals(4, wtx.getNodeKey());
              assertTrue(wtx.moveTo(1));
              wtx.insertObjectRecordAsLastChild("discard", new NumberValue(0));
              assertEquals(5, wtx.getNodeKey());
              wtx.remove();
              assertChildLinks(wtx, 1, 4, 3);
              expected.add("{\"a\":7,\"b\":1}");
            }
            case "reparented" -> {
              assertTrue(wtx.moveTo(4));
              wtx.moveSubtreeToFirstChild(3);
              assertTrue(wtx.moveTo(2));
              wtx.remove();
              assertChildLinks(wtx, 1, 4);
              assertChildLinks(wtx, 4, 3);
              expected.add("[{\"x\":0}]");
            }
            case "replaced_root" -> {
              assertTrue(wtx.moveTo(1));
              wtx.remove();
              wtx.moveToDocumentRoot();
              wtx.insertArrayAsFirstChild();
              assertEquals(4, wtx.getNodeKey());
              wtx.insertNumberValueAsFirstChild(42);
              wtx.insertNumberValueAsRightSibling(0);
              assertEquals(6, wtx.getNodeKey());
              wtx.remove();
              assertChildLinks(wtx, 0, 4);
              assertChildLinks(wtx, 4, 5);
              expected.add("[42]");
            }
            case "empty", "empty_repeat" -> {
              assertTrue(wtx.moveTo(1));
              wtx.remove();
              assertChildLinks(wtx, 0);
              assertEquals(2, wtx.getMaxNodeKey());
              expected.add("");
            }
            default -> throw new AssertionError(scenario);
          }
          wtx.commit();
          try (final var files = Files.list(diffDirectory(session))) {
            assertFalse(files.findAny().isPresent(), "edited first commit must not emit sidecars");
          }
          if (scenario.equals("empty") || scenario.equals("empty_repeat")) {
            if (scenario.equals("empty_repeat")) {
              wtx.moveToDocumentRoot();
              wtx.insertArrayAsFirstChild();
              assertEquals(3, wtx.getNodeKey());
              wtx.remove();
              assertChildLinks(wtx, 0);
              assertEquals(3, wtx.getMaxNodeKey());
              wtx.commit();
              expected.add("");
            }
            wtx.moveToDocumentRoot();
            wtx.insertArrayAsFirstChild();
            final long arrayKey = scenario.equals("empty_repeat") ? 4 : 3;
            assertEquals(arrayKey, wtx.getNodeKey());
            wtx.commit();
            expected.add("[]");
            assertTrue(wtx.moveTo(arrayKey));
            wtx.insertNumberValueAsFirstChild(40);
            assertEquals(arrayKey + 1, wtx.getNodeKey());
            wtx.commit();
            expected.add("[40]");
            wtx.setNumberValue(41);
            wtx.commit();
            expected.add("[41]");
          } else {
            final long updatedKey = switch (scenario) {
              case "reordered" -> 2;
              case "removed", "replaced" -> 4;
              case "reparented" -> 3;
              case "replaced_root" -> 5;
              default -> throw new AssertionError(scenario);
            };
            assertTrue(wtx.moveTo(updatedKey));
            wtx.setNumberValue(100);
            wtx.commit();
            expected.add(switch (scenario) {
              case "reordered" -> "[1,2,100]";
              case "removed" -> "[0,100]";
              case "replaced" -> "{\"a\":100,\"b\":1}";
              case "reparented" -> "[{\"x\":100}]";
              case "replaced_root" -> "[100]";
              default -> throw new AssertionError(scenario);
            });
            final long deletedKey = switch (scenario) {
              case "reordered" -> 4;
              case "removed" -> 2;
              case "replaced", "reparented" -> 3;
              case "replaced_root" -> 5;
              default -> throw new AssertionError(scenario);
            };
            assertTrue(wtx.moveTo(deletedKey));
            wtx.remove();
            wtx.commit();
            expected.add(switch (scenario) {
              case "reordered" -> "[1,100]";
              case "removed" -> "[100]";
              case "replaced_root" -> "[]";
              case "replaced" -> "{\"a\":100}";
              case "reparented" -> "[{}]";
              default -> throw new AssertionError(scenario);
            });
            final long parent = scenario.equals("reparented") || scenario.equals("replaced_root") ? 4 : 1;
            assertTrue(wtx.moveTo(parent));
            if (scenario.equals("replaced") || scenario.equals("reparented")) {
              wtx.insertObjectRecordAsFirstChild("new", new NumberValue(9));
            } else {
              wtx.insertNumberValueAsFirstChild(9);
            }
            final long insertedKey = wtx.getNodeKey();
            assertEquals(switch (scenario) {
              case "reordered", "reparented" -> 5;
              case "removed", "replaced" -> 6;
              case "replaced_root" -> 7;
              default -> throw new AssertionError(scenario);
            }, insertedKey);
            wtx.commit();
            expected.add(switch (scenario) {
              case "reordered" -> "[9,1,100]";
              case "removed" -> "[9,100]";
              case "replaced" -> "{\"new\":9,\"a\":100}";
              case "reparented" -> "[{\"new\":9}]";
              case "replaced_root" -> "[9]";
              default -> throw new AssertionError(scenario);
            });
            if (scenario.equals("reordered") || scenario.equals("removed") || scenario.equals("replaced")) {
              assertTrue(wtx.moveTo(updatedKey));
              wtx.moveSubtreeToRightSibling(insertedKey);
              wtx.commit();
              expected.add(switch (scenario) {
                case "reordered" -> "[1,100,9]";
                case "removed" -> "[100,9]";
                case "replaced" -> "{\"a\":100,\"new\":9}";
                default -> throw new AssertionError(scenario);
              });
            }
          }
        }
        for (int revision = 1; revision <= expected.size(); revision++) {
          assertEquals(expected.get(revision - 1), serialize(session, revision));
        }
        for (int revision = 2; revision <= expected.size(); revision++) {
          assertEquals(revision, readDiff(session, revision - 1, revision).get("new-revision").getAsInt());
          if (recompute) {
            final JsonObject diff = JsonParser.parseString(new BasicJsonDiff(database.getName()).generateDiff(session,
                revision - 1, revision, 0, 0, false)).getAsJsonObject();
            if (revision == 2 && scenario.equals("empty_repeat")) {
              assertEquals(0, diff.getAsJsonArray("diffs").size());
            }
            if (revision == 3 && (scenario.equals("reparented") || scenario.equals("replaced_root"))) {
              assertEquals(Set.of(scenario.equals("reparented") ? 3L : 5L), operationKeys(diff, "delete"));
            }
            if (revision == 5 && (scenario.equals("reordered") || scenario.equals("removed"))) {
              assertFalse(operationKeys(diff, "insert").isEmpty(), "a reorder must emit retained placements");
            }
            Files.delete(diffDirectory(session).resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json"));
          }
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void singleSnapshotCopyKeepsAllocatingDestinationKeys(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var sourceDatabase =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var source = sourceDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
          final var destinationDatabase =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH2.getFile(), config(deweyIDs));
          final var destination = destinationDatabase.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (final var wtx = source.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,1,2]"), JsonNodeTrx.Commit.NO);
          assertTrue(wtx.moveTo(4));
          wtx.moveSubtreeToRightSibling(2);
          wtx.commit();
        }
        seed(destination, "[9,8]");
        try (final var rtx = source.beginNodeReadOnlyTrx(1);
            final var wtx = destination.beginNodeTrx()) {
          assertTrue(rtx.moveTo(1));
          assertTrue(wtx.moveTo(position == InsertPosition.AS_FIRST_CHILD ? 1 : 2));
          new JsonResourceCopy.Builder(wtx, rtx, position).commitAfterwards().build().call();
          assertEquals(position == InsertPosition.AS_RIGHT_SIBLING ? "[9,[1,2,0],8]" : "[[1,2,0],9,8]",
              serialize(destination, 2));
          assertChildLinks(wtx, 4, 5, 6, 7);
          assertTrue(wtx.moveTo(1));
          wtx.insertNumberValueAsLastChild(42);
          assertEquals(8, wtx.getNodeKey());
          wtx.commit();
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void explicitCopyKeyRejectsCollisionsAndKeepsTheAllocationFrontier(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var sourceDatabase =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var source = sourceDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
          final var destinationDatabase =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH2.getFile(), config(deweyIDs));
          final var destination = destinationDatabase.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(source, "[0,1,2]");
        seed(destination, "[9,8]");
        try (final var rtx = source.beginNodeReadOnlyTrx(1);
            final var wtx = destination.beginNodeTrx()) {
          assertTrue(rtx.moveTo(4));
          assertTrue(wtx.moveTo(position == InsertPosition.AS_FIRST_CHILD ? 1 : 2));
          wtx.copyNodeWithKey(rtx, position);
          assertEquals(4, wtx.getNodeKey());
          assertEquals(4, wtx.getMaxNodeKey());
          assertTrue(wtx.moveTo(3));
          assertThrows(IllegalStateException.class, () -> wtx.copyNodeWithKey(rtx, position));
          assertEquals(3, wtx.getNodeKey());
          assertEquals(4, wtx.getMaxNodeKey());
          wtx.insertNumberValueAsRightSibling(42);
          assertEquals(5, wtx.getNodeKey());
          wtx.commit();
          assertEquals(position == InsertPosition.AS_RIGHT_SIBLING ? "[9,2,8,42]" : "[2,9,8,42]",
              serialize(destination, 2));
        }
      }
    }
  }

  private static void assertCopiedRevisions(final JsonResourceSession source, final boolean deweyIDs) throws Exception {
    try (final var database =
        JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH2.getFile(), config(deweyIDs));
        final var destination = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = source.beginNodeReadOnlyTrx(1);
        final var wtx = destination.beginNodeTrx()) {
      wtx.addPreCommitHook(trx -> {
        if (wtx.getRevisionNumber() == 1) {
          assertCopiedStructure(rtx, wtx);
        }
      });
      new JsonResourceCopy.Builder(wtx, rtx, InsertPosition.AS_FIRST_CHILD).copyAllRevisionsUpToMostRecent()
                                                                          .build()
                                                                          .call();
      assertFalse(Files.exists(diffDirectory(destination).resolve("diffFromRev0toRev1.json")));
      assertEquals(source.getMostRecentRevisionNumber(), destination.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        assertEquals(serialize(source, revision), serialize(destination, revision));
        try (final var sourceRevision = source.beginNodeReadOnlyTrx(revision);
            final var copiedRevision = destination.beginNodeReadOnlyTrx(revision)) {
          assertCopiedStructure(sourceRevision, copiedRevision);
        }
      }
    }
  }

  private static void assertCopiedStructure(final JsonNodeReadOnlyTrx source, final JsonNodeReadOnlyTrx copy) {
    final long sourceKey = source.getNodeKey();
    final long copiedKey = copy.getNodeKey();
    source.moveToDocumentRoot();
    copy.moveToDocumentRoot();
    assertEquals(source.getMaxNodeKey(), copy.getMaxNodeKey(), "copy must preserve the source allocation frontier");
    final var sourceNodes = new DescendantAxis(source, IncludeSelf.YES);
    final var copiedNodes = new DescendantAxis(copy, IncludeSelf.YES);
    while (sourceNodes.hasNext()) {
      assertTrue(copiedNodes.hasNext());
      assertEquals(sourceNodes.nextLong(), copiedNodes.nextLong());
      assertEquals(source.getKind(), copy.getKind());
      assertEquals(source.getParentKey(), copy.getParentKey());
      assertEquals(source.getFirstChildKey(), copy.getFirstChildKey());
      assertEquals(source.getLastChildKey(), copy.getLastChildKey());
      assertEquals(source.getLeftSiblingKey(), copy.getLeftSiblingKey());
      assertEquals(source.getRightSiblingKey(), copy.getRightSiblingKey());
      assertEquals(source.getChildCount(), copy.getChildCount());
    }
    assertFalse(copiedNodes.hasNext());
    assertTrue(source.moveTo(sourceKey));
    assertTrue(copy.moveTo(copiedKey));
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
  @ValueSource(booleans = {false, true})
  void nestedNewRootsAllocateEachKeyOnce(final boolean earlierRootIsParent) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean withValues : new boolean[] {false, true}) {
        for (final boolean recompute : new boolean[] {false, true}) {
          JsonTestHelper.deleteEverything();
          try (final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
              final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
            final long array = seed(session, "[0]");
            final long laterRoot = withValues ? 5 : 4;
            final long parent = earlierRootIsParent ? 3 : laterRoot;
            final long child = earlierRootIsParent ? laterRoot : 3;
            final int parentValue = earlierRootIsParent ? 10 : 20;
            final int childValue = earlierRootIsParent ? 20 : 10;
            try (final var wtx = session.beginNodeTrx()) {
              assertTrue(wtx.moveTo(array));
              insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, withValues ? "[[10],[20]]" : "[[],[]]");
              assertTrue(wtx.moveTo(parent));
              wtx.moveSubtreeToFirstChild(child);
              wtx.commit();
              assertEquals(withValues ? "[0,[[" + childValue + "]," + parentValue + "]]" : "[0,[[]]]",
                  serialize(session, 2));
              if (withValues) {
                assertTrue(wtx.moveTo(child + 1));
                wtx.setNumberValue(100);
              } else {
                assertTrue(wtx.moveTo(child));
                wtx.insertNumberValueAsFirstChild(100);
              }
              wtx.commit();
              assertTrue(wtx.moveTo(parent));
              wtx.insertNumberValueAsFirstChild(9);
              wtx.commit();
              assertEquals(withValues ? "[0,[9,[100]," + parentValue + "]]" : "[0,[9,[100]]]",
                  serialize(session, 4));
            }
            if (recompute) {
              Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
            }
            assertCopiedRevisions(session, deweyIDs);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedNamedRootsAllocateEachKeyOnce(final boolean earlierRootIsParent) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final String childValue : new String[] {"{}", "[]"}) {
        for (final boolean recompute : new boolean[] {false, true}) {
          JsonTestHelper.deleteEverything();
          try (final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
              final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
            seed(session, "{\"old\":0}");
            final long parent = earlierRootIsParent ? 3 : 4;
            final long child = earlierRootIsParent ? 4 : 3;
            final String parentName = earlierRootIsParent ? "a" : "b";
            final String childName = earlierRootIsParent ? "b" : "a";
            try (final var wtx = session.beginNodeTrx()) {
              assertTrue(wtx.moveTo(1));
              insertSkipped(wtx, InsertPosition.AS_LAST_CHILD,
                  earlierRootIsParent ? "{\"a\":{},\"b\":" + childValue + "}"
                      : "{\"a\":" + childValue + ",\"b\":{}}");
              assertTrue(wtx.moveTo(parent));
              wtx.moveSubtreeToFirstChild(child);
              wtx.commit();
              assertEquals(JsonParser.parseString("{\"old\":0,\"" + parentName + "\":{\"" + childName
                  + "\":" + childValue + "}}"), JsonParser.parseString(serialize(session, 2)));
              assertTrue(wtx.moveTo(child));
              wtx.replaceObjectRecordValue(new NumberValue(100));
              assertEquals(5, wtx.getNodeKey());
              wtx.commit();
              assertTrue(wtx.moveTo(5));
              wtx.setNumberValue(200);
              assertTrue(wtx.moveTo(2));
              wtx.setNumberValue(10);
              wtx.commit();
              assertEquals(JsonParser.parseString("{\"old\":10,\"" + parentName + "\":{\"" + childName
                  + "\":200}}"), JsonParser.parseString(serialize(session, 4)));
            }
            if (recompute) {
              Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
            }
            assertCopiedRevisions(session, deweyIDs);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replacementPreservesMovedChildrenAndAllocationIdentity(final boolean recompute) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "{\"a\":{\"x\":0},\"b\":{}}");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(1));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "{\"c\":{}}");
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToFirstChild(3);
          assertTrue(wtx.moveTo(2));
          wtx.replaceObjectRecordValue(new NumberValue(1));
          assertEquals(6, wtx.getNodeKey());
          wtx.commit();
          assertEquals(JsonParser.parseString("{\"a\":1,\"b\":{},\"c\":{\"x\":0}}"),
              JsonParser.parseString(serialize(session, 2)));
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(10);
          wtx.commit();
          assertEquals(JsonParser.parseString("{\"a\":10,\"b\":{},\"c\":{\"x\":100}}"),
              JsonParser.parseString(serialize(session, 4)));
        }
        readDiff(session, 1, 2);
        if (recompute) {
          Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void compactFragmentsMergeInterleavedRootAllocations() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[[],10]"), JsonNodeTrx.Commit.NO);
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[[20]]");
          assertTrue(wtx.moveTo(6));
          wtx.moveSubtreeToFirstChild(4);
          wtx.commit();
          assertEquals("[0,[10],[[],20]]", serialize(session, 2));
          assertTrue(wtx.moveTo(5));
          wtx.setNumberValue(100);
          assertTrue(wtx.moveTo(7));
          wtx.setNumberValue(200);
          wtx.commit();
          assertEquals("[0,[100],[[],200]]", serialize(session, 3));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void replayPreservesUnusedAllocationKeysWithoutCreatingNodes() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsLastChild(10);
          wtx.remove();
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsLastChild(20);
          assertEquals(4, wtx.getNodeKey());
          wtx.commit();
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsLastChild(30);
          assertEquals(5, wtx.getNodeKey());
          wtx.remove();
          wtx.commit();
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsLastChild(200);
          assertEquals(6, wtx.getNodeKey());
          wtx.commit();
          assertEquals("[0,100,200]", serialize(session, 4));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void documentRootReplacementProtectsRetainedChildren(final boolean recompute) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[[0],1]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(1));
          wtx.insertArrayAsFirstChild();
          assertEquals(5, wtx.getNodeKey());
          wtx.moveToDocumentRoot();
          wtx.moveSubtreeToFirstChild(5);
          assertTrue(wtx.moveTo(5));
          wtx.moveSubtreeToFirstChild(2);
          assertTrue(wtx.moveTo(1));
          wtx.remove();
          wtx.commit();
          assertEquals("[[0]]", serialize(session, 2));
          assertTrue(wtx.moveTo(3));
          wtx.setNumberValue(100);
          wtx.commit();
          assertTrue(wtx.moveTo(5));
          wtx.insertNumberValueAsFirstChild(200);
          wtx.commit();
          assertEquals("[200,[100]]", serialize(session, 4));
        }
        if (recompute) {
          Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {16, 64})
  void deeplyNestedNewRootsPreserveAllocationIdentity(final int count) throws Exception {
    final List<String> roots = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      roots.add("[]");
    }
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, roots.toString());
          for (int index = 3; index < count + 2; index++) {
            assertTrue(wtx.moveTo(index + 1));
            wtx.moveSubtreeToFirstChild(index);
          }
          wtx.commit();
          final String nested = "[".repeat(count) + "]".repeat(count);
          assertEquals("[0," + nested + "]", serialize(session, 2));
          assertTrue(wtx.moveTo(3));
          wtx.insertNumberValueAsFirstChild(100);
          assertEquals(count + 3, wtx.getNodeKey());
          wtx.commit();
          assertEquals("[0," + "[".repeat(count) + "100" + "]".repeat(count) + "]", serialize(session, 3));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void movingOnlyChildSupportsLastChildAppendAndRemovalOfItsOldParent() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[[[0]]]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(2));
          wtx.insertArrayAsLeftSibling();
          assertEquals(5, wtx.getNodeKey());
          wtx.moveSubtreeToFirstChild(3);
          assertChildLinks(wtx, 2);
          assertChildLinks(wtx, 5, 3);
          assertTrue(wtx.moveTo(5));
          wtx.insertNumberValueAsLastChild(9);
          assertEquals(6, wtx.getNodeKey());
          assertChildLinks(wtx, 5, 3, 6);
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          assertTrue(wtx.moveTo(2));
          wtx.remove();
          wtx.commit();
          assertEquals("[[[100],9]]", serialize(session, 2));
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(90);
          wtx.commit();
          assertEquals("[[[100],90]]", serialize(session, 3));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[0,1,2]", "{\"a\":0,\"b\":1,\"c\":2}",
      "{\"container\":[0,1,2]}", "{\"container\":{\"a\":0,\"b\":1,\"c\":2}}"})
  void movesMaintainBothEndsOfOrdinaryAndFusedChildChains(final String json) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, json);
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveToFirstChild());
          if (wtx.getChildCount() == 1) {
            assertTrue(wtx.moveToFirstChild());
          }
          final long parent = wtx.getNodeKey();
          final long first = wtx.getFirstChildKey();
          final boolean array = wtx.isArray();
          assertChildLinks(wtx, parent, first, first + 1, first + 2);
          assertTrue(wtx.moveTo(first + 2));
          wtx.moveSubtreeToRightSibling(first);
          assertChildLinks(wtx, parent, first + 1, first + 2, first);
          assertTrue(wtx.moveTo(parent));
          wtx.moveSubtreeToFirstChild(first + 2);
          assertChildLinks(wtx, parent, first + 2, first + 1, first);
          assertTrue(wtx.moveTo(first + 2));
          wtx.moveSubtreeToLeftSibling(first);
          assertChildLinks(wtx, parent, first, first + 2, first + 1);
          wtx.commit();
          assertTrue(wtx.moveTo(parent));
          if (array) {
            wtx.insertNumberValueAsLastChild(9);
          } else {
            wtx.insertObjectRecordAsLastChild("d", new NumberValue(9));
          }
          final long appended = wtx.getNodeKey();
          assertChildLinks(wtx, parent, first, first + 2, first + 1, appended);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  private static void assertChildLinks(final JsonNodeTrx wtx, final long parent, final long... children) {
    assertTrue(wtx.moveTo(parent));
    assertEquals(children.length, wtx.getChildCount());
    assertEquals(children.length == 0 ? -1 : children[0], wtx.getFirstChildKey());
    assertEquals(children.length == 0 ? -1 : children[children.length - 1], wtx.getLastChildKey());
    for (int index = 0; index < children.length; index++) {
      assertTrue(wtx.moveTo(children[index]));
      assertEquals(parent, wtx.getParentKey());
      assertEquals(index == 0 ? -1 : children[index - 1], wtx.getLeftSiblingKey());
      assertEquals(index + 1 == children.length ? -1 : children[index + 1], wtx.getRightSiblingKey());
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class,
      names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_FLUSH", "KEEP_OPEN_ASYNC_COMMIT"})
  void disabledDiffsDoNotAccumulatePendingOperations(final AfterCommitState afterCommitState) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      final ResourceConfiguration configuration = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
          .storageType(StorageType.FILE_CHANNEL)
          .useDeweyIDs(deweyIDs)
          .storeDiffs(false)
          .build();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), configuration);
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array = seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx(5, afterCommitState)) {
          for (int batch = 0; batch < 4; batch++) {
            for (int value = 0; value < 16; value++) {
              assertTrue(wtx.moveTo(array));
              wtx.insertNumberValueAsLastChild(batch * 16 + value + 1);
              assertTrue(pendingOperations(wtx).isEmpty());
            }
            wtx.commit();
            wtx.awaitPendingAsyncCommit();
            assertTrue(pendingOperations(wtx).isEmpty());
            assertTrue(wtx.moveTo(array));
            assertEquals((batch + 1) * 16 + 1, wtx.getChildCount());
          }
          assertTrue(wtx.moveTo(array));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[65,66,67,68,69,70,71,72]");
          assertTrue(pendingOperations(wtx).isEmpty());
          wtx.commit();
          wtx.awaitPendingAsyncCommit();
          assertTrue(pendingOperations(wtx).isEmpty());
          assertTrue(wtx.moveTo(array));
          assertEquals(73, wtx.getChildCount());
        }
        try (final var files = Files.list(diffDirectory(session))) {
          assertEquals(0, files.count());
        }
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

  @ParameterizedTest
  @EnumSource(InsertPosition.class)
  void vacatedDeweyPositionsRetainDeletesAndBulkInserts(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final boolean sibling = position == InsertPosition.AS_LEFT_SIBLING
            || position == InsertPosition.AS_RIGHT_SIBLING;
        seed(session, sibling ? "[0,99]" : "[0]");
        final long removed = position == InsertPosition.AS_RIGHT_SIBLING ? 3 : 2;
        final long first = sibling ? 4 : 3;
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(removed));
          wtx.remove();
          final long anchor = position == InsertPosition.AS_LEFT_SIBLING ? 3
              : position == InsertPosition.AS_RIGHT_SIBLING ? 2 : 1;
          assertTrue(wtx.moveTo(anchor));
          insertSkipped(wtx, position, "[1,2]");
          final Set<PendingOperation> expected = Set.of(new PendingOperation(DiffType.DELETED, 0, removed),
              new PendingOperation(DiffType.INSERTED, first, 0),
              new PendingOperation(DiffType.INSERTED, first + 1, 0));
          assertEquals(expected, pendingOperations(wtx));
          wtx.commit();
          final JsonObject diff = readDiff(session, 1, 2);
          assertEquals(Set.of(removed), operationKeys(diff, "delete"));
          assertEquals(Set.of(first, first + 1), operationKeys(diff, "insert"));
          final String expectedContent = position == InsertPosition.AS_LEFT_SIBLING ? "[2,1,99]"
              : position == InsertPosition.AS_RIGHT_SIBLING ? "[0,1,2]" : "[1,2]";
          assertEquals(expectedContent, serialize(session, 2));
          assertTrue(wtx.moveTo(first));
          wtx.setNumberValue(10);
          wtx.setNumberValue(100);
          wtx.commit();
          assertEquals(Set.of(first), operationKeys(readDiff(session, 2, 3), "update"));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void ancestorRekeyingPreservesDescendantOperationsAtVacatedPositions(final InsertPosition movePosition)
      throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[99,[0],98]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(3));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[1,2]");
          assertTrue(wtx.moveTo(4));
          wtx.setNumberValue(100);
          wtx.setNumberValue(200);
          switch (movePosition) {
            case AS_FIRST_CHILD -> {
              assertTrue(wtx.moveTo(1));
              wtx.moveSubtreeToFirstChild(3);
            }
            case AS_LEFT_SIBLING -> {
              assertTrue(wtx.moveTo(2));
              wtx.moveSubtreeToLeftSibling(3);
            }
            case AS_RIGHT_SIBLING -> {
              assertTrue(wtx.moveTo(5));
              wtx.moveSubtreeToRightSibling(3);
            }
            default -> throw new AssertionError(movePosition);
          }
          assertTrue(wtx.moveTo(2));
          wtx.insertArrayAsRightSibling();
          assertEquals(8, wtx.getNodeKey());
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[10,11,12]");
          assertEquals(Set.of(new PendingOperation(DiffType.INSERTED, 3, 0),
              new PendingOperation(DiffType.DELETED, 0, 3), new PendingOperation(DiffType.UPDATED, 4, 4),
              new PendingOperation(DiffType.INSERTED, 6, 0), new PendingOperation(DiffType.INSERTED, 7, 0),
              new PendingOperation(DiffType.INSERTED, 8, 0), new PendingOperation(DiffType.INSERTED, 9, 0),
              new PendingOperation(DiffType.INSERTED, 10, 0), new PendingOperation(DiffType.INSERTED, 11, 0)),
              pendingOperations(wtx));
          wtx.commit();
          assertEquals(movePosition == InsertPosition.AS_RIGHT_SIBLING
              ? "[99,[10,11,12],98,[200,1,2]]" : "[[200,1,2],99,[10,11,12],98]", serialize(session, 2));
          final JsonObject diff = readDiff(session, 1, 2);
          assertEquals(Set.of(3L, 6L, 7L, 8L, 9L, 10L, 11L), operationKeys(diff, "insert"));
          assertEquals(Set.of(3L), operationKeys(diff, "delete"));
          assertEquals(Set.of(4L), operationKeys(diff, "update"));
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(1000);
          assertTrue(wtx.moveTo(10));
          wtx.remove();
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"replace_twice", "replace_then_move", "replace_then_delete", "new_then_replace",
      "move_then_replace"})
  void replacementAccumulationPreservesOriginalIdentity(final String action) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "{\"old\":{},\"keep\":1}");
        try (final var wtx = session.beginNodeTrx()) {
          if (action.equals("new_then_replace")) {
            assertTrue(wtx.moveTo(1));
            wtx.insertObjectRecordAsLastChild("new", new StringValue("new"));
          } else {
            assertTrue(wtx.moveTo(2));
          }
          if (action.equals("move_then_replace")) {
            assertTrue(wtx.moveTo(3));
            wtx.moveSubtreeToRightSibling(2);
            assertTrue(wtx.moveTo(2));
          }
          wtx.replaceObjectRecordValue(new NumberValue(10));
          final long replaced = wtx.getNodeKey();
          if (action.equals("replace_twice")) {
            wtx.replaceObjectRecordValue(new StringValue("twice"));
            wtx.setStringValue("updated");
            wtx.setStringValue("final");
          } else if (action.equals("replace_then_move")) {
            assertTrue(wtx.moveTo(3));
            wtx.moveSubtreeToRightSibling(replaced);
            assertTrue(wtx.moveTo(replaced));
            wtx.setNumberValue(100);
          } else if (action.equals("replace_then_delete")) {
            wtx.remove();
          }
          wtx.commit();
          final String expected = switch (action) {
            case "replace_twice" -> "{\"old\":\"final\",\"keep\":1}";
            case "replace_then_move" -> "{\"keep\":1,\"old\":100}";
            case "replace_then_delete" -> "{\"keep\":1}";
            case "new_then_replace" -> "{\"old\":{},\"keep\":1,\"new\":10}";
            case "move_then_replace" -> "{\"keep\":1,\"old\":10}";
            default -> throw new AssertionError(action);
          };
          assertEquals(expected, serialize(session, 2));
          final JsonObject diff = readDiff(session, 1, 2);
          if (action.equals("replace_then_delete")) {
            assertEquals(Set.of(2L), operationKeys(diff, "delete"));
          }
          if (!action.equals("new_then_replace")) {
            assertTrue(wtx.moveTo(1));
            wtx.insertObjectRecordAsLastChild("next", new NumberValue(20));
            wtx.commit();
            wtx.setNumberValue(200);
            wtx.commit();
          }
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class,
      names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_FLUSH", "KEEP_OPEN_ASYNC_COMMIT"})
  void collisionFreePendingDiffsSurviveBulkCommitBoundaries(final AfterCommitState state) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx(5, state)) {
          assertTrue(wtx.moveTo(2));
          wtx.remove();
          assertTrue(wtx.moveTo(1));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[1,2,3,4,5,6,7,8,9,10,11,12]");
          assertTrue(wtx.moveTo(1));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[]");
          assertEquals(13, pendingOperations(wtx).size());
          assertTrue(pendingOperations(wtx).contains(new PendingOperation(DiffType.DELETED, 0, 2)));
          final int revision = wtx.getRevisionNumber();
          wtx.commit();
          wtx.awaitPendingAsyncCommit();
          assertTrue(pendingOperations(wtx).isEmpty());
          final JsonObject diff = readDiff(session, 1, revision);
          assertEquals(Set.of(2L), operationKeys(diff, "delete"));
          assertEquals(12, operationKeys(diff, "insert").size());
          assertEquals("[1,2,3,4,5,6,7,8,9,10,11,12]", serialize(session, revision));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void compactDescendantReplacementSurvivesMovingOutsideItsRoot(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean recompute : new boolean[] {false, true}) {
        JsonTestHelper.deleteEverything();
        try (final var database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
            final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
          seed(session, "{\"holder\":{},\"keep\":1}");
          try (final var wtx = session.beginNodeTrx()) {
            assertTrue(wtx.moveTo(1));
            wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"a\":{\"x\":\"v\"}}"),
                JsonNodeTrx.Commit.NO);
            assertEquals(Set.of(new PendingOperation(DiffType.INSERTED, 4, 0)), pendingOperations(wtx));
            assertTrue(wtx.moveTo(5));
            wtx.replaceObjectRecordValue(new NumberValue(7));
            assertEquals(6, wtx.getNodeKey());
            assertTrue(wtx.moveTo(2));
            wtx.insertObjectRecordAsFirstChild("before", new NumberValue(0));
            wtx.insertObjectRecordAsRightSibling("after", new NumberValue(1));
            switch (position) {
              case AS_FIRST_CHILD -> {
                assertTrue(wtx.moveTo(2));
                wtx.moveSubtreeToFirstChild(6);
              }
              case AS_LEFT_SIBLING -> {
                assertTrue(wtx.moveTo(8));
                wtx.moveSubtreeToLeftSibling(6);
              }
              case AS_RIGHT_SIBLING -> {
                assertTrue(wtx.moveTo(7));
                wtx.moveSubtreeToRightSibling(6);
              }
              default -> throw new AssertionError(position);
            }
            assertChildLinks(wtx, 1, 2, 3, 4);
            if (position == InsertPosition.AS_FIRST_CHILD) {
              assertChildLinks(wtx, 2, 6, 7, 8);
            } else {
              assertChildLinks(wtx, 2, 7, 6, 8);
            }
            assertChildLinks(wtx, 4);
            final Set<PendingOperation> pending = pendingOperations(wtx);
            wtx.commit();
            final JsonObject diff = readDiff(session, 1, 2);
            assertEquals(Set.of(4L, 6L, 7L, 8L), operationKeys(diff, "insert"));
            assertTrue(pending.contains(new PendingOperation(DiffType.INSERTED, 6, 0)));
            assertEquals(Set.of(), operationKeys(diff, "delete"));
            assertEquals(JsonParser.parseString("{\"holder\":{\"before\":0,\"x\":7,\"after\":1},"
                + "\"keep\":1,\"a\":{}}"), JsonParser.parseString(serialize(session, 2)));
            assertTrue(wtx.moveTo(6));
            wtx.setNumberValue(70);
            wtx.commit();
            assertTrue(wtx.moveTo(7));
            wtx.setNumberValue(10);
            wtx.commit();
          }
          if (recompute) {
            Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
          }
          assertCopiedRevisions(session, deweyIDs);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class,
      names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_FLUSH", "KEEP_OPEN_ASYNC_COMMIT"})
  void compactDescendantReplacementUsesTheEarliestBulkRevision(final AfterCommitState state) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "{\"holder\":{},\"keep\":1}");
        try (final var wtx = session.beginNodeTrx(5, state)) {
          assertTrue(wtx.moveTo(1));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(
              "{\"a\":{\"x\":\"v\",\"padding\":[0,1,2,3,4,5,6]}}"), JsonNodeTrx.Commit.NO);
          final int revision = wtx.getRevisionNumber();
          assertTrue(wtx.moveTo(5));
          wtx.replaceObjectRecordValue(new NumberValue(7));
          assertEquals(14, wtx.getNodeKey());
          assertEquals(revision, wtx.getRevisionNumber());
          assertTrue(wtx.moveTo(2));
          wtx.moveSubtreeToFirstChild(14);
          wtx.commit();
          wtx.awaitPendingAsyncCommit();
          final JsonObject diff = readDiff(session, 1, revision);
          assertEquals(Set.of(4L, 14L), operationKeys(diff, "insert"));
          assertEquals(JsonParser.parseString("{\"holder\":{\"x\":7},\"keep\":1,"
              + "\"a\":{\"padding\":[0,1,2,3,4,5,6]}}"), JsonParser.parseString(serialize(session, revision)));
          assertTrue(wtx.moveTo(14));
          wtx.setNumberValue(70);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compactDescendantReplacementChainRemainsAnInsertion(final boolean recompute) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "{\"holder\":{},\"keep\":1}");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(1));
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"a\":{\"x\":\"v\"}}"),
              JsonNodeTrx.Commit.NO);
          assertTrue(wtx.moveTo(5));
          wtx.replaceObjectRecordValue(new NumberValue(7));
          wtx.replaceObjectRecordValue(new StringValue("again"));
          wtx.replaceObjectRecordValue(new NumberValue(7));
          assertEquals(8, wtx.getNodeKey());
          assertTrue(wtx.moveTo(2));
          wtx.moveSubtreeToFirstChild(8);
          assertChildLinks(wtx, 2, 8);
          assertChildLinks(wtx, 4);
          wtx.commit();
          assertEquals(Set.of(4L, 8L), operationKeys(readDiff(session, 1, 2), "insert"));
          assertEquals("{\"holder\":{\"x\":7},\"keep\":1,\"a\":{}}", serialize(session, 2));
          assertTrue(wtx.moveTo(8));
          wtx.setNumberValue(70);
          wtx.commit();
        }
        if (recompute) {
          Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"delete_then_replace", "replacement_under_deleted_ancestor", "retained_descendant"})
  void replacementDeletesCoalesceWithoutLosingSurvivingInserts(final String action) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean recompute : new boolean[] {false, true}) {
        JsonTestHelper.deleteEverything();
        try (final var database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
            final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
          final boolean retained = action.equals("retained_descendant");
          seed(session, switch (action) {
            case "delete_then_replace" -> "{\"a\":{\"x\":0},\"b\":1}";
            case "replacement_under_deleted_ancestor" -> "{\"a\":{\"x\":{}},\"b\":1}";
            case "retained_descendant" -> "{\"a\":{\"x\":{\"kept\":0,\"removed\":1}},\"b\":{}}";
            default -> throw new AssertionError(action);
          });
          final long replacementKey = retained ? 7 : 5;
          try (final var wtx = session.beginNodeTrx()) {
            if (action.equals("replacement_under_deleted_ancestor")) {
              assertTrue(wtx.moveTo(3));
              wtx.replaceObjectRecordValue(new NumberValue(7));
              assertTrue(wtx.moveTo(4));
              wtx.moveSubtreeToRightSibling(5);
              assertTrue(wtx.moveTo(2));
              wtx.remove();
              assertChildLinks(wtx, 1, 4, 5);
            } else {
              assertTrue(wtx.moveTo(retained ? 5 : 3));
              wtx.remove();
              if (retained) {
                assertTrue(wtx.moveTo(6));
                wtx.moveSubtreeToFirstChild(3);
              }
              assertTrue(wtx.moveTo(2));
              wtx.replaceObjectRecordValue(new NumberValue(7));
              assertEquals(replacementKey, wtx.getNodeKey());
              assertChildLinks(wtx, 1, replacementKey, retained ? 6 : 4);
              if (retained) {
                assertChildLinks(wtx, 6, 3);
                assertChildLinks(wtx, 3, 4);
              }
            }
            wtx.commit();
            final String expected = switch (action) {
              case "delete_then_replace" -> "{\"a\":7,\"b\":1}";
              case "replacement_under_deleted_ancestor" -> "{\"b\":1,\"x\":7}";
              case "retained_descendant" -> "{\"a\":7,\"b\":{\"x\":{\"kept\":0}}}";
              default -> throw new AssertionError(action);
            };
            assertEquals(expected, serialize(session, 2));
            final JsonObject diff = readDiff(session, 1, 2);
            assertNormalizedOperations(session, diff, retained ? Set.of(2L, 5L) : Set.of(2L),
                retained ? Set.of(3L, 7L) : Set.of(5L));
            assertTrue(wtx.moveTo(replacementKey));
            wtx.setNumberValue(70);
            wtx.commit();
            if (retained) {
              assertTrue(wtx.moveTo(4));
              wtx.setNumberValue(100);
              wtx.commit();
            }
          }
          if (recompute) {
            Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
          }
          assertCopiedRevisions(session, deweyIDs);
        }
      }
    }
  }

  private static void assertNormalizedOperations(final JsonResourceSession session, final JsonObject diff,
      final Set<Long> deletes, final Set<Long> inserts) {
    try (final var previousRevision = session.beginNodeReadOnlyTrx(1);
        final var newRevision = session.beginNodeReadOnlyTrx(2)) {
      final JsonArray forward = diff.getAsJsonArray("diffs");
      final JsonArray reverse = new JsonArray(forward.size());
      final JsonArray repeatedDeletes = new JsonArray(forward.size());
      for (int index = forward.size() - 1; index >= 0; index--) {
        reverse.add(forward.get(index));
      }
      for (final var operation : forward) {
        repeatedDeletes.add(operation);
        if (operation.getAsJsonObject().has("delete")) {
          repeatedDeletes.add(operation);
        }
      }
      for (final JsonArray operations : new JsonArray[] {forward, reverse, repeatedDeletes}) {
        final JsonObject normalized = new JsonObject();
        normalized.add("diffs", JsonDiffSidecar.normalizeReplacements(operations, previousRevision, newRevision));
        assertEquals(deletes, operationKeys(normalized, "delete"));
        assertEquals(inserts, operationKeys(normalized, "insert"));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void removingAMovedChildsParentRetainsOnlyIndependentDeletes(final boolean differentParent) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, differentParent ? "[[0],[1],9]" : "[[0,1],9]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(4));
          if (differentParent) {
            wtx.moveSubtreeToFirstChild(3);
          } else {
            wtx.moveSubtreeToRightSibling(3);
          }
          assertTrue(wtx.moveTo(differentParent ? 4 : 2));
          wtx.remove();
          wtx.commit();
          assertEquals(differentParent ? "[[],9]" : "[9]", serialize(session, 2));
          assertEquals(differentParent ? Set.of(3L, 4L) : Set.of(2L),
              operationKeys(readDiff(session, 1, 2), "delete"));
          assertTrue(wtx.moveTo(differentParent ? 6 : 5));
          wtx.setNumberValue(90);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @Test
  void removingAReplacementInsideAnotherParentPreservesItsOriginalDeletion() throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[{\"a\":{}},{\"b\":{}},9]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(3));
          wtx.replaceObjectRecordValue(new NumberValue(10));
          assertEquals(7, wtx.getNodeKey());
          assertTrue(wtx.moveTo(4));
          wtx.moveSubtreeToFirstChild(7);
          assertTrue(wtx.moveTo(4));
          wtx.remove();
          wtx.commit();
          assertEquals("[{},9]", serialize(session, 2));
          assertEquals(Set.of(3L, 4L), operationKeys(readDiff(session, 1, 2), "delete"));
          assertTrue(wtx.moveTo(6));
          wtx.setNumberValue(90);
          wtx.commit();
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = InsertPosition.class, names = {"AS_FIRST_CHILD", "AS_LEFT_SIBLING", "AS_RIGHT_SIBLING"})
  void deletingInsideARetainedSubtreePreservesIndependentDeletes(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean recompute : new boolean[] {false, true}) {
        JsonTestHelper.deleteEverything();
        try (final var database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
            final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
          seed(session, "[[[0,1]],9]");
          try (final var wtx = session.beginNodeTrx()) {
            switch (position) {
              case AS_FIRST_CHILD -> {
                assertTrue(wtx.moveTo(1));
                wtx.moveSubtreeToFirstChild(3);
              }
              case AS_LEFT_SIBLING -> {
                assertTrue(wtx.moveTo(2));
                wtx.moveSubtreeToLeftSibling(3);
              }
              case AS_RIGHT_SIBLING -> {
                assertTrue(wtx.moveTo(6));
                wtx.moveSubtreeToRightSibling(3);
              }
              default -> throw new AssertionError(position);
            }
            assertTrue(wtx.moveTo(4));
            wtx.remove();
            assertTrue(wtx.moveTo(2));
            wtx.remove();
            assertChildLinks(wtx, 1, position == InsertPosition.AS_RIGHT_SIBLING ? 6 : 3,
                position == InsertPosition.AS_RIGHT_SIBLING ? 3 : 6);
            assertChildLinks(wtx, 3, 5);
            wtx.commit();
            assertEquals(position == InsertPosition.AS_RIGHT_SIBLING ? "[9,[1]]" : "[[1],9]",
                serialize(session, 2));
            assertEquals(Set.of(2L, 4L), operationKeys(readDiff(session, 1, 2), "delete"));
            assertEquals(Set.of(3L), operationKeys(readDiff(session, 1, 2), "insert"));
            assertTrue(wtx.moveTo(5));
            wtx.setNumberValue(100);
            wtx.commit();
          }
          if (recompute) {
            Files.delete(diffDirectory(session).resolve("diffFromRev1toRev2.json"));
            final JsonObject diff = JsonParser.parseString(
                new BasicJsonDiff(database.getName()).generateDiff(session, 1, 2, 0, 0, false)).getAsJsonObject();
            assertEquals(Set.of(2L, 4L), operationKeys(diff, "delete"));
          }
          assertCopiedRevisions(session, deweyIDs);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void abortReleasesIdentityAccumulationBeforeVacatedPositionsAreReused(final boolean revert) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      try (final var database =
          JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config(deweyIDs));
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        seed(session, "[0]");
        try (final var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(2));
          wtx.remove();
          assertTrue(wtx.moveTo(1));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[1,2]");
          if (revert) {
            wtx.revertTo(1);
          } else {
            wtx.rollback();
          }
          assertTrue(pendingOperations(wtx).isEmpty());
          assertTrue(wtx.moveTo(1));
          insertSkipped(wtx, InsertPosition.AS_LAST_CHILD, "[10,20]");
          wtx.commit();
          assertEquals("[0,10,20]", serialize(session, 2));
          assertEquals(Set.of(), operationKeys(readDiff(session, 1, 2), "delete"));
          assertEquals(Set.of(3L, 4L), operationKeys(readDiff(session, 1, 2), "insert"));
        }
        assertCopiedRevisions(session, deweyIDs);
      }
    }
  }

  private record PendingOperation(DiffType type, long newKey, long oldKey) {}

  private static Set<PendingOperation> pendingOperations(final JsonNodeTrx wtx) {
    final var tuples = IngestArrayPositionProbe.pendingDiffs(wtx);
    final Set<PendingOperation> operations = new HashSet<>();
    for (final DiffTuple tuple : tuples) {
      assertTrue(operations.add(new PendingOperation(tuple.getDiff(), tuple.getNewNodeKey(), tuple.getOldNodeKey())),
          "one pending tuple per node and operation kind");
    }
    return operations;
  }

  private static Set<Long> operationKeys(final JsonObject diff, final String kind) {
    final Set<Long> keys = new HashSet<>();
    for (final var operation : diff.getAsJsonArray("diffs")) {
      if (operation.getAsJsonObject().has(kind)) {
        assertTrue(keys.add(operation.getAsJsonObject().getAsJsonObject(kind).get("nodeKey").getAsLong()));
      }
    }
    return keys;
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

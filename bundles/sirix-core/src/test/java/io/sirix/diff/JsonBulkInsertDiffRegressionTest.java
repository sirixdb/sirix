package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
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
        }
      }
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
    try (final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,99]"), JsonNodeTrx.Commit.NO);
      final long array = wtx.getNodeKey();
      wtx.commit();
      try (final var files = Files.list(diffDirectory(session))) {
        assertFalse(files.findAny().isPresent(), "fresh-resource first commit must not emit sidecars");
      }
      return array;
    }
  }
}

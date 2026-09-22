package io.sirix.diff;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.IngestArrayPositionProbe;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The no-hint structural resolver is the byte oracle for every real commit below. */
final class JsonDiffIngestPositionsTest {
  enum Edit {
    NONE, HEAD, MIDDLE, REMOVE, MOVE, REPLACE_FIELD, BULK_HEAD, UPDATE
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest
  @EnumSource(Edit.class)
  void appendThenEditMatchesStructuralResolver(final Edit edit) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      final ResourceConfiguration config = config().useDeweyIDs(deweyIDs).build();
      try (
          final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
          final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final long array;
        try (final JsonNodeTrx seed = session.beginNodeTrx()) {
          seed.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[[0,1],{\"rows\":[10,11]},2]"),
              JsonNodeTrx.Commit.NO);
          array = seed.getNodeKey();
          seed.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
          append(wtx, array, "[[3,4],{\"more\":[5,6]},7]");
          assertFalse(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
          final long appendedTail = wtx.getNodeKey();
          assertTrue(wtx.moveTo(array));
          switch (edit) {
            case NONE -> {
            }
            case HEAD -> wtx.insertNumberValueAsFirstChild(-1);
            case MIDDLE -> {
              assertTrue(wtx.moveToFirstChild());
              wtx.insertNumberValueAsRightSibling(-1);
            }
            case REMOVE -> {
              assertTrue(wtx.moveToFirstChild());
              wtx.remove();
            }
            case MOVE -> wtx.moveSubtreeToFirstChild(appendedTail);
            case REPLACE_FIELD -> {
              assertTrue(wtx.moveToFirstChild());
              assertTrue(wtx.moveToRightSibling());
              assertTrue(wtx.moveToFirstChild());
              wtx.replaceObjectRecordValue(new StringValue("replacement"));
            }
            case BULK_HEAD -> wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[[-1,-2],0]"),
                JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
            case UPDATE -> {
              assertTrue(wtx.moveToFirstChild());
              assertTrue(wtx.moveToFirstChild());
              wtx.setNumberValue(99);
            }
          }
          if (edit != Edit.NONE && edit != Edit.UPDATE && edit != Edit.BULK_HEAD) {
            assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty(), "shifting edits must discard hints");
          }
          assertCompatibleCommit(database.getName(), session, wtx);

          // A later revision shifts old append ordinals: hints from the previous commit cannot survive.
          assertTrue(wtx.moveTo(array));
          wtx.insertNumberValueAsFirstChild(-99);
          assertTrue(wtx.moveTo(appendedTail));
          wtx.setNumberValue(8);
          assertCompatibleCommit(database.getName(), session, wtx);
        }
      }
    }
  }

  @Test
  void jacksonAppendIntoFusedArrayAndRollbackRevertReleaseHints() throws Exception {
    final ResourceConfiguration config = config().build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rows\":[0]}"), JsonNodeTrx.Commit.NO);
      assertTrue(wtx.moveToFirstChild());
      final long array = wtx.getNodeKey();
      wtx.commit();
      try (final var parser = new JsonFactory().createParser("[1,[2,3],{\"nested\":[4,5]}]")) {
        assertTrue(wtx.moveTo(array));
        wtx.insertSubtreeAsLastChild(parser, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
            JsonNodeTrx.SkipRootToken.YES);
      }
      assertEquals(7, IngestArrayPositionProbe.snapshot(wtx).size());
      assertCompatibleCommit(database.getName(), session, wtx);

      append(wtx, array, "[6,7]");
      assertFalse(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
      wtx.rollback();
      assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
      append(wtx, array, "[8]");
      assertCompatibleCommit(database.getName(), session, wtx);

      append(wtx, array, "[9,10]");
      assertFalse(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
      wtx.revertTo(1);
      assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
      append(wtx, array, "[11]");
      assertCompatibleCommit(database.getName(), session, wtx);
    }
  }

  enum Fallback {
    NO_CHILD_COUNTS, NO_PATH_SUMMARY, NO_DIFF_STORAGE
  }

  @Test
  void intermediateCommitsRetainOnlyTheCurrentBatch() throws Exception {
    final ResourceConfiguration config = config().build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx(5)) {
      wtx.insertArrayAsFirstChild();
      final long array = wtx.getNodeKey();
      wtx.commit();
      append(wtx, array, "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]");
      assertTrue(wtx.getRevisionNumber() > 2, "must actually cross intermediate commit boundaries");
      assertTrue(IngestArrayPositionProbe.snapshot(wtx).size() < 23, "intermediate commits must drop old hints");
      assertCompatibleCommit(database.getName(), session, wtx);
    }
  }

  @Test
  void failedPreCommitRetainsValidHintsForRetry() throws Exception {
    final ResourceConfiguration config = config().build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      final long array = wtx.getNodeKey();
      wtx.commit();
      append(wtx, array, "[0,1,2]");
      final var hints = IngestArrayPositionProbe.snapshot(wtx);
      final AtomicBoolean failOnce = new AtomicBoolean(true);
      wtx.addPreCommitHook(trx -> {
        if (failOnce.getAndSet(false)) {
          throw new IllegalStateException("injected before durability");
        }
      });
      assertThrows(IllegalStateException.class, wtx::commit);
      assertEquals(hints, IngestArrayPositionProbe.snapshot(wtx));
      assertCompatibleCommit(database.getName(), session, wtx);
    }
  }

  @ParameterizedTest
  @EnumSource(Fallback.class)
  void unavailableOrdinalsOrUnusedDiffsAllocateNoHints(final Fallback fallback) throws Exception {
    final ResourceConfiguration config = config().storeChildCount(fallback != Fallback.NO_CHILD_COUNTS)
                                                 .buildPathSummary(fallback != Fallback.NO_PATH_SUMMARY)
                                                 .storeDiffs(fallback != Fallback.NO_DIFF_STORAGE)
                                                 .build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      final long array = wtx.getNodeKey();
      wtx.commit();
      append(wtx, array, "[0,1,2]");
      assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
      if (config.storeDiffs()) {
        assertCompatibleCommit(database.getName(), session, wtx);
      } else {
        wtx.commit();
      }
    }
  }

  private static ResourceConfiguration.Builder config() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storageType(StorageType.FILE_CHANNEL);
  }

  private static void append(final JsonNodeTrx wtx, final long array, final String json) {
    assertTrue(wtx.moveTo(array));
    wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO,
        JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
  }

  private static void assertCompatibleCommit(final String database, final JsonResourceSession session,
      final JsonNodeTrx wtx) throws Exception {
    final ResourceConfiguration config = session.getResourceConfig();
    final var diffs = IngestArrayPositionProbe.pendingDiffs(wtx, config.areDeweyIDsStored);
    final var hints = IngestArrayPositionProbe.snapshot(wtx);
    final int revision = wtx.getRevisionNumber();
    wtx.commit();
    assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty());
    final byte[] committed;
    try (final var sidecars =
        Files.list(config.getResource().resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath()))) {
      committed = Files.readAllBytes(
          sidecars.filter(path -> path.getFileName().toString().endsWith("toRev" + revision + ".json"))
                  .findFirst()
                  .orElseThrow());
    }
    final int oldRevision = JsonParser.parseString(new String(committed, StandardCharsets.UTF_8))
                                      .getAsJsonObject()
                                      .get("old-revision")
                                      .getAsInt();
    final JsonDiffSerializer baseline = new JsonDiffSerializer(database, session, oldRevision, revision, diffs);
    assertArrayEquals(baseline.serializeSidecar().getBytes(StandardCharsets.UTF_8), committed);
    assertArrayEquals(baseline.serializeSidecar(hints).getBytes(StandardCharsets.UTF_8), committed);
  }
}

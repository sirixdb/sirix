package io.sirix.budget;

import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class BasicJsonDiffWorkBudgetTest {
  private static final VersioningType REPLAY_VERSIONING =
      VersioningType.valueOf(System.getProperty("sirix.replay.versioning", "SLIDING_SNAPSHOT"));

  @BeforeEach
  @AfterEach
  void cleanUp() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"ROLLING", "POSTORDER"})
  void unchangedPublicDiffSkipsTheEqualSubtree(final HashType hashType) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                                .storageType(StorageType.FILE_CHANNEL)
                                                                .versioningApproach(REPLAY_VERSIONING)
                                                                .hashKind(hashType)
                                                                .useDeweyIDs(deweyIDs)
                                                                .build();
      try (
          final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var writer = session.beginNodeTrx()) {
        final StringBuilder array = new StringBuilder("[");
        for (int index = 0; index < 256; index++) {
          if (index != 0) {
            array.append(',');
          }
          array.append(index);
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(array.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        writer.commit();
        writer.commit();
        final CursorWork work = new CursorWork();
        final JsonResourceSession counted = countingSession(session, work);
        final BasicJsonDiff diff = new BasicJsonDiff(database.getName());
        for (int overload = 0; overload < 4; overload++) {
          final int selected = overload;
          final var capture = WorkCapture.of(work.childMoves, work.siblingMoves, work.hashReads, work.readerOpens)
                                         .call(() -> switch (selected) {
                                           case 0 -> diff.generateDiff(counted, 1, 2);
                                           case 1 -> diff.generateDiff(counted, 1, 2, 0, 0);
                                           case 2 -> diff.generateDiff(counted, 1, 2, 0, 0, true);
                                           case 3 -> diff.generateDiff(counted, 1, 2, 0, 0, false);
                                           default -> throw new AssertionError(selected);
                                         });
          assertEquals(0, JsonParser.parseString(capture.result()).getAsJsonObject().getAsJsonArray("diffs").size());
          capture.work()
                 .assertExactly(work.childMoves, 2, "unchanged public diff descended into the equal array")
                 .assertZero(work.siblingMoves, "unchanged public diff walked the array elements")
                 .assertExactly(work.hashReads, 2, "both revision hashes must pass through the counting cursor")
                 .assertExactly(work.readerOpens, 2, "unchanged public diff opened redundant revision readers");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(HashType.class)
  void compactPublicDiffSkipsInsertedDescendants(final HashType hashType) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final int length : new int[] {16, 256}) {
        JsonTestHelper.deleteEverything();
        final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                                  .storageType(StorageType.FILE_CHANNEL)
                                                                  .versioningApproach(REPLAY_VERSIONING)
                                                                  .hashKind(hashType)
                                                                  .useDeweyIDs(deweyIDs)
                                                                  .build();
        try (
            final var database =
                JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
            final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
          writer.commit();
          assertTrue(writer.moveTo(1));
          final StringBuilder array = new StringBuilder("[");
          for (int index = 0; index < length; index++) {
            if (index != 0) {
              array.append(',');
            }
            array.append(index);
          }
          writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(array.append(']').toString()),
              JsonNodeTrx.Commit.NO);
          final long insertedRoot = writer.getNodeKey();
          assertEquals(length, writer.getChildCount());
          writer.commit();
          final CursorWork work = new CursorWork();
          work.skippedRoot = insertedRoot;
          final var capture =
              WorkCapture.of(work.childMoves, work.siblingMoves, work.readerOpens, work.insertedChildMoves)
                         .call(() -> new BasicJsonDiff(database.getName()).generateDiff(countingSession(session, work),
                             1, 2, 0, 0, false));
          final var document = JsonParser.parseString(capture.result()).getAsJsonObject();
          assertEquals(1, document.get("old-revision").getAsInt());
          assertEquals(2, document.get("new-revision").getAsInt());
          final var operations = document.getAsJsonArray("diffs");
          assertEquals(1, operations.size());
          final var insert = operations.get(0).getAsJsonObject().getAsJsonObject("insert");
          assertEquals(insertedRoot, insert.get("nodeKey").getAsLong());
          assertEquals(2, insert.get("insertPositionNodeKey").getAsLong());
          assertEquals("asRightSibling", insert.get("insertPosition").getAsString());
          assertEquals("jsonFragment", insert.get("type").getAsString());
          assertFalse(insert.has("data"));
          capture.work()
                 .assertZero(work.insertedChildMoves, "compact public diff descended into the inserted fragment")
                 .assertExactly(work.childMoves, 4,
                     "compact public diff must visit only the two existing array prefixes")
                 .assertExactly(work.readerOpens, 4, "compact public diff opened replay-expansion readers");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"ROLLING", "POSTORDER"})
  void smallEditSkipsUnchangedSibling(final HashType hashType) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      JsonTestHelper.deleteEverything();
      final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                                .storageType(StorageType.FILE_CHANNEL)
                                                                .versioningApproach(REPLAY_VERSIONING)
                                                                .hashKind(hashType)
                                                                .useDeweyIDs(deweyIDs)
                                                                .build();
      try (
          final var database =
              JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
          final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder("[[");
        for (int index = 0; index < 256; index++) {
          if (index != 0) {
            json.append(',');
          }
          json.append(index);
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append("],0]").toString()),
            JsonNodeTrx.Commit.NO);
        writer.commit();
        assertTrue(writer.moveTo(1));
        assertTrue(writer.moveToLastChild());
        writer.setNumberValue(1);
        writer.commit();
        final CursorWork work = new CursorWork();
        work.skippedRoot = 2;
        final var capture =
            WorkCapture.of(work.insertedChildMoves, work.childMoves, work.siblingMoves)
                       .call(() -> new BasicJsonDiff(database.getName()).generateDiff(countingSession(session, work), 1,
                           2, 0, 0, false));
        assertEquals(1, JsonParser.parseString(capture.result()).getAsJsonObject().getAsJsonArray("diffs").size());
        capture.work()
               .assertZero(work.insertedChildMoves, "small public edit must not descend into unchanged sibling")
               .assertAtMost(work.childMoves, 6, "only root and changed branch may be descended")
               .assertAtMost(work.siblingMoves, 6, "unchanged sibling elements must not be walked");
      }
    }
  }

  private static JsonResourceSession countingSession(final JsonResourceSession delegate, final CursorWork work) {
    return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
        new Class<?>[] {JsonResourceSession.class}, (proxy, method, arguments) -> {
          final Object result = invoke(delegate, method, arguments);
          if (result instanceof JsonNodeReadOnlyTrx reader) {
            work.readers++;
            return Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
                new Class<?>[] {JsonNodeReadOnlyTrx.class}, (cursor, cursorMethod, cursorArguments) -> {
                  switch (cursorMethod.getName()) {
                    case "moveToFirstChild" -> {
                      work.children++;
                      if (reader.getNodeKey() == work.skippedRoot) {
                        work.insertedChildren++;
                      }
                    }
                    case "moveToLeftSibling", "moveToRightSibling" -> work.siblings++;
                    case "getHash" -> work.hashes++;
                    default -> {
                    }
                  }
                  return invoke(reader, cursorMethod, cursorArguments);
                });
          }
          return result;
        });
  }

  private static Object invoke(final Object delegate, final Method method, final Object[] arguments) throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (final InvocationTargetException exception) {
      throw exception.getCause();
    }
  }

  private static final class CursorWork {
    private long children;
    private long siblings;
    private long hashes;
    private long readers;
    private long skippedRoot = -1;
    private long insertedChildren;
    private final WorkCounter childMoves =
        WorkCounter.alwaysOn("diffChildMoves", "one move to a first child", () -> children);
    private final WorkCounter siblingMoves =
        WorkCounter.alwaysOn("diffSiblingMoves", "one move to a sibling", () -> siblings);
    private final WorkCounter hashReads =
        WorkCounter.alwaysOn("diffHashReads", "one read of a subtree hash", () -> hashes);
    private final WorkCounter readerOpens =
        WorkCounter.alwaysOn("diffReaderOpens", "one revision reader opened", () -> readers);
    private final WorkCounter insertedChildMoves = WorkCounter.alwaysOn("diffInsertedChildMoves",
        "one descent into the inserted fragment", () -> insertedChildren);
  }
}

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Isolated
final class BasicJsonDiffWorkBudgetTest {
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

  private static JsonResourceSession countingSession(final JsonResourceSession delegate, final CursorWork work) {
    return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
        new Class<?>[] {JsonResourceSession.class}, (proxy, method, arguments) -> {
          final Object result = invoke(delegate, method, arguments);
          if (result instanceof JsonNodeReadOnlyTrx reader) {
            work.readers++;
            return Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
                new Class<?>[] {JsonNodeReadOnlyTrx.class}, (cursor, cursorMethod, cursorArguments) -> {
                  switch (cursorMethod.getName()) {
                    case "moveToFirstChild" -> work.children++;
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
    private final WorkCounter childMoves =
        WorkCounter.alwaysOn("diffChildMoves", "one move to a first child", () -> children);
    private final WorkCounter siblingMoves =
        WorkCounter.alwaysOn("diffSiblingMoves", "one move to a sibling", () -> siblings);
    private final WorkCounter hashReads =
        WorkCounter.alwaysOn("diffHashReads", "one read of a subtree hash", () -> hashes);
    private final WorkCounter readerOpens =
        WorkCounter.alwaysOn("diffReaderOpens", "one revision reader opened", () -> readers);
  }
}

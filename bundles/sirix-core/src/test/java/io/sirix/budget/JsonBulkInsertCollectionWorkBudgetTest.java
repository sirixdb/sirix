package io.sirix.budget;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.IngestArrayPositionProbe;
import io.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonBulkInsertCollectionWorkBudgetTest {
  @BeforeEach
  @AfterEach
  void cleanUp() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest
  @EnumSource(InsertPosition.class)
  void disabledDiffsSkipCollectionWork(final InsertPosition position) throws Exception {
    for (final boolean deweyIDs : new boolean[] {false, true}) {
      for (final boolean storeDiffs : new boolean[] {true, false}) {
        for (final int length : new int[] {16, 256}) {
          for (final JsonNodeTrx.SkipRootToken skipRoot : JsonNodeTrx.SkipRootToken.values()) {
            assertCollectionWork(position, deweyIDs, storeDiffs, length, skipRoot);
          }
        }
      }
    }
  }

  private static void assertCollectionWork(final InsertPosition position, final boolean deweyIDs,
      final boolean storeDiffs, final int length, final JsonNodeTrx.SkipRootToken skipRoot) throws Exception {
    JsonTestHelper.deleteEverything();
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .useDeweyIDs(deweyIDs)
                                                              .storeDiffs(storeDiffs)
                                                              .build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = session.beginNodeTrx(100_000)) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,99]"), JsonNodeTrx.Commit.NO);
      writer.commit();
      final boolean sibling = position == InsertPosition.AS_LEFT_SIBLING || position == InsertPosition.AS_RIGHT_SIBLING;
      final boolean skipped = skipRoot == JsonNodeTrx.SkipRootToken.YES;
      final int roots = skipped
          ? length
          : 1;
      assertTrue(writer.moveTo(sibling
          ? 2
          : 1));
      final JsonArray inserted = new JsonArray();
      for (int value = 0; value < length; value++) {
        inserted.add(value + 100);
      }
      try (final CursorWork work = new CursorWork(writer);
          final JsonReader reader = new ObservedReader(inserted.toString(), work)) {
        final var capture = WorkCapture.of(work.boundaryReads, work.deweyReads, work.siblingMoves, work.childMoves)
                                       .run(() -> insertSubtree(writer, reader, position, skipRoot));
        assertCollectionCounts(capture, work, storeDiffs, skipped, sibling, roots, length);
      }
      assertEquals(storeDiffs
          ? roots
          : 0, IngestArrayPositionProbe.pendingDiffs(writer).size());
      assertEquals(length + (skipped
          ? 3
          : 4), writer.getMaxNodeKey());
      writer.commit();
      final JsonArray expected = expectedContent(position, inserted, skipped);
      final StringWriter result = new StringWriter();
      JsonSerializer.newBuilder(session, result).build().call();
      assertEquals(expected, JsonParser.parseString(result.toString()));
    }
  }

  private static void insertSubtree(final JsonNodeTrx writer, final JsonReader reader, final InsertPosition position,
      final JsonNodeTrx.SkipRootToken skipRoot) {
    switch (position) {
      case AS_FIRST_CHILD ->
        writer.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, skipRoot);
      case AS_LAST_CHILD ->
        writer.insertSubtreeAsLastChild(reader, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, skipRoot);
      case AS_LEFT_SIBLING ->
        writer.insertSubtreeAsLeftSibling(reader, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, skipRoot);
      case AS_RIGHT_SIBLING ->
        writer.insertSubtreeAsRightSibling(reader, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, skipRoot);
    }
  }

  private static void assertCollectionCounts(final WorkReport capture, final CursorWork work, final boolean storeDiffs,
      final boolean skipped, final boolean sibling, final int roots, final int length) {
    capture.assertExactly(work.boundaryReads, storeDiffs && skipped
        ? 1
        : 0, "disabled diff storage must not capture a sibling boundary")
           .assertExactly(work.deweyReads, storeDiffs
               ? roots
               : 0, "disabled diff storage must not read inserted-root Dewey IDs after shredding")
           .assertExactly(work.siblingMoves, (storeDiffs && skipped
               ? length
               : 0)
               + (sibling
                   ? 1
                   : 0),
               "only enabled collection may sweep the inserted siblings")
           .assertExactly(work.childMoves, sibling
               ? 0
               : 1, "the counting cursor must observe the returned inserted root");
  }

  private static JsonArray expectedContent(final InsertPosition position, final JsonArray inserted,
      final boolean skipped) {
    final JsonArray expected = new JsonArray();
    if (position == InsertPosition.AS_RIGHT_SIBLING || position == InsertPosition.AS_LAST_CHILD) {
      expected.add(0);
    }
    if (position == InsertPosition.AS_LAST_CHILD) {
      expected.add(99);
    }
    if (!skipped) {
      expected.add(inserted);
    } else if (position == InsertPosition.AS_LEFT_SIBLING) {
      for (int index = inserted.size() - 1; index >= 0; index--) {
        expected.add(inserted.get(index));
      }
    } else {
      expected.addAll(inserted);
    }
    if (position == InsertPosition.AS_FIRST_CHILD || position == InsertPosition.AS_LEFT_SIBLING) {
      expected.add(0);
    }
    if (position != InsertPosition.AS_LAST_CHILD) {
      expected.add(99);
    }
    return expected;
  }

  private static final class ObservedReader extends JsonReader {
    private final CursorWork work;

    private ObservedReader(final String json, final CursorWork work) {
      super(new StringReader(json));
      this.work = work;
    }

    @Override
    public void beginArray() throws IOException {
      super.beginArray();
      work.phase = 1;
    }

    @Override
    public void endArray() throws IOException {
      super.endArray();
      work.phase = 2;
    }
  }

  private static final class CursorWork implements AutoCloseable {
    private final JsonNodeTrx writer;
    private final Field cursorField;
    private final InternalJsonNodeReadOnlyTrx delegate;
    private int phase;
    private long boundaries;
    private long dewey;
    private long siblings;
    private long children;
    private final WorkCounter boundaryReads =
        WorkCounter.alwaysOn("bulkDiffBoundaryReads", "one boundary read before shredding", () -> boundaries);
    private final WorkCounter deweyReads =
        WorkCounter.alwaysOn("bulkDiffDeweyReads", "one Dewey ID read after shredding", () -> dewey);
    private final WorkCounter siblingMoves =
        WorkCounter.alwaysOn("bulkDiffSiblingMoves", "one sibling move after shredding", () -> siblings);
    private final WorkCounter childMoves =
        WorkCounter.alwaysOn("bulkDiffChildMoves", "one child move after shredding", () -> children);

    private CursorWork(final JsonNodeTrx writer) throws ReflectiveOperationException {
      this.writer = writer;
      cursorField = AbstractNodeTrxImpl.class.getDeclaredField("nodeReadOnlyTrx");
      cursorField.setAccessible(true);
      delegate = (InternalJsonNodeReadOnlyTrx) cursorField.get(writer);
      cursorField.set(writer, Proxy.newProxyInstance(InternalJsonNodeReadOnlyTrx.class.getClassLoader(),
          new Class<?>[] {InternalJsonNodeReadOnlyTrx.class}, (proxy, method, arguments) -> {
            if (phase == 0) {
              switch (method.getName()) {
                case "getFirstChildKey", "getLastChildKey", "getLeftSiblingKey", "getRightSiblingKey" -> boundaries++;
                default -> {
                }
              }
            } else if (phase == 2) {
              switch (method.getName()) {
                case "getDeweyID" -> dewey++;
                case "moveToLeftSibling", "moveToRightSibling" -> siblings++;
                case "moveToFirstChild", "moveToLastChild" -> children++;
                default -> {
                }
              }
            }
            try {
              return method.invoke(delegate, arguments);
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
    }

    @Override
    public void close() throws IllegalAccessException {
      cursorField.set(writer, delegate);
    }
  }
}

package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonDiffSerializerArrayPositionTest {

  private static final int LARGE_ARRAY_LENGTH = 10_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void nestedArraySidecarRemainsByteForByteCompatible() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storeDiffs(false).build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,[10,11],{\"items\":[20,21]}]"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      final long outerNumberKey = wtx.getNodeKey();

      assertTrue(wtx.moveToRightSibling());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      final long nestedNumberKey = wtx.getNodeKey();

      assertTrue(wtx.moveToParent());
      assertTrue(wtx.moveToRightSibling());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      final long namedArrayNumberKey = wtx.getNodeKey();

      final List<DiffTuple> diffs =
          List.of(inserted(outerNumberKey), inserted(nestedNumberKey), inserted(namedArrayNumberKey));
      final String actual = new JsonDiffSerializer(database.getName(), session, 1, 1, diffs).serializeSidecar();

      final String expected = "{\"database\":\"json-path1\",\"resource\":\"shredded\",\"old-revision\":1,"
          + "\"new-revision\":1,\"diffs\":[{\"insert\":{\"nodeKey\":2,\"insertPositionNodeKey\":1,"
          + "\"insertPosition\":\"asFirstChild\",\"path\":\"/[0]\",\"type\":\"number\",\"data\":0}},"
          + "{\"insert\":{\"nodeKey\":5,\"insertPositionNodeKey\":4,\"insertPosition\":\"asRightSibling\","
          + "\"path\":\"/[1]/[1]\",\"type\":\"number\",\"data\":11}},{\"insert\":{\"nodeKey\":9,"
          + "\"insertPositionNodeKey\":8,\"insertPosition\":\"asRightSibling\","
          + "\"path\":\"/[2]/items/[1]\",\"type\":\"number\",\"data\":21}}],\"sirix-diff-format\":1,"
          + "\"operation-count\":3,\"operations-sha256\":"
          + "\"67c86d9b0a761e2183a7318f10e871a68bc0c6e771b0e7ed623e02082c001e78\"}";
      assertEquals(expected, actual);
    }
  }

  @Test
  void positionsAreIsolatedBetweenRevisions() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storeDiffs(false).build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[10,20,30]"), JsonNodeTrx.Commit.NO);
      wtx.commit();

      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      final long arrayKey = wtx.getNodeKey();
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      assertTrue(wtx.moveToRightSibling());
      final long shiftedNodeKey = wtx.getNodeKey();

      assertTrue(wtx.moveTo(arrayKey));
      wtx.insertNumberValueAsFirstChild(5);
      final long insertedNodeKey = wtx.getNodeKey();
      assertTrue(wtx.moveTo(shiftedNodeKey));
      wtx.setNumberValue(31);
      wtx.commit();

      final List<DiffTuple> diffs =
          List.of(deleted(shiftedNodeKey), inserted(insertedNodeKey), updated(shiftedNodeKey));
      final String serialized = new JsonDiffSerializer(database.getName(), session, 1, 2, diffs).serializeSidecar();
      final JsonObject document = JsonParser.parseString(serialized).getAsJsonObject();

      assertEquals("/[2]",
          document.getAsJsonArray("diffs")
                  .get(0)
                  .getAsJsonObject()
                  .getAsJsonObject("delete")
                  .get("path")
                  .getAsString());
      assertEquals("/[0]",
          document.getAsJsonArray("diffs")
                  .get(1)
                  .getAsJsonObject()
                  .getAsJsonObject("insert")
                  .get("path")
                  .getAsString());
      assertEquals("/[3]",
          document.getAsJsonArray("diffs")
                  .get(2)
                  .getAsJsonObject()
                  .getAsJsonObject("update")
                  .get("path")
                  .getAsString());
    }
  }

  /**
   * The legacy prefix walk needs 49,995,000 sibling moves for this fixture. Its counted run exceeds
   * the 10,000-move budget after only 142 elements; the cached forward scan resolves all 10,000
   * positions in exactly 9,999 moves.
   */
  @Test
  void largeArrayPositionsRequireOneSiblingPass() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storeDiffs(false).build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(largeArray(LARGE_ARRAY_LENGTH)),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      final List<DiffTuple> diffs = new ArrayList<>(LARGE_ARRAY_LENGTH);
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      for (int index = 0; index < LARGE_ARRAY_LENGTH; index++) {
        diffs.add(inserted(wtx.getNodeKey()));
        if (index + 1 < LARGE_ARRAY_LENGTH) {
          assertTrue(wtx.moveToRightSibling());
        }
      }

      final SiblingMoveCounter moves = new SiblingMoveCounter(LARGE_ARRAY_LENGTH);
      final JsonResourceSession countedSession = countingSession(session, moves);
      final String serialized =
          new JsonDiffSerializer(database.getName(), countedSession, 1, 1, diffs).serializeSidecar();
      final JsonObject document = JsonParser.parseString(serialized).getAsJsonObject();

      assertEquals(LARGE_ARRAY_LENGTH, document.getAsJsonArray("diffs").size());
      assertEquals(LARGE_ARRAY_LENGTH, document.get(JsonDiffIntegrity.OPERATION_COUNT_FIELD).getAsInt());
      assertEquals(0, moves.leftMoves, "position resolution must not walk array prefixes backwards");
      assertEquals(LARGE_ARRAY_LENGTH - 1, moves.rightMoves,
          "all positions in one array must be resolved by one forward sibling pass");
    }
  }

  private static DiffTuple inserted(final long nodeKey) {
    return new DiffTuple(DiffFactory.DiffType.INSERTED, nodeKey, 0, null);
  }

  private static DiffTuple deleted(final long nodeKey) {
    return new DiffTuple(DiffFactory.DiffType.DELETED, 0, nodeKey, null);
  }

  private static DiffTuple updated(final long nodeKey) {
    return new DiffTuple(DiffFactory.DiffType.UPDATED, nodeKey, nodeKey, null);
  }

  private static String largeArray(final int length) {
    final StringBuilder json = new StringBuilder(length * 6);
    json.append('[');
    for (int index = 0; index < length; index++) {
      if (index != 0) {
        json.append(',');
      }
      json.append(index);
    }
    return json.append(']').toString();
  }

  private static JsonResourceSession countingSession(final JsonResourceSession delegate,
      final SiblingMoveCounter moves) {
    return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
        new Class<?>[] {JsonResourceSession.class}, (proxy, method, arguments) -> {
          final Object result = invoke(delegate, method, arguments);
          if (result instanceof JsonNodeReadOnlyTrx rtx) {
            return countingTransaction(rtx, moves);
          }
          return result;
        });
  }

  private static JsonNodeReadOnlyTrx countingTransaction(final JsonNodeReadOnlyTrx delegate,
      final SiblingMoveCounter moves) {
    return (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
        new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, arguments) -> {
          if (method.getName().equals("moveToLeftSibling")) {
            moves.recordLeftMove();
          } else if (method.getName().equals("moveToRightSibling")) {
            moves.recordRightMove();
          }
          return invoke(delegate, method, arguments);
        });
  }

  private static Object invoke(final Object delegate, final Method method, final Object[] arguments) throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (final InvocationTargetException exception) {
      throw exception.getCause();
    }
  }

  private static final class SiblingMoveCounter {
    private final long maximumMoves;
    private long leftMoves;
    private long rightMoves;

    private SiblingMoveCounter(final long maximumMoves) {
      this.maximumMoves = maximumMoves;
    }

    private void recordLeftMove() {
      leftMoves++;
      checkBudget();
    }

    private void recordRightMove() {
      rightMoves++;
      checkBudget();
    }

    private void checkBudget() {
      final long totalMoves = leftMoves + rightMoves;
      assertTrue(totalMoves <= maximumMoves, () -> "array-position resolution exceeded its linear sibling-move budget: "
          + totalMoves + " > " + maximumMoves);
    }
  }
}

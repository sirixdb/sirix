package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ordinals the sidecar writes. How much traversal producing them costs is budgeted separately,
 * by {@code io.sirix.budget.JsonDiffArrayPositionWorkBudgetTest}.
 */
final class JsonDiffSerializerArrayPositionTest {

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

      final List<DiffTuple> reordered =
          List.of(inserted(namedArrayNumberKey), inserted(outerNumberKey), inserted(nestedNumberKey));
      final JsonObject document = JsonParser.parseString(
          new JsonDiffSerializer(database.getName(), session, 1, 1, reordered).serializeSidecar()).getAsJsonObject();
      assertEquals("/[2]/items/[1]", pathOf(document, 0));
      assertEquals("/[0]", pathOf(document, 1));
      assertEquals("/[1]/[1]", pathOf(document, 2));
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

  private static String pathOf(final JsonObject document, final int diffIndex) {
    return document.getAsJsonArray("diffs")
                   .get(diffIndex)
                   .getAsJsonObject()
                   .getAsJsonObject("insert")
                   .get("path")
                   .getAsString();
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
}

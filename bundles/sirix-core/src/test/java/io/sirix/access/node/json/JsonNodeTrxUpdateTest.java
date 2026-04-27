package io.sirix.access.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.service.json.shredder.JsonShredder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonNodeTrxUpdateTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testDeweyIDs() {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = manager.beginNodeReadOnlyTrx()) {
      new DescendantAxis(rtx).forEach(nodeKey -> {
        if (rtx.isObjectKey()) {
          System.out.print("name:" + rtx.getName() + " ");
        } else if (rtx.isObject()) {
          System.out.print("object ");
        } else if (rtx.isArray()) {
          System.out.print("array ");
        }
        System.out.println(
            "nodeKey:" + rtx.getNodeKey() + " deweyID:" + rtx.getDeweyID() + " level:" + rtx.getDeweyID().getLevel());
      });
    }
  }

  @Test
  public void testUpdateObjectRecordName() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);

      assertEquals(new QNm("foo"), wtx.getName());
      wtx.setObjectKeyName("foobar");

      assertsForUpdateObjectRecordName(wtx);

      wtx.commit();

      assertsForUpdateObjectRecordName(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateObjectRecordName(rtx);
      }
    }
  }

  private void assertsForUpdateObjectRecordName(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(2);

    Assert.assertEquals(new QNm("foobar"), rtx.getName());
  }

  @Test
  public void testUpdateStringValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy STRING "bar" inside foo array was at key 4 → fused: key 3 (foo subtree shrank by
      // one because the OBJECT_KEY+ARRAY pair fused into a single OBJECT_NAMED_ARRAY).
      wtx.moveTo(3);

      assertEquals("bar", wtx.getValue());
      wtx.setStringValue("baz");

      assertsForUpdateStringValue(wtx);

      wtx.commit();

      assertsForUpdateStringValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateStringValue(rtx);
      }
    }
  }

  private void assertsForUpdateStringValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(3);

    assertEquals("baz", rtx.getValue());
  }

  @Test
  public void testUpdateNumberValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy NUMBER 2.33 inside foo array was at key 6 → fused: key 5.
      wtx.moveTo(5);

      assertEquals(2.33, wtx.getNumberValue());
      wtx.setNumberValue(5.77);

      assertsForUpdateNumberValue(wtx);

      wtx.commit();

      assertsForUpdateNumberValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateNumberValue(rtx);
      }
    }
  }

  private void assertsForUpdateNumberValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(5);

    assertEquals(5.77, rtx.getNumberValue());
  }

  @Test
  public void testUpdateBooleanValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy "helloo":true was OBJECT_KEY(11)+BOOLEAN(12). Under fusion these collapse into a
      // single OBJECT_NAMED_BOOLEAN at key 8.
      wtx.moveTo(8);

      assertTrue(wtx.getBooleanValue());
      wtx.setBooleanValue(false);

      assertsForUpdateBooleanValue(wtx);

      wtx.commit();

      assertsForUpdateBooleanValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateBooleanValue(rtx);
      }
    }
  }

  private void assertsForUpdateBooleanValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(8);

    assertFalse(rtx.getBooleanValue());
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenStoreUpdateOperations() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      mutateForDiffScenario(wtx);
      wtx.commit();

      final var diffPath = manager.getResourceConfig()
                                  .getResource()
                                  .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                  .resolve("diffFromRev1toRev2.json");

      // Fusion shifts the inserted-subtree's interior nodeKey by one (primitive-valued fields
      // collapse two records to one), so we need a variant golden file for the fused shredder.
      final String golden = "diffFromRev1toRev2-fused.json";
      assertEquals(Files.readString(JSON.resolve(golden)), Files.readString(diffPath));
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenGetUpdateOperationsInSubtree() {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      mutateForDiffScenario(wtx);
      wtx.commit();

      // foo (root's first field) is at key 2 under fusion, same as legacy.
      wtx.moveTo(2);

      var rootDeweyId = wtx.getDeweyID();
      var updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, Integer.MAX_VALUE);

      assertEquals(4, updateOperations.size());
      assertTrue(updateOperations.get(0).has("insert"));
      assertTrue(updateOperations.get(1).has("delete"));
      assertTrue(updateOperations.get(2).has("insert"));
      assertTrue(updateOperations.get(3).has("update"));

      // Legacy first OBJECT inside tada array was key 17 → fused: key 11 (tada at 10, its first
      // child is OBJECT at 11).
      wtx.moveTo(11);
      wtx.insertObjectRecordAsFirstChild("foo1", new StringValue("bar1"));
      wtx.commit();

      // tada was OBJECT_KEY at legacy key 15 → fused OBJECT_NAMED_ARRAY at key 10.
      wtx.moveTo(10);
      rootDeweyId = wtx.getDeweyID();
      updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, 1);

      assertEquals(0, updateOperations.size());

      updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, 2);

      assertEquals(1, updateOperations.size());
      assertTrue(updateOperations.get(0).has("insert"));
    }
  }

  /**
   * Drives the rev1 → rev2 mutation sequence shared by the diff and update-ops tests. Legacy
   * keys are shifted by one per fused field encountered before the targeted node:
   * <ul>
   *   <li>foo's NULL — legacy 5 → fused 4 (foo: OBJECT_KEY+ARRAY → OBJECT_NAMED_ARRAY).</li>
   *   <li>foo's STRING "bar" — legacy 4 → fused 3.</li>
   *   <li>foo's NUMBER 2.33 — legacy 6 → fused 5.</li>
   *   <li>bar's "hello" field — legacy OBJECT_KEY 9 → fused OBJECT_NAMED_STRING 7
   *     (bar: OBJECT_KEY+OBJECT → OBJECT_NAMED_OBJECT, then hello: OBJECT_KEY+STRING →
   *     OBJECT_NAMED_STRING).</li>
   *   <li>top-level "baz" — legacy OBJECT_KEY 13 → fused OBJECT_NAMED_STRING 9.</li>
   *   <li>top-level "tada" — legacy OBJECT_KEY 15 → fused OBJECT_NAMED_ARRAY 10.</li>
   *   <li>"baz" inside tada[1] — legacy BOOLEAN 22 → fused OBJECT_NAMED_BOOLEAN 14.</li>
   * </ul>
   */
  private static void mutateForDiffScenario(final JsonNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
    wtx.moveTo(4); // legacy 5 = NULL_VALUE
    wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"), JsonNodeTrx.Commit.NO);
    wtx.moveTo(4); // legacy 5 = NULL_VALUE
    wtx.remove();
    wtx.moveTo(3); // legacy 4 = STRING_VALUE "bar"
    wtx.insertBooleanValueAsRightSibling(true);
    wtx.setBooleanValue(false);
    wtx.moveTo(5); // legacy 6 = NUMBER_VALUE 2.33
    wtx.setNumberValue(1.2);
    wtx.moveTo(7); // legacy 9 = OBJECT_KEY "hello" (inside "bar"). Fused OBJECT_NAMED_STRING.
    wtx.remove();
    wtx.moveTo(9); // legacy 13 = OBJECT_KEY "baz" (top-level). Fused OBJECT_NAMED_STRING.
    wtx.remove();
    wtx.moveTo(10); // legacy 15 = OBJECT_KEY "tada". Fused OBJECT_NAMED_ARRAY.
    wtx.setObjectKeyName("tadaa");
    wtx.moveTo(14); // legacy 22 = BOOLEAN false inside tada[1]. Fused OBJECT_NAMED_BOOLEAN.
    wtx.setBooleanValue(true);
  }
}

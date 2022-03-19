package org.sirix.access.node.json;

import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.service.json.shredder.JsonShredder;

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
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testDeweyIDs() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {
      new DescendantAxis(rtx).forEach(
          nodeKey -> {
            if (rtx.isObjectKey()) {
              System.out.print("name:" + rtx.getName() + " ");
            }
            else if (rtx.isObject()) {
              System.out.print("object ");
            }
            else if (rtx.isArray()) {
              System.out.print("array ");
            }
            System.out.println("nodeKey:" + rtx.getNodeKey() + " deweyID:" + rtx.getDeweyID() + " level:" + rtx.getDeweyID().getLevel());
          });
    }
  }

  @Test
  public void testUpdateObjectRecordName() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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

    assertEquals(new QNm("foobar"), rtx.getName());
  }

  @Test
  public void testUpdateStringValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);

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
    rtx.moveTo(4);

    assertEquals("baz", rtx.getValue());
  }

  @Test
  public void testUpdateNumberValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(6);

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
    rtx.moveTo(6);

    assertEquals(5.77, rtx.getNumberValue());
  }

  @Test
  public void testUpdateBooleanValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(12);

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
    rtx.moveTo(12);

    assertFalse(rtx.getBooleanValue());
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenStoreUpdateOperations() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot().trx().moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.moveTo(5);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"), JsonNodeTrx.Commit.NO);
      wtx.moveTo(5);
      wtx.remove();
      wtx.moveTo(4);
      wtx.insertBooleanValueAsRightSibling(true);
      wtx.setBooleanValue(false);
      wtx.moveTo(6);
      wtx.setNumberValue(1.2);
      wtx.moveTo(9);
      wtx.remove();
      wtx.moveTo(13);
      wtx.remove();
      wtx.moveTo(15);
      wtx.setObjectKeyName("tadaa");
      wtx.moveTo(22);
      wtx.setBooleanValue(true);
      wtx.commit();

      final var diffPath = manager.getResourceConfig()
                                  .getResource()
                                  .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                  .resolve("diffFromRev1toRev2.json");

      assertEquals(Files.readString(JSON.resolve("diffFromRev1toRev2.json")), Files.readString(diffPath));
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenGetUpdateOperationsInSubtree() {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot().trx().moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.moveTo(5);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"), JsonNodeTrx.Commit.NO);
      wtx.moveTo(5);
      wtx.remove();
      wtx.moveTo(4);
      wtx.insertBooleanValueAsRightSibling(true);
      wtx.setBooleanValue(false);
      wtx.moveTo(6);
      wtx.setNumberValue(1.2);
      wtx.moveTo(9);
      wtx.remove();
      wtx.moveTo(13);
      wtx.remove();
      wtx.moveTo(15);
      wtx.setObjectKeyName("tadaa");
      wtx.moveTo(22);
      wtx.setBooleanValue(true);
      wtx.commit();

      wtx.moveTo(2);

      var rootDeweyId = wtx.getDeweyID();
      var updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, Integer.MAX_VALUE);

      assertEquals(4, updateOperations.size());
      assertTrue(updateOperations.get(0).has("insert"));
      assertTrue(updateOperations.get(1).has("delete"));
      assertTrue(updateOperations.get(2).has("insert"));
      assertTrue(updateOperations.get(3).has("update"));

      wtx.moveTo(17);
      wtx.insertObjectRecordAsFirstChild("foo1", new StringValue("bar1"));
      wtx.commit();

      wtx.moveTo(15);
      rootDeweyId = wtx.getDeweyID();
      updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, 1);

      assertEquals(0, updateOperations.size());

      updateOperations = wtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, 2);

      assertEquals(1, updateOperations.size());
      assertTrue(updateOperations.get(0).has("insert"));
    }
  }
}

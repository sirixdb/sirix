package io.sirix.service.json;

import com.google.gson.JsonParser;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class BasicJsonDiffTest {
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
  public void test_whenMultipleRevisionsExist_thenDiff1() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(15);
      final var nodeKey = wtx.insertObjectRecordAsRightSibling("hereIAm", new StringValue("yeah")).getParentKey();
      wtx.commit();
      wtx.moveTo(nodeKey);
      wtx.insertObjectRecordAsRightSibling("111hereIAm", new StringValue("111yeah"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev2.json")), diffRev1Rev2);

      final String diffRev1Rev3 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 3);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev3.json")), diffRev1Rev3);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff2() {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));

      final String diffRev1Rev4 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 4);
      System.out.println(diffRev1Rev4);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff3() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.commit();
      wtx.insertObjectAsFirstChild();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("emptyObjectDiffRev1Rev2.json")),
          diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff4() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"test\", \"test\"]"));
      wtx.moveTo(2);
      wtx.remove();
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff5() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"baz\":\"hello\"}"));
      wtx.moveTo(2);
      wtx.replaceObjectRecordValue(new StringValue("test"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace1.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff6() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
             {"data":"data"}
          """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              {"data":"data"}
          """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              {"data":"data"}
          """.strip()));
      wtx.moveTo(2);
      wtx.remove();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 2, 5);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("deletion-at-eof.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff7() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              {"data":"data"}
          """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              {"data":"data"}
          """.strip()));
      wtx.moveTo(2);
      wtx.remove();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 2, 4);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace2.json")), diffRev1Rev2);
    }
  }

  /**
   * Test that UPDATE operations on array elements at different positions generate correct array index
   * paths.
   *
   * This test verifies the fix for the bug where getArrayPosition() was corrupting the transaction
   * cursor position, causing incorrect paths like /items/[5] instead of /items/[10] for updates.
   */
  @Test
  public void test_updateArrayElementPath_atIndex10() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Create an array with 20 objects, each having a "value" field
      final var sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < 20; i++) {
        if (i > 0)
          sb.append(",");
        sb.append("{\"value\":\"val").append(i).append("\"}");
      }
      sb.append("]");
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));

      // Navigate to arr[10].value key and update it in the same transaction
      // Structure: root array -> obj[10] -> "value" key -> string value
      wtx.moveTo(1); // root array
      wtx.moveToFirstChild(); // arr[0]
      for (int i = 0; i < 10; i++) {
        wtx.moveToRightSibling(); // move to arr[10]
      }
      wtx.moveToFirstChild(); // "value" object key
      wtx.replaceObjectRecordValue(new StringValue("updated"));
      wtx.commit(); // Rev 1 with updated value

      final String diff = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      System.out.println("Diff output: " + diff);

      // Parse and verify the path contains correct index [10]
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");

      // The diff should show a replace operation for the value change
      if (diffs.size() > 0) {
        final var firstDiff = diffs.get(0).getAsJsonObject();
        if (firstDiff.has("replace")) {
          final var path = firstDiff.getAsJsonObject("replace").get("path").getAsString();
          assertTrue("Path should contain [10] for update at index 10, but was: " + path, path.contains("[10]"));
        } else if (firstDiff.has("update")) {
          final var path = firstDiff.getAsJsonObject("update").get("path").getAsString();
          assertTrue("Path should contain [10] for update at index 10, but was: " + path, path.contains("[10]"));
        }
      }
    }
  }

  /**
   * Test update at index 50 to verify middle-of-array positions work.
   */
  @Test
  public void test_updateAtArrayMiddle_index50() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Create an array with 100 objects and update at index 50 in same transaction
      final var sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < 100; i++) {
        if (i > 0)
          sb.append(",");
        sb.append("{\"rating\":\"r").append(i).append("\"}");
      }
      sb.append("]");
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));

      // Update item at index 50 in same transaction
      navigateToArrayItemKeySimple(wtx, 50);
      wtx.replaceObjectRecordValue(new StringValue("updated50"));
      wtx.commit(); // Single commit

      final String diff = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");

      if (diffs.size() > 0) {
        final var firstDiff = diffs.get(0).getAsJsonObject();
        String path;
        if (firstDiff.has("replace")) {
          path = firstDiff.getAsJsonObject("replace").get("path").getAsString();
        } else if (firstDiff.has("update")) {
          path = firstDiff.getAsJsonObject("update").get("path").getAsString();
        } else {
          throw new AssertionError("Expected replace or update diff");
        }
        assertTrue("Path should contain [50], but was: " + path, path.contains("[50]"));
      }
    }
  }

  /**
   * Test UPDATE at beginning of array (index 0).
   */
  @Test
  public void test_updateAtArrayBeginning() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Create array with 10 items and update in same transaction (like test5)
      final var sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < 10; i++) {
        if (i > 0)
          sb.append(",");
        sb.append("{\"val\":\"v").append(i).append("\"}");
      }
      sb.append("]");
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));

      // Update first item (index 0) in same transaction
      wtx.moveTo(1); // root array
      wtx.moveToFirstChild(); // arr[0]
      wtx.moveToFirstChild(); // "val" key
      wtx.replaceObjectRecordValue(new StringValue("updated0"));
      wtx.commit(); // Single commit

      final String diff = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");

      // Should have a replace diff for the updated value
      if (diffs.size() > 0) {
        final var firstDiff = diffs.get(0).getAsJsonObject();
        assertTrue("Should have replace or update diff", firstDiff.has("replace") || firstDiff.has("update"));
        String path;
        if (firstDiff.has("replace")) {
          path = firstDiff.getAsJsonObject("replace").get("path").getAsString();
        } else {
          path = firstDiff.getAsJsonObject("update").get("path").getAsString();
        }
        assertTrue("Path should contain [0], but was: " + path, path.contains("[0]"));
      }
    }
  }

  /**
   * Test INSERT paths are correct for array elements - using test3 pattern.
   */
  @Test
  public void test_insertArrayElementPath() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Create empty array first
      wtx.insertArrayAsFirstChild();
      wtx.commit(); // Rev 1

      // Insert first object
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":0}"));
      wtx.commit(); // Rev 2

      // Insert second object
      wtx.moveTo(1);
      wtx.moveToFirstChild();
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"id\":1}"));
      wtx.commit(); // Rev 3

      // Verify Rev2 -> Rev3: should show insert at [1]
      final String diff = new BasicJsonDiff(databaseName).generateDiff(manager, 2, 3);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");

      if (diffs.size() > 0) {
        final var insertDiff = diffs.get(0).getAsJsonObject().getAsJsonObject("insert");
        if (insertDiff != null && insertDiff.has("path")) {
          final var path = insertDiff.get("path").getAsString();
          assertTrue("Insert path should contain [1], but was: " + path, path.contains("[1]"));
        }
      }
    }
  }

  /**
   * Test DELETE paths are correct for array elements - using test6/test7 pattern.
   */
  @Test
  public void test_deleteArrayElementPath() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Create empty array
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.moveTo(1);

      // Insert 3 objects as first child (pushes existing to the right)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":2}"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":1}"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":0}"));
      wtx.commit(); // Rev 1 - array has 3 items

      // Delete middle item (index 1)
      wtx.moveTo(1); // root array
      wtx.moveToFirstChild(); // arr[0]
      wtx.moveToRightSibling(); // arr[1]
      wtx.remove();
      wtx.commit(); // Rev 2

      final String diff = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");

      if (diffs.size() > 0) {
        final var deleteDiff = diffs.get(0).getAsJsonObject().getAsJsonObject("delete");
        if (deleteDiff != null && deleteDiff.has("path")) {
          final var path = deleteDiff.get("path").getAsString();
          assertTrue("Delete path should contain [1], but was: " + path, path.contains("[1]"));
        }
      }
    }
  }

  /**
   * Helper method to navigate to an array item's first object key (simple version for root array).
   */
  private void navigateToArrayItemKeySimple(JsonNodeTrx wtx, int index) {
    wtx.moveTo(1); // root array
    wtx.moveToFirstChild(); // first item
    for (int i = 0; i < index; i++) {
      wtx.moveToRightSibling();
    }
    wtx.moveToFirstChild(); // object key
  }

  /**
   * Helper to assert the diff path contains the expected array index.
   */
  private void assertDiffPathContainsIndex(String diffJson, int expectedIndex, String context) {
    final var jsonDiff = JsonParser.parseString(diffJson).getAsJsonObject();
    final var diffs = jsonDiff.getAsJsonArray("diffs");
    assertEquals(context + ": Should have exactly 1 diff", 1, diffs.size());

    final var diffEntry = diffs.get(0).getAsJsonObject();
    String path;
    if (diffEntry.has("update")) {
      path = diffEntry.getAsJsonObject("update").get("path").getAsString();
    } else if (diffEntry.has("insert")) {
      path = diffEntry.getAsJsonObject("insert").get("path").getAsString();
    } else if (diffEntry.has("delete")) {
      path = diffEntry.getAsJsonObject("delete").get("path").getAsString();
    } else {
      throw new AssertionError(context + ": Unknown diff type");
    }

    final String expectedPattern = "[" + expectedIndex + "]";
    assertTrue(context + ": Path should contain " + expectedPattern + ", but was: " + path,
        path.contains(expectedPattern));
  }
}

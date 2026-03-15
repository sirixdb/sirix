package io.sirix.service.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Edge-case tests for {@link BasicJsonDiff}.
 */
public final class JsonDiffEdgeCaseTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testIdenticalDocumentsDiff() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // insertSubtreeAsFirstChild auto-commits -> rev 1
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"foo\":\"bar\",\"num\":42}"));

      // Diff rev 1 vs rev 1: no changes
      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 1);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs, "diffs array must be present");
      assertEquals(0, diffs.size(), "identical document should produce empty diffs");
    }
  }

  @Test
  void testInsertOnlyDiff() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: empty array (insertArrayAsFirstChild does NOT auto-commit)
      wtx.insertArrayAsFirstChild();
      wtx.commit();

      // Rev 2: insert one element into the array
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":1}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff entry");
      assertTrue(hasOperationType(diffs, "insert"), "diff should contain an insert operation");
      assertFalse(hasOperationType(diffs, "delete"), "diff should not contain a delete operation");
    }
  }

  @Test
  void testDeleteOnlyDiff() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with two keys (auto-commits)
      // Tree: doc(0) -> OBJECT(1)
      //   -> OBJECT_KEY "a"(2) -> OBJECT_STRING_VALUE "1"(3)
      //   -> OBJECT_KEY "b"(4) -> OBJECT_STRING_VALUE "2"(5)
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"a\":\"1\",\"b\":\"2\"}"));

      // Rev 2: remove the second key "b" (keeps one key, deletes the other)
      wtx.moveTo(4); // OBJECT_KEY "b"
      wtx.remove();
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff entry");
      assertTrue(hasOperationType(diffs, "delete"), "diff should contain a delete operation");
      assertFalse(hasOperationType(diffs, "insert"), "diff should not contain an insert operation");
    }
  }

  @Test
  void testStringValueUpdate() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with string value (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "name"(2) -> OBJECT_STRING_VALUE "alice"(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"alice\"}"));

      // Rev 2: update the string value in-place
      wtx.moveTo(3); // OBJECT_STRING_VALUE node
      wtx.setStringValue("bob");
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff for the value change");
      assertTrue(hasOperationType(diffs, "update"), "diff should contain an update operation");

      final var updateObj = getFirstOperationOfType(diffs, "update");
      assertNotNull(updateObj, "update object must be present");
      assertEquals("bob", updateObj.get("value").getAsString(), "updated value should be 'bob'");
    }
  }

  @Test
  void testNumberValueUpdate() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with number value (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "count"(2) -> OBJECT_NUMBER_VALUE 10(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"count\":10}"));

      // Rev 2: update the number value in-place
      wtx.moveTo(3); // OBJECT_NUMBER_VALUE node
      wtx.setNumberValue(99);
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff for the value change");
      assertTrue(hasOperationType(diffs, "update"), "diff should contain an update operation");

      final var updateObj = getFirstOperationOfType(diffs, "update");
      assertNotNull(updateObj, "update object must be present");
      assertEquals(99, updateObj.get("value").getAsInt(), "updated value should be 99");
    }
  }

  @Test
  void testBooleanValueUpdate() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with boolean value (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "active"(2) -> OBJECT_BOOLEAN_VALUE true(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"active\":true}"));

      // Rev 2: update the boolean value in-place
      wtx.moveTo(3); // OBJECT_BOOLEAN_VALUE node
      wtx.setBooleanValue(false);
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff for the value change");
      assertTrue(hasOperationType(diffs, "update"), "diff should contain an update operation");

      final var updateObj = getFirstOperationOfType(diffs, "update");
      assertNotNull(updateObj, "update object must be present");
      assertFalse(updateObj.get("value").getAsBoolean(), "updated value should be false");
    }
  }

  @Test
  void testMultipleOperationsOneDiff() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: array with three string elements (auto-commits)
      // Tree: doc(0) -> ARRAY(1) -> STRING_VALUE "first"(2) -> STRING_VALUE "second"(3) -> STRING_VALUE "third"(4)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"first\",\"second\",\"third\"]"));

      // Rev 2: update first, remove second, insert new at end
      wtx.moveTo(2); // STRING_VALUE "first"
      wtx.setStringValue("updated");

      wtx.moveTo(3); // STRING_VALUE "second"
      wtx.remove();

      // After removing node 3, node 4 ("third") is now the right sibling of node 2 ("updated")
      wtx.moveTo(4); // STRING_VALUE "third"
      wtx.insertStringValueAsRightSibling("appended");
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() >= 3, "should have diffs for update, delete, and insert");

      assertTrue(hasOperationType(diffs, "update") || hasOperationType(diffs, "replace"),
          "diff should contain an update or replace operation");
      assertTrue(hasOperationType(diffs, "delete"), "diff should contain a delete operation");
      assertTrue(hasOperationType(diffs, "insert"), "diff should contain an insert operation");
    }
  }

  @Test
  void testDeepNestedChange() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: deeply nested structure (auto-commits)
      // Tree: doc(0) -> OBJECT(1)
      //   -> OBJECT_KEY "a"(2) -> OBJECT(3)
      //     -> OBJECT_KEY "b"(4) -> OBJECT(5)
      //       -> OBJECT_KEY "c"(6) -> OBJECT(7)
      //         -> OBJECT_KEY "d"(8) -> OBJECT_STRING_VALUE "deep"(9)
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"a\":{\"b\":{\"c\":{\"d\":\"deep\"}}}}"));

      // Rev 2: update deepest value
      wtx.moveTo(9); // OBJECT_STRING_VALUE "deep"
      wtx.setStringValue("deeper");
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should detect the deep nested value change");

      final var updateObj = getFirstOperationOfType(diffs, "update");
      assertNotNull(updateObj, "deep nested change should produce an update");
      assertEquals("deeper", updateObj.get("value").getAsString());
    }
  }

  @Test
  void testInsertObjectRecord() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with one key (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "existing"(2) -> OBJECT_STRING_VALUE "value"(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"existing\":\"value\"}"));

      // Rev 2: add a new key-value pair
      wtx.moveTo(2); // OBJECT_KEY "existing"
      wtx.insertObjectRecordAsRightSibling("newKey", new StringValue("newValue"));
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff for the inserted record");
      assertTrue(hasOperationType(diffs, "insert"), "diff should contain an insert operation");
    }
  }

  @Test
  void testRemoveObjectRecord() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with two keys (auto-commits)
      // Tree: doc(0) -> OBJECT(1)
      //   -> OBJECT_KEY "keep"(2) -> OBJECT_STRING_VALUE "yes"(3)
      //   -> OBJECT_KEY "remove"(4) -> OBJECT_STRING_VALUE "me"(5)
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"keep\":\"yes\",\"remove\":\"me\"}"));

      // Rev 2: remove the "remove" key
      wtx.moveTo(4); // OBJECT_KEY "remove"
      wtx.remove();
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should have at least one diff for the removed record");
      assertTrue(hasOperationType(diffs, "delete"), "diff should contain a delete operation");
    }
  }

  @Test
  void testThreeRevisionDiffChain() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: initial document (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "step"(2) -> OBJECT_STRING_VALUE "one"(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"step\":\"one\"}"));

      // Rev 2: update value
      wtx.moveTo(3); // OBJECT_STRING_VALUE "one"
      wtx.setStringValue("two");
      wtx.commit();

      // Rev 3: add another key
      wtx.moveTo(2); // OBJECT_KEY "step"
      wtx.insertObjectRecordAsRightSibling("extra", new StringValue("three"));
      wtx.commit();

      // Diff rev1 -> rev2: should show update
      final String diff12 = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff12 = JsonParser.parseString(diff12).getAsJsonObject();
      final var diffs12 = jsonDiff12.getAsJsonArray("diffs");
      assertNotNull(diffs12);
      assertTrue(diffs12.size() > 0, "rev1->rev2 should have diffs");
      assertTrue(hasOperationType(diffs12, "update"), "rev1->rev2 should have update");

      // Diff rev2 -> rev3: should show insert
      final String diff23 = new BasicJsonDiff(databaseName).generateDiff(session, 2, 3);
      final var jsonDiff23 = JsonParser.parseString(diff23).getAsJsonObject();
      final var diffs23 = jsonDiff23.getAsJsonArray("diffs");
      assertNotNull(diffs23);
      assertTrue(diffs23.size() > 0, "rev2->rev3 should have diffs");
      assertTrue(hasOperationType(diffs23, "insert"), "rev2->rev3 should have insert");
    }
  }

  @Test
  void testDiffWithEmptyStartDocument() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: empty object (no auto-commit for insertObjectAsFirstChild)
      wtx.insertObjectAsFirstChild();
      wtx.commit();

      // Rev 2: add content to the empty object
      wtx.moveTo(1); // OBJECT node
      wtx.insertObjectRecordAsFirstChild("key", new StringValue("value"));
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "adding content to empty doc should produce diffs");
      assertTrue(hasOperationType(diffs, "insert"), "diff should contain an insert operation");
    }
  }

  @Test
  void testInsertMultipleElements() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: empty array (no auto-commit for insertArrayAsFirstChild)
      wtx.insertArrayAsFirstChild();
      wtx.commit();

      // Rev 2: insert three object elements (subtrees must begin with object or array)
      wtx.moveTo(1); // root array
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":3}"),
          JsonNodeTrx.Commit.NO);
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":2}"),
          JsonNodeTrx.Commit.NO);
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":1}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);

      int insertCount = 0;
      for (int i = 0; i < diffs.size(); i++) {
        final var entry = diffs.get(i).getAsJsonObject();
        if (entry.has("insert")) {
          insertCount++;
        }
      }
      assertEquals(3, insertCount, "should have exactly 3 insert operations");
    }
  }

  @Test
  void testObjectKeyRename() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with original key (auto-commits)
      // Tree: doc(0) -> OBJECT(1) -> OBJECT_KEY "oldKey"(2) -> OBJECT_STRING_VALUE "value"(3)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"oldKey\":\"value\"}"));

      // Rev 2: simulate rename by removing old key and inserting new key
      wtx.moveTo(2); // OBJECT_KEY "oldKey"
      wtx.remove();

      wtx.moveTo(1); // OBJECT
      wtx.insertObjectRecordAsFirstChild("newKey", new StringValue("value"));
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() >= 1, "rename should produce at least one diff");

      // A rename is represented as either replace or delete+insert
      final boolean hasReplace = hasOperationType(diffs, "replace");
      final boolean hasDeleteAndInsert =
          hasOperationType(diffs, "delete") && hasOperationType(diffs, "insert");
      assertTrue(hasReplace || hasDeleteAndInsert,
          "rename should produce either replace or delete+insert, got: " + diff);
    }
  }

  // --- Helper methods ---

  /**
   * Check whether a diffs array contains at least one operation of the given type.
   *
   * @param diffs the JSON array of diff entries
   * @param operationType the operation type key (e.g. "insert", "delete", "update", "replace")
   * @return true if at least one entry has the given operation type
   */
  private static boolean hasOperationType(final JsonArray diffs, final String operationType) {
    for (int i = 0; i < diffs.size(); i++) {
      final var entry = diffs.get(i).getAsJsonObject();
      if (entry.has(operationType)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return the inner JSON object for the first diff entry of the given type.
   *
   * @param diffs the JSON array of diff entries
   * @param operationType the operation type key (e.g. "insert", "delete", "update", "replace")
   * @return the inner JsonObject, or null if not found
   */
  private static JsonObject getFirstOperationOfType(final JsonArray diffs,
      final String operationType) {
    for (int i = 0; i < diffs.size(); i++) {
      final var entry = diffs.get(i).getAsJsonObject();
      if (entry.has(operationType)) {
        return entry.getAsJsonObject(operationType);
      }
    }
    return null;
  }
}

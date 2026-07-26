package io.sirix.service.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.NodeKind;
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

    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with string value (auto-commits)
      // Legacy: doc(0) -> OBJECT(1) -> OBJECT_KEY "name"(2) -> OBJECT_STRING_VALUE "alice"(3)
      // Fused:  doc(0) -> OBJECT(1) -> OBJECT_NAMED_STRING "name":"alice"(2)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"alice\"}"));

      // Rev 2: navigate structurally to the string value and update it in-place.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "name" (legacy) or OBJECT_NAMED_STRING (fused)
      if (wtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        wtx.moveToFirstChild(); // OBJECT_STRING_VALUE "alice"
      }
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

    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with number value (auto-commits)
      // Legacy: doc(0) -> OBJECT(1) -> OBJECT_KEY "count"(2) -> OBJECT_NUMBER_VALUE 10(3)
      // Fused:  doc(0) -> OBJECT(1) -> OBJECT_NAMED_NUMBER "count":10(2)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"count\":10}"));

      // Rev 2: navigate structurally to the number value and update it in-place.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "count" (legacy) or OBJECT_NAMED_NUMBER (fused)
      if (wtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        wtx.moveToFirstChild(); // OBJECT_NUMBER_VALUE 10
      }
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

    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: object with boolean value (auto-commits)
      // Legacy: doc(0) -> OBJECT(1) -> OBJECT_KEY "active"(2) -> OBJECT_BOOLEAN_VALUE true(3)
      // Fused:  doc(0) -> OBJECT(1) -> OBJECT_NAMED_BOOLEAN "active":true(2)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"active\":true}"));

      // Rev 2: navigate structurally to the boolean value and update it in-place.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "active" (legacy) or OBJECT_NAMED_BOOLEAN (fused)
      if (wtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        wtx.moveToFirstChild(); // OBJECT_BOOLEAN_VALUE true
      }
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

    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: deeply nested structure (auto-commits)
      // Legacy: doc -> OBJECT -> OBJECT_KEY "a" -> OBJECT -> OBJECT_KEY "b" -> OBJECT
      //   -> OBJECT_KEY "c" -> OBJECT -> OBJECT_KEY "d" -> OBJECT_STRING_VALUE "deep"
      // Fused:  parent "a"/"b"/"c" remain OBJECT_KEY (non-primitive children)
      //   but innermost "d":"deep" becomes OBJECT_NAMED_STRING.
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"a\":{\"b\":{\"c\":{\"d\":\"deep\"}}}}"));

      // Rev 2: navigate structurally to the innermost value.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "a"
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "b"
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "c"
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // OBJECT_KEY "d" (legacy) or OBJECT_NAMED_STRING (fused)
      if (wtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        wtx.moveToFirstChild(); // OBJECT_STRING_VALUE "deep"
      }
      wtx.setStringValue("deeper");
      wtx.commit();

      final String diff = new BasicJsonDiff(databaseName).generateDiff(session, 1, 2);
      final var jsonDiff = JsonParser.parseString(diff).getAsJsonObject();
      final var diffs = jsonDiff.getAsJsonArray("diffs");
      assertNotNull(diffs);
      assertTrue(diffs.size() > 0, "should detect the deep nested value change");

      // Under fusion the diff emits an "update" entry for each ancestor whose hash changed
      // (path-only updates) plus a final entry with the new value. Locate the value-bearing
      // entry rather than the first match.
      JsonObject valueUpdate = null;
      for (int i = 0; i < diffs.size(); i++) {
        final var entry = diffs.get(i).getAsJsonObject();
        if (entry.has("update")) {
          final var inner = entry.getAsJsonObject("update");
          if (inner.has("value")) {
            valueUpdate = inner;
            break;
          }
        }
      }
      assertNotNull(valueUpdate, "deep nested change should produce a value-carrying update");
      assertEquals("deeper", valueUpdate.get("value").getAsString());
    }
  }

  @Test
  void testInsertObjectRecord() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());

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

    final var databaseName = database.getName();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Rev 1: initial document (auto-commits)
      // Legacy: doc(0) -> OBJECT(1) -> OBJECT_KEY "step"(2) -> OBJECT_STRING_VALUE "one"(3)
      // Fused:  doc(0) -> OBJECT(1) -> OBJECT_NAMED_STRING "step":"one"(2)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"step\":\"one\"}"));

      // Rev 2: navigate to the value node structurally and update.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // "step" key-role node
      if (wtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        wtx.moveToFirstChild(); // OBJECT_STRING_VALUE "one"
      }
      wtx.setStringValue("two");
      wtx.commit();

      // Rev 3: add another key. Re-position on the "step" key-role node regardless of mode.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // OBJECT
      wtx.moveToFirstChild(); // key-role node ("step")
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

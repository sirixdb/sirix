package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration workflow tests for JSON operations in SirixDB.
 *
 * <p>These tests exercise full pipelines: create, shred, modify, serialize,
 * reopen, and verify — ensuring correctness across revisions, hashing,
 * DeweyIDs, path summaries, and auto-commit workflows.</p>
 */
public final class JsonIntegrationWorkflowTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Test 1: Create database, shred JSON, commit, serialize, and verify round-trip.
   */
  @Test
  void testCreateInsertCommitSerializePipeline() throws Exception {
    final String inputJson = "{\"name\":\"Alice\",\"age\":30,\"active\":true}";

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(inputJson), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      JSONAssert.assertEquals(inputJson, writer.toString(), true);
    }
  }

  /**
   * Test 2: Shred JSON, modify a string value, serialize, and verify the change.
   */
  @Test
  void testShredModifySerialize() throws Exception {
    final String inputJson = "{\"greeting\":\"hello\",\"target\":\"world\"}";

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(inputJson), JsonNodeTrx.Commit.NO);
      wtx.commit();

      // Navigate to "hello" value: documentRoot -> object -> "greeting" key -> string value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "greeting" key
      wtx.moveToFirstChild(); // "hello" value
      assertEquals("hello", wtx.getValue());

      wtx.setStringValue("goodbye");
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      final String expected = "{\"greeting\":\"goodbye\",\"target\":\"world\"}";
      JSONAssert.assertEquals(expected, writer.toString(), true);
    }
  }

  /**
   * Test 3: Create database, insert data, commit, close, reopen, insert more, and verify.
   */
  @Test
  void testCreateReopenInsertMore() throws Exception {
    // Phase 1: Create and populate
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"phase\":\"one\"}"), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }

    // Phase 2: Close and reopen
    JsonTestHelper.closeEverything();

    try (final var reopened = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = reopened.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Navigate to the object and add a new field
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.insertObjectRecordAsFirstChild("phase2", new StringValue("two"));
      wtx.commit();
    }

    // Phase 3: Verify both fields present
    try (final var reopened = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = reopened.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      final String result = writer.toString();
      assertTrue(result.contains("\"phase\":\"one\""), "Original field should be present");
      assertTrue(result.contains("\"phase2\":\"two\""), "Newly inserted field should be present");
    }
  }

  /**
   * Test 4: Create 5 revisions (insert, modify, delete, insert, modify) and verify each.
   */
  @Test
  void testFiveRevisionWorkflow() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {

      // Revision 1: Insert initial document
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"counter\":1,\"label\":\"start\"}"), JsonNodeTrx.Commit.NO);
      wtx.commit();
      assertEquals(1, session.getMostRecentRevisionNumber());

      // Revision 2: Modify the counter value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "counter" key
      wtx.moveToFirstChild(); // number value 1
      wtx.setNumberValue(2);
      wtx.commit();
      assertEquals(2, session.getMostRecentRevisionNumber());

      // Revision 3: Delete the "label" field (key + value)
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "counter" key
      wtx.moveToRightSibling(); // "label" key
      assertTrue(wtx.isObjectKey());
      wtx.remove();
      wtx.commit();
      assertEquals(3, session.getMostRecentRevisionNumber());

      // Revision 4: Insert a new field "status"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.insertObjectRecordAsFirstChild("status", new StringValue("active"));
      wtx.commit();
      assertEquals(4, session.getMostRecentRevisionNumber());

      // Revision 5: Modify "status" value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "status" key (just inserted as first child)
      wtx.moveToFirstChild(); // "active" value
      wtx.setStringValue("completed");
      wtx.commit();
      assertEquals(5, session.getMostRecentRevisionNumber());
    }

    // Verify revision 1
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx(1);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer, 1).build();
      serializer.call();
      JSONAssert.assertEquals("{\"counter\":1,\"label\":\"start\"}", writer.toString(), true);
    }

    // Verify revision 5 (latest)
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      final String latest = writer.toString();
      assertTrue(latest.contains("\"status\":\"completed\""), "Latest revision should have updated status");
      assertTrue(latest.contains("\"counter\":2"), "Counter should be 2");
      assertFalse(latest.contains("\"label\""), "Label should have been deleted");
    }
  }

  /**
   * Test 5: Shred complex JSON and navigate to specific nodes verifying values.
   */
  @Test
  void testShredComplexJsonAndNavigate() throws Exception {
    final String complexJson = """
        {
          "users": [
            {"name": "Alice", "age": 30, "admin": true},
            {"name": "Bob", "age": 25, "admin": false}
          ],
          "metadata": {"version": 2, "description": "test data"}
        }
        """;

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(complexJson), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      // Navigate: documentRoot -> object
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isObject());

      // Navigate: object -> "users" key
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isObjectKey());
      assertEquals("users", rtx.getName().getLocalName());

      // Navigate: "users" key -> array
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isArray());

      // Navigate: array -> first user object
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isObject());

      // Navigate: first user object -> "name" key -> "Alice" value
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isObjectKey());
      assertEquals("name", rtx.getName().getLocalName());
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isStringValue());
      assertEquals("Alice", rtx.getValue());

      // Navigate back and to the sibling to find "age"
      rtx.moveToParent(); // back to "name" key
      assertTrue(rtx.moveToRightSibling()); // "age" key
      assertEquals("age", rtx.getName().getLocalName());
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isNumberValue());
      assertEquals(30, rtx.getNumberValue().intValue());
    }
  }

  /**
   * Test 6: Insert subtree, modify a value, remove a node, and verify resulting structure.
   */
  @Test
  void testInsertModifyRemoveVerifyStructure() throws Exception {
    final String json = "[\"first\",\"second\",\"third\"]";

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      wtx.commit();

      // Modify "second" to "modified"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // "first"
      wtx.moveToRightSibling(); // "second"
      assertEquals("second", wtx.getValue());
      wtx.setStringValue("modified");

      // Remove "third"
      wtx.moveToRightSibling(); // "third"
      assertEquals("third", wtx.getValue());
      wtx.remove();

      wtx.commit();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[\"first\",\"modified\"]", writer.toString());
    }
  }

  /**
   * Test 7: Enable DeweyIDs, create 3 revisions, and verify IDs are assigned.
   */
  @Test
  void testDeweyIDsAcrossRevisions() throws Exception {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Revision 1
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"key1\":\"val1\"}"), JsonNodeTrx.Commit.NO);
      wtx.commit();

      // Revision 2
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("key2", new StringValue("val2"));
      wtx.commit();

      // Revision 3
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("key3", new StringValue("val3"));
      wtx.commit();
    }

    // Verify DeweyIDs are assigned on all nodes across revisions
    for (int rev = 1; rev <= 3; rev++) {
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = session.beginNodeReadOnlyTrx(rev)) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // root object

        final var axis = new DescendantAxis(rtx);
        int nodeCount = 0;
        while (axis.hasNext()) {
          axis.nextLong();
          assertNotNull(rtx.getDeweyID(), "DeweyID should be assigned for node " + rtx.getNodeKey()
              + " in revision " + rev);
          nodeCount++;
        }
        assertTrue(nodeCount > 0, "Revision " + rev + " should have descendant nodes");
      }
    }
  }

  /**
   * Test 8: Enable hashes, insert and modify data, verify hashes change after modification.
   */
  @Test
  void testHashesAcrossModifications() throws Exception {
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());

    final long hashAfterInsert;
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"data\":\"original\"}"), JsonNodeTrx.Commit.NO);
      wtx.commit();

      // Capture hash of root object after insert
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      hashAfterInsert = wtx.getHash();
      assertNotEquals(0L, hashAfterInsert, "Hash should be non-zero after insert");
    }

    final long hashAfterModify;
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Modify the value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "data" key
      wtx.moveToFirstChild(); // "original" value
      wtx.setStringValue("modified");
      wtx.commit();

      // Capture hash of root object after modification
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      hashAfterModify = wtx.getHash();
      assertNotEquals(0L, hashAfterModify, "Hash should be non-zero after modify");
    }

    assertNotEquals(hashAfterInsert, hashAfterModify,
        "Hash of root object should change after modifying a descendant value");
  }

  /**
   * Test 9: Insert data, modify it, and verify path summary is built and accessible.
   */
  @Test
  void testPathSummaryAfterModifications() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"user\":{\"name\":\"Alice\",\"email\":\"alice@test.com\"}}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      // Add more data under a new key
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("count", new NumberValue(42));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final PathSummaryReader pathSummary = session.openPathSummary()) {
      // Path summary should exist and have entries; start from root to traverse all nodes
      int pathNodeCount = 0;
      final var axis = new DescendantAxis(pathSummary);
      while (axis.hasNext()) {
        axis.nextLong();
        pathNodeCount++;
      }
      // We expect path nodes for: user, name, email, count (at minimum)
      assertTrue(pathNodeCount >= 4,
          "Path summary should contain at least 4 path nodes, found " + pathNodeCount);
    }
  }

  /**
   * Test 10: Use auto-commit (insertSubtreeAsFirstChild with Commit.IMPLICIT) and verify
   * multiple implicit commits produce multiple revisions.
   */
  @Test
  void testAutoCommitWorkflow() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // First insert with implicit commit (default behavior)
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"a\"]"));
      // After implicit commit, revision should be 1
      assertEquals(1, session.getMostRecentRevisionNumber());

      // Add another element with a new implicit commit
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // "a"
      wtx.insertStringValueAsRightSibling("b");
      wtx.commit();
      assertEquals(2, session.getMostRecentRevisionNumber());
    }

    // Verify revision 1 has only ["a"]
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer1 = new StringWriter()) {
      final var serializer1 = new JsonSerializer.Builder(session, writer1, 1).build();
      serializer1.call();
      assertEquals("[\"a\"]", writer1.toString());
    }

    // Verify revision 2 has ["a","b"]
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer2 = new StringWriter()) {
      final var serializer2 = new JsonSerializer.Builder(session, writer2, 2).build();
      serializer2.call();
      assertEquals("[\"a\",\"b\"]", writer2.toString());
    }
  }

  /**
   * Test 11: Full CRUD (Create, Read, Update, Delete) cycle on a JSON document.
   */
  @Test
  void testFullCrudWorkflow() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());

    // CREATE
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"id\":1,\"title\":\"todo\",\"done\":false}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();
    }

    // READ
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.isObject());

      rtx.moveToFirstChild(); // "id" key
      assertEquals("id", rtx.getName().getLocalName());
      rtx.moveToFirstChild(); // value 1
      assertTrue(rtx.isNumberValue());
      assertEquals(1, rtx.getNumberValue().intValue());
    }

    // UPDATE
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Update "title" value from "todo" to "done task"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "id" key
      wtx.moveToRightSibling(); // "title" key
      assertEquals("title", wtx.getName().getLocalName());
      wtx.moveToFirstChild(); // "todo" value
      wtx.setStringValue("done task");

      // Update "done" from false to true
      wtx.moveToParent(); // "title" key
      wtx.moveToRightSibling(); // "done" key
      assertEquals("done", wtx.getName().getLocalName());
      wtx.moveToFirstChild(); // false value
      wtx.setBooleanValue(true);
      wtx.commit();
    }

    // Verify UPDATE
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      JSONAssert.assertEquals("{\"id\":1,\"title\":\"done task\",\"done\":true}", writer.toString(), true);
    }

    // DELETE: Remove the "done" field
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // "id" key
      wtx.moveToRightSibling(); // "title" key
      wtx.moveToRightSibling(); // "done" key
      assertEquals("done", wtx.getName().getLocalName());
      wtx.remove();
      wtx.commit();
    }

    // Verify DELETE
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      JSONAssert.assertEquals("{\"id\":1,\"title\":\"done task\"}", writer.toString(), true);
    }
  }

  /**
   * Test 12: Build a complex nested document programmatically (not via shredder),
   * modify parts, and serialize to verify correctness.
   */
  @Test
  void testComplexNestedDocumentWorkflow() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Build: { "config": { "debug": false, "version": 3 }, "items": [10, 20], "note": null }
      wtx.insertObjectAsFirstChild();
      // cursor: root object

      // "config" -> { "debug": false, "version": 3 }
      wtx.insertObjectRecordAsFirstChild("config", new ObjectValue());
      // cursor: inner object (value of "config" key)

      wtx.insertObjectRecordAsFirstChild("debug", new BooleanValue(false));
      // cursor: boolean value (false), child of "debug" key

      // Move up to "debug" key, then insert "version" as right sibling
      wtx.moveToParent(); // cursor: "debug" key
      wtx.insertObjectRecordAsRightSibling("version", new NumberValue(3));
      // cursor: number value (3), child of "version" key

      // Navigate back to root object to add "items"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // cursor: root object

      // "items" -> [10, 20] - insert as last child of root object
      wtx.insertObjectRecordAsLastChild("items", new ArrayValue());
      // cursor: array (value of "items" key)

      wtx.insertNumberValueAsFirstChild(10);
      // cursor: number value 10
      wtx.insertNumberValueAsRightSibling(20);
      // cursor: number value 20

      // Navigate back to root object to add "note"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // cursor: root object

      wtx.insertObjectRecordAsLastChild("note", new NullValue());
      // cursor: null value (child of "note" key)

      wtx.commit();
    }

    // Verify initial structure
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      final String result = writer.toString();
      JSONAssert.assertEquals(
          "{\"config\":{\"debug\":false,\"version\":3},\"items\":[10,20],\"note\":null}",
          result, true);
    }

    // Modify: change "debug" to true, change first item from 10 to 99
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // Change debug to true
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // "config" key
      wtx.moveToFirstChild(); // config object
      wtx.moveToFirstChild(); // "debug" key
      wtx.moveToFirstChild(); // false value
      assertTrue(wtx.isBooleanValue());
      wtx.setBooleanValue(true);

      // Change first array item from 10 to 99
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // "config" key
      wtx.moveToRightSibling(); // "items" key
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 10
      assertTrue(wtx.isNumberValue());
      assertEquals(10, wtx.getNumberValue().intValue());
      wtx.setNumberValue(99);

      wtx.commit();
    }

    // Verify modifications
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      JSONAssert.assertEquals(
          "{\"config\":{\"debug\":true,\"version\":3},\"items\":[99,20],\"note\":null}",
          writer.toString(), true);
    }
  }
}

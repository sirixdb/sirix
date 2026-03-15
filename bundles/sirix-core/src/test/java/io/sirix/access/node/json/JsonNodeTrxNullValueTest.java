package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for null value insertion, querying, removal, and serialization
 * via {@link io.sirix.api.json.JsonNodeTrx}.
 */
public final class JsonNodeTrxNullValueTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testInsertNullValueAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNullValueAsFirstChild();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[null]", writer.toString());
    }
  }

  @Test
  void testInsertNullValueAsLastChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("first");
      wtx.moveToParent();
      wtx.insertNullValueAsLastChild();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[\"first\",null]", writer.toString());
    }
  }

  @Test
  void testInsertNullValueAsLeftSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("right");
      wtx.insertNullValueAsLeftSibling();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[null,\"right\"]", writer.toString());
    }
  }

  @Test
  void testInsertNullValueAsRightSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("left");
      wtx.insertNullValueAsRightSibling();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[\"left\",null]", writer.toString());
    }
  }

  @Test
  void testObjectRecordWithNullValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertObjectAsFirstChild();
      wtx.insertObjectRecordAsFirstChild("key", new NullValue());
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("{\"key\":null}", writer.toString());
    }
  }

  @Test
  void testMixedArrayWithNulls() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,null,\"hello\",null,true]"));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[1,null,\"hello\",null,true]", writer.toString());
    }
  }

  @Test
  void testConsecutiveNulls() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNullValueAsFirstChild();
      wtx.insertNullValueAsRightSibling();
      wtx.insertNullValueAsRightSibling();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[null,null,null]", writer.toString());
    }
  }

  @Test
  void testRemoveNull() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("keep");
      wtx.insertNullValueAsRightSibling();
      final long nullNodeKey = wtx.getNodeKey();

      // Remove the null node
      wtx.moveTo(nullNodeKey);
      wtx.remove();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[\"keep\"]", writer.toString());
    }
  }

  @Test
  void testReplaceStringWithNull() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("to-replace");
      final long stringNodeKey = wtx.getNodeKey();

      // Insert null as right sibling, then remove the original string
      wtx.insertNullValueAsRightSibling();
      wtx.moveTo(stringNodeKey);
      wtx.remove();
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[null]", writer.toString());
    }
  }

  @Test
  void testIsNullValueCheck() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNullValueAsFirstChild();
      assertTrue(wtx.isNullValue(), "Null value node should return true for isNullValue()");

      // Move to the array node and check it returns false
      wtx.moveToParent();
      assertFalse(wtx.isNullValue(), "Array node should return false for isNullValue()");

      // Insert a string sibling and check
      wtx.insertStringValueAsLastChild("text");
      assertFalse(wtx.isNullValue(), "String node should return false for isNullValue()");

      wtx.commit();
    }
  }

  @Test
  void testNullValueNodeKind() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNullValueAsFirstChild();
      assertEquals(NodeKind.NULL_VALUE, wtx.getKind(), "Null value node should have NULL_VALUE kind");

      wtx.moveToParent();
      assertEquals(NodeKind.ARRAY, wtx.getKind(), "Array node should have ARRAY kind");

      wtx.commit();
    }
  }

  @Test
  void testNullValueRoundTrip() throws IOException {
    // Insert, commit, close, reopen, serialize, verify
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("before");
      wtx.insertNullValueAsRightSibling();
      wtx.insertStringValueAsRightSibling("after");
      wtx.commit();
    }

    // Reopen and serialize
    JsonTestHelper.closeEverything();
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[\"before\",null,\"after\"]", writer.toString());
    }
  }

  @Test
  void testNestedNulls() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"a\":null,\"b\":{\"c\":null}}"));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("{\"a\":null,\"b\":{\"c\":null}}", writer.toString());
    }
  }

  @Test
  void testNullValueViaShredder() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[null]"));

      // Navigate to the null value node (first child of root array)
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // null value
      assertTrue(wtx.isNullValue(), "Shredded null should be a null value node");
      assertEquals(NodeKind.NULL_VALUE, wtx.getKind());

      wtx.commit();
    }
  }

  @Test
  void testNullInComplexDocument() throws IOException {
    final var json = """
        {"users":[{"name":"Alice","age":30,"address":null},{"name":null,"age":null,"address":{"city":"Berlin","zip":null}}],"metadata":null}""";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx();
         final var writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals(json, writer.toString());
    }
  }
}

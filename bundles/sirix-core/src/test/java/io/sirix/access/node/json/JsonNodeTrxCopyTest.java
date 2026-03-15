package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonNodeTrx#copySubtreeAsFirstChild(JsonNodeReadOnlyTrx)},
 * {@link JsonNodeTrx#copySubtreeAsLeftSibling(JsonNodeReadOnlyTrx)}, and
 * {@link JsonNodeTrx#copySubtreeAsRightSibling(JsonNodeReadOnlyTrx)}.
 */
public final class JsonNodeTrxCopyTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testCopySubtreeAsFirstChild() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert an array with one element: [42]
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[42]"));
      wtx.commit();

      // Open read-only trx on the number value node (42)
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // 42

        // Position wtx at the array node, copy the number as first child
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);

        // Verify: array now has two children [42, 42]
        wtx.moveTo(1); // array
        wtx.moveToFirstChild();
        assertEquals(NodeKind.NUMBER_VALUE, wtx.getKind());
        assertEquals(42, wtx.getNumberValue().intValue());

        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling();
        assertEquals(NodeKind.NUMBER_VALUE, wtx.getKind());
        assertEquals(42, wtx.getNumberValue().intValue());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopySubtreeAsLeftSibling() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert: ["first", "second"]
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"first\",\"second\"]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at "first" (first child of array)
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "first"

        // Position wtx at "second" (right sibling of "first")
        wtx.moveTo(1); // array
        wtx.moveToFirstChild(); // "first"
        wtx.moveToRightSibling(); // "second"
        final long secondNodeKey = wtx.getNodeKey();

        wtx.copySubtreeAsLeftSibling(rtx);

        // After copySubtreeAsLeftSibling, verify the array has 3 children: "first", "first"(copy), "second"
        wtx.moveTo(1); // array
        wtx.moveToFirstChild();
        assertEquals("first", wtx.getValue());

        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling();
        assertEquals("first", wtx.getValue());

        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling();
        assertEquals("second", wtx.getValue());
        assertEquals(secondNodeKey, wtx.getNodeKey());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopySubtreeAsRightSibling() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert: ["alpha", "beta"]
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"alpha\",\"beta\"]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at "beta"
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "alpha"
        rtx.moveToRightSibling(); // "beta"

        // Position wtx at "alpha" and copy "beta" as its right sibling
        wtx.moveTo(1); // array
        wtx.moveToFirstChild(); // "alpha"

        wtx.copySubtreeAsRightSibling(rtx);

        // After copy, verify: ["alpha", "beta"(copy), "beta"(original)]
        wtx.moveTo(1); // array
        wtx.moveToFirstChild();
        assertEquals("alpha", wtx.getValue());

        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling();
        assertEquals("beta", wtx.getValue());

        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling();
        assertEquals("beta", wtx.getValue());

        assertFalse(wtx.hasRightSibling());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopyNestedObjectSubtree() {
    final String json = "{\"a\":{\"b\":{\"c\":1}}}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + json + "]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the nested object (first child of array = the object)
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // object {"a":{"b":{"c":1}}}

        // Position wtx at the array, copy the object as first child
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();

        // Serialize and verify: the array now has two copies of the nested object
        final var writer = new StringWriter();
        final var serializer = JsonSerializer.newBuilder(session, writer).build();
        serializer.call();

        final String result = writer.toString();
        assertEquals("[" + json + "," + json + "]", result);
      }
    }
  }

  @Test
  void testCopyPreservesValueTypes() {
    final String json = "[\"hello\",42,true,null]";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert two arrays: the source and a target
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[[],\"hello\",42,true,null]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Copy "hello" (string)
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // outer array
        rtx.moveToFirstChild(); // inner empty array []
        rtx.moveToRightSibling(); // "hello"
        final long helloKey = rtx.getNodeKey();

        wtx.moveTo(1); // outer array
        wtx.moveToFirstChild(); // inner empty array []
        wtx.copySubtreeAsFirstChild(rtx);

        // Copy 42 (number)
        rtx.moveTo(helloKey);
        rtx.moveToRightSibling(); // 42
        final long numKey = rtx.getNodeKey();

        wtx.moveTo(1);
        wtx.moveToFirstChild(); // inner array
        wtx.moveToFirstChild(); // the copied "hello"
        wtx.copySubtreeAsRightSibling(rtx);

        // Copy true (boolean)
        rtx.moveTo(numKey);
        rtx.moveToRightSibling(); // true
        final long boolKey = rtx.getNodeKey();

        wtx.moveToRightSibling(); // 42 copy
        wtx.copySubtreeAsRightSibling(rtx);

        // Copy null
        rtx.moveTo(boolKey);
        rtx.moveToRightSibling(); // null

        wtx.moveToRightSibling(); // true copy
        wtx.copySubtreeAsRightSibling(rtx);

        // Now verify the inner array has the right types
        wtx.moveTo(1);
        wtx.moveToFirstChild(); // inner array
        wtx.moveToFirstChild(); // "hello" copy
        assertEquals(NodeKind.STRING_VALUE, wtx.getKind());
        assertEquals("hello", wtx.getValue());

        wtx.moveToRightSibling(); // 42 copy
        assertEquals(NodeKind.NUMBER_VALUE, wtx.getKind());
        assertEquals(42, wtx.getNumberValue().intValue());

        wtx.moveToRightSibling(); // true copy
        assertEquals(NodeKind.BOOLEAN_VALUE, wtx.getKind());
        assertTrue(wtx.getBooleanValue());

        wtx.moveToRightSibling(); // null copy
        assertEquals(NodeKind.NULL_VALUE, wtx.getKind());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopyPreservesObjectKeyNames() {
    final String json = "{\"name\":\"Alice\",\"age\":30}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + json + "]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // object

        // Copy object as first child of the array
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);

        // Verify the copied object's keys
        // The newly copied object is now the first child of the array
        wtx.moveTo(1); // array
        wtx.moveToFirstChild(); // copied object

        assertEquals(NodeKind.OBJECT, wtx.getKind());
        wtx.moveToFirstChild(); // first key "name"
        assertEquals(NodeKind.OBJECT_KEY, wtx.getKind());
        assertEquals("name", wtx.getName().getLocalName());

        wtx.moveToFirstChild(); // "Alice"
        assertEquals("Alice", wtx.getValue());

        wtx.moveToParent(); // back to "name" key
        wtx.moveToRightSibling(); // "age" key
        assertEquals(NodeKind.OBJECT_KEY, wtx.getKind());
        assertEquals("age", wtx.getName().getLocalName());

        wtx.moveToFirstChild(); // 30
        assertEquals(30, wtx.getNumberValue().intValue());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopyOriginalUnmodified() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"original\",\"target\"]"));
      wtx.commit();

      // Serialize original state
      final var originalWriter = new StringWriter();
      JsonSerializer.newBuilder(session, originalWriter).build().call();
      final String originalJson = originalWriter.toString();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at "original"
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "original"

        // Copy "original" as right sibling of "target"
        wtx.moveTo(1); // array
        wtx.moveToFirstChild(); // "original"
        wtx.moveToRightSibling(); // "target"
        wtx.copySubtreeAsRightSibling(rtx);

        // Verify the original nodes are still intact via the read-only trx
        // The rtx is on revision 1, so it should be completely unaffected
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "original"
        assertEquals("original", rtx.getValue());
        assertTrue(rtx.hasRightSibling());
        rtx.moveToRightSibling();
        assertEquals("target", rtx.getValue());
        assertFalse(rtx.hasRightSibling());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopyEmptyObject() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{}]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the empty object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // {}

        // Copy as first child of array
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();

        final var writer = new StringWriter();
        JsonSerializer.newBuilder(session, writer).build().call();
        assertEquals("[{},{}]", writer.toString());
      }
    }
  }

  @Test
  void testCopyEmptyArray() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[[]]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the empty inner array
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // outer array
        rtx.moveToFirstChild(); // inner []

        // Copy as first child of outer array
        wtx.moveTo(1); // outer array
        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();

        final var writer = new StringWriter();
        JsonSerializer.newBuilder(session, writer).build().call();
        assertEquals("[[],[]]", writer.toString());
      }
    }
  }

  @Test
  void testCopyDeepNesting() {
    // 5-level deep structure: {"a":{"b":{"c":{"d":{"e":"deep"}}}}}
    final String deepJson = "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"deep\"}}}}}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + deepJson + "]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the deep object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // object

        // Copy the deep object as first child of the array
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();

        final var writer = new StringWriter();
        JsonSerializer.newBuilder(session, writer).build().call();
        assertEquals("[" + deepJson + "," + deepJson + "]", writer.toString());
      }
    }
  }

  @Test
  void testCopyBetweenResources() {
    final String secondResource = "second-resource";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      // Create the second resource
      database.createResource(ResourceConfiguration.newBuilder(secondResource).build());

      // Populate the first resource with source data
      try (final var session1 = database.beginResourceSession(RESOURCE);
           final var wtx1 = session1.beginNodeTrx()) {
        wtx1.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"source\":\"data\"}"));
        wtx1.commit();
      }

      // Populate the second resource with a target array
      try (final var session2 = database.beginResourceSession(secondResource);
           final var wtx2 = session2.beginNodeTrx()) {
        wtx2.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
        wtx2.commit();
      }

      // Now copy from first resource to second resource
      try (final var session1 = database.beginResourceSession(RESOURCE);
           final var rtx = session1.beginNodeReadOnlyTrx();
           final var session2 = database.beginResourceSession(secondResource);
           final var wtx = session2.beginNodeTrx()) {

        // Position rtx at the object in resource 1
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // the object {"source":"data"}

        // Position wtx at the empty array in resource 2
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // []

        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();

        final var writer = new StringWriter();
        JsonSerializer.newBuilder(session2, writer).build().call();
        assertEquals("[{\"source\":\"data\"}]", writer.toString());
      }
    }
  }

  @Test
  void testCopyAndSerialize() {
    final String sourceJson = "{\"items\":[1,2,3],\"active\":true}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert array with the source object
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + sourceJson + "]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // object

        // Copy as right sibling of itself
        wtx.moveTo(1); // array
        wtx.moveToFirstChild(); // the original object
        wtx.copySubtreeAsRightSibling(rtx);
        wtx.commit();

        final var writer = new StringWriter();
        JsonSerializer.newBuilder(session, writer).build().call();
        assertEquals("[" + sourceJson + "," + sourceJson + "]", writer.toString());
      }
    }
  }

  @Test
  void testCopySubtreeStructuralIntegrity() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Create: ["existing", {"key":"value"}]
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("[\"existing\",{\"key\":\"value\"}]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "existing"
        rtx.moveToRightSibling(); // object

        // Position wtx at the array, copy the object as first child
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);

        // Now array has 3 children: copied-object, "existing", original-object
        // Verify the copied object's parent link
        wtx.moveTo(1); // array
        final long arrayKey = wtx.getNodeKey();
        wtx.moveToFirstChild(); // copied object
        final long copiedObjKey = wtx.getNodeKey();
        assertEquals(arrayKey, wtx.getParentKey());
        assertEquals(NodeKind.OBJECT, wtx.getKind());

        // Verify right sibling link
        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling(); // "existing"
        final long existingKey = wtx.getNodeKey();
        assertEquals("existing", wtx.getValue());

        // Verify left sibling link of "existing" points to copied object
        assertTrue(wtx.hasLeftSibling());
        assertEquals(copiedObjKey, wtx.getLeftSiblingKey());

        // Verify "existing"'s right sibling is the original object
        assertTrue(wtx.hasRightSibling());
        wtx.moveToRightSibling(); // original object
        assertEquals(NodeKind.OBJECT, wtx.getKind());
        assertEquals(existingKey, wtx.getLeftSiblingKey());
        assertFalse(wtx.hasRightSibling());
        assertEquals(arrayKey, wtx.getParentKey());

        // Verify the copied object's internal structure: key "key" -> value "value"
        wtx.moveTo(copiedObjKey);
        wtx.moveToFirstChild(); // "key" object key
        assertEquals(NodeKind.OBJECT_KEY, wtx.getKind());
        assertEquals("key", wtx.getName().getLocalName());
        assertEquals(copiedObjKey, wtx.getParentKey());

        wtx.moveToFirstChild(); // "value"
        assertEquals(NodeKind.OBJECT_STRING_VALUE, wtx.getKind());
        assertEquals("value", wtx.getValue());
      }

      wtx.commit();
    }
  }

  @Test
  void testCopyAndCommitReadBack() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Insert source: [{"x":10}]
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"x\":10}]"));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        // Position rtx at the object
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // object {"x":10}

        // Copy the object as first child of the array
        wtx.moveTo(1); // array
        wtx.copySubtreeAsFirstChild(rtx);
        wtx.commit();
      }
    }

    // Reopen the database and verify persistence
    JsonTestHelper.closeEverything();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Serialize and verify
      final var writer = new StringWriter();
      JsonSerializer.newBuilder(session, writer).build().call();
      assertEquals("[{\"x\":10},{\"x\":10}]", writer.toString());

      // Also verify via read-only trx traversal
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        assertEquals(NodeKind.ARRAY, rtx.getKind());

        rtx.moveToFirstChild(); // first object (the copy)
        assertEquals(NodeKind.OBJECT, rtx.getKind());

        rtx.moveToFirstChild(); // "x" key
        assertEquals(NodeKind.OBJECT_KEY, rtx.getKind());
        assertEquals("x", rtx.getName().getLocalName());

        rtx.moveToFirstChild(); // 10
        assertEquals(10, rtx.getNumberValue().intValue());

        rtx.moveToParent(); // back to "x" key
        rtx.moveToParent(); // back to first object
        rtx.moveToRightSibling(); // second object (original)
        assertEquals(NodeKind.OBJECT, rtx.getKind());

        rtx.moveToFirstChild(); // "x" key
        rtx.moveToFirstChild(); // 10
        assertEquals(10, rtx.getNumberValue().intValue());
      }
    }
  }
}

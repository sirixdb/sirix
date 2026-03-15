package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.exception.SirixUsageException;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for error handling and validation of improper API usage in JSON transactions.
 */
public final class JsonErrorHandlingTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testInsertAfterClosedTransaction() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var wtx = session.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();
      wtx.close();

      assertThrows(IllegalStateException.class,
          () -> wtx.insertStringValueAsFirstChild("test"));
    }
  }

  @Test
  void testInsertStringChildOfStringValue() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"hello\"]"));
      wtx.commit();

      // Position on the string value node inside the array
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // string value "hello"
      assertEquals(NodeKind.STRING_VALUE, wtx.getKind());

      // String values are leaf nodes and cannot have children
      assertThrows(SirixUsageException.class,
          () -> wtx.insertStringValueAsFirstChild("child"));
    }
  }

  @Test
  void testInsertSiblingOfDocumentRoot() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3]"));
      wtx.commit();

      // Move to the first child of the document root (the array)
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();

      // The parent of this node is the document root, not an array,
      // so inserting a sibling should fail.
      // Note: checkAccessAndCommit() increments mod count before the parent kind check,
      // so we must rollback to clean the dirty state before close().
      assertThrows(SirixUsageException.class, wtx::insertObjectAsRightSibling);
      wtx.rollback();
    }
  }

  @Test
  void testRemoveDocumentRoot() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();

      wtx.moveToDocumentRoot();
      assertEquals(NodeKind.JSON_DOCUMENT, wtx.getKind());

      // Note: remove() calls checkAccessAndCommit() before the node kind check,
      // so we must rollback to clean the dirty state before close().
      assertThrows(SirixUsageException.class, wtx::remove);
      wtx.rollback();
    }
  }

  @Test
  void testSetStringValueOnNonStringNode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();

      // Position on the object node
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      assertEquals(NodeKind.OBJECT, wtx.getKind());

      assertThrows(SirixUsageException.class,
          () -> wtx.setStringValue("newValue"));
    }
  }

  @Test
  void testSetNumberValueOnNonNumberNode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"hello\"]"));
      wtx.commit();

      // Position on the string value node
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // string value
      assertEquals(NodeKind.STRING_VALUE, wtx.getKind());

      assertThrows(SirixUsageException.class,
          () -> wtx.setNumberValue(42));
    }
  }

  @Test
  void testSetBooleanValueOnNonBooleanNode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"hello\"]"));
      wtx.commit();

      // Position on the string value node
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // string value
      assertEquals(NodeKind.STRING_VALUE, wtx.getKind());

      assertThrows(SirixUsageException.class,
          () -> wtx.setBooleanValue(true));
    }
  }

  @Test
  void testSetObjectKeyNameOnNonObjectKey() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3]"));
      wtx.commit();

      // Position on the array node
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      assertEquals(NodeKind.ARRAY, wtx.getKind());

      assertThrows(SirixUsageException.class,
          () -> wtx.setObjectKeyName("newName"));
    }
  }

  @Test
  void testMoveToNonExistentNode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();

      // moveTo with a non-existent key should return false, not throw
      final boolean result = wtx.moveTo(999999);
      assertFalse(result);
    }
  }

  @Test
  void testMoveToFirstChildOfLeafNode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"leaf\"]"));
      wtx.commit();

      // Position on the string value (a leaf node)
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // string value "leaf"
      assertEquals(NodeKind.STRING_VALUE, wtx.getKind());

      // moveToFirstChild on a leaf should return false
      final boolean result = wtx.moveToFirstChild();
      assertFalse(result);
    }
  }

  @Test
  void testInsertObjectRecordOutsideObject() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3]"));
      wtx.commit();

      // Position on the array node (not an object)
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      assertEquals(NodeKind.ARRAY, wtx.getKind());

      assertThrows(SirixUsageException.class,
          () -> wtx.insertObjectRecordAsFirstChild("key", new StringValue("val")));
    }
  }

  @Test
  void testMalformedJsonShredding() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Malformed JSON should throw during shredding.
      // The shredder may partially modify the transaction before failing,
      // so we must rollback to clean the dirty state before close().
      assertThrows(Exception.class,
          () -> wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{invalid json!!}")));
      wtx.rollback();
    }
  }

  @Test
  void testReadAfterRemove() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"removeMe\"]"));
      wtx.commit();

      // Position on the string value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // string value "removeMe"
      assertEquals(NodeKind.STRING_VALUE, wtx.getKind());
      assertEquals("removeMe", wtx.getValue());

      // Remove the node
      wtx.remove();

      // After removal, the cursor moves to parent (the array) since there are no siblings.
      // The value should be empty string for a non-value node.
      assertEquals(NodeKind.ARRAY, wtx.getKind());
      assertEquals("", wtx.getValue());

      // Commit the removal so the transaction can be closed cleanly.
      wtx.commit();
    }
  }

  @Test
  void testDoubleCommit() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();

      // Committing again without changes should not throw
      assertDoesNotThrow(() -> { wtx.commit(); });
    }
  }
}

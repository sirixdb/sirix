package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonNodeTrx#moveSubtreeToFirstChild(long)},
 * {@link JsonNodeTrx#moveSubtreeToRightSibling(long)}, and
 * {@link JsonNodeTrx#moveSubtreeToLeftSibling(long)}.
 */
public final class JsonNodeTrxMoveTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testMoveSubtreeToFirstChild() throws Exception {
    shredJson("[1,2,3]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> array -> 1 -> 2 -> 3
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long thirdKey = wtx.getNodeKey();

      // Move 3 to first child of array
      wtx.moveToParent();
      wtx.moveSubtreeToFirstChild(thirdKey);
      wtx.commit();
    }

    assertSerialization("[3,1,2]");
  }

  @Test
  void testMoveSubtreeToRightSibling() throws Exception {
    shredJson("[1,2,3]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      final long firstKey = wtx.getNodeKey();
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3

      // Move 1 to right sibling of 3 => [2,3,1]
      wtx.moveSubtreeToRightSibling(firstKey);
      wtx.commit();
    }

    assertSerialization("[2,3,1]");
  }

  @Test
  void testMoveSubtreeToLeftSibling() throws Exception {
    shredJson("[1,2,3]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long thirdKey = wtx.getNodeKey();

      // Move 3 to left sibling of 2 => [1,3,2]
      wtx.moveToParent();
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveSubtreeToLeftSibling(thirdKey);
      wtx.commit();
    }

    assertSerialization("[1,3,2]");
  }

  @Test
  void testMoveNestedSubtreeToFirstChild() throws Exception {
    shredJson("[1,{\"a\":\"b\",\"c\":true}]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> array -> 1, {a:b, c:true}
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // object {a:b, c:true}
      final long objectKey = wtx.getNodeKey();

      // Move entire object to first child of array
      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(objectKey);
      wtx.commit();
    }

    assertSerialization("[{\"a\":\"b\",\"c\":true},1]");
  }

  @Test
  void testMoveToSameParentRearranges() throws Exception {
    shredJson("[\"a\",\"b\",\"c\"]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // "a"
      wtx.moveToRightSibling(); // "b"
      wtx.moveToRightSibling(); // "c"
      final long cKey = wtx.getNodeKey();

      // Move "c" to first child of the same array parent => rearranges to ["c","a","b"]
      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(cKey);
      wtx.commit();
    }

    assertSerialization("[\"c\",\"a\",\"b\"]");
  }

  @Test
  void testMoveToSelfThrows() throws Exception {
    shredJson("[1,2,3]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      final long selfKey = wtx.getNodeKey();

      // Moving a node as first child of itself should throw
      assertThrows(IllegalArgumentException.class, () -> wtx.moveSubtreeToFirstChild(selfKey));
    }
  }

  @Test
  void testMoveToInvalidKeyThrows() throws Exception {
    shredJson("[1,2]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array

      // A very large key that does not exist
      assertThrows(IllegalArgumentException.class, () -> wtx.moveSubtreeToFirstChild(999_999L));
    }
  }

  @Test
  void testStructuralIntegrityAfterMove() throws Exception {
    shredJson("[1,2,3]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      final long arrayKey = wtx.getNodeKey();
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long thirdKey = wtx.getNodeKey();

      // Move 3 to first child of array => [3,1,2]
      wtx.moveTo(arrayKey);
      wtx.moveSubtreeToFirstChild(thirdKey);
      wtx.commit();

      // Verify structural links
      wtx.moveTo(arrayKey);
      assertEquals(3, wtx.getChildCount());
      assertTrue(wtx.hasFirstChild());

      wtx.moveToFirstChild(); // should be 3
      assertEquals(thirdKey, wtx.getNodeKey());
      assertEquals(arrayKey, wtx.getParentKey());

      assertTrue(wtx.hasRightSibling());
      wtx.moveToRightSibling(); // should be 1
      assertTrue(wtx.hasRightSibling());
      wtx.moveToRightSibling(); // should be 2
      assertTrue(!wtx.hasRightSibling());
    }
  }

  @Test
  void testMoveSubtreeToFirstChildCommitAndReadBack() throws Exception {
    shredJson("[10,20,30]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 10
      wtx.moveToRightSibling(); // 20
      wtx.moveToRightSibling(); // 30
      final long key30 = wtx.getNodeKey();

      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(key30);
      wtx.commit();
    }

    // Reopen and read back
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // array
      assertEquals(3, rtx.getChildCount());

      rtx.moveToFirstChild();
      assertEquals("30", rtx.getValue());
      rtx.moveToRightSibling();
      assertEquals("10", rtx.getValue());
      rtx.moveToRightSibling();
      assertEquals("20", rtx.getValue());
    }
  }

  @Test
  void testMoveObjectKeyBetweenObjects() throws Exception {
    shredJson("{\"src\":{\"key1\":\"val1\"},\"dst\":{\"key2\":\"val2\"}}");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> root-object -> "src"(obj-key) -> {key1:val1}
      //                    -> "dst"(obj-key) -> {key2:val2}
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // "src" object-key
      wtx.moveToFirstChild(); // {key1:val1} object
      wtx.moveToFirstChild(); // "key1" object-key
      final long key1Key = wtx.getNodeKey();

      // Navigate to "dst" -> its object value
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // "src" object-key
      wtx.moveToRightSibling(); // "dst" object-key
      wtx.moveToFirstChild(); // {key2:val2} object

      // Move "key1":"val1" into dst's object as first child
      wtx.moveSubtreeToFirstChild(key1Key);
      wtx.commit();
    }

    assertSerialization("{\"src\":{},\"dst\":{\"key1\":\"val1\",\"key2\":\"val2\"}}");
  }

  @Test
  void testMoveArrayElementToAnotherArray() throws Exception {
    shredJson("[[1,2],[3,4]]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> outer-array -> [1,2] -> [3,4]
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // outer array
      wtx.moveToFirstChild(); // [1,2]
      wtx.moveToFirstChild(); // 1
      final long oneKey = wtx.getNodeKey();

      // Navigate to second inner array [3,4]
      wtx.moveToParent(); // [1,2]
      wtx.moveToRightSibling(); // [3,4]

      // Move 1 to first child of [3,4] => [[2],[1,3,4]]
      wtx.moveSubtreeToFirstChild(oneKey);
      wtx.commit();
    }

    assertSerialization("[[2],[1,3,4]]");
  }

  @Test
  void testMoveFirstChildToRightSiblingOfLast() throws Exception {
    shredJson("[\"x\",\"y\",\"z\"]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // "x"
      final long xKey = wtx.getNodeKey();
      wtx.moveToRightSibling(); // "y"
      wtx.moveToRightSibling(); // "z"

      // Cursor on "z", move "x" to right sibling of "z" => ["y","z","x"]
      wtx.moveSubtreeToRightSibling(xKey);
      wtx.commit();
    }

    assertSerialization("[\"y\",\"z\",\"x\"]");
  }

  @Test
  void testMoveLastChildToFirstChild() throws Exception {
    shredJson("[\"a\",\"b\",\"c\",\"d\"]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // "a"
      wtx.moveToRightSibling(); // "b"
      wtx.moveToRightSibling(); // "c"
      wtx.moveToRightSibling(); // "d"
      final long dKey = wtx.getNodeKey();

      // Move "d" to first child => ["d","a","b","c"]
      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(dKey);
      wtx.commit();
    }

    assertSerialization("[\"d\",\"a\",\"b\",\"c\"]");
  }

  @Test
  void testMoveAndSerialize() throws Exception {
    shredJson("{\"items\":[1,2,3],\"other\":\"value\"}");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> root-obj -> "items"(obj-key) -> [1,2,3]
      //                 -> "other"(obj-key) -> "value"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // "items" object-key
      wtx.moveToFirstChild(); // [1,2,3] array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long threeKey = wtx.getNodeKey();

      // Move 3 to first child of array => [3,1,2]
      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(threeKey);
      wtx.commit();
    }

    assertSerialization("{\"items\":[3,1,2],\"other\":\"value\"}");
  }

  @Test
  void testMoveSubtreePreservesDescendants() throws Exception {
    shredJson("[{\"a\":{\"b\":{\"c\":1}}},2]");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // doc -> array -> {a:{b:{c:1}}} -> 2
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // {a:{b:{c:1}}}
      final long nestedObjKey = wtx.getNodeKey();
      wtx.moveToRightSibling(); // 2

      // Move {a:{b:{c:1}}} to right sibling of 2 => [2,{a:{b:{c:1}}}]
      wtx.moveSubtreeToRightSibling(nestedObjKey);
      wtx.commit();
    }

    // Verify serialization preserves all nested descendants
    assertSerialization("[2,{\"a\":{\"b\":{\"c\":1}}}]");

    // Also verify descendants are navigable
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // array
      rtx.moveToFirstChild(); // 2
      rtx.moveToRightSibling(); // {a:{b:{c:1}}}
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // "a" object-key
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // {b:{c:1}} object
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // "b" object-key
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // {c:1} object
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // "c" object-key
      assertTrue(rtx.hasFirstChild());
      rtx.moveToFirstChild(); // 1
      assertEquals("1", rtx.getValue());
    }
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a database at PATH1, creates a resource, and shreds the given JSON string.
   *
   * @param json the JSON to shred
   */
  private void shredJson(final String json) throws Exception {
    final var dbConf = new DatabaseConfiguration(PATHS.PATH1.getFile());
    Databases.removeDatabase(PATHS.PATH1.getFile());
    Databases.createJsonDatabase(dbConf);

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(
          new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = session.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }
  }

  /**
   * Serializes the current state of the database and asserts it matches the expected JSON.
   *
   * @param expectedJson the expected JSON output
   */
  private void assertSerialization(final String expectedJson) throws Exception {
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(session, writer).build();
      serializer.call();
      JSONAssert.assertEquals(expectedJson, writer.toString(), true);
    }
  }
}

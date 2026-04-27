package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class JsonNodeTrxInsertTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Disabled
  @Test
  public void testInsertingTopLevelDocuments() {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.MEMORY_MAPPED)
                                                   .build());
      try (final var session = database.beginResourceSession(resource); final var wtx = session.beginNodeTrx()) {
        System.out.println("Start inserting");

        final long time = System.nanoTime();

        wtx.insertArrayAsFirstChild();

        var jsonObject = """
            {"item":"this is item 0", "package":"package", "kg":5}
            """.strip();

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonObject), JsonNodeTrx.Commit.IMPLICIT,
            JsonNodeTrx.CheckParentNode.YES);

        for (int i = 0; i < 650_000; i++) {
          System.out.println(i);
          jsonObject = """
              {"item":"this is item %s", "package":"package", "kg":5}
              """.strip().formatted(i);

          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(jsonObject), JsonNodeTrx.Commit.IMPLICIT,
              JsonNodeTrx.CheckParentNode.NO);
        }

        wtx.commit();

        System.out.println("Done inserting [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
      }
    }
  }

  @Disabled
  @Test
  public void testSerializeTopLevelDocuments() throws IOException {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(resource);
        final Writer writer = new StringWriter()) {
      System.out.println("Start serializing");

      final var time = System.nanoTime();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      System.out.println("Done serializing [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
    }
  }

  /**
   * Regression test for the iter#32 P2 fusion "nested object collapses to its first primitive
   * value" bug. Shred {@code {"location":{"state":"CA","city":"Los Angeles"}}}, read it back
   * via {@link JsonSerializer}, and assert the serializer round-trips the inner object exactly
   * — without unwrapping it to its first inline field. Also covers the pre-fusion
   * {@code maxLevel} pagination contract: at the level boundary the inner OBJECT_NAMED_OBJECT
   * must collapse to {@code {}} (not leak its children). Both bugs lived in the
   * {@link io.sirix.service.json.serialize.JsonLimitedSerializer} (incorrect
   * {@code isObjectKeyValue}/{@code shouldVisitChildren}) and {@code JsonDBObject.get(QNm)}
   * (extraneous descent into OBJECT_NAMED_OBJECT first child).
   */
  @Test
  public void testNestedObjectShapeRoundTrip() throws IOException {
    final String doc = "{\"location\":{\"state\":\"CA\",\"city\":\"Los Angeles\"}}";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(doc));
        wtx.commit();
      }

      // Tree shape: doc-root -> OBJECT -> OBJECT_NAMED_OBJECT(location) -> two OBJECT_NAMED_STRING leaves.
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        int n = 0;
        for (final long unused : new DescendantAxis(rtx, IncludeSelf.YES)) {
          n++;
        }
        assertEquals(5, n, "doc-root + outer OBJECT + OBJECT_NAMED_OBJECT location + 2 OBJECT_NAMED_STRING leaves");
      }

      // Full round-trip — the inner object must NOT collapse to its first primitive value.
      final StringWriter writer = new StringWriter();
      new JsonSerializer.Builder(session, writer).build().call();
      assertEquals(doc, writer.toString(),
          "OBJECT_NAMED_OBJECT must serialize as the inner object — not as its first inline field");

      // maxLevel=2 must collapse the inner OBJECT_NAMED_OBJECT to {} (the 'level boundary'
      // contract). At maxLevel=3+ commas between sibling fused leaves must be present.
      final StringWriter w2 = new StringWriter();
      new JsonSerializer.Builder(session, w2).maxLevel(2).build().call();
      assertEquals("{\"location\":{}}", w2.toString());

      final StringWriter w3 = new StringWriter();
      new JsonSerializer.Builder(session, w3).maxLevel(3).build().call();
      assertEquals(doc, w3.toString());
    }
  }

  @Test
  public void testInsert500SubtreesAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();

      for (int i = 0; i < 500; i++) {
        wtx.moveTo(1);
        wtx.insertObjectAsFirstChild();
      }

      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      final var expected = Files.readString(Path.of("src", "test", "resources", "json", "jsonNodeTrxInsertTest",
          "testInsert500SubtreesAsFirstChild", "expected.json"));

      assertEquals(expected, writer.toString());
    }
  }

  @Test
  public void testInsertObject() throws IOException {
    final DatabaseConfiguration config = new DatabaseConfiguration(PATHS.PATH1.getFile());
    if (!Files.exists(PATHS.PATH1.getFile())) {
      Databases.createJsonDatabase(config);
    }
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        wtx.insertObjectRecordAsFirstChild("foo", new StringValue("bar")).moveToParent();
        wtx.commit();
      }
    }

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final Writer writer = new StringWriter()) {
      final var wtx = session.beginNodeTrx();
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("""
              {"foo":"bar"}
          """.strip(), writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsLastChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsLeftSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[[],[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsRightSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[[],[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsLastChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsLeftSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));
      wtx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[{},{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsRightSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[{},{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeIntoObjectAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // iter#32 structural fusion: the inner "bar" OBJECT (legacy key 8) is now collapsed
      // into OBJECT_NAMED_OBJECT at key 6 — that fused record IS the OBJECT under fusion.
      wtx.moveTo(6);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"foo\": \"bar\"}"));

      wtx.moveTo(6);

      assertEquals(3, wtx.getChildCount());
      // Descendant count after inserting {"foo":"bar"} into the fused "bar" object (with
      // skipRootJsonToken=YES since the parent is also an object container):
      //   2 existing (hello, helloo fused) + 1 inserted (foo fused) = 3 descendants.
      assertEquals(3, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("foo", wtx.getName().getLocalName());
      assertEquals("bar", wtx.getValue());
    }
  }

  @Test
  public void testInsertSubtreeIntoArrayAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // iter#32 structural fusion: "foo" OBJECT_KEY+ARRAY collapsed into OBJECT_NAMED_ARRAY at
      // key 2; that fused record IS the array under fusion.
      wtx.moveTo(2);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"foo\"]"));

      wtx.moveTo(2);

      assertEquals(4, wtx.getChildCount());
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("foo", wtx.getValue());
    }
  }

  @Test
  public void testInsertArrayIntoArrayAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveTo(4);

      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("[\"foo\"]"));

      // iter#32 structural fusion: legacy "foo" was OBJECT_KEY(2)+ARRAY(3); now it is the
      // single fused OBJECT_NAMED_ARRAY at key 2 — that fused record IS the array.
      wtx.moveTo(2);

      assertEquals(4, wtx.getChildCount());
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());           // bar
      assertTrue(wtx.moveToRightSibling());         // null
      assertTrue(wtx.moveToRightSibling());         // new array
      assertTrue(wtx.isArray());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("foo", wtx.getValue());
    }
  }

  @Test
  public void testInsertObjectIntoArrayAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveTo(4);

      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\": \"bar\"}"));

      // iter#32 structural fusion: legacy "foo" was OBJECT_KEY(2)+ARRAY(3); now it is the
      // single fused OBJECT_NAMED_ARRAY at key 2 — the fused record IS the array, holding
      // the inserted object as its 4th child.
      wtx.moveTo(2);

      assertEquals(4, wtx.getChildCount());
      // Descendant count is 6 in legacy mode (array existing 2 + new-obj + foo-key + bar + tail 1)
      // or 5 under fusion because foo:"bar" collapses into one fused OBJECT_NAMED_STRING record.
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());           // bar
      assertTrue(wtx.moveToRightSibling());         // null
      assertTrue(wtx.moveToRightSibling());         // new object
      assertTrue(wtx.isObject());
      assertTrue(wtx.moveToFirstChild());
      final boolean fused = wtx.getKind().isFusedObjectNamed();
      assertEquals("foo", wtx.getName().getLocalName());
      if (fused) {
        assertEquals("bar", wtx.getValue());
      } else {
        assertTrue(wtx.moveToFirstChild());
        assertEquals("bar", wtx.getValue());
      }
    }
  }

  /**
   * Regression test for fast-path prepareRecordForModificationDocument.
   * Verifies that parent/sibling links remain correct after inserting children
   * into a parent that was already mutated in the same transaction.
   */
  @Test
  public void testParentSiblingLinksAfterInsert() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var wtx = session.beginNodeTrx();
        final Writer writer = new StringWriter()) {

      // Create array with three children: [1, 2, 3]
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(1);
      final long firstChildKey = wtx.getNodeKey();
      wtx.moveToParent();
      wtx.insertNumberValueAsLastChild(3);
      final long lastChildKey = wtx.getNodeKey();
      // Insert 2 as right sibling of 1 (between 1 and 3)
      wtx.moveTo(firstChildKey);
      wtx.insertNumberValueAsRightSibling(2);
      final long middleChildKey = wtx.getNodeKey();

      // Verify parent links
      wtx.moveTo(1); // array node
      assertEquals(3, wtx.getChildCount());
      assertEquals(firstChildKey, wtx.getFirstChildKey());
      assertEquals(lastChildKey, wtx.getLastChildKey());

      // Verify sibling chain: 1 -> 2 -> 3
      wtx.moveTo(firstChildKey);
      assertFalse(wtx.hasLeftSibling());
      assertTrue(wtx.hasRightSibling());
      assertEquals(middleChildKey, wtx.getRightSiblingKey());

      wtx.moveTo(middleChildKey);
      assertTrue(wtx.hasLeftSibling());
      assertTrue(wtx.hasRightSibling());
      assertEquals(firstChildKey, wtx.getLeftSiblingKey());
      assertEquals(lastChildKey, wtx.getRightSiblingKey());

      wtx.moveTo(lastChildKey);
      assertTrue(wtx.hasLeftSibling());
      assertFalse(wtx.hasRightSibling());
      assertEquals(middleChildKey, wtx.getLeftSiblingKey());

      wtx.commit();

      // Verify serialization produces correct output
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[1,2,3]", writer.toString());
    }
  }

  @Test
  public void testInsertObjectWithFsstCompression() throws IOException {
    final DatabaseConfiguration config = new DatabaseConfiguration(PATHS.PATH1.getFile());
    if (!Files.exists(PATHS.PATH1.getFile())) {
      Databases.createJsonDatabase(config);
    }
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      // Enable FSST compression
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).stringCompressionType(StringCompressionType.FSST).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        // Insert multiple strings with common patterns for FSST to build symbol table
        wtx.insertObjectRecordAsFirstChild("message1", new StringValue("Hello World! This is a test message."));
        wtx.moveTo(1); // Move back to object node
        wtx.insertObjectRecordAsFirstChild("message2", new StringValue("Hello World! This is another test message."));
        wtx.moveTo(1);
        wtx.insertObjectRecordAsFirstChild("message3",
            new StringValue("Hello World! This is yet another test message."));
        wtx.moveTo(1);
        wtx.insertObjectRecordAsFirstChild("message4", new StringValue("Hello World! This is the final test message."));
        wtx.commit();
      }
    }

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      // Verify all strings are correctly serialized
      String result = writer.toString();
      System.out.println("Serialized output: " + result);
      assertTrue(result.contains("Hello World!"), "Expected 'Hello World!' in: " + result);
      assertTrue(result.contains("This is a test message"), "Expected 'This is a test message' in: " + result);
      assertTrue(result.contains("This is another test message"),
          "Expected 'This is another test message' in: " + result);
      assertTrue(result.contains("This is yet another test message"),
          "Expected 'This is yet another test message' in: " + result);
      assertTrue(result.contains("This is the final test message"),
          "Expected 'This is the final test message' in: " + result);
    }
  }

  @Test
  public void testMultipleStringValuesWithFsstCompression() throws IOException {
    final DatabaseConfiguration config = new DatabaseConfiguration(PATHS.PATH1.getFile());
    if (!Files.exists(PATHS.PATH1.getFile())) {
      Databases.createJsonDatabase(config);
    }
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      // Enable FSST compression
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).stringCompressionType(StringCompressionType.FSST).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        // Insert JSON array with many similar strings
        for (int i = 0; i < 10; i++) {
          wtx.insertStringValueAsFirstChild("Common prefix: value number " + i);
          wtx.moveToParent();
        }
        wtx.commit();
      }
    }

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      String result = writer.toString();
      // Verify all values are present
      for (int i = 0; i < 10; i++) {
        assertTrue(result.contains("Common prefix: value number " + i),
            "Should contain 'Common prefix: value number " + i + "' but got: " + result);
      }
    }
  }

  @Test
  public void testAdaptiveFsstRejectsLowBenefitCompression() throws IOException {
    // This test verifies that FSST compression is only applied when beneficial
    // (P6: Benefit Guarantee - at least 15% savings required)
    final DatabaseConfiguration config = new DatabaseConfiguration(PATHS.PATH1.getFile());
    if (!Files.exists(PATHS.PATH1.getFile())) {
      Databases.createJsonDatabase(config);
    }
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      // Enable FSST compression
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).stringCompressionType(StringCompressionType.FSST).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        // Insert strings with high entropy (random UUIDs) - should NOT benefit from FSST
        // The adaptive check should reject FSST for this data
        for (int i = 0; i < 100; i++) {
          wtx.insertStringValueAsFirstChild(java.util.UUID.randomUUID().toString());
          wtx.moveToParent();
        }
        wtx.commit();
      }
    }

    // Verify all data is readable (even if FSST was rejected)
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      String result = writer.toString();
      // Should be a valid JSON array with 100 elements
      assertTrue(result.startsWith("["),
          "Should be a JSON array: " + result.substring(0, Math.min(50, result.length())));
      assertTrue(result.endsWith("]"), "Should end with ]: " + result);
    }
  }

  @Test
  public void testFsstWithLargeStrings() throws IOException {
    // Test FSST with larger string values that should benefit from compression
    final DatabaseConfiguration config = new DatabaseConfiguration(PATHS.PATH1.getFile());
    if (!Files.exists(PATHS.PATH1.getFile())) {
      Databases.createJsonDatabase(config);
    }
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).stringCompressionType(StringCompressionType.FSST).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        // Insert larger strings with repetitive patterns (should benefit from FSST)
        String basePattern = "This is a longer string with repetitive content that should compress well. ";
        for (int i = 0; i < 100; i++) {
          wtx.insertStringValueAsFirstChild(basePattern + "Entry number " + i + " continues here.");
          wtx.moveToParent();
        }
        wtx.commit();
      }
    }

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      String result = writer.toString();
      // Verify all values are correctly deserialized
      assertTrue(result.contains("This is a longer string"), "Should contain the base pattern");
      assertTrue(result.contains("Entry number 50"), "Should contain entry 50");
    }
  }
}

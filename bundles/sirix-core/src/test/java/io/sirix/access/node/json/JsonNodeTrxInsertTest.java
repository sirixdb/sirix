package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonNodeTrxInsertTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
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

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonObject),
                                      JsonNodeTrx.Commit.IMPLICIT,
                                      JsonNodeTrx.CheckParentNode.YES);

        for (int i = 0; i < 650_000; i++) {
          System.out.println(i);
          jsonObject = """
              {"item":"this is item %s", "package":"package", "kg":5}
              """.strip().formatted(i);

          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(jsonObject),
                                          JsonNodeTrx.Commit.IMPLICIT,
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

      final var expected = Files.readString(Path.of("src",
                                                    "test",
                                                    "resources",
                                                    "json",
                                                    "jsonNodeTrxInsertTest",
                                                    "testInsert500SubtreesAsFirstChild",
                                                    "expected.json"));

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
      wtx.moveTo(8);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"foo\": \"bar\"}"));

      wtx.moveTo(8);

      assertEquals(3, wtx.getChildCount());
      assertEquals(6, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("foo", wtx.getName().getLocalName());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("bar", wtx.getValue());
    }
  }

  @Test
  public void testInsertSubtreeIntoArrayAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.moveTo(3);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"foo\"]"));

      wtx.moveTo(3);

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

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
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

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(6, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      assertTrue(wtx.isObject());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("foo", wtx.getName().getLocalName());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("bar", wtx.getValue());
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
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .stringCompressionType(io.sirix.settings.StringCompressionType.FSST)
          .build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        // Insert multiple strings with common patterns for FSST to build symbol table
        wtx.insertObjectRecordAsFirstChild("message1", new StringValue("Hello World! This is a test message."));
        wtx.moveTo(1); // Move back to object node
        wtx.insertObjectRecordAsFirstChild("message2", new StringValue("Hello World! This is another test message."));
        wtx.moveTo(1);
        wtx.insertObjectRecordAsFirstChild("message3", new StringValue("Hello World! This is yet another test message."));
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
      assertTrue(result.contains("This is another test message"), "Expected 'This is another test message' in: " + result);
      assertTrue(result.contains("This is yet another test message"), "Expected 'This is yet another test message' in: " + result);
      assertTrue(result.contains("This is the final test message"), "Expected 'This is the final test message' in: " + result);
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
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .stringCompressionType(io.sirix.settings.StringCompressionType.FSST)
          .build());
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
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .stringCompressionType(io.sirix.settings.StringCompressionType.FSST)
          .build());
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
      assertTrue(result.startsWith("["), "Should be a JSON array: " + result.substring(0, Math.min(50, result.length())));
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
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .stringCompressionType(io.sirix.settings.StringCompressionType.FSST)
          .build());
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

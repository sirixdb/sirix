package io.sirix.access.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
      try (final var manager = database.beginResourceSession(resource); final var wtx = manager.beginNodeTrx()) {
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
         final var manager = database.beginResourceSession(resource);
         final Writer writer = new StringWriter()) {
      System.out.println("Start serializing");

      final var time = System.nanoTime();

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      System.out.println("Done serializing [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
    }
  }

  @Test
  public void testInsert500SubtreesAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();

      for (int i = 0; i < 500; i++) {
        wtx.moveTo(1);
        wtx.insertObjectAsFirstChild();
      }

      wtx.commit();

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
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
  public void testInsertSubtreeArrayAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsLastChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsLeftSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[[],[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeArrayAsRightSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("[]"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[[],[]]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsFirstChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsLastChild() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsLeftSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));
      wtx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[{},{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeObjectAsRightSibling() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{}"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();

      assertEquals("[{},{}]", writer.toString());
    }
  }

  @Test
  public void testInsertSubtreeIntoObjectAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
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
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
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
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
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
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
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
}

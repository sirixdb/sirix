package org.sirix.access.node.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.AfterCommitState;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.settings.VersioningType;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class JsonNodeTrxInsertTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Ignore
  @Test
  public void testInsertingTopLevelDocuments() {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .build());
      try (final var manager = database.openResourceManager(resource); final var wtx = manager.beginNodeTrx(100, TimeUnit.MILLISECONDS)) {
        System.out.println("Start inserting");

        final long time = System.nanoTime();

        wtx.insertArrayAsFirstChild();

        var jsonObject = """
            {"item":"this is item 0", "package":"package", "kg":5}
            """.strip();

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonObject),
                                      JsonNodeTrx.Commit.No,
                                      JsonNodeTrx.CheckParentNode.No);

        for (int i = 0; i < 650_000; i++) {
          System.out.println(i);
          jsonObject = """
              {"item":"this is item %s", "package":"package", "kg":5}
              """.strip().formatted(i);

          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(jsonObject),
                                          JsonNodeTrx.Commit.No,
                                          JsonNodeTrx.CheckParentNode.No);
        }

        wtx.commit();

        System.out.println("Done inserting [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
      }
    }
  }

  @Ignore
  @Test
  public void testSerializeTopLevelDocuments() throws IOException {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(resource);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(8);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"foo\": \"bar\"}"));

      wtx.moveTo(8);

      assertEquals(3, wtx.getChildCount());
      assertEquals(6, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("foo", wtx.getName().getLocalName());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("bar", wtx.getValue());
    }
  }

  @Test
  public void testInsertSubtreeIntoArrayAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(3);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"foo\"]"));

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild().trx().moveToFirstChild().hasMoved());
      assertEquals("foo", wtx.getValue());
    }
  }

  @Test
  public void testInsertArrayIntoArrayAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);

      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("[\"foo\"]"));

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(5, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertTrue(wtx.moveToRightSibling().hasMoved());
      assertTrue(wtx.isArray());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("foo", wtx.getValue());
    }
  }

  @Test
  public void testInsertObjectIntoArrayAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);

      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\": \"bar\"}"));

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(6, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertTrue(wtx.moveToRightSibling().hasMoved());
      assertTrue(wtx.isObject());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("foo", wtx.getName().getLocalName());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("bar", wtx.getValue());
    }
  }
}

package org.sirix.access.node.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.service.json.shredder.JsonShredder;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

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

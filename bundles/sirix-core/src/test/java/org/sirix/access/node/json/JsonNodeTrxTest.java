package org.sirix.access.node.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.io.StringWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.service.json.shredder.JsonShredder;

public final class JsonNodeTrxTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void insertSubtreeIntoObject() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter();
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
  public void insertSubtreeIntoArrayAsFirstChild() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter();
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(3);

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"foo\"]"));

      wtx.moveTo(3);

      assertEquals(4, wtx.getChildCount());
      assertEquals(4, wtx.getDescendantCount());
      assertTrue(wtx.moveToFirstChild().hasMoved());
      assertEquals("foo", wtx.getValue());
    }
  }

  @Test
  public void insertArrayIntoArrayAsRightSibling() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter();
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
  public void insertObjectIntoArrayAsRightSibling() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter();
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

package org.sirix.access.node.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.service.json.shredder.JsonShredder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
  public void removeObjectKeyAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);

      wtx.remove();

      assertsForRemoveNodeKeyTwo(wtx);

      wtx.commit();

      assertsForRemoveNodeKeyTwo(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveNodeKeyTwo(rtx);
      }
    }
  }

  private void assertsForRemoveNodeKeyTwo(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(1);

    assertFalse(rtx.hasNode(2));
    assertFalse(rtx.hasNode(3));
    assertFalse(rtx.hasNode(4));
    assertFalse(rtx.hasNode(5));
    assertFalse(rtx.hasNode(6));

    assertEquals(3, rtx.getChildCount());
    assertEquals(19, rtx.getDescendantCount());
    assertEquals(7, rtx.getFirstChildKey());

    rtx.moveTo(7);

    assertFalse(rtx.hasLeftSibling());
  }

  @Test
  public void removeObjectAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(17);

      wtx.remove();

      assertsForRemoveNodeKeySixteen(wtx);

      wtx.commit();

      assertsForRemoveNodeKeySixteen(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveNodeKeySixteen(rtx);
      }
    }
  }

  @Test
  public void removeObjectAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(20);

      wtx.remove();

      assertsForRemoveNodeKeyTwenty(wtx);

      wtx.commit();

      assertsForRemoveNodeKeyTwenty(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveNodeKeyTwenty(rtx);
      }
    }
  }

  @Test
  public void removeEmptyObject() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(24);

      wtx.remove();

      assertsForRemoveNodeKeyTwentyFour(wtx);

      wtx.commit();

      assertsForRemoveNodeKeyTwentyFour(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveNodeKeyTwentyFour(rtx);
      }
    }
  }

  private void assertsForRemoveNodeKeyTwentyFour(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(24));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertEquals(17, rtx.getFirstChildKey());

    rtx.moveTo(23);

    assertEquals(25, rtx.getRightSiblingKey());

    rtx.moveTo(25);

    assertEquals(23, rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveNodeKeyTwenty(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(20));
    assertFalse(rtx.hasNode(21));
    assertFalse(rtx.hasNode(22));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(6, rtx.getDescendantCount());
    assertEquals(17, rtx.getFirstChildKey());

    rtx.moveTo(17);

    assertEquals(23, rtx.getRightSiblingKey());

    rtx.moveTo(23);

    assertEquals(17, rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveNodeKeySixteen(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(17));
    assertFalse(rtx.hasNode(18));
    assertFalse(rtx.hasNode(19));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(6, rtx.getDescendantCount());
    assertEquals(20, rtx.getFirstChildKey());

    rtx.moveTo(20);

    assertFalse(rtx.hasLeftSibling());
  }

  @Test
  public void insertSubtreeIntoObjectAsFirstChild() {
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
  public void insertSubtreeIntoArrayAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
  public void insertArrayIntoArrayAsRightSibling() {
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
  public void insertObjectIntoArrayAsRightSibling() {
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

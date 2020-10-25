package org.sirix.access.node.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.settings.Fixed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonNodeTrxRemoveTest {
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

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);

      wtx.remove();

      assertsForRemoveObjectKeyAsFirstChild(wtx);

      wtx.commit();

      assertsForRemoveObjectKeyAsFirstChild(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectKeyAsFirstChild(rtx);
      }
    }
  }

  private void assertsForRemoveObjectKeyAsFirstChild(JsonNodeReadOnlyTrx rtx) {
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
  public void removeObjectKeyAsLastChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(15);

      wtx.remove();

      assertsForRemoveObjectKeyAsLastChild(wtx);

      wtx.commit();

      assertsForRemoveObjectKeyAsLastChild(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectKeyAsLastChild(rtx);
      }
    }
  }

  private void assertsForRemoveObjectKeyAsLastChild(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(1);

    assertFalse(rtx.hasNode(15));
    assertFalse(rtx.hasNode(16));
    assertFalse(rtx.hasNode(17));
    assertFalse(rtx.hasNode(18));
    assertFalse(rtx.hasNode(19));
    assertFalse(rtx.hasNode(20));

    assertEquals(3, rtx.getChildCount());
    assertEquals(13, rtx.getDescendantCount());
    assertEquals(13, rtx.getLastChildKey());

    rtx.moveTo(13);

    assertFalse(rtx.hasRightSibling());
  }

  @Test
  public void removeObjectKeyAsLeftSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);

      wtx.remove();

      assertsForRemoveObjectKeyAsLeftSibling(wtx);

      wtx.commit();

      assertsForRemoveObjectKeyAsLeftSibling(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectKeyAsLeftSibling(rtx);
      }
    }
  }

  private void assertsForRemoveObjectKeyAsLeftSibling(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(2));
    assertFalse(rtx.hasNode(3));
    assertFalse(rtx.hasNode(4));
    assertFalse(rtx.hasNode(5));
    assertFalse(rtx.hasNode(6));

    rtx.moveTo(1);

    assertEquals(3, rtx.getChildCount());
    assertEquals(19, rtx.getDescendantCount());
    assertEquals(7, rtx.getFirstChildKey());

    rtx.moveTo(7);

    assertFalse(rtx.hasLeftSibling());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
  }

  @Test
  public void removeObjectKeyAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(7);

      wtx.remove();

      assertsForRemoveObjectKeyAsRightSibling(wtx);

      wtx.commit();

      assertsForRemoveObjectKeyAsRightSibling(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectKeyAsRightSibling(rtx);
      }
    }
  }

  private void assertsForRemoveObjectKeyAsRightSibling(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(7));
    assertFalse(rtx.hasNode(8));
    assertFalse(rtx.hasNode(9));
    assertFalse(rtx.hasNode(10));
    assertFalse(rtx.hasNode(11));
    assertFalse(rtx.hasNode(12));

    rtx.moveTo(1);

    assertEquals(3, rtx.getChildCount());
    assertEquals(18, rtx.getDescendantCount());
    assertEquals(2, rtx.getFirstChildKey());

    rtx.moveTo(2);

    assertTrue(rtx.hasRightSibling());
    assertEquals(13, rtx.getRightSiblingKey());

    rtx.moveTo(13);

    assertTrue(rtx.hasLeftSibling());
    assertEquals(2, rtx.getLeftSiblingKey());
  }

  @Test
  public void removeObjectAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(17);

      wtx.remove();

      assertsForRemoveObjectAsFirstChild(wtx);

      wtx.commit();

      assertsForRemoveObjectAsFirstChild(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectAsFirstChild(rtx);
      }
    }
  }

  @Test
  public void removeObjectAsLastChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(25);

      wtx.remove();

      assertsForRemoveObjectAsLastChild(wtx);

      wtx.commit();

      assertsForRemoveObjectAsLastChild(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectAsLastChild(rtx);
      }
    }
  }

  @Test
  public void removeObjectAsLeftSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(17);

      wtx.remove();

      assertsForRemoveObjectAsLeftSibling(wtx);

      wtx.commit();

      assertsForRemoveObjectAsLeftSibling(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectAsLeftSibling(rtx);
      }
    }
  }

  @Test
  public void removeObjectAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(20);

      wtx.remove();

      assertsForRemoveObjectAsRightSibling(wtx);

      wtx.commit();

      assertsForRemoveObjectAsRightSibling(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveObjectAsRightSibling(rtx);
      }
    }
  }

  @Test
  public void removeEmptyObject() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(24);

      wtx.remove();

      assertsForRemoveEmptyObject(wtx);

      wtx.commit();

      assertsForRemoveEmptyObject(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveEmptyObject(rtx);
      }
    }
  }

  @Test
  public void removeArrayAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(25);

      wtx.remove();

      assertsForRemoveArray(wtx);

      wtx.commit();

      assertsForRemoveArray(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForRemoveArray(rtx);
      }
    }
  }

  private void assertsForRemoveArray(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(25));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertEquals(17, rtx.getFirstChildKey());

    rtx.moveTo(24);

    assertFalse(rtx.hasRightSibling());
  }

  private void assertsForRemoveEmptyObject(JsonNodeReadOnlyTrx rtx) {
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

  private void assertsForRemoveObjectAsRightSibling(JsonNodeReadOnlyTrx rtx) {
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

  private void assertsForRemoveObjectAsLeftSibling(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(17));
    assertFalse(rtx.hasNode(18));
    assertFalse(rtx.hasNode(19));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(6, rtx.getDescendantCount());
    assertEquals(20, rtx.getFirstChildKey());

    rtx.moveTo(20);

    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveObjectAsLastChild(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(25));

    rtx.moveTo(16);

    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertEquals(24, rtx.getLastChildKey());

    rtx.moveTo(24);

    assertFalse(rtx.hasRightSibling());
  }

  private void assertsForRemoveObjectAsFirstChild(JsonNodeReadOnlyTrx rtx) {
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
}

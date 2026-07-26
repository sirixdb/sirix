package io.sirix.access.node.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.settings.Fixed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * iter#32 fusion layout (verified via DescendantAxis dump):
 *
 * <pre>
 *  0 JSON_DOCUMENT
 *  1 OBJECT (root, descs=16)
 *  2 OBJECT_NAMED_ARRAY "foo" (descs=3)
 *  3   STRING_VALUE "bar"
 *  4   NULL_VALUE
 *  5   NUMBER_VALUE 2.33
 *  6 OBJECT_NAMED_OBJECT "bar" (descs=2)
 *  7   OBJECT_NAMED_STRING "hello"="world"
 *  8   OBJECT_NAMED_BOOLEAN "helloo"=true
 *  9 OBJECT_NAMED_STRING "baz"="hello"
 * 10 OBJECT_NAMED_ARRAY "tada" (descs=7)
 * 11   OBJECT (descs=1)
 * 12     OBJECT_NAMED_STRING "foo"="bar"
 * 13   OBJECT (descs=1)
 * 14     OBJECT_NAMED_BOOLEAN "baz"=false
 * 15   STRING_VALUE "boo"
 * 16   OBJECT (empty)
 * 17   ARRAY (empty)
 * </pre>
 *
 * Legacy → fused key remap is per-node based on the merging of OBJECT_KEY+VALUE pairs.
 */
public class JsonNodeTrxRemoveTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void removeObjectKeyAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // "foo" — OBJECT_NAMED_ARRAY at key 2 (unchanged from legacy because before "foo" only the
      // root OBJECT lives, both layouts agree).
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

    // Fused "foo" subtree spans keys 2..5 (4 records). Bar (key 6) stays.
    assertFalse(rtx.hasNode(2));
    assertFalse(rtx.hasNode(3));
    assertFalse(rtx.hasNode(4));
    assertFalse(rtx.hasNode(5));
    assertTrue(rtx.hasNode(6));

    Assert.assertEquals(3, rtx.getChildCount());
    // Root has 16 descendants under fusion. Removing foo subtree (4 records) leaves 12.
    Assert.assertEquals(12, rtx.getDescendantCount());
    Assert.assertEquals(6, rtx.getFirstChildKey());

    rtx.moveTo(6);

    assertFalse(rtx.hasLeftSibling());
  }

  @Test
  public void removeObjectKeyAsLastChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy "tada" OBJECT_KEY at key 15 → fused OBJECT_NAMED_ARRAY at key 10.
      wtx.moveTo(10);

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

    // Fused tada subtree spans keys 10..17 (8 records). All gone after remove.
    assertFalse(rtx.hasNode(10));
    assertFalse(rtx.hasNode(11));
    assertFalse(rtx.hasNode(12));
    assertFalse(rtx.hasNode(13));
    assertFalse(rtx.hasNode(14));
    assertFalse(rtx.hasNode(15));
    assertFalse(rtx.hasNode(16));
    assertFalse(rtx.hasNode(17));

    Assert.assertEquals(3, rtx.getChildCount());
    // After removing tada subtree (8 records): 16 - 8 = 8 descendants.
    Assert.assertEquals(8, rtx.getDescendantCount());
    // New last child of root is "baz" (OBJECT_NAMED_STRING) at key 9.
    Assert.assertEquals(9, rtx.getLastChildKey());

    rtx.moveTo(9);

    assertFalse(rtx.hasRightSibling());
  }

  @Test
  public void removeObjectKeyAsLeftSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Same operation as removeObjectKeyAsFirstChild — removing "foo" (the left sibling of
      // every other top-level field) at key 2.
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
    // Fused foo subtree spans 2..5; bar (key 6) survives.
    assertFalse(rtx.hasNode(2));
    assertFalse(rtx.hasNode(3));
    assertFalse(rtx.hasNode(4));
    assertFalse(rtx.hasNode(5));
    assertTrue(rtx.hasNode(6));

    rtx.moveTo(1);

    Assert.assertEquals(3, rtx.getChildCount());
    Assert.assertEquals(12, rtx.getDescendantCount());
    Assert.assertEquals(6, rtx.getFirstChildKey());

    rtx.moveTo(6);

    assertFalse(rtx.hasLeftSibling());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
  }

  @Test
  public void removeObjectKeyAsRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy "bar" OBJECT_KEY at key 7 → fused OBJECT_NAMED_OBJECT at key 6.
      wtx.moveTo(6);

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
    // Fused "bar" subtree spans keys 6..8 (3 records: OBJECT_NAMED_OBJECT + 2 inner fused fields).
    assertFalse(rtx.hasNode(6));
    assertFalse(rtx.hasNode(7));
    assertFalse(rtx.hasNode(8));

    rtx.moveTo(1);

    Assert.assertEquals(3, rtx.getChildCount());
    // After removing bar (3 records): root descendants = 16 - 3 = 13.
    Assert.assertEquals(13, rtx.getDescendantCount());
    Assert.assertEquals(2, rtx.getFirstChildKey());

    rtx.moveTo(2);

    assertTrue(rtx.hasRightSibling());
    // After removing "bar", "foo"'s right sibling is "baz" (OBJECT_NAMED_STRING) at key 9.
    Assert.assertEquals(9, rtx.getRightSiblingKey());

    rtx.moveTo(9);

    assertTrue(rtx.hasLeftSibling());
    Assert.assertEquals(2, rtx.getLeftSiblingKey());
  }

  @Test
  public void removeObjectAsFirstChild() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy first OBJECT in tada array at key 17 → fused: key 11.
      wtx.moveTo(11);

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
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy trailing empty ARRAY at key 25 → fused: key 17.
      wtx.moveTo(17);

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
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy first OBJECT in tada at key 17 → fused: key 11.
      wtx.moveTo(11);

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
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy second OBJECT in tada at key 20 → fused: key 13.
      wtx.moveTo(13);

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
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy empty OBJECT in tada at key 24 → fused: key 16.
      wtx.moveTo(16);

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
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      // Legacy trailing empty ARRAY at key 25 → fused: key 17.
      wtx.moveTo(17);

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
    // Empty trailing ARRAY removed at key 17.
    assertFalse(rtx.hasNode(17));

    rtx.moveTo(10); // tada OBJECT_NAMED_ARRAY

    Assert.assertEquals(4, rtx.getChildCount());
    // tada had 7 descendants; minus 1 (empty ARRAY) = 6.
    Assert.assertEquals(6, rtx.getDescendantCount());
    Assert.assertEquals(11, rtx.getFirstChildKey());

    rtx.moveTo(16); // empty OBJECT — now last child after the ARRAY removal.

    assertFalse(rtx.hasRightSibling());
  }

  private void assertsForRemoveEmptyObject(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(16));

    rtx.moveTo(10); // tada

    Assert.assertEquals(4, rtx.getChildCount());
    Assert.assertEquals(6, rtx.getDescendantCount());
    Assert.assertEquals(11, rtx.getFirstChildKey());

    rtx.moveTo(15); // STRING_VALUE "boo"

    Assert.assertEquals(17, rtx.getRightSiblingKey());

    rtx.moveTo(17); // empty ARRAY

    Assert.assertEquals(15, rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveObjectAsRightSibling(JsonNodeReadOnlyTrx rtx) {
    // {"baz":false} subtree (keys 13..14): OBJECT(13) + OBJECT_NAMED_BOOLEAN(14).
    assertFalse(rtx.hasNode(13));
    assertFalse(rtx.hasNode(14));

    rtx.moveTo(10); // tada

    Assert.assertEquals(4, rtx.getChildCount());
    // tada had 7 descs; minus 2 = 5.
    Assert.assertEquals(5, rtx.getDescendantCount());
    Assert.assertEquals(11, rtx.getFirstChildKey());

    rtx.moveTo(11); // first OBJECT (untouched)

    Assert.assertEquals(15, rtx.getRightSiblingKey()); // STRING "boo" follows

    rtx.moveTo(15);

    Assert.assertEquals(11, rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveObjectAsLeftSibling(JsonNodeReadOnlyTrx rtx) {
    // {"foo":"bar"} subtree (keys 11..12): OBJECT(11) + OBJECT_NAMED_STRING(12).
    assertFalse(rtx.hasNode(11));
    assertFalse(rtx.hasNode(12));

    rtx.moveTo(10);

    Assert.assertEquals(4, rtx.getChildCount());
    Assert.assertEquals(5, rtx.getDescendantCount());
    Assert.assertEquals(13, rtx.getFirstChildKey()); // OBJECT {baz:false} now first

    rtx.moveTo(13);

    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
  }

  private void assertsForRemoveObjectAsLastChild(JsonNodeReadOnlyTrx rtx) {
    assertFalse(rtx.hasNode(17));

    rtx.moveTo(10); // tada

    Assert.assertEquals(4, rtx.getChildCount());
    Assert.assertEquals(6, rtx.getDescendantCount());
    Assert.assertEquals(16, rtx.getLastChildKey()); // empty OBJECT now last

    rtx.moveTo(16);

    assertFalse(rtx.hasRightSibling());
  }

  private void assertsForRemoveObjectAsFirstChild(JsonNodeReadOnlyTrx rtx) {
    // {"foo":"bar"} subtree (keys 11..12).
    assertFalse(rtx.hasNode(11));
    assertFalse(rtx.hasNode(12));

    rtx.moveTo(10);

    Assert.assertEquals(4, rtx.getChildCount());
    Assert.assertEquals(5, rtx.getDescendantCount());
    Assert.assertEquals(13, rtx.getFirstChildKey()); // OBJECT {baz:false} now first

    rtx.moveTo(13);

    assertFalse(rtx.hasLeftSibling());
  }
}

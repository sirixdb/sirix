package io.sirix.node;

import io.sirix.exception.SirixException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixDeweyIDRelationsTest {
  @Test
  void ancestorChainsPreserveOrderAndStopAtTheRoot() {
    final SirixDeweyID empty = new SirixDeweyID(new int[0]);
    final SirixDeweyID root = new SirixDeweyID("1");
    final SirixDeweyID parent = new SirixDeweyID("1.3");
    final SirixDeweyID child = new SirixDeweyID("1.3.5");

    assertEquals(0, root.getLevel());
    assertArrayEquals(new SirixDeweyID[] {empty, root, parent}, child.getAncestors());
    assertArrayEquals(new SirixDeweyID[] {root, parent}, child.getAncestors(root));
    assertEquals(parent, child.getParent());
    assertEquals(parent, child.getAncestor(2));
    assertEquals(parent, child.getAncestor(2, root));
    assertNull(empty.getParent());
    assertNull(empty.getAncestors());
    assertNull(child.getAncestor(4));
    assertNull(child.getAncestor(2, new SirixDeweyID("1.7")));
    assertNull(child.getAncestors(new SirixDeweyID("1.7")));
  }

  @Test
  void siblingInsertionSupportsBothOpenBoundaries() {
    final SirixDeweyID left = new SirixDeweyID("1.3");
    final SirixDeweyID right = new SirixDeweyID("1.7");
    final SirixDeweyID before = SirixDeweyID.newBetween(null, left);
    final SirixDeweyID between = SirixDeweyID.newBetween(left, right);
    final SirixDeweyID after = SirixDeweyID.newBetween(right, null);

    assertTrue(before.compareTo(left) < 0);
    assertTrue(left.compareTo(between) < 0);
    assertTrue(between.compareTo(right) < 0);
    assertTrue(right.compareTo(after) < 0);
    assertEquals(new SirixDeweyID("1"), before.getParent());
    assertEquals(new SirixDeweyID("1"), between.getParent());
    assertEquals(new SirixDeweyID("1"), after.getParent());
  }

  @Test
  void siblingInsertionRejectsEmptyAndStructuralMarkerBounds() {
    final SirixDeweyID sibling = new SirixDeweyID("1.7");
    final SirixDeweyID[] invalidBounds =
        {new SirixDeweyID(new int[0]), new SirixDeweyID("1.3.0"), new SirixDeweyID("1.3.2")};
    for (final SirixDeweyID invalid : invalidBounds) {
      assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(null, invalid));
      assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(invalid, null));
      assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(sibling, invalid));
      assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(invalid, sibling));
    }
  }

  @Test
  void siblingInsertionPreservesAttributeNamespaceAndRecordPrefixes() {
    final SirixDeweyID root = SirixDeweyID.newRootID();
    final SirixDeweyID[] children = {root.getNewChildID(), root.getNewAttributeID(), root.getNewNamespaceID(),
        root.getNewRecordID(), new SirixDeweyID("1.2.2.3")};
    for (final SirixDeweyID child : children) {
      final SirixDeweyID before = SirixDeweyID.newBetween(null, child);
      final SirixDeweyID after = SirixDeweyID.newBetween(child, null);
      final SirixDeweyID between = SirixDeweyID.newBetween(before, child);
      assertTrue(before.compareTo(between) < 0);
      assertTrue(between.compareTo(child) < 0);
      assertTrue(child.compareTo(after) < 0);
      assertEquals(child.getParent(), before.getParent());
      assertEquals(child.getParent(), between.getParent());
      assertEquals(child.getParent(), after.getParent());
    }
  }

  @Test
  void boundedInsertionRejectsUnorderedOrUnrelatedNodes() {
    final SirixDeweyID left = new SirixDeweyID("1.3");
    final SirixDeweyID right = new SirixDeweyID("1.7");
    assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(right, left));
    final SirixDeweyID unrelated = new SirixDeweyID("1.7.3");
    assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(left, unrelated));
  }
}

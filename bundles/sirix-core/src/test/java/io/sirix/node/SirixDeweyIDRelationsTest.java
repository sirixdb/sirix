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
  void boundedInsertionRejectsUnorderedOrUnrelatedNodes() {
    final SirixDeweyID left = new SirixDeweyID("1.3");
    final SirixDeweyID right = new SirixDeweyID("1.7");
    assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(right, left));
    assertThrows(SirixException.class, () -> SirixDeweyID.newBetween(left, new SirixDeweyID("1.7.3")));
  }
}

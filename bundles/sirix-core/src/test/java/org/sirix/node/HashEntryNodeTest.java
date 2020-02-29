package org.sirix.node;

import org.junit.Test;

import static org.junit.Assert.*;

public final class HashEntryNodeTest {
  @Test
  public void test() {
    final var node = new HashEntryNode(1, 3247389, "foobar");

    assertEquals(NodeKind.HASH_ENTRY, node.getKind());
    assertEquals(1, node.getNodeKey());
    assertEquals(3247389, node.getKey());
    assertEquals(-1168208943, node.hashCode());
    assertNotNull(node.toString());

    final var otherNode = new HashEntryNode(2, 3247389, "foobar");
    assertTrue(node.equals(otherNode));
    assertTrue(otherNode.equals(node));

    final var otherUnequalNodeDueToKey = new HashEntryNode(3, 82193, "foobar");
    assertFalse(node.equals(otherUnequalNodeDueToKey));
    assertFalse(otherUnequalNodeDueToKey.equals(node));

    final var otherUnequalNodeDueToValue = new HashEntryNode(4, 3247389, "baz");
    assertFalse(node.equals(otherUnequalNodeDueToValue));
    assertFalse(otherUnequalNodeDueToValue.equals(node));

    final var otherUnequalNodeDueToType = new HashCountEntryNode(1, 55);
    assertFalse(node.equals(otherUnequalNodeDueToType));
    assertFalse(otherUnequalNodeDueToType.equals(node));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOperationNotSupportedException() {
    final var node = new HashEntryNode(1, 3247389, "foobar");

    node.getRevision();
  }
}

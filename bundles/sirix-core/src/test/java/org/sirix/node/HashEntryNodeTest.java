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
    assertEquals(node, otherNode);
    assertEquals(otherNode, node);

    final var otherUnequalNodeDueToKey = new HashEntryNode(3, 82193, "foobar");
    assertNotEquals(node, otherUnequalNodeDueToKey);
    assertNotEquals(otherUnequalNodeDueToKey, node);

    final var otherUnequalNodeDueToValue = new HashEntryNode(4, 3247389, "baz");
    assertNotEquals(node, otherUnequalNodeDueToValue);
    assertNotEquals(otherUnequalNodeDueToValue, node);

    final var otherUnequalNodeDueToType = new HashCountEntryNode(1, 55);
    assertNotEquals(node, otherUnequalNodeDueToType);
    assertNotEquals(otherUnequalNodeDueToType, node);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOperationNotSupportedException() {
    final var node = new HashEntryNode(1, 3247389, "foobar");

    node.getRevision();
  }
}

package org.sirix.node;

import org.junit.Test;

import static org.junit.Assert.*;

public final class HashCountEntryNodeTest {
  @Test
  public void test() {
    final var node = new HashCountEntryNode(1, 44);

    assertEquals(NodeKind.HASH_NAME_COUNT_TO_NAME_ENTRY, node.getKind());
    assertEquals(1, node.getNodeKey());
    assertEquals(44, node.getValue());
    assertEquals(44, node.hashCode());
    assertNotNull(node.toString());
    node.incrementValue();
    assertEquals(45, node.getValue());
    node.decrementValue();
    assertEquals(44, node.getValue());

    final var otherNode = new HashCountEntryNode(2, 44);
    assertTrue(node.equals(otherNode));
    assertTrue(otherNode.equals(node));

    final var otherUnequalNodeDueToValue = new HashCountEntryNode(3, 20);
    assertFalse(node.equals(otherUnequalNodeDueToValue));
    assertFalse(otherUnequalNodeDueToValue.equals(node));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOperationNotSupportedException() {
    final var node = new HashCountEntryNode(1, 44);

    node.getRevision();
  }
}

package org.sirix.index.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LeafNodeUnitTest {

	private final String key = "foo";
	private final String value = "bar";
	private final LeafNode<String, String> node = new LeafNode<>(BinaryComparables.forString().get(key), key, value);

	@Test
	public void testFirst() {
		Assertions.assertNull(node.first());
	}

	@Test
	public void testLast() {
		Assertions.assertNull(node.last());
	}

	@Test
	public void testEntry() {
		Assertions.assertEquals(key, node.getKey());
		Assertions.assertEquals(value, node.getValue());
		node.setValue("new value");
		Assertions.assertEquals("new value", node.getValue());
	}
}

package org.sirix.index.art;

import com.google.common.primitives.UnsignedBytes;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Node4UnitTest extends InnerNodeUnitTest {

	Node4UnitTest() {
		super(2);
	}

	@Test
	public void testGetOnlyChild() {
		// remove until only one child
		while (node.size() != 1) {
			node.removeChild(node.first().uplinkKey());
		}

		byte[] keys = existingKeys();
		UnsignedBytes.sortDescending(keys);
		Assert.assertEquals(keys[0], ((Node4) node).getOnlyChildKey());
	}

	@Override
	@Test
	public void testShrink() {
		Assertions.assertThrows(UnsupportedOperationException.class, () -> node.shrink());
	}

	@Test
	public void testShouldShrinkAlwaysFalse() {
		// remove all
		while (node.size() != 0) {
			node.removeChild(node.first().uplinkKey());
		}
		Assertions.assertFalse(node.shouldShrink());
	}
}

package org.sirix.index.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.sirix.index.art.InnerNodeUtils.getValidPrefixKey;

/*
	tests for helper methods defined in ART
 */
public class ARTUnitTest {

	@Test
	public void testCompare() {
		BinaryComparable<String> bc = BinaryComparables.forString();
		byte[] a = bc.get("pqrxyabce");
		byte[] b = bc.get("zabcd");
		// abc, abc (i == aTo && j == bTo)
		Assertions.assertEquals(0, AdaptiveRadixTree.compare(a, 5, 8, b, 1, 4));
		// abc, abcd (i == aTo)
		Assertions.assertEquals(-1, AdaptiveRadixTree.compare(a, 5, 8, b, 1, 5));
		// abce, abc (j == bTo)
		Assertions.assertEquals(1, AdaptiveRadixTree.compare(a, 5, 9, b, 1, 4));
		// abce, abcd (a[i] < b[j])
		Assertions.assertEquals(1, AdaptiveRadixTree.compare(a, 5, 9, b, 1, 5));
	}

	/*
		cp for all i == key for all i
			but len(key) >= len(cp) expect 0
			but len(key) < len(cp) expect 1
		cp at i < key at i expect -1
		cp at i > key at i expect 1
	 */
	@Test
	public void testCompareCompressedPath() {
		InnerNode node = new Node4();
		BinaryComparable<String> bc = BinaryComparables.forString();

		// 0 (even when key length more than compressed path)
		String compressedPath = "abcd";
		String key = "xx" + compressedPath + "ef";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		Assertions.assertEquals(0, AdaptiveRadixTree.comparePessimisticCompressedPath(node, bc.get(key), 2));

		// 0 (totally equal and length same)
		key = compressedPath;
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		Assertions.assertEquals(0, AdaptiveRadixTree.comparePessimisticCompressedPath(node, bc.get(key), 0));


		// 1 (compressed path length is more than key)
		key = "cab";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		Assertions.assertTrue(0 < AdaptiveRadixTree.comparePessimisticCompressedPath(node, bc.get(key), 1));

		// 1 (inequality and compressed path being greater)
		compressedPath = "xxz";
		key = "xxa";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		Assertions.assertTrue(0 < AdaptiveRadixTree.comparePessimisticCompressedPath(node, bc.get(key), 0));

		// -1 (only in case of inequality of partial key byte)
		compressedPath = "xxaa";
		key = "xxabcd";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		Assertions.assertTrue(0 > AdaptiveRadixTree.comparePessimisticCompressedPath(node, bc.get(key), 0));

	}

	/*
		cover all windows (toCompress, linking key, onlyChild)
		everything from toCompress
		everything from toCompress + linking key
		everything from toCompress + linking key + some from child
		everything from toCompress + linking key + all from child
	 */
	@Test
	public void testUpdateCompressedPathOfOnlyChild() {
		// everything from toCompress
		Node4 node = new Node4();
		node.prefixLen = 10;
		String toCompressPrefix = "abcdefgh";
		System.arraycopy(toCompressPrefix.getBytes(), 0, node.prefixKeys, 0, toCompressPrefix.length());
		InnerNode onlyChild = new Node4();
		byte linkingKey = 1;
		node.addChild(linkingKey, onlyChild);
		onlyChild.prefixLen = 3;
		String onlyChildPrefix = "pqr";
		System.arraycopy(onlyChildPrefix.getBytes(), 0, onlyChild.prefixKeys, 0, onlyChildPrefix.length());
		AdaptiveRadixTree.updateCompressedPathOfOnlyChild(node, onlyChild);
		Assertions.assertEquals(14, onlyChild.prefixLen);
		Assertions.assertArrayEquals(getValidPrefixKey(node), getValidPrefixKey(onlyChild));

		// everything from toCompress + linking key
		node = new Node4();
		node.prefixLen = 7;
		toCompressPrefix = "abcdefg";
		System.arraycopy(toCompressPrefix.getBytes(), 0, node.prefixKeys, 0, toCompressPrefix.length());
		onlyChild = new Node4();
		node.addChild(linkingKey, onlyChild);
		onlyChild.prefixLen = 3;
		onlyChildPrefix = "pqr";
		System.arraycopy(onlyChildPrefix.getBytes(), 0, onlyChild.prefixKeys, 0, onlyChildPrefix.length());
		AdaptiveRadixTree.updateCompressedPathOfOnlyChild(node, onlyChild);
		Assertions.assertEquals(11, onlyChild.prefixLen);
		byte[] expected = new byte[InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT];
		for (int i = 0; i < node.prefixLen; i++) {
			expected[i] = node.prefixKeys[i];
		}
		expected[node.prefixLen] = linkingKey;
		Assertions.assertArrayEquals(expected, getValidPrefixKey(onlyChild));

		// everything from toCompress + linking key + some from child
		node = new Node4();
		node.prefixLen = 4;
		toCompressPrefix = "abcd";
		System.arraycopy(toCompressPrefix.getBytes(), 0, node.prefixKeys, 0, toCompressPrefix.length());
		onlyChild = new Node4();
		node.addChild(linkingKey, onlyChild);
		onlyChild.prefixLen = 5;
		onlyChildPrefix = "pqrst";
		System.arraycopy(onlyChildPrefix.getBytes(), 0, onlyChild.prefixKeys, 0, onlyChildPrefix.length());
		AdaptiveRadixTree.updateCompressedPathOfOnlyChild(node, onlyChild);
		Assertions.assertEquals(10, onlyChild.prefixLen);
		expected = new byte[InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT];
		for (int i = 0; i < node.prefixLen; i++) {
			expected[i] = node.prefixKeys[i];
		}
		expected[node.prefixLen] = linkingKey;
		for (int i = node.prefixLen + 1, j = 0; i < InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT && j < onlyChildPrefix
				.length(); i++, j++) {
			expected[i] = onlyChildPrefix.getBytes()[j];
		}
		Assertions.assertArrayEquals(expected, getValidPrefixKey(onlyChild));

		// everything from toCompress + linking key + all from child
		node = new Node4();
		node.prefixLen = 4;
		toCompressPrefix = "abcd";
		System.arraycopy(toCompressPrefix.getBytes(), 0, node.prefixKeys, 0, toCompressPrefix.length());
		onlyChild = new Node4();
		node.addChild(linkingKey, onlyChild);
		onlyChild.prefixLen = 2;
		onlyChildPrefix = "pq";
		System.arraycopy(onlyChildPrefix.getBytes(), 0, onlyChild.prefixKeys, 0, onlyChildPrefix.length());
		AdaptiveRadixTree.updateCompressedPathOfOnlyChild(node, onlyChild);
		Assertions.assertEquals(7, onlyChild.prefixLen);
		expected = new byte[InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT - 1];
		for (int i = 0; i < node.prefixLen; i++) {
			expected[i] = node.prefixKeys[i];
		}
		expected[node.prefixLen] = linkingKey;
		for (int i = node.prefixLen + 1, j = 0; i < InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT && j < onlyChildPrefix
				.length(); i++, j++) {
			expected[i] = onlyChildPrefix.getBytes()[j];
		}
		Assertions.assertArrayEquals(expected, getValidPrefixKey(onlyChild));

		// coverage for assert onlyChild != null;
		Assertions.assertThrows(AssertionError.class, () -> AdaptiveRadixTree
				.updateCompressedPathOfOnlyChild(new Node4(), null));
	}

	// current prefix len <= InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT
	@Test
	public void testRemovePessimisticLCPFromPessimisticCompressedPath() {
		InnerNode node = new Node4();
		String compressedPath = "abcd";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		// LCP = 3, hence "d" would be the differing partial key, therefore new compressed path
		// would be "", hence 0 length
		AdaptiveRadixTree.removePessimisticLCPFromCompressedPath(node, -1, 3);
		Assertions.assertEquals(0, node.prefixLen);

		// LCP = 2, hence "c" would be differing partial key
		// and new compressed path would be "d"
		node.prefixLen = compressedPath.length();
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		AdaptiveRadixTree.removePessimisticLCPFromCompressedPath(node, -1, 2);
		Assertions.assertEquals(1, node.prefixLen);
		Assertions.assertArrayEquals("d".getBytes(), getValidPrefixKey(node));

		// LCP = 4, does not obey constraint of method
		// since LCP == compressedPath.length()
		// which would mean, we have totally matched!
		// in which case there's no need to remove LCP from branching out node
		node.prefixLen = compressedPath.length();
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		Assertions.assertThrows(AssertionError.class, () -> AdaptiveRadixTree
				.removePessimisticLCPFromCompressedPath(node, -1, compressedPath.length()));
	}

	// case 1: new prefix len > InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT
	@Test
	public void testRemovePessimisticLCPFromOptimisticCompressedPath1() {
		InnerNode node = new Node4();
		// number of optimistic equal characters in all children of this InnerNode
		int optimisticCPLength = 10, lcp = 3;
		String compressedPath = "abcdefgh"; // pessimistic compressed path
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath
				.length() + optimisticCPLength;
		int expectedNewPrefixLen = compressedPath.length() + optimisticCPLength - lcp - 1;

		InnerNode nodeLeft = new Node4();
		node.addChild((byte) 'i', nodeLeft);
		node.addChild((byte) 'j', Mockito.spy(Node.class));

		String prevDepth = "prevdepthbytes";
		String optimisticPath = "0123456789";
		String key = prevDepth + compressedPath + optimisticPath + "ik";
		LeafNode<String, String> nodeLeftLeft = new LeafNode<>(BinaryComparables.forString().get(key), key, "value");
		nodeLeft.addChild((byte) 'k', nodeLeftLeft);
		nodeLeft.addChild((byte) 'l', Mockito.spy(Node.class));

		AdaptiveRadixTree.removePessimisticLCPFromCompressedPath(node, prevDepth.length() + lcp, lcp);
		Assertions.assertEquals(expectedNewPrefixLen, node.prefixLen);


		Assertions.assertArrayEquals("efgh0123".getBytes(), getValidPrefixKey(node));
	}

	// case 2: new prefix len <= InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT
	@Test
	public void testRemovePessimisticLCPFromOptimisticCompressedPath2() {
		InnerNode node = new Node4();
		// number of optimistic equal characters in all children of this InnerNode
		int optimisticCPLength = 2, lcp = 3;
		String compressedPath = "abcdefgh"; // pessimistic compressed path
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath
				.length() + optimisticCPLength;

		int expectedNewPrefixLen = compressedPath.length() + optimisticCPLength - lcp - 1;

		InnerNode nodeLeft = new Node4();
		node.addChild((byte) 'i', nodeLeft);
		node.addChild((byte) 'j', Mockito.spy(Node.class));

		String prevDepth = "prevdepthbytes";
		String optimisticPath = "01";
		String key = prevDepth + compressedPath + optimisticPath + "ik";
		LeafNode<String, String> nodeLeftLeft = new LeafNode<>(BinaryComparables.forString().get(key), key, "value");
		nodeLeft.addChild((byte) 'k', nodeLeftLeft);
		nodeLeft.addChild((byte) 'l', Mockito.spy(Node.class));

		AdaptiveRadixTree.removePessimisticLCPFromCompressedPath(node, prevDepth.length() + lcp, lcp);
		Assertions.assertEquals(expectedNewPrefixLen, node.prefixLen);


		Assertions.assertArrayEquals("efgh01".getBytes(), getValidPrefixKey(node));
	}

	@Test
	public void testBranchOutPessimistic() {
		InnerNode node = new Node4();
		BinaryComparable<String> bc = BinaryComparables.forString();
		String compressedPath = "abcxyz";
		System.arraycopy(compressedPath.getBytes(), 0, node.prefixKeys, 0, compressedPath.length());
		node.prefixLen = compressedPath.length();
		String key = "xxabcdef";
		String value = "value";
		// lcp == "abc"
		InnerNode newNode = AdaptiveRadixTree.branchOutPessimistic(node, bc.get(key), key, value, 3, 5);
		Assertions.assertEquals(2, newNode.size());
		Assertions.assertEquals(node, newNode.findChild((byte) 'x'));
		Node leaf = newNode.findChild((byte) 'd');
		Assertions.assertTrue(leaf instanceof LeafNode);
		Assertions.assertEquals(key, ((LeafNode) leaf).getKey());
		Assertions.assertEquals(value, ((LeafNode) leaf).getValue());
		Assertions.assertEquals(3, ((InnerNode) newNode).prefixLen);
		Assertions.assertArrayEquals("abc".getBytes(), getValidPrefixKey(newNode));

		// test removeLCPFromCompressedPath
		Assertions.assertEquals(2, node.prefixLen);
		Assertions.assertArrayEquals("yz".getBytes(), getValidPrefixKey(node));

		// obey constraints
		node.prefixLen = 1;
		Assertions.assertThrows(AssertionError.class, () -> AdaptiveRadixTree
				.branchOutPessimistic(node, bc.get(key), key, value, 3, 5));

		node.prefixLen = 10;
		Assertions.assertThrows(AssertionError.class, () -> AdaptiveRadixTree
				.branchOutPessimistic(node, bc.get(key), key, value, InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT, 5));

	}

	@Test
	public void testReplace() {
		BinaryComparable<String> bc = BinaryComparables.forString();
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(bc);
		String key = "foo";
		String value = "value";
		// adding the very first key would result in replacing root
		LeafNode<String, String> leafNode = new LeafNode<>(bc.get(key), key, value);
		art.replace(0, bc.get("foo"), null, leafNode);
		Assertions.assertEquals(value, art.get(key));

		art = new AdaptiveRadixTree<>(bc);
		// setup root with one child
		Node4 root = new Node4();
		art.replace(0, new byte[] {}, null, leafNode);
		Node child = Mockito.spy(Node.class);
		root.addChild((byte) 'x', child);

		// replace root's x downlink with new child (for various reasons, for example because we just grew this child)
		Node newChild = Mockito.spy(Node.class);
		art.replace(1, bc.get("x"), root, newChild);

		Assertions.assertEquals(1, root.size());
		Assertions.assertSame(newChild, root.findChild((byte) 'x'));
	}

}

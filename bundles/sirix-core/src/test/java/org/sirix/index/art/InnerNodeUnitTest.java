package org.sirix.index.art;

import com.google.common.primitives.UnsignedBytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.sirix.index.art.InnerNodeUtils.getValidPrefixKey;

public abstract class InnerNodeUnitTest {
	protected static class Pair implements Comparable<Pair> {
		final byte partialKey;
		final Node child;

		Pair(byte partialKey, Node child) {
			this.partialKey = partialKey;
			this.child = child;
		}

		@Override
		public int compareTo(Pair o) {
			return UnsignedBytes.compare(partialKey, o.partialKey);
		}
	}

	protected InnerNode node;
	protected Pair[] existingData;

	InnerNodeUnitTest(int nodeSize) {
		InnerNode node = new Node4();
		existingData = new Pair[nodeSize + 1];
		for (int j = 0, i = -nodeSize / 2; j < nodeSize + 1; i++, j++) {
			if (node.isFull()) {
				node = node.grow();
			}
			Pair p = new Pair((byte) i, Mockito.spy(Node.class));
			existingData[j] = p;
			node.addChild(p.partialKey, p.child);
		}
		this.node = node;
	}

	@BeforeEach
	public void setup() {
		int i = 0;
		for (; i < existingData.length; i++) {
			if (existingData[i].partialKey < 0) {
				break;
			}
		}
		assertTrue(i < existingData.length, "sample key set should contain at least"
				+ " one negative integer to test for unsigned lexicographic ordering");
	}

	// lexicographic sorted order: 0, 1, -2, -1
	// -2, -1, 0, 1
	byte[] existingKeys() {
		byte[] keys = new byte[existingData.length];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = existingData[i].partialKey;
		}
		return keys;
	}

	void verifyUnsignedLexicographicOrder() {
		verifyUnsignedLexicographicOrder(node);
	}

	/*
			work only with interface methods
			we don't care about the implementation details
			for example how Node4 stores bytes as unsigned, etc.
			we just care about the right lexicographic ordering.
			of course this requires us to test with negative as well as
			positive data set and hence the check in test setup.
			we don't test child mappings, since that is tested in findChild already (if the same mappings
			are maintained).
			this really is making sure negative bytes come after positives.
			we don't really want to test that children storage is sorted,
			all we want is if the lexicographic order dependant methods (first, last, greater, lesser)
			are answered correctly.
			they might be answered correctly even without storing the children in sorted order,
			but we don't care as a generic test suite.
			we base our assertions on invariants.
		 */
	void verifyUnsignedLexicographicOrder(InnerNode node) {
		boolean negExist = false;
		byte prev = node.first().uplinkKey();
		if (prev < 0) {
			negExist = true;
		}
		for (int i = 1; i < node.size(); i++) {
			byte next = node.greater(prev).uplinkKey();
			assertTrue(UnsignedBytes.compare(prev, next) < 0);
			prev = next;
			if (prev < 0) {
				negExist = true;
			}
		}
		assertTrue(negExist, "expected at least one negative byte to test lexicographic ordering");

		prev = node.last().uplinkKey();
		for (int i = node.size() - 2; i >= 0; i--) {
			byte next = node.lesser(prev).uplinkKey();
			assertTrue(UnsignedBytes.compare(prev, next) > 0);
			prev = next;
		}
	}


	/*
		add partial keys
		all key, child mappings should exist
		size increase
		uplinks setup
		expect keys to be in the right unsigned lexicographic order
	 */
	@Test
	public void testAddAndFindChild() {
		List<Pair> pairs = new ArrayList<>(Arrays.asList(existingData));
		for (byte i = 0; !node.isFull(); i++) {
			if (node.findChild(i) != null) {
				continue;
			}
			Pair p = new Pair(i, Mockito.spy(Node.class));
			pairs.add(p);
			node.addChild(p.partialKey, p.child);
		}

		// size
		assertEquals(node.size(), pairs.size());

		for (int i = 0; i < pairs.size(); i++) {
			Pair p = pairs.get(i);
			// uplinks setup
			assertEquals(node, p.child.parent());
			assertEquals(p.partialKey, p.child.uplinkKey());
			// all added partial keys exist
			assertEquals(p.child, node.findChild(p.partialKey));
		}

		verifyUnsignedLexicographicOrder();
	}

	/*
		sort sample data and expect the smallest lexicographic byte
	 */
	@Test
	public void testFirst() {
		byte[] data = existingKeys();
		UnsignedBytes.sort(data);
		assertEquals(node.first().uplinkKey(), data[0]);
	}

	/*
		sort sample data and expect the largest lexicographic byte
	 */
	@Test
	public void testLast() {
		byte[] data = existingKeys();
		UnsignedBytes.sortDescending(data);
		assertEquals(node.last().uplinkKey(), data[0]);
	}

	/*
		nothing greater than greatest
		first is greater than smallest lexicographic unsigned i.e. 0 (0000 0000)
	 */
	@Test
	public void testGreater() {
		Node last = node.last();
		assertNull(node.greater(last.uplinkKey()));
		Arrays.sort(existingData);
		for (int i = 0; i < node.size() - 1; i++) {
			Node greater = node.greater(existingData[i].partialKey);
			assertEquals(existingData[i + 1].child, greater);
		}
	}

	/*
		nothing lesser than least
		last is lesser than largest lexicographic unsigned i.e. -1 (1111 1111)
	 */
	@Test
	public void testLesser() {
		Node first = node.first();
		assertNull(node.lesser(first.uplinkKey()));
		Arrays.sort(existingData);
		for (int i = 1; i < node.size(); i++) {
			Node lesser = node.lesser(existingData[i].partialKey);
			assertEquals(existingData[i - 1].child, lesser);
		}
	}

	/*
		remove child
		unsigned lexicopgrahic order maintained
		removes uplink
		reduces size
		child no longer exists (findChild)
	 */
	@Test
	public void testRemove() {
		// since we remove two in the test
		// we must not break constraint of a node that it must have
		// a number of minimum elements (check node size assert in first, last assert)
		byte minByte = Byte.MAX_VALUE, maxByte = Byte.MIN_VALUE;
		for(int i = 0; i < existingKeys().length; i++){
			if(existingData[i].partialKey > maxByte){
				maxByte = existingData[i].partialKey;
			}
			if(existingData[i].partialKey < minByte){
				minByte = existingData[i].partialKey;
			}
		}
		Pair p = new Pair((byte)(minByte-1), Mockito.spy(Node.class));
		node.addChild(p.partialKey, p.child);
		p = new Pair((byte)(maxByte+1), Mockito.spy(Node.class));
		if(!node.isFull()){ // need for Node4 since we add 3 elements in test setup already
			node.addChild(p.partialKey, p.child);
		}

		int initialSize = node.size();

		// remove at head
		Node head = node.first();
		node.removeChild(head.uplinkKey());
		assertNull(node.findChild(head.uplinkKey()));
		assertEquals(initialSize - 1, node.size());
		assertNull(head.parent());

		// remove at tail
		Node tail = node.last();
		node.removeChild(tail.uplinkKey());
		assertNull(node.findChild(tail.uplinkKey()));
		assertEquals(initialSize - 2, node.size());
		assertNull(tail.parent());

		verifyUnsignedLexicographicOrder();
	}

	/*
		after growing, new node:
		 contains same key, child mappings in same lexicographic order but with uplinks to new grown node
		 same prefix key, no of children, uplink key, parent
	 */
	@Test
	public void testGrow() {
		List<Pair> pairs = new ArrayList<>(Arrays.asList(existingData));
		byte i;
		Pair pair;
		// fill node to capacity
		for (i = 0; ; i++) {
			if (node.findChild(i) != null) {
				continue; // find at least one non existent child to force add
			}
			pair = new Pair(i, Mockito.spy(Node.class));
			if (node.isFull()) {
				break;
			}
			pairs.add(pair);
			node.addChild(pair.partialKey, pair.child);
		}

		// capacity reached
		assertTrue(node.isFull());

		// hence we need to grow
		InnerNode grown = node.grow();
		assertEquals(node.size(), grown.size());
		assertEqualHeader(node, grown);

		// add child on newly grown node
		grown.addChild(pair.partialKey, pair.child);
		pairs.add(pair);

		// verify same key, child mappings exist
		for (i = 0; i < pairs.size(); i++) {
			Pair p = pairs.get(i);
			// uplinks setup
			assertEquals(grown, p.child.parent());
			assertEquals(p.partialKey, p.child.uplinkKey());
			// all added partial keys exist
			assertEquals(p.child, grown.findChild(p.partialKey));
		}
		verifyUnsignedLexicographicOrder(grown);
	}

	/*
		after shrinking contains same key, child mappings
		lexicographic order maintained
		same parent as before, prefix len, prefix keys
	 */
	@Test
	public void testShrink() {
		List<Pair> pairs = new ArrayList<>(Arrays.asList(existingData));
		while (!node.shouldShrink()) {
			node.removeChild(pairs.remove(0).partialKey);
		}
		assertTrue(node.shouldShrink());
		InnerNode shrunk = node.shrink();

		assertEquals(shrunk.size(), node.size());
		assertEqualHeader(node, shrunk);

		// verify same key, child mappings exist
		for (int i = 0; i < pairs.size(); i++) {
			Pair p = pairs.get(i);
			// uplinks setup
			assertEquals(shrunk, p.child.parent());
			assertEquals(p.partialKey, p.child.uplinkKey());
			// all added partial keys exist
			assertEquals(p.child, shrunk.findChild(p.partialKey));
		}
		verifyUnsignedLexicographicOrder(shrunk);
	}

	void assertEqualHeader(Node a, Node b) {
		InnerNode aa = (InnerNode) a;
		InnerNode bb = (InnerNode) b;
		assertEquals(aa.prefixLen, bb.prefixLen);
		assertArrayEquals(getValidPrefixKey(aa), getValidPrefixKey(bb));
		assertEquals(aa.parent(), bb.parent());
		assertEquals(aa.uplinkKey(), bb.uplinkKey());
	}

	/*
		replace the child associated with a key
		assert new child found
		same size
		lexicographic order maintained
		uplink setup for new child
		old child uplink stays:
			why? because in lazy leaf expansion case, we first link current leaf node with a
			new Node4() and later replace current down pointer to this leaf node with this new
			Node4() parent. If we remove old child's uplink, it could be the case that the old child
			has been linked with a new parent.
			Well we could make sure that explicitly in the branch, but it is fine
			to not do in replace as well.
	 */
	@Test
	public void testReplace() {
		Node first = node.first();
		Node newChild = Mockito.spy(Node.class);
		node.replace(first.uplinkKey(), newChild);
		assertEquals(newChild, node.findChild(first.uplinkKey()));
		assertEquals(existingData.length, node.size());
		assertEquals(newChild.uplinkKey(), first.uplinkKey());
		assertEquals(node, newChild.parent());
		assertEquals(first.uplinkKey(), first.uplinkKey());
		assertEquals(node, first.parent());
	}

}

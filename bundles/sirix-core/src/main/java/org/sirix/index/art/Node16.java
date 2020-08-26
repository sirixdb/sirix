package org.sirix.index.art;

import java.util.Arrays;

class Node16 extends InnerNode {
	static final int NODE_SIZE = 16;
	private final byte[] keys = new byte[NODE_SIZE];

	Node16(Node4 node) {
		super(node, NODE_SIZE);
		assert node.isFull();
		byte[] keys = node.getKeys();
		Node[] child = node.getChild();
		System.arraycopy(keys, 0, this.keys, 0, node.noOfChildren);
		System.arraycopy(child, 0, this.child, 0, node.noOfChildren);

		// update up links
		for (int i = 0; i < noOfChildren; i++) {
			replaceUplink(this, this.child[i]);
		}
	}

	Node16(Node48 node48) {
		super(node48, NODE_SIZE);
		assert node48.shouldShrink();
		byte[] keyIndex = node48.getKeyIndex();
		Node[] children = node48.getChild();

		// keyIndex by virtue of being "array indexed" is already sorted
		// so we can iterate and keep adding into Node16
		for (int i = 0, j = 0; i < Node48.KEY_INDEX_SIZE; i++) {
			if (keyIndex[i] != Node48.ABSENT) {
				child[j] = children[keyIndex[i]];
				keys[j] = BinaryComparableUtils.unsigned(child[j].uplinkKey());
				replaceUplink(this, child[j]);
				j++;
			}
		}
	}

	@Override
	public Node findChild(byte partialKey) {
		// TODO: use simple loop to see if -XX:+SuperWord applies SIMD JVM instrinsics
		partialKey = BinaryComparableUtils.unsigned(partialKey);
		for(int i = 0; i < noOfChildren; i++){
			if(keys[i] == partialKey){
				return child[i];
			}
		}
		return null;
	}

	@Override
	public void addChild(byte partialKey, Node child) {
		assert !isFull();
		byte unsignedPartialKey = BinaryComparableUtils.unsigned(partialKey);

		int index = Arrays.binarySearch(keys, 0, noOfChildren, unsignedPartialKey);
		// the partialKey should not exist
		assert index < 0;
		int insertionPoint = -(index + 1);
		// shift elements from this point to right by one place
		assert insertionPoint <= noOfChildren;
		for (int i = noOfChildren; i > insertionPoint; i--) {
			keys[i] = keys[i - 1];
			this.child[i] = this.child[i - 1];
		}
		keys[insertionPoint] = unsignedPartialKey;
		this.child[insertionPoint] = child;
		noOfChildren++;
		createUplink(this, child, partialKey);
	}

	@Override
	public void replace(byte partialKey, Node newChild) {
		byte unsignedPartialKey = BinaryComparableUtils.unsigned(partialKey);
		int index = Arrays.binarySearch(keys, 0, noOfChildren, unsignedPartialKey);
		assert index >= 0;
		child[index] = newChild;
		createUplink(this, newChild, partialKey);
	}

	@Override
	public void removeChild(byte partialKey) {
		assert !shouldShrink();
		byte unsignedPartialKey = BinaryComparableUtils.unsigned(partialKey);
		int index = Arrays.binarySearch(keys, 0, noOfChildren, unsignedPartialKey);
		// if this fails, the question is, how could you reach the leaf node?
		// this node must've been your follow on pointer holding the partialKey
		assert index >= 0;
		removeUplink(child[index]);
		for (int i = index; i < noOfChildren - 1; i++) {
			keys[i] = keys[i + 1];
			child[i] = child[i + 1];
		}
		child[noOfChildren - 1] = null;
		noOfChildren--;
	}

	@Override
	public InnerNode grow() {
		assert isFull();
		return new Node48(this);
	}

	@Override
	public boolean shouldShrink() {
		return noOfChildren == Node4.NODE_SIZE;
	}

	@Override
	public InnerNode shrink() {
		assert shouldShrink() : "Haven't crossed shrinking threshold yet";
		return new Node4(this);
	}

	@Override
	public Node first() {
		assert noOfChildren > Node4.NODE_SIZE;
		return child[0];
	}

	@Override
	public Node last() {
		assert noOfChildren > Node4.NODE_SIZE;
		return child[noOfChildren - 1];
	}

	@Override
	public Node ceil(byte partialKey) {
		partialKey = BinaryComparableUtils.unsigned(partialKey);
		for (int i = 0; i < noOfChildren; i++) {
			if (keys[i] >= partialKey) {
				return child[i];
			}
		}
		return null;
	}

	@Override
	public Node greater(byte partialKey) {
		partialKey = BinaryComparableUtils.unsigned(partialKey);
		for (int i = 0; i < noOfChildren; i++) {
			if (keys[i] > partialKey) {
				return child[i];
			}
		}
		return null;
	}

	@Override
	public Node lesser(byte partialKey) {
		partialKey = BinaryComparableUtils.unsigned(partialKey);
		for (int i = noOfChildren - 1; i >= 0; i--) {
			if (keys[i] < partialKey) {
				return child[i];
			}
		}
		return null;
	}

	@Override
	public Node floor(byte partialKey) {
		partialKey = BinaryComparableUtils.unsigned(partialKey);
		for (int i = noOfChildren - 1; i >= 0; i--) {
			if (keys[i] <= partialKey) {
				return child[i];
			}
		}
		return null;
	}

	@Override
	public boolean isFull() {
		return noOfChildren == NODE_SIZE;
	}

	byte[] getKeys() {
		return keys;
	}
}

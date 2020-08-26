package org.sirix.index.art;

import java.util.Arrays;

class Node48 extends InnerNode {
	/*
		48 * 8 (child pointers) + 256 = 640 bytes
	*/

	static final int NODE_SIZE = 48;
	static final int KEY_INDEX_SIZE = 256;

	// for partial keys of one byte size, you index directly into this array to find the
	// array index of the child pointer array
	// the index value can only be between 0 to 47 (to index into the child pointer array)
	private final byte[] keyIndex = new byte[KEY_INDEX_SIZE];

	// so that when you use the partial key to index into keyIndex
	// and you see a -1, you know there's no mapping for this key
	static final byte ABSENT = -1;

	Node48(Node16 node) {
		super(node, NODE_SIZE);
		assert node.isFull();

		Arrays.fill(keyIndex, ABSENT);

		byte[] keys = node.getKeys();
		Node[] child = node.getChild();

		for (int i = 0; i < Node16.NODE_SIZE; i++) {
			byte key = BinaryComparableUtils.signed(keys[i]);
			int index = Byte.toUnsignedInt(key);
			keyIndex[index] = (byte) i;
			this.child[i] = child[i];
			// update up link
			replaceUplink(this, this.child[i]);
		}
	}

	Node48(Node256 node256) {
		super(node256, NODE_SIZE);
		assert node256.shouldShrink();
		Arrays.fill(keyIndex, ABSENT);

		Node[] children = node256.getChild();
		byte j = 0;
		for (int i = 0; i < Node256.NODE_SIZE; i++) {
			if (children[i] != null) {
				keyIndex[i] = j;
				child[j] = children[i];
				replaceUplink(this, child[j]);
				j++;
			}
		}
		assert j == NODE_SIZE;
	}

	@Override
	public Node findChild(byte partialKey) {
		byte index = keyIndex[Byte.toUnsignedInt(partialKey)];
		if (index == ABSENT) {
			return null;
		}

		assert index >= 0 && index <= 47;
		return child[index];
	}

	@Override
	public void addChild(byte partialKey, Node child) {
		assert !isFull();
		int index = Byte.toUnsignedInt(partialKey);
		assert keyIndex[index] == ABSENT;
		// find a null place, left fragmented by a removeChild or has always been null
		byte insertPosition = 0;
		for (; this.child[insertPosition] != null && insertPosition < NODE_SIZE; insertPosition++) ;

		this.child[insertPosition] = child;
		keyIndex[index] = insertPosition;
		noOfChildren++;
		createUplink(this, child, partialKey);
	}

	@Override
	public void replace(byte partialKey, Node newChild) {
		byte index = keyIndex[Byte.toUnsignedInt(partialKey)];
		assert index >= 0 && index <= 47;
		child[index] = newChild;
		createUplink(this, newChild, partialKey);
	}

	@Override
	public void removeChild(byte partialKey) {
		assert !shouldShrink();
		int index = Byte.toUnsignedInt(partialKey);
		int pos = keyIndex[index];
		assert pos != ABSENT;
		removeUplink(child[pos]);
		child[pos] = null; // fragment
		keyIndex[index] = ABSENT;
		noOfChildren--;
	}

	@Override
	public InnerNode grow() {
		assert isFull();
		return new Node256(this);
	}

	@Override
	public boolean shouldShrink() {
		return noOfChildren == Node16.NODE_SIZE;
	}

	@Override
	public InnerNode shrink() {
		assert shouldShrink();
		return new Node16(this);
	}

	@Override
	public Node first() {
		assert noOfChildren > Node16.NODE_SIZE;
		int i = 0;
		while(keyIndex[i] == ABSENT)i++;
		return child[keyIndex[i]];
	}

	@Override
	public Node last() {
		assert noOfChildren > Node16.NODE_SIZE;
		int i = KEY_INDEX_SIZE - 1;
        while(keyIndex[i] == ABSENT)i--;
		return child[keyIndex[i]];
	}

	@Override
	public boolean isFull() {
		return noOfChildren == NODE_SIZE;
	}

	@Override
	public Node ceil(byte partialKey) {
		for (int i = Byte.toUnsignedInt(partialKey); i < KEY_INDEX_SIZE; i++) {
			if (keyIndex[i] != ABSENT) {
				return child[keyIndex[i]];
			}
		}
		return null;
	}

	@Override
	public Node greater(byte partialKey) {
		for (int i = Byte.toUnsignedInt(partialKey) + 1; i < KEY_INDEX_SIZE; i++) {
			if (keyIndex[i] != ABSENT) {
				return child[keyIndex[i]];
			}
		}
		return null;
	}

	@Override
	public Node lesser(byte partialKey) {
		for (int i = Byte.toUnsignedInt(partialKey) - 1; i >= 0; i--) {
			if (keyIndex[i] != ABSENT) {
				return child[keyIndex[i]];
			}
		}
		return null;
	}

	@Override
	public Node floor(byte partialKey) {
		for (int i = Byte.toUnsignedInt(partialKey); i >= 0; i--) {
			if (keyIndex[i] != ABSENT) {
				return child[keyIndex[i]];
			}
		}
		return null;
	}


	byte[] getKeyIndex() {
		return keyIndex;
	}
}
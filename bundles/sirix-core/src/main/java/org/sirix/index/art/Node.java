package org.sirix.index.art;

abstract class Node {
	/**
	 * @return child pointer for the smallest partialKey stored in this Node.
	 * 			Returns null if this node has no children.
	 */
	abstract Node first();

	abstract Node firstOrLeaf();

	/**
	 * @return child pointer for the largest partialKey stored in this Node.
	 * 			Returns null if this node has no children.
	 */
	abstract Node last();

	// for upwards traversal
	// dev note: wherever you setup downlinks, you setup uplinks as well
	private InnerNode parent;
	private byte partialKey;

	Node(){}

	// copy ctor. called when growing/shrinking
	Node(Node node) {
		this.partialKey = node.partialKey;
		this.parent = node.parent;
	}

	// do we need partial key for leaf nodes? we'll find out
	static void createUplink(InnerNode parent, LeafNode<?, ?> child) {
		Node c = child;
		c.parent = parent;
	}

	static void createUplink(InnerNode parent, Node child, byte partialKey) {
		child.parent = parent;
		child.partialKey = partialKey;
	}

	// called when growing/shrinking and all children now have a new parent
	static void replaceUplink(InnerNode parent, Node child) {
		child.parent = parent;
	}

	static void removeUplink(Node child) {
		child.parent = null;
	}

	/**
	 * @return the parent of this node. Returns null for root node.
	 */
	public InnerNode parent() {
		return parent;
	}

	/**
	 * @return the uplinking partial key to parent
	 */
	public byte uplinkKey() {
		return partialKey;
	}
}

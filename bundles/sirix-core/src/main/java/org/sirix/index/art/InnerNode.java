package org.sirix.index.art;

/*
	These are internal contracts/interfaces
 	They've been written with only what they're used for internally
 	For example InnerNode#remove could have returned a false indicative of a failed remove
 	due to partialKey entry not actually existing, but the return value is of no use in code till now
 	and is sure to be called from places where it'll surely exist.
 	since they're internal, we could change them later if a better contract makes more sense.

	The impls have assert conditions all around to make sure the methods are called being in the right
	state. For example you should not call shrink() if the Node is not ready to shrink, etc.
	Or for example when calling last() on Node16 or higher, we're sure we'll have at least
	X amount of children hence safe to return child[noOfChildren-1], without worrying about bounds.

 */
abstract class InnerNode extends Node {

  static final int PESSIMISTIC_PATH_COMPRESSION_LIMIT = 8;

  // max limit of 8 bytes (Pessimistic)
  final byte[] prefixKeys;

  // Optimistic
  int prefixLen; // 4 bytes

  // TODO: we could save space by making this a byte and returning
  // Byte.toUnsignedInt wherever comparison with it is done.
  short noOfChildren;

  final Node[] child;

  InnerNode(int size) {
    prefixKeys = new byte[PESSIMISTIC_PATH_COMPRESSION_LIMIT];
    child = new Node[size + 1];
  }

  // copy ctor. called when growing/shrinking
  InnerNode(InnerNode node, int size) {
    super(node);
    child = new Node[size + 1];
    // copy header
    this.noOfChildren = node.noOfChildren;
    this.prefixLen = node.prefixLen;
    this.prefixKeys = node.prefixKeys;

    // copy leaf & replace uplink
    child[size] = node.getLeaf();
    if (child[size] != null) {
      replaceUplink(this, child[size]);
    }
  }

  public void setLeaf(LeafNode<?, ?> leaf) {
    child[child.length - 1] = leaf;
    createUplink(this, leaf);
  }

  public void removeLeaf() {
    removeUplink(child[child.length - 1]);
    child[child.length - 1] = null;
  }

  public boolean hasLeaf() {
    return child[child.length - 1] != null;
  }

  public LeafNode<?, ?> getLeaf() {
    return (LeafNode<?, ?>) child[child.length - 1];
  }

  @Override
  public Node firstOrLeaf() {
    if (hasLeaf()) {
      return getLeaf();
    }
    return first();
  }

  Node[] getChild() {
    return child;
  }

  /**
   * @return no of children this Node has
   */
  public short size() {
    return noOfChildren;
  }

  /**
   * @param partialKey search if this node has an entry for given partialKey
   * @return if it does, then return the following child pointer.
   * Returns null if there is no corresponding entry.
   */
  abstract Node findChild(byte partialKey);

  /**
   * @param partialKey
   * @return a child which is equal or greater than given partial key, or null if there is no such child
   */
  abstract Node ceil(byte partialKey);

  /**
   * @param partialKey
   * @return a child which is equal or lesser than given partial key, or null if there is no such child
   */
  abstract Node floor(byte partialKey);

  /**
   * Note: caller needs to check if {@link InnerNode} {@link #isFull()} before calling this.
   * If it is full then call {@link #grow()} followed by {@link #addChild(byte, Node)} on the new node.
   *
   * @param partialKey partialKey to be mapped
   * @param child      the child node to be added
   */
  abstract void addChild(byte partialKey, Node child);

  /**
   * @param partialKey for which the child pointer mapping is to be updated
   * @param newChild   the new mapping to be added for given partialKey
   */
  abstract void replace(byte partialKey, Node newChild);

  /**
   * @param partialKey for which the child pointer mapping is to be removed
   */
  abstract void removeChild(byte partialKey);

  /**
   * creates and returns the next larger node type with the same mappings as this node
   *
   * @return a new node with the same mappings
   */
  abstract InnerNode grow();

  abstract boolean shouldShrink();

  /**
   * creates and returns the a smaller node type with the same mappings as this node
   *
   * @return a smaller node with the same mappings
   */
  abstract InnerNode shrink();

  /**
   * @return true if Node has reached it's capacity
   */
  abstract boolean isFull();

  /**
   * @return returns the smallest child node for the partialKey strictly greater than the partialKey passed.
   * Returns null if no such child.
   */
  abstract Node greater(byte partialKey);

  /**
   * @return returns the greatest child node for the partialKey strictly lesser than the partialKey passed.
   * Returns null if no such child.
   */
  abstract Node lesser(byte partialKey);
}

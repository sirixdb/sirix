package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.INodeCursor;
import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.TTIOException;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.NullNode;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.settings.EFixed;

/**
 * Simple AVLTree (balanced binary search-tree -- based on BaseX(.org) version).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <K>
 *          the key to search for or insert
 * @param <V>
 *          the value
 */
public class AVLTree<K extends Comparable<? super K>, V> implements INodeCursor {
  /** Tree root node. */
  private AVLNode<K, V> mRoot;

  /** Number of indexed tokens. */
  private int size;

  /** Determines if tree is closed or not. */
  private boolean mClosed;

  /** Strong reference to currently selected node. */
  private INode mCurrentNode;

  /** {@link IPageWriteTrx} for persistent storage. */
  private final IPageWriteTrx mPageWriteTrx;

  /**
   * Private constructor.
   * 
   * @param pPageWriteTrx
   *          {@link IPageWriteTrx} for persistent storage
   */
  private AVLTree(final @Nonnull IPageWriteTrx pPageWriteTrx) {
    mPageWriteTrx = pPageWriteTrx;
    mClosed = false;

    try {
      Optional<? extends INodeBase> node =
        mPageWriteTrx.getNode(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          EPage.VALUEPAGE);
      if (node.isPresent()) {
        mCurrentNode = (INode)node.get();
      } else {
        throw new IllegalStateException(
          "Node couldn't be fetched from persistent storage!");
      }
    } catch (final TTIOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Get a new instance.
   * 
   * @param pPageWriteTrx
   *          {@link IPageWriteTrx} for persistent storage
   * @return new tree instance
   */
  public static AVLTree getInstance(final @Nonnull IPageWriteTrx pPageWriteTrx) {
    return new AVLTree(checkNotNull(pPageWriteTrx));
  }

  /**
   * Checks if the specified token is already indexed; if yes, returns its
   * reference. Otherwise, creates a new index entry and returns a
   * reference of the indexed token.
   * 
   * @param token
   *          token to be indexed
   * @return indexed token reference
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @SuppressWarnings("unchecked")
  public V index(final @Nonnull K pKey, final @Nonnull V pValue)
    throws TTIOException {
    final RevisionRootPage root = mPageWriteTrx.getActualRevisionRootPage();
    // index is empty.. create root node
    if (mRoot == null) {
      mRoot =
        (AVLNode<K, V>)mPageWriteTrx.createNode(new AVLNode<>(pKey, pValue,
          new NodeDelegate(root.getMaxValueNodeKey() + 1, EFixed.NULL_NODE_KEY
            .getStandardProperty(), 0)), EPage.VALUEPAGE);
      final DocumentRootNode document =
        (DocumentRootNode)mPageWriteTrx.prepareNodeForModification(
          EFixed.DOCUMENT_NODE_KEY.getStandardProperty(), EPage.VALUEPAGE);
      document.setFirstChildKey(mRoot.getNodeKey());
      mPageWriteTrx.finishNodeModification(document, EPage.VALUEPAGE);
      size++;
      return pValue;
    }

    AVLNode<K, V> node = mRoot;
    while (true) {
      int c = pKey.compareTo(node.getKey());
      if (c == 0)
        return node.getValue();

      final boolean moved = c < 0 ? moveToLeftSibling() : moveToRightSibling();
      if (moved) {
        node = getAVLNode();
        continue;
      }

      final AVLNode<K, V> child =
        (AVLNode<K, V>)mPageWriteTrx.createNode(new AVLNode<>(pKey, pValue,
          new NodeDelegate(root.getMaxValueNodeKey(), EFixed.NULL_NODE_KEY
            .getStandardProperty(), 0)), EPage.VALUEPAGE);
      node =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(node
          .getNodeKey(), EPage.VALUEPAGE);
      if (c < 0) {
        node.setLeftChildKey(child.getNodeKey());
        adjust(child);
      } else {
        node.setRightChildKey(child.getNodeKey());
        adjust(child);
      }
      mPageWriteTrx.finishNodeModification(node, EPage.VALUEPAGE);
      size++;
      return pValue;
    }
  }

  /**
   * Get the {@link AVLNode}.
   * 
   * @return {@link AVLNode} instance
   */
  private AVLNode<K, V> getAVLNode() {
    if (mCurrentNode.getKind() == EKind.AVL) {
      @SuppressWarnings("unchecked")
      final AVLNode<K, V> node = (AVLNode<K, V>)mCurrentNode;
      return node;
    }
    return null;
  }

  /**
   * Finds the specified key in the index and returns its value.
   * 
   * @param pKey
   *          key to be found
   * @return {@link Optional} reference
   */
  public Optional<V> get(final @Nonnull K pKey) {
    if (mRoot == null) {
      return Optional.absent();
    }
    AVLNode<K, V> node = mRoot;
    while (true) {
      int c = pKey.compareTo(node.getKey());
      if (c == 0) {
        return Optional.fromNullable(node.getValue());
      }
      final boolean moved = c < 0 ? moveToLeftSibling() : moveToRightSibling();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.absent();
  }

  /**
   * Returns the number of index entries.
   * 
   * @return number of index entries
   */
  public int size() {
    return size;
  }

  /**
   * Adjusts the tree balance.
   * 
   * @param pNode
   *          node to be adjusted
   * @throws TTIOException
   */
  private void adjust(AVLNode<K, V> pNode) throws TTIOException {
    pNode.setChanged(true);

    while (pNode != null && pNode != mRoot && parent(pNode) != null
      && parent(pNode).isChanged()) {
      if (parent(pNode) == left(parent(parent(pNode)))) {
        AVLNode<K, V> y = right(parent(parent(pNode)));
        if (y != null && y.isChanged()) {
          setChanged(parent(pNode), false);
          y.setChanged(false);
          setChanged(parent(parent(pNode)), true);
          pNode = parent(parent(pNode));
        } else {
          if (pNode == right(parent(pNode))) {
            pNode = parent(pNode);
            rotateLeft(pNode);
          }
          setChanged(parent(pNode), false);
          setChanged(parent(parent(pNode)), true);
          if (parent(parent(pNode)) != null)
            rotateRight(parent(parent(pNode)));
        }
      } else {
        AVLNode<K, V> y = left(parent(parent(pNode)));
        if (y != null && y.isChanged()) {
          setChanged(parent(pNode), false);
          setChanged(y, false);
          setChanged(parent(parent(pNode)), true);
          pNode = parent(parent(pNode));
        } else {
          if (pNode == left(parent(pNode))) {
            pNode = parent(pNode);
            rotateRight(pNode);
          }
          setChanged(parent(pNode), false);
          setChanged(parent(parent(pNode)), true);
          if (parent(parent(pNode)) != null)
            rotateLeft(parent(parent(pNode)));
        }
      }
    }
    mRoot.setChanged(false);
  }

  /**
   * Set changed value.
   * 
   * @param pNode
   *          node to adjust
   * @param pChanged
   *          changed value
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private void setChanged(final @Nonnull AVLNode<K, V> pNode,
    final boolean pChanged) throws TTIOException {
    @SuppressWarnings("unchecked")
    final AVLNode<K, V> node =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(pNode
        .getNodeKey(), EPage.VALUEPAGE);
    node.setChanged(pChanged);
    mPageWriteTrx.finishNodeModification(node, EPage.VALUEPAGE);
  }

  /**
   * Returns the left child node.
   * 
   * @param node
   * @return left child node
   */
  private AVLNode<K, V> left(@Nullable AVLNode<K, V> pNode) {
    if (pNode == null)
      return null;
    return moveTo(pNode.getLeftChildKey()) ? getAVLNode() : null;
  }

  /**
   * Returns the right child node.
   * 
   * @param node
   * @return right child node
   */
  private AVLNode<K, V> right(@Nullable AVLNode<K, V> pNode) {
    if (pNode == null)
      return null;
    return moveTo(pNode.getRightChildKey()) ? getAVLNode() : null;
  }

  /**
   * Returns the parent node.
   * 
   * @param pNode
   * @return parent sub node
   */
  private AVLNode<K, V> parent(@Nullable AVLNode<K, V> pNode) {
    if (pNode == null)
      return null;
    return moveTo(pNode.getParentKey()) ? getAVLNode() : null;
  }

  /**
   * Left rotation.
   * 
   * @param node
   *          node to be rotated
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @SuppressWarnings({
    "unchecked", "null"
  })
  private void rotateLeft(@Nonnull AVLNode<K, V> pNode) throws TTIOException {
    moveTo(pNode.getNodeKey());

    AVLNode<K, V> right = (AVLNode<K, V>)getLastChild().get();

    pNode =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(pNode
        .getNodeKey(), EPage.VALUEPAGE);
    pNode.setRightChildKey(right.getLeftChildKey());
    mPageWriteTrx.finishNodeModification(pNode, EPage.VALUEPAGE);

    if (right.hasLeftChild()) {
      final AVLNode<K, V> rightLeftChild =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(right
          .getLeftChildKey(), EPage.VALUEPAGE);
      rightLeftChild.setParentKey(pNode.getNodeKey());
      mPageWriteTrx.finishNodeModification(rightLeftChild, EPage.VALUEPAGE);
    }

    right =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(right
        .getNodeKey(), EPage.VALUEPAGE);
    right.setParentKey(pNode.getParentKey());
    mPageWriteTrx.finishNodeModification(right, EPage.VALUEPAGE);

    if (pNode.getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      mRoot = right;
    } else if (moveTo(pNode.getParentKey())
      && getAVLNode().getLeftChildKey() == pNode.getNodeKey()) {
      final AVLNode<K, V> parent =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(getNode()
          .getNodeKey(), EPage.VALUEPAGE);
      parent.setLeftChildKey(right.getNodeKey());
      mPageWriteTrx.finishNodeModification(parent, EPage.VALUEPAGE);
    } else {
      final AVLNode<K, V> parent =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(getNode()
          .getNodeKey(), EPage.VALUEPAGE);
      parent.setRightChildKey(right.getNodeKey());
      mPageWriteTrx.finishNodeModification(parent, EPage.VALUEPAGE);
    }

    right =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(right
        .getNodeKey(), EPage.VALUEPAGE);
    right.setLeftChildKey(pNode.getNodeKey());
    mPageWriteTrx.finishNodeModification(right, EPage.VALUEPAGE);

    pNode =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(pNode
        .getNodeKey(), EPage.VALUEPAGE);
    pNode.setParentKey(right.getNodeKey());
    mPageWriteTrx.finishNodeModification(pNode, EPage.VALUEPAGE);
  }

  /**
   * Right rotation.
   * 
   * @param node
   *          node to be rotated
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @SuppressWarnings({
    "unchecked", "null"
  })
  private void rotateRight(@Nonnull AVLNode<K, V> pNode) throws TTIOException {
    moveTo(pNode.getNodeKey());

    AVLNode<K, V> leftChild = (AVLNode<K, V>)getFirstChild().get();
    pNode =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(pNode
        .getNodeKey(), EPage.VALUEPAGE);
    pNode.setLeftChildKey(leftChild.getRightChildKey());
    mPageWriteTrx.finishNodeModification(pNode, EPage.VALUEPAGE);

    if (leftChild.hasRightChild()) {
      final INode leftRightChild =
        (INode)mPageWriteTrx.prepareNodeForModification(leftChild
          .getRightChildKey(), EPage.VALUEPAGE);
      leftRightChild.setParentKey(pNode.getNodeKey());
      mPageWriteTrx.finishNodeModification(leftRightChild, EPage.VALUEPAGE);
    }

    leftChild =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(leftChild
        .getNodeKey(), EPage.VALUEPAGE);
    leftChild.setParentKey(pNode.getParentKey());
    mPageWriteTrx.finishNodeModification(leftChild, EPage.VALUEPAGE);

    if (pNode.getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      mRoot = leftChild;
    } else if (moveTo(pNode.getParentKey())
      && getAVLNode().getRightChildKey() == pNode.getNodeKey()) {
      final AVLNode<K, V> parent =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(getNode()
          .getNodeKey(), EPage.VALUEPAGE);
      parent.setRightChildKey(leftChild.getNodeKey());
      mPageWriteTrx.finishNodeModification(parent, EPage.VALUEPAGE);
    } else {
      final AVLNode<K, V> parent =
        (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(getNode()
          .getNodeKey(), EPage.VALUEPAGE);
      parent.setLeftChildKey(leftChild.getNodeKey());
      mPageWriteTrx.finishNodeModification(parent, EPage.VALUEPAGE);
    }

    leftChild =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(leftChild
        .getNodeKey(), EPage.VALUEPAGE);
    leftChild.setRightChildKey(pNode.getNodeKey());
    mPageWriteTrx.finishNodeModification(leftChild, EPage.VALUEPAGE);

    pNode =
      (AVLNode<K, V>)mPageWriteTrx.prepareNodeForModification(pNode
        .getNodeKey(), EPage.VALUEPAGE);
    pNode.setParentKey(leftChild.getNodeKey());
    mPageWriteTrx.finishNodeModification(pNode, EPage.VALUEPAGE);
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }

  /**
   * Make sure that the path summary is not yet closed when calling this method.
   */
  final void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Path summary is already closed.");
    }
  }

  /**
   * Set current node.
   * 
   * @param pNode
   */
  public void setCurrentNode(final @Nonnull AVLNode<K, V> pNode) {
    mCurrentNode = checkNotNull(pNode);
  }

  @Override
  public IStructNode getStructuralNode() {
    if (mCurrentNode instanceof IStructNode) {
      return (IStructNode)mCurrentNode;
    } else {
      return new NullNode(mCurrentNode);
    }
  }

  @Override
  public INode getNode() {
    return mCurrentNode;
  }

  @Override
  public boolean moveTo(final long pKey) {
    return false;
  }

  @Override
  public boolean moveToDocumentRoot() {
    return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public boolean moveToParent() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      final AVLNode<K, V> node = getAVLNode();
      if (!node.hasParent()) {
        return false;
      }
      return moveTo(node.getParentKey());
    }
    return false;
  }

  @Override
  public boolean moveToFirstChild() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      final AVLNode<K, V> node = getAVLNode();
      if (!node.hasLeftChild()) {
        return false;
      }
      return moveTo(node.getLeftChildKey());
    }
    return false;
  }

  @Override
  public boolean moveToLastChild() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      final AVLNode<K, V> node = getAVLNode();
      if (!node.hasRightChild()) {
        return false;
      }
      return moveTo(node.getRightChildKey());
    }
    return false;
  }

  @Override
  public boolean moveToLeftSibling() {
    return false;
  }

  @Override
  public boolean moveToRightSibling() {
    return false;
  }

  @Override
  public Optional<IStructNode> moveToAndGetRightSibling() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> moveToAndGetLeftSibling() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> moveToAndGetParent() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> moveToAndGetFirstChild() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> moveToAndGetLastChild() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> getRightSibling() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> getLeftSibling() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> getParent() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> getFirstChild() {
    assertNotClosed();
    return Optional.absent();
  }

  @Override
  public Optional<IStructNode> getLastChild() {
    assertNotClosed();
    return Optional.absent();
  }
}

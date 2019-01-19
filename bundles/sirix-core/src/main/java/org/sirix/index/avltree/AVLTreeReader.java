package org.sirix.index.avltree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Optional;
import javax.annotation.Nonnegative;
import org.sirix.access.trx.node.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import com.google.common.collect.AbstractIterator;

/**
 * Simple AVLTreeReader (balanced binary search-tree -- based on BaseX(.org) version).
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 * @param <K> the key to search for or insert
 * @param <V> the value
 */
public final class AVLTreeReader<K extends Comparable<? super K>, V extends References> implements NodeCursor {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(AVLTreeReader.class));

  /** Determines if tree is closed or not. */
  private boolean mClosed;

  /** Strong reference to currently selected node. */
  private Node mCurrentNode;

  /** {@link PageReadTrx} for persistent storage. */
  final PageReadTrx mPageReadTrx;

  /** Page kind. */
  final PageKind mPageKind;

  /** Index number. */
  final int mIndex;

  /** Determines movement of the internal cursor. */
  public enum MoveCursor {
    /** Cursor should be moved document root. */
    TO_DOCUMENT_ROOT,

    /** Cursor should not be moved. */
    NO_MOVE
  }

  /**
   * Private constructor.
   *
   * @param pageReadTrx {@link PageReadTrx} for persistent storage
   * @param type kind of index
   * @param index the index number
   */
  private AVLTreeReader(final PageReadTrx pageReadTrx, final IndexType type, final int index) {
    mPageReadTrx = checkNotNull(pageReadTrx);
    switch (type) {
      case PATH:
        mPageKind = PageKind.PATHPAGE;
        break;
      case CAS:
        mPageKind = PageKind.CASPAGE;
        break;
      case NAME:
        mPageKind = PageKind.NAMEPAGE;
        break;
      default:
        throw new IllegalStateException();
    }
    mClosed = false;
    mIndex = index;

    try {
      final Optional<? extends Record> node =
          mPageReadTrx.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind, index);
      if (node.isPresent()) {
        mCurrentNode = (Node) node.get();
      } else {
        throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
      }
    } catch (final SirixIOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Get a new instance.
   *
   * @param pageReadTrx {@link PageReadTrx} for persistent storage
   * @param type type of index
   * @return new tree instance
   */
  public static <K extends Comparable<? super K>, V extends References> AVLTreeReader<K, V> getInstance(
      final PageReadTrx pageReadTrx, final IndexType type, final @Nonnegative int index) {
    return new AVLTreeReader<K, V>(pageReadTrx, type, index);
  }

  /**
   * Dump the AVLTree in preorder.
   *
   * @param out {@link PrintStream} to print to
   */
  public void dump(final PrintStream out) {
    assertNotClosed();
    moveToDocumentRoot();
    if (((DocumentRootNode) getNode()).hasFirstChild()) {
      moveToFirstChild();
    } else {
      return;
    }

    internalDump(out);
  }

  // Internal function to dump data to a PrintStream instance.
  private void internalDump(final PrintStream out) {
    out.println(getAVLNode());
    final long nodeKey = getAVLNode().getNodeKey();
    if (getAVLNode().hasLeftChild()) {
      moveToFirstChild();
      internalDump(out);
    }
    moveTo(nodeKey);
    if (getAVLNode().hasRightChild()) {
      moveToLastChild();
      internalDump(out);
    }
  }

  /**
   * Get the {@link AVLNode}.
   *
   * @return {@link AVLNode} instance
   */
  AVLNode<K, V> getAVLNode() {
    assertNotClosed();
    if (mCurrentNode.getKind() != Kind.DOCUMENT) {
      @SuppressWarnings("unchecked")
      final AVLNode<K, V> node = (AVLNode<K, V>) mCurrentNode;
      return node;
    }
    return null;
  }

  /**
   * Finds the specified key in the index and returns its value.
   *
   * @param startNodeKey the key of the node to start from
   * @param key key to be found
   * @param mode the search mode
   * @return {@link Optional} reference (with the found value, or a reference which indicates that the
   *         value hasn't been found)
   */
  public Optional<V> get(final long startNodeKey, final K key, final SearchMode mode) {
    assertNotClosed();
    final boolean movedToStartNode = moveTo(startNodeKey).hasMoved();
    if (!movedToStartNode) {
      return Optional.empty();
    }
    moveToFirstChild();
    AVLNode<K, V> node = getAVLNode();
    while (true) {
      final int c = mode.compare(key, node.getKey());
      if (c == 0) {
        return Optional.ofNullable(node.getValue());
      }
      final boolean moved = c < 0
          ? moveToFirstChild().hasMoved()
          : moveToLastChild().hasMoved();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index and returns its value.
   *
   * @param key key to be found
   * @param mode the search mode
   * @return {@link Optional} reference (with the found value, or a reference which indicates that the
   *         value hasn't been found)
   */
  public Optional<V> get(final K key, final SearchMode mode) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((DocumentRootNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    AVLNode<K, V> node = getAVLNode();
    while (true) {
      final int c = mode.compare(key, node.getKey());
      if (c == 0) {
        return Optional.ofNullable(node.getValue());
      }
      final boolean moved = c < 0
          ? moveToFirstChild().hasMoved()
          : moveToLastChild().hasMoved();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index starting from the specified node key and returns its
   * AVLNode.
   *
   * @param key key to be found
   * @param mode the search mode
   * @return Optional {@link AVLNode} reference
   */
  public Optional<AVLNode<K, V>> getAVLNode(final long startNodeKey, final K key, final SearchMode mode) {
    assertNotClosed();
    final boolean movedToStartNode = moveTo(startNodeKey).hasMoved();
    if (!movedToStartNode) {
      return Optional.empty();
    }
    moveToFirstChild();
    AVLNode<K, V> node = getAVLNode();
    while (true) {
      final int c = mode.compare(key, node.getKey());
      if (c == 0) {
        return Optional.ofNullable(node);
      }
      final boolean moved = c < 0
          ? moveToFirstChild().hasMoved()
          : moveToLastChild().hasMoved();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index and returns its AVLNode.
   *
   * @param key key to be found
   * @param mode the search mode
   * @return Optional {@link AVLNode} reference
   */
  public Optional<AVLNode<K, V>> getAVLNode(final K key, final SearchMode mode) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((DocumentRootNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    AVLNode<K, V> node = getAVLNode();
    while (true) {
      final int c = mode.compare(key, node.getKey());
      if (c == 0) {
        return Optional.ofNullable(node);
      }
      final boolean moved = c < 0
          ? moveToFirstChild().hasMoved()
          : moveToLastChild().hasMoved();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index and returns its AVLNode.
   *
   * @param key key to be found
   * @param mode the search mode
   * @return Optional {@link AVLNode} reference
   */
  public Optional<AVLNode<K, V>> getAVLNode(final K key, final SearchMode mode, final Comparator<? super K> comp) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((DocumentRootNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    AVLNode<K, V> node = getAVLNode();
    while (true) {
      final int c = mode.compare(key, node.getKey(), comp);
      if (c == 0) {
        return Optional.ofNullable(node);
      }
      final boolean moved = c < 0
          ? moveToFirstChild().hasMoved()
          : moveToLastChild().hasMoved();
      if (moved) {
        node = getAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the number of index entries.
   *
   * @return number of index entries
   */
  public long size() {
    return ((DocumentRootNode) moveToDocumentRoot().get().getNode()).getDescendantCount();
  }

  @Override
  public void close() {
    mClosed = true;
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
   * @param node the node to set
   */
  public void setCurrentNode(final AVLNode<K, V> node) {
    mCurrentNode = checkNotNull(node);
  }

  /**
   * Get the structural node of the current node.
   *
   * @return structural node
   */
  private StructNode getStructuralNode() {
    assertNotClosed();
    if (mCurrentNode instanceof StructNode) {
      return (StructNode) mCurrentNode;
    } else {
      return new NullNode(mCurrentNode);
    }
  }

  @Override
  public boolean hasNode(final long key) {
    assertNotClosed();
    final long currKey = mCurrentNode.getNodeKey();
    final boolean moved = moveTo(key).hasMoved();
    final Move<AVLTreeReader<K, V>> movedCursor = moveTo(currKey);
    assert movedCursor.hasMoved() == true : "Must be moveable back!";
    return moved;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    return mCurrentNode.hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      return getAVLNode().hasRightChild();
    }
    return false;
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveTo(final long nodeKey) {
    assertNotClosed();

    if (nodeKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return Move.notMoved();
    }

    // Remember old node and fetch new one.
    final Node oldNode = mCurrentNode;
    Optional<? extends Node> newNode;
    try {
      // Immediately return node from item list if node key negative.
      @SuppressWarnings("unchecked")
      final Optional<? extends Node> node =
          (Optional<? extends Node>) mPageReadTrx.getRecord(nodeKey, mPageKind, mIndex);
      newNode = node;
    } catch (final SirixIOException e) {
      newNode = Optional.empty();
    }

    if (newNode.isPresent()) {
      mCurrentNode = newNode.get();
      return Move.moved(this);
    } else {
      mCurrentNode = oldNode;
      return Move.notMoved();
    }
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToParent() {
    assertNotClosed();
    return moveTo(mCurrentNode.getParentKey());
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToFirstChild() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      final AVLNode<K, V> node = getAVLNode();
      if (!node.hasLeftChild()) {
        return Move.notMoved();
      }
      return moveTo(node.getLeftChildKey());
    }
    return moveTo(((DocumentRootNode) mCurrentNode).getFirstChildKey());
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToLastChild() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      final AVLNode<K, V> node = getAVLNode();
      if (!node.hasRightChild()) {
        return Move.notMoved();
      }
      return moveTo(node.getRightChildKey());
    }
    return Move.notMoved();
  }

  @Override
  public Move<? extends NodeCursor> moveToPrevious() {
    assertNotClosed();
    return moveToParent();
  }

  @Override
  public Move<? extends NodeCursor> moveToNext() {
    assertNotClosed();
    if (mCurrentNode instanceof AVLNode) {
      @SuppressWarnings("unchecked")
      final AVLNode<K, V> node = (AVLNode<K, V>) mCurrentNode;
      if (node.hasLeftChild()) {
        moveToFirstChild();
      } else if (node.hasRightChild()) {
        moveToLastChild();
      } else {
        while (moveToParent().get().getNode() instanceof AVLNode && !hasLastChild()) {
        }

        if (getNode() instanceof AVLNode) {
          return Move.moved(moveToLastChild().get());
        } else {
          return Move.notMoved();
        }
      }
    }
    return Move.moved(moveToFirstChild().get());
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToLeftSibling() {
    assertNotClosed();
    return Move.notMoved();
  }

  @Override
  public Move<AVLTreeReader<K, V>> moveToRightSibling() {
    assertNotClosed();
    return Move.notMoved();
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    return mCurrentNode.getNodeKey();
  }

  @Override
  public Kind getRightSiblingKind() {
    assertNotClosed();
    return Kind.UNKNOWN;
  }

  @Override
  public Kind getLeftSiblingKind() {
    assertNotClosed();
    return Kind.UNKNOWN;
  }

  @Override
  public Kind getFirstChildKind() {
    assertNotClosed();
    final Kind firstChildKind = moveToFirstChild().get().getKind();
    moveToParent();
    return firstChildKind;
  }

  @Override
  public Kind getLastChildKind() {
    assertNotClosed();
    final Kind lastChildKind = moveToLastChild().get().getKind();
    moveToParent();
    return lastChildKind;
  }

  @Override
  public Kind getParentKind() {
    assertNotClosed();
    if (hasParent()) {
      if (mCurrentNode.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        return Kind.DOCUMENT;
      } else {
        final long nodeKey = mCurrentNode.getNodeKey();
        final Kind parentKind = moveToParent().get().getKind();
        moveTo(nodeKey);
        return parentKind;
      }
    }
    return Kind.UNKNOWN;
  }

  @Override
  public Kind getKind() {
    assertNotClosed();
    return mCurrentNode.getKind();
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();
    return mCurrentNode;
  }

  @Override
  public Move<? extends NodeCursor> moveToNextFollowing() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    return Constants.NULL_ID_LONG;
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    return Constants.NULL_ID_LONG;
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();

    final long firstChildKey;

    if (moveToFirstChild().hasMoved()) {
      firstChildKey = getNodeKey();
      moveToParent();
    } else {
      firstChildKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    }

    return firstChildKey;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    final long lastChildKey;

    if (moveToLastChild().hasMoved()) {
      lastChildKey = getNodeKey();
      moveToParent();
    } else {
      lastChildKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    }

    return lastChildKey;
  }

  @Override
  public long getParentKey() {
    return getNode().getParentKey();
  }

  /**
   * Iterator supporting different search modes.
   *
   * @author Johannes Lichtenberger
   *
   */
  public final class AVLNodeIterator extends AbstractIterator<AVLNode<K, V>> {

    /** Determines if it's the first call. */
    private boolean mFirst;

    /** All AVLNode keys which are part of the result sequence. */
    private final Deque<Long> mKeys;

    /** Start node key. */
    private final long mKey;

    /**
     * Constructor.
     *
     * @param nodeKey node key to start from, root node of AVLTree is selected if
     *        {@code Fixed.DOCUMENT_NODE_KEY.getStandardProperty} is specified.
     */
    public AVLNodeIterator(final long nodeKey) {
      mFirst = true;
      mKeys = new ArrayDeque<>();
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      mKey = nodeKey;
    }

    @Override
    protected AVLNode<K, V> computeNext() {
      if (!mFirst) {
        if (!mKeys.isEmpty()) {
          // Subsequent results.
          final AVLNode<K, V> node = moveTo(mKeys.pop()).get().getAVLNode();
          stackOperation(node);
          return node;
        }
        return endOfData();
      }

      // First search.
      mFirst = false;
      boolean moved = moveTo(mKey).hasMoved();
      if (mKey == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        moved = moveToFirstChild().hasMoved();
      }
      if (moved) {
        final AVLNode<K, V> node = getAVLNode();
        stackOperation(node);
        return node;
      }
      return endOfData();
    }

    private void stackOperation(final AVLNode<K, V> node) {
      if (node.hasRightChild()) {
        final AVLNode<K, V> right = moveToLastChild().get().getAVLNode();
        mKeys.push(right.getNodeKey());
      }
      moveTo(node.getNodeKey());
      if (node.hasLeftChild()) {
        final AVLNode<K, V> left = moveToFirstChild().get().getAVLNode();
        mKeys.push(left.getNodeKey());
      }
    }
  }
}

package org.sirix.index.redblacktree;

import com.google.common.collect.AbstractIterator;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.cache.RBIndexKey;
import org.sirix.cache.Cache;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.interfaces.References;
import org.sirix.node.NodeKind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple AVLTreeReader (balanced binary search-tree -- based on BaseX(.org) version).
 *
 * @param <K> the key to search for or insert
 * @param <V> the value
 * @author Johannes Lichtenberger, University of Konstanz
 */
@SuppressWarnings("ConstantConditions")
public final class RBTreeReader<K extends Comparable<? super K>, V extends References> implements NodeCursor {

  /**
   * Cache.
   */
  private final Cache<RBIndexKey, RBNode<?, ?>> cache;

  /**
   * The index type.
   */
  final IndexType indexType;

  /**
   * The index number.
   */
  private final int indexNumber;

  /**
   * The revision number.
   */
  private final int revisionNumber;

  /**
   * Determines if tree is closed or not.
   */
  private boolean isClosed;

  /**
   * Strong reference to currently selected node.
   */
  private Node currentNode;

  /**
   * {@link PageReadOnlyTrx} for persistent storage.
   */
  final PageReadOnlyTrx pageReadOnlyTrx;

  /**
   * Index number.
   */
  final int index;

  /**
   * Determines movement of the internal cursor.
   */
  public enum MoveCursor {
    /**
     * Cursor should be moved document root.
     */
    TO_DOCUMENT_ROOT,

    /**
     * Cursor should not be moved.
     */
    NO_MOVE
  }

  /**
   * Get a new instance.
   *
   * @param <K>         key instance which extends comparable
   * @param <V>         value
   * @param cache       a cache shared between all read-only tree readers
   * @param pageReadTrx {@link PageReadOnlyTrx} for persistent storage
   * @param type        type of index
   * @param index       index
   * @return new tree instance
   */
  public static <K extends Comparable<? super K>, V extends References> RBTreeReader<K, V> getInstance(
      final Cache<RBIndexKey, RBNode<?, ?>> cache, final PageReadOnlyTrx pageReadTrx, final IndexType type,
      @Nonnegative final int index) {
    return new RBTreeReader<>(cache, pageReadTrx, type, index);
  }

  /**
   * Private constructor.
   *
   * @param cache           a cache shared between all read-only tree readers
   * @param pageReadOnlyTrx {@link PageReadOnlyTrx} for persistent storage
   * @param indexType       kind of indexType
   * @param indexNumber     the indexNumber number
   */
  private RBTreeReader(final Cache<RBIndexKey, RBNode<?, ?>> cache, final PageReadOnlyTrx pageReadOnlyTrx,
      final IndexType indexType, final int indexNumber) {
    this.cache = checkNotNull(cache);
    this.pageReadOnlyTrx = checkNotNull(pageReadOnlyTrx);
    this.indexType = checkNotNull(indexType);
    this.indexNumber = indexNumber;
    revisionNumber = pageReadOnlyTrx.getRevisionNumber();
    isClosed = false;
    this.index = indexNumber;

    final Optional<? extends DataRecord> node =
        this.pageReadOnlyTrx.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.PATH_SUMMARY, 0);
    currentNode = (StructNode) node.orElseThrow(() -> new IllegalStateException(
        "Node couldn't be fetched from persistent storage!"));

    for (final long nodeKey : new DescendantAxis(this, IncludeSelf.YES)) {
      if (nodeKey == 0) {
        continue;
      }

      if (pageReadOnlyTrx instanceof PageTrx) {
        continue;
      }
      this.cache.put(new RBIndexKey(nodeKey, revisionNumber, indexType, indexNumber), getCurrentAVLNode());
    }
  }

  /**
   * Dump the AVLTree in preorder.
   *
   * @param out {@link PrintStream} to print to
   */
  public void dump(final PrintStream out) {
    assertNotClosed();
    moveToDocumentRoot();
    if (((StructNode) getNode()).hasFirstChild()) {
      moveToFirstChild();
    } else {
      return;
    }

    internalDump(out);
  }

  // Internal function to dump data to a PrintStream instance.
  private void internalDump(final PrintStream out) {
    out.println(getCurrentAVLNode());
    @SuppressWarnings("ConstantConditions")
    final long nodeKey = getCurrentAVLNode().getNodeKey();
    if (getCurrentAVLNode().hasLeftChild()) {
      moveToFirstChild();
      internalDump(out);
    }
    moveTo(nodeKey);
    if (getCurrentAVLNode().hasRightChild()) {
      moveToLastChild();
      internalDump(out);
    }
  }

  /**
   * Get the {@link RBNode}.
   *
   * @return {@link RBNode} instance
   */
  RBNode<K, V> getCurrentAVLNode() {
    assertNotClosed();
    if (currentNode.getKind() != NodeKind.XML_DOCUMENT && currentNode.getKind() != NodeKind.JSON_DOCUMENT) {
      @SuppressWarnings("unchecked")
      final RBNode<K, V> node = (RBNode<K, V>) currentNode;
      return node;
    }
    return null;
  }

  /**
   * Set the {@link RBNode}.
   *
   * @param node the node to set
   * @return {@link RBNode} instance
   */
  RBNode<K, V> setCurrentAVLNode(final RBNode<K, V> node) {
    assertNotClosed();
    currentNode = node;
    return node;
  }

  /**
   * Finds the specified key in the index and returns its value.
   *
   * @param startNodeKey the key of the node to start from
   * @param key          key to be found
   * @param mode         the search mode
   * @return {@link Optional} reference (with the found value, or a reference which indicates that the
   * value hasn't been found)
   */
  public Optional<V> get(final long startNodeKey, final K key, final SearchMode mode) {
    assertNotClosed();
    final boolean movedToStartNode = moveTo(startNodeKey).hasMoved();
    if (!movedToStartNode) {
      return Optional.empty();
    }
    moveToFirstChild();
    RBNode<K, V> node = getCurrentAVLNode();
    return getNode(key, mode, node);
  }

  @Nonnull
  private Optional<V> getNode(K key, SearchMode mode, RBNode<K, V> node) {
    while (true) {
      final int c = mode.compare(key, node.getKey());
      if (c == 0) {
        return Optional.ofNullable(node.getValue());
      }

      boolean moved;

      if (c < 0) {
        if (node.getLeftChild() != null) {
          node = node.getLeftChild();
          currentNode = node;
          moved = true;
        } else if (node.hasLeftChild()) {
          //noinspection unchecked
          node = pageReadOnlyTrx instanceof PageTrx
              ? null
              : (RBNode<K, V>) cache.get(new RBIndexKey(node.getLeftChildKey(),
                                                        revisionNumber,
                                                        indexType,
                                                        indexNumber));

          if (node == null) {
            moved = moveToFirstChild().hasMoved();
            if (moved) {
              node = getCurrentAVLNode();
            }
          } else {
            currentNode = node;
            moved = true;
          }
        } else {
          moved = false;
        }
      } else {
        if (node.getRightChild() != null) {
          node = node.getRightChild();
          currentNode = node;
          moved = true;
        } else if (node.hasRightChild()) {
          //noinspection unchecked
          node = pageReadOnlyTrx instanceof PageTrx
              ? null
              : (RBNode<K, V>) cache.get(new RBIndexKey(node.getRightChildKey(),
                                                        revisionNumber,
                                                        indexType,
                                                        indexNumber));

          if (node == null) {
            moved = moveToLastChild().hasMoved();
            if (moved) {
              node = getCurrentAVLNode();
            }
          } else {
            currentNode = node;
            moved = true;
          }
        } else {
          moved = false;
        }
      }

      if (!moved) {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index and returns its value.
   *
   * @param key  key to be found
   * @param mode the search mode
   * @return {@link Optional} reference (with the found value, or a reference which indicates that the
   * value hasn't been found)
   */
  public Optional<V> get(final K key, final SearchMode mode) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((StructNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    RBNode<K, V> node = getCurrentAVLNode();
    return getNode(key, mode, node);
  }

  /**
   * Finds the specified key in the index starting from the specified node key and returns its
   * AVLNode.
   *
   * @param startNodeKey specified node
   * @param key          key to be found
   * @param mode         the search mode
   * @return Optional {@link RBNode} reference
   */
  public Optional<RBNode<K, V>> getCurrentAVLNode(final long startNodeKey, final K key, final SearchMode mode) {
    assertNotClosed();
    final boolean movedToStartNode = moveTo(startNodeKey).hasMoved();
    if (!movedToStartNode) {
      return Optional.empty();
    }
    RBNode<K, V> node = getCurrentAVLNode();
    return getTheSearchedNode(key, mode, node);
  }

  @Nonnull
  private Optional<RBNode<K, V>> getTheSearchedNode(K key, SearchMode mode, RBNode<K, V> node) {
    while (true) {
      final int c = key.compareTo(node.getKey());
      if (mode != SearchMode.EQUAL && mode.compare(key, node.getKey()) == 0) {
        return Optional.ofNullable(node);
      }
      if (c == 0) {
        if (mode == SearchMode.EQUAL) {
          return Optional.ofNullable(node);
        }
      }
      final boolean moved = c < 0 ? moveToFirstChild().hasMoved() : moveToLastChild().hasMoved();
      if (moved) {
        node = getCurrentAVLNode();
      } else {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the specified key in the index and returns its AVLNode.
   *
   * @param key  key to be found
   * @param mode the search mode
   * @return Optional {@link RBNode} reference
   */
  public Optional<RBNode<K, V>> getCurrentAVLNode(final K key, final SearchMode mode) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((StructNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    RBNode<K, V> node = getCurrentAVLNode();
    return getTheSearchedNode(key, mode, node);
  }

  /**
   * Finds the specified key in the index and returns its AVLNode.
   *
   * @param key  key to be found
   * @param mode the search mode
   * @param comp comparator to be used to compare keys
   * @return Optional {@link RBNode} reference
   */
  public Optional<RBNode<K, V>> getCurrentAVLNode(final K key, final SearchMode mode,
      final Comparator<? super K> comp) {
    assertNotClosed();
    moveToDocumentRoot();
    if (!((StructNode) getNode()).hasFirstChild()) {
      return Optional.empty();
    }
    moveToFirstChild();
    RBNode<K, V> node = getCurrentAVLNode();
    while (true) {
      final int c = key.compareTo(node.getKey());
      if (mode.compare(key, node.getKey(), comp) == 0) {
        return Optional.ofNullable(node);
      }
      final boolean moved = c < 0 ? moveToFirstChild().hasMoved() : moveToLastChild().hasMoved();
      if (moved) {
        node = getCurrentAVLNode();
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
    return ((StructNode) moveToDocumentRoot().trx().getNode()).getDescendantCount();
  }

  @Override
  public void close() {
    isClosed = true;
  }

  /**
   * Make sure that the path summary is not yet closed when calling this method.
   */
  final void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("AVL tree reader is already closed.");
    }
  }

  /**
   * Get the structural node of the current node.
   *
   * @return structural node
   */
  private StructNode getStructuralNode() {
    assertNotClosed();
    if (currentNode instanceof StructNode) {
      return (StructNode) currentNode;
    } else {
      return new NullNode(currentNode);
    }
  }

  @Override
  public boolean hasNode(final long key) {
    assertNotClosed();
    final long currKey = currentNode.getNodeKey();
    final boolean moved = moveTo(key).hasMoved();
    final Move<RBTreeReader<K, V>> movedCursor = moveTo(currKey);
    assert movedCursor.hasMoved() : "Must be movable back!";
    return moved;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    return currentNode.hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    if (currentNode instanceof RBNode) {
      return getCurrentAVLNode().hasRightChild();
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
  public Move<RBTreeReader<K, V>> moveTo(final long nodeKey) {
    assertNotClosed();

    if (nodeKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return Move.notMoved();
    }

    // Remember old node and fetch new one.
    final Node oldNode = currentNode;
    Optional<? extends Node> newNode;
    try {
      // Immediately return node from item list if node key negative.
      newNode = pageReadOnlyTrx.getRecord(nodeKey, indexType, index);
    } catch (final SirixIOException e) {
      newNode = Optional.empty();
    }

    if (newNode.isPresent()) {
      currentNode = newNode.get();
      return Move.moved(this);
    } else {
      currentNode = oldNode;
      return Move.notMoved();
    }
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToParent() {
    assertNotClosed();
    return moveTo(currentNode.getParentKey());
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToFirstChild() {
    assertNotClosed();
    if (currentNode instanceof RBNode) {
      final RBNode<K, V> node = getCurrentAVLNode();
      if (!node.hasLeftChild()) {
        return Move.notMoved();
      }
      final var move = moveTo(node.getLeftChildKey());
      @SuppressWarnings("unchecked")
      final var currentNode = (RBNode<K, V>) this.currentNode;
      node.setLeftChild(currentNode);
      currentNode.setParent(node);
      return move;
    }
    return moveTo(((StructNode) currentNode).getFirstChildKey());
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToLastChild() {
    assertNotClosed();
    if (currentNode instanceof RBNode) {
      final RBNode<K, V> node = getCurrentAVLNode();
      if (!node.hasRightChild()) {
        return Move.notMoved();
      }
      final var move = moveTo(node.getRightChildKey());
      @SuppressWarnings("unchecked")
      final var currentNode = (RBNode<K, V>) this.currentNode;
      node.setRightChild(currentNode);
      currentNode.setParent(node);
      return move;
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
    if (currentNode instanceof RBNode) {
      @SuppressWarnings("unchecked")
      final RBNode<K, V> node = (RBNode<K, V>) currentNode;
      if (node.hasLeftChild()) {
        moveToFirstChild();
      } else if (node.hasRightChild()) {
        moveToLastChild();
      } else {
        do {
          moveToParent();
        } while (getNode() instanceof RBNode && !hasLastChild());

        if (getNode() instanceof RBNode) {
          return Move.moved(moveToLastChild().trx());
        } else {
          return Move.notMoved();
        }
      }
    }
    if (moveToFirstChild().hasMoved()) {
      return Move.moved(this);
    } else {
      return Move.notMoved();
    }
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToLeftSibling() {
    assertNotClosed();
    return Move.notMoved();
  }

  @Override
  public Move<RBTreeReader<K, V>> moveToRightSibling() {
    assertNotClosed();
    return Move.notMoved();
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    return currentNode.getNodeKey();
  }

  @Override
  public NodeKind getRightSiblingKind() {
    assertNotClosed();
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    assertNotClosed();
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    final NodeKind firstChildKind;
    if (moveToFirstChild().hasMoved()) {
      firstChildKind = getNode().getKind();
      moveToParent();
    } else {
      firstChildKind = NodeKind.UNKNOWN;
    }
    return firstChildKind;
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    final NodeKind lastChildKind;
    if (moveToLastChild().hasMoved()) {
      lastChildKind = getNode().getKind();
      moveToParent();
    } else {
      lastChildKind = NodeKind.UNKNOWN;
    }
    return lastChildKind;
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    if (hasParent()) {
      final long nodeKey = currentNode.getNodeKey();
      final NodeKind parentKind = moveToParent().trx().getKind();
      moveTo(nodeKey);
      return parentKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    return currentNode.getKind();
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();
    return currentNode;
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
   */
  public final class AVLNodeIterator extends AbstractIterator<RBNode<K, V>> {

    /**
     * Determines if it's the first call.
     */
    private boolean first;

    /**
     * All AVLNode keys which are part of the result sequence.
     */
    private final Deque<Long> keys;

    /**
     * Start node key.
     */
    private final long key;

    /**
     * Constructor.
     *
     * @param nodeKey node key to start from, root node of AVLTree is selected if
     *                {@code Fixed.DOCUMENT_NODE_KEY.getStandardProperty} is specified.
     */
    public AVLNodeIterator(final long nodeKey) {
      first = true;
      keys = new ArrayDeque<>();
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      key = nodeKey;
    }

    @Override
    protected RBNode<K, V> computeNext() {
      if (!first) {
        if (!keys.isEmpty()) {
          // Subsequent results.
          final RBNode<K, V> node = moveTo(keys.pop()).trx().getCurrentAVLNode();
          stackOperation(node);
          return node;
        }
        return endOfData();
      }

      // First search.
      first = false;
      boolean moved = moveTo(key).hasMoved();
      if (key == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        moved = moveToFirstChild().hasMoved();
      }
      if (moved) {
        final RBNode<K, V> node = getCurrentAVLNode();
        stackOperation(node);
        return node;
      }
      return endOfData();
    }

    private void stackOperation(final RBNode<K, V> node) {
      if (node.hasRightChild()) {
        final RBNode<K, V> right = moveToLastChild().trx().getCurrentAVLNode();
        keys.push(right.getNodeKey());
      }
      moveTo(node.getNodeKey());
      if (node.hasLeftChild()) {
        final RBNode<K, V> left = moveToFirstChild().trx().getCurrentAVLNode();
        keys.push(left.getNodeKey());
      }
    }
  }
}

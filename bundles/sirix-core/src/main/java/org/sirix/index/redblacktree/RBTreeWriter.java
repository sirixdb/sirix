package org.sirix.index.redblacktree;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.DatabaseType;
import org.sirix.access.trx.node.AbstractForwardingNodeCursor;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.interfaces.References;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.StructNode;
import org.sirix.page.*;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple RBTreeWriter (balanced binary search-tree -- based on BaseX(.org) version).
 *
 * @param <K> the key to search for or insert
 * @param <V> the value
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class RBTreeWriter<K extends Comparable<? super K>, V extends References>
    extends AbstractForwardingNodeCursor {
  /**
   * Logger.
   */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(RBTreeWriter.class));

  /**
   * {@link RBTreeReader} instance.
   */
  private final RBTreeReader<K, V> rbTreeReader;

  /**
   * {@link PageTrx} instance.
   */
  private final PageTrx pageTrx;

  /**
   * Private constructor.
   *
   * @param databaseType The type of database.
   * @param pageTrx      {@link PageTrx} for persistent storage
   * @param type         type of index
   */
  private RBTreeWriter(final DatabaseType databaseType, final PageTrx pageTrx, final IndexType type,
      final @NonNegative int index) {
    try {
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
      final PageReference reference;
      switch (type) {
        case PATH -> {
          // Create path index tree if needed.
          final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
          reference = revisionRootPage.getPathPageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(pathPage, pathPage));
          pathPage.createPathIndexTree(databaseType, pageTrx, index, pageTrx.getLog());
        }
        case CAS -> {
          // Create CAS index tree if needed.
          final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
          reference = revisionRootPage.getCASPageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(casPage, casPage));
          casPage.createCASIndexTree(databaseType, pageTrx, index, pageTrx.getLog());
        }
        case NAME -> {
          // Create name index tree if needed.
          final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
          reference = revisionRootPage.getNamePageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(namePage, namePage));
          namePage.createNameIndexTree(databaseType, pageTrx, index, pageTrx.getLog());
        }
        default -> {
        }
        // Must not happen.
      }
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    rbTreeReader = RBTreeReader.getInstance(pageTrx.getResourceManager().getIndexCache(), pageTrx, type, index);
    this.pageTrx = pageTrx;
  }

  /**
   * Get a new instance.
   *
   * @param databaseType The type of database.
   * @param pageWriteTrx {@link PageTrx} for persistent storage
   * @param type         type of index
   * @param index        the index number
   * @return new tree instance
   */
  public static <K extends Comparable<? super K>, V extends References> RBTreeWriter<K, V> getInstance(
      final DatabaseType databaseType, final PageTrx pageWriteTrx, final IndexType type, final int index) {
    return new RBTreeWriter<>(databaseType, pageWriteTrx, type, index);
  }

  /**
   * Checks if the specified token is already indexed; if yes, returns its reference. Otherwise,
   * creates a new index entry and returns a reference of the indexed token.
   *
   * @param key   token to be indexed
   * @param value node key references
   * @param move  determines if AVLNode cursor must be moved to document root/root node or not
   * @return indexed node key references
   * @throws SirixIOException if an I/O error occurs
   */
  public V index(final K key, final V value, final MoveCursor move) {
    if (move == MoveCursor.TO_DOCUMENT_ROOT) {
      moveToDocumentRoot();
    }
    final RevisionRootPage root = pageTrx.getActualRevisionRootPage();
    if (rbTreeReader.getCurrentNode() == null
        && ((StructNode) getNode()).getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      // Index is empty.. create root node.
      final long nodeKey = getNewNodeKey(root);
      final RBNode<K, V> treeRoot = pageTrx.createRecord(new RBNode<>(key,
                                                                      value,
                                                                      new NodeDelegate(nodeKey,
                                                                                       Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                                       null,
                                                                                       null,
                                                                                       0,
                                                                                       (SirixDeweyID) null)),
                                                         rbTreeReader.indexType,
                                                         rbTreeReader.index);
      final StructNode document = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                       rbTreeReader.indexType,
                                                                       rbTreeReader.index);
      document.setFirstChildKey(treeRoot.getNodeKey());
      document.incrementChildCount();
      document.incrementDescendantCount();
      return value;
    }

    if (move == MoveCursor.TO_DOCUMENT_ROOT || rbTreeReader.getCurrentNode() == null) {
      moveToDocumentRoot();
      moveToFirstChild();
    }
    RBNode<K, V> node = rbTreeReader.getCurrentNode();
    while (true) {
      final int c = key.compareTo(node.getKey());
      if (c == 0) {
        if (!value.equals(node.getValue())) {
          final RBNode<K, V> RBNode =
              pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
          RBNode.setValue(value);
        }
        return node.getValue();
      }

      final boolean moved = c < 0 ? moveToFirstChild() : moveToLastChild();
      if (moved) {
        node = rbTreeReader.getCurrentNode();
        continue;
      }

      final long nodeKey = getNewNodeKey(root);
      final RBNode<K, V> child = pageTrx.createRecord(new RBNode<>(key,
                                                                   value,
                                                                   new NodeDelegate(nodeKey,
                                                                                    node.getNodeKey(),
                                                                                    null,
                                                                                    null,
                                                                                    0,
                                                                                    (SirixDeweyID) null)),
                                                      rbTreeReader.indexType,
                                                      rbTreeReader.index);
      node = pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
      if (c < 0) {
        node.setLeftChildKey(child.getNodeKey());
      } else {
        node.setRightChildKey(child.getNodeKey());
      }
      adjust(child);
      final StructNode document = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                       rbTreeReader.indexType,
                                                                       rbTreeReader.index);
      document.incrementDescendantCount();
      return value;
    }
  }

  /**
   * Get the new maximum node key.
   *
   * @param root the current {@link RevisionRootPage}
   * @return maximum node key
   * @throws SirixIOException If any I/O operation fails
   */
  private long getNewNodeKey(final RevisionRootPage root) {
    // $CASES-OMITTED$
    return switch (rbTreeReader.indexType) {
      case PATH -> pageTrx.getPathPage(root).getMaxNodeKey(rbTreeReader.index) + 1;
      case CAS -> pageTrx.getCASPage(root).getMaxNodeKey(rbTreeReader.index) + 1;
      case NAME -> pageTrx.getNamePage(root).getMaxNodeKey(rbTreeReader.index) + 1;
      case PATH_SUMMARY -> pageTrx.getPathSummaryPage(root).getMaxNodeKey(rbTreeReader.index) + 1;
      default -> throw new IllegalStateException();
    };
  }

  /**
   * Remove a node key from the value, or remove the whole node, if no keys are stored anymore.
   *
   * @param key     the key for which to search the value
   * @param nodeKey the nodeKey to remove from the value
   * @throws SirixIOException if an I/O error occured
   */
  public boolean remove(final K key, final @NonNegative long nodeKey) {
    checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
    final Optional<V> searchedValue = rbTreeReader.get(checkNotNull(key), SearchMode.EQUAL);
    boolean removed = false;
    if (searchedValue.isPresent()) {
      final V value = searchedValue.get();

      removed = value.removeNodeKey(nodeKey);

      if (removed) {
        final RBNode<K, V> node =
            pageTrx.prepareRecordForModification(rbTreeReader.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
        node.getValue().removeNodeKey(nodeKey);
      }
    }
    return removed;
  }

  /**
   * Adjusts the tree balance.
   *
   * @param node node to be adjusted
   * @throws SirixIOException if an I/O error occurs
   */
  private void adjust(RBNode<K, V> node) {
    setChanged(node, true);

    while (node != null && node.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty() && parent(node) != null
        && parent(node).isChanged()) {
      if (parent(node).equals(left(parent(parent(node))))) {
        final RBNode<K, V> y = right(parent(parent(node)));
        if (y != null && y.isChanged()) {
          setChanged(parent(node), false);
          y.setChanged(false);
          setChanged(parent(parent(node)), true);
          node = parent(parent(node));
        } else {
          if (node.equals(right(parent(node)))) {
            node = parent(node);
            rotateLeft(node);
          }
          setChanged(parent(node), false);
          setChanged(parent(parent(node)), true);
          if (parent(parent(node)) != null)
            rotateRight(parent(parent(node)));
        }
      } else if (parent(node).equals(right(parent(parent(node))))) {
        final RBNode<K, V> y = left(parent(parent(node)));
        if (y != null && y.isChanged()) {
          setChanged(parent(node), false);
          setChanged(y, false);
          setChanged(parent(parent(node)), true);
          node = parent(parent(node));
        } else {
          if (node.equals(left(parent(node)))) {
            node = parent(node);
            rotateRight(node);
          }
          setChanged(parent(node), false);
          setChanged(parent(parent(node)), true);
          if (parent(parent(node)) != null)
            rotateLeft(parent(parent(node)));
        }
      } else {
        node = null;
      }
    }

    final long nodeKey = getNodeKey();
    moveToDocumentRoot();
    if (((StructNode) getNode()).hasFirstChild()) {
      moveToFirstChild();
      //noinspection ConstantConditions
      setChanged(rbTreeReader.getCurrentNode(), false);
    }
    moveTo(nodeKey);
  }

  /**
   * Set changed value.
   *
   * @param nodeToChange node to adjust
   * @param changed      changed value
   * @throws SirixIOException if an I/O error occurs
   */
  private void setChanged(final RBNode<K, V> nodeToChange, final boolean changed) {
    final RBNode<K, V> node =
        pageTrx.prepareRecordForModification(nodeToChange.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    node.setChanged(changed);
  }

  /**
   * Returns the left child node.
   *
   * @param node node from which to move to and return the left sibling
   * @return left child node or {@code null}
   */
  private RBNode<K, V> left(@Nullable final RBNode<K, V> node) {
    if (node == null || node.getLeftChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    final var leftChild = node.getLeftChild();
    if (leftChild != null) {
      rbTreeReader.setCurrentNode(leftChild);
      assert node.getLeftChildKey() == leftChild.getNodeKey();
      return leftChild;
    }
    return moveTo(node.getLeftChildKey()) ? rbTreeReader.getCurrentNode() : null;
  }

  /**
   * Returns the right child node.
   *
   * @param node node from which to move to and return the right sibling
   * @return right child node or {@code null}
   */
  private RBNode<K, V> right(@Nullable final RBNode<K, V> node) {
    if (node == null || node.getRightChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    final var rightChild = node.getRightChild();
    if (rightChild != null) {
      rbTreeReader.setCurrentNode(rightChild);
      assert node.getRightChildKey() == rightChild.getNodeKey();
      return rightChild;
    }
    return moveTo(node.getRightChildKey()) ? rbTreeReader.getCurrentNode() : null;
  }

  /**
   * Returns the parent node.
   *
   * @param node node from which to move to and return the parent node
   * @return parent node or {@code null}
   */
  private RBNode<K, V> parent(@Nullable final RBNode<K, V> node) {
    if (node == null || node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    final var parent = node.getParent();
    if (parent != null) {
      rbTreeReader.setCurrentNode(parent);
      assert node.getParentKey() == parent.getNodeKey();
      return parent;
    }
    return moveTo(node.getParentKey()) ? rbTreeReader.getCurrentNode() : null;
  }

  /**
   * Left rotation.
   *
   * @param node node to be rotated
   * @throws SirixIOException if an I/O error occurs
   */
  private void rotateLeft(RBNode<K, V> node) {
    moveTo(node.getNodeKey());

    moveToLastChild();
    RBNode<K, V> right = (RBNode<K, V>) getNode();

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    assert right != null;
    node.setRightChildKey(right.getLeftChildKey());
    node.setRightChild(right.getLeftChild());

    if (right.hasLeftChild()) {
      final RBNode<K, V> rightLeftChild =
          pageTrx.prepareRecordForModification(right.getLeftChildKey(), rbTreeReader.indexType, rbTreeReader.index);
      rightLeftChild.setParentKey(node.getNodeKey());
      rightLeftChild.setParent(node);
    }

    right = pageTrx.prepareRecordForModification(right.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    right.setParentKey(node.getParentKey());
    right.setParent(node.getParent());

    if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      final StructNode parent = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                     rbTreeReader.indexType,
                                                                     rbTreeReader.index);
      parent.setFirstChildKey(right.getNodeKey());
    } else //noinspection ConstantConditions
      if (moveTo(node.getParentKey()) && rbTreeReader.getCurrentNode().getLeftChildKey() == node.getNodeKey()) {
        final RBNode<K, V> parent =
            pageTrx.prepareRecordForModification(rbTreeReader.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
        parent.setLeftChildKey(right.getNodeKey());
        parent.setLeftChild(right);
      } else {
        final RBNode<K, V> parent =
            pageTrx.prepareRecordForModification(rbTreeReader.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
        parent.setRightChildKey(right.getNodeKey());
        parent.setRightChild(right);
      }

    right = pageTrx.prepareRecordForModification(right.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    right.setLeftChildKey(node.getNodeKey());
    right.setLeftChild(node);

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    node.setParentKey(right.getNodeKey());
    node.setParent(right);
  }

  /**
   * Right rotation.
   *
   * @param node node to be rotated
   * @throws SirixIOException if an I/O error occurs
   */
  private void rotateRight(RBNode<K, V> node) {
    moveTo(node.getNodeKey());

    moveToFirstChild();
    RBNode<K, V> leftChild = (RBNode<K, V>) getNode();
    node = pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    assert leftChild != null;
    node.setLeftChildKey(leftChild.getRightChildKey());
    node.setLeftChild(leftChild.getRightChild());

    if (leftChild.hasRightChild()) {
      final RBNode<K, V> leftRightChild = pageTrx.prepareRecordForModification(leftChild.getRightChildKey(),
                                                                               rbTreeReader.indexType,
                                                                               rbTreeReader.index);
      leftRightChild.setParentKey(node.getNodeKey());
      leftRightChild.setParent(node);
    }

    leftChild =
        pageTrx.prepareRecordForModification(leftChild.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    leftChild.setParentKey(node.getParentKey());
    leftChild.setParent(node.getParent());

    if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      final StructNode parent = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                     rbTreeReader.indexType,
                                                                     rbTreeReader.index);
      parent.setFirstChildKey(leftChild.getNodeKey());
    } else //noinspection ConstantConditions
      if (moveTo(node.getParentKey()) && rbTreeReader.getCurrentNode().getRightChildKey() == node.getNodeKey()) {
        final RBNode<K, V> parent =
            pageTrx.prepareRecordForModification(rbTreeReader.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
        parent.setRightChildKey(leftChild.getNodeKey());
        parent.setRightChild(leftChild);
      } else {
        final RBNode<K, V> parent =
            pageTrx.prepareRecordForModification(rbTreeReader.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
        parent.setLeftChildKey(leftChild.getNodeKey());
        parent.setLeftChild(leftChild);
      }

    leftChild =
        pageTrx.prepareRecordForModification(leftChild.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    leftChild.setRightChildKey(node.getNodeKey());
    leftChild.setRightChild(node);

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), rbTreeReader.indexType, rbTreeReader.index);
    node.setParentKey(leftChild.getNodeKey());
    node.setParent(leftChild);
  }

  @Override
  public void close() {
    rbTreeReader.close();
  }

  @Override
  protected NodeCursor delegate() {
    return rbTreeReader;
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
    return rbTreeReader.get(checkNotNull(key), checkNotNull(mode));
  }

  /**
   * Get the {@link RBTreeReader} used to navigate.
   *
   * @return {@link RBTreeReader} reference
   */
  public RBTreeReader<K, V> getReader() {
    return rbTreeReader;
  }
}

package org.sirix.index.avltree;

import org.sirix.access.trx.node.AbstractForwardingNodeCursor;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.page.*;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple AVLTreeWriter (balanced binary search-tree -- based on BaseX(.org) version).
 *
 * @param <K> the key to search for or insert
 * @param <V> the value
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class AVLTreeWriter<K extends Comparable<? super K>, V extends References>
    extends AbstractForwardingNodeCursor {
  /**
   * Logger.
   */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AVLTreeWriter.class));

  /**
   * {@link AVLTreeReader} instance.
   */
  private final AVLTreeReader<K, V> avlTreeReader;

  /**
   * {@link PageTrx} instance.
   */
  private final PageTrx pageTrx;

  /**
   * Private constructor.
   *
   * @param pageTrx {@link PageTrx} for persistent storage
   * @param type    type of index
   */
  private AVLTreeWriter(final PageTrx pageTrx, final IndexType type, final @Nonnegative int index) {
    try {
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
      final PageReference reference;
      switch (type) {
        case PATH:
          // Create path index tree if needed.
          final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
          reference = revisionRootPage.getPathPageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(pathPage, pathPage));
          pathPage.createPathIndexTree(pageTrx, index, pageTrx.getLog());
          break;
        case CAS:
          // Create CAS index tree if needed.
          final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
          reference = revisionRootPage.getCASPageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(casPage, casPage));
          casPage.createCASIndexTree(pageTrx, index, pageTrx.getLog());
          break;
        case NAME:
          // Create name index tree if needed.
          final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
          reference = revisionRootPage.getNamePageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(namePage, namePage));
          namePage.createNameIndexTree(pageTrx, index, pageTrx.getLog());
          break;
        default:
          // Must not happen.
      }
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    avlTreeReader = AVLTreeReader.getInstance(pageTrx, type, index);
    this.pageTrx = pageTrx;
  }

  /**
   * Get a new instance.
   *
   * @param pageWriteTrx {@link PageTrx} for persistent storage
   * @param type         type of index
   * @param index        the index number
   * @return new tree instance
   */
  public static <K extends Comparable<? super K>, V extends References> AVLTreeWriter<K, V> getInstance(
      final PageTrx pageWriteTrx, final IndexType type, final int index) {
    return new AVLTreeWriter<>(pageWriteTrx, type, index);
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
    if (avlTreeReader.getAVLNode() == null
        && ((StructNode) getNode()).getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      // Index is empty.. create root node.
      final long nodeKey = getNewNodeKey(root);
      final AVLNode<K, V> treeRoot = pageTrx.createRecord(nodeKey,
                                                          new AVLNode<>(key,
                                                                       value,
                                                                       new NodeDelegate(nodeKey,
                                                                                        Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                                        null,
                                                                                        null,
                                                                                        0,
                                                                                        null)),
                                                          avlTreeReader.pageKind,
                                                          avlTreeReader.index);
      final StructNode document = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                       avlTreeReader.pageKind,
                                                                       avlTreeReader.index);
      document.setFirstChildKey(treeRoot.getNodeKey());
      document.incrementChildCount();
      document.incrementDescendantCount();
      return value;
    }

    if (move == MoveCursor.TO_DOCUMENT_ROOT || avlTreeReader.getAVLNode() == null) {
      moveToDocumentRoot();
      moveToFirstChild();
    }
    AVLNode<K, V> node = avlTreeReader.getAVLNode();
    while (true) {
      final int c = key.compareTo(node.getKey());
      if (c == 0) {
        if (!value.equals(node.getValue())) {
          final AVLNode<K, V> avlNode =
              pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
          avlNode.setValue(value);
        }
        return node.getValue();
      }

      final boolean moved = c < 0 ? moveToFirstChild().hasMoved() : moveToLastChild().hasMoved();
      if (moved) {
        node = avlTreeReader.getAVLNode();
        continue;
      }

      final long nodeKey = getNewNodeKey(root);
      final AVLNode<K, V> child = pageTrx.createRecord(nodeKey,
                                                       new AVLNode<>(key,
                                                                    value,
                                                                    new NodeDelegate(nodeKey,
                                                                                     node.getNodeKey(),
                                                                                     null,
                                                                                     null,
                                                                                     0,
                                                                                     null)),
                                                       avlTreeReader.pageKind,
                                                       avlTreeReader.index);
      node = pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
      if (c < 0) {
        node.setLeftChildKey(child.getNodeKey());
      } else {
        node.setRightChildKey(child.getNodeKey());
      }
      adjust(child);
      final StructNode document = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                       avlTreeReader.pageKind,
                                                                       avlTreeReader.index);
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
    return switch (avlTreeReader.pageKind) {
      case PATHPAGE -> pageTrx.getPathPage(root).getMaxNodeKey(avlTreeReader.index) + 1;
      case CASPAGE -> pageTrx.getCASPage(root).getMaxNodeKey(avlTreeReader.index) + 1;
      case NAMEPAGE -> pageTrx.getNamePage(root).getMaxNodeKey(avlTreeReader.index) + 1;
      case PATHSUMMARYPAGE -> pageTrx.getPathSummaryPage(root).getMaxNodeKey(avlTreeReader.index) + 1;
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
  public boolean remove(final K key, final @Nonnegative long nodeKey) {
    checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
    final Optional<V> searchedValue = avlTreeReader.get(checkNotNull(key), SearchMode.EQUAL);
    boolean removed = false;
    if (searchedValue.isPresent()) {
      final V value = searchedValue.get();

      removed = value.removeNodeKey(nodeKey);

      if (removed) {
        final AVLNode<K, V> node = pageTrx.prepareRecordForModification(avlTreeReader.getNodeKey(),
                                                                        avlTreeReader.pageKind,
                                                                        avlTreeReader.index);
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
  private void adjust(AVLNode<K, V> node) {
    setChanged(node, true);

    while (node != null && node.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty() && parent(node) != null
        && parent(node).isChanged()) {
      if (parent(node).equals(left(parent(parent(node))))) {
        final AVLNode<K, V> y = right(parent(parent(node)));
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
        final AVLNode<K, V> y = left(parent(parent(node)));
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
      setChanged(avlTreeReader.getAVLNode(), false);
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
  private void setChanged(final AVLNode<K, V> nodeToChange, final boolean changed) {
    final AVLNode<K, V> node =
        pageTrx.prepareRecordForModification(nodeToChange.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    node.setChanged(changed);
  }

  /**
   * Returns the left child node.
   *
   * @param node node from which to move to and return the left sibling
   * @return left child node or {@code null}
   */
  private AVLNode<K, V> left(@Nullable final AVLNode<K, V> node) {
    if (node == null || node.getLeftChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    return moveTo(node.getLeftChildKey()).hasMoved() ? avlTreeReader.getAVLNode() : null;
  }

  /**
   * Returns the right child node.
   *
   * @param node node from which to move to and return the right sibling
   * @return right child node or {@code null}
   */
  private AVLNode<K, V> right(@Nullable final AVLNode<K, V> node) {
    if (node == null || node.getRightChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    return moveTo(node.getRightChildKey()).hasMoved() ? avlTreeReader.getAVLNode() : null;
  }

  /**
   * Returns the parent node.
   *
   * @param node node from which to move to and return the parent node
   * @return parent node or {@code null}
   */
  private AVLNode<K, V> parent(@Nullable final AVLNode<K, V> node) {
    if (node == null || node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }
    return moveTo(node.getParentKey()).hasMoved() ? avlTreeReader.getAVLNode() : null;
  }

  /**
   * Left rotation.
   *
   * @param node node to be rotated
   * @throws SirixIOException if an I/O error occurs
   */
  @SuppressWarnings({ "unchecked" })
  private void rotateLeft(AVLNode<K, V> node) {
    moveTo(node.getNodeKey());

    AVLNode<K, V> right = ((AVLTreeReader<K, V>) moveToLastChild().trx()).getAVLNode();

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    assert right != null;
    node.setRightChildKey(right.getLeftChildKey());

    if (right.hasLeftChild()) {
      final AVLNode<K, V> rightLeftChild =
          pageTrx.prepareRecordForModification(right.getLeftChildKey(), avlTreeReader.pageKind, avlTreeReader.index);
      rightLeftChild.setParentKey(node.getNodeKey());
    }

    right = pageTrx.prepareRecordForModification(right.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    right.setParentKey(node.getParentKey());

    if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      final StructNode parent = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                     avlTreeReader.pageKind,
                                                                     avlTreeReader.index);
      parent.setFirstChildKey(right.getNodeKey());
    } else //noinspection ConstantConditions
      if (moveTo(node.getParentKey()).hasMoved()
        && avlTreeReader.getAVLNode().getLeftChildKey() == node.getNodeKey()) {
      final AVLNode<K, V> parent = pageTrx.prepareRecordForModification(avlTreeReader.getNodeKey(),
                                                                        avlTreeReader.pageKind,
                                                                        avlTreeReader.index);
      parent.setLeftChildKey(right.getNodeKey());
    } else {
      final AVLNode<K, V> parent = pageTrx.prepareRecordForModification(avlTreeReader.getNodeKey(),
                                                                        avlTreeReader.pageKind,
                                                                        avlTreeReader.index);
      parent.setRightChildKey(right.getNodeKey());
    }

    right = pageTrx.prepareRecordForModification(right.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    right.setLeftChildKey(node.getNodeKey());

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    node.setParentKey(right.getNodeKey());
  }

  /**
   * Right rotation.
   *
   * @param node node to be rotated
   * @throws SirixIOException if an I/O error occurs
   */
  @SuppressWarnings({ "unchecked" })
  private void rotateRight(AVLNode<K, V> node) {
    moveTo(node.getNodeKey());

    AVLNode<K, V> leftChild = ((AVLTreeReader<K, V>) moveToFirstChild().trx()).getAVLNode();
    node = pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    assert leftChild != null;
    node.setLeftChildKey(leftChild.getRightChildKey());

    if (leftChild.hasRightChild()) {
      final Node leftRightChild = pageTrx.prepareRecordForModification(leftChild.getRightChildKey(),
                                                                       avlTreeReader.pageKind,
                                                                       avlTreeReader.index);
      leftRightChild.setParentKey(node.getNodeKey());
    }

    leftChild =
        pageTrx.prepareRecordForModification(leftChild.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    leftChild.setParentKey(node.getParentKey());

    if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      final StructNode parent = pageTrx.prepareRecordForModification(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                                     avlTreeReader.pageKind,
                                                                     avlTreeReader.index);
      parent.setFirstChildKey(leftChild.getNodeKey());
    } else //noinspection ConstantConditions
      if (moveTo(node.getParentKey()).hasMoved()
        && avlTreeReader.getAVLNode().getRightChildKey() == node.getNodeKey()) {
      final AVLNode<K, V> parent = pageTrx.prepareRecordForModification(avlTreeReader.getNodeKey(),
                                                                        avlTreeReader.pageKind,
                                                                        avlTreeReader.index);
      parent.setRightChildKey(leftChild.getNodeKey());
    } else {
      final AVLNode<K, V> parent = pageTrx.prepareRecordForModification(avlTreeReader.getNodeKey(),
                                                                        avlTreeReader.pageKind,
                                                                        avlTreeReader.index);
      parent.setLeftChildKey(leftChild.getNodeKey());
    }

    leftChild =
        pageTrx.prepareRecordForModification(leftChild.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    leftChild.setRightChildKey(node.getNodeKey());

    node = pageTrx.prepareRecordForModification(node.getNodeKey(), avlTreeReader.pageKind, avlTreeReader.index);
    node.setParentKey(leftChild.getNodeKey());
  }

  @Override
  public void close() {
    avlTreeReader.close();
  }

  @Override
  protected NodeCursor delegate() {
    return avlTreeReader;
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
    return avlTreeReader.get(checkNotNull(key), checkNotNull(mode));
  }

  /**
   * Get the {@link AVLTreeReader} used to navigate.
   *
   * @return {@link AVLTreeReader} reference
   */
  public AVLTreeReader<K, V> getReader() {
    return avlTreeReader;
  }
}

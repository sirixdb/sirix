package org.sirix.access.trx.node;

import org.sirix.access.User;
import org.sirix.access.trx.page.NodePageReadOnlyTrx;
import org.sirix.api.*;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.NullNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A skeletal implementation of a read-only node transaction.
 * @param <T> the type of node cursor
 */
public abstract class AbstractNodeReadOnlyTrx<T extends NodeCursor & NodeReadOnlyTrx, W extends NodeTrx & NodeCursor,
        N extends ImmutableNode>
        implements InternalNodeReadOnlyTrx<N>, NodeCursor, NodeReadOnlyTrx {

  /** ID of transaction. */
  protected final long id;

  /** State of transaction including all cached stuff. */
  protected PageReadOnlyTrx pageReadOnlyTrx;

  /** The current node. */
  private N currentNode;

  /**
   * Resource manager this write transaction is bound to.
   */
  protected final InternalResourceManager<T, W> resourceManager;

  /**
   * Tracks whether the transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Read-transaction-exclusive item list.
   */
  protected final ItemList<AtomicValue> itemList;

  /**
   * Constructor.
   *
   * @param trxId               the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode        the document root node
   * @param resourceManager     The resource manager for the current transaction
   * @param itemList            Read-transaction-exclusive item list.
   */
  protected AbstractNodeReadOnlyTrx(final @NonNegative long trxId,
                                    final @NonNull PageReadOnlyTrx pageReadTransaction,
                                    final @NonNull N documentNode,
                                    final InternalResourceManager<T, W> resourceManager,
                                    final ItemList<AtomicValue> itemList) {
    checkArgument(trxId >= 0);

    this.itemList = itemList;
    this.resourceManager = checkNotNull(resourceManager);
    this.id = trxId;
    this.pageReadOnlyTrx = checkNotNull(pageReadTransaction);
    this.currentNode = checkNotNull(documentNode);
    this.isClosed = false;
  }

  @Override
  public N getCurrentNode() {
    return currentNode;
  }

  @Override
  public void setCurrentNode(final @Nullable N currentNode) {
    assertNotClosed();
    this.currentNode = currentNode;
  }

  @Override
  public boolean storeDeweyIDs() {
    return resourceManager.getResourceConfig().areDeweyIDsStored;
  }

  @Override
  public ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceManager() {
    return resourceManager;
  }

  @Override
  public Optional<User> getUser() {
    return pageReadOnlyTrx.getActualRevisionRootPage().getUser();
  }

  @Override
  public boolean moveToPrevious() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasLeftSibling()) {
      // Left sibling node.
      boolean leftSiblMove = moveTo(node.getLeftSiblingKey());
      // Now move down to rightmost descendant node if it has one.
      while (hasFirstChild()) {
        leftSiblMove = moveToLastChild();
      }
      return leftSiblMove;
    }
    // Parent node.
    return moveTo(node.getParentKey());
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    assertNotClosed();
    if (currentNode instanceof StructNode && hasLeftSibling()) {
      final long nodeKey = currentNode.getNodeKey();
      moveToLeftSibling();
      final NodeKind leftSiblKind = currentNode.getKind();
      moveTo(nodeKey);
      return leftSiblKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  public boolean moveToLeftSibling() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasLeftSibling()) {
      return false;
    }
    return moveTo(node.getLeftSiblingKey());
  }

  @Override
  public int keyForName(final String name) {
    assertNotClosed();
    return NamePageHash.generateHashForString(name);
  }

  @Override
  public String nameForKey(final int key) {
    assertNotClosed();
    return pageReadOnlyTrx.getName(key, currentNode.getKind());
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node instanceof ObjectKeyNode objectKeyNode) {
      return objectKeyNode.getPathNodeKey();
    }
    if (node instanceof ArrayNode arrayNode) {
      return arrayNode.getPathNodeKey();
    }
    if (node.getKind() == NodeKind.XML_DOCUMENT || node.getKind() == NodeKind.JSON_DOCUMENT) {
      return 0;
    }
    return -1;
  }

  @Override
  public long getId() {
    assertNotClosed();
    return id;
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    return pageReadOnlyTrx.getActualRevisionRootPage().getRevision();
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(pageReadOnlyTrx.getActualRevisionRootPage().getRevisionTimestamp());
  }

  @Override
  public boolean moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public boolean moveToParent() {
    assertNotClosed();
    return moveTo(currentNode.getParentKey());
  }

  @Override
  public boolean moveToFirstChild() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasFirstChild()) {
      return false;
    }
    return moveTo(node.getFirstChildKey());
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    assertNotClosed();

    // Remember old node and fetch new one.
    final N oldNode = currentNode;
    DataRecord newNode;
    try {
      // Immediately return node from item list if node key negative.
      if (nodeKey < 0) {
        if (itemList.size() > 0) {
          newNode = itemList.getItem(nodeKey);
        } else {
          newNode = null;
        }
      } else {
        newNode = getPageTransaction().getRecord(nodeKey, IndexType.DOCUMENT, -1);
      }
    } catch (final SirixIOException | UncheckedIOException | IllegalArgumentException e) {
      newNode = null;
    }

    if (newNode == null) {
      setCurrentNode(oldNode);
      return false;
    } else {
      setCurrentNode((N) newNode);
      return true;
    }
  }

  @Override
  public boolean moveToRightSibling() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasRightSibling()) {
      return false;
    }
    return moveTo(node.getRightSiblingKey());
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    return currentNode.getNodeKey();
  }

  @Override
  public BigInteger getHash() {
    assertNotClosed();
    return currentNode.getHash();
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    return currentNode.getKind();
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  public void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  /**
   * Get the {@link PageReadOnlyTrx}.
   *
   * @return current {@link PageReadOnlyTrx}
   */
  public PageReadOnlyTrx getPageTransaction() {
    assertNotClosed();
    return pageReadOnlyTrx;
  }

  /**
   * Replace the current {@link NodePageReadOnlyTrx}.
   *
   * @param pageReadTransaction {@link NodePageReadOnlyTrx} instance
   */
  public final void setPageReadTransaction(@Nullable final PageReadOnlyTrx pageReadTransaction) {
    assertNotClosed();
    pageReadOnlyTrx = pageReadTransaction;
  }

  @Override
  public final long getMaxNodeKey() {
    assertNotClosed();
    return pageReadOnlyTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    final var node = getCurrentNode();
    if (node instanceof StructNode) {
      return (StructNode) node;
    } else {
      return new NullNode(node);
    }
  }

  @Override
  public boolean moveToNextFollowing() {
    assertNotClosed();
    while (!getStructuralNode().hasRightSibling() && currentNode.hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final @NonNegative long key) {
    assertNotClosed();
    final long nodeKey = currentNode.getNodeKey();
    final boolean retVal = moveTo(key);
    moveTo(nodeKey);
    return retVal;
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
  public boolean hasRightSibling() {
    assertNotClosed();
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    return getStructuralNode().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    return getStructuralNode().getFirstChildKey();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    return currentNode.getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    final long nodeKey = node.getNodeKey();
    moveToParent();
    final NodeKind parentKind = currentNode.getKind();
    moveTo(nodeKey);
    return parentKind;
  }

  @Override
  public boolean moveToNext() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasRightSibling()) {
      // Right sibling node.
      return moveTo(node.getRightSiblingKey());
    }
    // Next following node.
    return moveToNextFollowing();
  }

  protected abstract T thisInstance();

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    final long nodeKey = currentNode.getNodeKey();
    final boolean retVal = moveToLastChild();
    moveTo(nodeKey);
    return retVal;
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final NodeKind lastChildKind = currentNode.getKind();
      moveTo(nodeKey);
      return lastChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node instanceof StructNode && hasFirstChild()) {
      final long nodeKey = node.getNodeKey();
      moveToFirstChild();
      final NodeKind firstChildKind = currentNode.getKind();
      moveTo(nodeKey);
      return firstChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final long lastChildNodeKey = currentNode.getNodeKey();
      moveTo(nodeKey);
      return lastChildNodeKey;
    }
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    assertNotClosed();
    return getStructuralNode().getChildCount();
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
    return getStructuralNode().getDescendantCount();
  }

  @Override
  public NodeKind getPathKind() {
    assertNotClosed();
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getRightSiblingKind() {
    assertNotClosed();
    final ImmutableNode node = currentNode;
    if (node instanceof StructNode && hasRightSibling()) {
      final long nodeKey = node.getNodeKey();
      moveToRightSibling();
      final NodeKind rightSiblKind = currentNode.getKind();
      moveTo(nodeKey);
      return rightSiblKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public PageReadOnlyTrx getPageTrx() {
    return pageReadOnlyTrx;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return pageReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    assertNotClosed();

    return currentNode.getDeweyID();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public synchronized void close() {
    if (!isClosed) {
      // Close own state.
      pageReadOnlyTrx.close();

      // Callback on session to make sure everything is cleaned up.
      resourceManager.closeReadTransaction(id);

      setPageReadTransaction(null);

      // Immediately release all references.
      pageReadOnlyTrx = null;
      currentNode = null;

      // Close state.
      isClosed = true;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AbstractNodeReadOnlyTrx<?, ?, ?> that = (AbstractNodeReadOnlyTrx<?, ?, ?>) o;
    return currentNode.getNodeKey() == that.currentNode.getNodeKey()
            && pageReadOnlyTrx.getRevisionNumber() == that.pageReadOnlyTrx.getRevisionNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentNode.getNodeKey(), pageReadOnlyTrx.getRevisionNumber());
  }
}

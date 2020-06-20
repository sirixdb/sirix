package org.sirix.access.trx.node;

import org.sirix.access.User;
import org.sirix.access.trx.page.NodePageReadOnlyTrx;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.NodeKind;
import org.sirix.node.NullNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A skeletal implementation of a read-only node transaction.
 * @param <T> the type of node cursor
 */
public abstract class AbstractNodeReadOnlyTrx<T extends NodeCursor> implements NodeCursor, NodeReadOnlyTrx {

  /** ID of transaction. */
  private final long id;

  /** State of transaction including all cached stuff. */
  protected PageReadOnlyTrx pageReadOnlyTrx;

  /** The current node. */
  protected ImmutableNode currentNode;

  /**
   * Constructor.
   * @param trxId the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode the document root node
   */
  public AbstractNodeReadOnlyTrx(final @Nonnegative long trxId, final @Nonnull PageReadOnlyTrx pageReadTransaction,
      final @Nonnull ImmutableNode documentNode) {
    checkArgument(trxId >= 0);
    id = trxId;
    pageReadOnlyTrx = checkNotNull(pageReadTransaction);
    currentNode = checkNotNull(documentNode);
  }

  @Override
  public Optional<User> getUser() {
    return pageReadOnlyTrx.getActualRevisionRootPage().getUser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Move<T> moveToPrevious() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasLeftSibling()) {
      // Left sibling node.
      Move<T> leftSiblMove = moveTo(node.getLeftSiblingKey());
      // Now move down to rightmost descendant node if it has one.
      while (leftSiblMove.trx().hasFirstChild()) {
        leftSiblMove = (Move<T>) leftSiblMove.trx().moveToLastChild();
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
  public Move<T> moveToLeftSibling() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasLeftSibling()) {
      return Move.notMoved();
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
    if (node instanceof ObjectKeyNode) {
      return ((ObjectKeyNode) node).getPathNodeKey();
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
  public Move<T> moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public Move<T> moveToParent() {
    assertNotClosed();
    return moveTo(currentNode.getParentKey());
  }

  @Override
  public Move<T> moveToFirstChild() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasFirstChild()) {
      return Move.notMoved();
    }
    return moveTo(node.getFirstChildKey());
  }

  @Override
  public abstract Move<T> moveTo(long key);

  @Override
  public Move<T> moveToRightSibling() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasRightSibling()) {
      return Move.notMoved();
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
  protected abstract void assertNotClosed();

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
    return pageReadOnlyTrx.getActualRevisionRootPage().getMaxNodeKey();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    final ImmutableNode node = currentNode;
    if (node instanceof StructNode) {
      return (StructNode) node;
    } else {
      return new NullNode(node);
    }
  }

  @Override
  public Move<T> moveToNextFollowing() {
    assertNotClosed();
    while (!getStructuralNode().hasRightSibling() && currentNode.hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final @Nonnegative long key) {
    assertNotClosed();
    final long nodeKey = currentNode.getNodeKey();
    final boolean retVal = !moveTo(key).equals(Move.notMoved());
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
  public Move<T> moveToNext() {
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
  public Move<T> moveToLastChild() {
    assertNotClosed();
    if (getStructuralNode().hasFirstChild()) {
      moveToFirstChild();

      while (getStructuralNode().hasRightSibling()) {
        moveToRightSibling();
      }

      return Move.moved(thisInstance());
    }
    return Move.notMoved();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    final long nodeKey = currentNode.getNodeKey();
    final boolean retVal = moveToLastChild() != null;
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
    return currentNode.getDeweyID();
  }
}

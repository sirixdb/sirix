package org.sirix.access.trx.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sirix.access.User;
import org.sirix.access.trx.page.NodePageReadOnlyTrx;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.NodeKind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

/**
 * A skeletal implementation of a read-only node transaction.
 * @param <T> the type of node cursor
 */
public abstract class AbstractNodeReadTrx<T extends NodeCursor> implements NodeCursor, NodeReadOnlyTrx {

  /** ID of transaction. */
  private final long mId;

  /** State of transaction including all cached stuff. */
  protected PageReadOnlyTrx mPageReadTrx;

  /** The current node. */
  protected ImmutableNode mCurrentNode;

  /**
   * Constructor.
   * @param trxId the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode the document root node
   */
  public AbstractNodeReadTrx(final @Nonnegative long trxId, final @Nonnull PageReadOnlyTrx pageReadTransaction,
      final @Nonnull ImmutableNode documentNode) {
    checkArgument(trxId >= 0);
    mId = trxId;
    mPageReadTrx = checkNotNull(pageReadTransaction);
    mCurrentNode = checkNotNull(documentNode);
  }

  @Override
  public Optional<User> getUser() {
    return mPageReadTrx.getActualRevisionRootPage().getUser();
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
    if (mCurrentNode instanceof StructNode && hasLeftSibling()) {
      final long nodeKey = mCurrentNode.getNodeKey();
      moveToLeftSibling();
      final NodeKind leftSiblKind = mCurrentNode.getKind();
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
    return mPageReadTrx.getName(key, mCurrentNode.getKind());
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node.getKind() == NodeKind.XML_DOCUMENT || node.getKind() == NodeKind.JSON_DOCUMENT) {
      return 0;
    }
    return -1;
  }

  @Override
  public long getId() {
    assertNotClosed();
    return mId;
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    return mPageReadTrx.getActualRevisionRootPage().getRevision();
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(mPageReadTrx.getActualRevisionRootPage().getRevisionTimestamp());
  }

  @Override
  public Move<T> moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public Move<T> moveToParent() {
    assertNotClosed();
    return moveTo(mCurrentNode.getParentKey());
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
    return mCurrentNode.getNodeKey();
  }

  @Override
  public BigInteger getHash() {
    assertNotClosed();
    return mCurrentNode.getHash();
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    return mCurrentNode.getKind();
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
    return mPageReadTrx;
  }

  /**
   * Replace the current {@link NodePageReadOnlyTrx}.
   *
   * @param pageReadTransaction {@link NodePageReadOnlyTrx} instance
   */
  public final void setPageReadTransaction(@Nullable final PageReadOnlyTrx pageReadTransaction) {
    assertNotClosed();
    mPageReadTrx = pageReadTransaction;
  }

  @Override
  public final long getMaxNodeKey() {
    assertNotClosed();
    return mPageReadTrx.getActualRevisionRootPage().getMaxNodeKey();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode) {
      return (StructNode) node;
    } else {
      return new NullNode(node);
    }
  }

  @Override
  public Move<T> moveToNextFollowing() {
    assertNotClosed();
    while (!getStructuralNode().hasRightSibling() && mCurrentNode.hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final @Nonnegative long key) {
    assertNotClosed();
    final long nodeKey = mCurrentNode.getNodeKey();
    final boolean retVal = !moveTo(key).equals(Move.notMoved());
    moveTo(nodeKey);
    return retVal;
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
    return mCurrentNode.getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    final long nodeKey = node.getNodeKey();
    moveToParent();
    final NodeKind parentKind = mCurrentNode.getKind();
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
    final long nodeKey = mCurrentNode.getNodeKey();
    final boolean retVal = moveToLastChild() != null;
    moveTo(nodeKey);
    return retVal;
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final NodeKind lastChildKind = mCurrentNode.getKind();
      moveTo(nodeKey);
      return lastChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasFirstChild()) {
      final long nodeKey = node.getNodeKey();
      moveToFirstChild();
      final NodeKind firstChildKind = mCurrentNode.getKind();
      moveTo(nodeKey);
      return firstChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final long lastChildNodeKey = mCurrentNode.getNodeKey();
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
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasRightSibling()) {
      final long nodeKey = node.getNodeKey();
      moveToRightSibling();
      final NodeKind rightSiblKind = mCurrentNode.getKind();
      moveTo(nodeKey);
      return rightSiblKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public PageReadOnlyTrx getPageTrx() {
    return mPageReadTrx;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return mPageReadTrx.getCommitCredentials();
  }

}

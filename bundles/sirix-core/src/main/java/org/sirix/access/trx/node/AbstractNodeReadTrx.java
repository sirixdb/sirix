package org.sirix.access.trx.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sirix.access.trx.page.PageReadTrxImpl;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

public abstract class AbstractNodeReadTrx<T extends NodeCursor> implements NodeCursor, NodeReadOnlyTrx {

  /** ID of transaction. */
  private final long mId;

  /** State of transaction including all cached stuff. */
  protected PageReadOnlyTrx mPageReadTrx;

  protected ImmutableNode mCurrentNode;

  public AbstractNodeReadTrx(final @Nonnegative long trxId, final @Nonnull PageReadOnlyTrx pageReadTransaction,
      final ImmutableNode documentNode) {
    checkArgument(trxId >= 0);
    mId = trxId;
    mPageReadTrx = checkNotNull(pageReadTransaction);
    mCurrentNode = checkNotNull(documentNode);
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
      while (leftSiblMove.getCursor().hasFirstChild()) {
        leftSiblMove = (Move<T>) leftSiblMove.getCursor().moveToLastChild();
      }
      return leftSiblMove;
    }
    // Parent node.
    return moveTo(node.getParentKey());
  }

  @Override
  public Kind getLeftSiblingKind() {
    assertNotClosed();
    if (mCurrentNode instanceof StructNode && hasLeftSibling()) {
      final long nodeKey = mCurrentNode.getNodeKey();
      moveToLeftSibling();
      final Kind leftSiblKind = mCurrentNode.getKind();
      moveTo(nodeKey);
      return leftSiblKind;
    }
    return Kind.UNKNOWN;
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
    if (node.getKind() == Kind.XDM_DOCUMENT) {
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
  public long getRevisionTimestamp() {
    assertNotClosed();
    return mPageReadTrx.getActualRevisionRootPage().getRevisionTimestamp();
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
  public Kind getKind() {
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
   * Replace the current {@link PageReadTrxImpl}.
   *
   * @param pageReadTransaction {@link PageReadTrxImpl} instance
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
    final boolean retVal = moveTo(key).equals(Move.notMoved())
        ? false
        : true;
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
  public Kind getParentKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return Kind.UNKNOWN;
    }
    final long nodeKey = node.getNodeKey();
    moveToParent();
    final Kind parentKind = mCurrentNode.getKind();
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
    final boolean retVal = moveToLastChild() == null
        ? false
        : true;
    moveTo(nodeKey);
    return retVal;
  }

  @Override
  public Kind getLastChildKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final Kind lastChildKind = node.getKind();
      moveTo(nodeKey);
      return lastChildKind;
    }
    return Kind.UNKNOWN;
  }

  @Override
  public Kind getFirstChildKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasFirstChild()) {
      final long nodeKey = node.getNodeKey();
      moveToFirstChild();
      final Kind firstChildKind = node.getKind();
      moveTo(nodeKey);
      return firstChildKind;
    }
    return Kind.UNKNOWN;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasLastChild()) {
      final long nodeKey = node.getNodeKey();
      moveToLastChild();
      final long lastChildNodeKey = node.getNodeKey();
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
  public Kind getPathKind() {
    assertNotClosed();
    return Kind.UNKNOWN;
  }

  @Override
  public Kind getRightSiblingKind() {
    assertNotClosed();
    final ImmutableNode node = mCurrentNode;
    if (node instanceof StructNode && hasRightSibling()) {
      final long nodeKey = node.getNodeKey();
      moveToRightSibling();
      final Kind rightSiblKind = node.getKind();
      moveTo(nodeKey);
      return rightSiblKind;
    }
    return Kind.UNKNOWN;
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

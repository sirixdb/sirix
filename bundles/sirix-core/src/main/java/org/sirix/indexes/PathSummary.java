package org.sirix.indexes;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeTraversal;
import org.sirix.api.IPageReadTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.settings.EFixed;
import org.sirix.utils.Util;

public class PathSummary implements INodeTraversal {

  /** Strong reference to currently selected node. */
  private PathNode mCurrentNode;
  
  private final IPageReadTrx mPageReadTrx;

  private boolean mClosed;
  
  private PathSummary(@Nonnull final IPageReadTrx pPageReadTrx) {
    mPageReadTrx = checkNotNull(pPageReadTrx);
    mClosed = false;
  }
  
  public static final PathSummary getInstance(@Nonnull final IPageReadTrx pPageReadTrx) {
    return new PathSummary(checkNotNull(pPageReadTrx));
  }
  
  public PathNode getNode() {
    return mCurrentNode;
  }
  
  @Override
  public final boolean moveTo(final long pNodeKey) {
    assertNotClosed();
    if (pNodeKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
      return false;
    }

    // Remember old node and fetch new one.
    final PathNode oldNode = mCurrentNode;
    Optional<? extends INode> newNode;
    try {
      // Immediately return node from item list if node key negative.
      newNode = mPageReadTrx.getNode(pNodeKey, EPage.PATHSUMMARY);
    } catch (final TTIOException e) {
      newNode = Optional.absent();
    }

    if (newNode.isPresent()) {
      mCurrentNode = (PathNode)newNode.get();
      return true;
    } else {
      mCurrentNode = oldNode;
      return false;
    }
  }
  
  /**
   * Get {@link QName} of current path node.
   * @return
   */
  public final QName getQName() {
    assertNotClosed();
    if (mCurrentNode instanceof INameNode) {
      final String name =
      mPageReadTrx.getName(((INameNode)mCurrentNode).getNameKey(),
          mCurrentNode.getKind());
      final String uri =
      mPageReadTrx.getName(((INameNode)mCurrentNode).getURIKey(),
          EKind.NAMESPACE);
      return Util.buildQName(uri, name);
    } else {
      return null;
    }
  }
  
  @Override
  public final boolean moveToParent() {
    assertNotClosed();
    return moveTo(mCurrentNode.getParentKey());
  }

  @Override
  public final boolean moveToFirstChild() {
    assertNotClosed();
    if (!mCurrentNode.hasFirstChild()) {
      return false;
    }
    return moveTo(mCurrentNode.getFirstChildKey());
  }

  @Override
  public final boolean moveToLeftSibling() {
    assertNotClosed();
    if (!mCurrentNode.hasLeftSibling()) {
      return false;
    }
    return moveTo(mCurrentNode.getLeftSiblingKey());
  }
  
  @Override
  public final boolean moveToRightSibling() {
    assertNotClosed();
    if (!mCurrentNode.hasRightSibling()) {
      return false;
    }
    return moveTo(mCurrentNode.getRightSiblingKey());
  }
  
  @Override
  public void close() throws AbsTTException {
    if (!mClosed) {
      // Close own state.
      mPageReadTrx.close();

      // Immediately release all references.
      mCurrentNode = null;

      mClosed = true;
    }
  }
  
  /**
   * Make sure that the path summary is not yet closed when calling this method.
   */
  final void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Path summary is already closed.");
    }
  }

  public void setCurrentNode(@Nonnull final PathNode pNode) {
    mCurrentNode = checkNotNull(pNode);
  }

  @Override
  public boolean moveToDocumentRoot() {
    return false;
  }

  @Override
  public IStructNode getStructuralNode() {
    return getNode();
  }
}

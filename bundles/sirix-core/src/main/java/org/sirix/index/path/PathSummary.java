package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.Charset;

import com.google.common.base.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeTraversal;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.node.EKind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.EFixed;
import org.sirix.utils.IConstants;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.Util;

public class PathSummary implements INodeReadTrx {

  /** Strong reference to currently selected node. */
  private INode mCurrentNode;

  private final IPageReadTrx mPageReadTrx;

  /** Determines if path summary is closed or not. */
  private boolean mClosed;

  private PathSummary(@Nonnull final IPageReadTrx pPageReadTrx) {
    mPageReadTrx = pPageReadTrx;
    mClosed = false;
    Optional<INode> node;
    try {
      node =
        mPageReadTrx.getNode(EFixed.ROOT_NODE_KEY.getStandardProperty(),
          EPage.PATHSUMMARY);
      if (node.isPresent()) {
        mCurrentNode = node.get();
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
   * Get a new path summary instance
   * 
   * @param pPageReadTrx
   *          {@link IPageReaderTrx} implementation
   * @return new path summary instance
   */
  public static final PathSummary getInstance(
    @Nonnull final IPageReadTrx pPageReadTrx) {
    return new PathSummary(pPageReadTrx);
  }

  @Override
  public INode getNode() {
    return mCurrentNode;
  }

  @Override
  public final boolean moveTo(final long pNodeKey) {
    assertNotClosed();
    if (pNodeKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
      return false;
    }

    // Remember old node and fetch new one.
    final INode oldNode = mCurrentNode;
    Optional<INode> newNode;
    try {
      // Immediately return node from item list if node key negative.
      newNode = mPageReadTrx.getNode(pNodeKey, EPage.PATHSUMMARY);
    } catch (final TTIOException e) {
      newNode = Optional.absent();
    }

    if (newNode.isPresent()) {
      mCurrentNode = newNode.get();
      return true;
    } else {
      mCurrentNode = oldNode;
      return false;
    }
  }

  @Override
  public final boolean moveToParent() {
    assertNotClosed();
    return moveTo(getStructuralNode().getParentKey());
  }

  @Override
  public final boolean moveToFirstChild() {
    assertNotClosed();
    if (!getStructuralNode().hasFirstChild()) {
      return false;
    }
    return moveTo(getStructuralNode().getFirstChildKey());
  }

  @Override
  public final boolean moveToLeftSibling() {
    assertNotClosed();
    if (!getStructuralNode().hasLeftSibling()) {
      return false;
    }
    return moveTo(getStructuralNode().getLeftSiblingKey());
  }

  @Override
  public final boolean moveToRightSibling() {
    assertNotClosed();
    if (!getStructuralNode().hasRightSibling()) {
      return false;
    }
    return moveTo(getStructuralNode().getRightSiblingKey());
  }

  @Override
  public void close() throws AbsTTException {
    if (!mClosed) {
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
    return moveTo(EFixed.NULL_NODE_KEY.getStandardProperty());
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
  public long getTransactionID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRevisionNumber() throws TTIOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRevisionTimestamp() throws TTIOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMaxNodeKey() throws TTIOException {
    return mPageReadTrx.getActualRevisionRootPage().getMaxPathNodeKey();
  }

  @Override
  public boolean moveToAttribute(@Nonnegative int pIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean moveToAttributeByName(@Nonnull QName pName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean moveToNamespace(@Nonnegative int pIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean moveToNextFollowing() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getValueOfCurrentNode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QName getQNameOfCurrentNode() {
    assertNotClosed();
    final String name =
      mPageReadTrx.getName(((INameNode)mCurrentNode).getNameKey(), mCurrentNode
        .getKind());
    final String uri =
      mPageReadTrx.getName(((INameNode)mCurrentNode).getURIKey(),
        EKind.NAMESPACE);
    return Util.buildQName(uri, name);
  }

  @Override
  public String getTypeOfCurrentNode() {
    assertNotClosed();
    return mPageReadTrx.getName(mCurrentNode.getTypeKey(), getNode().getKind());
  }

  @Override
  public int keyForName(@Nonnull String pName) {
    assertNotClosed();
    return NamePageHash.generateHashForString(pName);
  }

  @Override
  public String nameForKey(int pKey) {
    assertNotClosed();
    if (mCurrentNode instanceof PathNode) {
      final PathNode node = (PathNode)mCurrentNode;
      return mPageReadTrx.getName(pKey, node.getPathKind());
    } else {
      return "";
    }
  }

  @Override
  public byte[] rawNameForKey(int pKey) {
    assertNotClosed();
    if (mCurrentNode instanceof PathNode) {
      final PathNode node = (PathNode)mCurrentNode;
      return mPageReadTrx.getName(pKey, node.getPathKind()).getBytes(
        IConstants.DEFAULT_ENCODING);
    } else {
      return "".getBytes(IConstants.DEFAULT_ENCODING);
    }
  }

  @Override
  public IItemList<AtomicValue> getItemList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public ISession getSession() {
    throw new UnsupportedOperationException();
  }

  @Override
  public INodeReadTrx cloneInstance() throws AbsTTException {
    throw new UnsupportedOperationException();
  }
}

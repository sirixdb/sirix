package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.EKind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.EFixed;
import org.sirix.settings.IConstants;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.Util;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;

/**
 * Path summary organizing the path classes of a resource.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PathSummary implements INodeReadTrx {

	/** Logger. */
	private final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(PathSummary.class));
	
  /** Strong reference to currently selected node. */
  private INode mCurrentNode;

  /** Page reader. */
  private final IPageReadTrx mPageReadTrx;

  /** {@link ISession} reference. */
  private final ISession mSession;

  /** Determines if path summary is closed or not. */
  private boolean mClosed;

  /**
   * Private constructor.
   * 
   * @param pPageReadTrx
   *          page reader
   * @param pSession
   *          {@link ISession} reference
   */
  private PathSummary(final @Nonnull IPageReadTrx pPageReadTrx,
    final @Nonnull ISession pSession) {
    mPageReadTrx = pPageReadTrx;
    mClosed = false;
    mSession = pSession;
    try {
      final Optional<? extends INodeBase> node =
        mPageReadTrx.getNode(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          EPage.PATHSUMMARYPAGE);
      if (node.isPresent()) {
        mCurrentNode = (INode)node.get();
      } else {
        throw new IllegalStateException(
          "Node couldn't be fetched from persistent storage!");
      }
    } catch (final SirixIOException e) {
    	LOGWRAPPER.error(e.getMessage(), e.getCause());
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
    final @Nonnull IPageReadTrx pPageReadTrx, final @Nonnull ISession pSession) {
    return new PathSummary(checkNotNull(pPageReadTrx), checkNotNull(pSession));
  }

  @Override
  public INode getNode() {
    assertNotClosed();
    return mCurrentNode;
  }

  /**
   * Get a path node.
   * 
   * @return {@link PathNode} reference or null for the document root.
   */
  public PathNode getPathNode() {
    assertNotClosed();
    if (mCurrentNode instanceof PathNode) {
      return (PathNode)mCurrentNode;
    } else {
      return null;
    }
  }

  @Override
  public final boolean moveTo(final long pNodeKey) {
    assertNotClosed();

    // Remember old node and fetch new one.
    final INode oldNode = mCurrentNode;
    Optional<? extends INode> newNode;
    try {
      // Immediately return node from item list if node key negative.
      @SuppressWarnings("unchecked")
      final Optional<? extends INode> node =
        (Optional<? extends INode>)mPageReadTrx.getNode(pNodeKey,
          EPage.PATHSUMMARYPAGE);
      newNode = node;
    } catch (final SirixIOException e) {
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
  public void close() throws SirixException {
    if (!mClosed) {
      // Immediately release all references.
      mCurrentNode = null;
      mClosed = true;

      if (mPageReadTrx != null && !mPageReadTrx.isClosed()) {
        mPageReadTrx.close();
      }
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
    return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
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
  public int getRevisionNumber() throws SirixIOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRevisionTimestamp() throws SirixIOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMaxNodeKey() throws SirixIOException {
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
    if (mCurrentNode instanceof INameNode) {
      final String name =
        mPageReadTrx.getName(((INameNode)mCurrentNode).getNameKey(),
          ((PathNode)mCurrentNode).getPathKind());
      final String uri =
        mPageReadTrx.getName(((INameNode)mCurrentNode).getURIKey(),
          EKind.NAMESPACE);
      return Util.buildQName(uri, name);
    } else {
      return null;
    }
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
  public synchronized INodeReadTrx cloneInstance() throws SirixException {
    final INodeReadTrx rtx =
      getInstance(mSession.beginPageReadTrx(mPageReadTrx.getRevisionNumber()),
        mSession);
    rtx.moveTo(mCurrentNode.getNodeKey());
    return rtx;
  }

  @Override
  public boolean moveToLastChild() {
    assertNotClosed();
    if (getStructuralNode().hasFirstChild()) {
      moveToFirstChild();

      while (getStructuralNode().hasRightSibling()) {
        moveToRightSibling();
      }

      return true;
    }
    return false;
  }

  @Override
  public Optional<IStructNode> moveToAndGetRightSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> moveToAndGetLeftSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> moveToAndGetParent() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> moveToAndGetFirstChild() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> moveToAndGetLastChild() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> getRightSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> getLeftSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> getParent() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> getFirstChild() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<IStructNode> getLastChild() {
    return null;
  }

  @Override
  public int getNameCount(@Nonnull String pName, @Nonnull EKind pKind) {
    return mPageReadTrx.getNameCount(NamePageHash.generateHashForString(pName),
      pKind);
  }

  @Override
  public String toString() {
    final ToStringHelper helper = Objects.toStringHelper(this);

    if (mCurrentNode instanceof PathNode) {
      final PathNode node = (PathNode)mCurrentNode;
      helper.add("QName", mPageReadTrx.getName(node.getNameKey(), node
        .getPathKind()));
    }

    helper.add("node", mCurrentNode);
    return helper.toString();
  }
}

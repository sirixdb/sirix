package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
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
public final class PathSummary implements INodeReadTrx {

	/** Logger. */
	private final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(PathSummary.class));

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
			final Optional<? extends INodeBase> node = mPageReadTrx
					.getNode(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
							EPage.PATHSUMMARYPAGE);
			if (node.isPresent()) {
				mCurrentNode = (INode) node.get();
			} else {
				throw new IllegalStateException(
						"Node couldn't be fetched from persistent storage!");
			}
		} catch (final SirixIOException e) {
			LOGWRAPPER.error(e.getMessage(), e.getCause());
		}
	}

	/**
	 * Get a new path summary instance.
	 * 
	 * @param pPageReadTrx
	 *          {@link IPageReaderTrx} implementation
	 * @return new path summary instance
	 */
	public static final PathSummary getInstance(
			final @Nonnull IPageReadTrx pPageReadTrx, final @Nonnull ISession pSession) {
		return new PathSummary(checkNotNull(pPageReadTrx), checkNotNull(pSession));
	}

	/**
	 * Get the node.
	 * 
	 * @return the node
	 */
	private INode getNode() {
		assertNotClosed();
		return mCurrentNode;
	}

	/**
	 * Get a path node.
	 * 
	 * @return {@link PathNode} reference or null for the document root.
	 */
	private PathNode getPathNode() {
		assertNotClosed();
		if (mCurrentNode instanceof PathNode) {
			return (PathNode) mCurrentNode;
		} else {
			return null;
		}
	}

	@Override
	public Move<? extends PathSummary> moveTo(final long pNodeKey) {
		assertNotClosed();

		// Remember old node and fetch new one.
		final INode oldNode = mCurrentNode;
		Optional<? extends INode> newNode;
		try {
			// Immediately return node from item list if node key negative.
			@SuppressWarnings("unchecked")
			final Optional<? extends INode> node = (Optional<? extends INode>) mPageReadTrx
					.getNode(pNodeKey, EPage.PATHSUMMARYPAGE);
			newNode = node;
		} catch (final SirixIOException e) {
			newNode = Optional.absent();
		}

		if (newNode.isPresent()) {
			mCurrentNode = newNode.get();
			return Move.moved(this);
		} else {
			mCurrentNode = oldNode;
			return Moved.notMoved();
		}
	}

	@Override
	public Move<? extends PathSummary> moveToParent() {
		assertNotClosed();
		return moveTo(getStructuralNode().getParentKey());
	}

	@Override
	public Move<? extends PathSummary> moveToFirstChild() {
		assertNotClosed();
		if (!getStructuralNode().hasFirstChild()) {
			return Move.notMoved();
		}
		return moveTo(getStructuralNode().getFirstChildKey());
	}

	@Override
	public Move<? extends PathSummary> moveToLeftSibling() {
		assertNotClosed();
		if (!getStructuralNode().hasLeftSibling()) {
			return Move.notMoved();
		}
		return moveTo(getStructuralNode().getLeftSiblingKey());
	}

	@Override
	public Move<? extends PathSummary> moveToRightSibling() {
		assertNotClosed();
		if (!getStructuralNode().hasRightSibling()) {
			return Move.notMoved();
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
	public Move<? extends PathSummary> moveToDocumentRoot() {
		return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	private IStructNode getStructuralNode() {
		if (mCurrentNode instanceof IStructNode) {
			return (IStructNode) mCurrentNode;
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
	public Move<? extends PathSummary> moveToAttribute(@Nonnegative int pIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<? extends PathSummary> moveToAttributeByName(@Nonnull QName pName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<? extends PathSummary> moveToNamespace(@Nonnegative int pIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<? extends PathSummary> moveToNextFollowing() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public QName getQName() {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			final String name = mPageReadTrx.getName(
					((INameNode) mCurrentNode).getNameKey(),
					((PathNode) mCurrentNode).getPathKind());
			final String uri = mPageReadTrx.getName(
					((INameNode) mCurrentNode).getURIKey(), EKind.NAMESPACE);
			return Util.buildQName(uri, name);
		} else {
			return null;
		}
	}

	@Override
	public String getType() {
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
			final PathNode node = (PathNode) mCurrentNode;
			return mPageReadTrx.getName(pKey, node.getPathKind());
		} else {
			return "";
		}
	}

	@Override
	public byte[] rawNameForKey(int pKey) {
		assertNotClosed();
		if (mCurrentNode instanceof PathNode) {
			final PathNode node = (PathNode) mCurrentNode;
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
		final INodeReadTrx rtx = getInstance(
				mSession.beginPageReadTrx(mPageReadTrx.getRevisionNumber()), mSession);
		rtx.moveTo(mCurrentNode.getNodeKey());
		return rtx;
	}

	@Override
	public Move<? extends PathSummary> moveToLastChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			moveToFirstChild();

			while (getStructuralNode().hasRightSibling()) {
				moveToRightSibling();
			}

			return Moved.moved(this);
		}
		return Moved.notMoved();
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
			final PathNode node = (PathNode) mCurrentNode;
			helper.add("QName",
					mPageReadTrx.getName(node.getNameKey(), node.getPathKind()));
		}

		helper.add("node", mCurrentNode);
		return helper.toString();
	}

	/**
	 * Get level of currently selected path node.
	 * 
	 * @return level of currently selected path node
	 */
	public int getLevel() {
		if (mCurrentNode instanceof PathNode) {
			return getPathNode().getLevel();
		}
		return 0;
	}

	@Override
	public boolean hasNode(final @Nonnegative long pKey) {
		final long currNodeKey = mCurrentNode.getNodeKey();
		final boolean retVal = moveTo(pKey).hasMoved();
		final boolean movedBack = moveTo(currNodeKey).hasMoved();
		assert movedBack : "moveTo(currNodeKey) must succeed!";
		return retVal;
	}

	@Override
	public boolean hasParent() {
		return mCurrentNode.hasParent();
	}

	@Override
	public boolean hasFirstChild() {
		return getStructuralNode().hasFirstChild();
	}

	@Override
	public boolean hasLastChild() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasLeftSibling() {
		return getStructuralNode().hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		return getStructuralNode().hasRightSibling();
	}

	@Override
	public EVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getNodeKey() {
		return mCurrentNode.getNodeKey();
	}

	@Override
	public long getLeftSiblingKey() {
		return getStructuralNode().getLeftSiblingKey();
	}

	@Override
	public long getRightSiblingKey() {
		return getStructuralNode().getRightSiblingKey();
	}

	@Override
	public long getFirstChildKey() {
		return getStructuralNode().getFirstChildKey();
	}

	@Override
	public long getLastChildKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getParentKey() {
		return mCurrentNode.getParentKey();
	}

	@Override
	public int getAttributeCount() {
		return 0;
	}

	@Override
	public int getNamespaceCount() {
		return 0;
	}

	@Override
	public EKind getKind() {
		return EKind.PATH;
	}

	@Override
	public boolean isNameNode() {
		if (mCurrentNode instanceof INameNode) {
			return true;
		}
		return false;
	}

	@Override
	public int getNameKey() {
		if (mCurrentNode instanceof INameNode) {
			return ((INameNode) mCurrentNode).getNameKey();
		}
		return -1;
	}

	@Override
	public int getTypeKey() {
		return mCurrentNode.getTypeKey();
	}

	@Override
	public long getAttributeKey(final @Nonnegative int pIndex) {
		return -1;
	}

	@Override
	public long getPathNodeKey() {
		return -1;
	}

	@Override
	public EKind getPathKind() {
		if (mCurrentNode instanceof PathNode) {
			return ((PathNode) mCurrentNode).getPathKind();
		}
		return EKind.NULL;
	}

	@Override
	public boolean isStructuralNode() {
		return true;
	}

	@Override
	public int getURIKey() {
		if (mCurrentNode instanceof INameNode) {
			return ((INameNode) mCurrentNode).getURIKey();
		}
		return -1;
	}

	@Override
	public long getHash() {
		return mCurrentNode.getHash();
	}

	@Override
	public List<Long> getAttributeKeys() {
		return Collections.emptyList();
	}

	@Override
	public List<Long> getNamespaceKeys() {
		return Collections.emptyList();
	}

	@Override
	public byte[] getRawValue() {
		return null;
	}

	@Override
	public long getChildCount() {
		return getStructuralNode().getChildCount();
	}

	@Override
	public long getDescendantCount() {
		return getStructuralNode().getDescendantCount();
	}

	@Override
	public String getNamespaceURI() {
		return null;
	}
	
	@Override
	public EKind getFirstChildKind() {
		return EKind.PATH;
	}
	
	@Override
	public EKind getLastChildKind() {
		return EKind.PATH;
	}
	
	@Override
	public EKind getLeftSiblingKind() {
		return EKind.PATH;
	}
	
	@Override
	public EKind getParentKind() {
		if (mCurrentNode.getKind() == EKind.DOCUMENT_ROOT) {
			return EKind.DOCUMENT_ROOT;
		}
		return EKind.PATH;
	}
	
	@Override
	public EKind getRightSiblingKind() {
		return EKind.PATH;
	}

	/**
	 * Get references.
	 * 
	 * @return number of references of a node
	 */
	public int getReferences() {
		if (mCurrentNode.getKind() == EKind.DOCUMENT_ROOT) {
			return 1;
		} else {
			return getPathNode().getReferences();
		}
	}
}

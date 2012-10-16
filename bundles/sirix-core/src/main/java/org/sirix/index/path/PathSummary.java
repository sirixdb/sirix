package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.ItemList;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.node.interfaces.StructNode;
import org.sirix.page.PageKind;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.Fixed;
import org.sirix.settings.Constants;
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
public final class PathSummary implements NodeReadTrx {

	/** Logger. */
	private final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(PathSummary.class));

	/** Strong reference to currently selected node. */
	private Node mCurrentNode;

	/** Page reader. */
	private final PageReadTrx mPageReadTrx;

	/** {@link Session} reference. */
	private final Session mSession;

	/** Determines if path summary is closed or not. */
	private boolean mClosed;

	/**
	 * Private constructor.
	 * 
	 * @param pageReadTrx
	 *          page reader
	 * @param session
	 *          {@link Session} reference
	 */
	private PathSummary(final @Nonnull PageReadTrx pageReadTrx,
			final @Nonnull Session session) {
		mPageReadTrx = pageReadTrx;
		mClosed = false;
		mSession = session;
		try {
			final Optional<? extends NodeBase> node = mPageReadTrx.getNode(
					Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
					PageKind.PATHSUMMARYPAGE);
			if (node.isPresent()) {
				mCurrentNode = (Node) node.get();
			} else {
				throw new IllegalStateException(
						"Node couldn't be fetched from persistent storage!");
			}
		} catch (final SirixIOException e) {
			LOGWRAPPER.error(e.getMessage(), e.getCause());
		}
	}

	@Override
	public boolean isValueNode() {
		return false;
	}

	/**
	 * Get a new path summary instance.
	 * 
	 * @param pageReadTrx
	 *          {@link IPageReaderTrx} implementation
	 * @return new path summary instance
	 */
	public static final PathSummary getInstance(
			final @Nonnull PageReadTrx pageReadTrx, final @Nonnull Session session) {
		return new PathSummary(checkNotNull(pageReadTrx), checkNotNull(session));
	}

	@Override
	public Node getNode() {
		assertNotClosed();
		// FIXME: Do not expose a mutable node.
		return mCurrentNode;
	}

	@Override
	public boolean hasAttributes() {
		return getStructuralNode().hasFirstChild();
	}

	@Override
	public boolean hasChildren() {
		return getStructuralNode().getChildCount() > 0;
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
		final Node oldNode = mCurrentNode;
		Optional<? extends Node> newNode;
		try {
			// Immediately return node from item list if node key negative.
			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) mPageReadTrx
					.getNode(pNodeKey, PageKind.PATHSUMMARYPAGE);
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

	@Override
	public Move<? extends PathSummary> moveToDocumentRoot() {
		return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	/**
	 * Get the current node as a structural node.
	 * 
	 * @return structural node
	 */
	private StructNode getStructuralNode() {
		if (mCurrentNode instanceof StructNode) {
			return (StructNode) mCurrentNode;
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
	public Move<? extends PathSummary> moveToAttribute(@Nonnegative int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<? extends PathSummary> moveToAttributeByName(@Nonnull QName name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<? extends PathSummary> moveToNamespace(@Nonnegative int index) {
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
	public QName getName() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			final String name = mPageReadTrx.getName(
					((NameNode) mCurrentNode).getNameKey(),
					((PathNode) mCurrentNode).getPathKind());
			final String uri = mPageReadTrx.getName(
					((NameNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
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
	public String nameForKey(int key) {
		assertNotClosed();
		if (mCurrentNode instanceof PathNode) {
			final PathNode node = (PathNode) mCurrentNode;
			return mPageReadTrx.getName(key, node.getPathKind());
		} else {
			return "";
		}
	}

	@Override
	public byte[] rawNameForKey(int key) {
		assertNotClosed();
		if (mCurrentNode instanceof PathNode) {
			final PathNode node = (PathNode) mCurrentNode;
			return mPageReadTrx.getName(key, node.getPathKind()).getBytes(
					Constants.DEFAULT_ENCODING);
		} else {
			return "".getBytes(Constants.DEFAULT_ENCODING);
		}
	}

	@Override
	public ItemList<AtomicValue> getItemList() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isClosed() {
		return mClosed;
	}

	@Override
	public Session getSession() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized NodeReadTrx cloneInstance() throws SirixException {
		final NodeReadTrx rtx = getInstance(
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
	public int getNameCount(@Nonnull String name, @Nonnull Kind kind) {
		return mPageReadTrx.getNameCount(NamePageHash.generateHashForString(name),
				kind);
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
	public boolean hasNode(final @Nonnegative long key) {
		final long currNodeKey = mCurrentNode.getNodeKey();
		final boolean retVal = moveTo(key).hasMoved();
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
	public VisitResultType acceptVisitor(final @Nonnull Visitor visitor) {
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
	public Kind getKind() {
		return Kind.PATH;
	}

	@Override
	public boolean isNameNode() {
		if (mCurrentNode instanceof NameNode) {
			return true;
		}
		return false;
	}

	@Override
	public int getNameKey() {
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getNameKey();
		}
		return -1;
	}

	@Override
	public int getTypeKey() {
		return mCurrentNode.getTypeKey();
	}

	@Override
	public long getAttributeKey(final @Nonnegative int index) {
		return -1;
	}

	@Override
	public long getPathNodeKey() {
		return -1;
	}

	@Override
	public Kind getPathKind() {
		if (mCurrentNode instanceof PathNode) {
			return ((PathNode) mCurrentNode).getPathKind();
		}
		return Kind.NULL;
	}

	@Override
	public boolean isStructuralNode() {
		return true;
	}

	@Override
	public int getURIKey() {
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getURIKey();
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
	public Kind getFirstChildKind() {
		return Kind.PATH;
	}

	@Override
	public Kind getLastChildKind() {
		return Kind.PATH;
	}

	@Override
	public Kind getLeftSiblingKind() {
		return Kind.PATH;
	}

	@Override
	public Kind getParentKind() {
		if (mCurrentNode.getKind() == Kind.DOCUMENT_ROOT) {
			return Kind.DOCUMENT_ROOT;
		}
		return Kind.PATH;
	}

	@Override
	public Kind getRightSiblingKind() {
		return Kind.PATH;
	}

	/**
	 * Get references.
	 * 
	 * @return number of references of a node
	 */
	public int getReferences() {
		if (mCurrentNode.getKind() == Kind.DOCUMENT_ROOT) {
			return 1;
		} else {
			return getPathNode().getReferences();
		}
	}

	@Override
	public boolean isElement() {
		return false;
	}

	@Override
	public boolean isText() {
		return false;
	}

	@Override
	public boolean isDocumentRoot() {
		return false;
	}

	@Override
	public boolean isComment() {
		return false;
	}

	@Override
	public boolean isAttribute() {
		return false;
	}

	@Override
	public boolean isNamespace() {
		return false;
	}

	@Override
	public boolean isPI() {
		return false;
	}
}

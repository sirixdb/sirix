package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Una;
import org.brackit.xquery.node.parser.NavigationalSubtreeParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Scope;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.type.NodeType;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.AttributeAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NonStructuralWrapperAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.SirixDeweyID;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.stream.SirixStream;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

/**
 * A node which is used to provide all XDM functionality as well as temporal
 * functions.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class DBNode extends AbstractTemporalNode {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DBNode.class));

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mRtx;

	/** Sirix node key. */
	private final long mNodeKey;

	/** Kind of node. */
	private final org.sirix.node.Kind mKind;

	/** Collection this node is part of. */
	private final DBCollection mCollection;

	/** Determines if write-transaction is present. */
	private final boolean mIsWtx;

	/** {@link Scope} of node. */
	private final SirixScope mScope;

	/** Optional dewey ID. */
	private final Optional<SirixDeweyID> mDeweyID;

	/**
	 * Constructor.
	 * 
	 * @param rtx
	 *          {@link NodeReadTrx} for providing reading access to the underlying
	 *          node
	 * @param collection
	 *          {@link DBCollection} reference
	 */
	public DBNode(final @Nonnull NodeReadTrx rtx,
			final @Nonnull DBCollection collection) {
		mCollection = checkNotNull(collection);
		mRtx = checkNotNull(rtx);
		mIsWtx = mRtx instanceof NodeWriteTrx;
		mNodeKey = mRtx.getNodeKey();
		mKind = mRtx.getKind();
		mDeweyID = mRtx.getNode().getDeweyID();
		mScope = new SirixScope(this);
	}

	/**
	 * Create a new {@link IReadTransaction} and move to {@link mKey}.
	 * 
	 * @return new read transaction instance which is moved to {@link mKey}
	 */
	private final void moveRtx() {
		mRtx.moveTo(mNodeKey);
	}

	/**
	 * Get underlying node.
	 * 
	 * @return underlying node
	 */
	public org.sirix.node.interfaces.immutable.ImmutableNode getUnderlyingNode() {
		moveRtx();
		return mRtx.getNode();
	}

	@Override
	public boolean isSelfOf(final @Nonnull Node<?> other) {
		moveRtx();
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (node.getUnderlyingNode().getNodeKey() == this.getUnderlyingNode()
					.getNodeKey()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isParentOf(final @Nonnull Node<?> other) {
		moveRtx();
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (node.getUnderlyingNode().getParentKey() == mRtx.getNodeKey()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isChildOf(final @Nonnull Node<?> other) {
		moveRtx();
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (node.getUnderlyingNode().getNodeKey() == mRtx.getParentKey()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isDescendantOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			moveRtx();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (mDeweyID.isPresent()) {
					mDeweyID.get().isDescendantOf(node.mDeweyID.get());
				} else {
					for (final Axis axis = new AncestorAxis(mRtx); axis.hasNext();) {
						axis.next();
						if (node.getUnderlyingNode().getNodeKey() == mRtx.getNodeKey()) {
							retVal = true;
						}
					}
				}
			}
		}
		return retVal;
	}

	/**
	 * Get the transaction.
	 * 
	 * @return transaction handle
	 */
	public NodeReadTrx getTrx() {
		moveRtx();
		return mRtx;
	}

	@Override
	public boolean isDescendantOrSelfOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (isSelfOf(other)) {
				retVal = true;
			}
			retVal = isDescendantOf(other);
		}
		return retVal;
	}

	@Override
	public boolean isAncestorOf(final @Nonnull Node<?> other) {
		moveRtx();
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (mDeweyID.isPresent()) {
				return mDeweyID.get().isAncestorOf(node.mDeweyID.get());
			} else {
				return other.isDescendantOf(this);
			}
		}
		return false;
	}

	@Override
	public boolean isAncestorOrSelfOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (isSelfOf(other)) {
				retVal = true;
			}
			retVal = other.isDescendantOf(this);
		}
		return retVal;
	}

	@Override
	public boolean isSiblingOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			try {
				if (node.getKind() != Kind.NAMESPACE
						&& node.getKind() != Kind.ATTRIBUTE
						&& ((DBNode) node.getParent()).getUnderlyingNode().getNodeKey() == ((DBNode) other
								.getParent()).getUnderlyingNode().getNodeKey()) {
					retVal = true;
				}
			} catch (final DocumentException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
		return retVal;
	}

	@Override
	public boolean isPrecedingSiblingOf(final @Nonnull Node<?> other) {
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			moveRtx();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (mDeweyID.isPresent()) {
					return mDeweyID.get().isPrecedingSiblingOf(node.mDeweyID.get());
				} else {
					while (mRtx.hasRightSibling()) {
						mRtx.moveToRightSibling();
						if (mRtx.getNodeKey() == node.getNodeKey()) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean isFollowingSiblingOf(final @Nonnull Node<?> other) {
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			moveRtx();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (mDeweyID.isPresent()) {
					return mDeweyID.get().isFollowingSiblingOf(node.mDeweyID.get());
				} else {
					while (mRtx.hasLeftSibling()) {
						mRtx.moveToLeftSibling();
						if (mRtx.getNodeKey() == node.getNodeKey()) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean isPrecedingOf(final @Nonnull Node<?> other) {
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			moveRtx();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (mDeweyID.isPresent()) {
					return mDeweyID.get().isPrecedingOf(node.mDeweyID.get());
				} else {
					for (final Axis axis = new FollowingAxis(mRtx); axis.hasNext();) {
						axis.next();
						if (mRtx.getNodeKey() == node.getNodeKey()) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean isFollowingOf(final @Nonnull Node<?> other) {
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			moveRtx();
			if (mKind != org.sirix.node.Kind.ATTRIBUTE
					&& mKind != org.sirix.node.Kind.NAMESPACE) {
				if (mDeweyID.isPresent()) {
					return mDeweyID.get().isFollowingOf(node.mDeweyID.get());
				} else {
					for (final Axis axis = new PrecedingAxis(mRtx); axis.hasNext();) {
						axis.next();
						if (mRtx.getNodeKey() == node.getNodeKey()) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean isAttributeOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			try {
				if (((DBNode) getParent()).getUnderlyingNode().getNodeKey() == node
						.getUnderlyingNode().getNodeKey()) {
					retVal = true;
				}
			} catch (final DocumentException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
		return retVal;
	}

	@Override
	public boolean isDocumentOf(final @Nonnull Node<?> other) {
		moveRtx();
		boolean retVal = false;
		if (getKind() == Kind.DOCUMENT && other instanceof DBNode) {
			final DBNode node = (DBNode) other;
			assert node.getNodeClassID() == this.getNodeClassID();
			final NodeReadTrx rtx = node.getTrx();
			try {
				if (rtx.getRevisionNumber() == mRtx.getRevisionNumber()
						&& rtx.getSession().getResourceConfig().getID() == mRtx
								.getSession().getResourceConfig().getID()) {
					retVal = true;
				}
			} catch (final SirixIOException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
		return retVal;
	}

	@Override
	public boolean isRoot() {
		moveRtx();
		// TODO: Actually it seems it must check if it's the document root node.
		return mRtx.getNode().getParentKey() == Fixed.NULL_NODE_KEY
				.getStandardProperty() ? true : false;
	}

	@Override
	public int getNodeClassID() {
		return 1732483;
	}

	@Override
	public Collection<AbstractTemporalNode> getCollection() {
		return mCollection;
	}

	@Override
	public Scope getScope() {
		if (mScope == null && mKind == org.sirix.node.Kind.ELEMENT) {
			// mScope = new SirixScope(mCollection);
		}
		return null;
	}

	@Override
	public Kind getKind() {
		moveRtx();
		switch (mRtx.getKind()) {
		case DOCUMENT_ROOT:
			return Kind.DOCUMENT;
		case ELEMENT:
			return Kind.ELEMENT;
		case TEXT:
			return Kind.TEXT;
		case COMMENT:
			return Kind.COMMENT;
		case PROCESSING:
			return Kind.PROCESSING_INSTRUCTION;
		case NAMESPACE:
			return Kind.NAMESPACE;
		case ATTRIBUTE:
			return Kind.ATTRIBUTE;
		default:
			throw new IllegalStateException("Kind not known!");
		}
	}

	@Override
	public QNm getName() throws DocumentException {
		moveRtx();
		final QName name = mRtx.getName();
		if (name == null) {
			return null;
		}
		return new QNm(name.getNamespaceURI(), name.getPrefix(),
				name.getLocalPart());
	}

	@Override
	public void setName(final QNm name) throws OperationNotSupportedException,
			DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			if (wtx.isNameNode()) {
				try {
					wtx.setName(new QName(name.nsURI, name.localName, name.prefix));
				} catch (final SirixException e) {
					throw new DocumentException(e.getCause());
				}
			} else {
				throw new DocumentException("Node has no name!");
			}
		} else {
			throw new DocumentException("Node has no name!");
		}
	}

	@Override
	public Atomic getValue() throws DocumentException {
		moveRtx();
		return new Una(mRtx.getValue());
	}

	@Override
	public void setValue(final Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			if (wtx.isValueNode()) {
				try {
					wtx.setValue(value.stringValue());
				} catch (final SirixException e) {
					throw new DocumentException(e.getCause());
				}
			} else {
				throw new DocumentException("Node has no value!");
			}
		} else {
			throw new DocumentException("Node has no value!");
		}
	}

	@Override
	public DBNode getParent() throws DocumentException {
		moveRtx();
		return mRtx.hasParent() ? new DBNode(mRtx.moveToParent().get(), mCollection)
				: null;
	}

	@Override
	public DBNode getFirstChild() throws DocumentException {
		moveRtx();
		return mRtx.hasFirstChild() ? new DBNode(mRtx.moveToFirstChild().get(),
				mCollection) : null;
	}

	@Override
	public DBNode getLastChild() throws DocumentException {
		moveRtx();
		return mRtx.hasLastChild() ? new DBNode(mRtx.moveToLastChild().get(),
				mCollection) : null;
	}

	@Override
	public Stream<DBNode> getChildren() throws DocumentException {
		moveRtx();
		return new SirixStream(new ChildAxis(mRtx), mCollection);
	}

	// Returns all nodes in the subtree _including_ the subtree root.
	@Override
	public Stream<DBNode> getSubtree() throws DocumentException {
		moveRtx();
		return new SirixStream(new NonStructuralWrapperAxis(new DescendantAxis(
				mRtx, IncludeSelf.YES)), mCollection);
	}

	@Override
	public boolean hasChildren() throws DocumentException {
		return mRtx.getChildCount() > 0;
	}

	@Override
	public DBNode getNextSibling() throws DocumentException {
		moveRtx();
		return mRtx.hasRightSibling() ? new DBNode(mRtx.moveToRightSibling().get(),
				mCollection) : null;
	}

	@Override
	public DBNode getPreviousSibling() throws DocumentException {
		moveRtx();
		return mRtx.hasLeftSibling() ? new DBNode(mRtx.moveToLeftSibling().get(),
				mCollection) : null;
	}

	@Override
	public DBNode append(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				if (wtx.hasFirstChild()) {
					wtx.moveToLastChild();
					switch (kind) {
					case DOCUMENT:
						break;
					case ELEMENT:
						wtx.insertElementAsRightSibling(new QName(name.nsURI,
								name.localName, name.prefix));
						break;
					case ATTRIBUTE:
						wtx.insertAttribute(new QName(name.nsURI, name.localName,
								name.prefix), value.asStr().stringValue());
						break;
					case NAMESPACE:
						wtx.insertNamespace(new QName(name.nsURI, name.localName,
								name.prefix));
						break;
					case TEXT:
						wtx.insertTextAsRightSibling(value.asStr().stringValue());
						break;
					case COMMENT:
						wtx.insertCommentAsRightSibling(value.asStr().stringValue());
						break;
					case PROCESSING_INSTRUCTION:
						wtx.insertPIAsRightSibling(value.asStr().stringValue(),
								name.localName);
						break;
					}
				} else {
					switch (kind) {
					case DOCUMENT:
						break;
					case ELEMENT:
						wtx.insertElementAsFirstChild(new QName(name.nsURI, name.localName,
								name.prefix));
						break;
					case ATTRIBUTE:
						wtx.insertAttribute(new QName(name.nsURI, name.localName,
								name.prefix), value.asStr().stringValue());
						break;
					case NAMESPACE:
						wtx.insertNamespace(new QName(name.nsURI, name.localName,
								name.prefix));
						break;
					case TEXT:
						wtx.insertTextAsFirstChild(value.asStr().stringValue());
						break;
					case COMMENT:
						wtx.insertCommentAsFirstChild(value.asStr().stringValue());
						break;
					case PROCESSING_INSTRUCTION:
						wtx.insertPIAsFirstChild(value.asStr().stringValue(),
								name.localName);
						break;
					}
				}
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode append(final @Nonnull Node<?> child)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				if (mRtx.hasFirstChild()) {
					mRtx.moveToLastChild();
				}

				final SubtreeBuilder builder = new SubtreeBuilder(mCollection, (NodeWriteTrx) mRtx,
						Insert.ASRIGHTSIBLING,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				child.parse(builder);
				mRtx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode append(final @Nonnull SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				if (mRtx.hasFirstChild()) {
					mRtx.moveToLastChild();
				}
				
				parser.parse(new SubtreeBuilder(mCollection, (NodeWriteTrx) mRtx,
						Insert.ASRIGHTSIBLING, Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList()));
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
			moveRtx();
			mRtx.moveToFirstChild();
			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode prepend(final Kind kind, final QNm name, final Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				switch (kind) {
				case DOCUMENT:
					break;
				case ELEMENT:
					wtx.insertElementAsFirstChild(new QName(name.nsURI, name.localName,
							name.prefix));
					break;
				case ATTRIBUTE:
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value.asStr()
									.stringValue());
					break;
				case NAMESPACE:
					wtx.insertNamespace(new QName(name.nsURI, name.localName, name.prefix));
					break;
				case TEXT:
					wtx.insertTextAsFirstChild(value.asStr().stringValue());
					break;
				case COMMENT:
					wtx.insertCommentAsFirstChild(value.asStr().stringValue());
					break;
				case PROCESSING_INSTRUCTION:
					wtx.insertPIAsFirstChild(value.asStr().stringValue(), name.localName);
					break;
				}
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode prepend(final Node<?> child)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				final SubtreeBuilder builder = new SubtreeBuilder(mCollection, wtx,
						Insert.ASFIRSTCHILD,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				child.parse(builder);
				mRtx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode prepend(final @Nonnull SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				parser.parse(new SubtreeBuilder(mCollection, (NodeWriteTrx) mRtx,
						Insert.ASFIRSTCHILD, Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList()));
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
			moveRtx();
			mRtx.moveToFirstChild();
			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode insertBefore(final Kind kind, final QNm name, final Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				switch (kind) {
				case DOCUMENT:
					break;
				case ELEMENT:
					wtx.insertElementAsLeftSibling(new QName(name.nsURI, name.localName,
							name.prefix));
					break;
				case ATTRIBUTE:
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value.asStr()
									.stringValue());
					break;
				case NAMESPACE:
					wtx.insertNamespace(new QName(name.nsURI, name.localName, name.prefix));
					break;
				case TEXT:
					wtx.insertTextAsLeftSibling(value.asStr().stringValue());
					break;
				case COMMENT:
					wtx.insertCommentAsLeftSibling(value.asStr().stringValue());
					break;
				case PROCESSING_INSTRUCTION:
					wtx.insertPIAsLeftSibling(value.asStr().stringValue(), name.localName);
					break;
				}
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode insertBefore(final Node<?> node)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				final SubtreeBuilder builder = new SubtreeBuilder(mCollection, wtx,
						Insert.ASLEFTSIBLING,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				node.parse(builder);
				mRtx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode insertBefore(final SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				final SubtreeBuilder builder = new SubtreeBuilder(mCollection,
						(NodeWriteTrx) mRtx, Insert.ASLEFTSIBLING,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				parser.parse(builder);
				return new DBNode(mRtx.moveTo(builder.getStartNodeKey()).get(),
						mCollection);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public AbstractTemporalNode insertAfter(final Kind kind, final QNm name,
			final Atomic value) throws OperationNotSupportedException,
			DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				switch (kind) {
				case DOCUMENT:
					break;
				case ELEMENT:
					wtx.insertElementAsRightSibling(new QName(name.nsURI, name.localName,
							name.prefix));
					break;
				case ATTRIBUTE:
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value.asStr()
									.stringValue());
					break;
				case NAMESPACE:
					wtx.insertNamespace(new QName(name.nsURI, name.localName, name.prefix));
					break;
				case TEXT:
					wtx.insertTextAsRightSibling(value.asStr().stringValue());
					break;
				case COMMENT:
					wtx.insertCommentAsRightSibling(value.asStr().stringValue());
					break;
				case PROCESSING_INSTRUCTION:
					wtx.insertPIAsRightSibling(value.asStr().stringValue(),
							name.localName);
					break;
				}
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode insertAfter(final Node<?> node)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				final SubtreeBuilder builder = new SubtreeBuilder(mCollection, wtx,
						Insert.ASRIGHTSIBLING,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				node.parse(builder);
				mRtx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode insertAfter(final @Nonnull SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				final SubtreeBuilder builder = new SubtreeBuilder(mCollection,
						(NodeWriteTrx) mRtx, Insert.ASRIGHTSIBLING,
						Collections
								.<SubtreeListener<? super AbstractTemporalNode>> emptyList());
				parser.parse(builder);
				return new DBNode(mRtx.moveTo(builder.getStartNodeKey()).get(),
						mCollection);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public DBNode setAttribute(final Node<?> attribute)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			if (wtx.isElement()) {
				try {
					final String value = attribute.getValue().asStr().stringValue();
					final QNm name = attribute.getName();
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value);
					return new DBNode(mRtx, mCollection);
				} catch (final SirixException e) {
					throw new DocumentException(e.getCause());
				}
			}
		}
		return null;
	}

	@Override
	public DBNode setAttribute(final QNm name, final Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			if (wtx.isElement()) {
				try {
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value.asStr()
									.stringValue());
					return new DBNode(mRtx, mCollection);
				} catch (final SirixException e) {
					throw new DocumentException(e.getCause());
				}
			}
		}
		return null;
	}

	@Override
	public boolean deleteAttribute(final QNm pName)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			if (wtx.isElement()) {
				if (wtx.moveToAttributeByName(
						new QName(pName.nsURI, pName.localName, pName.prefix)).hasMoved()) {
					try {
						wtx.remove();
						return true;
					} catch (final SirixException e) {
						throw new DocumentException(e.getCause());
					}
				}
				throw new DocumentException("No attribute with name " + pName
						+ " exists!");
			}
			throw new DocumentException("No element node selected!");
		}
		return false;
	}

	@Override
	public Stream<DBNode> getAttributes() throws OperationNotSupportedException,
			DocumentException {
		moveRtx();
		return new SirixStream(new AttributeAxis(mRtx), mCollection);
	}

	@Override
	public DBNode getAttribute(final QNm name) throws DocumentException {
		moveRtx();
		if (mRtx.isElement()
				&& mRtx.moveToAttributeByName(
						new QName(name.nsURI, name.localName, name.prefix)).hasMoved()) {
			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public DBNode replaceWith(final Node<?> node)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx && node instanceof DBNode) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			final DBNode other = (DBNode) node;
			try {
				final NodeReadTrx rtx = other.getTrx();
				rtx.moveTo(other.getNodeKey());
				wtx.replaceNode(rtx);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	/**
	 * Get the node key.
	 * 
	 * @return node key
	 */
	public long getNodeKey() {
		return mNodeKey;
	}

	@Override
	public DBNode replaceWith(SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		throw new OperationNotSupportedException();
	}

	@Override
	public DBNode replaceWith(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		throw new OperationNotSupportedException();
	}

	@Override
	public boolean hasAttributes() throws DocumentException {
		moveRtx();
		return mRtx.getAttributeCount() > 0;
	}

	/**
	 * Get the sibling position.
	 * 
	 * @return sibling position
	 */
	public int getSiblingPosition() {
		moveRtx();
		int index = 0;
		while (mRtx.hasLeftSibling()) {
			mRtx.moveToLeftSibling();
			index++;
		}
		return index;
	}

	@Override
	public void delete() throws DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mRtx;
			try {
				wtx.remove();
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
	}

	@Override
	public void parse(final @Nonnull SubtreeHandler handler)
			throws DocumentException {
		final SubtreeParser parser = new NavigationalSubtreeParser(this);
		parser.parse(handler);
	}

	@Override
	protected int cmpInternal(final @Nonnull AbstractTemporalNode otherNode) {
		// are they the same node?
		if (this == otherNode) {
			return 0;
		}

		// Compare collection IDs.
		final int firstCollectionID = mCollection.getID();
		final int secondCollectionID = ((DBCollection) otherNode.getCollection())
				.getID();
		if (firstCollectionID != secondCollectionID) {
			return firstCollectionID < secondCollectionID ? -1 : 1;
		}

		// Compare document IDs.
		final long firstDocumentID = getTrx().getSession().getResourceConfig()
				.getID();
		final long secondDocumentID = ((DBNode) otherNode).getTrx().getSession()
				.getResourceConfig().getID();
		if (firstDocumentID != secondDocumentID) {
			return firstDocumentID < secondDocumentID ? -1 : 1;
		}

		if (mNodeKey == ((DBNode) otherNode).mNodeKey) {
			return 0;
		}

		if (mDeweyID.isPresent()) {
			if (mNodeKey == 4 && ((DBNode) otherNode).mNodeKey == 5) {
				System.out.println();
			}
			return mDeweyID.get().compareTo(((DBNode) otherNode).mDeweyID.get());
		}

		try {
			final DBNode firstParent = (DBNode) this.getParent();
			if (firstParent == null) {
				// first node is the root
				return -1;
			}

			final DBNode secondParent = (DBNode) otherNode.getParent();
			if (secondParent == null) {
				// second node is the root
				return +1;
			}

			// do they have the same parent (common case)?
			if (firstParent.getNodeKey() == secondParent.getNodeKey()) {
				int cat1 = nodeCategories(this.getKind());
				int cat2 = nodeCategories(otherNode.getKind());
				if (cat1 == cat2) {
					final DBNode other = (DBNode) otherNode;
					if (cat1 == 1) {
						mRtx.moveToParent();
						for (int i = 0, nspCount = mRtx.getNamespaceCount(); i < nspCount; i++) {
							mRtx.moveToNamespace(i);
							if (mRtx.getNodeKey() == other.mNodeKey) {
								return +1;
							}
							if (mRtx.getNodeKey() == this.mNodeKey) {
								return -1;
							}
							mRtx.moveToParent();
						}
					}
					if (cat1 == 2) {
						mRtx.moveToParent();
						for (int i = 0, attCount = mRtx.getAttributeCount(); i < attCount; i++) {
							mRtx.moveToAttribute(i);
							if (mRtx.getNodeKey() == other.mNodeKey) {
								return +1;
							}
							if (mRtx.getNodeKey() == this.mNodeKey) {
								return -1;
							}
							mRtx.moveToParent();
						}
					}
					return this.getSiblingPosition()
							- ((DBNode) otherNode).getSiblingPosition();
				} else {
					return cat1 - cat2;
				}
			}

			// find the depths of both nodes in the tree
			int depth1 = 0;
			int depth2 = 0;
			DBNode p1 = this;
			DBNode p2 = (DBNode) otherNode;
			while (p1 != null) {
				depth1++;
				p1 = (DBNode) p1.getParent();
			}
			while (p2 != null) {
				depth2++;
				p2 = (DBNode) p2.getParent();
			}
			// move up one branch of the tree so we have two nodes on the same level

			p1 = this;
			while (depth1 > depth2) {
				p1 = (DBNode) p1.getParent();
				assert p1 != null;
				if (p1.getNodeKey() == ((DBNode) otherNode).getNodeKey()) {
					return +1;
				}
				depth1--;
			}

			p2 = ((DBNode) otherNode);
			while (depth2 > depth1) {
				p2 = (DBNode) p2.getParent();
				assert p2 != null;
				if (p2.getNodeKey() == this.getNodeKey()) {
					return -1;
				}
				depth2--;
			}

			// now move up both branches in sync until we find a common parent
			while (true) {
				final DBNode par1 = (DBNode) p1.getParent();
				final DBNode par2 = (DBNode) p2.getParent();
				if (par1 == null || par2 == null) {
					throw new NullPointerException(
							"Node order comparison - internal error");
				}
				if (par1.getNodeKey() == par2.getNodeKey()) {
					if (p1.getKind() == Kind.ATTRIBUTE && p2.getKind() != Kind.ATTRIBUTE) {
						return -1; // attributes first
					}
					if (p1.getKind() != Kind.ATTRIBUTE && p2.getKind() == Kind.ATTRIBUTE) {
						return +1; // attributes first
					}
					return p1.getSiblingPosition() - p2.getSiblingPosition();
				}
				p1 = par1;
				p2 = par2;
			}
		} catch (final DocumentException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return 0;
	}

	/**
	 * Determine node category.
	 * 
	 * @param kind
	 *          node kind
	 * @return category number
	 */
	private int nodeCategories(final @Nonnull Kind kind) {
		switch (kind) {
		case DOCUMENT:
			return 0;
		case COMMENT:
		case PROCESSING_INSTRUCTION:
		case TEXT:
		case ELEMENT:
			return 3;
		case ATTRIBUTE:
			return 2;
		case NAMESPACE:
			return 1;
		default:
			throw new IllegalStateException("Node kind not known!");
		}
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mRtx.getNodeKey(), mRtx.getValue(), mRtx.getName());
	}

	@Override
	public String toString() {
		moveRtx();
		return Objects.toStringHelper(this).add("rtx", mRtx).toString();
	}

	@Override
	public Stream<? extends Node<?>> performStep(
			final org.brackit.xquery.xdm.Axis axis, final NodeType test)
			throws DocumentException {
		// TODO
		return null;
	}

}

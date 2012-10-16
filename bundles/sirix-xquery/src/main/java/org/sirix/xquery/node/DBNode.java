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
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.NonStructuralWrapperAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.stream.SirixStream;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * A node which is used to provide all XDM functionality as well as temporal
 * functions.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class DBNode extends AbsTemporalNode {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DBNode.class));

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mNodeReadTrx;

	/** Sirix node key. */
	private final long mNodeKey;

	/** Collection this node is part of. */
	private final DBCollection<? extends AbsTemporalNode> mCollection;

	/** Determines if write-transaction is present. */
	private final boolean mIsWtx;

	/**
	 * Constructor.
	 * 
	 * @param pNodeReadTrx
	 *          {@link NodeReadTrx} for providing reading access to the underlying
	 *          node
	 * @param pCollection
	 *          {@link DBCollection} reference
	 */
	public DBNode(final @Nonnull NodeReadTrx pNodeReadTrx,
			final @Nonnull DBCollection<? extends AbsTemporalNode> pCollection) {
		mCollection = checkNotNull(pCollection);
		mNodeReadTrx = checkNotNull(pNodeReadTrx);
		mIsWtx = mNodeReadTrx instanceof NodeWriteTrx;
		mNodeKey = mNodeReadTrx.getNodeKey();
	}

	/**
	 * Create a new {@link IReadTransaction} and move to {@link mKey}.
	 * 
	 * @return new read transaction instance which is moved to {@link mKey}
	 */
	private final void moveRtx() {
		mNodeReadTrx.moveTo(mNodeKey);
	}

	/**
	 * Get underlying node.
	 * 
	 * @return underlying node
	 */
	public org.sirix.node.interfaces.Node getUnderlyingNode() {
		moveRtx();
		return mNodeReadTrx.getNode();
	}

	@Override
	public boolean isSelfOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (node.getUnderlyingNode().getNodeKey() == this.getUnderlyingNode()
					.getNodeKey()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isParentOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			moveRtx();
			if (node.getUnderlyingNode().getParentKey() == mNodeReadTrx.getNodeKey()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isChildOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			moveRtx();
			if (node.getUnderlyingNode().getNodeKey() == mNodeReadTrx.getParentKey()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isDescendantOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			moveRtx();
			for (final Axis axis = new AncestorAxis(mNodeReadTrx); axis.hasNext();) {
				axis.next();
				if (node.getUnderlyingNode().getNodeKey() == mNodeReadTrx.getNodeKey()) {
					retVal = true;
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
		return mNodeReadTrx;
	}

	@Override
	public boolean isDescendantOrSelfOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (isSelfOf(pNode)) {
				retVal = true;
			}
			retVal = isDescendantOf(pNode);
		}
		return retVal;
	}

	@Override
	public boolean isAncestorOf(final @Nonnull Node<?> pNode) {
		return pNode.isDescendantOf(this);
	}

	@Override
	public boolean isAncestorOrSelfOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			if (isSelfOf(pNode)) {
				retVal = true;
			}
			retVal = pNode.isDescendantOf(this);
		}
		return retVal;
	}

	@Override
	public boolean isSiblingOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			try {
				if (node.getKind() != Kind.NAMESPACE
						&& node.getKind() != Kind.ATTRIBUTE
						&& ((DBNode) node.getParent()).getUnderlyingNode().getNodeKey() == ((DBNode) pNode
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
	public boolean isPrecedingSiblingOf(final @Nonnull Node<?> pNode) {
		if (pNode instanceof DBNode) {
			final DBNode other = (DBNode) pNode;
			moveRtx();
			while (mNodeReadTrx.hasRightSibling()) {
				mNodeReadTrx.moveToRightSibling();
				if (mNodeReadTrx.getNodeKey() == other.getNodeKey()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isFollowingSiblingOf(final @Nonnull Node<?> pNode) {
		if (pNode instanceof DBNode) {
			final DBNode other = (DBNode) pNode;
			moveRtx();
			while (mNodeReadTrx.hasLeftSibling()) {
				mNodeReadTrx.moveToLeftSibling();
				if (mNodeReadTrx.getNodeKey() == other.getNodeKey()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isPrecedingOf(final @Nonnull Node<?> pNode) {
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			moveRtx();
			for (final Axis axis = new FollowingAxis(mNodeReadTrx); axis.hasNext();) {
				axis.next();
				if (mNodeReadTrx.getNodeKey() == node.getNodeKey()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isFollowingOf(final @Nonnull Node<?> pNode) {
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			moveRtx();
			for (final Axis axis = new PrecedingAxis(mNodeReadTrx); axis.hasNext();) {
				axis.next();
				if (mNodeReadTrx.getNodeKey() == node.getNodeKey()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isAttributeOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
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
	public boolean isDocumentOf(final @Nonnull Node<?> pNode) {
		moveRtx();
		boolean retVal = false;
		if (getKind() == Kind.DOCUMENT && pNode instanceof DBNode) {
			final DBNode node = (DBNode) pNode;
			assert node.getNodeClassID() == this.getNodeClassID();
			final NodeReadTrx rtx = node.getTrx();
			try {
				if (rtx.getRevisionNumber() == mNodeReadTrx.getRevisionNumber()
						&& rtx.getSession().getResourceConfig().getID() == mNodeReadTrx
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
		boolean retVal = false;
		// TODO: Actually it seems it must check if it's the document root node.
		if (mNodeReadTrx.getNode().getParentKey() == Fixed.NULL_NODE_KEY
				.getStandardProperty()) {
			retVal = true;
		}
		return retVal;
	}

	@Override
	public int getNodeClassID() {
		return 1732483;
	}

	@Override
	public Collection<AbsTemporalNode> getCollection() {
		return mCollection;
	}

	@Override
	public Scope getScope() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Kind getKind() {
		moveRtx();
		switch (mNodeReadTrx.getKind()) {
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
		final QName name = mNodeReadTrx.getName();
		if (name == null) {
			return null;
		}
		return new QNm(name.getNamespaceURI(), name.getPrefix(),
				name.getLocalPart());
	}

	@Override
	public void setName(final QNm pName) throws OperationNotSupportedException,
			DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			if (wtx.isNameNode()) {
				try {
					wtx.setQName(new QName(pName.nsURI, pName.localName, pName.prefix));
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
		return new Una(mNodeReadTrx.getValue());
	}

	@Override
	public void setValue(final Atomic pValue)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			if (wtx.isValueNode()) {
				try {
					wtx.setValue(pValue.stringValue());
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
	public AbsTemporalNode getParent() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.hasParent() ? new DBNode(mNodeReadTrx.moveToParent()
				.get(), mCollection) : null;
	}

	@Override
	public AbsTemporalNode getFirstChild() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.hasFirstChild() ? new DBNode(mNodeReadTrx
				.moveToFirstChild().get(), mCollection) : null;
	}

	@Override
	public AbsTemporalNode getLastChild() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.hasLastChild() ? new DBNode(mNodeReadTrx
				.moveToLastChild().get(), mCollection) : null;
	}

	@Override
	public Stream<? extends AbsTemporalNode> getChildren()
			throws DocumentException {
		moveRtx();
		return new SirixStream(new ChildAxis(mNodeReadTrx), mCollection);
	}

	// Returns all nodes in the subtree _including_ the subtree root.
	@Override
	public Stream<? extends AbsTemporalNode> getSubtree()
			throws DocumentException {
		moveRtx();
		return new SirixStream(new NonStructuralWrapperAxis(new DescendantAxis(
				mNodeReadTrx, IncludeSelf.YES)), mCollection);
	}

	@Override
	public boolean hasChildren() throws DocumentException {
		return mNodeReadTrx.getChildCount() > 0;
	}

	@Override
	public AbsTemporalNode getNextSibling() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.hasRightSibling() ? new DBNode(mNodeReadTrx
				.moveToRightSibling().get(), mCollection) : null;
	}

	@Override
	public AbsTemporalNode getPreviousSibling() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.hasLeftSibling() ? new DBNode(mNodeReadTrx
				.moveToLeftSibling().get(), mCollection) : null;
	}

	@Override
	public AbsTemporalNode append(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
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

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode append(final Node<?> pChild)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			try {
				if (wtx.hasFirstChild()) {
					wtx.moveToLastChild();
				}

				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<DBNode>(
						mCollection, wtx, Insert.ASRIGHTSIBLING,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pChild.parse(builder);
				mNodeReadTrx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode append(SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbsTemporalNode prepend(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
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

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode prepend(final Node<?> pChild)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			try {
				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<DBNode>(
						mCollection, wtx, Insert.ASFIRSTCHILD,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pChild.parse(builder);
				mNodeReadTrx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode prepend(final SubtreeParser pParser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				pParser.parse(new SubtreeBuilder<DBNode>(mCollection,
						(NodeWriteTrx) mNodeReadTrx, Insert.ASFIRSTCHILD, Collections
								.<SubtreeListener<? super AbsTemporalNode>> emptyList()));
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
			moveRtx();
			mNodeReadTrx.moveToFirstChild();
			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertBefore(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
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

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertBefore(final Node<?> pNode)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			try {
				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<DBNode>(
						mCollection, wtx, Insert.ASLEFTSIBLING,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pNode.parse(builder);
				mNodeReadTrx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertBefore(final SubtreeParser pParser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<>(
						mCollection, (NodeWriteTrx) mNodeReadTrx, Insert.ASLEFTSIBLING,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pParser.parse(builder);
				return new DBNode(mNodeReadTrx.moveTo(builder.getStartNodeKey()).get(),
						mCollection);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertAfter(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
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

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertAfter(final Node<?> pNode)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			try {
				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<DBNode>(
						mCollection, wtx, Insert.ASRIGHTSIBLING,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pNode.parse(builder);
				mNodeReadTrx.moveTo(builder.getStartNodeKey());
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}

			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode insertAfter(final SubtreeParser pParser)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			try {
				final SubtreeBuilder<DBNode> builder = new SubtreeBuilder<>(
						mCollection, (NodeWriteTrx) mNodeReadTrx, Insert.ASRIGHTSIBLING,
						Collections.<SubtreeListener<? super AbsTemporalNode>> emptyList());
				pParser.parse(builder);
				return new DBNode(mNodeReadTrx.moveTo(builder.getStartNodeKey()).get(),
						mCollection);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public AbsTemporalNode setAttribute(final Node<?> pAttribute)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			if (wtx.isElement()) {
				try {
					final String value = pAttribute.getValue().asStr().stringValue();
					final QNm name = pAttribute.getName();
					wtx.insertAttribute(
							new QName(name.nsURI, name.localName, name.prefix), value);
					return new DBNode(mNodeReadTrx, mCollection);
				} catch (final SirixException e) {
					throw new DocumentException(e.getCause());
				}
			}
		}
		return null;
	}

	@Override
	public AbsTemporalNode setAttribute(final QNm pName, final Atomic pValue)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			if (wtx.isElement()) {
				try {
					wtx.insertAttribute(new QName(pName.nsURI, pName.localName,
							pName.prefix), pValue.asStr().stringValue());
					return new DBNode(mNodeReadTrx, mCollection);
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
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
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
	public Stream<? extends AbsTemporalNode> getAttributes()
			throws OperationNotSupportedException, DocumentException {
		moveRtx();
		return new SirixStream(new AttributeAxis(mNodeReadTrx), mCollection);
	}

	@Override
	public AbsTemporalNode getAttribute(final QNm pName) throws DocumentException {
		moveRtx();
		if (mNodeReadTrx.isElement()
				&& mNodeReadTrx.moveToAttributeByName(
						new QName(pName.nsURI, pName.localName, pName.prefix)).hasMoved()) {
			return new DBNode(mNodeReadTrx, mCollection);
		}
		return null;
	}

	@Override
	public AbsTemporalNode replaceWith(final Node<?> pNode)
			throws OperationNotSupportedException, DocumentException {
		if (mIsWtx && pNode instanceof DBNode) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			final DBNode node = (DBNode) pNode;
			try {
				final NodeReadTrx rtx = node.getTrx();
				rtx.moveTo(node.getNodeKey());
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
	public AbsTemporalNode replaceWith(final SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		throw new OperationNotSupportedException();
	}

	@Override
	public AbsTemporalNode replaceWith(Kind kind, QNm name, Atomic value)
			throws OperationNotSupportedException, DocumentException {
		throw new OperationNotSupportedException();
	}

	@Override
	public boolean hasAttributes() throws DocumentException {
		moveRtx();
		return mNodeReadTrx.getAttributeCount() > 0;
	}

	/**
	 * Get the sibling position.
	 * 
	 * @return sibling position
	 */
	public int getSiblingPosition() {
		moveRtx();
		int index = 0;
		while (mNodeReadTrx.hasLeftSibling()) {
			mNodeReadTrx.moveToLeftSibling();
			index++;
		}
		return index;
	}

	@Override
	public void delete() throws DocumentException {
		if (mIsWtx) {
			moveRtx();
			final NodeWriteTrx wtx = (NodeWriteTrx) mNodeReadTrx;
			try {
				wtx.remove();
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
	}

	@Override
	public void parse(final @Nonnull SubtreeHandler pHandler)
			throws DocumentException {
		final SubtreeParser parser = new NavigationalSubtreeParser(this);
		parser.parse(pHandler);
	}

	@Override
	protected int cmpInternal(final @Nonnull AbsTemporalNode pOther) {
		// are they the same node?
		if (this == pOther) {
			return 0;
		}

		// Compare collection IDs.
		final int firstCollectionID = mCollection.getID();
		final int secondCollectionID = ((DBCollection<AbsTemporalNode>) pOther
				.getCollection()).getID();
		if (firstCollectionID != secondCollectionID) {
			return firstCollectionID < secondCollectionID ? -1 : 1;
		}

		// Compare document IDs.
		final long firstDocumentID = getTrx().getSession().getResourceConfig()
				.getID();
		final long secondDocumentID = ((DBNode) pOther).getTrx().getSession()
				.getResourceConfig().getID();
		if (firstDocumentID != secondDocumentID) {
			return firstDocumentID < secondDocumentID ? -1 : 1;
		}

		if (this.mNodeKey == ((DBNode) pOther).getNodeKey()) {
			return 0;
		}

		try {
			final DBNode firstParent = (DBNode) this.getParent();
			if (firstParent == null) {
				// first node is the root
				return -1;
			}

			final DBNode secondParent = (DBNode) pOther.getParent();
			if (secondParent == null) {
				// second node is the root
				return +1;
			}

			// do they have the same parent (common case)?
			if (firstParent.getNodeKey() == secondParent.getNodeKey()) {
				int cat1 = nodeCategories(this.getKind());
				int cat2 = nodeCategories(pOther.getKind());
				if (cat1 == cat2) {
					final DBNode other = (DBNode) pOther;
					if (cat1 == 1) {
						mNodeReadTrx.moveToParent();
						for (int i = 0, nspCount = mNodeReadTrx.getNamespaceCount(); i < nspCount; i++) {
							mNodeReadTrx.moveToNamespace(i);
							if (mNodeReadTrx.getNodeKey() == other.mNodeKey) {
								return +1;
							}
							if (mNodeReadTrx.getNodeKey() == this.mNodeKey) {
								return -1;
							}
							mNodeReadTrx.moveToParent();
						}
					}
					if (cat1 == 2) {
						mNodeReadTrx.moveToParent();
						for (int i = 0, attCount = mNodeReadTrx.getAttributeCount(); i < attCount; i++) {
							mNodeReadTrx.moveToAttribute(i);
							if (mNodeReadTrx.getNodeKey() == other.mNodeKey) {
								return +1;
							}
							if (mNodeReadTrx.getNodeKey() == this.mNodeKey) {
								return -1;
							}
							mNodeReadTrx.moveToParent();
						}
					}
					return this.getSiblingPosition()
							- ((DBNode) pOther).getSiblingPosition();
				} else {
					return cat1 - cat2;
				}
			}

			// find the depths of both nodes in the tree
			int depth1 = 0;
			int depth2 = 0;
			DBNode p1 = this;
			DBNode p2 = (DBNode) pOther;
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
				if (p1.getNodeKey() == ((DBNode) pOther).getNodeKey()) {
					return +1;
				}
				depth1--;
			}

			p2 = ((DBNode) pOther);
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
	 * @param pKind
	 *          node kind
	 * @return category number
	 */
	private int nodeCategories(final @Nonnull Kind pKind) {
		switch (pKind) {
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
		return Objects.hashCode(mNodeReadTrx.getNodeKey(), mNodeReadTrx.getValue(),
				mNodeReadTrx.getName());
	}

	@Override
	public String toString() {
		moveRtx();
		return Objects.toStringHelper(this).add("rtx", mNodeReadTrx).toString();
	}

	@Override
	public Stream<? extends Node<?>> performStep(
			org.brackit.xquery.xdm.Axis axis, NodeType test) throws DocumentException {
		// TODO
		return null;
	}

}

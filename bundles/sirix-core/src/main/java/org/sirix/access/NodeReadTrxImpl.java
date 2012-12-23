/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.ItemList;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.NullNode;
import org.sirix.node.PINode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.TextNode;
import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.immutable.ImmutableDocument;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.immutable.ImmutableText;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.ItemListImpl;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;

/**
 * <h1>NodeReadTrxImpl</h1>
 * 
 * <p>
 * Read-only transaction with single-threaded cursor semantics. Each read-only
 * transaction works on a given revision key.
 * </p>
 */
public final class NodeReadTrxImpl implements NodeReadTrx {

	/** ID of transaction. */
	private final long mId;

	/** Session state this write transaction is bound to. */
	protected final SessionImpl mSession;

	/** State of transaction including all cached stuff. */
	private PageReadTrx mPageReadTrx;

	/** Strong reference to currently selected node. */
	private ImmutableNode mCurrentNode;

	/** Tracks whether the transaction is closed. */
	private boolean mClosed;

	/** Read-transaction-exclusive item list. */
	private final ItemList<AtomicValue> mItemList;

	/**
	 * Constructor.
	 * 
	 * @param session
	 *          the current {@link Session} the transaction is bound to
	 * @param transactionID
	 *          ID of transaction
	 * @param pageReadTransaction
	 *          {@link PageReadTrx} to interact with the page layer
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NodeReadTrxImpl(final @Nonnull SessionImpl session,
			final @Nonnegative long transactionID,
			final @Nonnull PageReadTrx pageReadTransaction) throws SirixIOException {
		mSession = checkNotNull(session);
		checkArgument(transactionID >= 0);
		mId = transactionID;
		mPageReadTrx = checkNotNull(pageReadTransaction);
		@SuppressWarnings("unchecked")
		final Optional<? extends Node> node = (Optional<? extends Node>) mPageReadTrx
				.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
						PageKind.NODEPAGE);
		if (node.isPresent()) {
			mCurrentNode = node.get();
		} else {
			throw new IllegalStateException(
					"Node couldn't be fetched from persistent storage!");
		}
		mClosed = false;
		mItemList = new ItemListImpl();
	}

	/**
	 * Get the current node.
	 * 
	 * @return current node
	 */
	public ImmutableNode getCurrentNode() {
		return mCurrentNode;
	}

	@Override
	public ImmutableNode getNode() {
		switch (mCurrentNode.getKind()) {
		case ELEMENT:
			return ImmutableElement.of((ElementNode) mCurrentNode);
		case TEXT:
			return ImmutableText.of((TextNode) mCurrentNode);
		case COMMENT:
			return ImmutableComment.of((CommentNode) mCurrentNode);
		case PROCESSING_INSTRUCTION:
			return ImmutablePI.of((PINode) mCurrentNode);
		case ATTRIBUTE:
			return ImmutableAttribute.of((AttributeNode) mCurrentNode);
		case NAMESPACE:
			return ImmutableNamespace.of((NamespaceNode) mCurrentNode);
		case DOCUMENT:
			return ImmutableDocument.of((DocumentRootNode) mCurrentNode);
		default:
			throw new IllegalStateException("Node kind not known!");
		}
	}

	@Override
	public long getTransactionID() {
		assertNotClosed();
		return mId;
	}

	@Override
	public int getRevisionNumber() throws SirixIOException {
		assertNotClosed();
		return mPageReadTrx.getActualRevisionRootPage().getRevision();
	}

	@Override
	public long getRevisionTimestamp() throws SirixIOException {
		assertNotClosed();
		return mPageReadTrx.getActualRevisionRootPage().getRevisionTimestamp();
	}

	@Override
	public Move<? extends NodeReadTrx> moveTo(final long nodeKey) {
		assertNotClosed();
		// NOT NEEDED: Actually slows down Sirix in most cases (slightly), as it's a
		// special case.
		// if (nodeKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
		// return Move.notMoved();
		// }

		// Remember old node and fetch new one.
		final ImmutableNode oldNode = mCurrentNode;
		Optional<? extends Record> newNode;
		try {
			// Immediately return node from item list if node key negative.
			if (nodeKey < 0) {
				if (mItemList.size() > 0) {
					newNode = mItemList.getItem(nodeKey);
				} else {
					newNode = Optional.absent();
				}
			} else {
				final Optional<? extends Record> node = mPageReadTrx.getRecord(nodeKey,
						PageKind.NODEPAGE);
				newNode = node;
			}
		} catch (final SirixIOException e) {
			newNode = Optional.absent();
		}

		if (newNode.isPresent()) {
			mCurrentNode = (Node) newNode.get();
			return Move.moved(this);
		} else {
			mCurrentNode = oldNode;
			return Move.notMoved();
		}
	}

	@Override
	public Move<? extends NodeReadTrx> moveToDocumentRoot() {
		assertNotClosed();
		return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToParent() {
		assertNotClosed();
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToFirstChild() {
		assertNotClosed();
		final StructNode node = getStructuralNode();
		if (!node.hasFirstChild()) {
			return Moved.notMoved();
		}
		return moveTo(node.getFirstChildKey());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToLeftSibling() {
		assertNotClosed();
		final StructNode node = getStructuralNode();
		if (!node.hasLeftSibling()) {
			return Moved.notMoved();
		}
		return moveTo(node.getLeftSiblingKey());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToRightSibling() {
		assertNotClosed();
		final StructNode node = getStructuralNode();
		if (!node.hasRightSibling()) {
			return Move.notMoved();
		}
		return moveTo(node.getRightSiblingKey());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToAttribute(final int index) {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getAttributeCount() > index) {
				final Move<? extends NodeReadTrx> moved = (Move<? extends NodeReadTrx>) moveTo(element
						.getAttributeKey(index));
				return moved;
			} else {
				return Moved.notMoved();
			}
		} else {
			return Moved.notMoved();
		}
	}

	@Override
	public Move<? extends NodeReadTrx> moveToNamespace(final int index) {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getNamespaceCount() > index) {
				final Move<? extends NodeReadTrx> moved = (Move<? extends NodeReadTrx>) moveTo(element
						.getNamespaceKey(index));
				return moved;
			} else {
				return Moved.notMoved();
			}
		} else {
			return Moved.notMoved();
		}
	}

	@Override
	public String getValue() {
		assertNotClosed();
		String returnVal;
		if (mCurrentNode instanceof ValueNode) {
			returnVal = new String(((ValueNode) mCurrentNode).getRawValue());
		} else if (mCurrentNode.getKind() == Kind.NAMESPACE) {
			returnVal = mPageReadTrx.getName(
					((NamespaceNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
		} else {
			returnVal = "";
		}
		return returnVal;
	}

	@Override
	public QNm getName() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			final String uri = mPageReadTrx.getName(
					((NameNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
			final int prefixKey = ((NameNode) mCurrentNode).getPrefixKey();
			final String prefix = prefixKey == -1 ? "" : mPageReadTrx.getName(
					prefixKey, mCurrentNode.getKind());
			final int localNameKey = ((NameNode) mCurrentNode).getLocalNameKey();
			final String localName = localNameKey == -1 ? "" : mPageReadTrx.getName(
					localNameKey, mCurrentNode.getKind());
			return new QNm(uri, prefix, localName);
		} else {
			return null;
		}
	}

	@Override
	public long getNodeKey() {
		assertNotClosed();
		return mCurrentNode.getNodeKey();
	}

	@Override
	public boolean isValueNode() {
		assertNotClosed();
		return mCurrentNode instanceof ValueNode;
	}

	@Override
	public Kind getKind() {
		assertNotClosed();
		return mCurrentNode.getKind();
	}

	@Override
	public String getType() {
		assertNotClosed();
		return mPageReadTrx.getName(mCurrentNode.getTypeKey(),
				mCurrentNode.getKind());
	}

	@Override
	public int keyForName(final @Nonnull String name) {
		assertNotClosed();
		return NamePageHash.generateHashForString(name);
	}

	@Override
	public String nameForKey(final int key) {
		assertNotClosed();
		return mPageReadTrx.getName(key, mCurrentNode.getKind());
	}

	@Override
	public byte[] rawNameForKey(final int key) {
		assertNotClosed();
		return mPageReadTrx.getRawName(key, mCurrentNode.getKind());
	}

	@Override
	public ItemList<AtomicValue> getItemList() {
		assertNotClosed();
		return mItemList;
	}

	@Override
	public void close() throws SirixException {
		if (!mClosed) {
			// Callback on session to make sure everything is cleaned up.
			mSession.closeReadTransaction(mId);

			// Close own state.
			mPageReadTrx.close();
			setPageReadTransaction(null);

			// Immediately release all references.
			mPageReadTrx = null;
			mCurrentNode = null;

			// Close state.
			mClosed = true;
		}
	}

	@Override
	public String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this);
		try {
			helper.add("Revision number", getRevisionNumber());
		} catch (final SirixIOException e) {
		}

		if (mCurrentNode.getKind() == Kind.ATTRIBUTE
				|| mCurrentNode.getKind() == Kind.ELEMENT) {
			helper.add("Name of Node", getName().toString());
		}

		if (mCurrentNode.getKind() == Kind.ATTRIBUTE
				|| mCurrentNode.getKind() == Kind.TEXT) {
			helper.add("Value of Node", getValue());
		}

		if (mCurrentNode.getKind() == Kind.DOCUMENT) {
			helper.addValue("Node is DocumentRoot");
		}
		helper.add("node", mCurrentNode.toString());

		return helper.toString();
	}

	/**
	 * Set state to closed.
	 */
	void setClosed() {
		mClosed = true;
	}

	/**
	 * Is the transaction closed?
	 * 
	 * @return {@code true} if the transaction was closed, {@code false} otherwise
	 */
	@Override
	public boolean isClosed() {
		return mClosed;
	}

	/**
	 * Make sure that the transaction is not yet closed when calling this method.
	 */
	final void assertNotClosed() {
		if (mClosed) {
			throw new IllegalStateException("Transaction is already closed.");
		}
	}

	/**
	 * Get the {@link PageReadTrx}.
	 * 
	 * @return current {@link PageReadTrx}
	 */
	public PageReadTrx getPageTransaction() {
		assertNotClosed();
		return mPageReadTrx;
	}

	/**
	 * Replace the current {@link PageReadTrxImpl}.
	 * 
	 * @param pageReadTransaction
	 *          {@link PageReadTrxImpl} instance
	 */
	final void setPageReadTransaction(
			@Nullable final PageReadTrx pageReadTransaction) {
		assertNotClosed();
		mPageReadTrx = pageReadTransaction;
	}

	/**
	 * Set current node.
	 * 
	 * @param currentNode
	 *          the current node to set
	 */
	final void setCurrentNode(@Nullable final ImmutableNode currentNode) {
		assertNotClosed();
		mCurrentNode = currentNode;
	}

	@Override
	public final long getMaxNodeKey() throws SirixIOException {
		assertNotClosed();
		return getPageTransaction().getActualRevisionRootPage().getMaxNodeKey();
	}

	/**
	 * Retrieve the current node as a structural node.
	 * 
	 * @return structural node instance of current node
	 */
	final StructNode getStructuralNode() {
		if (mCurrentNode instanceof StructNode) {
			return (StructNode) mCurrentNode;
		} else {
			return new NullNode(mCurrentNode);
		}
	}

	@Override
	public Session getSession() {
		assertNotClosed();
		return mSession;
	}

	@Override
	public Move<? extends NodeReadTrx> moveToNextFollowing() {
		assertNotClosed();
		while (!getStructuralNode().hasRightSibling() && mCurrentNode.hasParent()) {
			moveToParent();
		}
		return moveToRightSibling();
	}

	@Override
	public Move<? extends NodeReadTrx> moveToAttributeByName(
			final @Nonnull QNm name) {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			final Optional<Long> attrKey = element.getAttributeKeyByName(name);
			if (attrKey.isPresent()) {
				final Move<? extends NodeReadTrx> moved = (Move<? extends NodeReadTrx>) moveTo(attrKey
						.get());
				return moved;
			}
		}
		return Move.notMoved();
	}

	@Override
	public NodeReadTrx cloneInstance() throws SirixException {
		assertNotClosed();
		final NodeReadTrx rtx = mSession.beginNodeReadTrx(mPageReadTrx
				.getActualRevisionRootPage().getRevision());
		rtx.moveTo(mCurrentNode.getNodeKey());
		return rtx;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof NodeReadTrxImpl) {
			final NodeReadTrxImpl rtx = (NodeReadTrxImpl) obj;
			return Objects.equal(mCurrentNode.getNodeKey(),
					rtx.mCurrentNode.getNodeKey())
					&& Objects.equal(mPageReadTrx.getRevisionNumber(),
							rtx.mPageReadTrx.getRevisionNumber());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mCurrentNode.getNodeKey(),
				mPageReadTrx.getRevisionNumber());
	}

	@Override
	public final int getNameCount(final @Nonnull String name,
			final @Nonnull Kind kind) {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			return mPageReadTrx.getNameCount(
					NamePageHash.generateHashForString(name), kind);
		} else {
			return 0;
		}
	}

	@Override
	public Move<? extends NodeReadTrx> moveToLastChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			moveToFirstChild();

			while (getStructuralNode().hasRightSibling()) {
				moveToRightSibling();
			}

			return Move.moved(this);
		}
		return Move.notMoved();
	}

	@Override
	public boolean hasNode(final @Nonnegative long key) {
		assertNotClosed();
		final long nodeKey = mCurrentNode.getNodeKey();
		final boolean retVal = moveTo(key) == null ? false : true;
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
	public boolean hasLeftSibling() {
		assertNotClosed();
		return getStructuralNode().hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		assertNotClosed();
		return getStructuralNode().hasRightSibling();
	}

	@Override
	public boolean hasLastChild() {
		assertNotClosed();
		final long nodeKey = mCurrentNode.getNodeKey();
		final boolean retVal = moveToLastChild() == null ? false : true;
		moveTo(nodeKey);
		return retVal;
	}

	@Override
	public int getAttributeCount() {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			final ElementNode node = (ElementNode) mCurrentNode;
			return node.getAttributeCount();
		}
		return 0;
	}

	@Override
	public int getNamespaceCount() {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			final ElementNode node = (ElementNode) mCurrentNode;
			return node.getNamespaceCount();
		}
		return 0;
	}

	@Override
	public boolean isNameNode() {
		assertNotClosed();
		return mCurrentNode instanceof NameNode;
	}

	@Override
	public int getPrefixKey() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getPrefixKey();
		} else {
			return -1;
		}
	}
	
	@Override
	public int getLocalNameKey() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getLocalNameKey();
		} else {
			return -1;
		}
	}

	@Override
	public int getTypeKey() {
		assertNotClosed();
		return mCurrentNode.getTypeKey();
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor visitor) {
		assertNotClosed();
		return mCurrentNode.acceptVisitor(visitor);
	}

	@Override
	public long getLeftSiblingKey() {
		assertNotClosed();
		return getStructuralNode().getLeftSiblingKey();
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
	public long getLastChildKey() {
		throw new UnsupportedOperationException();
		// return getStructuralNode(;
	}

	@Override
	public long getParentKey() {
		assertNotClosed();
		return mCurrentNode.getParentKey();
	}

	@Override
	public long getAttributeKey(final @Nonnegative int index) {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			return ((ElementNode) mCurrentNode).getAttributeKey(index);
		} else {
			return -1;
		}
	}

	@Override
	public long getPathNodeKey() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getPathNodeKey();
		}
		return -1;
	}

	@Override
	public Kind getPathKind() {
		assertNotClosed();
		return Kind.UNKNOWN;
	}

	@Override
	public boolean isStructuralNode() {
		assertNotClosed();
		return mCurrentNode instanceof StructNode;
	}

	@Override
	public int getURIKey() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			return ((NameNode) mCurrentNode).getURIKey();
		}
		return -1;
	};

	@Override
	public List<Long> getAttributeKeys() {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			return ((ElementNode) mCurrentNode).getAttributeKeys();
		}
		return Collections.emptyList();
	}

	@Override
	public List<Long> getNamespaceKeys() {
		assertNotClosed();
		if (mCurrentNode.getKind() == Kind.ELEMENT) {
			return ((ElementNode) mCurrentNode).getNamespaceKeys();
		}
		return Collections.emptyList();
	}

	@Override
	public long getHash() {
		assertNotClosed();
		return mCurrentNode.getHash();
	}

	@Override
	public byte[] getRawValue() {
		assertNotClosed();
		if (mCurrentNode instanceof ValueNode) {
			return ((ValueNode) mCurrentNode).getRawValue();
		}
		return null;
	}

	@Override
	public long getChildCount() {
		assertNotClosed();
		return getStructuralNode().getChildCount();
	}

	@Override
	public long getDescendantCount() {
		assertNotClosed();
		return getStructuralNode().getDescendantCount();
	}

	@Override
	public String getNamespaceURI() {
		assertNotClosed();
		if (mCurrentNode instanceof NameNode) {
			final String URI = mPageReadTrx.getName(
					((NameNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
			return URI;
		}
		return null;
	}

	@Override
	public Kind getRightSiblingKind() {
		assertNotClosed();
		if (mCurrentNode instanceof StructNode && hasRightSibling()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToRightSibling();
			final Kind rightSiblKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return rightSiblKind;
		}
		return Kind.UNKNOWN;
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
	public Kind getFirstChildKind() {
		assertNotClosed();
		if (mCurrentNode instanceof StructNode && hasFirstChild()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToFirstChild();
			final Kind firstChildKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return firstChildKind;
		}
		return Kind.UNKNOWN;
	}

	@Override
	public Kind getLastChildKind() {
		assertNotClosed();
		if (mCurrentNode instanceof StructNode && hasLastChild()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToLastChild();
			final Kind lastChildKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return lastChildKind;
		}
		return Kind.UNKNOWN;
	}

	@Override
	public Kind getParentKind() {
		assertNotClosed();
		if (mCurrentNode.getParentKey() == Fixed.NULL_NODE_KEY
				.getStandardProperty()) {
			return Kind.UNKNOWN;
		}
		final long nodeKey = mCurrentNode.getNodeKey();
		moveToParent();
		final Kind parentKind = mCurrentNode.getKind();
		moveTo(nodeKey);
		return parentKind;
	}

	@Override
	public boolean isElement() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.ELEMENT;
	}

	@Override
	public boolean isText() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.TEXT;
	}

	@Override
	public boolean isDocumentRoot() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.DOCUMENT;
	}

	@Override
	public boolean isComment() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.COMMENT;
	}

	@Override
	public boolean isAttribute() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.ATTRIBUTE;
	}

	@Override
	public boolean isNamespace() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.NAMESPACE;
	}

	@Override
	public boolean isPI() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.PROCESSING_INSTRUCTION;
	}

	@Override
	public boolean hasChildren() {
		assertNotClosed();
		return getStructuralNode().hasFirstChild();
	}

	@Override
	public boolean hasAttributes() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.ELEMENT
				&& ((ElementNode) mCurrentNode).getAttributeCount() > 0;
	}

	@Override
	public boolean hasNamespaces() {
		assertNotClosed();
		return mCurrentNode.getKind() == Kind.ELEMENT
				&& ((ElementNode) mCurrentNode).getNamespaceCount() > 0;
	}

	@Override
	public Move<? extends NodeReadTrx> moveToPrevious() {
		assertNotClosed();
		final StructNode node = getStructuralNode();
		if (node.hasLeftSibling()) {
			// Left sibling node.
			Move<? extends NodeReadTrx> leftSiblMove = moveTo(node
					.getLeftSiblingKey());
			// Now move down to rightmost descendant node if it has one.
			while (leftSiblMove.get().hasFirstChild()) {
				leftSiblMove = leftSiblMove.get().moveToLastChild();
			}
			return leftSiblMove;
		}
		// Parent node.
		return moveTo(node.getParentKey());
	}

	@Override
	public Move<? extends NodeReadTrx> moveToNext() {
		assertNotClosed();
		final StructNode node = getStructuralNode();
		if (node.hasRightSibling()) {
			// Right sibling node.
			return moveTo(node.getRightSiblingKey());
		}
		// Next following node.
		return moveToNextFollowing();
	}

	@Override
	public Optional<SirixDeweyID> getDeweyID() {
		assertNotClosed();
		return mCurrentNode.getDeweyID();
	}

	@Override
	public Optional<SirixDeweyID> getLeftSiblingDeweyID() {
		assertNotClosed();
		if (mSession.mResourceConfig.mDeweyIDsStored) {
			final StructNode node = getStructuralNode();
			final long nodeKey = node.getNodeKey();
			Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID> absent();
			if (node.hasLeftSibling()) {
				// Left sibling node.
				deweyID = moveTo(node.getLeftSiblingKey()).get().getDeweyID();
			}
			moveTo(nodeKey);
			return deweyID;
		}
		return Optional.<SirixDeweyID> absent();
	}

	@Override
	public Optional<SirixDeweyID> getRightSiblingDeweyID() {
		if (mSession.mResourceConfig.mDeweyIDsStored) {
			final StructNode node = getStructuralNode();
			final long nodeKey = node.getNodeKey();
			Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID> absent();
			if (node.hasRightSibling()) {
				// Right sibling node.
				deweyID = moveTo(node.getRightSiblingKey()).get().getDeweyID();
			}
			moveTo(nodeKey);
			return deweyID;
		}
		return Optional.<SirixDeweyID> absent();
	}

	@Override
	public Optional<SirixDeweyID> getParentDeweyID() {
		if (mSession.mResourceConfig.mDeweyIDsStored) {
			final long nodeKey = mCurrentNode.getNodeKey();
			Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID> absent();
			if (mCurrentNode.hasParent()) {
				// Parent node.
				deweyID = moveTo(mCurrentNode.getParentKey()).get().getDeweyID();
			}
			moveTo(nodeKey);
			return deweyID;
		}
		return Optional.<SirixDeweyID> absent();
	}

	@Override
	public Optional<SirixDeweyID> getFirstChildDeweyID() {
		if (mSession.mResourceConfig.mDeweyIDsStored) {
			final StructNode node = getStructuralNode();
			final long nodeKey = node.getNodeKey();
			Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID> absent();
			if (node.hasFirstChild()) {
				// Right sibling node.
				deweyID = moveTo(node.getFirstChildKey()).get().getDeweyID();
			}
			moveTo(nodeKey);
			return deweyID;
		}
		return Optional.<SirixDeweyID> absent();
	}
}

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
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.page.EPage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.ItemList;
import org.sirix.settings.EFixed;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.Util;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;

/**
 * <h1>ReadTransaction</h1>
 * 
 * <p>
 * Read-only transaction with single-threaded cursor semantics. Each read-only
 * transaction works on a given revision key.
 * </p>
 */
final class NodeReadTrx implements INodeReadTrx {

	/** ID of transaction. */
	private final long mId;

	/** Session state this write transaction is bound to. */
	protected final Session mSession;

	/** State of transaction including all cached stuff. */
	private IPageReadTrx mPageReadTrx;

	/** Strong reference to currently selected node. */
	private INode mCurrentNode;

	/** Tracks whether the transaction is closed. */
	private boolean mClosed;

	/** Read-transaction-exclusive item list. */
	private final ItemList mItemList;

	/**
	 * Constructor.
	 * 
	 * @param pSession
	 *          the current {@link ISession} the transaction is bound to
	 * @param pTransactionID
	 *          ID of transaction
	 * @param pPageReadTransaction
	 *          {@link IPageReadTrx} to interact with the page layer
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NodeReadTrx(@Nonnull final Session pSession,
			@Nonnegative final long pTransactionID,
			@Nonnull final IPageReadTrx pPageReadTransaction) throws SirixIOException {
		mSession = checkNotNull(pSession);
		checkArgument(pTransactionID >= 0);
		mId = pTransactionID;
		mPageReadTrx = checkNotNull(pPageReadTransaction);
		@SuppressWarnings("unchecked")
		final Optional<? extends INode> node = (Optional<? extends INode>) mPageReadTrx
				.getNode(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(), EPage.NODEPAGE);
		if (node.isPresent()) {
			mCurrentNode = node.get();
		} else {
			throw new IllegalStateException(
					"Node couldn't be fetched from persistent storage!");
		}
		mClosed = false;
		mItemList = new ItemList();
	}

	/**
	 * Get the currently selected node.
	 * 
	 * @return
	 */
	INode getNode() {
		return mCurrentNode;
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
	public Move<? extends INodeReadTrx> moveTo(final long pNodeKey) {
		assertNotClosed();
		if (pNodeKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
			return Move.notMoved();
		}

		// Remember old node and fetch new one.
		final INode oldNode = mCurrentNode;
		Optional<? extends INodeBase> newNode;
		try {
			// Immediately return node from item list if node key negative.
			if (pNodeKey < 0) {
				if (mItemList.size() > 0) {
					newNode = mItemList.getItem(pNodeKey);
				} else {
					newNode = Optional.absent();
				}
			} else {
				final Optional<? extends INodeBase> node = mPageReadTrx.getNode(
						pNodeKey, EPage.NODEPAGE);
				newNode = node;
			}
		} catch (final SirixIOException e) {
			newNode = Optional.absent();
		}

		if (newNode.isPresent()) {
			mCurrentNode = (INode) newNode.get();
			return Move.moved(this);
		} else {
			mCurrentNode = oldNode;
			return Move.notMoved();
		}
	}

	@Override
	public Move<? extends INodeReadTrx> moveToDocumentRoot() {
		assertNotClosed();
		return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public Move<? extends INodeReadTrx> moveToParent() {
		assertNotClosed();
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public Move<? extends INodeReadTrx> moveToFirstChild() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasFirstChild()) {
			return Moved.notMoved();
		}
		return moveTo(node.getFirstChildKey());
	}

	@Override
	public Move<? extends INodeReadTrx> moveToLeftSibling() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasLeftSibling()) {
			return Moved.notMoved();
		}
		return moveTo(node.getLeftSiblingKey());
	}

	@Override
	public Move<? extends INodeReadTrx> moveToRightSibling() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasRightSibling()) {
			return Move.notMoved();
		}
		return moveTo(node.getRightSiblingKey());
	}

	@Override
	public Move<? extends INodeReadTrx> moveToAttribute(final int pIndex) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getAttributeCount() > pIndex) {
				final Move<? extends INodeReadTrx> moved = (Move<? extends INodeReadTrx>) moveTo(element
						.getAttributeKey(pIndex));
				return moved;
			} else {
				return Moved.notMoved();
			}
		} else {
			return Moved.notMoved();
		}
	}

	@Override
	public Move<? extends INodeReadTrx> moveToNamespace(final int pIndex) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getNamespaceCount() > pIndex) {
				final Move<? extends INodeReadTrx> moved = (Move<? extends INodeReadTrx>) moveTo(element
						.getNamespaceKey(pIndex));
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
		if (mCurrentNode instanceof IValNode) {
			returnVal = new String(((IValNode) mCurrentNode).getRawValue());
		} else if (mCurrentNode.getKind() == EKind.NAMESPACE) {
			returnVal = mPageReadTrx.getName(
					((NamespaceNode) mCurrentNode).getURIKey(), EKind.NAMESPACE);
		} else {
			returnVal = "";
		}
		return returnVal;
	}

	@Override
	public QName getQName() {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			final String name = mPageReadTrx.getName(
					((INameNode) mCurrentNode).getNameKey(), mCurrentNode.getKind());
			final String uri = mPageReadTrx.getName(
					((INameNode) mCurrentNode).getURIKey(), EKind.NAMESPACE);
			return Util.buildQName(uri, name);
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
	public EKind getKind() {
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
	public int keyForName(@Nonnull final String pName) {
		assertNotClosed();
		return NamePageHash.generateHashForString(pName);
	}

	@Override
	public String nameForKey(final int pKey) {
		assertNotClosed();
		return mPageReadTrx.getName(pKey, mCurrentNode.getKind());
	}

	@Override
	public byte[] rawNameForKey(final int pKey) {
		assertNotClosed();
		return mPageReadTrx.getRawName(pKey, mCurrentNode.getKind());
	}

	@Override
	public IItemList<AtomicValue> getItemList() {
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

		if (mCurrentNode.getKind() == EKind.ATTRIBUTE
				|| mCurrentNode.getKind() == EKind.ELEMENT) {
			helper.add("Name of Node", getQName().toString());
		}

		if (mCurrentNode.getKind() == EKind.ATTRIBUTE
				|| mCurrentNode.getKind() == EKind.TEXT) {
			helper.add("Value of Node", getValue());
		}

		if (mCurrentNode.getKind() == EKind.DOCUMENT_ROOT) {
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
	 * Get the {@link IPageReadTrx}.
	 * 
	 * @return current {@link IPageReadTrx}
	 */
	public IPageReadTrx getPageTransaction() {
		assertNotClosed();
		return mPageReadTrx;
	}

	/**
	 * Replace the current {@link PageReadTrx}.
	 * 
	 * @param pPageReadTransaction
	 *          {@link PageReadTrx} instance
	 */
	final void setPageReadTransaction(
			@Nullable final IPageReadTrx pPageReadTransaction) {
		assertNotClosed();
		mPageReadTrx = pPageReadTransaction;
	}

	/**
	 * Set current node.
	 * 
	 * @param pCurrentNode
	 *          the current node to set
	 */
	final void setCurrentNode(@Nullable final INode pCurrentNode) {
		assertNotClosed();
		mCurrentNode = pCurrentNode;
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
	final IStructNode getStructuralNode() {
		if (mCurrentNode instanceof IStructNode) {
			return (IStructNode) mCurrentNode;
		} else {
			return new NullNode(mCurrentNode);
		}
	}

	@Override
	public ISession getSession() {
		assertNotClosed();
		return mSession;
	}

	@Override
	public Move<? extends INodeReadTrx> moveToNextFollowing() {
		assertNotClosed();
		while (!getStructuralNode().hasRightSibling() && mCurrentNode.hasParent()) {
			moveToParent();
		}
		final Move<? extends INodeReadTrx> moved = (Move<? extends INodeReadTrx>) moveToRightSibling();
		return moved;
	}

	@Override
	public Move<? extends INodeReadTrx> moveToAttributeByName(
			@Nonnull final QName pQName) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			final Optional<Long> attrKey = element.getAttributeKeyByName(pQName);
			if (attrKey.isPresent()) {
				final Move<? extends INodeReadTrx> moved = (Move<? extends INodeReadTrx>) moveTo(attrKey
						.get());
				return moved;
			}
		}
		return Move.notMoved();
	}

	@Override
	public INodeReadTrx cloneInstance() throws SirixException {
		assertNotClosed();
		final INodeReadTrx rtx = mSession.beginNodeReadTrx(mPageReadTrx
				.getActualRevisionRootPage().getRevision());
		rtx.moveTo(mCurrentNode.getNodeKey());
		return rtx;
	}

	@Override
	public boolean equals(final Object pObj) {
		if (pObj instanceof NodeReadTrx) {
			final NodeReadTrx rtx = (NodeReadTrx) pObj;
			return Objects.equal(mId, rtx.mId)
					&& Objects.equal(mCurrentNode, rtx.mCurrentNode)
					&& Objects.equal(mPageReadTrx, rtx.mPageReadTrx)
					&& Objects.equal(mSession, rtx.mSession);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mId, mCurrentNode, mPageReadTrx, mSession);
	}

	@Override
	public final int getNameCount(final @Nonnull String pString,
			final @Nonnull EKind pKind) {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			return mPageReadTrx.getNameCount(
					NamePageHash.generateHashForString(pString), pKind);
		} else {
			return 0;
		}
	}

	@Override
	public Move<? extends INodeReadTrx> moveToLastChild() {
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
	public boolean hasNode(final @Nonnegative long pKey) {
		assertNotClosed();
		final long nodeKey = mCurrentNode.getNodeKey();
		final boolean retVal = moveTo(pKey) == null ? false : true;
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
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode node = (ElementNode) mCurrentNode;
			return node.getAttributeCount();
		}
		return 0;
	}

	@Override
	public int getNamespaceCount() {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode node = (ElementNode) mCurrentNode;
			return node.getNamespaceCount();
		}
		return 0;
	}

	@Override
	public boolean isNameNode() {
		assertNotClosed();
		return mCurrentNode instanceof INameNode;
	}

	@Override
	public int getNameKey() {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			return ((INameNode) mCurrentNode).getNameKey();
		} else {
			return 0;
		}
	}

	@Override
	public int getTypeKey() {
		assertNotClosed();
		return mCurrentNode.getTypeKey();
	}

	@Override
	public IVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		assertNotClosed();
		return mCurrentNode.acceptVisitor(pVisitor);
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
	public long getAttributeKey(final @Nonnegative int pIndex) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			return ((ElementNode) mCurrentNode).getAttributeKey(pIndex);
		} else {
			return -1;
		}
	}

	@Override
	public long getPathNodeKey() {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			return ((INameNode) mCurrentNode).getPathNodeKey();
		}
		return -1;
	}

	@Override
	public EKind getPathKind() {
		assertNotClosed();
		return EKind.UNKNOWN;
	}

	@Override
	public boolean isStructuralNode() {
		assertNotClosed();
		return mCurrentNode instanceof IStructNode;
	}

	@Override
	public int getURIKey() {
		assertNotClosed();
		if (mCurrentNode instanceof INameNode) {
			return ((INameNode) mCurrentNode).getURIKey();
		}
		return -1;
	};

	@Override
	public List<Long> getAttributeKeys() {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			return ((ElementNode) mCurrentNode).getAttributeKeys();
		}
		return Collections.emptyList();
	}

	@Override
	public List<Long> getNamespaceKeys() {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
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
		if (mCurrentNode instanceof IValNode) {
			return ((IValNode) mCurrentNode).getRawValue();
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
		if (mCurrentNode instanceof INameNode) {
			final String URI = mPageReadTrx.getName(
					((INameNode) mCurrentNode).getURIKey(), EKind.NAMESPACE);
			return URI;
		}
		return null;
	}

	@Override
	public EKind getRightSiblingKind() {
		assertNotClosed();
		if (mCurrentNode instanceof IStructNode && hasRightSibling()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToRightSibling();
			final EKind rightSiblKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return rightSiblKind;
		}
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getLeftSiblingKind() {
		assertNotClosed();
		if (mCurrentNode instanceof IStructNode && hasLeftSibling()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToLeftSibling();
			final EKind leftSiblKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return leftSiblKind;
		}
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getFirstChildKind() {
		assertNotClosed();
		if (mCurrentNode instanceof IStructNode && hasFirstChild()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToFirstChild();
			final EKind firstChildKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return firstChildKind;
		}
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getLastChildKind() {
		assertNotClosed();
		if (mCurrentNode instanceof IStructNode && hasLastChild()) {
			final long nodeKey = mCurrentNode.getNodeKey();
			moveToLastChild();
			final EKind lastChildKind = mCurrentNode.getKind();
			moveTo(nodeKey);
			return lastChildKind;
		}
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getParentKind() {
		assertNotClosed();
		if (mCurrentNode.getParentKey() == EFixed.NULL_NODE_KEY
				.getStandardProperty()) {
			return EKind.UNKNOWN;
		}
		final long nodeKey = mCurrentNode.getNodeKey();
		moveToParent();
		final EKind parentKind = mCurrentNode.getKind();
		moveTo(nodeKey);
		return parentKind;
	}

	@Override
	public boolean isElement() {
		return mCurrentNode.getKind() == EKind.ELEMENT;
	}

	@Override
	public boolean isText() {
		return mCurrentNode.getKind() == EKind.TEXT;
	}

	@Override
	public boolean isDocumentRoot() {
		return mCurrentNode.getKind() == EKind.DOCUMENT_ROOT;
	}

	@Override
	public boolean isComment() {
		return mCurrentNode.getKind() == EKind.COMMENT;
	}

	@Override
	public boolean isAttribute() {
		return mCurrentNode.getKind() == EKind.ATTRIBUTE;
	}

	@Override
	public boolean isNamespace() {
		return mCurrentNode.getKind() == EKind.NAMESPACE;
	}

	@Override
	public boolean isPI() {
		return mCurrentNode.getKind() == EKind.PROCESSING;
	}
}

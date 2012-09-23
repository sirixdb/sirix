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
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
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

	@Override
	public final long getTransactionID() {
		assertNotClosed();
		return mId;
	}

	@Override
	public final int getRevisionNumber() throws SirixIOException {
		assertNotClosed();
		return mPageReadTrx.getActualRevisionRootPage().getRevision();
	}

	@Override
	public final long getRevisionTimestamp() throws SirixIOException {
		assertNotClosed();
		return mPageReadTrx.getActualRevisionRootPage().getRevisionTimestamp();
	}

	@Override
	public final boolean moveTo(final long pNodeKey) {
		assertNotClosed();
		if (pNodeKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
			return false;
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
			return true;
		} else {
			mCurrentNode = oldNode;
			return false;
		}
	}

	@Override
	public final boolean moveToDocumentRoot() {
		assertNotClosed();
		return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public final boolean moveToParent() {
		assertNotClosed();
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public final boolean moveToFirstChild() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasFirstChild()) {
			return false;
		}
		return moveTo(node.getFirstChildKey());
	}

	@Override
	public final boolean moveToLeftSibling() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasLeftSibling()) {
			return false;
		}
		return moveTo(node.getLeftSiblingKey());
	}

	@Override
	public final boolean moveToRightSibling() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasRightSibling()) {
			return false;
		}
		return moveTo(node.getRightSiblingKey());
	}

	@Override
	public final boolean moveToAttribute(final int pIndex) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getAttributeCount() > pIndex) {
				return moveTo(element.getAttributeKey(pIndex));
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public final boolean moveToNamespace(final int pIndex) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			if (element.getNamespaceCount() > pIndex) {
				return moveTo(element.getNamespaceKey(pIndex));
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public final String getValueOfCurrentNode() {
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
	public final QName getQNameOfCurrentNode() {
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
	public final String getTypeOfCurrentNode() {
		assertNotClosed();
		return mPageReadTrx.getName(mCurrentNode.getTypeKey(), getNode().getKind());
	}

	@Override
	public final int keyForName(@Nonnull final String pName) {
		assertNotClosed();
		return NamePageHash.generateHashForString(pName);
	}

	@Override
	public final String nameForKey(final int pKey) {
		assertNotClosed();
		return mPageReadTrx.getName(pKey, getNode().getKind());
	}

	@Override
	public final byte[] rawNameForKey(final int pKey) {
		assertNotClosed();
		return mPageReadTrx.getRawName(pKey, getNode().getKind());
	}

	@Override
	public final IItemList<AtomicValue> getItemList() {
		assertNotClosed();
		return mItemList;
	}

	@Override
	public void close() throws SirixException {
		if (!mClosed) {
			// Close own state.
			mPageReadTrx.close();
			setPageReadTransaction(null);

			// Callback on session to make sure everything is cleaned up.
			mSession.closeReadTransaction(mId);

			// Immediately release all references.
			mPageReadTrx = null;
			mCurrentNode = null;

			// Close state.
			mClosed = true;
		}
	}

	@Override
	public final String toString() {
		ToStringHelper helper = Objects.toStringHelper(this);
		try {
			helper.add("Revision number", getRevisionNumber());
		} catch (final SirixIOException e) {
		}

		if (getNode().getKind() == EKind.ATTRIBUTE
				|| getNode().getKind() == EKind.ELEMENT) {
			helper.add("Name of Node", getQNameOfCurrentNode().toString());
		}

		if (getNode().getKind() == EKind.ATTRIBUTE
				|| getNode().getKind() == EKind.TEXT) {
			helper.add("Value of Node", getValueOfCurrentNode());
		}

		if (getNode().getKind() == EKind.DOCUMENT_ROOT) {
			helper.addValue("Node is DocumentRoot");
		}
		helper.add("node", getNode().toString());

		return helper.toString();
	}

	/**
	 * Set state to closed.
	 */
	final void setClosed() {
		mClosed = true;
	}

	/**
	 * Is the transaction closed?
	 * 
	 * @return {@code true} if the transaction was closed, {@code false} otherwise
	 */
	@Override
	public final boolean isClosed() {
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
	protected final void setPageReadTransaction(
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
	protected final void setCurrentNode(@Nullable final INode pCurrentNode) {
		assertNotClosed();
		mCurrentNode = pCurrentNode;
	}

	@Override
	public final INode getNode() {
		assertNotClosed();
		return mCurrentNode;
	}

	@Override
	public final long getMaxNodeKey() throws SirixIOException {
		assertNotClosed();
		return getPageTransaction().getActualRevisionRootPage().getMaxNodeKey();
	}

	@Override
	public final IStructNode getStructuralNode() {
		assertNotClosed();
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
	public final boolean moveToNextFollowing() {
		while (!getStructuralNode().hasRightSibling() && getNode().hasParent()) {
			moveToParent();
		}
		return moveToRightSibling();
	}

	@Override
	public boolean moveToAttributeByName(@Nonnull final QName pQName) {
		assertNotClosed();
		if (mCurrentNode.getKind() == EKind.ELEMENT) {
			final ElementNode element = ((ElementNode) mCurrentNode);
			final Optional<Long> attrKey = element.getAttributeKeyByName(pQName);
			if (attrKey.isPresent()) {
				return moveTo(attrKey.get());
			}
		}
		return false;
	}

	@Override
	public INodeReadTrx cloneInstance() throws SirixException {
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
	public int getNameCount(final @Nonnull String pString,
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
		assertNotClosed();
		if (getStructuralNode().hasRightSibling()) {
			moveToRightSibling();
			return Optional.of(getStructuralNode());
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> moveToAndGetLeftSibling() {
		assertNotClosed();
		if (getStructuralNode().hasLeftSibling()) {
			moveToLeftSibling();
			return Optional.of(getStructuralNode());
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> moveToAndGetParent() {
		assertNotClosed();
		if (getStructuralNode().hasParent()) {
			moveToParent();
			return Optional.of(getStructuralNode());
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> moveToAndGetFirstChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			moveToFirstChild();
			return Optional.of(getStructuralNode());
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> moveToAndGetLastChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			moveToFirstChild();

			while (getStructuralNode().hasRightSibling()) {
				moveToRightSibling();
			}

			return Optional.of(getStructuralNode());
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> getRightSibling() {
		assertNotClosed();
		if (getStructuralNode().hasRightSibling()) {
			moveToRightSibling();
			final IStructNode rightSibl = getStructuralNode();
			moveToLeftSibling();
			return Optional.of(rightSibl);
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> getLeftSibling() {
		assertNotClosed();
		if (getStructuralNode().hasLeftSibling()) {
			moveToLeftSibling();
			final IStructNode leftSibl = getStructuralNode();
			moveToRightSibling();
			return Optional.of(leftSibl);
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> getParent() {
		assertNotClosed();
		if (getStructuralNode().hasParent()) {
			final long nodeKey = getNode().getNodeKey();
			moveToParent();
			final IStructNode parent = getStructuralNode();
			moveTo(nodeKey);
			return Optional.of(parent);
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> getFirstChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			final long nodeKey = getNode().getNodeKey();
			moveToFirstChild();
			final IStructNode firstChild = getStructuralNode();
			moveTo(nodeKey);
			return Optional.of(firstChild);
		}
		return Optional.absent();
	}

	@Override
	public Optional<IStructNode> getLastChild() {
		assertNotClosed();
		if (getStructuralNode().hasFirstChild()) {
			final long nodeKey = getNode().getNodeKey();
			moveToLastChild();
			final IStructNode lastChild = getStructuralNode();
			moveTo(nodeKey);
			return Optional.of(lastChild);
		}
		return Optional.absent();
	}
}

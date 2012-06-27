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

import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.node.ENode;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.service.xml.xpath.ItemList;
import org.sirix.settings.EFixed;
import org.sirix.utils.NamePageHash;

/**
 * <h1>ReadTransaction</h1>
 * 
 * <p>
 * Read-only transaction with single-threaded cursor semantics. Each read-only transaction works on a given
 * revision key.
 * </p>
 */
public final class NodeReadTrx implements INodeReadTrx {

  /** ID of transaction. */
  private final long mId;

  /** Session state this write transaction is bound to. */
  protected final Session mSession;

  /** State of transaction including all cached stuff. */
  private IPageReadTrx mPageReadTransaction;

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
   * @throws TTIOException
   *           if an I/O error occurs
   */
  NodeReadTrx(@Nonnull final Session pSession, @Nonnegative final long pTransactionID,
    @Nonnull final IPageReadTrx pPageReadTransaction) throws TTIOException {
    mSession = checkNotNull(pSession);
    checkArgument(pTransactionID >= 0);
    mId = pTransactionID;
    mPageReadTransaction = checkNotNull(pPageReadTransaction);
    final Optional<INode> node = getPageTransaction().getNode(EFixed.ROOT_NODE_KEY.getStandardProperty());
    if (node.isPresent()) {
      mCurrentNode = node.get();
    } else {
      throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
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
  public final long getRevisionNumber() throws TTIOException {
    assertNotClosed();
    return mPageReadTransaction.getActualRevisionRootPage().getRevision();
  }

  @Override
  public final long getRevisionTimestamp() throws TTIOException {
    assertNotClosed();
    return mPageReadTransaction.getActualRevisionRootPage().getRevisionTimestamp();
  }

  @Override
  public final boolean moveTo(final long pNodeKey) {
    assertNotClosed();
    if (pNodeKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
      return false;
    } else {
      // Remember old node and fetch new one.
      final INode oldNode = mCurrentNode;
      Optional<? extends INode> newNode;
      try {
        // Immediately return node from item list if node key negative.
        if (pNodeKey < 0) {
          if (mItemList.size() > 0) {
            newNode = mItemList.getItem(pNodeKey);
          } else {
            newNode = Optional.absent();
          }
        } else {
          newNode = mPageReadTransaction.getNode(pNodeKey);
        }
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
  }

  @Override
  public final boolean moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(EFixed.ROOT_NODE_KEY.getStandardProperty());
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
    if (mCurrentNode.getKind() == ENode.ELEMENT_KIND) {
      final ElementNode element = ((ElementNode)mCurrentNode);
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
    if (mCurrentNode.getKind() == ENode.ELEMENT_KIND) {
      final ElementNode element = ((ElementNode)mCurrentNode);
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
      returnVal = new String(((IValNode)mCurrentNode).getRawValue());
    } else if (mCurrentNode.getKind() == ENode.NAMESPACE_KIND) {
      returnVal =
        mPageReadTransaction.getName(((NamespaceNode)mCurrentNode).getURIKey(), ENode.NAMESPACE_KIND);
    } else {
      returnVal = "";
    }
    return returnVal;
  }

  @Override
  public final QName getQNameOfCurrentNode() {
    assertNotClosed();
    String name = "";
    String uri = "";
    if (mCurrentNode instanceof INameNode) {
      name = mPageReadTransaction.getName(((INameNode)mCurrentNode).getNameKey(), mCurrentNode.getKind());
      uri = mPageReadTransaction.getName(((INameNode)mCurrentNode).getURIKey(), ENode.NAMESPACE_KIND);
      return buildQName(uri, name);
    } else {
      return null;
    }
  }

  @Override
  public final String getTypeOfCurrentNode() {
    assertNotClosed();
    return mPageReadTransaction.getName(mCurrentNode.getTypeKey(), getNode().getKind());
  }

  @Override
  public final int keyForName(final String pName) {
    assertNotClosed();
    return NamePageHash.generateHashForString(pName);
  }

  @Override
  public final String nameForKey(final int pKey) {
    assertNotClosed();
    return mPageReadTransaction.getName(pKey, getNode().getKind());
  }

  @Override
  public final byte[] rawNameForKey(final int pKey) {
    assertNotClosed();
    return mPageReadTransaction.getRawName(pKey, getNode().getKind());
  }

  @Override
  public final IItemList<AtomicValue> getItemList() {
    assertNotClosed();
    return mItemList;
  }

  @Override
  public void close() throws AbsTTException {
    if (!mClosed) {
      // Close own state.
      mPageReadTransaction.close();

      // Callback on session to make sure everything is cleaned up.
      mSession.closeReadTransaction(mId);

      // Immediately release all references.
      mPageReadTransaction = null;
      mCurrentNode = null;

      mClosed = true;
    }
  }

  @Override
  public final String toString() {
    ToStringHelper helper = Objects.toStringHelper(this);
    try {
      helper.add("Revision number", getRevisionNumber());
    } catch (final TTIOException e) {
    }

    if (getNode().getKind() == ENode.ATTRIBUTE_KIND || getNode().getKind() == ENode.ELEMENT_KIND) {
      helper.add("Name of Node", getQNameOfCurrentNode().toString());
    }

    if (getNode().getKind() == ENode.ATTRIBUTE_KIND || getNode().getKind() == ENode.TEXT_KIND) {
      helper.add("Value of Node", getValueOfCurrentNode());
    }

    if (getNode().getKind() == ENode.ROOT_KIND) {
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
   * Make sure that the session is not yet closed when calling this method.
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
    return mPageReadTransaction;
  }

  /**
   * Replace the current {@link PageReadTrx}.
   * 
   * @param pPageReadTransaction
   *          {@link PageReadTrx} instance
   */
  protected final void setPageReadTransaction(@Nullable final IPageReadTrx pPageReadTransaction) {
    assertNotClosed();
    mPageReadTransaction = pPageReadTransaction;
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
  public final long getMaxNodeKey() throws TTIOException {
    assertNotClosed();
    return getPageTransaction().getActualRevisionRootPage().getMaxNodeKey();
  }

  /**
   * Building a {@link QName} from a URI and a name. The name can have a prefix denoted
   * by a ":" delimiter.
   * 
   * @param pUri
   *          the namespaceuri
   * @param pName
   *          the name including a possible prefix
   * @return {@link QName} instance
   */
  protected static final QName buildQName(@Nonnull final String pUri, @Nonnull final String pName) {
    QName qname;
    if (pName.contains(":")) {
      qname = new QName(pUri, pName.split(":")[1], pName.split(":")[0]);
    } else {
      qname = new QName(pUri, pName);
    }
    return qname;
  }

  @Override
  public final IStructNode getStructuralNode() {
    assertNotClosed();
    if (mCurrentNode instanceof IStructNode) {
      return (IStructNode)mCurrentNode;
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
  public boolean moveToAttributeByNameKey(final int pNameKey) {
    assertNotClosed();
    if (mCurrentNode.getKind() == ENode.ELEMENT_KIND) {
      final ElementNode element = ((ElementNode)mCurrentNode);
      final Optional<Long> attrKey = element.getAttributeKeyByName(pNameKey);
      if (attrKey.isPresent()) {
        return moveTo(attrKey.get());
      }
    }
    return false;
  }
}

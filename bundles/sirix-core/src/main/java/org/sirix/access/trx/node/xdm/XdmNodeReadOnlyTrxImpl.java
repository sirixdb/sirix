/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access.trx.node.xdm;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.AbstractNodeReadTrx;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.Move;
import org.sirix.api.ItemList;
import org.sirix.api.PageReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XdmNodeVisitor;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.xdm.ImmutableAttributeNode;
import org.sirix.node.immutable.xdm.ImmutableComment;
import org.sirix.node.immutable.xdm.ImmutableDocumentNode;
import org.sirix.node.immutable.xdm.ImmutableElement;
import org.sirix.node.immutable.xdm.ImmutableNamespace;
import org.sirix.node.immutable.xdm.ImmutablePI;
import org.sirix.node.immutable.xdm.ImmutableText;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXdmNode;
import org.sirix.node.xdm.AttributeNode;
import org.sirix.node.xdm.CommentNode;
import org.sirix.node.xdm.XdmDocumentRootNode;
import org.sirix.node.xdm.ElementNode;
import org.sirix.node.xdm.NamespaceNode;
import org.sirix.node.xdm.PINode;
import org.sirix.node.xdm.TextNode;
import org.sirix.page.PageKind;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.ItemListImpl;
import org.sirix.settings.Constants;
import org.sirix.utils.NamePageHash;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>XdmNodeReadTrxImpl</h1>
 *
 * <p>
 * Node reading transaction with single-threaded cursor semantics. Each reader is bound to a given
 * revision.
 * </p>
 */
public final class XdmNodeReadOnlyTrxImpl extends AbstractNodeReadTrx<XdmNodeReadOnlyTrx> implements InternalXdmNodeReadTrx {

  /** Resource manager this write transaction is bound to. */
  protected final InternalResourceManager<XdmNodeReadOnlyTrx, XdmNodeTrx> mResourceManager;

  /** Tracks whether the transaction is closed. */
  private boolean mClosed;

  /** Read-transaction-exclusive item list. */
  private final ItemList<AtomicValue> mItemList;

  /** The transaction-ID. */
  private final long mTrxId;

  /**
   * Constructor.
   *
   * @param resourceManager the current {@link ResourceManager} the reader is bound to
   * @param trxId ID of the reader
   * @param pageReadTransaction {@link PageReadTrx} to interact with the page layer
   * @param documentNode the document node
   */
  XdmNodeReadOnlyTrxImpl(final InternalResourceManager<XdmNodeReadOnlyTrx, XdmNodeTrx> resourceManager,
      final @Nonnegative long trxId, final PageReadTrx pageReadTransaction, final ImmutableXdmNode documentNode) {
    super(trxId, pageReadTransaction, documentNode);
    mResourceManager = checkNotNull(resourceManager);
    checkArgument(trxId >= 0);
    mTrxId = trxId;
    mClosed = false;
    mItemList = new ItemListImpl();
  }

  @Override
  public void setCurrentNode(final @Nullable ImmutableXdmNode currentNode) {
    assertNotClosed();
    mCurrentNode = currentNode;
  }

  @Override
  public Move<XdmNodeReadOnlyTrx> moveTo(final long nodeKey) {
    assertNotClosed();

    // Remember old node and fetch new one.
    final ImmutableXdmNode oldNode = (ImmutableXdmNode) mCurrentNode;
    Optional<? extends Record> newNode;
    try {
      // Immediately return node from item list if node key negative.
      if (nodeKey < 0) {
        if (mItemList.size() > 0) {
          newNode = mItemList.getItem(nodeKey);
        } else {
          newNode = Optional.empty();
        }
      } else {
        final Optional<? extends Record> node = getPageTransaction().getRecord(nodeKey, PageKind.RECORDPAGE, -1);
        newNode = node;
      }
    } catch (final SirixIOException e) {
      newNode = Optional.empty();
    }

    if (newNode.isPresent()) {
      mCurrentNode = (ImmutableXdmNode) newNode.get();
      return Move.moved(this);
    } else {
      mCurrentNode = oldNode;
      return Move.notMoved();
    }
  }

  @Override
  public ImmutableXdmNode getNode() {
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
        return ImmutableAttributeNode.of((AttributeNode) mCurrentNode);
      case NAMESPACE:
        return ImmutableNamespace.of((NamespaceNode) mCurrentNode);
      case XDM_DOCUMENT:
        return ImmutableDocumentNode.of((XdmDocumentRootNode) mCurrentNode);
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("Node kind not known!");
    }
  }

  @Override
  public ImmutableNameNode getNameNode() {
    assertNotClosed();
    return (ImmutableNameNode) mCurrentNode;
  }

  @Override
  public ImmutableValueNode getValueNode() {
    assertNotClosed();
    return (ImmutableValueNode) mCurrentNode;
  }

  @Override
  public Move<? extends XdmNodeReadOnlyTrx> moveToLeftSibling() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (!node.hasLeftSibling()) {
      return Move.notMoved();
    }
    return moveTo(node.getLeftSiblingKey());
  }

  @Override
  public Move<? extends XdmNodeReadOnlyTrx> moveToAttribute(final int index) {
    assertNotClosed();
    if (mCurrentNode.getKind() == Kind.ELEMENT) {
      final ElementNode element = ((ElementNode) mCurrentNode);
      if (element.getAttributeCount() > index) {
        final Move<? extends XdmNodeReadOnlyTrx> moved = moveTo(element.getAttributeKey(index));
        return moved;
      } else {
        return Move.notMoved();
      }
    }

    return Move.notMoved();
  }

  @Override
  public Move<? extends XdmNodeReadOnlyTrx> moveToNamespace(final int index) {
    assertNotClosed();
    if (mCurrentNode.getKind() == Kind.ELEMENT) {
      final ElementNode element = ((ElementNode) mCurrentNode);
      if (element.getNamespaceCount() > index) {
        final Move<? extends XdmNodeReadOnlyTrx> moved = moveTo(element.getNamespaceKey(index));
        return moved;
      } else {
        return Move.notMoved();
      }
    }

    return Move.notMoved();
  }

  @Override
  public QNm getName() {
    assertNotClosed();
    if (mCurrentNode instanceof NameNode) {
      final String uri = mPageReadTrx.getName(((NameNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
      final int prefixKey = ((NameNode) mCurrentNode).getPrefixKey();
      final String prefix = prefixKey == -1
          ? ""
          : mPageReadTrx.getName(prefixKey, mCurrentNode.getKind());
      final int localNameKey = ((NameNode) mCurrentNode).getLocalNameKey();
      final String localName = localNameKey == -1
          ? ""
          : mPageReadTrx.getName(localNameKey, mCurrentNode.getKind());
      return new QNm(uri, prefix, localName);
    }

    return null;
  }

  @Override
  public String getType() {
    assertNotClosed();
    return mPageReadTrx.getName(((ImmutableXdmNode) mCurrentNode).getTypeKey(), mCurrentNode.getKind());
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
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Revision number", getRevisionNumber());

    if (mCurrentNode.getKind() == Kind.ATTRIBUTE || mCurrentNode.getKind() == Kind.ELEMENT) {
      helper.add("Name of Node", getName().toString());
    }

    if (mCurrentNode.getKind() == Kind.ATTRIBUTE || mCurrentNode.getKind() == Kind.TEXT) {
      helper.add("Value of Node", getValue());
    }

    if (mCurrentNode.getKind() == Kind.XDM_DOCUMENT) {
      helper.addValue("Node is DocumentRoot");
    }
    helper.add("node", mCurrentNode.toString());

    return helper.toString();
  }

  @Override
  public Move<? extends XdmNodeReadOnlyTrx> moveToAttributeByName(final QNm name) {
    assertNotClosed();
    if (mCurrentNode.getKind() == Kind.ELEMENT) {
      final ElementNode element = ((ElementNode) mCurrentNode);
      final Optional<Long> attrKey = element.getAttributeKeyByName(name);
      if (attrKey.isPresent()) {
        final Move<? extends XdmNodeReadOnlyTrx> moved = moveTo(attrKey.get());
        return moved;
      }
    }
    return Move.notMoved();
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof XdmNodeReadOnlyTrxImpl) {
      final XdmNodeReadOnlyTrxImpl rtx = (XdmNodeReadOnlyTrxImpl) obj;
      return mCurrentNode.getNodeKey() == rtx.mCurrentNode.getNodeKey()
          && mPageReadTrx.getRevisionNumber() == rtx.mPageReadTrx.getRevisionNumber();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCurrentNode.getNodeKey(), mPageReadTrx.getRevisionNumber());
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  protected XdmNodeReadOnlyTrx thisInstance() {
    return this;
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
    }
    return -1;
  }

  @Override
  public int getLocalNameKey() {
    assertNotClosed();
    if (mCurrentNode instanceof NameNode) {
      return ((NameNode) mCurrentNode).getLocalNameKey();
    }
    return -1;
  }

  @Override
  public int getTypeKey() {
    assertNotClosed();
    return ((ImmutableXdmNode) mCurrentNode).getTypeKey();
  }

  @Override
  public VisitResult acceptVisitor(final XdmNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableXdmNode) mCurrentNode).acceptVisitor(visitor);
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public long getAttributeKey(final @Nonnegative int index) {
    assertNotClosed();
    if (mCurrentNode.getKind() == Kind.ELEMENT) {
      return ((ElementNode) mCurrentNode).getAttributeKey(index);
    }
    return -1;
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
  public String getNamespaceURI() {
    assertNotClosed();
    if (mCurrentNode instanceof NameNode) {
      final String URI = mPageReadTrx.getName(((NameNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
      return URI;
    }
    return null;
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
    return mCurrentNode.getKind() == Kind.XDM_DOCUMENT;
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
  public boolean hasAttributes() {
    assertNotClosed();
    return mCurrentNode.getKind() == Kind.ELEMENT && ((ElementNode) mCurrentNode).getAttributeCount() > 0;
  }

  @Override
  public boolean hasNamespaces() {
    assertNotClosed();
    return mCurrentNode.getKind() == Kind.ELEMENT && ((ElementNode) mCurrentNode).getNamespaceCount() > 0;
  }

  @Override
  public Move<? extends XdmNodeReadOnlyTrx> moveToPrevious() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasLeftSibling()) {
      // Left sibling node.
      Move<? extends XdmNodeReadOnlyTrx> leftSiblMove = moveTo(node.getLeftSiblingKey());
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
  public Optional<SirixDeweyID> getDeweyID() {
    assertNotClosed();
    return ((ImmutableXdmNode) mCurrentNode).getDeweyID();
  }

  @Override
  public Optional<SirixDeweyID> getLeftSiblingDeweyID() {
    assertNotClosed();
    if (mResourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID>empty();
      if (node.hasLeftSibling()) {
        // Left sibling node.
        deweyID = moveTo(node.getLeftSiblingKey()).get().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return Optional.<SirixDeweyID>empty();
  }

  @Override
  public Optional<SirixDeweyID> getRightSiblingDeweyID() {
    if (mResourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID>empty();
      if (node.hasRightSibling()) {
        // Right sibling node.
        deweyID = moveTo(node.getRightSiblingKey()).get().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return Optional.<SirixDeweyID>empty();
  }

  @Override
  public Optional<SirixDeweyID> getParentDeweyID() {
    if (mResourceManager.getResourceConfig().areDeweyIDsStored) {
      final long nodeKey = mCurrentNode.getNodeKey();
      Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID>empty();
      if (mCurrentNode.hasParent()) {
        // Parent node.
        deweyID = moveTo(mCurrentNode.getParentKey()).get().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return Optional.<SirixDeweyID>empty();
  }

  @Override
  public Optional<SirixDeweyID> getFirstChildDeweyID() {
    if (mResourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      Optional<SirixDeweyID> deweyID = Optional.empty();
      if (node.hasFirstChild()) {
        // Right sibling node.
        deweyID = moveTo(node.getFirstChildKey()).get().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return Optional.empty();
  }

  @Override
  public void close() {
    if (!mClosed) {
      // Callback on session to make sure everything is cleaned up.
      mResourceManager.closeReadTransaction(mTrxId);

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
  public String getValue() {
    assertNotClosed();

    final String returnVal;

    if (mCurrentNode instanceof ValueNode) {
      returnVal = new String(((ValueNode) mCurrentNode).getRawValue(), Constants.DEFAULT_ENCODING);
    } else if (mCurrentNode.getKind() == Kind.NAMESPACE) {
      returnVal = mPageReadTrx.getName(((NamespaceNode) mCurrentNode).getURIKey(), Kind.NAMESPACE);
    } else {
      returnVal = "";
    }

    return returnVal;
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public int getNameCount(String name, Kind kind) {
    assertNotClosed();
    if (mCurrentNode instanceof NameNode) {
      return mPageReadTrx.getNameCount(NamePageHash.generateHashForString(name), kind);
    }
    return 0;
  }

  @Override
  public boolean isValueNode() {
    assertNotClosed();
    return mCurrentNode instanceof ValueNode;
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
  public XdmResourceManager getResourceManager() {
    assertNotClosed();
    return (XdmResourceManager) mResourceManager;
  }

  @Override
  public void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @Override
  public ImmutableXdmNode getCurrentNode() {
    return (ImmutableXdmNode) mCurrentNode;
  }
}

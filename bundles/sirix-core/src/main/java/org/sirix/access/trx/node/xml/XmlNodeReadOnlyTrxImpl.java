/*
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

package org.sirix.access.trx.node.xml;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.api.ItemList;
import org.sirix.api.Move;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.xml.ImmutableAttributeNode;
import org.sirix.node.immutable.xml.ImmutableComment;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.immutable.xml.ImmutableNamespace;
import org.sirix.node.immutable.xml.ImmutablePI;
import org.sirix.node.immutable.xml.ImmutableText;
import org.sirix.node.immutable.xml.ImmutableXmlDocumentRootNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.AttributeNode;
import org.sirix.node.xml.CommentNode;
import org.sirix.node.xml.ElementNode;
import org.sirix.node.xml.NamespaceNode;
import org.sirix.node.xml.PINode;
import org.sirix.node.xml.TextNode;
import org.sirix.node.xml.XmlDocumentRootNode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.ItemListImpl;
import org.sirix.settings.Constants;
import org.sirix.utils.NamePageHash;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Node reading transaction with single-threaded cursor semantics. Each reader is bound to a given
 * revision.
 */
public final class XmlNodeReadOnlyTrxImpl extends AbstractNodeReadOnlyTrx<XmlNodeReadOnlyTrx, XmlNodeTrx>
    implements InternalXmlNodeReadOnlyTrx {

  /**
   * Constructor.
   *
   * @param resourceManager the current {@link ResourceManager} the reader is bound to
   * @param trxId ID of the reader
   * @param pageReadTransaction {@link PageReadOnlyTrx} to interact with the page layer
   * @param documentNode the document node
   */
  XmlNodeReadOnlyTrxImpl(final InternalResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager,
      final @Nonnegative long trxId, final PageReadOnlyTrx pageReadTransaction, final ImmutableXmlNode documentNode) {
    super(trxId, pageReadTransaction, documentNode, resourceManager, new ItemListImpl());

  }

  @Override
  public boolean storeDeweyIDs() {
    // TODO should this be resourceManager.getResourceConfig().areDeweyIDsStored?
    return false;
  }

  @Override
  public ImmutableXmlNode getNode() {
    assertNotClosed();

    // $CASES-OMITTED$
    return switch (currentNode.getKind()) {
      case ELEMENT -> ImmutableElement.of((ElementNode) currentNode);
      case TEXT -> ImmutableText.of((TextNode) currentNode);
      case COMMENT -> ImmutableComment.of((CommentNode) currentNode);
      case PROCESSING_INSTRUCTION -> ImmutablePI.of((PINode) currentNode);
      case ATTRIBUTE -> ImmutableAttributeNode.of((AttributeNode) currentNode);
      case NAMESPACE -> ImmutableNamespace.of((NamespaceNode) currentNode);
      case XML_DOCUMENT -> ImmutableXmlDocumentRootNode.of((XmlDocumentRootNode) currentNode);
      default -> throw new IllegalStateException("Node kind not known!");
    };
  }

  @Override
  public ImmutableNameNode getNameNode() {
    assertNotClosed();
    return (ImmutableNameNode) currentNode;
  }

  @Override
  public ImmutableValueNode getValueNode() {
    assertNotClosed();
    return (ImmutableValueNode) currentNode;
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToAttribute(final int index) {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      final ElementNode element = ((ElementNode) currentNode);
      if (element.getAttributeCount() > index) {
        return moveTo(element.getAttributeKey(index));
      } else {
        return Move.notMoved();
      }
    }

    return Move.notMoved();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToNamespace(final int index) {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      final ElementNode element = ((ElementNode) currentNode);
      if (element.getNamespaceCount() > index) {
        return moveTo(element.getNamespaceKey(index));
      } else {
        return Move.notMoved();
      }
    }

    return Move.notMoved();
  }

  @Override
  public QNm getName() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      final String uri = pageReadOnlyTrx.getName(((NameNode) currentNode).getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = ((NameNode) currentNode).getPrefixKey();
      final String prefix = prefixKey == -1
          ? ""
          : pageReadOnlyTrx.getName(prefixKey, currentNode.getKind());
      final int localNameKey = ((NameNode) currentNode).getLocalNameKey();
      final String localName = localNameKey == -1
          ? ""
          : pageReadOnlyTrx.getName(localNameKey, currentNode.getKind());
      return new QNm(uri, prefix, localName);
    }

    return null;
  }

  @Override
  public String getType() {
    assertNotClosed();
    return pageReadOnlyTrx.getName(((ImmutableXmlNode) currentNode).getTypeKey(), currentNode.getKind());
  }

  @Override
  public byte[] rawNameForKey(final int key) {
    assertNotClosed();
    return pageReadOnlyTrx.getRawName(key, currentNode.getKind());
  }

  @Override
  public ItemList<AtomicValue> getItemList() {
    assertNotClosed();
    return itemList;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Revision number", getRevisionNumber());

    if (currentNode.getKind() == NodeKind.ATTRIBUTE || currentNode.getKind() == NodeKind.ELEMENT) {
      helper.add("Name of Node", getName().toString());
    }

    if (currentNode.getKind() == NodeKind.ATTRIBUTE || currentNode.getKind() == NodeKind.TEXT) {
      helper.add("Value of Node", getValue());
    }

    if (currentNode.getKind() == NodeKind.XML_DOCUMENT) {
      helper.addValue("Node is DocumentRoot");
    }
    helper.add("node", currentNode.toString());

    return helper.toString();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToLastChild() {
    assertNotClosed();
    if (getStructuralNode().hasFirstChild()) {
      moveToFirstChild();

      while (getStructuralNode().hasRightSibling()) {
        moveToRightSibling();
      }

      return Move.moved(thisInstance());
    }
    return Move.notMoved();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToAttributeByName(final QNm name) {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      final ElementNode element = ((ElementNode) currentNode);
      final Optional<Long> attrKey = element.getAttributeKeyByName(name);
      if (attrKey.isPresent()) {
        return moveTo(attrKey.get());
      }
    }
    return Move.notMoved();
  }

  @Override
  protected XmlNodeReadOnlyTrx thisInstance() {
    return this;
  }

  @Override
  public int getAttributeCount() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      final ElementNode node = (ElementNode) currentNode;
      return node.getAttributeCount();
    }
    return 0;
  }

  @Override
  public int getNamespaceCount() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      final ElementNode node = (ElementNode) currentNode;
      return node.getNamespaceCount();
    }
    return 0;
  }

  @Override
  public boolean isNameNode() {
    assertNotClosed();
    return currentNode instanceof NameNode;
  }

  @Override
  public int getPrefixKey() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return ((NameNode) currentNode).getPrefixKey();
    }
    return -1;
  }

  @Override
  public int getLocalNameKey() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return ((NameNode) currentNode).getLocalNameKey();
    }
    return -1;
  }

  @Override
  public int getTypeKey() {
    assertNotClosed();
    return ((ImmutableXmlNode) currentNode).getTypeKey();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableXmlNode) currentNode).acceptVisitor(visitor);
  }

  @Override
  public long getAttributeKey(final @Nonnegative int index) {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      return ((ElementNode) currentNode).getAttributeKey(index);
    }
    return -1;
  }

  @Override
  public boolean isStructuralNode() {
    assertNotClosed();
    return currentNode instanceof StructNode;
  }

  @Override
  public int getURIKey() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return ((NameNode) currentNode).getURIKey();
    }
    return -1;
  }

  @Override
  public List<Long> getAttributeKeys() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      return ((ElementNode) currentNode).getAttributeKeys();
    }
    return Collections.emptyList();
  }

  @Override
  public List<Long> getNamespaceKeys() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.ELEMENT) {
      return ((ElementNode) currentNode).getNamespaceKeys();
    }
    return Collections.emptyList();
  }

  @Override
  public String getNamespaceURI() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return pageReadOnlyTrx.getName(((NameNode) currentNode).getURIKey(), NodeKind.NAMESPACE);
    }
    return null;
  }

  @Override
  public boolean isElement() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.ELEMENT;
  }

  @Override
  public boolean isText() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.TEXT;
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.XML_DOCUMENT;
  }

  @Override
  public boolean isComment() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.COMMENT;
  }

  @Override
  public boolean isAttribute() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.ATTRIBUTE;
  }

  @Override
  public boolean isNamespace() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.NAMESPACE;
  }

  @Override
  public boolean isPI() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public boolean hasAttributes() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.ELEMENT && ((ElementNode) currentNode).getAttributeCount() > 0;
  }

  @Override
  public boolean hasNamespaces() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.ELEMENT && ((ElementNode) currentNode).getNamespaceCount() > 0;
  }

  @Override
  public SirixDeweyID getLeftSiblingDeweyID() {
    assertNotClosed();
    if (resourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      SirixDeweyID deweyID = null;
      if (node.hasLeftSibling()) {
        // Left sibling node.
        deweyID = moveTo(node.getLeftSiblingKey()).trx().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return null;
  }

  @Override
  public SirixDeweyID getRightSiblingDeweyID() {
    if (resourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      SirixDeweyID deweyID = null;
      if (node.hasRightSibling()) {
        // Right sibling node.
        deweyID = moveTo(node.getRightSiblingKey()).trx().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return null;
  }

  @Override
  public SirixDeweyID getParentDeweyID() {
    if (resourceManager.getResourceConfig().areDeweyIDsStored) {
      final long nodeKey = currentNode.getNodeKey();
      SirixDeweyID deweyID = null;
      if (currentNode.hasParent()) {
        // Parent node.
        deweyID = moveTo(currentNode.getParentKey()).trx().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return null;
  }

  @Override
  public SirixDeweyID getFirstChildDeweyID() {
    if (resourceManager.getResourceConfig().areDeweyIDsStored) {
      final StructNode node = getStructuralNode();
      final long nodeKey = node.getNodeKey();
      SirixDeweyID deweyID = null;
      if (node.hasFirstChild()) {
        // Right sibling node.
        deweyID = moveTo(node.getFirstChildKey()).trx().getDeweyID();
      }
      moveTo(nodeKey);
      return deweyID;
    }
    return null;
  }

  @Override
  public String getValue() {
    assertNotClosed();

    final String returnVal;

    if (currentNode instanceof ValueNode) {
      returnVal = new String(((ValueNode) currentNode).getRawValue(), Constants.DEFAULT_ENCODING);
    } else if (currentNode.getKind() == NodeKind.NAMESPACE) {
      returnVal = pageReadOnlyTrx.getName(((NamespaceNode) currentNode).getURIKey(), NodeKind.NAMESPACE);
    } else {
      returnVal = "";
    }

    return returnVal;
  }

  @Override
  public int getNameCount(String name, @Nonnull NodeKind kind) {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return pageReadOnlyTrx.getNameCount(NamePageHash.generateHashForString(name), kind);
    }
    return 0;
  }

  @Override
  public boolean isValueNode() {
    assertNotClosed();
    return currentNode instanceof ValueNode;
  }

  @Override
  public byte[] getRawValue() {
    assertNotClosed();
    if (currentNode instanceof ValueNode) {
      return ((ValueNode) currentNode).getRawValue();
    }
    return null;
  }

  @Override
  public XmlResourceManager getResourceManager() {
    assertNotClosed();
    return (XmlResourceManager) resourceManager;
  }

}

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

package org.sirix.api.xml;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.ItemList;
import org.sirix.api.Move;
import org.sirix.api.Moved;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
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

/**
 * <h2>Description</h2>
 *
 * <p>
 * Interface to access nodes based on the Key/ParentKey/FirstChildKey/LeftSiblingKey
 * /RightSiblingKey/ChildCount/DescendantCount encoding. This encoding keeps the children ordered
 * but has no knowledge of the global node ordering. The underlying tree is accessed in a
 * cursor-like fashion.
 * </p>
 *
 * <h2>Convention</h2>
 *
 * <ol>
 * <li>Only a single thread accesses each NodeReadTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>NodeReadTrx.getNodeKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after moving cursor:
 * <code>(NodeReadTrx.moveTo(m).hasMoved() &amp;&amp;
 *       NodeReadTrx.getNodeKey() == m) ||
 *       (!NodeReadTrx.moveTo(m).hasMoved() &amp;&amp;
 *       NodeReadTrx.getNodeKey() == n)</code>.</li>
 * </ol>
 *
 * <h2>User Example</h2>
 *
 *
 * <pre>
 *   try(final NodeReadTrx rtx = resourcemgr.beginNodeReadTrx()) {
 *     // Either test before moving...
 *     if (rtx.hasFirstChild()) {
 *       rtx.moveToFirstChild();
 *       ...
 *     }
 *
 *     // Or test after moving. Whatever, do the test!
 *     if (rtx.moveToFirstChild().hasMoved()) {
 *       ...
 *     }
 *
 *     // Access local part of element.
 *     if (rtx.isElement() &amp;&amp; &quot;foo&quot;.equalsIgnoreCase(rtx.getQName().getLocalName()) {
 *       ...
 *     }
 *
 *     // Access value of first attribute of element.
 *     if (rtx.isElement() &amp;&amp; (rtx.getAttributeCount() &gt; 0)) {
 *       rtx.moveToAttribute(0);
 *       LOGGER.info(rtx.getValue());
 *     }
 *   }
 * </pre>
 *
 *
 * <h2>Developer Example</h2>
 *
 *
 * <pre>
 *   void someNodeReadTrxMethod() {
 *     // This must be called to make sure the transaction is not closed.
 *     assertNotClosed();
 *     ...
 *   }
 * </pre>
 *
 */
public interface XmlNodeReadOnlyTrx extends NodeCursor, NodeReadOnlyTrx {
  /**
   * Get the page reading transaction.
   *
   * @return The page reading transaction.
   */
  @Override
  PageReadOnlyTrx getPageTrx();

  /** String constants used by XPath. */
  String[] XPATHCONSTANTS = {"xs:anyType", "xs:anySimpleType", "xs:anyAtomicType", "xs:untypedAtomic", "xs:untyped",
      "xs:string", "xs:duration", "xs:yearMonthDuration", "xs:dayTimeDuration", "xs:dateTime", "xs:time", "xs:date",
      "xs:gYearMonth", "xs:gYear", "xs:gMonthDay", "xs:gDay", "xs:gMonth", "xs:boolean", "xs:base64Binary",
      "xs:hexBinary", "xs:anyURI", "xs:QName", "xs:NOTATION", "xs:float", "xs:double", "xs:pDecimal", "xs:decimal",
      "xs:integer", "xs:long", "xs:int", "xs:short", "xs:byte", "xs:nonPositiveInteger", "xs:negativeInteger",
      "xs:nonNegativeInteger", "xs:positiveInteger", "xs:unsignedLong", "xs:unsignedInt", "xs:unsignedShort",
      "xs:unsignedByte", "xs:normalizedString", "xs:token", "xs:language", "xs:name", "xs:NCName", "xs:ID", "xs:IDREF",
      "xs:ENTITY", "xs:IDREFS", "xs:NMTOKEN", "xs:NMTOKENS",};



  // --- Node Selectors
  // --------------------------------------------------------

  /**
   * Accept a visitor.
   *
   * @param visitor {@link XmlNodeVisitor} implementation
   * @return {@link VisitResultType} value
   */
  VisitResult acceptVisitor(XmlNodeVisitor visitor);

  /**
   * Get the immutable node where the cursor currently is located.
   *
   * @return the immutable node instance
   */
  @Override
  ImmutableXmlNode getNode();

  /**
   * Move cursor to attribute by its index.
   *
   * @param index index of attribute to move to
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XmlNodeReadOnlyTrx> moveToAttribute(@Nonnegative int index);

  /**
   * Move cursor to attribute by its name key.
   *
   * @param name {@link QNm} of attribute
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XmlNodeReadOnlyTrx> moveToAttributeByName(QNm name);

  /**
   * Move cursor to namespace declaration by its index.
   *
   * @param index index of attribute to move to
   * @return {@link Moved} instance if the namespace node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XmlNodeReadOnlyTrx> moveToNamespace(@Nonnegative int index);

  // --- Node Getters
  // ----------------------------------------------------------

  /**
   * Getting the value of the current node.
   *
   * @return the current value of the node
   */
  @Override
  String getValue();

  /**
   * Getting the name of a current node.
   *
   * @return the {@link QNm} of the node
   */
  @Override
  QNm getName();

  /**
   * Getting the type of the current node.
   *
   * @return the type of the node
   */
  String getType();

  /**
   * Get raw name for key. This is used for efficient key testing.
   *
   * @param key key, i.e., local part key, URI key, or prefix key.
   * @return byte array containing name for given key
   */
  byte[] rawNameForKey(int key);

  /**
   * Get item list containing volatile items such as atoms or fragments.
   *
   * @return item list
   */
  ItemList<AtomicValue> getItemList();

  /**
   * Determines if this transaction is closed.
   *
   * @return {@code true} if closed, {@code false} otherwise
   */
  @Override
  boolean isClosed();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToNextFollowing();

  /**
   * Get the left {@link SirixDeweyID} of the currently selected node.
   *
   * @return left {@link SirixDeweyID} of the currently selected node
   */
  Optional<SirixDeweyID> getLeftSiblingDeweyID();

  /**
   * Get the right {@link SirixDeweyID} of the currently selected node.
   *
   * @return right {@link SirixDeweyID} of the currently selected node
   */
  Optional<SirixDeweyID> getRightSiblingDeweyID();

  /**
   * Get the parent {@link SirixDeweyID} of the currently selected node.
   *
   * @return parent {@link SirixDeweyID} of the currently selected node
   */
  Optional<SirixDeweyID> getParentDeweyID();

  /**
   * Get the first child {@link SirixDeweyID} of the currently selected node.
   *
   * @return first child {@link SirixDeweyID} of the currently selected node
   */
  Optional<SirixDeweyID> getFirstChildDeweyID();

  /**
   * Get the {@link SirixDeweyID} of the currently selected node.
   *
   * @return first {@link SirixDeweyID} of the currently selected node
   */
  Optional<SirixDeweyID> getDeweyID();

  /**
   * Get the number of attributes the currently selected node has.
   *
   * @return number of attributes of the currently selected node
   */
  int getAttributeCount();

  /**
   * Get the number of namespaces the currently selected node has.
   *
   * @return number of namespaces of the currently selected node
   */
  int getNamespaceCount();

  /**
   * Determines if the current node is a node with a name (element-, attribute-, namespace- and
   * processing instruction).
   *
   * @return {@code true}, if it is, {@code false} otherwise
   */
  boolean isNameNode();

  /**
   * Get the URI-key of a node.
   *
   * @return URI-key of the currently selected node, or {@code -1} if it is not a node with a name
   *         (element, attribute, namespace, processing instruction)
   */
  int getURIKey();

  /**
   * Prefix key of currently selected node.
   *
   * @return name key of currently selected node, or {@code -1} if it is not a node with a name
   */
  int getPrefixKey();

  /**
   * LocalName key of currently selected node.
   *
   * @return name key of currently selected node, or {@code -1} if it is not a node with a name
   */
  int getLocalNameKey();



  // /**
  // * Clone an instance, that is just create a new instance and move the new
  // * {@link XdmNodeReadTrx} to the current node.
  // *
  // * @return new instance
  // * @throws SirixException
  // * if Sirix fails
  // */
  // XdmNodeReadTrx cloneInstance() throws SirixException;

  /**
   * Get the number of nodes which reference to the name.
   *
   * @param name name to lookup
   * @param kind node kind
   * @return number of nodes with the same name and node kind
   */
  int getNameCount(String name, @Nonnull NodeKind kind);

  /**
   * Get the type key of the node.
   *
   * @return type key
   */
  int getTypeKey();

  /**
   * Get the attribute key of the index (for element nodes).
   *
   * @param index the index to get key for
   * @return attribute key for index or {@code -1} if no attribute with the given index is available
   */
  long getAttributeKey(@Nonnegative int index);

  /**
   * Determines if current node has children.
   *
   * @return {@code true} if it has children, {@code false} otherwise
   */
  @Override
  boolean hasChildren();

  /**
   * Determines if current node has attributes (only elements might have attributes).
   *
   * @return {@code true} if it has attributes, {@code false} otherwise
   */
  boolean hasAttributes();

  /**
   * Determines if current node has namespaces (only elements might have namespaces).
   *
   * @return {@code true} if it has namespaces, {@code false} otherwise
   */
  boolean hasNamespaces();

  /**
   * Get the path node key of the currently selected node. Make sure to check if the node has a name
   * through calling {@link #isNameNode()} at first.
   *
   * @return the path node key if the currently selected node is a name node, {@code -1} else
   */
  @Override
  long getPathNodeKey();

  /**
   * Get the type of path. Make sure to check if the node has a name through calling
   * {@link #isNameNode()} at first.
   *
   * @return the path kind of the currently selected node or {@code null} if the node isn't a node
   *         with a name
   */
  @Override
  NodeKind getPathKind();

  /**
   * Determines if current node is a structural node (element-, text-, comment- and processing
   * instruction)
   *
   * @return {@code true} if it is a structural node, {@code false} otherwise
   */
  boolean isStructuralNode();

  /**
   * Determines if current node is a {@link ValueNode}.
   *
   * @return {@code true} if it has a value, {@code false} otherwise
   */
  boolean isValueNode();

  /**
   * Get the hash of the currently selected node.
   *
   * @return hash value
   */
  @Override
  BigInteger getHash();

  /**
   * Get all attributes of currently selected node (only for elements useful, otherwise returns an
   * empty list).
   *
   * @return all attribute keys
   */
  List<Long> getAttributeKeys();

  /**
   * Get all namespaces of currently selected node (only for elements useful, otherwise returns an
   * empty list).
   *
   * @return all namespace keys
   */
  List<Long> getNamespaceKeys();

  /**
   * Get raw value byte-array of currently selected node
   *
   * @return value of node
   */
  byte[] getRawValue();

  /**
   * Number of children of current node.
   *
   * @return number of children of current node
   */
  @Override
  long getChildCount();

  /**
   * Number of descendants of current node.
   *
   * @return number of descendants of current node
   */
  @Override
  long getDescendantCount();

  /**
   * Get the namespace URI of the current node.
   *
   * @return namespace URI
   */
  String getNamespaceURI();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveTo(long nodeKey);

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToDocumentRoot();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToFirstChild();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToLastChild();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToLeftSibling();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToParent();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToRightSibling();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToPrevious();

  @Override
  Move<? extends XmlNodeReadOnlyTrx> moveToNext();

  /**
   * Determines if current node is an {@link ElementNode}.
   *
   * @return {@code true}, if it is an element node, {@code false} otherwise
   */
  boolean isElement();

  /**
   * Determines if current node is a {@link TextNode}.
   *
   * @return {@code true}, if it is an text node, {@code false} otherwise
   */
  boolean isText();

  /**
   * Determines if current node is the {@link XmlDocumentRootNode}.
   *
   * @return {@code true}, if it is the document root node, {@code false} otherwise
   */
  @Override
  boolean isDocumentRoot();

  /**
   * Determines if current node is a {@link CommentNode}.
   *
   * @return {@code true}, if it is a comment node, {@code false} otherwise
   */
  boolean isComment();

  /**
   * Determines if current node is an {@link AttributeNode}.
   *
   * @return {@code true}, if it is an attribute node, {@code false} otherwise
   */
  boolean isAttribute();

  /**
   * Determines if current node is a {@link NamespaceNode}.
   *
   * @return {@code true}, if it is a namespace node, {@code false} otherwise
   */
  boolean isNamespace();

  /**
   * Determines if current node is a {@link PINode}.
   *
   * @return {@code true}, if it is a processing instruction node, {@code false} otherwise
   */
  boolean isPI();

  /**
   * Get the current node as a {@link ImmutableNameNode}. First check with {@link#isNameNode()}.
   *
   * @return the current node, casted to a {@link ImmutableNameNode}
   */
  ImmutableNameNode getNameNode();

  /**
   * Get the current node as a {@link ImmutableValueNode}. First check with {@link#isValueNode()}.
   *
   * @return the current node, casted to a {@link ImmutableValueNode}
   */
  ImmutableValueNode getValueNode();

  /**
   * Get the {@link ResourceManager} this instance is bound to.
   *
   * @return the resource manager
   */
  @Override
  XmlResourceManager getResourceManager();
}

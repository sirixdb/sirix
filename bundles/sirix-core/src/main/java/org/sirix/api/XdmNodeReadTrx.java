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

package org.sirix.api;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.service.xml.xpath.AtomicValue;

/**
 * <h1>NodeReadTrx</h1>
 *
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
 * <p>
 * <ol>
 * <li>Only a single thread accesses each NodeReadTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>NodeReadTrx.getNodeKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after moving cursor:
 * <code>(NodeReadTrx.moveToX().hasMoved() &&
 *       NodeReadTrx.getNodeKey() == m) ||
 *       (!NodeReadTrx.moveToX().hasMoved() &&
 *       NodeReadTrx.getNodeKey() == n)</code>.</li>
 * </ol>
 * </p>
 *
 * <h2>User Example</h2>
 *
 * <p>
 *
 * <pre>
 *   try(final NodeReadTrx rtx = session.beginNodeReadTrx()) {
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
 * </p>
 *
 * <h2>Developer Example</h2>
 *
 * <p>
 *
 * <pre>
 *   public void someNodeReadTrxMethod() {
 *     // This must be called to make sure the transaction is not closed.
 *     assertNotClosed();
 *     ...
 *   }
 * </pre>
 *
 * </p>
 */
public interface XdmNodeReadTrx extends NodeCursor {

  PageReadTrx getPageTrx();

  /** String constants used by XPath. */
  String[] XPATHCONSTANTS = {"xs:anyType", "xs:anySimpleType", "xs:anyAtomicType",
      "xs:untypedAtomic", "xs:untyped", "xs:string", "xs:duration", "xs:yearMonthDuration",
      "xs:dayTimeDuration", "xs:dateTime", "xs:time", "xs:date", "xs:gYearMonth", "xs:gYear",
      "xs:gMonthDay", "xs:gDay", "xs:gMonth", "xs:boolean", "xs:base64Binary", "xs:hexBinary",
      "xs:anyURI", "xs:QName", "xs:NOTATION", "xs:float", "xs:double", "xs:pDecimal", "xs:decimal",
      "xs:integer", "xs:long", "xs:int", "xs:short", "xs:byte", "xs:nonPositiveInteger",
      "xs:negativeInteger", "xs:nonNegativeInteger", "xs:positiveInteger", "xs:unsignedLong",
      "xs:unsignedInt", "xs:unsignedShort", "xs:unsignedByte", "xs:normalizedString", "xs:token",
      "xs:language", "xs:name", "xs:NCName", "xs:ID", "xs:IDREF", "xs:ENTITY", "xs:IDREFS",
      "xs:NMTOKEN", "xs:NMTOKENS",};

  /**
   * Get ID of reader.
   *
   * @return ID of reader
   */
  long getId();

  /**
   * Get the revision number of this transaction.
   *
   * @return immutable revision number of this IReadTransaction
   */
  int getRevisionNumber();

  /**
   * UNIX-style timestamp of the commit of the revision.
   *
   * @throws SirixIOException if can't get timestamp
   */
  long getRevisionTimestamp();

  /**
   * Getting the maximum nodekey available in this revision.
   *
   * @return the maximum nodekey
   */
  long getMaxNodeKey();

  // --- Node Selectors
  // --------------------------------------------------------

  /**
   * Move cursor to attribute by its index.
   *
   * @param index index of attribute to move to
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XdmNodeReadTrx> moveToAttribute(@Nonnegative int index);

  /**
   * Move cursor to attribute by its name key.
   *
   * @param name {@link QNm} of attribute
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XdmNodeReadTrx> moveToAttributeByName(QNm name);

  /**
   * Move cursor to namespace declaration by its index.
   *
   * @param index index of attribute to move to
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XdmNodeReadTrx> moveToNamespace(@Nonnegative int index);

  /**
   * Move to the next following node, that is the next node on the XPath {@code following::-axis},
   * that is the next node which is not a descendant of the current node.
   *
   * @return {@link Moved} instance if the attribute node is selected, {@code NotMoved} instance
   *         otherwise
   */
  Move<? extends XdmNodeReadTrx> moveToNextFollowing();

  // --- Node Getters
  // ----------------------------------------------------------

  /**
   * Getting the value of the current node.
   *
   * @return the current value of the node
   */
  String getValue();

  /**
   * Getting the name of a current node.
   *
   * @return the {@link QNm} of the node
   */
  QNm getName();

  /**
   * Getting the type of the current node.
   *
   * @return the type of the node
   */
  String getType();

  /**
   * Get key for given name. This is used for efficient name testing.
   *
   * @param name name, i.e., local part, URI, or prefix
   * @return internal key assigned to given name
   */
  int keyForName(String name);

  /**
   * Get name for key. This is used for efficient key testing.
   *
   * @param key key, i.e., local part key, URI key, or prefix key.
   * @return String containing name for given key
   */
  String nameForKey(int key);

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
   * Close shared read transaction and immediately release all resources.
   *
   * This is an idempotent operation and does nothing if the transaction is already closed.
   *
   * @throws SirixException if can't close {@link XdmNodeReadTrx}
   */
  @Override
  void close();

  /**
   * Is this transaction closed?
   *
   * @return {@code true} if closed, {@code false} otherwise
   */
  boolean isClosed();

  /**
   * Get the node key of the currently selected node.
   *
   * @return node key of the currently selected node
   */
  @Override
  long getNodeKey();

  /**
   * Get the left sibling node key of the currently selected node.
   *
   * @return left sibling node key of the currently selected node
   */
  long getLeftSiblingKey();

  /**
   * Get the right sibling node key of the currently selected node.
   *
   * @return right sibling node key of the currently selected node
   */
  long getRightSiblingKey();

  /**
   * Get the first child key of the currently selected node.
   *
   * @return first child key of the currently selected node
   */
  long getFirstChildKey();

  /**
   * Get the last child key of the currently selected node.
   *
   * @return last child key of the currently selected node
   */
  long getLastChildKey();

  /**
   * Get the parent key of the currently selected node.
   *
   * @return parent key of the currently selected node
   */
  long getParentKey();

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

  /**
   * Get the {@link ResourceManager} this instance is bound to.
   *
   * @return session instance
   */
  ResourceManager getResourceManager();

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
   * @return number of nodes with the same name and node kind
   */
  int getNameCount(String name, @Nonnull Kind kind);

  /**
   * Get the type key of the node.
   *
   * @return type key
   */
  int getTypeKey();

  /**
   * Get the attribute key of the index (for element nodes).
   *
   * @return attribute key for index or {@code -1} if no attribute with the given index is available
   */
  long getAttributeKey(@Nonnegative int index);

  /**
   * Determines if current node has children.
   *
   * @return {@code true} if it has children, {@code false} otherwise
   */
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
  long getPathNodeKey();

  /**
   * Get the type of path. Make sure to check if the node has a name through calling
   * {@link #isNameNode()} at first.
   *
   * @return the path kind of the currently selected node or {@code null} if the node isn't a node
   *         with a name
   */
  Kind getPathKind();

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
  long getHash();

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
  long getChildCount();

  /**
   * Number of descendants of current node.
   *
   * @return number of descendants of current node
   */
  long getDescendantCount();

  /**
   * Get the namespace URI of the current node.
   *
   * @return namespace URI
   */
  String getNamespaceURI();

  @Override
  public Move<? extends XdmNodeReadTrx> moveTo(long key);

  @Override
  public Move<? extends XdmNodeReadTrx> moveToDocumentRoot();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToFirstChild();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToLastChild();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToLeftSibling();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToParent();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToRightSibling();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToPrevious();

  @Override
  public Move<? extends XdmNodeReadTrx> moveToNext();

  /**
   * Determines if current node is an {@link ElementNode}.
   *
   * @return {@code true}, if it is an element node, {@code false} otherwise
   */
  public boolean isElement();

  /**
   * Determines if current node is a {@link TextNode}.
   *
   * @return {@code true}, if it is an text node, {@code false} otherwise
   */
  public boolean isText();

  /**
   * Determines if current node is the {@link DocumentRootNode}.
   *
   * @return {@code true}, if it is the document root node, {@code false} otherwise
   */
  public boolean isDocumentRoot();

  /**
   * Determines if current node is a {@link CommentNode}.
   *
   * @return {@code true}, if it is a comment node, {@code false} otherwise
   */
  public boolean isComment();

  /**
   * Determines if current node is an {@link AttributeNode}.
   *
   * @return {@code true}, if it is an attribute node, {@code false} otherwise
   */
  public boolean isAttribute();

  /**
   * Determines if current node is a {@link NamespaceNode}.
   *
   * @return {@code true}, if it is a namespace node, {@code false} otherwise
   */
  public boolean isNamespace();

  /**
   * Determines if current node is a {@link PINode}.
   *
   * @return {@code true}, if it is a processing instruction node, {@code false} otherwise
   */
  public boolean isPI();

  /**
   * Get the current node as a {@link ImmutableNameNode}. First check with {@link#isNameNode()}.
   *
   * @return the current node, casted to a {@link ImmutableNameNode}
   */
  public ImmutableNameNode getNameNode();

  /**
   * Get the current node as a {@link ImmutableValueNode}. First check with {@link#isValueNode()}.
   *
   * @return the current node, casted to a {@link ImmutableValueNode}
   */
  public ImmutableValueNode getValueNode();
}

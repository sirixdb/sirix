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

package org.sirix.api;

import javax.xml.namespace.QName;

import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.service.xml.xpath.AtomicValue;

/**
 * <h1>IReadTransaction</h1>
 * 
 * <h2>Description</h2>
 * 
 * <p>
 * Interface to access nodes based on the
 * Key/ParentKey/FirstChildKey/LeftSiblingKey/RightSiblingKey/ChildCount/DescendantCount encoding. This
 * encoding keeps the children ordered but has no knowledge of the global node ordering. The underlying tree
 * is accessed in a cursor-like fashion.
 * </p>
 * 
 * <h2>Convention</h2>
 * 
 * <p>
 * <ol>
 * <li>Only a single thread accesses each IReadTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>IReadTransaction.getItem().getKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after moving cursor: <code>(IReadTransaction.moveX() == true &&
 *       IReadTransaction.getItem().getKey() == m) ||
 *       (IReadTransaction.moveX() == false &&
 *       IReadTransaction.getItem().getKey() == n)</code>.</li>
 * </ol>
 * </p>
 * 
 * <h2>User Example</h2>
 * 
 * <p>
 * 
 * <pre>
 *   final IReadTransaction rtx = session.beginReadTransaction();
 *   
 *   // Either test before moving...
 *   if (rtx.getRelatedNode().hasFirstChild()) {
 *     rtx.moveToFirstChild();
 *     ...
 *   }
 *   
 *   // or test after moving. Whatever, do the test!
 *   if (rtx.moveToFirstChild()) {
 *     ...
 *   }
 *   
 *   // Access local part of element.
 *   if (rtx.getRelatedNode().isElement() &amp;&amp; 
 *   rtx.getRelatedNode().getName().equalsIgnoreCase(&quot;foo&quot;) {
 *     ...
 *   }
 *   
 *   // Access value of first attribute of element.
 *   if (rtx.getRelatedNode().isElement() &amp;&amp; (rtx.getRelatedNode().getAttributeCount() &gt; 0)) {
 *     rtx.moveToAttribute(0);
 *     System.out.println(UTF.parseString(rtx.getValue()));
 *   }
 *   
 *   rtx.close();
 * </pre>
 * 
 * </p>
 * 
 * <h2>Developer Example</h2>
 * 
 * <p>
 * 
 * <pre>
 *   public final void someIReadTransactionMethod() {
 *     // This must be called to make sure the transaction is not closed.
 *     assertNotClosed();
 *     ...
 *   }
 * </pre>
 * 
 * </p>
 */
public interface INodeReadTrx extends INodeTraversal {

  /** String constants used by XPath. */
  String[] XPATHCONSTANTS = {
    "xs:anyType", "xs:anySimpleType", "xs:anyAtomicType", "xs:untypedAtomic", "xs:untyped", "xs:string",
    "xs:duration", "xs:yearMonthDuration", "xs:dayTimeDuration", "xs:dateTime", "xs:time", "xs:date",
    "xs:gYearMonth", "xs:gYear", "xs:gMonthDay", "xs:gDay", "xs:gMonth", "xs:boolean", "xs:base64Binary",
    "xs:hexBinary", "xs:anyURI", "xs:QName", "xs:NOTATION", "xs:float", "xs:double", "xs:pDecimal",
    "xs:decimal", "xs:integer", "xs:long", "xs:int", "xs:short", "xs:byte", "xs:nonPositiveInteger",
    "xs:negativeInteger", "xs:nonNegativeInteger", "xs:positiveInteger", "xs:unsignedLong", "xs:unsignedInt",
    "xs:unsignedShort", "xs:unsignedByte", "xs:normalizedString", "xs:token", "xs:language", "xs:name",
    "xs:NCName", "xs:ID", "xs:IDREF", "xs:ENTITY", "xs:IDREFS", "xs:NMTOKEN", "xs:NMTOKENS",
  };

  /**
   * Get ID of transaction.
   * 
   * @return ID of transaction
   */
  long getTransactionID();

  /**
   * Get the revision number of this transaction.
   * 
   * @return immutable revision number of this IReadTransaction
   * @throws TTIOException
   *           if can't get revision number
   */
  long getRevisionNumber() throws TTIOException;

  /**
   * UNIX-style timestamp of the commit of the revision.
   * 
   * @throws TTIOException
   *           if can't get timestamp
   * @return timestamp of revision commit
   */
  long getRevisionTimestamp() throws TTIOException;

  /**
   * Getting the maximum nodekey available in this revision.
   * 
   * @return the maximum nodekey
   * @throws TTIOException
   *           if can't get maxNodKey
   */
  long getMaxNodeKey() throws TTIOException;

  // --- Node Selectors
  // --------------------------------------------------------

  /**
   * Move cursor to attribute by its index.
   * 
   * @param pIndex
   *          index of attribute to move to
   * @return {@code true} if the attribute node is selected, {@code false} otherwise
   */
  boolean moveToAttribute(int pIndex);
  
  /**
   * Move cursor to attribute by its name key.
   * 
   * @param pNameKey
   *          name key of attribute to move to
   * @return {@code true} if the attribute node is selected, {@code false} otherwise
   */
  boolean moveToAttributeByNameKey(int pNameKey);

  /**
   * Move cursor to namespace declaration by its index.
   * 
   * @param pIndex
   *          Index of attribute to move to.
   * @return {@code true} if the namespace node is selected, {@code false} otherwise
   */
  boolean moveToNamespace(int pIndex);

  /**
   * Move to the next following node, that is the next node on the following axis.
   * 
   * @return {@code true} if the transaction moved, {@code false} otherwise
   */
  boolean moveToNextFollowing();

  // --- Node Getters
  // ----------------------------------------------------------

  /**
   * Getting the value of the current node.
   * 
   * @return the current value of the node
   */
  String getValueOfCurrentNode();

  /**
   * Getting the name of a current node.
   * 
   * @return the {@link QName} of the node
   */
  QName getQNameOfCurrentNode();

  /**
   * Getting the type of the current node.
   * 
   * @return the normal type of the node
   */
  String getTypeOfCurrentNode();

  /**
   * Get key for given name. This is used for efficient name testing.
   * 
   * @param pName
   *          Name, i.e., local part, URI, or prefix.
   * @return internal key assigned to given name
   */
  int keyForName(String pName);

  /**
   * Get name for key. This is used for efficient key testing.
   * 
   * @param pKey
   *          key, i.e., local part key, URI key, or prefix key.
   * @return String containing name for given key
   */
  String nameForKey(int pKey);

  /**
   * Get raw name for key. This is used for efficient key testing.
   * 
   * @param pKey
   *          key, i.e., local part key, URI key, or prefix key.
   * @return byte array containing name for given key
   */
  byte[] rawNameForKey(int pKey);

  /**
   * Get item list containing volatile items such as atoms or fragments.
   * 
   * @return item list
   */
  IItemList<AtomicValue> getItemList();

  /**
   * Close shared read transaction and immediately release all resources.
   * 
   * This is an idempotent operation and does nothing if the transaction is
   * already closed.
   * 
   * @throws AbsTTException
   *           if can't close {@link INodeReadTrx}
   */
  @Override
  void close() throws AbsTTException;

  /**
   * Is this transaction closed?
   * 
   * @return {@code true} if closed, {@code false} otherwise
   */
  boolean isClosed();

  /**
   * Get the {@link ISession} this instance is bound to.
   * 
   * @return session instance
   */
  ISession getSession();
  
  INodeReadTrx cloneInstance() throws AbsTTException;
}

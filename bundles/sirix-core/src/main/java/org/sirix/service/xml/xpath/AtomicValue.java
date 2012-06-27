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

package org.sirix.service.xml.xpath;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.ENode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.service.xml.xpath.types.Type;
import org.sirix.settings.EFixed;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.TypedValue;

/**
 * <h1>AtomicValue</h1>
 * <p>
 * An item represents either an atomic value or a node. An atomic value is a value in the value space of an
 * atomic type, as defined in <a href="http://www.w3.org/TR/xmlschema11-2/">XMLSchema 1.1</a>. (Definition:
 * Atomic types are anyAtomicType and all types derived from it.)
 * </p>
 */
public class AtomicValue implements INode, IValNode {

  /** Value of the item as byte array. */
  private byte[] mValue;

  /** The item's value type. */
  private int mType;

  /**
   * The item's key. In case of an Atomic value this is always a negative to
   * make them distinguishable from nodes.
   */
  private long mItemKey;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param mValue
   *          the value of the Item
   * @param mType
   *          the item's type
   */
  public AtomicValue(final byte[] mValue, final int mType) {

    this.mValue = mValue;
    this.mType = mType;
  }

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param mValue
   *          the value of the Item
   */
  public AtomicValue(final boolean mValue) {

    this.mValue = TypedValue.getBytes(Boolean.toString(mValue));
    this.mType = NamePageHash.generateHashForString("xs:boolean");

  }

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param mValue
   *          the value of the Item
   * @param mType
   *          the item's type
   */
  public AtomicValue(final Number mValue, final Type mType) {

    this.mValue = TypedValue.getBytes(mValue.toString());
    this.mType = NamePageHash.generateHashForString(mType.getStringRepr());
  }

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param mValue
   *          the value of the Item
   * @param mType
   *          the item's type
   */
  public AtomicValue(final String mValue, final Type mType) {

    this.mValue = TypedValue.getBytes(mValue);
    this.mType = NamePageHash.generateHashForString(mType.getStringRepr());
  }

  /**
   * Set node key.
   * 
   * @param pItemKey
   *          unique item key
   */
  public void setNodeKey(final long pItemKey) {
    mItemKey = pItemKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getParentKey() {
    return EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasParent() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getNodeKey() {
    return mItemKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ENode getKind() {
    return ENode.UNKOWN_KIND;
  }

  /**
   * Check if is fulltext.
   * 
   * @return true if fulltext, false otherwise
   */
  public boolean isFullText() {

    return false;
  }

  /**
   * Test if the lead is tes.
   * 
   * @return true if fulltest leaf, false otherwise
   */
  public boolean isFullTextLeaf() {

    return false;
  }

  /**
   * Test if the root is full text.
   * 
   * @return true if fulltest root, false otherwise
   */
  public boolean isFullTextRoot() {

    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int getTypeKey() {
    return mType;
  }

  /**
   * Getting the type of the value.
   * 
   * @return the type of this value
   */
  public final String getType() {
    return Type.getType(mType).getStringRepr();
  }

  /**
   * Returns the atomic value as an integer.
   * 
   * @return the value as an integer
   */
  public int getInt() {

    return (int)getDBL();
  }

  /**
   * Returns the atomic value as a boolean.
   * 
   * @return the value as a boolean
   */
  public boolean getBool() {

    return Boolean.parseBoolean(new String(mValue));
  }

  /**
   * Returns the atomic value as a float.
   * 
   * @return the value as a float
   */
  public float getFLT() {

    return Float.parseFloat(new String(mValue));
  }

  /**
   * Returns the atomic value as a double.
   * 
   * @return the value as a double
   */
  public double getDBL() {

    return Double.parseDouble(new String(mValue));
  }

  /**
   * To String method.
   * 
   * @return String String representation of this node
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Atomic Value: ");
    builder.append(new String(mValue));
    builder.append("\nKey: ");
    builder.append(mItemKey);
    return builder.toString();
  }

  @Override
  public void setHash(long hash) {
    // TODO Auto-generated method stub

  }

  @Override
  public long getHash() {
    // TODO Auto-generated method stub
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public EVisitResult acceptVisitor(final IVisitor paramVisitor) {
    // Do nothing.
    return EVisitResult.CONTINUE;
  }

  @Override
  public AtomicValue clone() {
    return this;
  }

  @Override
  public void setParentKey(long paramKey) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTypeKey(int paramType) {
    // TODO Auto-generated method stub

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getRawValue() {
    return mValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setValue(byte[] paramVal) {
    mValue = paramVal;
  }

  @Override
  public boolean isSameItem(INode pOther) {
    // TODO Auto-generated method stub
    return false;
  }
}

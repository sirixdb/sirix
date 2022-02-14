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

package org.sirix.service.xml.xpath;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.service.xml.xpath.types.Type;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.TypedValue;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * An item represents either an atomic value or a node. An atomic value is a value in the value
 * space of an atomic type, as defined in <a href="http://www.w3.org/TR/xmlschema11-2/">XMLSchema
 * 1.1</a>. (Definition: Atomic types are anyAtomicType and all types derived from it.)
 * </p>
 */
public final class AtomicValue implements Node, ValueNode, ImmutableXmlNode {

  /** Value of the item as byte array. */
  private byte[] mValue;

  /** The item's value type. */
  private int mType;

  /**
   * The item's key. In case of an Atomic value this is always a negative to make them distinguishable
   * from nodes.
   */
  private long mItemKey;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param value the value of the Item
   * @param type the item's type
   */
  public AtomicValue(final byte[] value, final int type) {
    mValue = checkNotNull(value);
    mType = type;
  }

  /**
   * Constructor. Initializes the internal state.
   *
   * @param pValue the value of the Item
   */
  public AtomicValue(final boolean pValue) {
    mValue = TypedValue.getBytes(Boolean.toString(pValue));
    mType = NamePageHash.generateHashForString("xs:boolean");
  }

  /**
   * Constructor. Initializes the internal state.
   *
   * @param pValue the value of the Item
   * @param pType the item's type
   */
  public AtomicValue(final Number pValue, final Type pType) {

    mValue = TypedValue.getBytes(pValue.toString());
    mType = NamePageHash.generateHashForString(pType.getStringRepr());
  }

  /**
   * Constructor. Initializes the internal state.
   *
   * @param pValue the value of the Item
   * @param pType the item's type
   */
  public AtomicValue(final String pValue, @NonNull final Type pType) {
    mValue = TypedValue.getBytes(pValue);
    mType = NamePageHash.generateHashForString(pType.getStringRepr());
  }

  /**
   * Set node key.
   *
   * @param pItemKey unique item key
   */
  public void setNodeKey(final long pItemKey) {
    mItemKey = pItemKey;
  }

  @Override
  public long getParentKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasParent() {
    return false;
  }

  @Override
  public long getNodeKey() {
    return mItemKey;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ATOMIC;
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
    return (int) getDBL();
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
   * @return String representation of this node
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
  public BigInteger computeHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final BigInteger hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigInteger getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AtomicValue clone() {
    return this;
  }

  @Override
  public void setParentKey(final long pKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTypeKey(final int pType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getRawValue() {
    return mValue;
  }

  @Override
  public void setValue(byte[] pVal) {
    mValue = checkNotNull(pVal);
  }

  @Override
  public boolean isSameItem(@Nullable final Node pOther) {
    return false;
  }

  @Override
  public long getRevision() {
    return -1; // Not needed over here.
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getValue() {
    return new String(mValue, Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    throw new UnsupportedOperationException();
  }
}

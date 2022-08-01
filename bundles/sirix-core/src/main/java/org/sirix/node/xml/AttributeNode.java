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

package org.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.HashCode;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.immutable.xml.ImmutableAttributeNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Constants;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

/**
 * <p>
 * Node representing an attribute.
 * </p>
 */
public final class AttributeNode extends AbstractForwardingNode implements ValueNode, NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate nameDel;

  /** Delegate for val node information. */
  private final ValueNodeDelegate valDel;

  /** Node delegate. */
  private final NodeDelegate nodeDel;

  /** The qualified name. */
  private final QNm qNm;

  private BigInteger hash;

  /**
   * Creating an attribute.
   *
   * @param nodeDel {@link NodeDelegate} to be set
   * @param nodeDel {@link StructNodeDelegate} to be set
   * @param valDel {@link ValueNodeDelegate} to be set
   */
  public AttributeNode(final NodeDelegate nodeDel, final NameNodeDelegate nameDel, final ValueNodeDelegate valDel,
      final QNm qNm) {
    assert nodeDel != null : "nodeDel must not be null!";
    this.nodeDel = nodeDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valDel = valDel;
    assert qNm != null : "qNm must not be null!";
    this.qNm = qNm;
  }

  /**
   * Creating an attribute.
   *
   * @param nodeDel {@link NodeDelegate} to be set
   * @param nodeDel {@link StructNodeDelegate} to be set
   * @param valDel {@link ValueNodeDelegate} to be set
   */
  public AttributeNode(final BigInteger hashCode, final NodeDelegate nodeDel, final NameNodeDelegate nameDel,
      final ValueNodeDelegate valDel, final QNm qNm) {
    hash = hashCode;
    assert nodeDel != null : "nodeDel must not be null!";
    this.nodeDel = nodeDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valDel = valDel;
    assert qNm != null : "qNm must not be null!";
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ATTRIBUTE;
  }

  @Override
  public BigInteger computeHash() {
    final HashCode valueHashCode = nodeDel.getHashFunction().hashBytes(getRawValue());

    final BigInteger valueBigInteger = new BigInteger(1, valueHashCode.asBytes());

    var result = BIG_INT_31.add(nodeDel.computeHash());
    result = BIG_INT_31.multiply(result).add(nameDel.computeHash());
    result = BIG_INT_31.multiply(result).add(valueBigInteger);

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableAttributeNode.of(this));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("nameDel", nameDel).add("valDel", valDel).toString();
  }

  @Override
  public int getPrefixKey() {
    return nameDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameDel.getURIKey();
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    nameDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    nameDel.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    nameDel.setURIKey(uriKey);
  }

  @Override
  public byte[] getRawValue() {
    return valDel.getRawValue();
  }

  @Override
  public void setValue(final byte[] value) {
    valDel.setValue(value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nameDel, valDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof AttributeNode) {
      final AttributeNode other = (AttributeNode) obj;
      return Objects.equal(nameDel, other.nameDel) && Objects.equal(valDel, other.valDel);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    nameDel.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameDel.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return the {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameDel;
  }

  /**
   * Getting the inlying {@link ValueNodeDelegate}.
   *
   * @return the {@link ValueNodeDelegate} instance
   */
  public ValueNodeDelegate getValNodeDelegate() {
    return valDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return nodeDel;
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  @Override
  public String getValue() {
    return new String(valDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeDel.getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return nodeDel.getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDel.getDeweyIDAsBytes();
  }
}

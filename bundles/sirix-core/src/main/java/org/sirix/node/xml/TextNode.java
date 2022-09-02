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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.immutable.xml.ImmutableText;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import java.math.BigInteger;

/**
 * <p>
 * Node representing a text node.
 * </p>
 */
public final class TextNode extends AbstractStructForwardingNode implements ValueNode, ImmutableXmlNode {

  /** Delegate for common value node information. */
  private final ValueNodeDelegate valDel;

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate structNodeDel;

  /** Value of the node. */
  private byte[] value;

  private BigInteger hash;

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public TextNode(final BigInteger hashCode, final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    hash = hashCode;
    assert structDel != null;
    structNodeDel = structDel;
    assert valDel != null;
    this.valDel = valDel;
  }

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public TextNode(final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    assert structDel != null;
    structNodeDel = structDel;
    assert valDel != null;
    this.valDel = valDel;
  }

  @Override
  public BigInteger computeHash() {
    var result = BIG_INT_31.add(structNodeDel.getNodeDelegate().computeHash());
    result = BIG_INT_31.multiply(result).add(structNodeDel.computeHash());
    result = BIG_INT_31.multiply(result).add(valDel.computeHash());

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hash;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.TEXT;
  }

  @Override
  public byte[] getRawValue() {
    if (value == null) {
      value = valDel.getRawValue();
    }
    return value;
  }

  @Override
  public void setValue(final byte[] value) {
    this.value = null;
    valDel.setValue(value);
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableText.of(this));
  }

  @Override
  public void decrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void decrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(structNodeDel.getNodeDelegate(), valDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof TextNode other) {
      return Objects.equal(structNodeDel.getNodeDelegate(), other.getNodeDelegate()) && valDel.equals(other.valDel);
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", structNodeDel.getNodeDelegate())
                      .add("struct delegate", structNodeDel)
                      .add("value delegate", valDel)
                      .toString();
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return valDel;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return structNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  @Override
  public String getValue() {
    return new String(valDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDel.getNodeDelegate().getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structNodeDel.getDeweyIDAsBytes();
  }
}

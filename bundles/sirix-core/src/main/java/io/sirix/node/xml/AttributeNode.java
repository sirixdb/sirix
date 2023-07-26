/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Node representing an attribute.
 */
public final class AttributeNode extends AbstractForwardingNode implements ValueNode, NameNode, ImmutableXmlNode {

  /**
   * Delegate for name node information.
   */
  private final NameNodeDelegate nameNodeDelegate;

  /**
   * Delegate for val node information.
   */
  private final ValueNodeDelegate valueNodeDelegate;

  /**
   * Node delegate.
   */
  private final NodeDelegate nodeDelegate;

  /**
   * The qualified name.
   */
  private final QNm qNm;

  private long hash;

  /**
   * Creating an attribute.
   *
   * @param nodeDel {@link NodeDelegate} to be set
   * @param nameDel {@link StructNodeDelegate} to be set
   * @param valDel  {@link ValueNodeDelegate} to be set
   * @param qNm     the QName to be set
   */
  public AttributeNode(final NodeDelegate nodeDel, final NameNodeDelegate nameDel, final ValueNodeDelegate valDel,
      final QNm qNm) {
    assert nodeDel != null : "nodeDel must not be null!";
    this.nodeDelegate = nodeDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameNodeDelegate = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valueNodeDelegate = valDel;
    assert qNm != null : "qNm must not be null!";
    this.qNm = qNm;
  }

  /**
   * Creating an attribute.
   *
   * @param hashCode the hash code
   * @param nodeDel  {@link NodeDelegate} to be set
   * @param nameDel  {@link StructNodeDelegate} to be set
   * @param valDel   {@link ValueNodeDelegate} to be set
   * @param qNm      the QName to be set
   */
  public AttributeNode(final long hashCode, final NodeDelegate nodeDel, final NameNodeDelegate nameDel,
      final ValueNodeDelegate valDel, final QNm qNm) {
    hash = hashCode;
    assert nodeDel != null : "nodeDel must not be null!";
    this.nodeDelegate = nodeDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameNodeDelegate = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valueNodeDelegate = valDel;
    assert qNm != null : "qNm must not be null!";
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ATTRIBUTE;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    final var rawValue = valueNodeDelegate.getRawValue();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(nameNodeDelegate.getPrefixKey())
         .writeLong(nameNodeDelegate.getLocalNameKey())
         .writeLong(nameNodeDelegate.getURIKey());

    bytes.write(rawValue);

    final var buffer = bytes.underlyingObject().rewind();
    buffer.limit((int) bytes.readLimit());

    return nodeDelegate.getHashFunction().hashBytes(buffer);
  }

  @Override
  public void setHash(long hash) {
    this.hash = hash;
  }

  @Override
  public long getHash() {
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableAttributeNode.of(this));
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nameDel", nameNodeDelegate)
                      .add("valDel", valueNodeDelegate)
                      .toString();
  }

  @Override
  public int getPrefixKey() {
    return nameNodeDelegate.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameNodeDelegate.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameNodeDelegate.getURIKey();
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    hash = 0L;
    nameNodeDelegate.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    hash = 0L;
    nameNodeDelegate.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    hash = 0L;
    nameNodeDelegate.setURIKey(uriKey);
  }

  @Override
  public byte[] getRawValue() {
    return valueNodeDelegate.getRawValue();
  }

  @Override
  public void setRawValue(final byte[] value) {
    hash = 0L;
    valueNodeDelegate.setRawValue(value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nameNodeDelegate, valueNodeDelegate);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final AttributeNode other) {
      return Objects.equal(nameNodeDelegate, other.nameNodeDelegate) && Objects.equal(valueNodeDelegate,
                                                                                      other.valueNodeDelegate);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    nameNodeDelegate.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameNodeDelegate.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return the {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameNodeDelegate;
  }

  /**
   * Getting the inlying {@link ValueNodeDelegate}.
   *
   * @return the {@link ValueNodeDelegate} instance
   */
  public ValueNodeDelegate getValNodeDelegate() {
    return valueNodeDelegate;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  @Override
  public String getValue() {
    return new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeDelegate.getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return nodeDelegate.getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDelegate.getDeweyIDAsBytes();
  }
}

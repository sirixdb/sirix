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
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.immutable.xml.ImmutableComment;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;

/**
 * Comment node implementation.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CommentNode extends AbstractStructForwardingNode implements ValueNode, ImmutableXmlNode {

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate structNodeDelegate;

  /** {@link ValueNodeDelegate} reference. */
  private final ValueNodeDelegate valueNodeDelegate;

  /** Value of the node. */
  private byte[] value;

  private long hash;

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public CommentNode(final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    assert valDel != null;
    this.valueNodeDelegate = valDel;
    assert structDel != null;
    structNodeDelegate = structDel;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.COMMENT;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getLeftSiblingKey()).writeLong(structNodeDelegate.getRightSiblingKey());

    bytes.writeUtf8(new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING));

    final var buffer = bytes.underlyingObject().rewind();
    buffer.limit((int) bytes.readLimit());

    return nodeDelegate.getHashFunction().hashBytes(buffer);
  }

  @Override
  public void setHash(final long hash) {
    this.hash = hash;
  }

  @Override
  public long getHash() {
    if (hash == 0L) {
      hash = computeHash(Bytes.elasticHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public byte[] getRawValue() {
    if (value == null) {
      value = valueNodeDelegate.getRawValue();
    }
    return value;
  }

  @Override
  public void setRawValue(final byte[] value) {
    this.value = null;
    valueNodeDelegate.setRawValue(value);
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableComment.of(this));
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
  public void setDescendantCount(final long descendantCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(structNodeDelegate.getNodeDelegate(), valueNodeDelegate);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final CommentNode other) {
      return Objects.equal(structNodeDelegate.getNodeDelegate(), other.getNodeDelegate()) && valueNodeDelegate.equals(other.valueNodeDelegate);
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", structNodeDelegate.getNodeDelegate())
                      .add("value delegate", valueNodeDelegate)
                      .toString();
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return valueNodeDelegate;
  }

  @Override
  protected @NonNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  @Override
  public String getValue() {
    return new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDelegate.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDelegate.getNodeDelegate().getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structNodeDelegate.getDeweyIDAsBytes();
  }
}

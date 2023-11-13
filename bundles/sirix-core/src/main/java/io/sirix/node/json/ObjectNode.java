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

package io.sirix.node.json;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.chronicle.bytes.Bytes;

import io.sirix.node.xml.AbstractStructForwardingNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;

/**
 * @author Johannes Lichtenberger
 */
public final class ObjectNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  /**
   * {@link StructNodeDelegate} reference.
   */
  private final StructNodeDelegate structNodeDelegate;

  private long hash;

  /**
   * Constructor
   *
   * @param structNodeDelegate {@link StructNodeDelegate} to be set
   */
  public ObjectNode(final long hashCode, final StructNodeDelegate structNodeDelegate) {
    hash = hashCode;
    assert structNodeDelegate != null;
    this.structNodeDelegate = structNodeDelegate;
  }

  /**
   * Constructor
   *
   * @param structNodeDelegate {@link StructNodeDelegate} to be set
   */
  public ObjectNode(final StructNodeDelegate structNodeDelegate) {
    assert structNodeDelegate != null;
    this.structNodeDelegate = structNodeDelegate;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT;
  }

  @Override
  public long computeHash(final Bytes<ByteBuffer> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getChildCount())
         .writeLong(structNodeDelegate.getDescendantCount())
         .writeLong(structNodeDelegate.getLeftSiblingKey())
         .writeLong(structNodeDelegate.getRightSiblingKey())
         .writeLong(structNodeDelegate.getFirstChildKey());

    if (structNodeDelegate.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(structNodeDelegate.getLastChildKey());
    }

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
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableObjectNode.of(this));
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
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this).add("structDelegate", structNodeDelegate).toString();
  }

  @Override
  public int hashCode() {
    return delegate().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectKeyNode other))
      return false;

    return Objects.equal(delegate(), other.delegate());
  }

}

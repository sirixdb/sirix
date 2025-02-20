/*
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

package io.sirix.node.json;

import com.google.common.base.Objects;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.node.xml.AbstractStructForwardingNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Node representing the root of a document. This node is guaranteed to exist in revision 0 and can
 * not be removed.
 * </p>
 */
public final class JsonDocumentRootNode extends AbstractStructForwardingNode implements StructNode, ImmutableJsonNode {

  /**
   * {@link NodeDelegate} reference.
   */
  private final NodeDelegate nodeDelegate;

  /**
   * {@link StructNodeDelegate} reference.
   */
  private final StructNodeDelegate structNodeDelegate;

  /**
   * The hash code of the node.
   */
  private long hash;

  /**
   * Constructor.
   *
   * @param nodeDelegate   {@link NodeDelegate} reference
   * @param structNodeDelegate {@link StructNodeDelegate} reference
   */
  public JsonDocumentRootNode(@NonNull final NodeDelegate nodeDelegate, @NonNull final StructNodeDelegate structNodeDelegate) {
    this.nodeDelegate = requireNonNull(nodeDelegate);
    this.structNodeDelegate = requireNonNull(structNodeDelegate);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.JSON_DOCUMENT;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
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
    return visitor.visit(ImmutableJsonDocumentRootNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDelegate);
  }

  @Override
  public boolean equals(@Nullable final Object obj) {
    if (obj instanceof JsonDocumentRootNode other) {
      return Objects.equal(nodeDelegate, other.nodeDelegate);
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return super.toString();
  }

  @Override
  protected @NonNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }
}

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

package org.sirix.node.json;

import com.google.common.base.Objects;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Fixed;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 * <p>
 * Node representing the root of a document. This node is guaranteed to exist in revision 0 and can
 * not be removed.
 * </p>
 */
public final class JsonDocumentRootNode extends AbstractStructForwardingNode implements StructNode, ImmutableJsonNode {

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate nodeDel;

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate structNodeDel;

  /** The hash code of the node. */
  private BigInteger hash;

  /**
   * Constructor.
   *
   * @param nodeDel {@link NodeDelegate} reference
   * @param structDel {@link StructNodeDelegate} reference
   */
  public JsonDocumentRootNode(@NonNull final NodeDelegate nodeDel, @NonNull final StructNodeDelegate structDel) {
    this.nodeDel = checkNotNull(nodeDel);
    structNodeDel = checkNotNull(structDel);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.JSON_DOCUMENT;
  }

  @Override
  public BigInteger computeHash() {
    final var nodeDelegate = structNodeDel.getNodeDelegate();
    final HashFunction hashFunction = nodeDelegate.getHashFunction();

    final Funnel<StructNode> nodeFunnel = (StructNode node, PrimitiveSink into) -> {
      into = into.putLong(node.getNodeKey()).putLong(node.getParentKey()).putByte(node.getKind().getId());

      if (node.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
        into.putLong(node.getChildCount())
            .putLong(node.getDescendantCount())
            .putLong(node.getLeftSiblingKey())
            .putLong(node.getRightSiblingKey())
            .putLong(node.getFirstChildKey())
            .putLong(node.getLastChildKey());
      } else {
        into.putLong(node.getChildCount())
            .putLong(node.getDescendantCount())
            .putLong(node.getLeftSiblingKey())
            .putLong(node.getRightSiblingKey())
            .putLong(node.getFirstChildKey());
      }
    };

    return Node.to128BitsAtMaximumBigInteger(new BigInteger(1,
                                                            nodeDelegate.getHashFunction()
                                                                        .hashObject(this, nodeFunnel)
                                                                        .asBytes()));
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    if (hash == null) {
      hash = computeHash();
    }
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableJsonDocumentRootNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDel);
  }

  @Override
  public boolean equals(@Nullable final Object obj) {
    if (obj instanceof JsonDocumentRootNode) {
      final JsonDocumentRootNode other = (JsonDocumentRootNode) obj;
      return Objects.equal(nodeDel, other.nodeDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  protected NodeDelegate delegate() {
    return nodeDel;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }
}

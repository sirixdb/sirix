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

package org.sirix.node;

import com.google.common.base.Objects;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.settings.Fixed;

import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Null node (NullObject pattern).
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class NullNode implements StructNode {

  /** The underlying item. */
  private final ImmutableNode node;

  /**
   * Constructor.
   *
   * @param node the underlying node which is wrapped
   * @throws NullPointerException if {@code pNode} is {@code null}
   */
  public NullNode(final ImmutableNode node) {
    this.node = checkNotNull(node);
  }

  @Override
  public BigInteger computeHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setFirstChildKey(long firstChildKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setLastChildKey(long nodeKey) {
    throw new UnsupportedOperationException();
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
  public void setHash(final BigInteger hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigInteger getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNodeKey() {
    return node.getNodeKey();
  }

  @Override
  public long getParentKey() {
    return node.getParentKey();
  }

  @Override
  public boolean hasParent() {
    return node.hasParent();
  }

  @Override
  public NodeKind getKind() {
    // Node kind is always of type Kind.
    return node.getKind();
  }

  @Override
  public void setParentKey(long nodeKey) {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setTypeKey(int typeKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFirstChild() {
    return false;
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public boolean hasLeftSibling() {
    return false;
  }

  @Override
  public boolean hasRightSibling() {
    return false;
  }

  @Override
  public long getChildCount() {
    return 0;
  }

  @Override
  public long getLeftSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setRightSiblingKey(long nodeKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLeftSiblingKey(long nodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Get the underlying node. */
  public ImmutableNode getUnderlyingNode() {
    return node;
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
  public boolean isSameItem(final @Nullable Node other) {
    return node.isSameItem(other);
  }

  @Override
  public long getRevision() {
    return node.getRevision();
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof NullNode) {
      final NullNode other = (NullNode) obj;
      return Objects.equal(node, other.node);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(node);
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    throw new UnsupportedOperationException();
  }
}

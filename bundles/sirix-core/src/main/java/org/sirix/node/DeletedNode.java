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

package org.sirix.node;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * If a node is deleted, it will be encapsulated over this class.
 *
 * @author Sebastian Graf
 *
 */
public final class DeletedNode extends AbstractForwardingNode {

  /**
   * Delegate for common data.
   */
  private final NodeDelegate nodeDelegate;

  /**
   * Constructor.
   *
   * @param nodeDel node delegate
   */
  public DeletedNode(final NodeDelegate nodeDel) {
    nodeDelegate = checkNotNull(nodeDel);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.DELETE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDelegate);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof DeletedNode))
      return false;

    final DeletedNode other = (DeletedNode) obj;
    return Objects.equal(nodeDelegate, other.nodeDelegate);
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this).add("delegate", nodeDelegate.toString()).toString();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return nodeDelegate.isSameItem(other);
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }
}

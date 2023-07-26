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
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.StructNode;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;

/**
 * Skeletal implementation of {@link StructNode} interface.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractStructForwardingNode extends AbstractForwardingNode
    implements StructNode {

  /** Constructor for use by subclasses. */
  protected AbstractStructForwardingNode() {}

  /** 
   * {@link StructNodeDelegate} instance.
   * 
   * @return the struct node delegate
  */
  protected abstract StructNodeDelegate structDelegate();

  /**
   * Getting the struct node delegate.
   *
   * @return the struct node delegate
   */
  public StructNodeDelegate getStructNodeDelegate() {
    return structDelegate();
  }

  @Override
  public long getHash() {
    return structDelegate().getHash();
  }

  @Override
  public boolean hasFirstChild() {
    return structDelegate().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return structDelegate().hasLastChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return structDelegate().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return structDelegate().hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return structDelegate().getChildCount();
  }

  @Override
  public long getFirstChildKey() {
    return structDelegate().getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    return structDelegate().getLastChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return structDelegate().getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return structDelegate().getRightSiblingKey();
  }

  @Override
  public void setRightSiblingKey(final long key) {
    structDelegate().setRightSiblingKey(key);
  }

  @Override
  public void setLeftSiblingKey(final long key) {
    structDelegate().setLeftSiblingKey(key);
  }

  @Override
  public void setFirstChildKey(final long key) {
    structDelegate().setFirstChildKey(key);
  }

  @Override
  public void setLastChildKey(final long key) {
    structDelegate().setLastChildKey(key);
  }

  @Override
  public void decrementChildCount() {
    structDelegate().decrementChildCount();
  }

  @Override
  public void incrementChildCount() {
    structDelegate().incrementChildCount();
  }

  @Override
  public long getDescendantCount() {
    return structDelegate().getDescendantCount();
  }

  @Override
  public void decrementDescendantCount() {
    structDelegate().decrementDescendantCount();
  }

  @Override
  public void incrementDescendantCount() {
    structDelegate().incrementDescendantCount();
  }

  @Override
  public void setDescendantCount(final @NonNegative long descendantCount) {
    structDelegate().setDescendantCount(descendantCount);
  }

  @Override
  public void setPreviousRevision(int revision) {
    structDelegate().setPreviousRevision(revision);
  }

  @Override
  public int getPreviousRevisionNumber() {
    return structDelegate().getPreviousRevisionNumber();
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeDelegate", super.toString())
                      .add("structDelegate", structDelegate().toString())
                      .toString();
  }

}

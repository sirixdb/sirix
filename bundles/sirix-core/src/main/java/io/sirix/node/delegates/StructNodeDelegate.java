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

package io.sirix.node.delegates;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.settings.Fixed;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Delegate method for all nodes building up the structure. That means that all nodes representing
 * trees in Sirix are represented by an instance of the interface {@link StructNode} namely
 * containing the position of all related siblings, the first-child and all nodes defined by the
 * {@link NodeDelegate} as well.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public class StructNodeDelegate extends AbstractForwardingNode implements StructNode {

  /**
   * Pointer to the first child of the current node.
   */
  private long firstChild;

  /**
   * Pointer to the last child of the current node.
   */
  private long lastChild;

  /**
   * Pointer to the right sibling of the current node.
   */
  private long rightSibling;

  /**
   * Pointer to the left sibling of the current node.
   */
  private long leftSibling;

  /**
   * Number of children.
   */
  private long childCount;

  /**
   * Number of descendants.
   */
  private long descendantCount;

  /**
   * Delegate for common node information.
   */
  private final NodeDelegate nodeDelegate;

  /**
   * Constructor.
   *
   * @param nodeDelegate    {@link NodeDelegate} instance
   * @param firstChild      first child key
   * @param rightSibling    right sibling key
   * @param leftSibling     left sibling key
   * @param childCount      number of children of the node
   * @param descendantCount number of descendants of the node
   */
  public StructNodeDelegate(final NodeDelegate nodeDelegate, final long firstChild, final long rightSibling,
      final long leftSibling, final @NonNegative long childCount, final @NonNegative long descendantCount) {
    assert nodeDelegate != null : "del must not be null!";
    this.nodeDelegate = nodeDelegate;
    this.firstChild = firstChild;
    this.lastChild = Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty();
    this.rightSibling = rightSibling;
    this.leftSibling = leftSibling;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
  }

  /**
   * Constructor.
   *
   * @param nodeDelegate    {@link NodeDelegate} instance
   * @param firstChild      first child key
   * @param lastChild       last child key
   * @param rightSibling    right sibling key
   * @param leftSibling     left sibling key
   * @param childCount      number of children of the node
   * @param descendantCount number of descendants of the node
   */
  public StructNodeDelegate(final NodeDelegate nodeDelegate, final long firstChild, final long lastChild,
      final long rightSibling, final long leftSibling, final @NonNegative long childCount,
      final @NonNegative long descendantCount) {
    assert nodeDelegate != null : "del must not be null!";
    this.nodeDelegate = nodeDelegate;
    this.firstChild = firstChild;
    this.lastChild = lastChild;
    this.rightSibling = rightSibling;
    this.leftSibling = leftSibling;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
  }

  @Override
  public NodeKind getKind() {
    return nodeDelegate.getKind();
  }

  @Override
  public boolean hasFirstChild() {
    return firstChild != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return lastChild != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()
        && lastChild != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSibling != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return rightSibling != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    return childCount;
  }

  @Override
  public long getFirstChildKey() {
    return firstChild;
  }

  @Override
  public long getLastChildKey() {
    return lastChild;
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSibling;
  }

  @Override
  public long getRightSiblingKey() {
    return rightSibling;
  }

  @Override
  public void setRightSiblingKey(final long key) {
    rightSibling = key;
  }

  @Override
  public void setLeftSiblingKey(final long key) {
    leftSibling = key;
  }

  @Override
  public void setFirstChildKey(final long key) {
    firstChild = key;
  }

  @Override
  public void setLastChildKey(final long key) {
    if (key == Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      throw new UnsupportedOperationException();
    }
    lastChild = key;
  }

  @Override
  public void decrementChildCount() {
    childCount--;
  }

  @Override
  public void incrementChildCount() {
    childCount++;
  }

  @Override
  public int hashCode() {
    return lastChild == Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()
        ? Objects.hashCode(childCount,
                           nodeDelegate,
                           firstChild,
                           leftSibling,
                           rightSibling,
                           descendantCount)
        : Objects.hashCode(childCount,
                           nodeDelegate,
                           firstChild,
                           lastChild,
                           leftSibling,
                           rightSibling,
                           descendantCount);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final long hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final StructNodeDelegate other))
      return false;

    return Objects.equal(childCount, other.childCount) && Objects.equal(nodeDelegate, other.nodeDelegate)
        && Objects.equal(firstChild, other.firstChild) && Objects.equal(lastChild, other.lastChild) && Objects.equal(
        leftSibling,
        other.leftSibling) && Objects.equal(rightSibling, other.rightSibling) && Objects.equal(descendantCount,
                                                                                               other.descendantCount);
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("first child", getFirstChildKey())
                      .add("last child", getLastChildKey())
                      .add("left sib", getLeftSiblingKey())
                      .add("right sib", getRightSiblingKey())
                      .add("child count", getChildCount())
                      .add("descendant count", getDescendantCount())
                      .add("node delegate", getNodeDelegate().toString())
                      .toString();
  }

  @Override
  public long getDescendantCount() {
    return descendantCount;
  }

  @Override
  public void decrementDescendantCount() {
    descendantCount--;
  }

  @Override
  public void incrementDescendantCount() {
    descendantCount++;
  }

  @Override
  public void setDescendantCount(final @NonNegative long descendantCount) {
    this.descendantCount = descendantCount;
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return nodeDelegate.isSameItem(other);
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDelegate.getDeweyIDAsBytes();
  }
}

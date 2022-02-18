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

package org.sirix.diff;

import com.google.common.base.MoreObjects;
import org.sirix.diff.DiffFactory.DiffType;

import org.checkerframework.checker.index.qual.NonNegative;
import java.io.Serializable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Container for diffs.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class DiffTuple implements Serializable {
  /**
   * Serialization UID.
   */
  private static final long serialVersionUID = -8805161170968505227L;

  /** {@link DiffType} which specifies the kind of diff between two nodes. */
  private DiffType diff;

  /** Node key of node in new revision. */
  private final long newNodeKey;

  /** Node key of node in old revision. */
  private final long oldNodeKey;

  /** {@link DiffDepth} instance. */
  private final DiffDepth depth;

  /** Key of index in a Map (used for move-detection). */
  private int index;

  /**
   * Constructor.
   *
   * @param diff {@link DiffType} which specifies the kind of diff between two nodes
   * @param newNodeKey node key of node in new revision
   * @param oldNodeKey node key of node in old revision
   * @param depth current {@link DiffDepth} instance
   */
  public DiffTuple(final DiffType diff, final long newNodeKey, final long oldNodeKey,
      final DiffDepth depth) {
    checkArgument(newNodeKey >= 0);
    checkArgument(oldNodeKey >= 0);

    this.diff = checkNotNull(diff);
    this.newNodeKey = newNodeKey;
    this.oldNodeKey = oldNodeKey;
    this.depth = depth;
  }

  /**
   * Get diff.
   *
   * @return the kind of diff
   */
  public DiffType getDiff() {
    return diff;
  }

  /**
   * Set diff.
   *
   * @param diffType kind of diff
   */
  public DiffTuple setDiff(final DiffType diffType) {
    diff = checkNotNull(diffType);
    return this;
  }

  /**
   * Set index of node in {@link Map}, if a moved node is encountered.
   *
   * @param index index to set
   */
  public DiffTuple setIndex(final @NonNegative int index) {
    checkArgument(index >= 0);
    this.index = index;
    return this;
  }

  /**
   * Get new node key.
   *
   * @return the new node key
   */
  public long getNewNodeKey() {
    return newNodeKey;
  }

  /**
   * Get old node key.
   *
   * @return the old node key
   */
  public long getOldNodeKey() {
    return oldNodeKey;
  }

  /**
   * Get depth.
   *
   * @return the depth
   */
  public DiffDepth getDepth() {
    return depth;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("diff", diff)
                      .add("new nodeKey", newNodeKey)
                      .add("old nodeKey", oldNodeKey)
                      .toString();
  }

  /**
   * Get index.
   *
   * @return index
   */
  public int getIndex() {
    return index;
  }
}

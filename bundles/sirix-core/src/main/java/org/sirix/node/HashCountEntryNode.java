/**
 * Copyright (c) 2019.
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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.sirix.node.interfaces.Record;

/**
 * Hash entry node.
 *
 * @author Johannes Lichtenberger <johannes.lichtenberger@sirix.io>
 *
 */
public final class HashCountEntryNode implements Record {

  private final long mNodeKey;

  private int mValue;

  /**
   * Constructor.
   *
   * @param nodeKey the node key
   * @param value the String value
   */
  public HashCountEntryNode(final long nodeKey, final int value) {
    mNodeKey = nodeKey;
    mValue = value;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.HASH_NAME_COUNT_TO_NAME_ENTRY;
  }

  public int getValue() {
    return mValue;
  }

  public HashCountEntryNode incrementValue() {
    mValue++;
    return this;
  }

  public HashCountEntryNode decrementValue() {
    mValue--;
    return this;
  }

  @Override
  public int hashCode() {
    return Integer.valueOf(mValue).hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof HashCountEntryNode))
      return false;

    final HashCountEntryNode other = (HashCountEntryNode) obj;
    return Objects.equal(mValue, other.mValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("value", mValue).toString();
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public long getRevision() {
    throw new UnsupportedOperationException();
  }
}

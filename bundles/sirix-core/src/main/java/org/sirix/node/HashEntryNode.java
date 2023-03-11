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
import org.sirix.node.interfaces.DataRecord;

import static java.util.Objects.requireNonNull;

/**
 * Hash entry node.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class HashEntryNode implements DataRecord {

  private final long nodeKey;

  private final int key;

  private final String value;

  /**
   * Constructor.
   *
   * @param nodeKey the node key
   * @param key the integer hash code
   * @param value the String value
   */
  public HashEntryNode(final long nodeKey, final int key, final String value) {
    this.nodeKey = nodeKey;
    this.key = key;
    this.value = requireNonNull(value);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.HASH_ENTRY;
  }

  public int getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof HashEntryNode))
      return false;

    final HashEntryNode other = (HashEntryNode) obj;
    return Objects.equal(key, other.key) && Objects.equal(value, other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key).add("value", value).toString();
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }
}

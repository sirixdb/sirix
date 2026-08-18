/**
 * Copyright (c) 2026.
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

package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * One value of a global projection value dictionary: the reverse (id &rarr; value) direction.
 *
 * <p>The record's own node key <em>is</em> the value's dictionary id, offset by the namespace base
 * — so materialising the value behind an id is a single record read and needs no auxiliary index.
 * That is the whole reason the dictionary lives in a record trie rather than in a blob: a
 * high-cardinality column has millions of entries, and the only affordable reverse lookup is one
 * that reads the page holding the wanted id and nothing else.
 *
 * <p>Immutable once written, for the same reason an FSST symbol table is
 * ({@link FsstSymbolTableNode}): row cells in every already-written row group refer to values by
 * id, so re-pointing an id at a different value would silently rewrite the meaning of data in
 * revisions that have already been committed. Ids are therefore minted monotonically and never
 * reused, and copy-on-write keeps each revision's view of the dictionary reachable from that
 * revision's root.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryEntryNode implements DataRecord {

  private final long nodeKey;

  /** The dictionary value, UTF-8 encoded. Empty is a legitimate value (the empty string). */
  private final byte[] value;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is the namespace base plus the value's dictionary id
   * @param value the UTF-8 encoded value; never {@code null}, possibly empty
   */
  public ValueDictionaryEntryNode(final long nodeKey, final byte[] value) {
    this.nodeKey = nodeKey;
    this.value = requireNonNull(value, "value must not be null");
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_ENTRY;
  }

  /**
   * The UTF-8 encoded value.
   *
   * <p>Returned directly rather than copied: this sits on the reverse-mapping path and the array is
   * treated as immutable throughout. Callers must not modify it.
   *
   * @return the UTF-8 encoded value
   */
  public byte[] getValue() {
    return value;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryEntryNode other && Arrays.equals(value, other.value);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("nodeKey", nodeKey).add("valueBytes", value.length).toString();
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

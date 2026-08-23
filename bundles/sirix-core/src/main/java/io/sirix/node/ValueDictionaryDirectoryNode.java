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
 * One block of a global projection value dictionary's forward (value &rarr; id) directory: a run of
 * {@code (valueHash, id)} pairs sorted by {@code valueHash}, with {@code id} breaking ties.
 *
 * <h2>Why a sorted block directory rather than a hash table</h2>
 *
 * A record trie is keyed by a {@code long} record key, so it can answer "what is at id {@code i}"
 * directly but has no way at all to answer "which id holds these bytes" — the forward direction
 * needs a structure built on top. An open-addressed bucket table would need its capacity fixed
 * before the first insert and a full rewrite whenever the load factor is exceeded; a sorted
 * directory needs neither, because it is produced in one pass at the end of a build when the entry
 * count is already known exactly.
 *
 * <p>
 * It is also far cheaper to write: at {@link #ENTRIES_PER_BLOCK} pairs per record, a
 * five-million-entry dictionary costs some tens of thousands of records instead of five million,
 * and a probe costs a binary search over blocks ({@code log2(blockCount)} record reads) plus a
 * binary search inside one — paid once per literal, never per row.
 *
 * <p>
 * A hash match is <em>not</em> proof of equality. The probe must confirm the candidate by reading
 * the {@link ValueDictionaryEntryNode} the id names and comparing the bytes, which is why ties on
 * {@code valueHash} are kept adjacent and are all visited.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryDirectoryNode implements DataRecord {

  /**
   * Pairs per block. Each pair costs 12 bytes on the wire, so a full block is 1.5 KiB — small enough
   * that a thousand of them still fit inside a record page's slotted-buffer ceiling, and large enough
   * that the binary search over blocks stays short.
   */
  public static final int ENTRIES_PER_BLOCK = 128;

  private final long nodeKey;

  /** Value hashes, ascending. Parallel to {@link #ids}. */
  private final long[] hashes;

  /** The dictionary id each hash belongs to. Parallel to {@link #hashes}. */
  private final int[] ids;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is the namespace's directory base plus the block index
   * @param hashes the block's value hashes, ascending; never {@code null} and never empty
   * @param ids the dictionary ids, parallel to {@code hashes}
   * @throws IllegalArgumentException if the arrays differ in length, or the block is empty
   */
  public ValueDictionaryDirectoryNode(final long nodeKey, final long[] hashes, final int[] ids) {
    this.nodeKey = nodeKey;
    this.hashes = requireNonNull(hashes, "hashes must not be null");
    this.ids = requireNonNull(ids, "ids must not be null");
    if (hashes.length != ids.length) {
      throw new IllegalArgumentException(
          "hashes and ids must be parallel; got " + hashes.length + " and " + ids.length);
    }
    if (hashes.length == 0) {
      throw new IllegalArgumentException("refusing to store an empty directory block");
    }
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_DIRECTORY;
  }

  /** The block's value hashes, ascending. Not copied; callers must not modify. */
  public long[] getHashes() {
    return hashes;
  }

  /** The dictionary ids, parallel to {@link #getHashes()}. Not copied; callers must not modify. */
  public int[] getIds() {
    return ids;
  }

  /** How many pairs this block holds. */
  public int size() {
    return hashes.length;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(hashes) + Arrays.hashCode(ids);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryDirectoryNode other && Arrays.equals(hashes, other.hashes)
        && Arrays.equals(ids, other.ids);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("nodeKey", nodeKey).add("entries", hashes.length).toString();
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

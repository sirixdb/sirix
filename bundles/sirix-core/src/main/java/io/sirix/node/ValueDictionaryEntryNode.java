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
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * One value of a global projection value dictionary: the reverse (id &rarr; value) direction.
 *
 * <p>The reverse-id radix maps a stable dictionary id to this record's opaque persistence key.
 * Append generations can therefore reserve dense key runs without renumbering any existing id.
 * Keeping each immutable value as its own record lets a lookup read only the radix path, its small
 * value bucket, and the page holding the requested value rather than materialising the dictionary.
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

  /**
   * Largest value payload admitted by V0.  The minimum G1 region is 1 MiB and an object becomes
   * humongous at half a region; a 256 KiB payload plus its array header therefore retains almost
   * another 256 KiB of safety margin even under that smallest region configuration.
   */
  public static final int MAX_VALUE_LENGTH = 256 << 10;

  private final long nodeKey;

  /** The dictionary value, UTF-8 encoded. Empty is a legitimate value (the empty string). */
  private final byte[] value;

  /**
   * Constructor.
   *
   * @param nodeKey the positive persistence key reserved for this entry
   * @param value the UTF-8 encoded value; never {@code null}, possibly empty
   */
  public ValueDictionaryEntryNode(final long nodeKey, final byte[] value) {
    this(nodeKey, value, false);
  }

  private ValueDictionaryEntryNode(final long nodeKey, final byte[] value,
      final boolean takeOwnership) {
    if (nodeKey <= 0L) {
      throw new IllegalArgumentException("value dictionary entry key must be positive");
    }
    this.nodeKey = nodeKey;
    final byte[] checkedValue = requireNonNull(value, "value must not be null");
    if (checkedValue.length > MAX_VALUE_LENGTH) {
      throw new IllegalArgumentException("value dictionary entry exceeds the safe V0 payload limit of "
          + MAX_VALUE_LENGTH + " bytes");
    }
    this.value = takeOwnership ? checkedValue : checkedValue.clone();
  }

  /**
   * Create an entry by transferring ownership of {@code value} to the immutable node.
   *
   * <p>This is the allocation-free persistence seam for dictionary writers and deserializers that
   * have just produced a fresh byte array. The caller must neither retain nor mutate the array after
   * this call. Ordinary callers should use {@link #ValueDictionaryEntryNode(long, byte[])}, which
   * defensively copies its input.
   */
  public static ValueDictionaryEntryNode takeOwnership(final long nodeKey, final byte[] value) {
    return new ValueDictionaryEntryNode(nodeKey, value, true);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_ENTRY;
  }

  /**
   * The UTF-8 encoded value.
   *
   * @return a copy of the UTF-8 encoded value
   */
  public byte[] getValue() {
    return value.clone();
  }

  /** Compare a caller-owned byte range without exposing or copying the stored bytes. */
  public boolean valueEquals(final byte[] candidate, final int offset, final int length) {
    requireNonNull(candidate, "candidate must not be null");
    Objects.checkFromIndexSize(offset, length, candidate.length);
    return Arrays.equals(value, 0, value.length, candidate, offset, offset + length);
  }

  /** Compare a caller-owned byte range to the stored bytes using unsigned byte ordering. */
  public int compareCandidateUnsigned(final byte[] candidate, final int offset,
      final int length) {
    requireNonNull(candidate, "candidate must not be null");
    Objects.checkFromIndexSize(offset, length, candidate.length);
    final int commonLength = Math.min(length, value.length);
    for (int index = 0; index < commonLength; index++) {
      final int comparison = Integer.compare(Byte.toUnsignedInt(candidate[offset + index]),
          Byte.toUnsignedInt(value[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(length, value.length);
  }

  /** Number of UTF-8 bytes retained by this entry. */
  public int getValueLength() {
    return value.length;
  }

  /** Package-private, allocation-free serialization seam for the owning node package. */
  byte[] borrowedValueForSerialization() {
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

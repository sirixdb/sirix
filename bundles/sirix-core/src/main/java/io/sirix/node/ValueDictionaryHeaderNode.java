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

/**
 * The header of one global projection value dictionary namespace.
 *
 * <p>
 * It sits at local key 0 of the namespace, so a reader that knows only the namespace can find
 * everything else with one read. Everything it carries is derived state that a reader cannot
 * reconstruct without scanning the whole namespace, which is exactly what a header is for.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryHeaderNode implements DataRecord {

  /** Layout version of the namespace; readers reject anything they do not know. */
  public static final int VERSION = 0;

  private final long nodeKey;

  private final int version;

  /** Ids {@code 1..entryCount} are live; {@code entryCount + 1} is the next id to mint. */
  private final int entryCount;

  private final long forwardRootKey;

  private final long reverseRootKey;

  private final int generation;

  /**
   * Ids {@code 1..orderedPrefixCount} are in UTF-16 collation order of their VALUES; ids above it are
   * in append (first-intern) order.
   *
   * <p>
   * Zero for every dictionary the streaming mint built, which is semantically correct rather than
   * merely safe: those ids are in intern order and their ordered prefix is genuinely empty. It is
   * never decreased by an append — an append raises {@link #entryCount} only — and is set to
   * {@code entryCount} exactly once, by the rank pass, in the transaction that wrote the ranked run.
   * Every reader that needs ORDER must test {@code orderedPrefixCount == entryCount}, never
   * {@code > 0}: a single maintenance append leaves a sorted prefix with an unsorted tail, and an arm
   * that only checked for non-zero would emit that tail in the wrong place.
   * </p>
   */
  private final int orderedPrefixCount;

  /**
   * Record key of the {@link ValueDictionaryBlockIndexNode} over the ordered prefix, or 0 when there
   * is none. Purely an accelerator: a probe without it is slower, never wrong.
   */
  private final long blockIndexKey;

  /** {@code false} for an {@link #unknownLayout(long, int)} carrier this build cannot interpret. */
  private final boolean currentLayout;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is the namespace base (local key 0)
   * @param version the layout version
   * @param entryCount how many values the namespace holds
   * @param forwardRootKey root of the hash-prefix radix directory
   * @param reverseRootKey root of the id-prefix radix directory
   * @param generation number of successful append generations
   * @throws IllegalArgumentException if any count is negative
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long forwardRootKey, final long reverseRootKey, final int generation) {
    this(nodeKey, version, entryCount, forwardRootKey, reverseRootKey, generation, 0);
  }

  /**
   * Constructor carrying the ordered-prefix boundary.
   *
   * @param orderedPrefixCount how many ids from 1 are in collation order of their values
   * @throws IllegalArgumentException if any count is negative, if the boundary exceeds
   *         {@code entryCount}, or if a live dictionary has no reverse root
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long forwardRootKey, final long reverseRootKey, final int generation, final int orderedPrefixCount) {
    this(nodeKey, version, entryCount, forwardRootKey, reverseRootKey, generation, orderedPrefixCount, 0L);
  }

  /**
   * Constructor carrying the block index key.
   *
   * @param blockIndexKey record key of the separator array, or 0
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long forwardRootKey, final long reverseRootKey, final int generation, final int orderedPrefixCount,
      final long blockIndexKey) {
    if (nodeKey <= 0 || version != VERSION || entryCount < 0 || forwardRootKey < 0 || reverseRootKey < 0
        || generation < 0 || orderedPrefixCount < 0 || orderedPrefixCount > entryCount || blockIndexKey < 0) {
      throw new IllegalArgumentException("invalid value dictionary header");
    }
    // The reverse root is what makes a dictionary readable at all, so it keeps the old biconditional.
    if ((entryCount == 0) != (reverseRootKey == 0)) {
      throw new IllegalArgumentException("invalid value dictionary header");
    }
    // RELAXED for the rank pass (design §3.3.2): a FULLY ordered dictionary needs no forward hash
    // index, because "which id holds this value" is a binary search over a reverse index that is
    // already sorted by value. A zero forward root is therefore legal exactly when the whole
    // dictionary is ordered; anywhere else it means a directory that cannot be probed at all.
    if (forwardRootKey == 0 && entryCount != 0 && orderedPrefixCount != entryCount) {
      throw new IllegalArgumentException("value dictionary header has no forward index but only " + orderedPrefixCount
          + " of " + entryCount + " ids are ordered");
    }
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = entryCount;
    this.forwardRootKey = forwardRootKey;
    this.reverseRootKey = reverseRootKey;
    this.generation = generation;
    this.orderedPrefixCount = orderedPrefixCount;
    this.blockIndexKey = blockIndexKey;
    this.currentLayout = true;
  }

  private ValueDictionaryHeaderNode(final long nodeKey, final int version) {
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = 0;
    this.forwardRootKey = 0;
    this.reverseRootKey = 0;
    this.generation = 0;
    this.orderedPrefixCount = 0;
    this.blockIndexKey = 0L;
    this.currentLayout = false;
  }

  /**
   * A header whose serialized layout version this build cannot interpret. Only the version is carried
   * — the payload behind it is unreadable by definition. Every consumer declines it
   * ({@code GlobalValueDictionary#header} answers {@code null}), and re-serializing it is refused so
   * a newer build's data is never overwritten with a lossy reconstruction.
   *
   * @throws IllegalArgumentException for a negative version — that is corruption, not a future
   *         layout, and corruption stays loud
   */
  public static ValueDictionaryHeaderNode unknownLayout(final long nodeKey, final int version) {
    if (nodeKey <= 0 || version < 0 || version == VERSION) {
      throw new IllegalArgumentException("not an unknown-layout value dictionary header: version " + version);
    }
    return new ValueDictionaryHeaderNode(nodeKey, version);
  }

  /**
   * Whether this build can interpret the header's layout ({@link #getVersion()} == {@link #VERSION}).
   */
  public boolean isCurrentLayout() {
    return currentLayout;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_HEADER;
  }

  public int getVersion() {
    return version;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public long getForwardRootKey() {
    return forwardRootKey;
  }

  public long getReverseRootKey() {
    return reverseRootKey;
  }

  public int getGeneration() {
    return generation;
  }

  /** Record key of the block separator array, or 0 when the dictionary carries none. */
  public long getBlockIndexKey() {
    return blockIndexKey;
  }

  /** Ids {@code 1..this} are in collation order of their values; see the field's contract. */
  public int getOrderedPrefixCount() {
    return orderedPrefixCount;
  }

  /**
   * Whether every live id is in collation order — the ONE test an ordering arm may make.
   *
   * <p>
   * Deliberately not {@code getOrderedPrefixCount() > 0}: after a single maintenance append the
   * prefix is still sorted but the dictionary is not, and an arm that took a non-empty prefix as
   * permission to compare ids would place the appended tail wrong.
   * </p>
   */
  public boolean isFullyOrdered() {
    return orderedPrefixCount == entryCount;
  }

  /** Whether a probe may report "absent" rather than declining. */
  public boolean isDirectoryComplete() {
    if (entryCount == 0) {
      return forwardRootKey == 0 && reverseRootKey == 0;
    }
    // A fully ordered dictionary is probed by binary search over the reverse index, so it is
    // complete without a forward root; a partly ordered one needs the forward index for its tail.
    return reverseRootKey > 0 && (forwardRootKey > 0 || isFullyOrdered());
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    int result = version;
    result = 31 * result + entryCount;
    result = 31 * result + Long.hashCode(forwardRootKey);
    result = 31 * result + Long.hashCode(reverseRootKey);
    result = 31 * result + generation;
    result = 31 * result + orderedPrefixCount;
    return 31 * result + Long.hashCode(blockIndexKey);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryHeaderNode other && version == other.version && entryCount == other.entryCount
        && forwardRootKey == other.forwardRootKey && reverseRootKey == other.reverseRootKey
        && generation == other.generation && orderedPrefixCount == other.orderedPrefixCount
        && blockIndexKey == other.blockIndexKey;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("version", version)
                         .add("entryCount", entryCount)
                         .add("forwardRootKey", forwardRootKey)
                         .add("reverseRootKey", reverseRootKey)
                         .add("generation", generation)
                         .toString();
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

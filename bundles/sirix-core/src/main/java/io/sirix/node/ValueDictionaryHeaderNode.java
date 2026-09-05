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

  /**
   * Byte length of the ordering trailer's PAIR shape {@code (orderedPrefixCount, blockIndexKey)} --
   * what every header before the rank table carried; the 100M artefact holds only pairs.
   */
  public static final int PAIR_TRAILER_BYTES = Integer.BYTES + Long.BYTES;

  /** Byte length of the TRIPLE shape, the pair followed by {@code rankTableKey}. */
  public static final int TRIPLE_TRAILER_BYTES = PAIR_TRAILER_BYTES + Long.BYTES;

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

  /**
   * Record key of the first {@link ValueDictionaryRankTableNode} of this generation's
   * {@code mint -> rank} table, or 0 when ids ARE storage positions.
   *
   * <p>
   * Non-zero only for a dictionary sealed from ids that were minted in arrival order and then
   * stored in collation order (the segment lane's seal): the ids in the pages stay what they were,
   * and the table says where each one's value went. Under a table {@link #isFullyOrdered()} still
   * means the STORAGE is ordered (binary-search probe legal, no forward index needed), but id order
   * is no longer value order — an arm that wants to compare ids as strings must ask
   * {@link #idsAreCollationOrdered()}. The forward run's records live at {@code rankTableKey + i} for
   * the {@code i}-th run of {@link ValueDictionaryRankTableNode#ENTRIES_PER_RECORD} mints, the inverse
   * run ({@code rank -> mint}, for the probe) directly behind it at
   * {@code rankTableKey + recordCountFor(orderedPrefixCount) + i}; mints above
   * {@link #orderedPrefixCount} (an appended tail) are their own position.
   * </p>
   */
  private final long rankTableKey;

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
    this(nodeKey, version, entryCount, forwardRootKey, reverseRootKey, generation, orderedPrefixCount, blockIndexKey,
        0L);
  }

  /**
   * Constructor carrying the rank table key.
   *
   * @param rankTableKey record key of the first rank table record (the {@code mint -> rank} run, the
   *        inverse run behind it), or 0 when ids are storage positions
   * @throws IllegalArgumentException as the other constructors, and if a rank table is claimed for a
   *         dictionary with no ordered prefix or with a forward index (whose answers would be
   *         positions, not ids)
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long forwardRootKey, final long reverseRootKey, final int generation, final int orderedPrefixCount,
      final long blockIndexKey, final long rankTableKey) {
    if (nodeKey <= 0 || version != VERSION || entryCount < 0 || forwardRootKey < 0 || reverseRootKey < 0
        || generation < 0 || orderedPrefixCount < 0 || orderedPrefixCount > entryCount || blockIndexKey < 0
        || rankTableKey < 0) {
      throw new IllegalArgumentException("invalid value dictionary header");
    }
    // A rank table translates ids in 1..orderedPrefixCount, so it needs a prefix to translate; and a
    // forward index answers a probe with a STORAGE position, which under a table is not the id the
    // pages carry — the two must never coexist.
    if (rankTableKey != 0L && (orderedPrefixCount == 0 || forwardRootKey != 0L)) {
      throw new IllegalArgumentException("invalid value dictionary header: a rank table needs an ordered prefix ("
          + orderedPrefixCount + ") and excludes a forward index (" + forwardRootKey + ")");
    }
    // The reverse root is what makes a dictionary readable at all, so it keeps the old biconditional.
    if ((entryCount == 0) != (reverseRootKey == 0)) {
      throw new IllegalArgumentException("invalid value dictionary header");
    }
    // A zero forward root used to be legal ONLY for a fully ordered dictionary (the rank pass,
    // design §3.3.2), where "which id holds this value" is a binary search over a reverse index that
    // is already sorted by value. It is now also legal for a DECODE-ONLY dictionary, which answers
    // id -> value and refuses value -> id.
    //
    // That distinction has to exist because the forward index is what makes an INCREMENTAL
    // dictionary unaffordable: every bounded append writes a fresh set of radix nodes at new keys
    // and copy-on-write retains all of them (64.7 B/entry at D = 275K, 173 B/entry at D = 2.62M --
    // GlobalValueDictionaryRadix.append), so a dictionary sealed per segment pays it again per
    // segment while nothing ever probes it (SegmentScopedReadDictionaries.idOf returns ID_ABSENT).
    //
    // What is NOT relaxed: the reverse root. It is what turns an id back into bytes, the
    // biconditional above still demands it, and it is the positive witness that this header
    // describes a readable dictionary rather than a truncated one. A caller that needs the encode
    // direction must ask supportsValueProbe(); "no forward index" is an answer to that question, not
    // a claim that the directory is incomplete.
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = entryCount;
    this.forwardRootKey = forwardRootKey;
    this.reverseRootKey = reverseRootKey;
    this.generation = generation;
    this.orderedPrefixCount = orderedPrefixCount;
    this.blockIndexKey = blockIndexKey;
    this.rankTableKey = rankTableKey;
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
    this.rankTableKey = 0L;
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
   * Record key of the first rank table record, or 0 when ids are storage positions. The table is two
   * runs of {@link ValueDictionaryRankTableNode#recordCountFor} records at consecutive keys:
   * {@code mint -> rank} first, {@code rank -> mint} behind it.
   */
  public long getRankTableKey() {
    return rankTableKey;
  }

  /** Whether ids must be translated to storage positions before any value is addressed. */
  public boolean hasRankTable() {
    return rankTableKey != 0L;
  }

  /**
   * Whether comparing two ids as integers compares their values under UTF-16 collation: the storage
   * is fully ordered AND ids are storage positions. The test an ordering arm must make instead of
   * {@link #isFullyOrdered()}, which under a rank table is true of the storage but not of the ids.
   */
  public boolean idsAreCollationOrdered() {
    return isFullyOrdered() && rankTableKey == 0L;
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
    // DECODE completeness, which is what every serving path needs: the reverse index is the one that
    // turns an id back into bytes. Whether the dictionary can also be probed BY VALUE is a separate
    // question with its own predicate, because the two have different answers for a decode-only
    // dictionary and a caller that conflated them would either reject a readable dictionary or
    // accept one it cannot intern into.
    return reverseRootKey > 0;
  }

  /**
   * Whether "which id holds this value" can be answered, which every ENCODE-direction caller needs:
   * interning into this dictionary, binding it as a write-side resolver, or appending a generation
   * that must not mint a duplicate id for a value already present.
   *
   * @return {@code true} when a forward hash index exists or binary search over the reverse index
   *         serves instead, {@code false} for a decode-only dictionary
   */
  public boolean supportsValueProbe() {
    return entryCount == 0 || forwardRootKey > 0 || isFullyOrdered();
  }

  /**
   * Whether this dictionary answers only {@code id -> value}. The shape is unforgeable by older
   * writers: before the decode-only mode existed the header REFUSED a missing forward index on a
   * dictionary that was not fully ordered, so no database can contain this combination by accident.
   *
   * @return {@code true} when there is no forward index and the ids are not fully ordered
   */
  public boolean isDecodeOnly() {
    return entryCount != 0 && forwardRootKey == 0 && !isFullyOrdered();
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
    result = 31 * result + Long.hashCode(blockIndexKey);
    return 31 * result + Long.hashCode(rankTableKey);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryHeaderNode other && version == other.version && entryCount == other.entryCount
        && forwardRootKey == other.forwardRootKey && reverseRootKey == other.reverseRootKey
        && generation == other.generation && orderedPrefixCount == other.orderedPrefixCount
        && blockIndexKey == other.blockIndexKey && rankTableKey == other.rankTableKey;
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
                         .add("orderedPrefixCount", orderedPrefixCount)
                         .add("blockIndexKey", blockIndexKey)
                         .add("rankTableKey", rankTableKey)
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

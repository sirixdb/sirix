package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.SirixDeweyID;
import io.sirix.utils.ToStringHelper;

/**
 * The separator array over a rank-ordered dictionary's value blocks — the sparse index that makes a
 * binary-search probe cost ONE block decode instead of one per step.
 *
 * <p>
 * Dropping the forward hash index over an ordered prefix (§3.3.2) turns "which id holds this value"
 * into a binary search over the reverse index. Measured without this array that search costs
 * <b>26x</b> the hash probe, and the reason is not tree depth: a search over 275k values touches
 * ~18 DIFFERENT blocks and every block is a front-coded LZ77 frame that has to be decoded WHOLE
 * before a single value inside it can be compared. The hash probe decodes exactly one. Both are
 * decode-bound; the search simply decodes eighteen times more.
 * </p>
 *
 * <p>
 * This is the standard answer and the design's omission rather than an invention: an SSTable index
 * block, a B-tree's separator keys and a Parquet page index are all the same structure. Each entry
 * holds the SHORTEST PREFIX of its block's first value that still separates it from the previous
 * block's last value, so the array stays small enough to be read once and kept — whole values would
 * be ~13 MB for URL at 100M where prefixes are a couple of MB.
 * </p>
 *
 * <p>
 * <b>The separator contract</b>, which is what makes a search over truncated keys exact:
 * {@code previousBlockLastValue < separator[i] <= firstValueOf(block i)} under the engine's UTF-16
 * collation. A needle therefore belongs to block {@code i} exactly when {@code i} is the LARGEST
 * index whose separator does not exceed it — there is no ambiguity to resolve by probing
 * neighbours, and no case where a truncated separator makes a present value look absent. Entry 0's
 * separator is empty, so every value is at or after it.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryBlockIndexNode implements DataRecord {

  /** A dictionary can hold {@code Integer.MAX_VALUE} ids over blocks of at least one value. */
  public static final int MAX_ENTRIES = 1 << 24;

  private final long nodeKey;

  /** First id of each indexed block, strictly ascending. */
  private final int[] firstIds;

  /** Separator bytes, concatenated; entry {@code i} is {@code [offsets[i], offsets[i + 1])}. */
  private final byte[] separators;

  private final int[] offsets;

  private ValueDictionaryBlockIndexNode(final long nodeKey, final int[] firstIds, final byte[] separators,
      final int[] offsets) {
    this.nodeKey = nodeKey;
    this.firstIds = firstIds;
    this.separators = separators;
    this.offsets = offsets;
  }

  /**
   * Takes ownership of arrays the caller must not touch again.
   *
   * @param nodeKey the record key
   * @param firstIds first id of each block, strictly ascending, {@code firstIds[0] == 1}
   * @param separators concatenated separator bytes
   * @param offsets {@code firstIds.length + 1} prefix offsets into {@code separators}
   */
  public static ValueDictionaryBlockIndexNode takeOwnership(final long nodeKey, final int[] firstIds,
      final byte[] separators, final int[] offsets) {
    if (nodeKey <= 0) {
      throw new IllegalArgumentException("invalid value dictionary block index key " + nodeKey);
    }
    if (firstIds == null || separators == null || offsets == null) {
      throw new NullPointerException("block index arrays must not be null");
    }
    if (firstIds.length == 0 || firstIds.length > MAX_ENTRIES) {
      throw new IllegalArgumentException("value dictionary block index holds " + firstIds.length + " entries");
    }
    if (offsets.length != firstIds.length + 1 || offsets[0] != 0 || offsets[firstIds.length] != separators.length) {
      throw new IllegalArgumentException("value dictionary block index offsets do not frame its separators");
    }
    if (firstIds[0] != 1) {
      throw new IllegalArgumentException("the first indexed block must start at id 1, not " + firstIds[0]);
    }
    if (offsets[1] != 0) {
      throw new IllegalArgumentException("the first separator must be empty so every value is at or after it");
    }
    for (int i = 1; i < firstIds.length; i++) {
      if (firstIds[i] <= firstIds[i - 1]) {
        throw new IllegalArgumentException("block first ids must ascend strictly, at entry " + i);
      }
      if (offsets[i + 1] < offsets[i]) {
        throw new IllegalArgumentException("block index offsets must not descend, at entry " + i);
      }
    }
    return new ValueDictionaryBlockIndexNode(nodeKey, firstIds, separators, offsets);
  }

  /** How many blocks are indexed. */
  public int size() {
    return firstIds.length;
  }

  /** First id of block {@code index}. */
  public int firstId(final int index) {
    return firstIds[index];
  }

  /** RAW separator bytes; sliced with {@link #separatorOffset} and {@link #separatorLength}. */
  public byte[] separatorBytes() {
    return separators;
  }

  public int separatorOffset(final int index) {
    return offsets[index];
  }

  public int separatorLength(final int index) {
    return offsets[index + 1] - offsets[index];
  }

  /**
   * The block that would hold {@code utf8} — the largest entry whose separator does not exceed it.
   *
   * @return an index into this array, never negative because entry 0's separator is empty
   */
  public int blockOf(final byte[] utf8, final int offset, final int length) {
    int low = 0;
    int high = firstIds.length - 1;
    while (low < high) {
      final int mid = (low + high + 1) >>> 1;
      final int comparison = ValueDictionaryEntryNode.compareUtf16Range(separators, offsets[mid],
          offsets[mid + 1] - offsets[mid], utf8, offset, length);
      if (comparison <= 0) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_BLOCK_INDEX;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return 0;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return 0;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("blocks", firstIds.length)
                         .add("separatorBytes", separators.length)
                         .toString();
  }
}

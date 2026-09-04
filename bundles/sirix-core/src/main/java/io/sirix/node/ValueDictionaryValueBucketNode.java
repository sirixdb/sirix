/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;

/**
 * Directory of one reverse value bucket: which packed sub-blocks and which spilled records cover
 * its 256-id range.
 *
 * <h2>Sparse, not a per-id lane</h2>
 *
 * The bucket does NOT keep a key per id. A dense {@code long[256]} would cost ~2 KiB per bucket to
 * describe a partition that is almost always a handful of runs, and it would leave a second
 * representation of the same fact. Instead it stores the runs directly:
 *
 * <pre>
 *   blockFirstIds[i] / blockCounts[i] / blockKeys[i]   consecutive ids packed in one sub-block
 *   spillIds[j]      / spillKeys[j]                    one id with its own entry record
 * </pre>
 *
 * <h2>Exact partition</h2>
 *
 * Blocks and spills must together cover {@code [firstId, firstId + count)} EXACTLY: no gap, no
 * overlap, nothing outside. That invariant is checked here rather than assumed, because a gap would
 * make an id silently unresolvable and an overlap would make its value ambiguous — both of which a
 * dictionary must never express.
 *
 * <p>
 * A spill exists only for an individually oversized value (longer than a sub-block's byte target).
 * Ordinary values never spill, whatever their position in the bucket.
 */
public final class ValueDictionaryValueBucketNode implements DataRecord {

  public static final int VALUES_PER_BUCKET = 256;

  private static final int[] NO_INTS = {};
  private static final long[] NO_LONGS = {};

  private final long nodeKey;
  private final int firstId;
  private final int count;
  private final int[] blockFirstIds;
  private final int[] blockCounts;
  private final long[] blockKeys;
  private final int[] spillIds;
  private final long[] spillKeys;

  /**
   * @param nodeKey this record's key
   * @param firstId first dictionary id this bucket covers
   * @param count ids covered, {@code 1 .. VALUES_PER_BUCKET}
   * @param blockFirstIds ascending first id of each packed run
   * @param blockCounts ids in each packed run, index-aligned to {@code blockFirstIds}
   * @param blockKeys record key of each packed run
   * @param spillIds ascending ids that carry their own entry record
   * @param spillKeys those records' keys, index-aligned to {@code spillIds}
   */
  public ValueDictionaryValueBucketNode(final long nodeKey, final int firstId, final int count,
      final int[] blockFirstIds, final int[] blockCounts, final long[] blockKeys, final int[] spillIds,
      final long[] spillKeys) {
    this(nodeKey, firstId, count, blockFirstIds, blockCounts, blockKeys, spillIds, spillKeys, true);
  }

  /**
   * ADOPTS every array — the caller must not retain or mutate them.
   *
   * <p>
   * For the codec and the writer, which build these arrays for this record and drop them immediately.
   * A public constructor that silently adopted would be an ambiguous contract, so the copying
   * constructor stays the default and taking ownership is an explicit, named choice.
   */
  public static ValueDictionaryValueBucketNode takeOwnership(final long nodeKey, final int firstId, final int count,
      final int[] blockFirstIds, final int[] blockCounts, final long[] blockKeys, final int[] spillIds,
      final long[] spillKeys) {
    return new ValueDictionaryValueBucketNode(nodeKey, firstId, count, blockFirstIds, blockCounts, blockKeys, spillIds,
        spillKeys, false);
  }

  private ValueDictionaryValueBucketNode(final long nodeKey, final int firstId, final int count,
      final int[] blockFirstIds, final int[] blockCounts, final long[] blockKeys, final int[] spillIds,
      final long[] spillKeys, final boolean copy) {
    if (nodeKey <= 0 || firstId <= 0 || ((firstId - 1) & (VALUES_PER_BUCKET - 1)) != 0 || count <= 0
        || count > VALUES_PER_BUCKET) {
      throw new IllegalArgumentException("invalid value dictionary value bucket range");
    }
    if (blockFirstIds == null || blockCounts == null || blockKeys == null || spillIds == null || spillKeys == null
        || blockFirstIds.length != blockCounts.length || blockFirstIds.length != blockKeys.length
        || spillIds.length != spillKeys.length) {
      throw new IllegalArgumentException("value dictionary bucket directory arrays disagree");
    }
    // Walk blocks and spills together in id order and require them to tile the range exactly.
    long expected = firstId;
    int b = 0;
    int s = 0;
    while (b < blockFirstIds.length || s < spillIds.length) {
      final boolean takeBlock = s == spillIds.length || (b < blockFirstIds.length && blockFirstIds[b] <= spillIds[s]);
      if (takeBlock) {
        if (blockKeys[b] <= 0 || blockCounts[b] <= 0 || blockFirstIds[b] != expected) {
          throw new IllegalArgumentException(
              "value dictionary sub-block does not continue the bucket at id " + expected);
        }
        expected += blockCounts[b];
        b++;
      } else {
        if (spillKeys[s] <= 0 || spillIds[s] != expected) {
          throw new IllegalArgumentException("value dictionary spill does not continue the bucket at id " + expected);
        }
        expected++;
        s++;
      }
      if (expected > (long) firstId + count) {
        throw new IllegalArgumentException("value dictionary bucket directory overruns its range");
      }
    }
    if (expected != (long) firstId + count) {
      throw new IllegalArgumentException(
          "value dictionary bucket directory leaves ids " + expected + " .. " + (firstId + count - 1) + " uncovered");
    }
    this.nodeKey = nodeKey;
    this.firstId = firstId;
    this.count = count;
    this.blockFirstIds = adopt(blockFirstIds, copy);
    this.blockCounts = adopt(blockCounts, copy);
    this.blockKeys = adopt(blockKeys, copy);
    this.spillIds = adopt(spillIds, copy);
    this.spillKeys = adopt(spillKeys, copy);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_VALUE_BUCKET;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  public int getFirstId() {
    return firstId;
  }

  public int size() {
    return count;
  }

  private static int[] adopt(final int[] values, final boolean copy) {
    return values.length == 0
        ? NO_INTS
        : copy
            ? values.clone()
            : values;
  }

  private static long[] adopt(final long[] values, final boolean copy) {
    return values.length == 0
        ? NO_LONGS
        : copy
            ? values.clone()
            : values;
  }

  public int blockCount() {
    return blockKeys.length;
  }

  public int blockFirstId(final int index) {
    return blockFirstIds[index];
  }

  public int blockIdCount(final int index) {
    return blockCounts[index];
  }

  public long blockKey(final int index) {
    return blockKeys[index];
  }

  public int spillCount() {
    return spillKeys.length;
  }

  public int spillId(final int index) {
    return spillIds[index];
  }

  public long spillKeyAt(final int index) {
    return spillKeys[index];
  }

  /**
   * Record key of the sub-block packing {@code id}, or {@code 0} when {@code id} spilled. Floor
   * binary search over the ascending first-ids, then a range check; allocation-free.
   */
  public long blockKeyCovering(final int id) {
    // FLOOR search over the ascending first-ids, then a range check. A bucket may hold up to 256
    // runs, and this sits on the read view's miss path, so a linear scan is 256x the work it needs
    // to be at exactly the moment the id is not already cached.
    int low = 0;
    int high = blockFirstIds.length - 1;
    int candidate = -1;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      if (blockFirstIds[mid] <= id) {
        candidate = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return candidate >= 0 && (long) id < (long) blockFirstIds[candidate] + blockCounts[candidate]
        ? blockKeys[candidate]
        : 0L;
  }

  /** Record key of {@code id}'s own entry record, or {@code 0} when it is packed. */
  public long spillKeyCovering(final int id) {
    final int index = Arrays.binarySearch(spillIds, id);
    return index >= 0
        ? spillKeys[index]
        : 0L;
  }

  int[] blockFirstIdsRaw() {
    return blockFirstIds;
  }

  int[] blockCountsRaw() {
    return blockCounts;
  }

  long[] blockKeysRaw() {
    return blockKeys;
  }

  int[] spillIdsRaw() {
    return spillIds;
  }

  long[] spillKeysRaw() {
    return spillKeys;
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

  @Override
  public boolean equals(final Object object) {
    return object instanceof ValueDictionaryValueBucketNode other && nodeKey == other.nodeKey
        && firstId == other.firstId && count == other.count && Arrays.equals(blockFirstIds, other.blockFirstIds)
        && Arrays.equals(blockCounts, other.blockCounts) && Arrays.equals(blockKeys, other.blockKeys)
        && Arrays.equals(spillIds, other.spillIds) && Arrays.equals(spillKeys, other.spillKeys);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + firstId;
    result = 31 * result + count;
    // Every field equals() consults must appear here, or two unequal buckets can share a hash bucket
    // AND a hash code while comparing unequal — the contract this omitted blockCounts and spillKeys.
    result = 31 * result + Arrays.hashCode(blockFirstIds);
    result = 31 * result + Arrays.hashCode(blockCounts);
    result = 31 * result + Arrays.hashCode(blockKeys);
    result = 31 * result + Arrays.hashCode(spillIds);
    return 31 * result + Arrays.hashCode(spillKeys);
  }
}

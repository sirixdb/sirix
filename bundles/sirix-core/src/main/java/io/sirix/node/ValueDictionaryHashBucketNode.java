/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;

public final class ValueDictionaryHashBucketNode implements DataRecord {

  private final long nodeKey;
  private final int bucket;
  private final byte secondaryDepth;
  private final long secondaryPrefix;
  private final long nextBucketKey;
  private final long[] hashes;
  private final int[] ids;

  public ValueDictionaryHashBucketNode(final long nodeKey, final int bucket, final long[] hashes, final int[] ids) {
    this(nodeKey, bucket, (byte) 0, 0L, 0L, hashes, ids);
  }

  public ValueDictionaryHashBucketNode(final long nodeKey, final int bucket, final byte secondaryDepth,
      final long secondaryPrefix, final long nextBucketKey, final long[] hashes, final int[] ids) {
    if (nodeKey <= 0 || bucket < 0 || bucket > 0xFF_FFFF || hashes == null || ids == null || hashes.length != ids.length
        || hashes.length == 0 || secondaryDepth < 0 || secondaryDepth > Long.BYTES
        || (secondaryDepth == 0 && (secondaryPrefix != 0L || nextBucketKey != 0L)) || nextBucketKey < 0) {
      throw new IllegalArgumentException("invalid value dictionary hash bucket");
    }
    for (int i = 0; i < ids.length; i++) {
      if (ids[i] <= 0 || (i > 0 && Long.compareUnsigned(hashes[i - 1], hashes[i]) > 0)) {
        throw new IllegalArgumentException("hash bucket entries must be sorted and have positive ids");
      }
    }
    this.nodeKey = nodeKey;
    this.bucket = bucket;
    this.secondaryDepth = secondaryDepth;
    this.secondaryPrefix = secondaryPrefix;
    this.nextBucketKey = nextBucketKey;
    this.hashes = hashes.clone();
    this.ids = ids.clone();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_HASH_BUCKET;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  public int getBucket() {
    return bucket;
  }

  public byte getSecondaryDepth() {
    return secondaryDepth;
  }

  public long getSecondaryPrefix() {
    return secondaryPrefix;
  }

  public long getNextBucketKey() {
    return nextBucketKey;
  }

  public long[] getHashes() {
    return hashes.clone();
  }

  public int[] getIds() {
    return ids.clone();
  }

  public int size() {
    return ids.length;
  }

  public long hashAt(final int index) {
    return hashes[index];
  }

  public int idAt(final int index) {
    return ids[index];
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
    return object instanceof ValueDictionaryHashBucketNode other && nodeKey == other.nodeKey && bucket == other.bucket
        && secondaryDepth == other.secondaryDepth && secondaryPrefix == other.secondaryPrefix
        && nextBucketKey == other.nextBucketKey && Arrays.equals(hashes, other.hashes) && Arrays.equals(ids, other.ids);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + bucket;
    result = 31 * result + secondaryDepth;
    result = 31 * result + Long.hashCode(secondaryPrefix);
    result = 31 * result + Long.hashCode(nextBucketKey);
    result = 31 * result + Arrays.hashCode(hashes);
    return 31 * result + Arrays.hashCode(ids);
  }
}

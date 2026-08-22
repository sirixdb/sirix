/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;

public final class ValueDictionaryValueBucketNode implements DataRecord {

  public static final int VALUES_PER_BUCKET = 256;

  private final long nodeKey;
  private final int firstId;
  private final long[] entryKeys;

  public ValueDictionaryValueBucketNode(final long nodeKey, final int firstId, final long[] entryKeys) {
    if (nodeKey <= 0 || firstId <= 0 || ((firstId - 1) & (VALUES_PER_BUCKET - 1)) != 0
        || entryKeys == null || entryKeys.length == 0 || entryKeys.length > VALUES_PER_BUCKET) {
      throw new IllegalArgumentException("invalid value dictionary value bucket");
    }
    for (final long entryKey : entryKeys) {
      if (entryKey <= 0) {
        throw new IllegalArgumentException("value dictionary entry keys must be positive");
      }
    }
    this.nodeKey = nodeKey;
    this.firstId = firstId;
    this.entryKeys = entryKeys.clone();
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
    return entryKeys.length;
  }

  public long entryKey(final int index) {
    return entryKeys[index];
  }

  public long[] getEntryKeys() {
    return entryKeys.clone();
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
        && firstId == other.firstId && Arrays.equals(entryKeys, other.entryKeys);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + firstId;
    return 31 * result + Arrays.hashCode(entryKeys);
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

/**
 * Header of an immutable append segment in a global projection value dictionary.
 */
public final class ValueDictionarySegmentNode implements DataRecord {

  private final long nodeKey;
  private final long previousSegmentKey;
  private final int firstId;
  private final int entryCount;
  private final long entryBase;
  private final long directoryBase;
  private final int directoryBlockCount;

  public ValueDictionarySegmentNode(final long nodeKey, final long previousSegmentKey, final int firstId,
      final int entryCount, final long entryBase, final long directoryBase, final int directoryBlockCount) {
    if (nodeKey <= 0 || previousSegmentKey < 0 || firstId <= 0 || entryCount <= 0 || entryBase <= 0
        || directoryBase <= 0 || directoryBlockCount <= 0) {
      throw new IllegalArgumentException("invalid value dictionary segment layout");
    }
    final long lastId = (long) firstId + entryCount - 1L;
    if (lastId > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("value dictionary segment id range overflows int");
    }
    final int expectedDirectoryBlocks = (int) (((long) entryCount + ValueDictionaryDirectoryNode.ENTRIES_PER_BLOCK - 1L)
        / ValueDictionaryDirectoryNode.ENTRIES_PER_BLOCK);
    if (directoryBlockCount != expectedDirectoryBlocks) {
      throw new IllegalArgumentException("value dictionary segment directory does not cover every entry");
    }
    this.nodeKey = nodeKey;
    this.previousSegmentKey = previousSegmentKey;
    this.firstId = firstId;
    this.entryCount = entryCount;
    this.entryBase = entryBase;
    this.directoryBase = directoryBase;
    this.directoryBlockCount = directoryBlockCount;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_SEGMENT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  public long getPreviousSegmentKey() {
    return previousSegmentKey;
  }

  public int getFirstId() {
    return firstId;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public int getLastId() {
    return firstId + entryCount - 1;
  }

  public long getEntryBase() {
    return entryBase;
  }

  public long getDirectoryBase() {
    return directoryBase;
  }

  public int getDirectoryBlockCount() {
    return directoryBlockCount;
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
    return object instanceof ValueDictionarySegmentNode other && nodeKey == other.nodeKey
        && previousSegmentKey == other.previousSegmentKey && firstId == other.firstId && entryCount == other.entryCount
        && entryBase == other.entryBase && directoryBase == other.directoryBase
        && directoryBlockCount == other.directoryBlockCount;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + Long.hashCode(previousSegmentKey);
    result = 31 * result + firstId;
    result = 31 * result + entryCount;
    result = 31 * result + Long.hashCode(entryBase);
    result = 31 * result + Long.hashCode(directoryBase);
    return 31 * result + directoryBlockCount;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("previousSegmentKey", previousSegmentKey)
                         .add("firstId", firstId)
                         .add("entryCount", entryCount)
                         .add("entryBase", entryBase)
                         .add("directoryBase", directoryBase)
                         .add("directoryBlockCount", directoryBlockCount)
                         .toString();
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

public final class ValueDictionaryCollisionNode implements DataRecord {

  private final long nodeKey;
  private final int id;
  private final int height;
  private final long leftKey;
  private final long rightKey;

  public ValueDictionaryCollisionNode(final long nodeKey, final int id, final int height, final long leftKey,
      final long rightKey) {
    if (nodeKey <= 0 || id <= 0 || height <= 0 || leftKey < 0 || rightKey < 0) {
      throw new IllegalArgumentException("invalid value dictionary collision node");
    }
    this.nodeKey = nodeKey;
    this.id = id;
    this.height = height;
    this.leftKey = leftKey;
    this.rightKey = rightKey;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_COLLISION;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  public int getId() {
    return id;
  }

  public int getHeight() {
    return height;
  }

  public long getLeftKey() {
    return leftKey;
  }

  public long getRightKey() {
    return rightKey;
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
    return object instanceof ValueDictionaryCollisionNode other && nodeKey == other.nodeKey && id == other.id
        && height == other.height && leftKey == other.leftKey && rightKey == other.rightKey;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + id;
    result = 31 * result + height;
    result = 31 * result + Long.hashCode(leftKey);
    return 31 * result + Long.hashCode(rightKey);
  }
}

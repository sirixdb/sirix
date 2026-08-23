/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;

public final class ValueDictionaryRadixNode implements DataRecord {

  public static final byte FORWARD = 0;
  public static final byte REVERSE = 1;
  public static final int FANOUT = 256;

  private final long nodeKey;
  private final byte indexKind;
  private final byte depth;
  private final byte[] childSlots;
  private final long[] childKeys;

  public ValueDictionaryRadixNode(final long nodeKey, final byte indexKind, final byte depth, final long[] childKeys) {
    this(nodeKey, indexKind, depth, populatedSlots(childKeys), populatedKeys(childKeys));
  }

  public ValueDictionaryRadixNode(final long nodeKey, final byte indexKind, final byte depth, final byte[] childSlots,
      final long[] childKeys) {
    if (nodeKey <= 0 || (indexKind != FORWARD && indexKind != REVERSE) || depth < 0 || depth > 10 || childSlots == null
        || childKeys == null || childSlots.length != childKeys.length || childSlots.length > FANOUT) {
      throw new IllegalArgumentException("invalid value dictionary radix node");
    }
    int priorSlot = -1;
    for (int index = 0; index < childKeys.length; index++) {
      final int slot = Byte.toUnsignedInt(childSlots[index]);
      if (slot <= priorSlot || childKeys[index] <= 0) {
        throw new IllegalArgumentException("value dictionary children must be positive and slot-sorted");
      }
      priorSlot = slot;
    }
    this.nodeKey = nodeKey;
    this.indexKind = indexKind;
    this.depth = depth;
    this.childSlots = childSlots.clone();
    this.childKeys = childKeys.clone();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_RADIX;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  public byte getIndexKind() {
    return indexKind;
  }

  public byte getDepth() {
    return depth;
  }

  public long childKey(final int slot) {
    if (slot < 0 || slot >= FANOUT) {
      throw new IllegalArgumentException("value dictionary child slot out of range: " + slot);
    }
    int low = 0;
    int high = childSlots.length - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int candidate = Byte.toUnsignedInt(childSlots[middle]);
      if (candidate < slot) {
        low = middle + 1;
      } else if (candidate > slot) {
        high = middle - 1;
      } else {
        return childKeys[middle];
      }
    }
    return 0L;
  }

  public long[] getChildKeys() {
    final long[] dense = new long[FANOUT];
    for (int index = 0; index < childKeys.length; index++) {
      dense[Byte.toUnsignedInt(childSlots[index])] = childKeys[index];
    }
    return dense;
  }

  public byte[] getChildSlots() {
    return childSlots.clone();
  }

  public long[] getSparseChildKeys() {
    return childKeys.clone();
  }

  public int childCount() {
    return childKeys.length;
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
    return object instanceof ValueDictionaryRadixNode other && nodeKey == other.nodeKey && indexKind == other.indexKind
        && depth == other.depth && Arrays.equals(childSlots, other.childSlots)
        && Arrays.equals(childKeys, other.childKeys);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + indexKind;
    result = 31 * result + depth;
    result = 31 * result + Arrays.hashCode(childSlots);
    return 31 * result + Arrays.hashCode(childKeys);
  }

  private static byte[] populatedSlots(final long[] denseChildKeys) {
    validateDense(denseChildKeys);
    int count = 0;
    for (final long childKey : denseChildKeys) {
      if (childKey != 0)
        count++;
    }
    final byte[] slots = new byte[count];
    int out = 0;
    for (int slot = 0; slot < denseChildKeys.length; slot++) {
      if (denseChildKeys[slot] != 0)
        slots[out++] = (byte) slot;
    }
    return slots;
  }

  private static long[] populatedKeys(final long[] denseChildKeys) {
    validateDense(denseChildKeys);
    int count = 0;
    for (final long childKey : denseChildKeys) {
      if (childKey != 0)
        count++;
    }
    final long[] keys = new long[count];
    int out = 0;
    for (final long childKey : denseChildKeys) {
      if (childKey != 0)
        keys[out++] = childKey;
    }
    return keys;
  }

  private static void validateDense(final long[] denseChildKeys) {
    if (denseChildKeys == null || denseChildKeys.length != FANOUT) {
      throw new IllegalArgumentException("dense value dictionary radix children must have 256 slots");
    }
    for (final long childKey : denseChildKeys) {
      if (childKey < 0) {
        throw new IllegalArgumentException("value dictionary child keys must not be negative");
      }
    }
  }
}

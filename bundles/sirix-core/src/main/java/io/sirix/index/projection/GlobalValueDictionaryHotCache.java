/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;

final class GlobalValueDictionaryHotCache {

  private static final int SLOT_COUNT = 64;
  private static final int SLOT_MASK = SLOT_COUNT - 1;
  private static final int MAX_SLOT_LENGTH = 512;

  private final byte[] slotValues = new byte[SLOT_COUNT * MAX_SLOT_LENGTH];
  private final long[] slotHashes = new long[SLOT_COUNT];
  private final int[] slotLengths = new int[SLOT_COUNT];
  private final int[] slotIds = new int[SLOT_COUNT];
  private byte[] lastValue = new byte[64];
  private int lastLength = -1;
  private int lastId;

  int find(final byte[] source, final int offset, final int length) {
    if (lastId != 0 && lastLength == length && Arrays.equals(lastValue, 0, length, source, offset, offset + length)) {
      return lastId;
    }
    if (length > MAX_SLOT_LENGTH) {
      return 0;
    }
    final long hash = GlobalValueDictionary.valueHash(source, offset, length);
    final int slot = slot(hash);
    final int id = slotIds[slot];
    final int slotOffset = slot * MAX_SLOT_LENGTH;
    return id != 0 && slotHashes[slot] == hash && slotLengths[slot] == length
        && Arrays.equals(slotValues, slotOffset, slotOffset + length, source, offset, offset + length)
            ? id
            : 0;
  }

  void put(final byte[] source, final int offset, final int length, final int id) {
    if (id <= 0) {
      throw new IllegalArgumentException("dictionary id must be positive");
    }
    if (lastValue.length < length) {
      int capacity = lastValue.length;
      while (capacity < length) {
        capacity = Math.min(GlobalValueDictionaryWriter.MAX_VALUE_BYTES, Math.multiplyExact(capacity, 2));
      }
      lastValue = Arrays.copyOf(lastValue, capacity);
    }
    System.arraycopy(source, offset, lastValue, 0, length);
    lastLength = length;
    lastId = id;
    if (length > MAX_SLOT_LENGTH) {
      return;
    }
    final long hash = GlobalValueDictionary.valueHash(source, offset, length);
    final int slot = slot(hash);
    final int slotOffset = slot * MAX_SLOT_LENGTH;
    System.arraycopy(source, offset, slotValues, slotOffset, length);
    slotHashes[slot] = hash;
    slotLengths[slot] = length;
    slotIds[slot] = id;
  }

  private static int slot(final long hash) {
    return (int) (hash ^ (hash >>> 32)) & SLOT_MASK;
  }
}

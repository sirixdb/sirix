/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * Revision-persisted ordering of a projection's descriptor and segment slots. Payload formats and
 * side-page ownership are identical in both layouts; only the eight-byte HOT key changes.
 */
public enum ProjectionSlotLayout {
  /** Metadata version zero: all segments of a row group are adjacent. */
  ROW_GROUP_MAJOR(0),
  /** Metadata version one: each descriptor/segment kind occupies its own contiguous range. */
  COLUMN_MAJOR(1);

  // The inclusive row-group ceiling is 2^24, so its field needs 25 bits. The tag separates new
  // keys from every old row-group key, while all 16 slot-kind bits still fit below fence slots.
  static final long COLUMN_BASE = 1L << 41;
  private static final int ROW_GROUP_BITS = 25;
  private static final long ROW_GROUP_MASK = (1L << ROW_GROUP_BITS) - 1;
  private static final int MAX_SLOT_KIND = 0xFFFF;

  private final int metadataVersion;

  ProjectionSlotLayout(final int metadataVersion) {
    this.metadataVersion = metadataVersion;
  }

  int metadataVersion() {
    return metadataVersion;
  }

  long descriptorSlot(final long rowGroupId) {
    return slotKey(rowGroupId, 0);
  }

  long segmentSlot(final long rowGroupId, final int segmentId) {
    if (segmentId < 0 || segmentId >= MAX_SLOT_KIND) {
      throw new IllegalArgumentException("column segment id out of range: " + segmentId);
    }
    return slotKey(rowGroupId, segmentId + 1);
  }

  long slotKey(final long rowGroupId, final int slotKind) {
    checkRowGroup(rowGroupId);
    if (slotKind < 0 || slotKind > MAX_SLOT_KIND) {
      throw new IllegalArgumentException("slot kind out of range: " + slotKind);
    }
    return this == ROW_GROUP_MAJOR
        ? (rowGroupId << 16) | slotKind
        : COLUMN_BASE | ((long) slotKind << ROW_GROUP_BITS) | rowGroupId;
  }

  /** Whether a key belongs to this layout's namespace, including malformed row-group ids. */
  boolean contains(final long slotKey) {
    return this == ROW_GROUP_MAJOR
        ? slotKey >= (1L << 16) && slotKey < COLUMN_BASE
        : slotKey >= COLUMN_BASE && slotKey < ProjectionIndexFences.CHUNK_SLOT_BASE;
  }

  long rowGroupId(final long slotKey) {
    checkNamespace(slotKey);
    final long rowGroupId = this == ROW_GROUP_MAJOR
        ? slotKey >>> 16
        : slotKey & ROW_GROUP_MASK;
    checkRowGroup(rowGroupId);
    return rowGroupId;
  }

  int slotKind(final long slotKey) {
    checkNamespace(slotKey);
    return this == ROW_GROUP_MAJOR
        ? (int) (slotKey & MAX_SLOT_KIND)
        : (int) ((slotKey >>> ROW_GROUP_BITS) & MAX_SLOT_KIND);
  }

  private void checkNamespace(final long slotKey) {
    if (!contains(slotKey)) {
      throw new IllegalArgumentException("slot " + slotKey + " does not belong to " + this);
    }
  }

  private static void checkRowGroup(final long rowGroupId) {
    if (rowGroupId < 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("row-group id out of range: " + rowGroupId);
    }
  }
}

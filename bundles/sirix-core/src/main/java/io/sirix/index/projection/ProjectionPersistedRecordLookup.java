/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

/**
 * Authoritative persisted projection-record lookup: sparse exception locator first, then the
 * monotone normal-backbone fences. Stable node keys are identities only; the returned row ordinal
 * is the row's physical document-order position inside the leaf.
 *
 * <p>The result is a packed primitive, so lookup itself creates no per-result object. Loading a
 * distinct leaf necessarily materialises its verified descriptor, KEYS bytes, and decoded key lane
 * once; the ordinary one-leaf path retains those in a single cache entry. A primitive map is
 * allocated lazily only when one maintenance pass visits multiple leaves. Every candidate scans its
 * validated KEYS segment for an exact, unique identity. A missing exact row after a normal fence
 * range hit is ordinary absence (fences are ranges, not membership sets), while any exact-locator
 * disagreement is corruption.</p>
 */
final class ProjectionPersistedRecordLookup {

  static final long ABSENT = 0L;
  private static final int ROW_BITS = Integer.numberOfTrailingZeros(ProjectionIndexRowGroupPage.MAX_ROWS);
  private static final long EXCEPTION_BIT = 1L << ROW_BITS;
  private static final int ROW_MASK = ProjectionIndexRowGroupPage.MAX_ROWS - 1;
  private static final int SLOT_SHIFT = ROW_BITS + 1;

  static {
    if (Integer.bitCount(ProjectionIndexRowGroupPage.MAX_ROWS) != 1) {
      throw new ExceptionInInitializerError("projection row-group capacity must be a power of two");
    }
  }

  private final ProjectionIndexHOTStorage storage;
  private final ProjectionIndexFences.Accessor fences;
  private final ProjectionRecordLocator.Accessor locator;
  private int cachedSlot;
  private @Nullable Keys cachedKeys;
  private @Nullable Int2ObjectOpenHashMap<Keys> keysBySlot;
  private int descriptorsRead;
  private int keySegmentsRead;

  ProjectionPersistedRecordLookup(final ProjectionIndexHOTStorage storage,
      final ProjectionIndexFences.Accessor fences,
      final ProjectionRecordLocator.Accessor locator) {
    if (storage == null || fences == null || locator == null) {
      throw new NullPointerException("projection lookup dependencies are required");
    }
    this.storage = storage;
    this.fences = fences;
    this.locator = locator;
  }

  long find(final long recordKey) {
    if (recordKey < 0) {
      throw new IllegalArgumentException("projection record key must be non-negative: " + recordKey);
    }
    final int exactSlot = locator.find(recordKey);
    if (exactSlot != 0) {
      if (!fences.isLivePhysicalSlot(exactSlot)) {
        throw new IllegalStateException("projection exception locator " + recordKey
            + " targets non-live physical leaf " + exactSlot);
      }
      final long exact = exactMatch(recordKey, exactSlot, true);
      final int normalSlot = fences.findSlot(recordKey);
      if (normalSlot >= 1 && normalSlot != exactSlot
          && exactMatch(recordKey, normalSlot, false) != ABSENT) {
        throw new IllegalStateException("projection record " + recordKey
            + " occurs on both exception and normal lookup routes");
      }
      return exact;
    }

    final int normalSlot = fences.findSlot(recordKey);
    if (normalSlot < 1) {
      return ABSENT;
    }
    return exactMatch(recordKey, normalSlot, false);
  }

  Keys keys(final int physicalSlot) {
    if (physicalSlot == cachedSlot && cachedKeys != null) {
      return cachedKeys;
    }
    if (keysBySlot != null) {
      final Keys prior = keysBySlot.get(physicalSlot);
      if (prior != null) {
        cachedSlot = physicalSlot;
        cachedKeys = prior;
        return prior;
      }
    }
    if (!fences.isLivePhysicalSlot(physicalSlot)) {
      throw new IllegalStateException("projection KEYS requested for non-live physical leaf " + physicalSlot);
    }
    final byte[] descriptor = storage.getVerifiedRowGroupDescriptor(physicalSlot);
    if (descriptor == null) {
      throw new IllegalStateException("projection physical leaf " + physicalSlot + " has no descriptor");
    }
    descriptorsRead++;
    final byte[] keySegment = storage.getVerifiedColumnSegment(physicalSlot, descriptor,
        ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_KEYS);
    if (keySegment == null) {
      throw new IllegalStateException("projection physical leaf " + physicalSlot + " has no KEYS segment");
    }
    keySegmentsRead++;
    final ProjectionIndexColumnSegmentCodec.KeysView view =
        ProjectionIndexColumnSegmentCodec.decodeKeysView(descriptor, keySegment);
    if (view.recordKeys().length != RowGroupDescriptor.rowCount(descriptor)
        || view.firstRecordKey() != fences.first(physicalSlot)
        || view.lastRecordKey() != fences.last(physicalSlot)) {
      throw new IllegalStateException("projection KEYS/fence mirror mismatch at physical leaf " + physicalSlot);
    }
    final Keys loaded = new Keys(descriptor, keySegment, view);
    if (cachedKeys != null) {
      if (keysBySlot == null) {
        keysBySlot = new Int2ObjectOpenHashMap<>();
        keysBySlot.put(cachedSlot, cachedKeys);
      }
      keysBySlot.put(physicalSlot, loaded);
    }
    cachedSlot = physicalSlot;
    cachedKeys = loaded;
    return loaded;
  }

  int descriptorsRead() {
    return descriptorsRead;
  }

  int keySegmentsRead() {
    return keySegmentsRead;
  }

  static int slot(final long packed) {
    if (packed == ABSENT) {
      throw new IllegalArgumentException("absent projection location has no physical slot");
    }
    return Math.toIntExact(packed >>> SLOT_SHIFT);
  }

  static int row(final long packed) {
    if (packed == ABSENT) {
      throw new IllegalArgumentException("absent projection location has no row");
    }
    return (int) packed & ROW_MASK;
  }

  static boolean orderException(final long packed) {
    return packed != ABSENT && (packed & EXCEPTION_BIT) != 0L;
  }

  private long exactMatch(final long recordKey, final int physicalSlot,
      final boolean mustBeException) {
    final ProjectionIndexColumnSegmentCodec.KeysView view = keys(physicalSlot).view();
    int row = -1;
    for (int index = 0; index < view.recordKeys().length; index++) {
      if (view.recordKeys()[index] != recordKey) {
        continue;
      }
      if (row >= 0) {
        throw new IllegalStateException("projection record " + recordKey
            + " occurs more than once in physical leaf " + physicalSlot);
      }
      row = index;
    }
    if (row < 0) {
      if (mustBeException) {
        throw new IllegalStateException("projection exception locator " + recordKey
            + " targets leaf " + physicalSlot + " whose KEYS do not contain it");
      }
      return ABSENT;
    }
    final boolean exception = view.orderExceptionAt(row);
    if (exception != mustBeException) {
      throw new IllegalStateException("projection record " + recordKey + " in physical leaf "
          + physicalSlot + " has exception=" + exception + " but lookup route requires "
          + mustBeException);
    }
    return ((long) physicalSlot << SLOT_SHIFT) | (exception ? EXCEPTION_BIT : 0L) | row;
  }

  record Keys(byte[] descriptor, byte[] keySegment,
              ProjectionIndexColumnSegmentCodec.KeysView view) {
  }
}

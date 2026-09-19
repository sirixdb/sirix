/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;

import java.util.function.IntConsumer;

/** Revisioned, per-leaf blob placement for an independently sorted covering projection. */
final class ProjectionSortedLeafStore {

  /** Disjoint from the row/column, fence, Bloom, value-summary, and flag-summary namespaces. */
  static final long LEAF_SLOT_BASE = 1L << 46;
  private static final long LAST_LEAF_SLOT = LEAF_SLOT_BASE + (1L << 32) - 1;

  /** Test observation of data-leaf rewrites; production keeps it null and pays one null check. */
  private static volatile @Nullable IntConsumer writeObserverForTesting;

  /**
   * Test observation of data-leaf reads through a write transaction's storage; null in production.
   */
  private static volatile @Nullable IntConsumer storageReadObserverForTesting;

  private ProjectionSortedLeafStore() {}

  static void setWriteObserverForTesting(final @Nullable IntConsumer observer) {
    writeObserverForTesting = observer;
  }

  static void setStorageReadObserverForTesting(final @Nullable IntConsumer observer) {
    storageReadObserverForTesting = observer;
  }

  /** Write one leaf, its group summary and its bound in the owning transaction. */
  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf leaf,
      final ProjectionSortKeyCodec.Layout layout) {
    final ProjectionSortedLeafBounds.Updater bounds = new ProjectionSortedLeafBounds.Updater(storage);
    write(storage, leafId, leaf, layout, bounds);
    bounds.flush();
  }

  /** Maintenance form: the caller publishes the coalesced bound chunks once per pass. */
  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf leaf,
      final ProjectionSortKeyCodec.Layout layout, final ProjectionSortedLeafBounds.Updater bounds) {
    bounds.set(leafId, writeLeafAndSummary(storage, leafId, leaf, layout));
  }

  /** Initial-build form: bounds are appended in consecutive leaf-id order and published by chunk. */
  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf leaf,
      final ProjectionSortKeyCodec.Layout layout, final ProjectionSortedLeafBounds.Builder initialBounds) {
    initialBounds.append(leafId, writeLeafAndSummary(storage, leafId, leaf, layout));
  }

  private static @Nullable ProjectionSortedLeaf writeLeafAndSummary(final ProjectionIndexHOTStorage storage,
      final int leafId, final ProjectionSortedLeaf leaf, final ProjectionSortKeyCodec.Layout layout) {
    if (storage == null || leaf == null || layout == null) {
      throw new NullPointerException("sorted leaf storage, leaf and layout are required");
    }
    final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf, layout);
    storage.putBlob(slot(leafId), leaf.encodedBytes());
    if (summary != null) {
      storage.putBlob(ProjectionSortedGroupSummary.slot(leafId), summary.encodedBytes());
    } else {
      storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(leafId));
    }
    final IntConsumer observer = writeObserverForTesting;
    if (observer != null) {
      observer.accept(leafId);
    }
    return summary;
  }

  static @Nullable ProjectionSortedLeaf read(final StorageEngineReader reader, final int indexNumber,
      final int leafId) {
    if (reader == null) {
      throw new NullPointerException("storage reader is required");
    }
    final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot(leafId));
    return bytes == null
        ? null
        : ProjectionSortedLeaf.open(bytes);
  }

  static @Nullable ProjectionSortedLeaf read(final ProjectionIndexHOTStorage storage, final int leafId) {
    if (storage == null) {
      throw new NullPointerException("sorted leaf storage is required");
    }
    final IntConsumer observer = storageReadObserverForTesting;
    if (observer != null) {
      observer.accept(leafId);
    }
    final byte[] bytes = storage.getBlob(slot(leafId));
    return bytes == null
        ? null
        : ProjectionSortedLeaf.open(bytes);
  }

  static void remove(final ProjectionIndexHOTStorage storage, final int leafId) {
    final ProjectionSortedLeafBounds.Updater bounds = new ProjectionSortedLeafBounds.Updater(storage);
    remove(storage, leafId, bounds);
    bounds.flush();
  }

  static void remove(final ProjectionIndexHOTStorage storage, final int leafId,
      final ProjectionSortedLeafBounds.Updater bounds) {
    if (storage == null) {
      throw new NullPointerException("sorted leaf storage is required");
    }
    bounds.clear(leafId);
    storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(leafId));
    storage.tombstoneBlob(slot(leafId));
  }

  private static long slot(final int leafId) {
    if (leafId < 1 || LEAF_SLOT_BASE + leafId > LAST_LEAF_SLOT) {
      throw new IllegalArgumentException("sorted leaf id is outside the reserved slot range: " + leafId);
    }
    return LEAF_SLOT_BASE + leafId;
  }
}

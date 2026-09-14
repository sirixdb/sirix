/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;

/** Revisioned, per-leaf blob placement for an independently sorted covering projection. */
final class ProjectionSortedLeafStore {

  /** Disjoint from the row/column, fence, Bloom, value-summary, and flag-summary namespaces. */
  static final long LEAF_SLOT_BASE = 1L << 46;
  private static final long LAST_LEAF_SLOT = LEAF_SLOT_BASE + (1L << 32) - 1;

  private ProjectionSortedLeafStore() {}

  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf leaf) {
    write(storage, leafId, leaf, null);
  }

  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf leaf,
      final ProjectionSortedLeafBounds.@Nullable Builder initialBounds) {
    if (storage == null || leaf == null) {
      throw new NullPointerException("sorted leaf storage and leaf are required");
    }
    final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf);
    // Invalidate the old acceleration before changing its source. Even a failed second write
    // cannot leave a stale summary visible to a caller that inspects its open transaction.
    if (initialBounds == null) {
      ProjectionSortedLeafBounds.invalidate(storage, leafId);
    }
    storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(leafId));
    storage.putBlob(slot(leafId), leaf.encodedBytes());
    if (summary != null) {
      storage.putBlob(ProjectionSortedGroupSummary.slot(leafId), summary.encodedBytes());
      if (initialBounds == null) {
        ProjectionSortedLeafBounds.write(storage, leafId, summary);
      }
    }
    if (initialBounds != null) {
      initialBounds.append(leafId, summary);
    }
  }

  static @Nullable ProjectionSortedLeaf read(final StorageEngineReader reader, final int indexNumber,
      final int leafId) {
    if (reader == null) {
      throw new NullPointerException("storage reader is required");
    }
    final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot(leafId));
    return bytes == null ? null : ProjectionSortedLeaf.open(bytes);
  }

  static @Nullable ProjectionSortedLeaf read(final ProjectionIndexHOTStorage storage, final int leafId) {
    if (storage == null) {
      throw new NullPointerException("sorted leaf storage is required");
    }
    final byte[] bytes = storage.getBlob(slot(leafId));
    return bytes == null ? null : ProjectionSortedLeaf.open(bytes);
  }

  static void remove(final ProjectionIndexHOTStorage storage, final int leafId) {
    if (storage == null) {
      throw new NullPointerException("sorted leaf storage is required");
    }
    ProjectionSortedLeafBounds.invalidate(storage, leafId);
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

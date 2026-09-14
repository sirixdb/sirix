/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/** Per-data-leaf extrema for sorted (string group, ordered long, record key) tuples. */
final class ProjectionSortedGroupSummary {
  // Each namespace reserves 2^32 slots; all blob owners remain below the side-map's 2^47 ceiling.
  static final long SLOT_BASE = ProjectionSortedLeafStore.LEAF_SLOT_BASE + (2L << 32);
  static final int PAYLOAD_BYTES = 2 * Long.BYTES;

  private ProjectionSortedGroupSummary() {}

  /** Unsupported tuple shapes have no summary; callers retain the full-key route. */
  static @Nullable ProjectionSortedLeaf encode(final ProjectionSortedLeaf source) {
    final int rows = source.rowCount();
    final byte[][] groups = new byte[rows][];
    final byte[][] payloads = new byte[rows][];
    byte[] key = new byte[128];
    int count = 0;
    for (int row = 0; row < rows; row++) {
      final int length = source.keyLength(row);
      if (length > key.length) {
        key = new byte[length];
      }
      source.copyKeyTo(row, key);
      final int prefix = ProjectionSortedGroupScan.stringPrefixLength(key, length);
      if (prefix < 0 || length != prefix + 1 + 2 * Long.BYTES || key[prefix] != 1) {
        return null;
      }
      final long value = ProjectionSortedGroupScan.readOrderedLong(key, prefix + 1);
      if (count == 0 || groups[count - 1].length != prefix
          || !Arrays.equals(groups[count - 1], 0, prefix, key, 0, prefix)) {
        groups[count] = Arrays.copyOf(key, prefix);
        final byte[] payload = new byte[PAYLOAD_BYTES];
        ProjectionIndexRowGroupCodec.putLongLEAt(payload, 0, value);
        ProjectionIndexRowGroupCodec.putLongLEAt(payload, Long.BYTES, value);
        payloads[count++] = payload;
      } else {
        // The source was validated in tuple order, so this is the group's new maximum.
        ProjectionIndexRowGroupCodec.putLongLEAt(payloads[count - 1], Long.BYTES, value);
      }
    }
    return ProjectionSortedLeaf.encode(groups, payloads, count);
  }

  static @Nullable ProjectionSortedLeaf read(final StorageEngineReader reader, final int indexNumber,
      final int leafId) {
    final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot(leafId));
    return bytes == null ? null : ProjectionSortedLeaf.open(bytes);
  }

  static long slot(final int leafId) {
    if (leafId < 1) {
      throw new IllegalArgumentException("sorted summary leaf id must be positive");
    }
    return SLOT_BASE + leafId;
  }
}

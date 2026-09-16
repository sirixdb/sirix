/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** Revisioned extrema of bounded sorted leaves; one 1 KiB chunk covers 64 physical leaf ids. */
final class ProjectionSortedLeafBounds {
  static final long SLOT_BASE = ProjectionSortedGroupSummary.SLOT_BASE + (1L << 32);
  private static final int MAGIC = 0x31425350; // PSB1
  private static final int HEADER_BYTES = 16;
  private static final int CHUNK_BYTES = HEADER_BYTES + 64 * 2 * Long.BYTES;
  /** Bound the four primitive candidate arrays to 24 MiB; larger views retain the streaming scan. */
  private static final int MAX_CANDIDATE_LEAVES = 1 << 20;

  private ProjectionSortedLeafBounds() {}

  record Candidates(int[] leafIds, long[] minimums, long[] maximums, int[] order) {
  }

  /** Invalidate before changing the source; an interrupted update then takes the exact fallback. */
  static void invalidate(final ProjectionIndexHOTStorage storage, final int leafId) {
    final long slot = slot(leafId);
    final byte[] bytes = storage.getBlob(slot);
    if (bytes == null) {
      return;
    }
    validate(bytes);
    final long mask = 1L << ((leafId - 1) & 63);
    final long valid = ProjectionIndexRowGroupCodec.getLongLE(bytes, 8);
    if ((valid & mask) != 0) {
      // Referenced getBlob payloads alias an immutable page, including frozen async generations.
      final byte[] changed = bytes.clone();
      ProjectionIndexRowGroupCodec.putLongLEAt(changed, 8, valid & ~mask);
      storage.putBlob(slot, changed);
    }
  }

  /** Publish only after the source leaf and its group summary have both been written. */
  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf summary) {
    if (summary.rowCount() == 0) {
      invalidate(storage, leafId);
      return;
    }
    final long slot = slot(leafId);
    byte[] bytes = storage.getBlob(slot);
    if (bytes == null) {
      bytes = newChunk();
    } else {
      validate(bytes);
      bytes = bytes.clone();
    }
    writeEntry(bytes, leafId, summary, new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES]);
    storage.putBlob(slot, bytes);
  }

  private static byte[] newChunk() {
    final byte[] bytes = new byte[CHUNK_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MAGIC);
    bytes[4] = 1;
    bytes[5] = 6; // log2 leaves per chunk
    return bytes;
  }

  private static void writeEntry(final byte[] bytes, final int leafId, final ProjectionSortedLeaf summary,
      final byte[] payload) {
    long minimum = Long.MAX_VALUE;
    long maximum = Long.MIN_VALUE;
    for (int row = 0; row < summary.rowCount(); row++) {
      if (summary.payloadLength(row) != payload.length) {
        throw new IllegalStateException("invalid sorted group summary extrema payload");
      }
      summary.copyPayloadTo(row, payload, 0);
      final long min = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
      final long max = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
      if (min > max) {
        throw new IllegalStateException("invalid sorted group summary extrema");
      }
      minimum = Math.min(minimum, min);
      maximum = Math.max(maximum, max);
    }
    final int position = (leafId - 1) & 63;
    final int offset = HEADER_BYTES + position * 2 * Long.BYTES;
    ProjectionIndexRowGroupCodec.putLongLEAt(bytes, offset, minimum);
    ProjectionIndexRowGroupCodec.putLongLEAt(bytes, offset + Long.BYTES, maximum);
    ProjectionIndexRowGroupCodec.putLongLEAt(bytes, 8,
        ProjectionIndexRowGroupCodec.getLongLE(bytes, 8) | 1L << position);
  }

  /**
   * Initial-build accumulator. Consecutive physical ids let each chunk be published once, which keeps
   * its side-page key append-only until bulk staging has completed. Owns at most one chunk; a
   * published byte array is never reused because storage may retain it in an immutable page.
   */
  static final class Builder {
    private final ProjectionIndexHOTStorage storage;
    private final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
    private byte @Nullable [] pending;
    private long pendingSlot = -1;
    private int lastLeafId;
    private boolean finished;

    Builder(final ProjectionIndexHOTStorage storage) {
      this.storage = Objects.requireNonNull(storage, "storage");
    }

    void append(final int leafId, final @Nullable ProjectionSortedLeaf summary) {
      if (finished || leafId < 1 || leafId != lastLeafId + 1) {
        throw new IllegalStateException("sorted bounds builder requires consecutive new leaf ids");
      }
      final long slot = slot(leafId);
      if (slot != pendingSlot) {
        flush();
        pendingSlot = slot;
      }
      if (summary != null && summary.rowCount() != 0) {
        if (pending == null) {
          pending = newChunk();
        }
        writeEntry(pending, leafId, summary, payload);
      }
      lastLeafId = leafId;
    }

    void finish() {
      if (finished) {
        throw new IllegalStateException("sorted bounds builder is already finished");
      }
      flush();
      finished = true;
    }

    private void flush() {
      if (pending != null) {
        storage.putBlob(pendingSlot, pending);
        pending = null;
      }
    }
  }

  /**
   * Capture every live leaf's bound before pruning. Missing chunks or invalid entries decline this
   * optional acceleration. Sorting physical ids resolves each chunk once even after directory splits
   * reorder the ids; a primitive indirect sort then supplies ascending minima without boxing leaves.
   */
  static @Nullable Candidates read(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory) {
    return read(reader, indexNumber, directory, true);
  }

  /** Span scans supply their own priority order after resolving groups crossing leaf boundaries. */
  static @Nullable Candidates readUnordered(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory) {
    return read(reader, indexNumber, directory, false);
  }

  private static @Nullable Candidates read(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final boolean orderByMinimum) {
    final int count = directory.dataLeafCount();
    if (count > MAX_CANDIDATE_LEAVES) {
      return null;
    }
    final int[] leafIds = new int[count];
    final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
    int captured = 0;
    while (cursor.id() != 0) {
      if (captured == count) {
        throw new IllegalStateException("sorted directory exceeds its declared leaf count");
      }
      leafIds[captured++] = cursor.id();
      cursor.advance();
    }
    if (captured != count) {
      throw new IllegalStateException("sorted directory is missing declared leaves");
    }
    Arrays.sort(leafIds);
    final long[] minimums = new long[count];
    final long[] maximums = new long[count];
    final int[] order = new int[count];
    for (int from = 0; from < count;) {
      final long slot = slot(leafIds[from]);
      final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot);
      if (bytes == null) {
        return null;
      }
      validate(bytes);
      final long valid = ProjectionIndexRowGroupCodec.getLongLE(bytes, 8);
      int to = from;
      while (to < count && slot(leafIds[to]) == slot) {
        if (to > 0 && leafIds[to] == leafIds[to - 1]) {
          throw new IllegalStateException("sorted directory repeats a physical leaf");
        }
        final int position = (leafIds[to] - 1) & 63;
        if ((valid & 1L << position) == 0) {
          return null;
        }
        final int offset = HEADER_BYTES + position * 2 * Long.BYTES;
        minimums[to] = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset);
        maximums[to] = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset + Long.BYTES);
        if (minimums[to] > maximums[to]) {
          throw new IllegalStateException("invalid sorted leaf extrema");
        }
        order[to] = to;
        to++;
      }
      from = to;
    }
    if (orderByMinimum) {
      LongArrays.quickSortIndirect(order, minimums);
    }
    return new Candidates(leafIds, minimums, maximums, order);
  }

  static long slot(final int leafId) {
    if (leafId < 1) {
      throw new IllegalArgumentException("sorted bound leaf id must be positive");
    }
    return SLOT_BASE + ((leafId - 1) >>> 6);
  }

  private static void validate(final byte[] bytes) {
    if (bytes.length != CHUNK_BYTES || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MAGIC || bytes[4] != 1
        || bytes[5] != 6 || bytes[6] != 0 || bytes[7] != 0) {
      throw new IllegalStateException("invalid sorted leaf bounds chunk");
    }
  }
}

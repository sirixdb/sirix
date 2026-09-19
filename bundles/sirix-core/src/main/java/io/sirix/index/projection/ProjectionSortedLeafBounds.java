/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Revisioned extrema of bounded sorted leaves. One 272-byte chunk covers 16 physical leaf ids,
 * small enough to live inline in its trie entry: a chunk written by a build therefore never becomes
 * a staged side page, and maintenance later in the same transaction can still replace it.
 */
final class ProjectionSortedLeafBounds {
  static final long SLOT_BASE = ProjectionSortedGroupSummary.SLOT_BASE + (1L << 32);
  private static final int MAGIC = 0x31425350; // PSB1
  private static final int HEADER_BYTES = 16;
  private static final int LEAF_SHIFT = 4;
  private static final int POSITION_MASK = (1 << LEAF_SHIFT) - 1;
  private static final int CHUNK_BYTES = HEADER_BYTES + (1 << LEAF_SHIFT) * 2 * Long.BYTES;
  /** Bound the four primitive candidate arrays to 24 MiB; larger views retain the streaming scan. */
  static final int MAX_CANDIDATE_LEAVES = 1 << 20;

  private ProjectionSortedLeafBounds() {}

  record Candidates(int[] leafIds, long[] minimums, long[] maximums, int[] order) {
  }

  /** Clear one leaf's bound; readers then take the exact fallback for that leaf. */
  static void invalidate(final ProjectionIndexHOTStorage storage, final int leafId) {
    final Updater updater = new Updater(storage);
    updater.clear(leafId);
    updater.flush();
  }

  /** Publish only after the source leaf and its group summary have both been written. */
  static void write(final ProjectionIndexHOTStorage storage, final int leafId, final ProjectionSortedLeaf summary) {
    final Updater updater = new Updater(storage);
    updater.set(leafId, summary);
    updater.flush();
  }

  /**
   * Coalesces one maintenance pass's bound changes: each touched chunk is copied once, edited in
   * place for every touched leaf it covers, and published once. A published array is never edited
   * again, because storage may retain it in an immutable page.
   */
  static final class Updater {
    private final ProjectionIndexHOTStorage storage;
    private final Long2ObjectOpenHashMap<byte[]> chunks = new Long2ObjectOpenHashMap<>();
    private final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];

    Updater(final ProjectionIndexHOTStorage storage) {
      this.storage = Objects.requireNonNull(storage, "storage");
    }

    /** Record a leaf's extrema, or clear its bound when it has no summary. */
    void set(final int leafId, final @Nullable ProjectionSortedLeaf summary) {
      if (summary == null || summary.rowCount() == 0) {
        clear(leafId);
        return;
      }
      final long slot = slot(leafId);
      byte[] chunk = chunks.get(slot);
      if (chunk == null) {
        final byte[] stored = storage.getBlob(slot);
        if (stored == null) {
          chunk = newChunk();
        } else {
          validate(stored);
          chunk = stored.clone();
        }
        chunks.put(slot, chunk);
      }
      writeEntry(chunk, leafId, summary, payload);
    }

    void clear(final int leafId) {
      final long slot = slot(leafId);
      final long mask = 1L << ((leafId - 1) & POSITION_MASK);
      byte[] chunk = chunks.get(slot);
      if (chunk == null) {
        final byte[] stored = storage.getBlob(slot);
        if (stored == null) {
          return;
        }
        validate(stored);
        if ((ProjectionIndexRowGroupCodec.getLongLE(stored, 8) & mask) == 0) {
          return;
        }
        chunk = stored.clone();
        chunks.put(slot, chunk);
      }
      ProjectionIndexRowGroupCodec.putLongLEAt(chunk, 8, ProjectionIndexRowGroupCodec.getLongLE(chunk, 8) & ~mask);
    }

    void flush() {
      for (final Long2ObjectMap.Entry<byte[]> entry : chunks.long2ObjectEntrySet()) {
        storage.putBlob(entry.getLongKey(), entry.getValue());
      }
      chunks.clear();
    }
  }

  private static byte[] newChunk() {
    final byte[] bytes = new byte[CHUNK_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MAGIC);
    bytes[4] = 1;
    bytes[5] = LEAF_SHIFT;
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
    final int position = (leafId - 1) & POSITION_MASK;
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
    final int[] leafIds = directory.leafIds(ProjectionSortedDirectory.NO_BOUND, null, MAX_CANDIDATE_LEAVES);
    return leafIds == null
        ? null
        : read(reader, indexNumber, leafIds, true);
  }

  /** Bounds of the given live leaves, ordered by ascending minimum; the array is sorted in place. */
  static @Nullable Candidates read(final StorageEngineReader reader, final int indexNumber, final int[] leafIds) {
    return read(reader, indexNumber, leafIds, true);
  }

  /** Span scans supply their own priority order after resolving groups crossing leaf boundaries. */
  static @Nullable Candidates readUnordered(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory) {
    final int[] leafIds = directory.leafIds(ProjectionSortedDirectory.NO_BOUND, null, MAX_CANDIDATE_LEAVES);
    return leafIds == null
        ? null
        : read(reader, indexNumber, leafIds, false);
  }

  static @Nullable Candidates readUnordered(final StorageEngineReader reader, final int indexNumber,
      final int[] leafIds) {
    return read(reader, indexNumber, leafIds, false);
  }

  private static @Nullable Candidates read(final StorageEngineReader reader, final int indexNumber, final int[] leafIds,
      final boolean orderByMinimum) {
    final int count = leafIds.length;
    if (count > MAX_CANDIDATE_LEAVES) {
      return null;
    }
    Arrays.sort(leafIds);
    final long[] minimums = new long[count];
    final long[] maximums = new long[count];
    final int[] order = new int[count];
    // Every chunk slot is known now, so the chunks are fetched together (one coalesced batch per
    // 1,024 chunks) instead of one root-to-leaf descent plus one payload read per chunk; the loop
    // below still validates them in order and still stops at the first missing chunk.
    final byte @Nullable [][] chunks = readChunks(reader, indexNumber, leafIds);
    for (int from = 0, chunk = 0; from < count; chunk++) {
      final long slot = slot(leafIds[from]);
      final byte[] bytes = chunks != null
          ? chunks[chunk]
          : ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot);
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
        final int position = (leafIds[to] - 1) & POSITION_MASK;
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

  /**
   * Fetch every distinct bounds chunk of the sorted physical ids in batches. {@code null} means "read
   * them one by one": a single chunk gains nothing from a batch, and a failing batch must not report
   * a chunk the serial order would never have reached — the per-chunk reads then fail (or stop at a
   * missing chunk) exactly as before.
   */
  private static byte @Nullable [] @Nullable [] readChunks(final StorageEngineReader reader, final int indexNumber,
      final int[] sortedLeafIds) {
    int distinct = 0;
    long previous = -1;
    for (final int leafId : sortedLeafIds) {
      final long slot = slot(leafId);
      if (slot != previous) {
        distinct++;
        previous = slot;
      }
    }
    if (distinct < 2) {
      return null;
    }
    final long[] slots = new long[distinct];
    int at = 0;
    previous = -1;
    for (final int leafId : sortedLeafIds) {
      final long slot = slot(leafId);
      if (slot != previous) {
        slots[at++] = slot;
        previous = slot;
      }
    }
    final byte[][] chunks = new byte[distinct][];
    try {
      for (int from = 0; from < distinct; from += CHUNK_BATCH) {
        final int to = Math.min(distinct, from + CHUNK_BATCH);
        final byte[][] batch = ProjectionIndexHOTStorage.readBlobBatch(reader, indexNumber, from == 0 && to == distinct
            ? slots
            : Arrays.copyOfRange(slots, from, to));
        System.arraycopy(batch, 0, chunks, from, to - from);
      }
    } catch (final RuntimeException batchFailure) {
      return null;
    }
    return chunks;
  }

  /** Slots per {@link ProjectionIndexHOTStorage#readBlobBatch} call, its documented maximum. */
  private static final int CHUNK_BATCH = 1024;

  static long slot(final int leafId) {
    if (leafId < 1) {
      throw new IllegalArgumentException("sorted bound leaf id must be positive");
    }
    return SLOT_BASE + ((leafId - 1) >>> LEAF_SHIFT);
  }

  private static void validate(final byte[] bytes) {
    if (bytes.length != CHUNK_BYTES || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MAGIC || bytes[4] != 1
        || bytes[5] != LEAF_SHIFT || bytes[6] != 0 || bytes[7] != 0) {
      throw new IllegalStateException("invalid sorted leaf bounds chunk");
    }
  }
}

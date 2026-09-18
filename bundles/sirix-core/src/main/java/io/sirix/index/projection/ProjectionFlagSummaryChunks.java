/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Revision-local, bounded copy-on-write evidence for BODY-column flags.
 *
 * <p>
 * Each 32-leaf chunk keeps one liveness byte and one three-bit evidence byte per column and
 * physical leaf. A maintenance commit patches only chunks containing changed leaves. Old indexes
 * have no header and continue deriving evidence from descriptors.
 * </p>
 */
final class ProjectionFlagSummaryChunks {
  static final long CHUNK_SLOT_BASE = 1L << 45;
  static final long HEADER_SLOT = CHUNK_SLOT_BASE + (1L << 20);
  static final int CHUNK_LEAVES = ProjectionIndexFences.CHUNK_LEAVES;
  static final int MAX_COLUMNS = 8;

  private static final int MAGIC = 0x53464950;
  private static final byte VERSION = 1;
  private static final int HEADER_BYTES = 20;
  private static final byte LIVE = 1;
  private static final byte UNREP_ANY = 1;
  private static final byte NONINT_ANY = 2;
  private static final byte PURE_ALL = 4;
  private static final byte EVIDENCE_MASK = UNREP_ANY | NONINT_ANY | PURE_ALL;

  private ProjectionFlagSummaryChunks() {}

  static final class BuildWriter {
    private byte @Nullable [] chunk;
    private int columns = -1;
    private int physicalCount;
    private int entries;
    private int chunksWritten;
    private boolean finished;

    void append(final ProjectionIndexHOTStorage storage, final byte[] descriptor) {
      Objects.requireNonNull(storage, "storage");
      Objects.requireNonNull(descriptor, "descriptor");
      if (finished) {
        throw new IllegalStateException("projection flag-summary writer is finished");
      }
      if (columns < 0) {
        // The publishing storage validates every encoded descriptor. Validate the first one here
        // as well to establish this writer's shape, then keep subsequent appends O(1) for wide
        // projections that do not persist flag evidence.
        RowGroupDescriptor.validate(descriptor);
      }
      final int descriptorColumns = RowGroupDescriptor.columnCount(descriptor);
      if (columns < 0) {
        columns = descriptorColumns;
        if (columns <= MAX_COLUMNS) {
          chunk = new byte[CHUNK_LEAVES * (columns + 1)];
        }
      } else if (descriptorColumns != columns) {
        throw new IllegalStateException("projection flag-summary column shape changed during build");
      }
      if (physicalCount == ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        throw new IllegalStateException("projection flag-summary row-group limit reached");
      }
      physicalCount++;
      final byte[] current = chunk;
      if (current == null) {
        return; // wide projections keep their existing descriptor evidence path
      }
      writeEntry(current, entries * (columns + 1), descriptor, columns);
      entries++;
      if (entries == CHUNK_LEAVES) {
        flush(storage, current);
      }
    }

    void finish(final ProjectionIndexHOTStorage storage, final int expectedPhysicalCount, final int expectedColumns,
        final int revision) {
      Objects.requireNonNull(storage, "storage");
      if (finished || physicalCount != expectedPhysicalCount || revision < 0
          || (columns >= 0 && columns != expectedColumns) || expectedColumns < 0) {
        throw new IllegalStateException("projection flag-summary build shape or revision mismatch");
      }
      if (expectedColumns <= MAX_COLUMNS) {
        if (entries > 0) {
          flush(storage, Objects.requireNonNull(chunk, "chunk"));
        }
        storage.putBlob(HEADER_SLOT, header(expectedPhysicalCount, expectedPhysicalCount, expectedColumns, revision));
      }
      finished = true;
    }

    private void flush(final ProjectionIndexHOTStorage storage, final byte[] current) {
      storage.putBlob(CHUNK_SLOT_BASE + chunksWritten, Arrays.copyOf(current, entries * (columns + 1)));
      chunksWritten++;
      entries = 0;
    }
  }

  /**
   * Patch only units containing changed physical slots. An absent header means an older index and
   * leaves its descriptor fallback intact. A malformed existing header fails the owning commit.
   */
  static void rewriteTouched(final ProjectionIndexHOTStorage storage, final ProjectionIndexFences.Accessor fences,
      final LongOpenHashSet changedSlots, final int columns, final int priorLiveCount, final int priorRevision,
      final int newRevision) {
    Objects.requireNonNull(storage, "storage");
    Objects.requireNonNull(fences, "fences");
    Objects.requireNonNull(changedSlots, "changedSlots");
    if (columns > MAX_COLUMNS || changedSlots.isEmpty()) {
      return;
    }
    final byte[] headerBytes = storage.getBlob(HEADER_SLOT);
    if (headerBytes == null) {
      return;
    }
    final Header prior = parseHeader(headerBytes);
    if (prior == null || prior.columns != columns || prior.liveCount != priorLiveCount
        || prior.revision != priorRevision || newRevision < priorRevision
        || prior.physicalCount > fences.physicalRowGroupCount()) {
      throw new IllegalStateException("projection flag-summary header disagrees with prior metadata");
    }
    final int newPhysicalCount = fences.physicalRowGroupCount();
    final int chunkCount = (newPhysicalCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
    final Int2ObjectOpenHashMap<byte[]> chunks = new Int2ObjectOpenHashMap<>(Math.min(changedSlots.size(), chunkCount));
    int liveDelta = 0;
    for (final LongIterator iterator = changedSlots.iterator(); iterator.hasNext();) {
      final long slot = iterator.nextLong();
      if (slot < 1 || slot > newPhysicalCount) {
        throw new IllegalStateException("changed projection flag-summary slot out of range: " + slot);
      }
      final int chunkId = (int) ((slot - 1) / CHUNK_LEAVES);
      byte[] chunk = chunks.get(chunkId);
      if (chunk == null) {
        final int first = chunkId * CHUNK_LEAVES;
        final int oldEntries = Math.max(0, Math.min(CHUNK_LEAVES, prior.physicalCount - first));
        final int newEntries = Math.min(CHUNK_LEAVES, newPhysicalCount - first);
        final byte[] old = oldEntries == 0
            ? null
            : storage.getBlob(CHUNK_SLOT_BASE + chunkId);
        if (oldEntries > 0 && (old == null || old.length != oldEntries * (columns + 1))) {
          throw new IllegalStateException("missing or malformed prior projection flag-summary chunk " + chunkId);
        }
        chunk = old == null
            ? new byte[newEntries * (columns + 1)]
            : Arrays.copyOf(old, newEntries * (columns + 1));
        chunks.put(chunkId, chunk);
      }
      final int offset = (int) ((slot - 1) % CHUNK_LEAVES) * (columns + 1);
      final boolean wasLive = chunk[offset] == LIVE;
      if (chunk[offset] != 0 && !wasLive) {
        throw new IllegalStateException("malformed projection flag-summary liveness at slot " + slot);
      }
      final boolean isLive = fences.isLivePhysicalSlot((int) slot);
      if (isLive) {
        final byte[] descriptor = storage.getVerifiedRowGroupDescriptor(slot);
        if (descriptor == null || RowGroupDescriptor.columnCount(descriptor) != columns) {
          throw new IllegalStateException("live projection flag-summary slot has no matching descriptor: " + slot);
        }
        writeEntry(chunk, offset, descriptor, columns);
      } else {
        Arrays.fill(chunk, offset, offset + columns + 1, (byte) 0);
      }
      liveDelta += (isLive
          ? 1
          : 0)
          - (wasLive
              ? 1
              : 0);
    }
    if (prior.liveCount + liveDelta != fences.liveRowGroupCount()) {
      throw new IllegalStateException("projection flag-summary changed-slot set misses a live leaf");
    }
    for (final Int2ObjectMap.Entry<byte[]> entry : chunks.int2ObjectEntrySet()) {
      storage.putBlob(CHUNK_SLOT_BASE + entry.getIntKey(), entry.getValue());
    }
    storage.putBlob(HEADER_SLOT, header(newPhysicalCount, fences.liveRowGroupCount(), columns, newRevision));
  }

  /** Return three-bit per-column evidence, or {@code null} so the descriptor gate runs instead. */
  static byte @Nullable [] readAll(final StorageEngineReader reader, final int indexNumber, final int expectedLiveCount,
      final int expectedColumns, final int expectedRevision) {
    Objects.requireNonNull(reader, "reader");
    if (expectedColumns < 0 || expectedColumns > MAX_COLUMNS || expectedLiveCount < 0) {
      return null;
    }
    try {
      final Header header = parseHeader(ProjectionIndexHOTStorage.readBlob(reader, indexNumber, HEADER_SLOT));
      if (header == null || header.columns != expectedColumns || header.liveCount != expectedLiveCount
          || header.revision != expectedRevision) {
        return null;
      }
      final byte[] evidence = new byte[expectedColumns];
      Arrays.fill(evidence, PURE_ALL);
      int liveCount = 0;
      final int width = expectedColumns + 1;
      for (int chunkId = 0; chunkId < (header.physicalCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES; chunkId++) {
        final int entries = Math.min(CHUNK_LEAVES, header.physicalCount - chunkId * CHUNK_LEAVES);
        final byte[] chunk = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, CHUNK_SLOT_BASE + chunkId);
        if (chunk == null || chunk.length != entries * width) {
          return null;
        }
        for (int leaf = 0; leaf < entries; leaf++) {
          final int offset = leaf * width;
          if (chunk[offset] == 0) {
            for (int column = 0; column < expectedColumns; column++) {
              if (chunk[offset + 1 + column] != 0) {
                return null;
              }
            }
            continue;
          }
          if (chunk[offset] != LIVE) {
            return null;
          }
          liveCount++;
          for (int column = 0; column < expectedColumns; column++) {
            final byte bits = chunk[offset + 1 + column];
            if ((bits & ~EVIDENCE_MASK) != 0) {
              return null;
            }
            evidence[column] |= bits & (UNREP_ANY | NONINT_ANY);
            if ((bits & PURE_ALL) == 0) {
              evidence[column] &= ~PURE_ALL;
            }
          }
        }
      }
      return liveCount == expectedLiveCount
          ? evidence
          : null;
    } catch (final IllegalStateException unavailable) {
      return null;
    }
  }

  private static void writeEntry(final byte[] chunk, final int offset, final byte[] descriptor, final int columns) {
    chunk[offset] = LIVE;
    for (int column = 0; column < columns; column++) {
      final int entry =
          RowGroupDescriptor.entryIndexOf(descriptor, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column));
      final byte flags = entry < 0
          ? ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE
          : RowGroupDescriptor.entryColFlags(descriptor, entry);
      byte bits = 0;
      if ((flags & ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE) != 0) {
        bits |= UNREP_ANY;
      }
      if ((flags & ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL) != 0) {
        bits |= NONINT_ANY;
      }
      if ((flags & ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE) != 0) {
        bits |= PURE_ALL;
      }
      chunk[offset + 1 + column] = bits;
    }
  }

  private static byte[] header(final int physicalCount, final int liveCount, final int columns, final int revision) {
    if (physicalCount < 0 || physicalCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS || liveCount < 0
        || liveCount > physicalCount || columns < 0 || columns > MAX_COLUMNS || revision < 0) {
      throw new IllegalArgumentException("invalid projection flag-summary header shape");
    }
    final byte[] bytes = new byte[HEADER_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MAGIC);
    bytes[4] = VERSION;
    bytes[5] = (byte) columns;
    bytes[6] = (byte) CHUNK_LEAVES;
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 8, physicalCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 12, liveCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 16, revision);
    return bytes;
  }

  private static @Nullable Header parseHeader(final byte @Nullable [] bytes) {
    if (bytes == null) {
      return null;
    }
    if (bytes.length != HEADER_BYTES || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MAGIC || bytes[4] != VERSION
        || bytes[6] != CHUNK_LEAVES || bytes[7] != 0 || Byte.toUnsignedInt(bytes[5]) > MAX_COLUMNS) {
      return null;
    }
    final int physicalCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, 8);
    final int liveCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, 12);
    final int revision = ProjectionIndexRowGroupCodec.getIntLE(bytes, 16);
    if (physicalCount < 0 || physicalCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS || liveCount < 0
        || liveCount > physicalCount || revision < 0) {
      return null;
    }
    return new Header(physicalCount, liveCount, Byte.toUnsignedInt(bytes[5]), revision);
  }

  private record Header(int physicalCount, int liveCount, int columns, int revision) {
  }
}

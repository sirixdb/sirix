/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.LongArrays;

import java.util.Arrays;
import java.util.Objects;

/**
 * Packed, in-memory sort run for one initial build of a partial sorted projection.
 *
 * <p>Matching keys are copied into grow-only byte blocks; one primitive long per row names its
 * block and offset. Sorting moves only those longs. This avoids one Java array object per matching
 * record and lets a leaf encode directly from the sorted run. The build releases the entire run after
 * the owning transaction has published its sorted directory.</p>
 */
final class ProjectionSortedRunAccumulator {

  private static final int MIN_BLOCK_BYTES = 1 << 16;
  private static final int MAX_BLOCK_BYTES = 8 << 20;
  private static final int MAX_KEY_BYTES = 0xFFFF;

  private byte[][] blocks = new byte[8][];
  private int blockCount;
  private int blockUsed;
  private long[] references = new long[4096];
  private int count;
  private boolean sorted;

  int rowCount() {
    return count;
  }

  void append(final byte[] key, final int length) {
    Objects.requireNonNull(key, "key");
    if (sorted) {
      throw new IllegalStateException("sorted projection run is already sorted");
    }
    if (length < 0 || length > key.length || length > MAX_KEY_BYTES) {
      throw new IllegalArgumentException("sorted projection key length is outside its page format");
    }
    if (count == Integer.MAX_VALUE) {
      throw new IllegalStateException("sorted projection run exhausted row references");
    }
    final int needed = length + Short.BYTES;
    if (blockCount == 0 || blocks[blockCount - 1].length - blockUsed < needed) {
      allocateBlock(needed);
    }
    final byte[] block = blocks[blockCount - 1];
    final int offset = blockUsed;
    block[offset] = (byte) length;
    block[offset + 1] = (byte) (length >>> 8);
    System.arraycopy(key, 0, block, offset + Short.BYTES, length);
    blockUsed += needed;
    if (count == references.length) {
      references = Arrays.copyOf(references, references.length <= Integer.MAX_VALUE / 2
          ? references.length << 1
          : Integer.MAX_VALUE);
    }
    references[count++] = ((long) (blockCount - 1) << Integer.SIZE) | (offset & 0xFFFF_FFFFL);
  }

  void sort() {
    if (sorted) {
      return;
    }
    LongArrays.quickSort(references, 0, count, this::compare);
    for (int i = 1; i < count; i++) {
      if (compare(references[i - 1], references[i]) >= 0) {
        throw new IllegalStateException("sorted projection run contains a duplicate row key");
      }
    }
    sorted = true;
  }

  /** Persist sorted data leaves and publish their sparse directory last. */
  int persist(final ProjectionIndexHOTStorage storage) {
    Objects.requireNonNull(storage, "storage");
    sort();
    final ProjectionSortedDirectory.Builder directory = new ProjectionSortedDirectory.Builder(storage);
    int from = 0;
    while (from < count) {
      int rows = Math.min(ProjectionSortedLeaf.MAX_ROWS, count - from);
      ProjectionSortedLeaf leaf;
      do {
        leaf = ProjectionSortedLeaf.encodeSortedRun(this, from, rows);
        if (leaf == null) {
          rows >>>= 1;
        }
      } while (leaf == null && rows > 0);
      if (leaf == null) {
        throw new IllegalStateException("sorted projection key exceeds a bounded data leaf");
      }
      directory.append(leaf);
      from += rows;
    }
    directory.finish();
    return count;
  }

  void release() {
    blocks = new byte[0][];
    references = new long[0];
    blockCount = 0;
    blockUsed = 0;
    count = 0;
    sorted = true;
  }

  long referenceAt(final int sortedPosition) {
    if (!sorted || sortedPosition < 0 || sortedPosition >= count) {
      throw new IllegalStateException("sorted projection run is not positioned on a sorted row");
    }
    return references[sortedPosition];
  }

  byte[] blockOf(final long reference) {
    return blocks[(int) (reference >>> Integer.SIZE)];
  }

  int keyOffset(final long reference) {
    return ((int) reference) + Short.BYTES;
  }

  int keyLength(final long reference) {
    final byte[] block = blockOf(reference);
    final int offset = (int) reference;
    return (block[offset] & 0xFF) | (block[offset + 1] & 0xFF) << 8;
  }

  private int compare(final long left, final long right) {
    final byte[] leftBlock = blockOf(left);
    final byte[] rightBlock = blockOf(right);
    final int leftOffset = keyOffset(left);
    final int rightOffset = keyOffset(right);
    return Arrays.compareUnsigned(leftBlock, leftOffset, leftOffset + keyLength(left),
        rightBlock, rightOffset, rightOffset + keyLength(right));
  }

  private void allocateBlock(final int needed) {
    if (blockCount == blocks.length) {
      blocks = Arrays.copyOf(blocks, blocks.length << 1);
    }
    final int previous = blockCount == 0 ? 0 : blocks[blockCount - 1].length;
    final int doubled = previous >= MAX_BLOCK_BYTES ? MAX_BLOCK_BYTES : previous << 1;
    blocks[blockCount++] = new byte[Math.max(needed, Math.max(MIN_BLOCK_BYTES, doubled))];
    blockUsed = 0;
  }
}

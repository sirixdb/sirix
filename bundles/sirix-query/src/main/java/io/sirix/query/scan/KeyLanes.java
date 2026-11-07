/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import static java.util.Objects.requireNonNull;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.node.SegmentAccess;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.jspecify.annotations.Nullable;

/**
 * The record keys of a masked selection as lanes: per kept leaf, {@code rows} longs at a 64-byte
 * aligned base of the selection's slab (the cache entry's reservation or the query-local slab),
 * eight bytes per row, read in place by the join and membership kernels exactly like a numeric lane
 * ({@link #segment(int)} and {@link #base(int)} hoisted per leaf, {@link SegmentAccess} at linear
 * offsets). A pruned leaf has no lane. Only when the allocator refused the slab do the lanes view
 * the decoded heap arrays as heap segments ({@link #onHeap}), the same fallback a numeric column
 * takes.
 *
 * <p>
 * The store decodes the KEYS chain to heap arrays (its own resident KEYS lane, or a per-selection
 * decode of the kept leaves); {@link #copyInto} moves the kept leaves' keys into the slab once, at
 * the selection's construction, and the arrays are not retained. Immutable once built, shared by a
 * cache entry and every view over it.
 */
final class KeyLanes {
  private static final long[] NO_KEYS = new long[0];

  private final MemorySegment @Nullable [] segments;
  private final long[] bases;
  private final int[] rows;
  private volatile long @Nullable [][] firstKeys;

  private KeyLanes(final MemorySegment @Nullable [] segments, final long[] bases, final int[] rows) {
    this.segments = segments;
    this.bases = bases;
    this.rows = rows;
  }

  /**
   * Lay the kept leaves' key lanes out behind {@code offset}: per kept leaf its byte base (64-byte
   * aligned, leaf order; a rowless leaf takes no bytes) into {@code basesOut}, {@code -1} for a
   * pruned leaf; returns the byte offset after the last lane.
   *
   * @param keepWords the leaf keep mask, or {@code null} for every leaf
   */
  static long layout(final ProjectionColumnStore store, final long @Nullable [] keepWords, final long[] basesOut,
      final long offset) {
    final int leaves = store.leafCount();
    if (basesOut.length != leaves) {
      throw new IllegalArgumentException("key lane bases for " + basesOut.length + " leaves, store has " + leaves);
    }
    if (offset < 0 || (offset & 63L) != 0L) {
      throw new IllegalArgumentException("key lanes start at a 64-byte aligned offset, not " + offset);
    }
    long cursor = offset;
    for (int leaf = 0; leaf < leaves; leaf++) {
      if (!kept(keepWords, leaf)) {
        basesOut[leaf] = -1L;
        continue;
      }
      basesOut[leaf] = cursor;
      cursor += (8L * Math.max(0, store.rowCount(leaf)) + 63L) & -64L;
    }
    return cursor;
  }

  private static boolean kept(final long @Nullable [] keepWords, final int leaf) {
    if (keepWords == null) {
      return true;
    }
    final int word = leaf >>> 6;
    return word < keepWords.length && (keepWords[word] & 1L << (leaf & 63)) != 0L;
  }

  /**
   * Copy every laid-out leaf's keys into {@code slab} at the bases of {@link #layout}; a leaf whose
   * base is {@code -1} gets no lane. {@code keys[leaf]} must hold exactly that leaf's rows.
   */
  static KeyLanes copyInto(final MemorySegment slab, final long[] bases, final long[][] keys,
      final ProjectionColumnStore store) {
    requireNonNull(slab, "slab");
    if (keys.length != bases.length) {
      throw new IllegalArgumentException("one key array and one base per leaf");
    }
    final MemorySegment[] segments = new MemorySegment[bases.length];
    final int[] rows = new int[bases.length];
    for (int leaf = 0; leaf < bases.length; leaf++) {
      final long base = bases[leaf];
      if (base < 0L) {
        continue;
      }
      final long[] leafKeys = requireNonNull(keys[leaf], "keys of a kept leaf");
      final int rowCount = Math.max(0, store.rowCount(leaf));
      if (leafKeys.length != rowCount) {
        throw new IllegalStateException(
            "leaf " + leaf + " decoded " + leafKeys.length + " record keys for " + rowCount + " rows");
      }
      if (base + 8L * rowCount > slab.byteSize()) {
        throw new IllegalArgumentException("key lane of leaf " + leaf + " outside the slab");
      }
      MemorySegment.copy(leafKeys, 0, slab, ValueLayout.JAVA_LONG_UNALIGNED, base, rowCount);
      segments[leaf] = slab;
      rows[leaf] = rowCount;
    }
    return new KeyLanes(segments, bases.clone(), rows);
  }

  /**
   * The allocator-refused fallback: the decoded heap arrays viewed as heap segments. A leaf without a
   * key array has no lane; a pruned or rowless leaf's empty array is an empty lane.
   */
  static KeyLanes onHeap(final long[][] keys) {
    final MemorySegment[] segments = new MemorySegment[keys.length];
    final long[] bases = new long[keys.length];
    final int[] rows = new int[keys.length];
    for (int leaf = 0; leaf < keys.length; leaf++) {
      final long[] leafKeys = keys[leaf];
      if (leafKeys == null) {
        bases[leaf] = -1L;
        continue;
      }
      segments[leaf] = MemorySegment.ofArray(leafKeys);
      rows[leaf] = leafKeys.length;
    }
    return new KeyLanes(segments, bases, rows);
  }

  int leafCount() {
    return bases.length;
  }

  /** Whether {@code leaf} has a key lane (a kept leaf; its lane may be empty). */
  boolean has(final int leaf) {
    return bases[leaf] >= 0L;
  }

  /** The segment holding {@code leaf}'s keys; kernels hoist it per leaf with {@link #base(int)}. */
  MemorySegment segment(final int leaf) {
    final MemorySegment segment = segments[leaf];
    if (segment == null) {
      throw new IllegalStateException("leaf " + leaf + " has no key lane");
    }
    return segment;
  }

  /** Byte offset of row 0 of {@code leaf}'s keys within {@link #segment(int)}. */
  long base(final int leaf) {
    final long base = bases[leaf];
    if (base < 0L) {
      throw new IllegalStateException("leaf " + leaf + " has no key lane");
    }
    return base;
  }

  /** Rows of {@code leaf}'s lane, {@code 0} without one. */
  int rows(final int leaf) {
    return rows[leaf];
  }

  /** The key of {@code row} on {@code leaf}; kernels hoist the segment and base instead. */
  long key(final int leaf, final int row) {
    if (row < 0 || row >= rows[leaf]) {
      throw new IndexOutOfBoundsException("row " + row + " of " + rows[leaf] + " on leaf " + leaf);
    }
    return SegmentAccess.getLong(segment(leaf), bases[leaf] + ((long) row << 3));
  }

  /**
   * Per leaf, its first record key as a one-element array (empty without a lane): the shape the
   * membership filter of the core index scan takes for its per-leaf identity, which reads only the
   * first key and the emptiness of each leaf. Built once, shared.
   */
  long[][] firstKeyArrays() {
    long[][] first = firstKeys;
    if (first == null) {
      first = new long[bases.length][];
      for (int leaf = 0; leaf < bases.length; leaf++) {
        first[leaf] = rows[leaf] == 0
            ? NO_KEYS
            : new long[] {key(leaf, 0)};
      }
      firstKeys = first;
    }
    return first;
  }
}

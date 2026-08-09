/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Accumulating bulk loader for a HOT secondary index that is being built from scratch.
 *
 * <p>Building an index over an already-shredded revision is a bulk load, not a sequence of
 * updates: every {@code (key, nodeKey)} pair is known before the first byte is written. Feeding
 * them through the incremental-insert path one at a time makes the trie pay that path's machinery
 * — a descent, a merge-vs-branch decision and, whenever a fold leaves a node malformed, a scoped
 * {@link HOTBulkBuilder} rebuild of the touched subtree — per entry. The rebuilds are what hurt:
 * each is {@code O(subtree)}, and a build of n entries triggers them in proportion to n, so index
 * construction grows super-linearly.</p>
 *
 * <p>This loader instead collects the pairs, sorts them once, folds the node keys that share a
 * chunk slot into one Roaring bitmap, and hands the result to {@link HOTBulkBuilder#build} — a
 * single {@code Θ(n)} construction whose output is invariant-clean by construction, so no
 * self-heal ever runs. The tree it produces is the tree the incremental path converges to.</p>
 *
 * <h2>Layout</h2>
 * <p>Keys live in one growable byte arena addressed by {@code (start, length)} pairs rather than
 * in a {@code byte[]} per entry: an index build over millions of nodes would otherwise pay a
 * 16-byte object header and a separate allocation for every key. Node keys live in a parallel
 * {@code long[]}. Sorting permutes an {@code int[]} of entry ids, never the payload.</p>
 *
 * <h2>Memory</h2>
 * <p>A bulk load is not streaming: the whole entry set is resident until {@link #flush()} hands it
 * to the builder, which is the price of a single-pass canonical construction. The arena layout
 * keeps that to roughly {@code n × (key bytes + 16)} while accumulating; {@code flush} then
 * materialises one {@link HOTBulkBuilder.Entry} per <em>distinct</em> chunk slot. The buffers are
 * dropped as soon as the tree is spliced.</p>
 *
 * <h2>Threading</h2>
 * <p>Not thread-safe; a loader belongs to the single traversal that feeds it.</p>
 *
 * <p>Subclasses supply the key encoding only — {@link HOTBulkIndexLoader} for object keys
 * (CAS, NAME), {@link HOTLongBulkIndexLoader} for the PATH index's primitive long keys, which
 * would otherwise box once per indexed node.</p>
 *
 * @author Johannes Lichtenberger
 */
abstract sealed class AbstractHOTBulkIndexLoader permits HOTBulkIndexLoader, HOTLongBulkIndexLoader {

  /** Initial arena capacity in bytes — grown by doubling. */
  private static final int INITIAL_ARENA_BYTES = 64 * 1024;

  /** Initial entry capacity — grown by doubling. */
  private static final int INITIAL_ENTRIES = 1024;

  /**
   * Upper bound on one serialized composite key. Mirrors {@link HOTIndexWriter}'s thread-local key
   * buffer: the largest CAS prefix (10-byte header + 246 value bytes) plus the 4-byte chunkIdx
   * trailer, rounded up for headroom. PATH's composite key is a fixed 12 bytes.
   */
  private static final int MAX_COMPOSITE_KEY_BYTES = 512;

  /** The index whose root this loader replaces on {@link #flush()}. */
  private final AbstractHOTIndexWriter<?> writer;

  /** Composite key bytes, back to back. */
  private byte[] arena = new byte[INITIAL_ARENA_BYTES];

  /** Bytes used in {@link #arena}. */
  private int arenaLength;

  /** Start offset in {@link #arena} of entry {@code i}. */
  private int[] keyStart = new int[INITIAL_ENTRIES];

  /** Length in {@link #arena} of entry {@code i}'s composite key. */
  private int[] keyLength = new int[INITIAL_ENTRIES];

  /** The node key of entry {@code i}. */
  private long[] nodeKeys = new long[INITIAL_ENTRIES];

  /** Number of accumulated entries. */
  private int count;

  /** Set once {@link #flush()} has run, so a second flush cannot re-splice the tree. */
  private boolean flushed;

  AbstractHOTBulkIndexLoader(final AbstractHOTIndexWriter<?> writer) {
    this.writer = requireNonNull(writer, "writer");
  }

  /**
   * Make room for one more composite key and hand back the arena to serialize into. The write
   * must start at {@link #arenaOffset()} and be committed with {@link #commitKey(int, long)}.
   *
   * @param nodeKey the node key the key being written belongs to; validated here so a subclass
   *        never serializes a key it would have to roll back
   * @return the arena, guaranteed to have {@link #MAX_COMPOSITE_KEY_BYTES} free from
   *         {@link #arenaOffset()}
   */
  final byte[] reserveKeySpace(final long nodeKey) {
    if (flushed) {
      throw new IllegalStateException("Bulk loader already flushed");
    }
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);

    if (arenaLength + MAX_COMPOSITE_KEY_BYTES > arena.length) {
      arena = Arrays.copyOf(arena, Math.max(arena.length << 1, arenaLength + MAX_COMPOSITE_KEY_BYTES));
    }
    if (count == keyStart.length) {
      final int newCapacity = keyStart.length << 1;
      keyStart = Arrays.copyOf(keyStart, newCapacity);
      keyLength = Arrays.copyOf(keyLength, newCapacity);
      nodeKeys = Arrays.copyOf(nodeKeys, newCapacity);
    }
    return arena;
  }

  /** Offset in the reserved arena at which the next composite key must be written. */
  final int arenaOffset() {
    return arenaLength;
  }

  /** Record the key just written at {@link #arenaOffset()} as belonging to {@code nodeKey}. */
  final void commitKey(final int length, final long nodeKey) {
    keyStart[count] = arenaLength;
    keyLength[count] = length;
    nodeKeys[count] = nodeKey;
    arenaLength += length;
    count++;
  }

  /** Number of {@code (key, nodeKey)} pairs accumulated so far. */
  public final int size() {
    return count;
  }

  /**
   * Build the trie from everything accumulated and splice it in as the index's root.
   *
   * <p>A no-op when nothing was accumulated — the empty tree the index was initialized with stays
   * in place. After this call the loader is spent.</p>
   */
  public final void flush() {
    if (flushed) {
      throw new IllegalStateException("Bulk loader already flushed");
    }
    flushed = true;
    if (count == 0) {
      return;
    }

    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      order[i] = i;
    }
    // Composite key first, node key second. Grouping below relies on equal keys being adjacent,
    // and the node-key tiebreak makes this a total order, so the permutation is deterministic
    // even though quicksort is not stable. The comparator only reads, so sorting in parallel is
    // safe; fastutil runs small inputs sequentially anyway.
    final IntComparator byKeyThenNodeKey = (a, b) -> {
      final int cmp = Arrays.compareUnsigned(arena, keyStart[a], keyStart[a] + keyLength[a], arena,
          keyStart[b], keyStart[b] + keyLength[b]);
      return cmp != 0 ? cmp : Long.compare(nodeKeys[a], nodeKeys[b]);
    };
    IntArrays.parallelQuickSort(order, byKeyThenNodeKey);

    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>();
    int groupStart = 0;
    while (groupStart < count) {
      int groupEnd = groupStart + 1;
      while (groupEnd < count && sameKey(order[groupStart], order[groupEnd])) {
        groupEnd++;
      }
      entries.add(toEntry(order, groupStart, groupEnd));
      groupStart = groupEnd;
    }

    writer.spliceBulkBuiltRoot(entries);
    releaseBuffers();
  }

  private boolean sameKey(final int a, final int b) {
    return keyLength[a] == keyLength[b] && Arrays.compareUnsigned(arena, keyStart[a],
        keyStart[a] + keyLength[a], arena, keyStart[b], keyStart[b] + keyLength[b]) == 0;
  }

  /**
   * Fold {@code order[from..to)} — all entries sharing one composite key — into a single
   * {@link HOTBulkBuilder.Entry}. A chunk slot stores the low 16 bits of each node key; the high
   * bits are already carried by the key's chunkIdx trailer.
   */
  private HOTBulkBuilder.Entry toEntry(final int[] order, final int from, final int to) {
    final int representative = order[from];
    final byte[] key =
        Arrays.copyOfRange(arena, keyStart[representative], keyStart[representative] + keyLength[representative]);

    final Roaring64Bitmap bits = new Roaring64Bitmap();
    for (int i = from; i < to; i++) {
      bits.add(nodeKeys[order[i]] & 0xFFFFL);
    }
    bits.runOptimize();
    return new HOTBulkBuilder.Entry(key, NodeReferencesSerializer.serialize(NodeReferences.owning(bits)));
  }

  /** Drop the accumulation buffers — a spent loader must not pin an index-sized arena. */
  private void releaseBuffers() {
    arena = new byte[0];
    keyStart = new int[0];
    keyLength = new int[0];
    nodeKeys = new long[0];
  }
}

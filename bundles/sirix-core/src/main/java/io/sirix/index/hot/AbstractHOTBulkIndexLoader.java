/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

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
 * <p>
 * Building an index over an already-shredded revision is a bulk load, not a sequence of updates:
 * every {@code (key, nodeKey)} pair is known before the first byte is written. Feeding them through
 * the incremental-insert path one at a time makes the trie pay that path's machinery — a descent, a
 * merge-vs-branch decision and, whenever a fold leaves a node malformed, a scoped
 * {@link HOTBulkBuilder} rebuild of the touched subtree — per entry. The rebuilds are what hurt:
 * each is {@code O(subtree)}, and a build of n entries triggers them in proportion to n, so index
 * construction grows super-linearly.
 * </p>
 *
 * <p>
 * This loader instead collects the pairs, sorts them once, folds the node keys that share a chunk
 * slot into one payload, and hands the result to {@link HOTBulkBuilder#build} — a single
 * {@code Θ(n)} construction whose output is invariant-clean by construction, so no self-heal ever
 * runs. The tree it produces is the tree the incremental path converges to.
 * </p>
 *
 * <h2>Layout</h2>
 * <p>
 * Composite keys live in a chain of fixed-size byte blocks, addressed by a packed
 * {@code (block, offset)} {@code long} plus a length; node keys live in a parallel {@code long[]}.
 * Neither a {@code byte[]} per key (a 16-byte header and a separate allocation per indexed node)
 * nor one growable arena (whose doubling copies a hundreds-of-megabytes array, needs twice that
 * live at the moment of growth, and cannot address past {@code Integer.MAX_VALUE} at all). Sorting
 * permutes an {@code int[]} of entry ids, never the payload.
 * </p>
 *
 * <h2>Allocation</h2>
 * <p>
 * {@link #reserveKeySpace}/{@link #commitKey} allocate nothing per entry — only a fresh block every
 * {@value #BLOCK_BYTES} bytes and an amortized index-array growth. {@link #flush()} then allocates
 * exactly what {@link HOTBulkBuilder}'s input contract requires: one key array, one payload array
 * and one {@link HOTBulkBuilder.Entry} per <em>distinct</em> chunk slot, in an exactly pre-sized
 * list. The per-slot node-key run is gathered into a reusable buffer and serialized straight from
 * it, so no bitmap is built below the packed threshold.
 * </p>
 *
 * <h2>Memory</h2>
 * <p>
 * A bulk load is not streaming: the whole entry set is resident until {@link #flush()} hands it to
 * the builder, which is the price of a single-pass canonical construction. That comes to roughly
 * {@code n × (key bytes + 20)}. The buffers are dropped as soon as the tree is spliced.
 * </p>
 *
 * <h2>Threading</h2>
 * <p>
 * Not thread-safe; a loader belongs to the single traversal that feeds it.
 * </p>
 *
 * <p>
 * Subclasses supply the key encoding only — {@link HOTBulkIndexLoader} for object keys (CAS, NAME),
 * {@link HOTLongBulkIndexLoader} for the PATH index's primitive long keys, which would otherwise
 * box once per indexed node.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
abstract sealed class AbstractHOTBulkIndexLoader permits HOTBulkIndexLoader, HOTLongBulkIndexLoader {

  /** Size of one key block. A power of two so the packed position splits with shift and mask. */
  private static final int BLOCK_BYTES = 1 << 20;

  /** Bits of a packed position that address within a block. */
  private static final int BLOCK_SHIFT = 20;

  /** Mask of the in-block offset of a packed position. */
  private static final int BLOCK_MASK = BLOCK_BYTES - 1;

  /** Initial entry capacity — grown by doubling. */
  private static final int INITIAL_ENTRIES = 1024;

  /** The index whose root this loader replaces on {@link #flush()}. */
  private final AbstractHOTIndexWriter<?> writer;

  /**
   * Key blocks; a key never straddles two of them. A plain array rather than a {@code List} because
   * {@link #compareKeys} runs in the sort's inner loop.
   */
  private byte[][] blocks = new byte[8][];

  /** Number of allocated entries in {@link #blocks}. */
  private int blockCount;

  /** The block {@link #reserveKeySpace} currently writes into. */
  private byte[] currentBlock;

  /** Index of {@link #currentBlock} in {@link #blocks}. */
  private int currentBlockIndex;

  /** Bytes used in {@link #currentBlock}. */
  private int currentBlockOffset;

  /** Packed {@code (blockIndex << BLOCK_SHIFT) | offset} of entry {@code i}'s composite key. */
  private long[] keyPos = new long[INITIAL_ENTRIES];

  /** Length of entry {@code i}'s composite key. */
  private int[] keyLength = new int[INITIAL_ENTRIES];

  /** The node key of entry {@code i}. */
  private long[] nodeKeys = new long[INITIAL_ENTRIES];

  /** Number of accumulated entries. */
  private int count;

  /** Bytes {@link #reserveKeySpace} last promised the caller, and {@link #commitKey} holds it to. */
  private int reservedKeyBytes;

  /** Set once {@link #flush()} has run, so a second flush cannot re-splice the tree. */
  private boolean flushed;

  AbstractHOTBulkIndexLoader(final AbstractHOTIndexWriter<?> writer) {
    this.writer = requireNonNull(writer, "writer");
    currentBlock = new byte[BLOCK_BYTES];
    blocks[blockCount++] = currentBlock;
  }

  /**
   * Make room for one more composite key and hand back the block to serialize into. The write must
   * start at {@link #blockOffset()} and be committed with {@link #commitKey(int, long)}.
   *
   * @param nodeKey the node key the key being written belongs to; validated here so a subclass never
   *        serializes a key it would have to roll back
   * @param maxKeyBytes the caller's own upper bound on what it is about to write. Taken BEFORE the
   *        write, because the length a serializer <em>returns</em> arrives too late — by then a key
   *        larger than the room available has already been written past it
   * @return the current block, guaranteed to have {@code maxKeyBytes} free from
   *         {@link #blockOffset()}
   * @throws IllegalStateException if a single key cannot fit a block at all
   */
  final byte[] reserveKeySpace(final long nodeKey, final int maxKeyBytes) {
    if (flushed) {
      throw new IllegalStateException("Bulk loader already flushed");
    }
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);
    if (maxKeyBytes <= 0 || maxKeyBytes > BLOCK_BYTES) {
      throw new IllegalStateException(
          "Composite index key of up to " + maxKeyBytes + " bytes does not fit a " + BLOCK_BYTES + "-byte key block");
    }
    reservedKeyBytes = maxKeyBytes;

    if (currentBlockOffset + maxKeyBytes > BLOCK_BYTES) {
      if (blockCount == blocks.length) {
        blocks = Arrays.copyOf(blocks, blocks.length << 1);
      }
      currentBlock = new byte[BLOCK_BYTES];
      currentBlockIndex = blockCount;
      blocks[blockCount++] = currentBlock;
      currentBlockOffset = 0;
    }
    if (count == keyLength.length) {
      final int newCapacity = keyLength.length << 1;
      if (newCapacity < 0) {
        throw new IllegalStateException("Bulk index build exceeds " + keyLength.length + " entries");
      }
      keyPos = Arrays.copyOf(keyPos, newCapacity);
      keyLength = Arrays.copyOf(keyLength, newCapacity);
      nodeKeys = Arrays.copyOf(nodeKeys, newCapacity);
    }
    return currentBlock;
  }

  /** Offset in the reserved block at which the next composite key must be written. */
  final int blockOffset() {
    return currentBlockOffset;
  }

  /**
   * Record the key just written at {@link #blockOffset()} as belonging to {@code nodeKey}.
   *
   * <p>
   * Checked, not asserted: {@link #reserveKeySpace} guarantees only what the caller asked for, so a
   * serializer that under-reported its own bound has written into whatever followed the reservation
   * and must not be allowed to look like it succeeded.
   * </p>
   *
   * @param length bytes the subclass wrote
   * @param nodeKey the node key the written composite key belongs to
   * @throws IllegalStateException if {@code length} is outside the reservation
   */
  final void commitKey(final int length, final long nodeKey) {
    if (length <= 0 || length > reservedKeyBytes) {
      throw new IllegalStateException("Composite index key of " + length + " bytes overran the " + reservedKeyBytes
          + " bytes its serializer reserved");
    }
    keyPos[count] = ((long) currentBlockIndex << BLOCK_SHIFT) | currentBlockOffset;
    keyLength[count] = length;
    nodeKeys[count] = nodeKey;
    currentBlockOffset += length;
    count++;
  }

  /** Number of {@code (key, nodeKey)} pairs accumulated so far. */
  public final int size() {
    return count;
  }

  /**
   * Build the trie from everything accumulated and splice it in as the index's root.
   *
   * <p>
   * A no-op when nothing was accumulated — the empty tree the index was initialized with stays in
   * place. After this call the loader is spent.
   * </p>
   */
  public final void flush() {
    if (flushed) {
      throw new IllegalStateException("Bulk loader already flushed");
    }
    flushed = true;
    if (count == 0) {
      releaseBuffers();
      return;
    }

    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      order[i] = i;
    }
    // Composite key first, node key second. Grouping below relies on equal keys being adjacent and
    // on each group's node keys being ascending, which is also what the packed payload wants. The
    // node-key tiebreak makes this a total order, so the permutation is deterministic even though
    // quicksort is not stable. The comparator only reads, so sorting in parallel is safe; fastutil
    // runs small inputs sequentially anyway.
    final IntComparator byKeyThenNodeKey = (a, b) -> {
      final int cmp = compareKeys(a, b);
      return cmp != 0
          ? cmp
          : Long.compare(nodeKeys[a], nodeKeys[b]);
    };
    IntArrays.parallelQuickSort(order, byKeyThenNodeKey);

    // One counting pass so the entry list is allocated at its exact size rather than doubled into
    // place — at index scale those copies move hundreds of megabytes of references.
    int distinct = 1;
    for (int i = 1; i < count; i++) {
      if (compareKeys(order[i - 1], order[i]) != 0) {
        distinct++;
      }
    }

    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(distinct);
    final Roaring64Bitmap payloadScratch = new Roaring64Bitmap();
    long[] runScratch = new long[64];
    int groupStart = 0;
    while (groupStart < count) {
      int groupEnd = groupStart + 1;
      while (groupEnd < count && compareKeys(order[groupStart], order[groupEnd]) == 0) {
        groupEnd++;
      }
      if (groupEnd - groupStart > runScratch.length) {
        runScratch = new long[Math.max(runScratch.length << 1, groupEnd - groupStart)];
      }
      entries.add(toEntry(order, groupStart, groupEnd, runScratch, payloadScratch));
      groupStart = groupEnd;
    }

    writer.spliceBulkBuiltRoot(entries);
    releaseBuffers();
  }

  /** Unsigned byte comparison of the composite keys of entries {@code a} and {@code b}. */
  private int compareKeys(final int a, final int b) {
    final long posA = keyPos[a];
    final long posB = keyPos[b];
    final byte[] blockA = blocks[(int) (posA >>> BLOCK_SHIFT)];
    final byte[] blockB = blocks[(int) (posB >>> BLOCK_SHIFT)];
    final int offA = (int) (posA & BLOCK_MASK);
    final int offB = (int) (posB & BLOCK_MASK);
    return Arrays.compareUnsigned(blockA, offA, offA + keyLength[a], blockB, offB, offB + keyLength[b]);
  }

  /**
   * Fold {@code order[from..to)} — all entries sharing one composite key — into a single
   * {@link HOTBulkBuilder.Entry}. A chunk slot stores the low 16 bits of each node key; the high bits
   * are already carried by the key's chunkIdx trailer.
   *
   * <p>
   * The run is ascending by node key and may repeat one (a visitor is free to offer the same node
   * under the same key twice — {@code JsonPathIndexBuilder} does for a fused named array), so equal
   * neighbours collapse as the run is gathered. That also satisfies {@link HOTBulkBuilder}'s
   * distinct-key precondition, since a repeated pair would otherwise become a second entry with an
   * identical key.
   * </p>
   */
  private HOTBulkBuilder.Entry toEntry(final int[] order, final int from, final int to, final long[] runScratch,
      final Roaring64Bitmap payloadScratch) {
    final int representative = order[from];
    final long pos = keyPos[representative];
    final byte[] block = blocks[(int) (pos >>> BLOCK_SHIFT)];
    final int offset = (int) (pos & BLOCK_MASK);
    final byte[] key = Arrays.copyOfRange(block, offset, offset + keyLength[representative]);

    int run = 0;
    for (int i = from; i < to; i++) {
      final long bit16 = nodeKeys[order[i]] & 0xFFFFL;
      if (run == 0 || runScratch[run - 1] != bit16) {
        runScratch[run++] = bit16;
      }
    }
    return new HOTBulkBuilder.Entry(key,
        NodeReferencesSerializer.serializeAscendingRun(runScratch, 0, run, payloadScratch));
  }

  /** Drop the accumulation buffers — a spent loader must not pin an index-sized key arena. */
  private void releaseBuffers() {
    blocks = new byte[0][];
    blockCount = 0;
    currentBlock = null;
    currentBlockIndex = 0;
    currentBlockOffset = 0;
    keyPos = new long[0];
    keyLength = new int[0];
    nodeKeys = new long[0];
  }
}

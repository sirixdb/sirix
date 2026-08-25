/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Accumulating bulk loader for HOT <b>slot stores</b> — writers whose entries are opaque
 * {@code byte[]} payloads under 8-byte keys with REPLACE (last-writer-wins) semantics, such as the
 * projection index's {@code ProjectionIndexHOTStorage} (order-label directory, column-segment
 * slots, fence/Bloom/metadata blobs).
 *
 * <p>
 * Shares the {@link AbstractHOTBulkIndexLoader} pattern — block-arena accumulation, one
 * permutation sort, an exactly-sized entry list, one {@code spliceBulkBuiltRoot} — but is a
 * separate class in the same family because that loader is postings-shaped (it carries a
 * {@code nodeKey} per entry and its fold OR-merges chunked {@code NodeReferences} runs), it is
 * {@code sealed}, and slot semantics need a different fold: keep the LAST payload written per key.
 * Measured against the per-entry {@code writeSlotValue} path this construction is 9–14× faster at
 * 1 M–10 M entries ({@code docs/HOT_BULK_BUILD.md} §Seam 2a).
 *
 * <h2>Read-through</h2>
 * <p>
 * While a loader is active its owner serves point reads of accumulated keys from
 * {@link #lastPayload}: the owner engages the loader only on a VIRGIN tree and routes every write
 * through {@link #tryAdd}, so the accumulator and the (empty) tree partition the key space — a key
 * is in the map or it was never written. {@link #containsKey} lets the owner detect the one
 * operation that genuinely needs physical pages (a side-reference attach) and splice first.
 * </p>
 *
 * <h2>Capacity</h2>
 * <p>
 * Bounded by entry count and arena bytes ({@code docs/HOT_BULK_BUILD.md} §Seam 2a arithmetic: the
 * built pages cost ≈64 KiB per 512 entries ON TOP of the arena, and none of them are
 * spill-eligible until the splice registers them). {@link #tryAdd} returns {@code false} at
 * capacity; the owner splices the accumulated prefix and continues per-entry.
 * </p>
 *
 * <h2>Threading</h2>
 * <p>
 * Not thread-safe; a loader belongs to the single writer that owns its index tree.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTBulkSlotLoader {

  /** Size of one payload block. A power of two so the packed position splits with shift/mask. */
  private static final int BLOCK_BYTES = 1 << 20;

  private static final int BLOCK_SHIFT = 20;

  private static final int BLOCK_MASK = BLOCK_BYTES - 1;

  private static final int INITIAL_ENTRIES = 1024;

  /** Largest payload a HOT leaf slot can hold ({@code HOTLeafPage} value-length limit). */
  private static final int MAX_PAYLOAD_BYTES = 65_535;

  /** Hard entry-count cap. */
  private final int maxEntries;

  /** Hard arena-byte cap (payload bytes; the fixed per-entry index is amortized on top). */
  private final long maxArenaBytes;

  /** Payload blocks; a payload never straddles two of them. */
  private byte[][] blocks = new byte[8][];

  private int blockCount;

  private byte[] currentBlock;

  private int currentBlockIndex;

  private int currentBlockOffset;

  /** The slot key of entry {@code i}. */
  private long[] keys = new long[INITIAL_ENTRIES];

  /** Packed {@code (blockIndex << BLOCK_SHIFT) | offset} of entry {@code i}'s payload. */
  private long[] payloadPos = new long[INITIAL_ENTRIES];

  /** Length of entry {@code i}'s payload. */
  private int[] payloadLen = new int[INITIAL_ENTRIES];

  /** Number of accumulated entries (re-puts of one key append — the fold keeps the last). */
  private int count;

  /** Total payload bytes accumulated. */
  private long arenaBytes;

  /** slot key → ordinal of the LAST write of that key; also the membership set. */
  private final Long2IntOpenHashMap lastOrdinalByKey = new Long2IntOpenHashMap();

  public HOTBulkSlotLoader(final int maxEntries, final long maxArenaBytes) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be positive, got " + maxEntries);
    }
    if (maxArenaBytes <= 0) {
      throw new IllegalArgumentException("maxArenaBytes must be positive, got " + maxArenaBytes);
    }
    this.maxEntries = maxEntries;
    this.maxArenaBytes = maxArenaBytes;
    this.lastOrdinalByKey.defaultReturnValue(-1);
    this.currentBlock = new byte[BLOCK_BYTES];
    this.blocks[blockCount++] = currentBlock;
  }

  /**
   * Accumulate one slot write. A zero-length payload is a tombstone value and is kept like any
   * other (the projection's slot stores treat zero-length as "tombstoned").
   *
   * @param slotKey the slot key
   * @param payload the payload; copied, never retained
   * @return {@code true} when accumulated; {@code false} when the write does not fit this
   *         loader's contract (capacity reached, or an over-long payload) — the caller must
   *         splice and fall through to its per-entry path
   */
  public boolean tryAdd(final long slotKey, final byte[] payload) {
    requireNonNull(payload, "payload");
    if (count >= maxEntries || payload.length > MAX_PAYLOAD_BYTES
        || arenaBytes + payload.length > maxArenaBytes) {
      return false;
    }
    if (currentBlockOffset + payload.length > BLOCK_BYTES) {
      if (blockCount == blocks.length) {
        blocks = Arrays.copyOf(blocks, blocks.length << 1);
      }
      currentBlock = new byte[BLOCK_BYTES];
      currentBlockIndex = blockCount;
      blocks[blockCount++] = currentBlock;
      currentBlockOffset = 0;
    }
    if (count == keys.length) {
      final int newCapacity = keys.length << 1;
      keys = Arrays.copyOf(keys, newCapacity);
      payloadPos = Arrays.copyOf(payloadPos, newCapacity);
      payloadLen = Arrays.copyOf(payloadLen, newCapacity);
    }
    System.arraycopy(payload, 0, currentBlock, currentBlockOffset, payload.length);
    keys[count] = slotKey;
    payloadPos[count] = ((long) currentBlockIndex << BLOCK_SHIFT) | currentBlockOffset;
    payloadLen[count] = payload.length;
    lastOrdinalByKey.put(slotKey, count);
    currentBlockOffset += payload.length;
    arenaBytes += payload.length;
    count++;
    return true;
  }

  /** Whether {@code slotKey} has been written since this loader was engaged. */
  public boolean containsKey(final long slotKey) {
    return lastOrdinalByKey.containsKey(slotKey);
  }

  /**
   * The LAST payload written for {@code slotKey}, as a fresh array, or {@code null} when the key
   * was never written into this loader. A zero-length result is a tombstoned slot — exactly what
   * the per-entry read path returns for one.
   */
  public byte @Nullable [] lastPayload(final long slotKey) {
    final int ordinal = lastOrdinalByKey.get(slotKey);
    if (ordinal < 0) {
      return null;
    }
    final long pos = payloadPos[ordinal];
    final byte[] block = blocks[(int) (pos >>> BLOCK_SHIFT)];
    final int offset = (int) (pos & BLOCK_MASK);
    return Arrays.copyOfRange(block, offset, offset + payloadLen[ordinal]);
  }

  /** Number of accumulated writes (not distinct keys). */
  public int size() {
    return count;
  }

  /** Whether nothing has been accumulated. */
  public boolean isEmpty() {
    return count == 0;
  }

  /**
   * Fold to last-writer-wins, sort by key, build the canonical tree and splice it as
   * {@code writer}'s root — the production {@code spliceBulkBuiltRoot} path (empty-tree guard,
   * {@link HOTBulkBuilder}, fresh-subtree TIL registration). The loader is spent afterwards.
   *
   * @param writer the owning writer; its tree must still be empty
   * @return the number of DISTINCT entries spliced (0 for an empty loader)
   */
  public int spliceInto(final AbstractHOTIndexWriter<?> writer) {
    requireNonNull(writer, "writer");
    final int distinct = lastOrdinalByKey.size();
    if (distinct == 0) {
      releaseBuffers();
      return 0;
    }

    // The winning ordinals, sorted by slot key. Keys are sorted SIGNED: PathKeySerializer's
    // sign-flipped big-endian encoding makes unsigned byte order equal signed long order.
    final int[] winners = new int[distinct];
    int w = 0;
    for (final int ordinal : lastOrdinalByKey.values()) {
      winners[w++] = ordinal;
    }
    final IntComparator byKey = (a, b) -> Long.compare(keys[a], keys[b]);
    IntArrays.parallelQuickSort(winners, byKey);

    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(distinct);
    for (int i = 0; i < distinct; i++) {
      final int ordinal = winners[i];
      final byte[] keyBytes = new byte[8];
      PathKeySerializer.INSTANCE.serialize(keys[ordinal], keyBytes, 0);
      final long pos = payloadPos[ordinal];
      final byte[] block = blocks[(int) (pos >>> BLOCK_SHIFT)];
      final int offset = (int) (pos & BLOCK_MASK);
      entries.add(new HOTBulkBuilder.Entry(keyBytes,
          Arrays.copyOfRange(block, offset, offset + payloadLen[ordinal])));
    }
    writer.spliceBulkBuiltRoot(entries);
    releaseBuffers();
    return distinct;
  }

  /** Discard everything — used when the owning tree is reset mid-accumulation. */
  public void clear() {
    releaseBuffers();
  }

  private void releaseBuffers() {
    blocks = new byte[0][];
    blockCount = 0;
    currentBlock = null;
    currentBlockIndex = 0;
    currentBlockOffset = 0;
    keys = new long[0];
    payloadPos = new long[0];
    payloadLen = new int[0];
    count = 0;
    arenaBytes = 0;
    lastOrdinalByKey.clear();
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

/**
 * Chunked, copy-on-write-deduplicated store for the projection's per-leaf
 * record-key <b>fences</b> — the {@code (firstRecordKey, lastRecordKey)} zone
 * map that incremental maintenance ({@link ProjectionIndexChangeListener})
 * reads once per commit to locate the leaves a write touched.
 *
 * <h2>Why not keep the fences in the slot-0 metadata blob?</h2>
 *
 * <p>Every maintenance commit changes at least one leaf's range, which changes
 * the fence bytes, which — when all fences live inside the single slot-0
 * {@code PIXM} blob — rewrites the WHOLE array. At scale that array is large
 * (16 bytes per leaf: two little-endian longs), so a store with ~100k leaves
 * re-persisted ~1.5&nbsp;MB of fences on <i>every</i> commit, and SirixDB's
 * copy-on-write history keeps each rewrite forever. That is ~1.5&nbsp;MB of
 * permanent growth per commit for data that barely moved.
 *
 * <h2>The fix: split the fences into fixed-size chunks</h2>
 *
 * <p>The fences are cut into fixed spans of {@link #CHUNK_LEAVES} leaves, and
 * each chunk is stored as its own blob under a reserved slot key
 * ({@link #CHUNK_SLOT_BASE}&nbsp;+&nbsp;chunkIndex, far above the data leaves'
 * slots so the two never collide). Writing a chunk goes through
 * {@link ProjectionIndexHOTStorage#putBlob}, which already <b>carries an
 * unchanged blob forward by reference</b> (same length + content hash → no page
 * write) — the exact deduplication the hot trie and the segment directory use.
 *
 * <p>So a commit that touches a handful of leaves rewrites only the one or two
 * chunks those leaves fall in (plus the tail chunk for appends); every other
 * chunk is a byte-for-byte match and costs nothing. Per-commit growth drops
 * from the full fence array to a few chunks (a chunk is
 * {@code CHUNK_LEAVES × 16} bytes). Reads are unchanged in cost: maintenance
 * still loads every fence, just from several small chunks instead of one big
 * blob.
 *
 * <p>Fences are a <b>writer-only</b> zone map — the builder writes them, the
 * change listener reads and rewrites them, and nothing on the query/hydrate
 * path ever touches them. Losing or mis-sizing a chunk therefore never returns
 * a wrong answer; {@link #read} reports the damage as {@code null} and the
 * caller falls back to a full rebuild.
 */
public final class ProjectionIndexFences {

  /**
   * First reserved slot key for fence chunks. Data-leaf slots run 1..leafCount
   * (bounded far below 2^24 by {@code MAX_PROBED_LEAVES}) and slot 0 is the
   * metadata; 2^40 sits above every leaf slot, so leaf probing (which stops at
   * the first empty slot after the leaves) never reaches the fence chunks and
   * the two key ranges cannot alias. Chunk {@code c} lives at
   * {@code CHUNK_SLOT_BASE + c}.
   */
  static final long CHUNK_SLOT_BASE = 1L << 40;

  /**
   * Leaves per fence chunk. A full chunk is {@code CHUNK_LEAVES × 16} bytes
   * (512 → 8&nbsp;KiB): small enough that a commit touching a few leaves
   * re-persists little, large enough that the chunk count (and thus the number
   * of carry-forward probes per commit) stays modest even at 100M rows
   * (~200 chunks).
   */
  static final int CHUNK_LEAVES = 512;

  /** Bytes per leaf entry: firstRecordKey + lastRecordKey, both little-endian longs. */
  private static final int ENTRY_BYTES = 16;

  private ProjectionIndexFences() {
  }

  /** Number of chunks needed to cover {@code leafCount} leaves. */
  public static int chunkCount(final int leafCount) {
    return (leafCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
  }

  /**
   * Persist the per-leaf fences as carry-forward chunks. {@code first}/{@code last}
   * are 0-based and index-aligned with leaf slots 1..{@code leafCount} (entry
   * {@code i} describes slot {@code i + 1}). Chunks whose bytes match the prior
   * revision are no-ops (shared by reference); chunks that no longer exist
   * because the leaf count shrank (a rebuild) are tombstoned.
   *
   * @param priorLeafCount leaf count of the snapshot being replaced, so orphaned
   *                       trailing chunks can be dropped; pass {@code 0} when there
   *                       is nothing to reclaim
   */
  public static void write(final ProjectionIndexHOTStorage storage, final int leafCount,
      final long[] first, final long[] last, final int priorLeafCount) {
    // Exactly one entry per leaf (the invariant the old inline-fence metadata constructor
    // enforced): a shorter array reads out of bounds, a longer one carries stale trailing
    // entries beyond leafCount that read() would silently ignore.
    if (first.length != leafCount || last.length != leafCount) {
      throw new IllegalArgumentException("fence arrays must carry exactly leafCount " + leafCount
          + " entries, got " + first.length + "/" + last.length);
    }
    final int chunks = chunkCount(leafCount);
    for (int c = 0; c < chunks; c++) {
      final int start = c * CHUNK_LEAVES;
      final int end = Math.min(start + CHUNK_LEAVES, leafCount);
      final byte[] bytes = new byte[(end - start) * ENTRY_BYTES];
      int off = 0;
      for (int i = start; i < end; i++) {
        LeafDescriptor.putLongLE(bytes, off, first[i]);
        LeafDescriptor.putLongLE(bytes, off + 8, last[i]);
        off += ENTRY_BYTES;
      }
      storage.putBlob(CHUNK_SLOT_BASE + c, bytes);
    }
    // Reclaim chunks the shrunk (rebuilt) projection no longer covers.
    for (int c = chunks; c < chunkCount(priorLeafCount); c++) {
      storage.tombstoneLeaf(CHUNK_SLOT_BASE + c);
    }
  }

  /**
   * Reassemble the full 0-based fence arrays for {@code leafCount} leaves from
   * their chunks. Returns {@code {first, last}} (each length {@code leafCount}),
   * or {@code null} when any chunk is missing or the wrong size — an
   * inconsistency the caller must resolve with a full rebuild rather than trust
   * a partial zone map.
   */
  public static long @Nullable [][] read(final ProjectionIndexHOTStorage storage, final int leafCount) {
    final long[] first = new long[leafCount];
    final long[] last = new long[leafCount];
    final int chunks = chunkCount(leafCount);
    for (int c = 0; c < chunks; c++) {
      final int start = c * CHUNK_LEAVES;
      final int end = Math.min(start + CHUNK_LEAVES, leafCount);
      final byte[] bytes = storage.getBlob(CHUNK_SLOT_BASE + c);
      if (bytes == null || bytes.length != (end - start) * ENTRY_BYTES) {
        return null;
      }
      int off = 0;
      for (int i = start; i < end; i++) {
        first[i] = ProjectionIndexLeafCodec.getLongLE(bytes, off);
        last[i] = ProjectionIndexLeafCodec.getLongLE(bytes, off + 8);
        off += ENTRY_BYTES;
      }
    }
    return new long[][] {first, last};
  }
}

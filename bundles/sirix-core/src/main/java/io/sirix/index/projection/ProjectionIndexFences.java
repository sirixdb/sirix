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
   * First reserved slot key for fence chunks. In the DESCRIPTOR layout data-leaf slots run
   * 1..rowGroupCount (bounded by {@code MAX_PROBED_LEAVES = 2^24}) and slot 0 is the metadata. In the
   * segment-slot layout a leaf slot is {@code (rowGroupId << 16) | slotKind}, so its max is
   * {@code 2^24 << 16 = 2^40} — the base must clear that (the old 2^40 assumed an 8-bit slotKind and
   * a {@code << 8} shift). 2^42 sits above every leaf slot of both layouts, so leaf probing never
   * reaches the fence chunks and the ranges cannot alias; it also stays below the side-map owner-slot
   * ceiling (2^47) so a fence chunk that spills to an OverflowPage still keys legally. Chunk
   * {@code c} lives at {@code CHUNK_SLOT_BASE + c}.
   */
  static final long CHUNK_SLOT_BASE = 1L << 42;

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

  /** Number of chunks needed to cover {@code rowGroupCount} leaves. */
  public static int chunkCount(final int rowGroupCount) {
    return (rowGroupCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
  }

  /**
   * Persist the per-leaf fences as carry-forward chunks. {@code first}/{@code last}
   * are 0-based and index-aligned with leaf slots 1..{@code rowGroupCount} (entry
   * {@code i} describes slot {@code i + 1}). Chunks whose bytes match the prior
   * revision are no-ops (shared by reference); chunks that no longer exist
   * because the leaf count shrank (a rebuild) are tombstoned.
   *
   * @param priorRowGroupCount leaf count of the snapshot being replaced, so orphaned
   *                       trailing chunks can be dropped; pass {@code 0} when there
   *                       is nothing to reclaim
   */
  public static void write(final ProjectionIndexHOTStorage storage, final int rowGroupCount,
      final long[] first, final long[] last, final int priorRowGroupCount) {
    // Exactly one entry per leaf (the invariant the old inline-fence metadata constructor
    // enforced): a shorter array reads out of bounds, a longer one carries stale trailing
    // entries beyond rowGroupCount that read() would silently ignore.
    if (first.length != rowGroupCount || last.length != rowGroupCount) {
      throw new IllegalArgumentException("fence arrays must carry exactly rowGroupCount " + rowGroupCount
          + " entries, got " + first.length + "/" + last.length);
    }
    final int chunks = chunkCount(rowGroupCount);
    for (int c = 0; c < chunks; c++) {
      final int start = c * CHUNK_LEAVES;
      final int end = Math.min(start + CHUNK_LEAVES, rowGroupCount);
      final byte[] bytes = new byte[(end - start) * ENTRY_BYTES];
      int off = 0;
      for (int i = start; i < end; i++) {
        RowGroupDescriptor.putLongLE(bytes, off, first[i]);
        RowGroupDescriptor.putLongLE(bytes, off + 8, last[i]);
        off += ENTRY_BYTES;
      }
      storage.putBlob(CHUNK_SLOT_BASE + c, bytes);
    }
    // Reclaim chunks the shrunk (rebuilt) projection no longer covers.
    for (int c = chunks; c < chunkCount(priorRowGroupCount); c++) {
      storage.tombstoneRowGroup(CHUNK_SLOT_BASE + c);
    }
  }

  /**
   * Reassemble the full 0-based fence arrays for {@code rowGroupCount} leaves from
   * their chunks. Returns {@code {first, last}} (each length {@code rowGroupCount}),
   * or {@code null} when any chunk is missing or the wrong size — an
   * inconsistency the caller must resolve with a full rebuild rather than trust
   * a partial zone map.
   */
  public static long @Nullable [][] read(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
    final long[] first = new long[rowGroupCount];
    final long[] last = new long[rowGroupCount];
    final int chunks = chunkCount(rowGroupCount);
    for (int c = 0; c < chunks; c++) {
      final int start = c * CHUNK_LEAVES;
      final int end = Math.min(start + CHUNK_LEAVES, rowGroupCount);
      final byte[] bytes = storage.getBlob(CHUNK_SLOT_BASE + c);
      if (bytes == null || bytes.length != (end - start) * ENTRY_BYTES) {
        return null;
      }
      int off = 0;
      for (int i = start; i < end; i++) {
        first[i] = ProjectionIndexRowGroupCodec.getLongLE(bytes, off);
        last[i] = ProjectionIndexRowGroupCodec.getLongLE(bytes, off + 8);
        off += ENTRY_BYTES;
      }
    }
    return new long[][] {first, last};
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.LeafDirectory;
import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * Column-sliced view of a projection's persisted leaves (P5b stage 2,
 * docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7): built from {@link LeafDirectory}s — one
 * descriptor walk, ZERO segment reads — and fetching/decoding a column's BODY segments only
 * when that column is first touched. A query for {@code sum(age)} over a 3-column projection
 * loads one third of the store's segments instead of hydrating whole leaves.
 *
 * <p><b>Segment truth.</b> Every slice decodes from its BODY segment bytes after byteLen +
 * XXH3-64 verification against the descriptor — flags, zone map, presence, and values are
 * segment truth (5.1-7), never the descriptor mirror, so column-scoped serving gates carry
 * the same evidentiary weight as whole-leaf probes.
 *
 * <p><b>Laziness &amp; threading.</b> Per-column fill is double-checked on a volatile slot
 * array; the fill (fetch + decode, the store's only I/O) runs OUTSIDE any monitor so a
 * multi-leaf fill never blocks other columns or readers — a concurrent first-touch of the
 * same column races benignly (first publish wins, content is identical). Fetches walk
 * leaves in ascending offset order for read locality.
 *
 * <p><b>Failure contract.</b> Corruption (hash/length/kind mismatch) throws
 * {@link IllegalStateException} out of {@link #column(int)} — callers decline to the eager
 * path, which surfaces the same corruption through the established fail-soft flow. Decode
 * corruption is PERMANENT for this build and memoized ({@link #columnKnownCorrupt(int)}),
 * so later touches fail fast without re-fetching; fetch-level failures (a session closing
 * mid-read, transient I/O) are NOT memoized — the next query retries with the CALLER's own
 * live {@link SegmentFetcher}, threaded into every fill call.
 *
 * <p><b>Session binding.</b> The store holds NO session-bound source: the decoded column
 * state (slices, raw bytes) is immutable and shared, and a not-yet-filled column's source is
 * a per-call {@link SegmentFetcher} argument built from the CALLING reader's own live
 * transaction — so two concurrent readers sharing this cached store never overwrite each
 * other's I/O binding.
 */
public final class ProjectionColumnStore {

  /**
   * Fetches segment pages' bytes by durable offset, one BATCH per column fill — so an
   * implementation bound to a session can open one read transaction per fill instead of one
   * per segment. Result is index-aligned with {@code offsets}; a null element = missing.
   */
  @FunctionalInterface
  public interface SegmentFetcher {
    byte @Nullable [] @Nullable [] fetchAll(long[] offsets);
  }

  /**
   * One leaf's decoded column: BODY-segment truth. {@code numericValues} is set for
   * NUMERIC_LONG/NUMERIC_DOUBLE columns (transform domain for doubles), {@code boolWords}
   * for BOOLEAN; STRING_DICT columns are not sliced (the column path declines them —
   * dictionaries stay with the eager whole-leaf path until the R1 canonical-dictionary
   * work). {@code presenceWords} is always populated for {@code rowCount > 0}.
   */
  public record ColumnSlice(int rowCount, byte flags, long min, long max, long[] presenceWords,
      long @Nullable [] numericValues, long @Nullable [] boolWords) {
  }

  private final List<LeafDirectory> directories;
  private final byte[] columnKinds;

  /** Lazily filled per column; slot = decoded slices for every leaf, ascending leafIndex. */
  private volatile ColumnSlice[] @Nullable [] columns;

  /**
   * Lazily fetched per column: the VERIFIED raw BODY segment bytes for every leaf (ascending
   * leafIndex) — the fused fold kernels' scan substrate (P5b stage 4), and the decode source
   * for {@link #column(int)} slices. Verification (byteLen + XXH3-64 + header against the
   * descriptor) happens exactly once, at fill; kernels then trust the immutable bytes.
   */
  private volatile byte[][] @Nullable [] columnBytes;

  /**
   * Per-column permanent-corruption memo (1 = a fill hit a decode/hash/missing-segment
   * failure, which cannot heal for this build). Plain byte writes of a single value are
   * race-benign; fetch-level (transient) failures never set it.
   */
  private final byte[] corruptColumns;

  public ProjectionColumnStore(final List<LeafDirectory> directories) {
    if (directories == null) {
      throw new IllegalArgumentException("directories must not be null");
    }
    this.directories = List.copyOf(directories);
    if (this.directories.isEmpty()) {
      this.columnKinds = new byte[0];
    } else {
      final byte[] d0 = this.directories.get(0).descriptor();
      final int columnCount = LeafDescriptor.columnCount(d0);
      this.columnKinds = new byte[columnCount];
      for (int c = 0; c < columnCount; c++) {
        this.columnKinds[c] = LeafDescriptor.kind(d0, c);
      }
    }
    this.columns = new ColumnSlice[columnKinds.length][];
    this.columnBytes = new byte[columnKinds.length][][];
    this.corruptColumns = new byte[columnKinds.length];
  }

  /** Whether a fill of {@code col} hit permanent decode corruption (memoized fail-fast). */
  public boolean columnKnownCorrupt(final int col) {
    return col >= 0 && col < corruptColumns.length && corruptColumns[col] != 0;
  }

  public int leafCount() {
    return directories.size();
  }

  public int columnCount() {
    return columnKinds.length;
  }

  /** Column kind byte (from the descriptors — every leaf carries the same shape). */
  public byte columnKind(final int col) {
    return columnKinds[col];
  }

  /** Row count of 0-based leaf {@code i}, straight from its descriptor. */
  public int rowCount(final int leaf) {
    return LeafDescriptor.rowCount(directories.get(leaf).descriptor());
  }

  /**
   * Whether the column path can serve {@code col} at all: numeric or boolean kinds only
   * (string columns need their DICT segments and stay on the whole-leaf path).
   */
  public boolean columnSliceable(final int col) {
    return col >= 0 && col < columnKinds.length
        && (ProjectionIndexLeafPage.isNumericKind(columnKinds[col])
            || columnKinds[col] == ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN);
  }

  /**
   * The column's slices across all leaves (ascending leafIndex), fetching + decoding its
   * BODY segments on first touch through the CALLER's own live {@code fetcher}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public ColumnSlice[] column(final int col, final SegmentFetcher fetcher) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable (kind="
          + (col >= 0 && col < columnKinds.length ? columnKinds[col] : -1) + ")");
    }
    final ColumnSlice[][] slots = columns;
    ColumnSlice[] slices = slots[col];
    if (slices != null) {
      return slices;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    // Fill OUTSIDE the monitor: the fetch+decode is the store's only I/O and must not
    // serialize other columns (or evidence readers) behind it. A same-column race does the
    // work twice with identical results — first publish wins.
    slices = fillColumn(col, fetcher);
    synchronized (this) {
      final ColumnSlice[] existing = columns[col];
      if (existing != null) {
        return existing;
      }
      // Publish via a fresh array write so the unsynchronized volatile read stays safe.
      final ColumnSlice[][] next = columns.clone();
      next[col] = slices;
      columns = next;
    }
    return slices;
  }

  private ColumnSlice[] fillColumn(final int col, final SegmentFetcher fetcher) {
    // Bytes-first: the raw-segment cache does the fetch + verification; slice decode is a
    // pure in-memory transform over the already-verified bytes.
    final byte[][] segments = columnBytes(col, fetcher);
    final int n = directories.size();
    final ColumnSlice[] slices = new ColumnSlice[n];
    try {
      for (int i = 0; i < n; i++) {
        slices[i] = ProjectionIndexSegmentCodec.decodeBodySlice(directories.get(i).descriptor(),
            segments[i], col);
      }
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    return slices;
  }

  /**
   * The column's VERIFIED raw BODY segment bytes across all leaves (ascending leafIndex),
   * fetching them on first touch through the CALLER's own live {@code fetcher} — the fused
   * fold kernels' substrate. Same laziness, threading, and failure contract as
   * {@link #column(int, SegmentFetcher)}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public byte[][] columnBytes(final int col, final SegmentFetcher fetcher) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable (kind="
          + (col >= 0 && col < columnKinds.length ? columnKinds[col] : -1) + ")");
    }
    final byte[][][] slots = columnBytes;
    byte[][] segments = slots[col];
    if (segments != null) {
      return segments;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    segments = fetchColumnBytes(col, fetcher);
    synchronized (this) {
      final byte[][] existing = columnBytes[col];
      if (existing != null) {
        return existing;
      }
      final byte[][][] next = columnBytes.clone();
      next[col] = segments;
      columnBytes = next;
    }
    return segments;
  }

  private byte[][] fetchColumnBytes(final int col, final SegmentFetcher fetcher) {
    final int n = directories.size();
    final int bodyId = ProjectionIndexSegmentCodec.bodySegmentId(col);
    // Leaf order IS file order to within noise: the builder persists leaves 1..N in one
    // sequential commit, so a column's BODY offsets ascend with the leaf index — no
    // explicit sort needed for read locality. One batched fetch = one read transaction.
    final long[] offsets = new long[n];
    for (int i = 0; i < n; i++) {
      offsets[i] = offsetOf(directories.get(i), bodyId);
    }
    final byte[][] segments;
    try {
      segments = fetcher.fetchAll(offsets);
    } catch (final RuntimeException fetchFailed) {
      // Fetch-level failure (session closed mid-read, transient I/O): NOT memoized —
      // the next query retries against the caller's own live fetcher.
      throw new IllegalStateException("Segment fetch failed for column " + col + ": "
          + fetchFailed.getMessage(), fetchFailed);
    }
    if (segments == null || segments.length != n) {
      throw new IllegalStateException("Segment fetcher returned "
          + (segments == null ? "null" : segments.length + " results") + " for " + n + " offsets");
    }
    try {
      for (int i = 0; i < n; i++) {
        ProjectionIndexSegmentCodec.verifySegment(directories.get(i).descriptor(), segments[i],
            bodyId, ProjectionIndexSegmentCodec.SEG_KIND_BODY);
      }
    } catch (final IllegalStateException corrupt) {
      // Structural corruption (missing segment at a resolved offset, hash/length/kind
      // mismatch) cannot heal for this build — memoize so later touches fail fast.
      corruptColumns[col] = 1;
      throw corrupt;
    }
    return segments;
  }

  private static long offsetOf(final LeafDirectory dir, final int segmentId) {
    final int[] ids = dir.segmentIds();
    for (int i = 0; i < ids.length; i++) {
      if (ids[i] == segmentId) {
        return dir.segmentOffsets()[i];
      }
    }
    throw new IllegalStateException("Descriptor of leaf " + dir.leafIndex()
        + " lists no segment id " + segmentId);
  }
}

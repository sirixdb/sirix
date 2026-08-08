/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * Column-sliced view of a projection's persisted leaves (P5b stage 2,
 * docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7): built from {@link RowGroupDirectory}s — one
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
 * live {@link ColumnSegmentFetcher}, threaded into every fill call.
 *
 * <p><b>Session binding.</b> The store holds NO session-bound source: the decoded column
 * state (slices, raw bytes) is immutable and shared, and a not-yet-filled column's source is
 * a per-call {@link ColumnSegmentFetcher} argument built from the CALLING reader's own live
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
  public interface ColumnSegmentFetcher {
    byte @Nullable [] @Nullable [] fetchAll(long[] offsets);
  }

  /**
   * One leaf's decoded column: segment truth. {@code numericValues} is set for
   * NUMERIC_LONG/NUMERIC_DOUBLE columns (transform domain for doubles), {@code boolWords}
   * for BOOLEAN, and {@code stringDictIds}+{@code stringDict} for STRING_DICT — the BODY's
   * per-row dict-ids beside the DICT segment's decoded entries, which is what lets a string
   * equality run column-sliced instead of hydrating whole leaves. (Per-leaf dictionaries
   * still resolve the literal per leaf; the R1 canonical-dictionary work removes that remap,
   * not the slicing.) {@code presenceWords} is always populated for {@code rowCount > 0}.
   */
  public record ColumnSlice(int rowCount, byte flags, long min, long max, long[] presenceWords,
      long @Nullable [] numericValues, long @Nullable [] boolWords,
      int @Nullable [] stringDictIds, byte @Nullable [] @Nullable [] stringDict) {
  }

  private final List<RowGroupDirectory> directories;
  private final byte[] columnKinds;

  /** Lazily filled per column; slot = decoded slices for every leaf, ascending rowGroupId. */
  private volatile ColumnSlice[] @Nullable [] columns;

  /**
   * Lazily fetched per column: the VERIFIED raw BODY segment bytes for every leaf (ascending
   * rowGroupId) — the fused fold kernels' scan substrate (P5b stage 4), and the decode source
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

  /**
   * Lazily decoded per-leaf record keys (ascending rowGroupId; empty array for a rowless
   * leaf) — the KEYS chain, fetched once and shared by every sorted collection. Same
   * publication discipline as {@link #columns}.
   */
  private volatile long[] @Nullable [] recordKeySlices;

  /** KEYS-chain twin of {@link #corruptColumns} — permanent decode corruption, memoized. */
  private volatile boolean keysCorrupt;

  public ProjectionColumnStore(final List<RowGroupDirectory> directories) {
    if (directories == null) {
      throw new IllegalArgumentException("directories must not be null");
    }
    this.directories = List.copyOf(directories);
    if (this.directories.isEmpty()) {
      this.columnKinds = new byte[0];
    } else {
      final byte[] d0 = this.directories.get(0).descriptor();
      final int columnCount = RowGroupDescriptor.columnCount(d0);
      this.columnKinds = new byte[columnCount];
      for (int c = 0; c < columnCount; c++) {
        this.columnKinds[c] = RowGroupDescriptor.kind(d0, c);
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

  public int rowGroupCount() {
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
    return RowGroupDescriptor.rowCount(directories.get(leaf).descriptor());
  }

  /**
   * Whether the column path can serve {@code col} at all: numeric, boolean, and string-dict
   * kinds. A string column's fill fetches its DICT chain beside the BODY chain — two segment
   * chains instead of one, still only THIS column's bytes, never the whole leaf.
   */
  public boolean columnSliceable(final int col) {
    return col >= 0 && col < columnKinds.length
        && (ProjectionIndexRowGroupPage.isNumericKind(columnKinds[col])
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT);
  }

  /**
   * The column's slices across all leaves (ascending rowGroupId), fetching + decoding its
   * BODY segments on first touch through the CALLER's own live {@code fetcher}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public ColumnSlice[] column(final int col, final ColumnSegmentFetcher fetcher) {
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

  private ColumnSlice[] fillColumn(final int col, final ColumnSegmentFetcher fetcher) {
    // Bytes-first: the raw-segment cache does the fetch + verification; slice decode is a
    // pure in-memory transform over the already-verified bytes.
    final byte[][] segments = columnBytes(col, fetcher);
    // A string column needs its DICT chain beside the BODY chain — ids without the dictionary
    // are meaningless. The chain is OPTIONAL per leaf: a rowless leaf writes no DICT segment.
    final boolean string = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final byte[][] dictSegments = string
        ? fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
                            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher)
        : null;
    final int n = directories.size();
    final ColumnSlice[] slices = new ColumnSlice[n];
    try {
      for (int i = 0; i < n; i++) {
        final byte[] descriptor = directories.get(i).descriptor();
        slices[i] = string
            ? ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, segments[i],
                                                                  dictSegments[i], col)
            : ProjectionIndexColumnSegmentCodec.decodeBodySlice(descriptor, segments[i], col);
      }
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    return slices;
  }

  /**
   * The column's VERIFIED raw BODY segment bytes across all leaves (ascending rowGroupId),
   * fetching them on first touch through the CALLER's own live {@code fetcher} — the fused
   * fold kernels' substrate. Same laziness, threading, and failure contract as
   * {@link #column(int, ColumnSegmentFetcher)}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public byte[][] columnBytes(final int col, final ColumnSegmentFetcher fetcher) {
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

  /**
   * Fill {@code offsets} with each row group's page offset for segment {@code segId}, and return
   * the parallel array of INLINE bytes for the row groups that have no page —
   * {@code null} when the whole column is referenced, which is the common case at scale.
   *
   * <p>An inline entry gets the {@link Constants#NULL_ID_LONG} sentinel in {@code offsets} so the
   * batched fetch skips it (the fetcher yields {@code null} there) and the caller patches its bytes
   * in afterwards. With {@code optional}, a leaf whose descriptor lists no such segment gets the
   * same sentinel and simply stays absent — the shape of a DICT chain, which a rowless leaf never
   * writes; a required chain still refuses the whole fill on a missing entry.
   */
  private byte @Nullable [][] collectColumnOffsets(final int segId, final long[] offsets,
      final boolean optional, final boolean[] absent) {
    final int n = directories.size();
    byte[][] inlineBytes = null;
    for (int i = 0; i < n; i++) {
      final RowGroupDirectory dir = directories.get(i);
      final byte[] desc = dir.descriptor();
      // ONE lookup per row group. The directory's columnSegmentIds / columnSegmentOffsets /
      // inlineColumnSegmentBytes are filled in descriptor-entry order, so this single binary search
      // indexes all three; resolving each of them by scanning for the id separately made a column
      // fill O(rowGroups × segments), which the widened column cap turned into the dominant cost.
      final int entry = RowGroupDescriptor.entryIndexOf(desc, segId);
      if (entry < 0) {
        if (optional) {
          absent[i] = true;
          offsets[i] = Constants.NULL_ID_LONG;
          continue;
        }
        throw new IllegalStateException("Descriptor of leaf " + dir.rowGroupId()
            + " lists no segment id " + segId);
      }
      // Segment-slot layout: a bare INLINE segment's bytes were captured at directory build (its
      // zone-map-only descriptor carries no inline region), so they come straight from the directory.
      // Descriptor layout: an inline segment's bytes ride the descriptor's trailing region.
      final byte[] dirInline = dir.inlineBytesAt(entry);
      final byte[] inlineForEntry = dirInline != null ? dirInline
          : RowGroupDescriptor.entryIsInline(desc, entry)
              ? RowGroupDescriptor.inlineColumnSegmentBytes(desc, entry)
              : null;
      if (inlineForEntry != null) {
        if (inlineBytes == null) {
          inlineBytes = new byte[n][];
        }
        inlineBytes[i] = inlineForEntry;
        offsets[i] = Constants.NULL_ID_LONG;
      } else {
        offsets[i] = dir.columnSegmentOffsets()[entry];
      }
    }
    return inlineBytes;
  }

  private byte[][] fetchColumnBytes(final int col, final ColumnSegmentFetcher fetcher) {
    return fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col),
                             ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY, false, fetcher);
  }

  /**
   * Every leaf's per-row record keys (ascending rowGroupId), fetched and decoded from the
   * KEYS chain on first touch — what a sorted collection returns for lazy materialization,
   * without ever hydrating a whole leaf.
   *
   * @throws IllegalStateException on missing/corrupt KEYS segments
   */
  public long[][] recordKeys(final ColumnSegmentFetcher fetcher) {
    long[][] slices = recordKeySlices;
    if (slices != null) {
      return slices;
    }
    if (keysCorrupt) {
      throw new IllegalStateException("The KEYS chain has a known-corrupt segment");
    }
    // Fill outside the monitor, first publish wins — same discipline as column fills.
    final byte[][] segments = fetchSegmentChain(-1, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
                                                ProjectionIndexColumnSegmentCodec.SEG_KIND_KEYS, false, fetcher);
    final int n = directories.size();
    final long[][] decoded = new long[n][];
    try {
      for (int i = 0; i < n; i++) {
        decoded[i] = ProjectionIndexColumnSegmentCodec.decodeKeysSlice(directories.get(i).descriptor(),
                                                                       segments[i]);
      }
    } catch (final IllegalStateException corrupt) {
      keysCorrupt = true;
      throw corrupt;
    }
    synchronized (this) {
      slices = recordKeySlices;
      if (slices != null) {
        return slices;
      }
      recordKeySlices = decoded;
    }
    return decoded;
  }

  /**
   * Fetch and verify one segment chain (ascending rowGroupId) for {@code col} — the BODY chain
   * for every sliceable column, and additionally the DICT chain for a string column's fill.
   */
  private byte[][] fetchSegmentChain(final int col, final int segId, final byte segKind,
      final boolean optional, final ColumnSegmentFetcher fetcher) {
    final int n = directories.size();
    // Leaf order IS file order to within noise: the builder persists leaves 1..N in one
    // sequential commit, so a column's segment offsets ascend with the leaf index — no
    // explicit sort needed for read locality. One batched fetch = one read transaction.
    // Hybrid: an inline segment carries no page — its bytes come straight from the descriptor, so
    // it is skipped in the offset batch (the NULL_ID_LONG sentinel → the fetcher yields null there)
    // and filled in afterwards. inlineBytes stays null when the whole column is referenced.
    final long[] offsets = new long[n];
    final boolean[] absent = optional ? new boolean[n] : null;
    final byte[][] inlineBytes = collectColumnOffsets(segId, offsets, optional, absent);
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
    if (inlineBytes != null) {
      for (int i = 0; i < n; i++) {
        if (inlineBytes[i] != null) {
          segments[i] = inlineBytes[i];
        }
      }
    }
    try {
      for (int i = 0; i < n; i++) {
        if (absent != null && absent[i]) {
          continue;  // the descriptor genuinely lists no such segment for this leaf
        }
        ProjectionIndexColumnSegmentCodec.verifyColumnSegment(directories.get(i).descriptor(), segments[i],
            segId, segKind);
      }
    } catch (final IllegalStateException corrupt) {
      // Structural corruption (missing segment at a resolved offset, hash/length/kind
      // mismatch) cannot heal for this build — memoize so later touches fail fast.
      if (col >= 0) {
        corruptColumns[col] = 1;
      } else {
        keysCorrupt = true;  // the KEYS chain carries no column index
      }
      throw corrupt;
    }
    return segments;
  }

}

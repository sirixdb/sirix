/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.api.StorageEngineReader;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * Column-sliced view of a projection's persisted leaves (P5b stage 2,
 * docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7): built from {@link RowGroupDirectory}s — one
 * descriptor walk, ZERO segment reads — and fetching/decoding a column's BODY segments only when
 * that column is first touched. A query for {@code sum(age)} over a 3-column projection loads one
 * third of the store's segments instead of hydrating whole leaves.
 *
 * <p>
 * <b>Segment truth.</b> Every slice decodes from its BODY segment bytes after byteLen + XXH3-64
 * verification against the descriptor — flags, zone map, presence, and values are segment truth
 * (5.1-7), never the descriptor mirror, so column-scoped serving gates carry the same evidentiary
 * weight as whole-leaf probes.
 *
 * <p>
 * <b>Laziness &amp; threading.</b> Per-column fill is double-checked on a volatile slot array; the
 * fill (fetch + decode, the store's only I/O) runs OUTSIDE any monitor so a multi-leaf fill never
 * blocks other columns or readers — a concurrent first-touch of the same column races benignly
 * (first publish wins, content is identical). Fetches walk leaves in ascending offset order for
 * read locality.
 *
 * <p>
 * <b>Failure contract.</b> Corruption (hash/length/kind mismatch) throws
 * {@link IllegalStateException} out of {@link #column(int)} — callers decline to the eager path,
 * which surfaces the same corruption through the established fail-soft flow. Decode corruption is
 * PERMANENT for this build and memoized ({@link #columnKnownCorrupt(int)}), so later touches fail
 * fast without re-fetching; fetch-level failures (a session closing mid-read, transient I/O) are
 * NOT memoized — the next query retries with the CALLER's own live {@link ColumnSegmentFetcher},
 * threaded into every fill call.
 *
 * <p>
 * <b>Session binding.</b> The store holds NO session-bound source: the decoded column state
 * (slices, raw bytes) is immutable and shared, and a not-yet-filled column's source is a per-call
 * {@link ColumnSegmentFetcher} argument built from the CALLING reader's own live transaction — so
 * two concurrent readers sharing this cached store never overwrite each other's I/O binding.
 */
public final class ProjectionColumnStore {

  /**
   * Fetches segment pages' bytes by durable offset, one BATCH per column fill — so an implementation
   * bound to a session can open one read transaction per fill instead of one per segment. Result is
   * index-aligned with {@code offsets}; a null element = missing.
   */
  @FunctionalInterface
  public interface ColumnSegmentFetcher {
    byte @Nullable [] @Nullable [] fetchAll(long[] offsets);

    /**
     * Fetch the contiguous sub-range {@code [from, to)} of {@code offsets} into {@code out} at the
     * SAME indices.
     *
     * <p>
     * The default delegates to {@link #fetchAll} on the calling thread — correct, and exactly the
     * behaviour of a fetcher that never heard of ranges. A fetcher that can open a read transaction of
     * its own overrides this and answers {@code true} from {@link #rangedFetchIsConcurrent()}, which
     * is what lets a chain fetch fan its ranges across cores.
     */
    default void fetchRange(final long[] offsets, final int from, final int to, final byte[][] out) {
      final int len = to - from;
      final byte[][] part = fetchAll(Arrays.copyOfRange(offsets, from, to));
      if (part == null || part.length != len) {
        throw new IllegalStateException("Segment fetcher returned " + (part == null
            ? "null"
            : part.length + " results") + " for " + len + " offsets");
      }
      System.arraycopy(part, 0, out, from, len);
    }

    /**
     * Whether {@link #fetchRange} may be called CONCURRENTLY on disjoint ranges. Default {@code false}:
     * a fetcher bound to one shared transaction is not thread-safe, and a read transaction is the
     * usual binding, so the safe answer has to be the silent one.
     */
    default boolean rangedFetchIsConcurrent() {
      return false;
    }
  }

  /**
   * One leaf's decoded column: segment truth. {@code numericValues} is set for
   * NUMERIC_LONG/NUMERIC_DOUBLE columns (transform domain for doubles), {@code boolWords} for
   * BOOLEAN, and {@code stringDictIds}+{@code dictBytes}/{@code dictOffsets} for STRING_DICT — the
   * BODY's per-row dict-ids beside the DICT segment's decoded entries, which is what lets a string
   * equality run column-sliced instead of hydrating whole leaves. (Per-leaf dictionaries still
   * resolve the literal per leaf; the R1 canonical-dictionary work removes that remap, not the
   * slicing.) {@code presenceWords} is always populated for {@code rowCount > 0}.
   *
   * <p>
   * <b>The dictionary is FLAT</b>: one contiguous byte run plus {@code dictSize + 1} offsets, not one
   * {@code byte[]} per entry. On a high-cardinality column the per-leaf dictionary is nearly as large
   * as the leaf, so the per-entry arrays were the dominant allocation of a whole column fill — and
   * every consumer (hashing, comparison, UTF-8 inspection, string materialization) is (array, offset,
   * length)-shaped anyway. For a RAW-mode dictionary {@code dictBytes} IS the segment and the offsets
   * are absolute into it, so the decode copies nothing at all.
   */
  public record ColumnSlice(int rowCount, byte flags, long min, long max, long[] presenceWords,
      long @Nullable [] numericValues, long @Nullable [] boolWords, int @Nullable [] stringDictIds,
      byte @Nullable [] dictBytes, int @Nullable [] dictOffsets, int @Nullable [] setCounts,
      long @Nullable [] dictHashes) {

    /**
     * Slice with a set column but no precomputed dictionary hashes — every fill but the
     * distinct-identity one.
     */
    public ColumnSlice(int rowCount, byte flags, long min, long max, long[] presenceWords,
        long @Nullable [] numericValues, long @Nullable [] boolWords, int @Nullable [] stringDictIds,
        byte @Nullable [] dictBytes, int @Nullable [] dictOffsets, int @Nullable [] setCounts) {
      this(rowCount, flags, min, max, presenceWords, numericValues, boolWords, stringDictIds, dictBytes, dictOffsets,
          setCounts, null);
    }

    /**
     * Slice without a set column — every kind but
     * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET}.
     */
    public ColumnSlice(int rowCount, byte flags, long min, long max, long[] presenceWords,
        long @Nullable [] numericValues, long @Nullable [] boolWords, int @Nullable [] stringDictIds,
        byte @Nullable [] dictBytes, int @Nullable [] dictOffsets) {
      this(rowCount, flags, min, max, presenceWords, numericValues, boolWords, stringDictIds, dictBytes, dictOffsets,
          null, null);
    }

    /**
     * Number of dictionary entries — EXACT, unlike the {@code byte[][]} form's null-padded tail, so a
     * caller walks {@code 0..dictSize()} with no hole to skip. Dict ids from the BODY are always in
     * range; the encoder writes exactly the entries it counted. A DISTINCT-IDENTITY slice carries no
     * dictionary bytes, so its size comes from the precomputed hashes — one per entry, same order.
     */
    public int dictSize() {
      final int[] offsets = dictOffsets;
      if (offsets != null) {
        return offsets.length - 1;
      }
      final long[] hashes = dictHashes;
      return hashes == null
          ? 0
          : hashes.length;
    }

    /** Start of dictionary entry {@code id} within {@link #dictBytes()}. */
    public int dictOffset(final int id) {
      return dictOffsets[id];
    }

    /** Byte length of dictionary entry {@code id}. */
    public int dictLength(final int id) {
      final int[] offsets = dictOffsets;
      return offsets[id + 1] - offsets[id];
    }

    /**
     * FNV-64 of dictionary entry {@code id} — the flat string kernels' group identity, in the ONE
     * domain {@link ProjectionIndexByteScan#fnv1a64} defines for both kernel families, so a caller
     * outside this package can rebuild a winner's hash from its {@code (leaf, dictId)} reference.
     */
    public long dictHash(final int id) {
      final long[] hashes = dictHashes;
      if (hashes != null) {
        return hashes[id];
      }
      final int[] offsets = dictOffsets;
      final int off = offsets[id];
      return ProjectionIndexByteScan.fnv1a64(dictBytes, off, offsets[id + 1] - off);
    }

    /**
     * Dictionary entry {@code id} decoded as a string. ALLOCATING — for the materialization of a
     * handful of winners, never for a per-row or per-entry loop, which is exactly the discipline the
     * flat form exists to enforce.
     */
    public String dictString(final int id) {
      final int[] offsets = dictOffsets;
      return new String(dictBytes, offsets[id], offsets[id + 1] - offsets[id], StandardCharsets.UTF_8);
    }
  }

  private final List<RowGroupDirectory> directories;
  private final byte[] columnKinds;

  /** Lazily filled per column; slot = decoded slices for every leaf, ascending rowGroupId. */
  private volatile ColumnSlice[] @Nullable [] columns;

  /**
   * Lazily filled per column: DISTINCT-IDENTITY slices (ids + precomputed dict hashes, no dictionary
   * bytes). Its own cache rather than {@link #columns}, because these slices deliberately lack the
   * dictionary every other consumer needs — publishing them there would starve a later group-key or
   * materialization read. See {@link #columnDistinctIdentity}.
   */
  private volatile ColumnSlice[] @Nullable [] identityColumns;

  /**
   * Lazily fetched per column: the VERIFIED raw BODY segment bytes for every leaf (ascending
   * rowGroupId) — the fused fold kernels' scan substrate (P5b stage 4), and the decode source for
   * {@link #column(int)} slices. Verification (byteLen + XXH3-64 + header against the descriptor)
   * happens exactly once, at fill; kernels then trust the immutable bytes.
   */
  private volatile byte[][] @Nullable [] columnBytes;

  /** Per-column fingerprint chains ({@code SEG_KIND_STRING_BLOOM}), published once per store. */
  private volatile byte[][] @Nullable [] bloomBytes;

  /**
   * Per-column fingerprint BLOCKS (the contiguous acceleration; {@code null} per column when absent).
   * Attached once by the catalog right after construction, before the handle escapes.
   */
  private byte @Nullable [] @Nullable [] bloomBlocks;

  /** See {@link ProjectionIndexColumnSegmentCodec#encodeBloomBlock}. Catalog-attach, set once. */
  public void attachBloomBlocks(final byte @Nullable [] @Nullable [] blocks) {
    this.bloomBlocks = blocks;
  }

  /**
   * Clear {@code keep} bits for leaves whose string-column fingerprint PROVES the literal absent.
   * Evidence order: the contiguous block (already in memory, zero I/O), else the per-leaf chain
   * (cached after the first fetch), else nothing — leaves without evidence stay kept.
   *
   * @return number of leaves newly dropped
   */
  public int applyBloomPrune(final int col, final long literalHash, final long[] keep,
      final ColumnSegmentFetcher fetcher) {
    final int n = directories.size();
    int dropped = 0;
    final byte[][] blocks = bloomBlocks;
    final byte[] block = blocks != null
        ? blocks[col]
        : null;
    if (block != null) {
      for (int i = 0; i < n; i++) {
        if ((keep[i >>> 6] & 1L << (i & 63)) == 0) {
          continue;
        }
        if (!ProjectionIndexColumnSegmentCodec.bloomBlockMayContainHash(block, i, literalHash)) {
          keep[i >>> 6] &= ~(1L << (i & 63));
          dropped++;
        }
      }
      return dropped;
    }
    final byte[][] chain = stringBloomSegments(col, fetcher);
    if (chain == null) {
      return 0;
    }
    for (int i = 0; i < n; i++) {
      if ((keep[i >>> 6] & 1L << (i & 63)) == 0 || chain[i] == null) {
        continue;
      }
      if (!ProjectionIndexColumnSegmentCodec.bloomMayContainHash(chain[i], literalHash)) {
        keep[i >>> 6] &= ~(1L << (i & 63));
        dropped++;
      }
    }
    return dropped;
  }

  /**
   * Per-column permanent-corruption memo (1 = a fill hit a decode/hash/missing-segment failure, which
   * cannot heal for this build). Plain byte writes of a single value are race-benign; fetch-level
   * (transient) failures never set it.
   */
  private final byte[] corruptColumns;

  /**
   * Lazily decoded per-leaf record keys (ascending rowGroupId; empty array for a rowless leaf) — the
   * KEYS chain, fetched once and shared by every sorted collection. Same publication discipline as
   * {@link #columns}.
   */
  private volatile long[] @Nullable [] recordKeySlices;

  /** KEYS-chain twin of {@link #corruptColumns} — permanent decode corruption, memoized. */
  private volatile boolean keysCorrupt;

  /**
   * Lazily computed per STRING_DICT column: {@code [2 * leaf]} = dict id of the leaf's SMALLEST
   * present value, {@code [2 * leaf + 1]} = its LARGEST, both {@code -1} for a leaf with no present
   * value. See {@link #stringValueExtrema}.
   */
  private volatile int[] @Nullable [] stringExtrema;

  /**
   * Lazily computed per STRING_DICT column: {@code 0} = not yet swept, {@code 1} = no dictionary
   * entry anywhere holds a supplementary character, {@code 2} = at least one does. See
   * {@link #stringDictSupplementaryMemo}.
   */
  private final byte[] stringSupplementary;

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
    this.identityColumns = new ColumnSlice[columnKinds.length][];
    this.columnBytes = new byte[columnKinds.length][][];
    this.bloomBytes = new byte[columnKinds.length][][];
    this.stringExtrema = new int[columnKinds.length][];
    this.stringSupplementary = new byte[columnKinds.length];
    this.corruptColumns = new byte[columnKinds.length];
  }

  /** {@link #stringDictSupplementaryMemo} — not yet established for this column. */
  public static final byte SUPPLEMENTARY_UNKNOWN = 0;

  /** {@link #stringDictSupplementaryMemo} — no dictionary entry holds a supplementary character. */
  public static final byte SUPPLEMENTARY_NONE = 1;

  /** {@link #stringDictSupplementaryMemo} — at least one dictionary entry holds one. */
  public static final byte SUPPLEMENTARY_PRESENT = 2;

  /**
   * Whether ANY dictionary entry of a STRING_DICT column holds a supplementary character (a 4-byte
   * UTF-8 sequence) — the one case where unsigned byte order and the interpreter's collation
   * disagree, so a comparison must decode instead of running
   * {@link Arrays#compareUnsigned(byte[], byte[])}.
   *
   * <p>
   * READ-ONLY: this never sweeps. Callers that compare the column's values millions of times (top-k
   * selection) otherwise re-derive it per COMPARISON, rescanning both operands' bytes each time — but
   * a sweep of its own costs more than it saves, so the verdict is a by-product of
   * {@link #stringValueExtrema}, which already walks every entry. Answer
   * {@link #SUPPLEMENTARY_UNKNOWN} by taking the exact per-pair path; it is never wrong, only slower.
   *
   * @return one of {@link #SUPPLEMENTARY_UNKNOWN}, {@link #SUPPLEMENTARY_NONE},
   *         {@link #SUPPLEMENTARY_PRESENT}
   */
  public byte stringDictSupplementaryMemo(final int col) {
    if (col < 0 || col >= columnKinds.length
        || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      return SUPPLEMENTARY_UNKNOWN;
    }
    return stringSupplementary[col];
  }

  /**
   * Per-leaf VALUE extrema of a STRING_DICT column, as dict ids: {@code [2 * leaf]} the smallest
   * present value's id, {@code [2 * leaf + 1]} the largest', {@code -1} when the leaf holds no
   * present value. A slice's own {@code min()}/{@code max()} are dict IDS for this kind — meaningless
   * for value order — so an order-aware caller (the sorted scan's leaf pruning) needs this instead.
   *
   * <p>
   * Referenced-and-present gated, like {@link ProjectionColumnScan#stringDictMinMax}: a dictionary
   * can hold PHANTOM entries no live row points at, and a phantom extremum would weaken every prune
   * built on it. Data-derived and literal-independent, so it is memoized per column and shared by
   * every query — the same publication discipline as {@link #column}.
   *
   * @throws IllegalStateException if {@code col} is not a STRING_DICT column
   */
  public int[] stringValueExtrema(final int col, final ColumnSegmentFetcher fetcher) {
    if (col < 0 || col >= columnKinds.length
        || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " is not STRING_DICT");
    }
    final int[][] cached = stringExtrema;
    final int[] hit = cached[col];
    if (hit != null) {
      return hit;
    }
    final ColumnSlice[] slices = column(col, fetcher);
    final int n = slices.length;
    final int[] extrema = new int[2 * n];
    // The collation gate, hoisted out of the comparisons and recorded for later callers: whether a
    // supplementary character is in play is a property of the DICTIONARY, and re-deriving it per
    // comparison rescans both operands' bytes every time. The verdict rides along on this walk
    // because a sweep of its own costs about as much as the extrema themselves.
    final boolean[] leafSupplementary = new boolean[n];
    // SERIAL on purpose, though the leaves are independent and {@link #fillColumn} fans its decode
    // out over the very same shape. Measured on the 1M ClickBench corpus (977 leaves, min of three
    // interleaved full-suite rounds), the cold walk costs 44 ms serially and 91 ms across the common
    // pool — a fan-out here LOSES, and loses again with a serial warmup prefix in front of it. The
    // per-leaf body chases byte[] dictionary entries all over the heap, so it is memory-bound rather
    // than compute-bound, and the pool is already carrying this store's column decodes beside it.
    for (int leaf = 0; leaf < n; leaf++) {
      extremaOfLeaf(slices[leaf], leaf, extrema, leafSupplementary);
    }
    boolean anySupplementary = false;
    for (final boolean leafHasOne : leafSupplementary) {
      anySupplementary |= leafHasOne;
    }
    // A leaf whose sweep short-circuited leaves the column verdict at PRESENT, which is exactly
    // right; NONE is only recorded after every entry of every leaf was read.
    stringSupplementary[col] = anySupplementary
        ? SUPPLEMENTARY_PRESENT
        : SUPPLEMENTARY_NONE;
    synchronized (this) {
      final int[] existing = stringExtrema[col];
      if (existing != null) {
        return existing;
      }
      final int[][] next = stringExtrema.clone();
      next[col] = extrema;
      stringExtrema = next;
    }
    return extrema;
  }

  /**
   * One leaf's contribution to {@link #stringValueExtrema}: its smallest and largest REFERENCED
   * dictionary entry into {@code extrema[2 * leaf]} / {@code [2 * leaf + 1]} ({@code -1} when the
   * leaf has no present value), and whether its dictionary holds a supplementary character into
   * {@code leafSupplementary[leaf]}.
   *
   * <p>
   * Writes only its own three disjoint slots and reads only its own immutable slice, so nothing here
   * forces the walk to be serial — see the caller for why it is anyway.
   */
  private static void extremaOfLeaf(final @Nullable ColumnSlice slice, final int leaf, final int[] extrema,
      final boolean[] leafSupplementary) {
    extrema[2 * leaf] = -1;
    extrema[2 * leaf + 1] = -1;
    if (slice == null || slice.rowCount() <= 0) {
      return;
    }
    final byte[] dictBytes = slice.dictBytes();
    final int[] dictOffsets = slice.dictOffsets();
    final int[] ids = slice.stringDictIds();
    if (dictBytes == null || dictOffsets == null || ids == null) {
      return; // no dict lanes: the caller sees -1 and declines to prune this leaf
    }
    final int dictSize = dictOffsets.length - 1;
    if (dictSize == 0) {
      return;
    }
    // Scratch is per leaf rather than reused across them: the walk runs once per column, so a
    // 128-byte bitset per leaf costs nothing, and sharing one would be the only thing standing
    // between this and running the leaves concurrently.
    final long[] referenced = new long[(dictSize + 63) >>> 6];
    final long[] presence = slice.presenceWords();
    final int rowCount = slice.rowCount();
    for (int r = 0; r < rowCount; r++) {
      if (presence != null && (presence[r >>> 6] & 1L << (r & 63)) == 0L) {
        continue;
      }
      final int id = ids[r];
      referenced[id >>> 6] |= 1L << (id & 63);
    }
    // One pass over this leaf's entries settles the collation gate for all of its comparisons.
    boolean supplementary = false;
    for (int i = 0; i < dictSize && !supplementary; i++) {
      supplementary =
          ProjectionIndexScan.hasFourByteUtf8(dictBytes, dictOffsets[i], dictOffsets[i + 1] - dictOffsets[i]);
    }
    leafSupplementary[leaf] = supplementary;
    int minId = -1;
    int maxId = -1;
    for (int i = 0; i < dictSize; i++) {
      if ((referenced[i >>> 6] & 1L << (i & 63)) == 0L) {
        continue;
      }
      if (minId < 0) {
        minId = i;
        maxId = i;
        continue;
      }
      final int off = dictOffsets[i];
      final int len = dictOffsets[i + 1] - off;
      if (supplementary) {
        if (ProjectionIndexByteScan.compareStrSlices(dictBytes, off, len, dictBytes, dictOffsets[minId],
            dictOffsets[minId + 1] - dictOffsets[minId]) < 0) {
          minId = i;
        }
        if (ProjectionIndexByteScan.compareStrSlices(dictBytes, off, len, dictBytes, dictOffsets[maxId],
            dictOffsets[maxId + 1] - dictOffsets[maxId]) > 0) {
          maxId = i;
        }
      } else {
        if (Arrays.compareUnsigned(dictBytes, off, off + len, dictBytes, dictOffsets[minId],
            dictOffsets[minId + 1]) < 0) {
          minId = i;
        }
        if (Arrays.compareUnsigned(dictBytes, off, off + len, dictBytes, dictOffsets[maxId],
            dictOffsets[maxId + 1]) > 0) {
          maxId = i;
        }
      }
    }
    extrema[2 * leaf] = minId;
    extrema[2 * leaf + 1] = maxId;
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
  /**
   * The column's flags byte on {@code leaf}, straight from the descriptor.
   *
   * <p>
   * Identical to {@code ColumnSlice.flags()} — the encoder writes the one byte into both — but
   * obtainable without decoding the slice, and therefore without fetching the column's segments.
   *
   * @return the flags, or {@code COLUMN_FLAG_UNREPRESENTABLE} when the entry is missing, which fails
   *         closed exactly as an unreadable slice does
   */
  public byte columnFlags(final int leaf, final int col) {
    final byte[] descriptor = directories.get(leaf).descriptor();
    final int entry =
        RowGroupDescriptor.entryIndexOf(descriptor, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col));
    if (entry < 0) {
      return ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE;
    }
    return RowGroupDescriptor.entryColFlags(descriptor, entry);
  }

  /**
   * The column's zone-map range on {@code leaf} — {@code out2[0]} = min, {@code out2[1]} = max.
   *
   * <p>
   * Descriptor truth at ZERO I/O: the encoder writes the same pair into the descriptor entry that
   * {@link ColumnSlice#min()} / {@link ColumnSlice#max()} report, so a range read here is worth
   * exactly as much as one read off a decoded slice, without fetching the column's segments. A
   * {@code min > max} pair is the format's all-missing marker (rowless leaf, or every cell missing on
   * this leaf), never a real range — callers must skip such leaves rather than fold the sentinels in.
   *
   * @return {@code false} when the entry is absent, meaning the range is UNKNOWN. Callers fail closed
   *         on that; a fabricated range would under-report the union and let an index-by-subtraction
   *         accumulator address out of bounds.
   */
  public boolean columnZoneRange(final int leaf, final int col, final long[] out2) {
    if (out2 == null || out2.length < 2) {
      throw new IllegalArgumentException("out2 must hold at least two longs");
    }
    if (col < 0 || col >= columnKinds.length) {
      return false;
    }
    final byte[] descriptor = directories.get(leaf).descriptor();
    final int entry =
        RowGroupDescriptor.entryIndexOf(descriptor, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col));
    if (entry < 0) {
      return false;
    }
    out2[0] = RowGroupDescriptor.entryMin(descriptor, entry);
    out2[1] = RowGroupDescriptor.entryMax(descriptor, entry);
    return true;
  }

  /** The leaf's descriptor — metadata this store already holds, needing no fetch. */
  public byte[] descriptor(final int leaf) {
    return directories.get(leaf).descriptor();
  }

  public int rowCount(final int leaf) {
    return RowGroupDescriptor.rowCount(directories.get(leaf).descriptor());
  }

  /**
   * Whether the column path can serve {@code col} at all: numeric, boolean, and both string kinds. A
   * per-leaf string column's fill fetches its DICT chain beside the BODY chain — two segment chains
   * instead of one, still only THIS column's bytes, never the whole leaf.
   *
   * <p>
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} is admitted through
   * {@link ProjectionIndexRowGroupPage#isLongLaneKind}: its slice carries dictionary ids in the
   * NUMERIC lane, which is the shape every consumer here already handles. What makes that safe is
   * that a predicate over such a column is resolved to an ID before it ever reaches a kernel (see
   * the executor's leaf conversion), so the numeric evaluator the slice's shape selects is asking
   * the right question. A string literal meeting a long-lane slice is a route defect, not a slow
   * path, and {@link ProjectionColumnScan#evaluateMask} refuses it loudly rather than comparing an
   * unset long against ids.
   */
  public boolean columnSliceable(final int col) {
    return col >= 0 && col < columnKinds.length
        && (ProjectionIndexRowGroupPage.isLongLaneKind(columnKinds[col])
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET);
  }

  /**
   * The column's slices across all leaves (ascending rowGroupId), fetching + decoding its BODY
   * segments on first touch through the CALLER's own live {@code fetcher}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public ColumnSlice[] column(final int col, final ColumnSegmentFetcher fetcher) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable (kind="
          + (col >= 0 && col < columnKinds.length
              ? columnKinds[col]
              : -1)
          + ")");
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

  /**
   * Whether {@code col}'s full slices are ALREADY filled and cached. Lets a caller that would
   * otherwise ask for the cheaper distinct-identity fill reuse what another consumer has paid for,
   * instead of fetching a second chain for the same column.
   */
  public boolean columnFilled(final int col) {
    final ColumnSlice[][] slots = columns;
    return col >= 0 && col < slots.length && slots[col] != null;
  }

  /**
   * A STRING_DICT column's slices in DISTINCT-IDENTITY mode: per-row dict ids beside the
   * {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_DICT_HASHES} segment's precomputed content
   * hashes, with NO dictionary bytes fetched and no FSST decode.
   *
   * <p>
   * For a {@code COUNT(DISTINCT s)} fold that is the whole dictionary: dict ids are leaf-local, so
   * the set member has to be the entry's 64-bit content hash, and nothing else about the string is
   * ever read. On a high-cardinality column the dictionary chain is the dominant cold cost of such a
   * query — the hashes are ~8 B/entry against a dictionary entry's tens of bytes, and cost no decode.
   *
   * <p>
   * FAIL-SOFT per leaf: a leaf whose descriptor lists no hash segment (written before the kind
   * existed) falls back to the full BODY+DICT decode for that leaf alone, and the kernels hash its
   * entries on the fly exactly as before — the identity is the same function over the same bytes, so
   * a mixed column is as correct as a uniform one.
   *
   * <p>
   * The result is NOT published to the full-slice cache: these slices deliberately lack the
   * dictionary every other consumer needs. They get their own cache slot instead.
   *
   * @throws IllegalStateException on missing/corrupt segments, or a column that is not STRING_DICT
   */
  public ColumnSlice[] columnDistinctIdentity(final int col, final ColumnSegmentFetcher fetcher) {
    if (col < 0 || col >= columnKinds.length
        || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " is not STRING_DICT (kind="
          + (col >= 0 && col < columnKinds.length
              ? columnKinds[col]
              : -1)
          + ")");
    }
    // Another consumer already paid for the dictionary: its slices answer identity too (the kernels
    // hash on the fly), and reusing them beats fetching a second chain.
    final ColumnSlice[][] full = columns;
    if (full[col] != null) {
      return full[col];
    }
    final ColumnSlice[][] slots = identityColumns;
    ColumnSlice[] slices = slots[col];
    if (slices != null) {
      return slices;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    slices = fillIdentityColumn(col, fetcher);
    if (slices == null) {
      return column(col, fetcher); // no leaf carries hashes — the whole column falls back
    }
    synchronized (this) {
      final ColumnSlice[] existing = identityColumns[col];
      if (existing != null) {
        return existing;
      }
      final ColumnSlice[][] next = identityColumns.clone();
      next[col] = slices;
      identityColumns = next;
    }
    return slices;
  }

  /**
   * @return the identity slices, or {@code null} when NO leaf carries a hash segment (the caller
   *         falls back to the full fill wholesale rather than decoding dictionaries twice)
   */
  private ColumnSlice @Nullable [] fillIdentityColumn(final int col, final ColumnSegmentFetcher fetcher) {
    final boolean diag = Boolean.getBoolean("sirix.projDiag");
    final long startNanos = diag
        ? System.nanoTime()
        : 0L;
    final int n = directories.size();
    // Which leaves lack the segment — decided from the DESCRIPTORS, which this store already holds,
    // so an OLD projection is recognised before a single page is read (and never confused with a
    // fetch hole). Ordered ahead of any I/O for exactly that reason.
    long[] fallbackWords = null;
    int fallbackLeaves = 0;
    int rowLeaves = 0;
    final int hashSegId = ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(col);
    for (int i = 0; i < n; i++) {
      final byte[] descriptor = directories.get(i).descriptor();
      if (RowGroupDescriptor.rowCount(descriptor) == 0) {
        continue; // a rowless leaf writes neither dictionary nor hashes
      }
      rowLeaves++;
      if (RowGroupDescriptor.entryIndexOf(descriptor, hashSegId) < 0) {
        if (fallbackWords == null) {
          fallbackWords = new long[(n + 63) >>> 6];
        }
        fallbackWords[i >>> 6] |= 1L << (i & 63);
        fallbackLeaves++;
      }
    }
    if (fallbackLeaves == rowLeaves && rowLeaves > 0) {
      if (diag) {
        System.err.println("[proj] identity fill col=" + col + " DECLINED: no leaf of " + rowLeaves
            + " carries a DICT_HASHES segment");
      }
      return null; // no leaf carries hashes — one full fill beats a per-leaf fallback everywhere
    }
    final byte[][] bodySegments = columnBytes(col, fetcher);
    final long tBody = diag
        ? System.nanoTime()
        : 0L;
    final byte[][] hashSegments =
        fetchSegmentChain(col, hashSegId, ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT_HASHES, true, fetcher);
    // Only the leaves that lack hashes pay for a dictionary — the keep-mask is exactly what stops
    // this from degenerating into the full fill it exists to avoid.
    final byte[][] dictSegments = fallbackWords != null
        ? fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher, fallbackWords)
        : null;
    final long tHash = diag
        ? System.nanoTime()
        : 0L;
    final long[] fallback = fallbackWords;
    final ColumnSlice[] slices = new ColumnSlice[n];
    try {
      if (n >= PARALLEL_DECODE_MIN) {
        final AtomicReference<IllegalStateException> failed = new AtomicReference<>();
        IntStream.range(0, n).parallel().forEach(i -> {
          if (failed.get() != null) {
            return;
          }
          try {
            slices[i] = decodeOneIdentitySlice(i, col, bodySegments, hashSegments, dictSegments, fallback);
          } catch (final IllegalStateException corrupt) {
            failed.compareAndSet(null, corrupt);
          }
        });
        final IllegalStateException corrupt = failed.get();
        if (corrupt != null) {
          throw corrupt;
        }
      } else {
        for (int i = 0; i < n; i++) {
          slices[i] = decodeOneIdentitySlice(i, col, bodySegments, hashSegments, dictSegments, fallback);
        }
      }
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    if (diag) {
      final long tDone = System.nanoTime();
      System.err.printf("[proj] identity fill col=%d leaves=%d hashed=%d fallback=%d in %.1f ms | body=%.1f ms "
          + "hash+dict=%.1f ms decode=%.1f ms | t=%.1f..%.1f%n", col, n, rowLeaves - fallbackLeaves, fallbackLeaves,
          (tDone - startNanos) / 1e6, (tBody - startNanos) / 1e6, (tHash - tBody) / 1e6, (tDone - tHash) / 1e6,
          startNanos / 1e6, tDone / 1e6);
    }
    return slices;
  }

  private ColumnSlice decodeOneIdentitySlice(final int i, final int col, final byte[][] bodySegments,
      final byte[] @Nullable [] hashSegments, final byte[] @Nullable [] dictSegments,
      final long @Nullable [] fallbackWords) {
    final byte[] descriptor = directories.get(i).descriptor();
    if (fallbackWords != null && (fallbackWords[i >>> 6] & 1L << (i & 63)) != 0) {
      return ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, bodySegments[i], dictSegments[i], col);
    }
    final long[] hashes = hashSegments == null
        ? null
        : ProjectionIndexColumnSegmentCodec.decodeDictHashes(descriptor, hashSegments[i], col);
    return hashes == null
        // A rowless leaf writes neither dictionary nor hashes; the body decode yields the empty slice
        // every evaluator skips, and passing an empty hash array keeps that path allocation-free.
        ? ProjectionIndexColumnSegmentCodec.decodeStringIdentitySlice(descriptor, bodySegments[i], NO_HASHES, col)
        : ProjectionIndexColumnSegmentCodec.decodeStringIdentitySlice(descriptor, bodySegments[i], hashes, col);
  }

  private static final long[] NO_HASHES = new long[0];

  /** Number of row-group leaves this store spans. */
  public int leafCount() {
    return directories.size();
  }

  /** Leaf {@code i}'s zone-map descriptor — the pruning evidence a caller consults pre-fetch. */
  public byte[] leafDescriptor(final int i) {
    return directories.get(i).descriptor();
  }

  /**
   * Per-leaf {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_STRING_BLOOM} payloads for a string
   * column, or {@code null} when the column is not a string kind. Individual entries are {@code null}
   * for leaves without a fingerprint (rowless, or written before the segment kind existed) — the
   * caller keeps those leaves, so an old index simply never prunes.
   */
  private byte @Nullable [] @Nullable [] stringBloomSegments(final int col, final ColumnSegmentFetcher fetcher) {
    if (!columnSliceable(col) || (columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        && columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET)) {
      return null;
    }
    // Cached like the column fills: the fingerprints are literal-INDEPENDENT, and the morsel
    // chain resolves predicate columns once per worker range — without this cache every range
    // re-fetched the whole ~N-leaf fingerprint chain, which cost more than the pruning saved
    // (measured: S6 381 → ~950 ms). Same double-checked volatile publish as {@link #column}.
    final byte[][][] cached = bloomBytes;
    byte[][] chain = cached[col];
    if (chain != null) {
      return chain;
    }
    chain = fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_STRING_BLOOM, true, fetcher, null);
    synchronized (this) {
      final byte[][] existing = bloomBytes[col];
      if (existing != null) {
        return existing;
      }
      final byte[][][] next = bloomBytes.clone();
      next[col] = chain;
      bloomBytes = next;
    }
    return chain;
  }

  /** Canonical pruned slice: {@code rowCount == 0} short-circuits every evaluator. */
  private static final long[] NO_WORDS = new long[0];
  private static final ColumnSlice PRUNED_SLICE =
      new ColumnSlice(0, (byte) 0, Long.MAX_VALUE, Long.MIN_VALUE, NO_WORDS, null, null, null, null, null);

  /**
   * Like {@link #column(int, ColumnSegmentFetcher)} but fetching and decoding ONLY the leaves set in
   * {@code keepWords} (a bitset over leaf indices); dropped leaves yield a shared
   * {@code rowCount == 0} slice that every evaluator already skips. The result is predicate-specific,
   * so it is NOT published to the column cache.
   */
  public ColumnSlice[] columnMasked(final int col, final ColumnSegmentFetcher fetcher, final long[] keepWords) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable (kind="
          + (col >= 0 && col < columnKinds.length
              ? columnKinds[col]
              : -1)
          + ")");
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    final byte[][] segments = fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY, false, fetcher, keepWords);
    final boolean set = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
    final boolean string = set || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final byte[][] dictSegments = string
        ? fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher, keepWords)
        : null;
    final int n = directories.size();
    final ColumnSlice[] slices = new ColumnSlice[n];
    try {
      if (n >= PARALLEL_DECODE_MIN) {
        // Across the common pool, exactly as the unmasked fill decodes — and for the SAME reason,
        // with more at stake: masked slices are predicate-specific and therefore never cached, so
        // every execution of a pruning query pays this decode again. Serially it is the whole hot
        // time of a repeated group-by (measured: ~1.33 s of a 1.35 s JSONBench Q4 at 100M rows,
        // 97,656 leaves, on the CALLING thread while twenty scan workers waited for it).
        final AtomicReference<IllegalStateException> failed = new AtomicReference<>();
        IntStream.range(0, n).parallel().forEach(i -> {
          if (failed.get() != null) {
            return;
          }
          try {
            slices[i] = decodeMaskedSlice(i, col, set, string, segments, dictSegments, keepWords);
          } catch (final IllegalStateException corrupt) {
            failed.compareAndSet(null, corrupt);
          }
        });
        final IllegalStateException corrupt = failed.get();
        if (corrupt != null) {
          throw corrupt;
        }
      } else {
        for (int i = 0; i < n; i++) {
          slices[i] = decodeMaskedSlice(i, col, set, string, segments, dictSegments, keepWords);
        }
      }
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    return slices;
  }

  /** One leaf of a masked fill: the canonical pruned slice, or the same decode the full fill does. */
  private ColumnSlice decodeMaskedSlice(final int i, final int col, final boolean set, final boolean string,
      final byte[][] segments, final byte[][] dictSegments, final long[] keepWords) {
    if ((keepWords[i >>> 6] & 1L << (i & 63)) == 0) {
      return PRUNED_SLICE;
    }
    return decodeOneSlice(i, col, set, string, segments, dictSegments);
  }

  private ColumnSlice[] fillColumn(final int col, final ColumnSegmentFetcher fetcher) {
    final long tEnter = FILL_DIAG
        ? System.nanoTime()
        : 0L;
    // Bytes-first: the raw-segment cache does the fetch + verification; slice decode is a
    // pure in-memory transform over the already-verified bytes.
    final byte[][] segments = columnBytes(col, fetcher);
    final long tBody = FILL_DIAG
        ? System.nanoTime()
        : 0L;
    // A string column needs its DICT chain beside the BODY chain — ids without the dictionary
    // are meaningless. The chain is OPTIONAL per leaf: a rowless leaf writes no DICT segment.
    final boolean set = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
    final boolean string = set || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final byte[][] dictSegments = string
        ? fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher)
        : null;
    final long tDict = FILL_DIAG
        ? System.nanoTime()
        : 0L;
    final int n = directories.size();
    final ColumnSlice[] slices = new ColumnSlice[n];
    try {
      if (n >= PARALLEL_DECODE_MIN) {
        // The per-leaf decode is a pure in-memory transform over already-verified bytes
        // (no shared state) — and for FSST string dictionaries it is the COLD whale: a
        // serial fill measured ~120 ms behind the 15-worker whole-leaf assembly on a 1M-row
        // URL grouping. Decode across the common pool; first failure wins the rethrow.
        final AtomicReference<IllegalStateException> failed = new AtomicReference<>();
        IntStream.range(0, n).parallel().forEach(i -> {
          if (failed.get() != null) {
            return;
          }
          try {
            slices[i] = decodeOneSlice(i, col, set, string, segments, dictSegments);
          } catch (final IllegalStateException corrupt) {
            failed.compareAndSet(null, corrupt);
          }
        });
        final IllegalStateException corrupt = failed.get();
        if (corrupt != null) {
          throw corrupt;
        }
      } else {
        for (int i = 0; i < n; i++) {
          slices[i] = decodeOneSlice(i, col, set, string, segments, dictSegments);
        }
      }
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    if (FILL_DIAG) {
      final long tDone = System.nanoTime();
      long decoded = 0;
      long raw = 0;
      for (final ColumnSlice s : slices) {
        decoded += sliceRetainedBytes(s);
      }
      for (final byte[] b : segments) {
        raw += b == null ? 0 : b.length;
      }
      if (dictSegments != null) {
        for (final byte[] b : dictSegments) {
          raw += b == null ? 0 : b.length;
        }
      }
      final long total = FILL_DECODED_BYTES.addAndGet(decoded);
      final long rawTotal = FILL_RAW_BYTES.addAndGet(raw);
      System.err.printf("[fill] col=%d kind=%d leaves=%d decoded=%.1f MB raw=%.1f MB | body=%.1f ms dict=%.1f ms "
          + "decode=%.1f ms (par=%s) | t=%.1f..%.1f | store totals: decoded=%.1f MB raw=%.1f MB%n", col,
          columnKinds[col], n, decoded / 1048576.0, raw / 1048576.0, (tBody - tEnter) / 1e6, (tDict - tBody) / 1e6,
          (tDone - tDict) / 1e6, n >= PARALLEL_DECODE_MIN, tEnter / 1e6, tDone / 1e6, total / 1048576.0,
          rawTotal / 1048576.0);
    }
    return slices;
  }

  /**
   * {@code -Dsirix.projection.fillDiag=true} reports what each column fill retains. A filled column is
   * held for the store's lifetime, so at large leaf counts the fills — not the query's own working set
   * — dominate the heap, and nothing else makes that visible.
   */
  private static final boolean FILL_DIAG = Boolean.getBoolean("sirix.projection.fillDiag");

  /** Running total of decoded slice bytes retained across every store, for {@link #FILL_DIAG}. */
  private static final java.util.concurrent.atomic.AtomicLong FILL_DECODED_BYTES =
      new java.util.concurrent.atomic.AtomicLong();

  /** Running total of raw segment bytes retained across every store, for {@link #FILL_DIAG}. */
  private static final java.util.concurrent.atomic.AtomicLong FILL_RAW_BYTES =
      new java.util.concurrent.atomic.AtomicLong();

  private static long sliceRetainedBytes(final ColumnSlice s) {
    if (s == null) {
      return 0;
    }
    long b = 0;
    b += s.presenceWords() == null ? 0 : s.presenceWords().length * 8L;
    b += s.numericValues() == null ? 0 : s.numericValues().length * 8L;
    b += s.boolWords() == null ? 0 : s.boolWords().length * 8L;
    b += s.stringDictIds() == null ? 0 : s.stringDictIds().length * 4L;
    b += s.dictBytes() == null ? 0 : s.dictBytes().length;
    b += s.dictOffsets() == null ? 0 : s.dictOffsets().length * 4L;
    b += s.setCounts() == null ? 0 : s.setCounts().length * 4L;
    b += s.dictHashes() == null ? 0 : s.dictHashes().length * 8L;
    return b;
  }

  /** Leaves below this count decode serially — fork/join overhead beats the win. */
  private static final int PARALLEL_DECODE_MIN = 128;

  private ColumnSlice decodeOneSlice(final int i, final int col, final boolean set, final boolean string,
      final byte[][] segments, final byte[] @Nullable [] dictSegments) {
    final byte[] descriptor = directories.get(i).descriptor();
    return set
        ? ProjectionIndexColumnSegmentCodec.decodeStringSetSlice(descriptor, segments[i], dictSegments[i], col)
        : string
            ? ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, segments[i], dictSegments[i], col)
            : ProjectionIndexColumnSegmentCodec.decodeBodySlice(descriptor, segments[i], col);
  }

  /**
   * Per-leaf {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_SET_COUNTS} payloads.
   *
   * <p>
   * These segments are forced INLINE, so their bytes live in the descriptors this store already holds
   * and {@code fetchAll} is handed nothing to fetch — the whole chain resolves without a page read.
   * That is the difference between this and {@link #dictRowCounts}, which needs the dictionary
   * segment and therefore a page per leaf.
   *
   * @return one payload per leaf, or {@code null} when the column has none
   */
  public byte[][] setCountsSegments(final int col, final ColumnSegmentFetcher fetcher) {
    if (!columnSliceable(col) || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      return null;
    }
    return fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_SET_COUNTS, true, fetcher);
  }

  /**
   * Per-leaf dictionaries WITH their per-value row counts, fetching ONLY the DICT chain.
   *
   * <p>
   * The point of the counts living in the dictionary: a bare membership count needs the dictionary to
   * resolve its literal and nothing else, so this never touches the BODY segments — the per-row
   * cardinalities and the flat element run, which are the bulk of the column. On the movies corpus
   * that is a 41-entry dictionary per leaf instead of ~6.2M elements in total.
   *
   * @return one entry per leaf, ascending rowGroupId; an entry is {@code null} for a rowless leaf,
   *         and its {@code rowCounts} is {@code null} when that leaf predates the counts
   */
  public ProjectionIndexColumnSegmentCodec.DictWithRowCounts[] dictRowCounts(final int col,
      final ColumnSegmentFetcher fetcher) {
    if (!columnSliceable(col) || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      return null;
    }
    final byte[][] dictSegments = fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher);
    if (dictSegments == null) {
      return null;
    }
    final int n = directories.size();
    final var out = new ProjectionIndexColumnSegmentCodec.DictWithRowCounts[n];
    for (int i = 0; i < n; i++) {
      out[i] = ProjectionIndexColumnSegmentCodec.decodeDictWithRowCounts(directories.get(i).descriptor(),
          dictSegments[i], col);
    }
    return out;
  }

  /**
   * The column's VERIFIED raw BODY segment bytes across all leaves (ascending rowGroupId), fetching
   * them on first touch through the CALLER's own live {@code fetcher} — the fused fold kernels'
   * substrate. Same laziness, threading, and failure contract as
   * {@link #column(int, ColumnSegmentFetcher)}.
   *
   * @throws IllegalStateException on missing/corrupt segments or a non-sliceable column
   */
  public byte[][] columnBytes(final int col, final ColumnSegmentFetcher fetcher) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable (kind="
          + (col >= 0 && col < columnKinds.length
              ? columnKinds[col]
              : -1)
          + ")");
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
   * Fill {@code offsets} with each row group's page offset for segment {@code segId}, and return the
   * parallel array of INLINE bytes for the row groups that have no page — {@code null} when the whole
   * column is referenced, which is the common case at scale.
   *
   * <p>
   * An inline entry gets the {@link Constants#NULL_ID_LONG} sentinel in {@code offsets} so the
   * batched fetch skips it (the fetcher yields {@code null} there) and the caller patches its bytes
   * in afterwards. With {@code optional}, a leaf whose descriptor lists no such segment gets the same
   * sentinel and simply stays absent — the shape of a DICT chain, which a rowless leaf never writes;
   * a required chain still refuses the whole fill on a missing entry.
   */
  private byte @Nullable [][] collectColumnOffsets(final int segId, final long[] offsets, final boolean optional,
      final boolean[] absent) {
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
        throw new IllegalStateException("Descriptor of leaf " + dir.rowGroupId() + " lists no segment id " + segId);
      }
      // Segment-slot layout: a bare INLINE segment's bytes were captured at directory build (its
      // zone-map-only descriptor carries no inline region), so they come straight from the directory.
      // Descriptor layout: an inline segment's bytes ride the descriptor's trailing region.
      final byte[] dirInline = dir.inlineBytesAt(entry);
      final byte[] inlineForEntry = dirInline != null
          ? dirInline
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

  /**
   * How many ranges a chain fetch of {@code n} leaves splits into: one (i.e. stay serial) below
   * {@link #PARALLEL_CHAIN_MIN}, else at most one worker per {@link #CHAIN_RANGE_MIN} leaves and never
   * more than there are cores. A small store keeps the single batched call it was tuned for.
   */
  private static int chainWorkers(final int n) {
    // Read per CHAIN, not per leaf — this runs a handful of times per query, so the property
    // lookups are free here and they are what lets a test drive the parallel path without a
    // 97k-leaf fixture. Same pattern the diagnostics in this file already use.
    if (!PARALLEL_CHAIN_FETCH || n < Integer.getInteger("sirix.projection.chainFetchMinLeaves", PARALLEL_CHAIN_MIN)) {
      return 1;
    }
    final int rangeMin = Math.max(1, Integer.getInteger("sirix.projection.chainFetchRangeLeaves", CHAIN_RANGE_MIN));
    return Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), n / rangeMin));
  }

  /**
   * Whether ANY leaf in {@code [from, to)} resolves to a segment PAGE. False for a chain that is
   * inline throughout, and for a range the keep-mask pruned away entirely — neither has anything to
   * fetch, and the fetch is the only step that costs a transaction.
   */
  private static boolean hasResolvableOffset(final long[] offsets, final int from, final int to) {
    for (int i = from; i < to; i++) {
      final long off = offsets[i];
      if (off >= 0 && off != Constants.NULL_ID_LONG) {
        return true;
      }
    }
    return false;
  }

  /** Rollback switch for the parallel chain fetch ({@code -Dsirix.projection.parallelChainFetch=false}). */
  private static final boolean PARALLEL_CHAIN_FETCH =
      !"false".equals(System.getProperty("sirix.projection.parallelChainFetch"));

  /**
   * Chain fetches that actually ran across ranges. A fast path that silently fails to engage looks
   * exactly like one that engages and does not help, so the split reports itself: a test asserts on
   * this rather than on the threshold arithmetic it is trying to prove.
   */
  private static final java.util.concurrent.atomic.AtomicLong PARALLEL_CHAIN_FETCHES =
      new java.util.concurrent.atomic.AtomicLong();

  /** @return how many chain fetches have been split across ranges in this JVM */
  public static long parallelChainFetchCount() {
    return PARALLEL_CHAIN_FETCHES.get();
  }

  /** Below this many leaves a chain fetch stays serial — the fan-out costs more than the walk. */
  private static final int PARALLEL_CHAIN_MIN = 8192;

  /** Fewest leaves a chain-fetch range carries, so the split never degenerates into tiny tasks. */
  private static final int CHAIN_RANGE_MIN = 1024;

  /**
   * {@link #collectColumnOffsets} split across {@code workers} contiguous leaf ranges. Identical
   * output: the ranges are disjoint, {@code offsets}/{@code absent} are written per index, and the
   * inline carrier is allocated at most once and then written per index too.
   */
  private byte @Nullable [][] collectColumnOffsetsParallel(final int segId, final long[] offsets,
      final boolean optional, final boolean[] absent, final int workers) {
    final int n = directories.size();
    final int chunk = (n + workers - 1) / workers;
    final AtomicReference<byte[][]> inline = new AtomicReference<>();
    final AtomicReference<RuntimeException> failed = new AtomicReference<>();
    IntStream.range(0, workers).parallel().forEach(w -> {
      final int from = w * chunk;
      final int to = Math.min(from + chunk, n);
      if (from >= to || failed.get() != null) {
        return;
      }
      try {
        collectColumnOffsetsRange(segId, offsets, optional, absent, from, to, inline, n);
      } catch (final RuntimeException malformed) {
        failed.compareAndSet(null, malformed);
      }
    });
    final RuntimeException malformed = failed.get();
    if (malformed != null) {
      throw malformed;
    }
    return inline.get();
  }

  /** One worker's leaf range of {@link #collectColumnOffsetsParallel}. */
  private void collectColumnOffsetsRange(final int segId, final long[] offsets, final boolean optional,
      final boolean[] absent, final int from, final int to, final AtomicReference<byte[][]> inline, final int n) {
    byte[][] inlineBytes = inline.get();
    for (int i = from; i < to; i++) {
      final RowGroupDirectory dir = directories.get(i);
      final byte[] desc = dir.descriptor();
      final int entry = RowGroupDescriptor.entryIndexOf(desc, segId);
      if (entry < 0) {
        if (optional) {
          absent[i] = true;
          offsets[i] = Constants.NULL_ID_LONG;
          continue;
        }
        throw new IllegalStateException("Descriptor of leaf " + dir.rowGroupId() + " lists no segment id " + segId);
      }
      final byte[] dirInline = dir.inlineBytesAt(entry);
      final byte[] inlineForEntry = dirInline != null
          ? dirInline
          : RowGroupDescriptor.entryIsInline(desc, entry)
              ? RowGroupDescriptor.inlineColumnSegmentBytes(desc, entry)
              : null;
      if (inlineForEntry != null) {
        if (inlineBytes == null) {
          // First inline entry seen by ANY worker allocates the shared carrier; the losers of the
          // race adopt the winner's array. Every later write lands at this worker's own index, so
          // the array is only ever written disjointly.
          final byte[][] fresh = new byte[n][];
          inlineBytes = inline.compareAndSet(null, fresh)
              ? fresh
              : inline.get();
        }
        inlineBytes[i] = inlineForEntry;
        offsets[i] = Constants.NULL_ID_LONG;
      } else {
        offsets[i] = dir.columnSegmentOffsets()[entry];
      }
    }
  }

  /**
   * Fetch every leaf's segment through {@code workers} contiguous ranges at once. Only ever called
   * for a fetcher that declares {@link ColumnSegmentFetcher#rangedFetchIsConcurrent()}, which is what
   * promises each range a transaction of its own.
   */
  private static byte[][] fetchRangesParallel(final ColumnSegmentFetcher fetcher, final long[] offsets, final int n,
      final int workers) {
    final byte[][] segments = new byte[n][];
    final int chunk = (n + workers - 1) / workers;
    final AtomicReference<RuntimeException> failed = new AtomicReference<>();
    IntStream.range(0, workers).parallel().forEach(w -> {
      final int from = w * chunk;
      final int to = Math.min(from + chunk, n);
      if (from >= to || failed.get() != null || !hasResolvableOffset(offsets, from, to)) {
        return; // a range of inline-only or pruned leaves needs no transaction at all
      }
      try {
        fetcher.fetchRange(offsets, from, to, segments);
      } catch (final RuntimeException fetchFailed) {
        failed.compareAndSet(null, fetchFailed);
      }
    });
    final RuntimeException fetchFailed = failed.get();
    if (fetchFailed != null) {
      throw fetchFailed;
    }
    return segments;
  }

  /** {@link #verifyFetchedSegments} split across {@code workers} contiguous leaf ranges. */
  private void verifyFetchedSegmentsParallel(final int n, final byte[][] segments, final int segId, final byte segKind,
      final boolean @Nullable [] absent, final long @Nullable [] keepWords, final int workers) {
    final int chunk = (n + workers - 1) / workers;
    final AtomicReference<IllegalStateException> failed = new AtomicReference<>();
    IntStream.range(0, workers).parallel().forEach(w -> {
      final int from = w * chunk;
      final int to = Math.min(from + chunk, n);
      if (from >= to || failed.get() != null) {
        return;
      }
      for (int i = from; i < to; i++) {
        if (absent != null && absent[i]) {
          continue;
        }
        if (keepWords != null && (keepWords[i >>> 6] & 1L << (i & 63)) == 0) {
          continue;
        }
        try {
          ProjectionIndexColumnSegmentCodec.verifyColumnSegment(directories.get(i).descriptor(), segments[i], segId,
              segKind);
        } catch (final IllegalStateException corrupt) {
          failed.compareAndSet(null, corrupt);
          return;
        }
      }
    });
    final IllegalStateException corrupt = failed.get();
    if (corrupt != null) {
      throw corrupt;
    }
  }

  private byte[][] fetchColumnBytes(final int col, final ColumnSegmentFetcher fetcher) {
    return fetchSegmentChain(col, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY, false, fetcher);
  }

  /**
   * Every leaf's per-row record keys (ascending rowGroupId), fetched and decoded from the KEYS chain
   * on first touch — what a sorted collection returns for lazy materialization, without ever
   * hydrating a whole leaf.
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
        decoded[i] = ProjectionIndexColumnSegmentCodec.decodeKeysSlice(directories.get(i).descriptor(), segments[i]);
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
   * Fetch and verify one segment chain (ascending rowGroupId) for {@code col} — the BODY chain for
   * every sliceable column, and additionally the DICT chain for a string column's fill.
   */
  private byte[][] fetchSegmentChain(final int col, final int segId, final byte segKind, final boolean optional,
      final ColumnSegmentFetcher fetcher) {
    return fetchSegmentChain(col, segId, segKind, optional, fetcher, null);
  }

  /**
   * As above, but when {@code keepWords} is non-null, leaves whose bit is clear are neither fetched
   * (their offset is replaced by the no-page sentinel), patched from the inline region, nor verified
   * — a leaf the caller has already proven irrelevant costs zero I/O and zero CPU.
   */
  private byte[][] fetchSegmentChain(final int col, final int segId, final byte segKind, final boolean optional,
      final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords) {
    final int n = directories.size();
    // Leaf order IS file order to within noise: the builder persists leaves 1..N in one
    // sequential commit, so a column's segment offsets ascend with the leaf index — no
    // explicit sort needed for read locality. One batched fetch = one read transaction.
    // Hybrid: an inline segment carries no page — its bytes come straight from the descriptor, so
    // it is skipped in the offset batch (the NULL_ID_LONG sentinel → the fetcher yields null there)
    // and filled in afterwards. inlineBytes stays null when the whole column is referenced.
    final long[] offsets = new long[n];
    final boolean[] absent = optional
        ? new boolean[n]
        : null;
    // Every step of a chain fetch is per-leaf and independent — the descriptor binary search, the
    // page read, and the integrity check. Serially that is ONE core walking 97k leaves while the
    // other nineteen idle, and at that scale it was measured at 2x the (already parallel) decode it
    // feeds. Ranges are contiguous so the backend's run coalescing survives: only a range boundary
    // loses a merge.
    final int workers = chainWorkers(n);
    if (workers > 1) {
      PARALLEL_CHAIN_FETCHES.incrementAndGet();
    }
    final byte[][] inlineBytes = workers > 1
        ? collectColumnOffsetsParallel(segId, offsets, optional, absent, workers)
        : collectColumnOffsets(segId, offsets, optional, absent);
    if (keepWords != null) {
      dropPrunedLeaves(n, keepWords, offsets, inlineBytes);
    }
    final byte[][] segments;
    try {
      // A chain can be entirely INLINE — every leaf's bytes ride its descriptor and not one page is
      // referenced. Several chains per query are exactly that, and the fetch for them resolved
      // nothing while still opening a read transaction and allocating a reference per leaf. Answer
      // it from the offsets instead: no page to read, no transaction to open.
      segments = !hasResolvableOffset(offsets, 0, n)
          ? new byte[n][]
          : workers > 1 && fetcher.rangedFetchIsConcurrent()
              ? fetchRangesParallel(fetcher, offsets, n, workers)
              : fetcher.fetchAll(offsets);
    } catch (final RuntimeException fetchFailed) {
      // Fetch-level failure (session closed mid-read, transient I/O): NOT memoized —
      // the next query retries against the caller's own live fetcher.
      throw new IllegalStateException("Segment fetch failed for column " + col + ": " + fetchFailed.getMessage(),
          fetchFailed);
    }
    if (segments == null || segments.length != n) {
      throw new IllegalStateException("Segment fetcher returned " + (segments == null
          ? "null"
          : segments.length + " results") + " for " + n + " offsets");
    }
    if (inlineBytes != null) {
      for (int i = 0; i < n; i++) {
        if (inlineBytes[i] != null) {
          segments[i] = inlineBytes[i];
        }
      }
    }
    try {
      if (workers > 1) {
        verifyFetchedSegmentsParallel(n, segments, segId, segKind, absent, keepWords, workers);
      } else {
        verifyFetchedSegments(n, segments, segId, segKind, absent, keepWords);
      }
    } catch (final IllegalStateException corrupt) {
      // Structural corruption (missing segment at a resolved offset, hash/length/kind
      // mismatch) cannot heal for this build — memoize so later touches fail fast.
      if (col >= 0) {
        corruptColumns[col] = 1;
      } else {
        keysCorrupt = true; // the KEYS chain carries no column index
      }
      throw corrupt;
    }
    return segments;
  }

  /**
   * Blank out the leaves the keep-mask excludes, so the batch neither resolves their offsets nor
   * carries their inline bytes — the whole point of pruning is that those pages are never read.
   */
  private static void dropPrunedLeaves(final int n, final long[] keepWords, final long[] offsets,
      final byte[] @Nullable [] inlineBytes) {
    for (int i = 0; i < n; i++) {
      if ((keepWords[i >>> 6] & 1L << (i & 63)) == 0) {
        offsets[i] = Constants.NULL_ID_LONG;
        if (inlineBytes != null) {
          inlineBytes[i] = null;
        }
      }
    }
  }

  /**
   * Check each fetched segment against its descriptor, skipping the leaves that were never fetched:
   * those the descriptor lists no such segment for, and those the keep-mask pruned.
   */
  private void verifyFetchedSegments(final int n, final byte[][] segments, final int segId, final byte segKind,
      final boolean @Nullable [] absent, final long @Nullable [] keepWords) {
    for (int i = 0; i < n; i++) {
      if (absent != null && absent[i]) {
        continue; // the descriptor genuinely lists no such segment for this leaf
      }
      if (keepWords != null && (keepWords[i >>> 6] & 1L << (i & 63)) == 0) {
        continue; // pruned — nothing was fetched, nothing to verify
      }
      ProjectionIndexColumnSegmentCodec.verifyColumnSegment(directories.get(i).descriptor(), segments[i], segId,
          segKind);
    }
  }

  /**
   * Advisory readahead of EVERY sliceable column's segment chains (BODY + DICT), issued as batched
   * span hints through the reader — descriptor-only offset gathering, zero decode. A fresh process
   * otherwise demand-faults these pages column-by-column across its first queries; one background
   * sweep turns that into streaming readahead overlapped with early query compute. Purely a hint:
   * failures are ignored and nothing is retained.
   */
  public void prefetchAllSegments(final StorageEngineReader reader) {
    final boolean diag = Boolean.getBoolean("sirix.projDiag");
    final long startNanos = diag
        ? System.nanoTime()
        : 0L;
    int sweptSegmentChains = 0;
    int hintedPages = 0;
    final int n = directories.size();
    final long[] offsets = new long[n];
    final boolean[] sweepAbsent = new boolean[directories.size()];
    final PageReference[] batch = new PageReference[128];
    int fill = 0;
    for (int col = 0; col < columnKinds.length; col++) {
      if (!columnSliceable(col)) {
        continue;
      }
      final boolean string = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
      // A STRING_DICT column has a third chain — the dict-entry hashes a distinct fold reads INSTEAD
      // of the dictionary. Sweeping it costs ~8 B/entry and is what makes that fold's first touch
      // land in cache; a leaf without the segment contributes no offset (optional collect).
      final int passes = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          ? 3
          : string
              ? 2
              : 1;
      for (int pass = 0; pass < passes; pass++) {
        final int segId = switch (pass) {
          case 0 -> ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
          case 1 -> ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col);
          default -> ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(col);
        };
        try {
          // A real absent array, never null: the optional=true arm WRITES absent[i] for a leaf
          // that lacks the segment, and a null here NPEd on the first such leaf — swallowed by
          // the advisory catch below, silently skipping the whole column's readahead. Absence
          // itself is normal (sparse columns); only the offsets matter to this sweep.
          collectColumnOffsets(segId, offsets, true, sweepAbsent);
        } catch (final RuntimeException ignored) {
          continue; // advisory — a malformed descriptor is the sync path's error to report
        }
        sweptSegmentChains++;
        for (int leaf = 0; leaf < n; leaf++) {
          final long off = offsets[leaf];
          if (off < 0 || off == Constants.NULL_ID_LONG) {
            continue;
          }
          PageReference ref = batch[fill];
          if (ref == null) {
            ref = batch[fill] = new PageReference();
          }
          ref.setKey(off);
          hintedPages++;
          if (++fill == batch.length) {
            reader.prefetchPageSpans(batch, fill);
            fill = 0;
          }
        }
        if (diag) {
          System.err.printf("[prefetchAll] col=%d pass=%d done t=%.1f%n", col, pass, System.nanoTime() / 1e6);
        }
      }
    }
    if (fill > 0) {
      reader.prefetchPageSpans(batch, fill);
    }
    if (diag) {
      final long doneNanos = System.nanoTime();
      System.err.printf("[prefetchAll] swept segment chains: %d pages=%d in %.1f ms | t=%.1f..%.1f%n",
          sweptSegmentChains, hintedPages, (doneNanos - startNanos) / 1e6, startNanos / 1e6, doneNanos / 1e6);
    }
  }

}

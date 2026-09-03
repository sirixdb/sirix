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
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
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
     * Fetch the contiguous sub-range {@code [from, to)} of {@code offsets} into {@code out} at the SAME
     * indices.
     *
     * <p>
     * The default delegates to {@link #fetchAll} on the calling thread — correct, and exactly the
     * behaviour of a fetcher that never heard of ranges. A fetcher that can open a read transaction of
     * its own overrides this and answers {@code true} from {@link #rangedFetchIsConcurrent()}, which is
     * what lets a chain fetch fan its ranges across cores.
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
     * a fetcher bound to one shared transaction is not thread-safe, and a read transaction is the usual
     * binding, so the safe answer has to be the silent one.
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
  private ProjectionBloomChunks.ColumnEvidence @Nullable [] bloomBlocks;

  /** Attach manifest-backed chunks loaded by the catalog. */
  public void attachBloomBlocks(final ProjectionBloomChunks.ColumnEvidence @Nullable [] blocks) {
    this.bloomBlocks = blocks;
  }

  /**
   * Clear {@code keep} bits for leaves whose string-column fingerprint PROVES the literal absent.
   * Evidence order: chunk-manifest blocks, else the per-leaf chain (cached after the first fetch),
   * else nothing — leaves without evidence stay kept.
   *
   * @return number of leaves newly dropped
   */
  public int applyBloomPrune(final int col, final long literalHash, final long[] keep,
      final ColumnSegmentFetcher fetcher) {
    final int n = directories.size();
    int dropped = 0;
    final ProjectionBloomChunks.ColumnEvidence[] blocks = bloomBlocks;
    final ProjectionBloomChunks.ColumnEvidence block = blocks != null
        ? blocks[col]
        : null;
    if (block != null) {
      return block.prune(literalHash, keep, n, fetcher);
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
   * {@link #applyBloomPrune} for MANY literals of ONE column in ONE evidence walk: {@code keeps[j]} is
   * narrowed by {@code hashes[j]}. The chunk blocks are fetched once for all literals (the walk is
   * the whole cost of a prune; probing is three bit tests per literal), so a disjunction of
   * equalities or a planner pricing candidate values costs one prune instead of one per literal.
   * Splits the chunk walk over the common pool when the column spans enough chunks and the fetcher
   * permits concurrent ranged fetches — chunks own disjoint 256-leaf ranges, so two ranges never
   * write the same keep word.
   *
   * @return number of bits newly cleared, summed over every mask
   */
  public long applyBloomPruneMany(final int col, final long[] hashes, final long[][] keeps,
      final ColumnSegmentFetcher fetcher) {
    if (hashes == null || keeps == null || hashes.length != keeps.length) {
      throw new IllegalArgumentException("hashes and keeps must pair up");
    }
    if (fetcher == null) {
      throw new IllegalArgumentException("fetcher is required");
    }
    final int n = directories.size();
    final int words = (n + 63) >>> 6;
    for (final long[] keep : keeps) {
      if (keep == null || keep.length < words) {
        throw new IllegalArgumentException("every keep mask must cover " + n + " leaves");
      }
    }
    if (hashes.length == 0) {
      return 0L;
    }
    final ProjectionBloomChunks.ColumnEvidence[] blocks = bloomBlocks;
    final ProjectionBloomChunks.ColumnEvidence block = blocks != null
        ? blocks[col]
        : null;
    if (block != null) {
      final int chunkCount = block.chunkCount();
      final int ranges = chunkCount < BLOOM_MANY_PARALLEL_MIN_CHUNKS || !fetcher.rangedFetchIsConcurrent()
          ? 1
          : Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(),
              chunkCount / BLOOM_MANY_CHUNKS_PER_RANGE));
      if (ranges <= 1) {
        return block.pruneMany(hashes, keeps, n, fetcher, 0, chunkCount);
      }
      final long[] droppedByRange = new long[ranges];
      final int rangeLen = (chunkCount + ranges - 1) / ranges;
      final AtomicReference<RuntimeException> failed = new AtomicReference<>();
      IntStream.range(0, ranges).parallel().forEach(r -> {
        final int from = r * rangeLen;
        final int to = Math.min(from + rangeLen, chunkCount);
        if (from >= to || failed.get() != null) {
          return;
        }
        try {
          droppedByRange[r] = block.pruneMany(hashes, keeps, n, fetcher, from, to);
        } catch (final RuntimeException rangeFailed) {
          failed.compareAndSet(null, rangeFailed);
        }
      });
      final RuntimeException rangeFailed = failed.get();
      if (rangeFailed != null) {
        throw rangeFailed;
      }
      long dropped = 0L;
      for (final long d : droppedByRange) {
        dropped += d;
      }
      return dropped;
    }
    final byte[][] chain = stringBloomSegments(col, fetcher);
    if (chain == null) {
      return 0L;
    }
    long dropped = 0L;
    final int literals = hashes.length;
    for (int i = 0; i < n; i++) {
      final byte[] segment = chain[i];
      if (segment == null) {
        continue;
      }
      final int word = i >>> 6;
      final long bit = 1L << (i & 63);
      long packed = 0L;
      boolean located = false;
      for (int j = 0; j < literals; j++) {
        final long[] keep = keeps[j];
        if ((keep[word] & bit) == 0) {
          continue;
        }
        if (!located) {
          packed = ProjectionIndexColumnSegmentCodec.bloomSegmentWords(segment);
          located = true;
          if (packed == ProjectionIndexColumnSegmentCodec.NO_FINGERPRINT) {
            break;
          }
        }
        if (!ProjectionIndexColumnSegmentCodec.bloomWordsMayContainHash(segment, packed, hashes[j])) {
          keep[word] &= ~bit;
          dropped++;
        }
      }
    }
    return dropped;
  }

  /** Below this many bloom chunks a multi-literal prune stays serial (the fan-out costs more). */
  private static final int BLOOM_MANY_PARALLEL_MIN_CHUNKS = 32;

  /** At least this many chunks per parallel range of a multi-literal prune (one range ≈ 4 fetches). */
  private static final int BLOOM_MANY_CHUNKS_PER_RANGE = 16;

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
   * Lazily computed per STRING_DICT column: the two smallest and two largest REFERENCED values of
   * every leaf, as dict ids and materialized bytes. See {@link #stringValueExtrema}.
   */
  private volatile StringValueExtrema @Nullable [] stringExtrema;

  /**
   * Lazily computed per numeric-lane column (the ordered-long kinds and STRING_GLOBAL): the two
   * smallest and two largest PRESENT long values of every leaf. See {@link #longValueExtrema}.
   */
  private volatile LongValueExtrema @Nullable [] longExtrema;

  /**
   * Lazily computed per column: bit {@code leaf} set when the column is ALL-PRESENT on that leaf
   * (rowless leaves vacuously). See {@link #allPresentLeaves}.
   */
  private volatile long[] @Nullable [] allPresentLeaves;

  /** Lazily computed per column: the descriptors' zone mirrors, leaf-indexed. See {@link #zoneIndex}. */
  private volatile ZoneIndex @Nullable [] zoneIndices;

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
      RowGroupDescriptor.validate(d0);
      final int columnCount = RowGroupDescriptor.columnCount(d0);
      this.columnKinds = new byte[columnCount];
      for (int c = 0; c < columnCount; c++) {
        this.columnKinds[c] = RowGroupDescriptor.kind(d0, c);
      }
      // Leaf 0 is a SAMPLE of the store's shape, and the twenty-five dispatch sites below treat it
      // as the definition. Check the rest agree, once, here — the descriptors are already in memory
      // (no page read) and the kinds are contiguous, so it is one range comparison per leaf.
      //
      // Task #45: commit-time maintenance rebuilt one leaf from the DECLARED column kinds and wrote
      // its descriptor as STRING_DICT while the payload and the metadata stayed STRING_GLOBAL. Every
      // guard downstream passed, because each was asking leaf 0. The wrong decode surfaced four
      // rounds later as "known-corrupt BODY segment", which named the wrong component and the wrong
      // problem. Refusing HERE reports the actual disagreement, at the only place that can see it.
      verifyEveryLeafAgreesWithLeafZero(d0);
    }
    this.columns = new ColumnSlice[columnKinds.length][];
    this.identityColumns = new ColumnSlice[columnKinds.length][];
    this.columnBytes = new byte[columnKinds.length][][];
    this.bloomBytes = new byte[columnKinds.length][][];
    this.stringExtrema = new StringValueExtrema[columnKinds.length];
    this.longExtrema = new LongValueExtrema[columnKinds.length];
    this.allPresentLeaves = new long[columnKinds.length][];
    this.zoneIndices = new ZoneIndex[columnKinds.length];
    this.stringSupplementary = new byte[columnKinds.length];
    this.corruptColumns = new byte[columnKinds.length];
    this.residencyPins = new AtomicIntegerArray(columnKinds.length + 1);
    this.chargedSliceBytes = new long[columnKinds.length];
    this.chargedIdentityBytes = new long[columnKinds.length];
    this.chargedBodyBytes = new long[columnKinds.length];
  }

  /**
   * Establish the invariant the whole class dispatches on: every leaf declares the same column
   * encodings.
   *
   * <p>
   * Throws rather than degrading, and that is deliberate. A kind-inconsistent store is not a store
   * with one bad column — its leaves disagree about what the bytes MEAN, so no route over it can be
   * trusted, including the ones that would otherwise "fall back". The throw is typed
   * ({@link ProjectionStoreInconsistentException}) precisely so the fallback machinery can tell it
   * apart from undecodable bytes and decline the whole store WITHOUT memoizing a column as corrupt
   * (task #50) — the memo is what turned one mis-dispatch into a permanently disabled column.
   * </p>
   *
   * @param d0 leaf 0's descriptor, already read by the caller; never {@code null}
   */
  private void verifyEveryLeafAgreesWithLeafZero(final byte[] d0) {
    final int leaves = directories.size();
    for (int leaf = 1; leaf < leaves; leaf++) {
      final byte[] di = directories.get(leaf).descriptor();
      RowGroupDescriptor.validate(di);
      if (!RowGroupDescriptor.kindsAgree(d0, di)) {
        throw new ProjectionStoreInconsistentException(leaf, describeDisagreement(d0, di));
      }
    }
  }

  /**
   * Name the first column the two descriptors disagree about, so the refusal says what is wrong
   * instead of that something is. Cold path — only ever reached on the way to throwing.
   */
  private static String describeDisagreement(final byte[] d0, final byte[] di) {
    final int columns = RowGroupDescriptor.columnCount(d0);
    final int otherColumns = RowGroupDescriptor.columnCount(di);
    if (columns != otherColumns) {
      return "leaf 0 declares " + columns + " columns, this leaf declares " + otherColumns;
    }
    for (int c = 0; c < columns; c++) {
      final byte expected = RowGroupDescriptor.kind(d0, c);
      final byte actual = RowGroupDescriptor.kind(di, c);
      if (expected != actual) {
        return "column " + c + " is kind " + expected + " in leaf 0 but kind " + actual + " here";
      }
    }
    return "column count and kinds agree — the disagreement was transient";
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
   * Per-leaf VALUE extrema of a STRING_DICT column — the two smallest and the two largest REFERENCED
   * values of every leaf, as dict ids and as materialized bytes ({@link StringValueExtrema}). A
   * slice's own {@code min()}/{@code max()} are dict IDS for this kind — meaningless for value
   * order — so an order-aware caller (the sorted scan's best-first plan) needs this instead, and it
   * needs the BYTES: a plan over 97,654 leaves compares extrema ACROSS leaves, and resolving each one
   * through its own leaf's dictionary would need every dictionary decoded — the whole column
   * resident, which is exactly what a bounded top-k must never require.
   *
   * <p>
   * The SECOND extremum is what makes a same-column {@code <>} predicate prunable: under
   * {@code WHERE c <> lit ORDER BY c} a leaf's best MATCHING value is its second-smallest whenever its
   * smallest is the excluded literal (ClickBench q25: the empty string is every leaf's minimum, so
   * on first extrema alone every leaf ties and nothing can be skipped).
   * </p>
   *
   * <p>
   * Referenced-and-present gated, like {@link ProjectionColumnScan#stringDictMinMax}: a dictionary
   * can hold PHANTOM entries no live row points at, and a phantom extremum would weaken every prune
   * built on it. Data-derived and literal-independent, so it is memoized per column and shared by
   * every query — the same publication discipline as {@link #column}. The walk reads the retained
   * slices when the column is resident and otherwise decodes leaf ranges through the fetcher and
   * drops them — it never holds the column — fanned out over the common pool in 64-aligned leaf
   * ranges when the store is large enough for the fan-out to pay ({@link #memoPassRanges}) and, for
   * a fetching pass, the fetcher permits concurrent ranged fetches.
   * </p>
   *
   * @throws IllegalStateException if {@code col} is not a STRING_DICT column
   */
  public StringValueExtrema stringValueExtrema(final int col, final ColumnSegmentFetcher fetcher) {
    if (col < 0 || col >= columnKinds.length
        || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " is not STRING_DICT");
    }
    if (fetcher == null) {
      throw new IllegalArgumentException("fetcher must not be null");
    }
    final StringValueExtrema hit = stringExtrema[col];
    if (hit != null) {
      return hit;
    }
    final long t0 = MEMO_DIAG
        ? System.nanoTime()
        : 0L;
    final int n = directories.size();
    final ColumnSlice[] resident = columns[col];
    final int ranges = resident != null || fetcher.rangedFetchIsConcurrent()
        ? memoPassRanges(n)
        : 1;
    final ExtremaRange[] parts = new ExtremaRange[ranges];
    forEachMemoRange(n, ranges, (from, to, r) -> parts[r] = extremaOfRange(col, from, to, resident, fetcher));
    // Stitch the ranges' entries into one leaf-indexed table: ids and lengths land at 4 * leaf, the
    // bytes at a running prefix, each range's bytes copied once as a block (they are already in
    // entry order within the range).
    long totalBytes = 0L;
    boolean anySupplementary = false;
    for (final ExtremaRange part : parts) {
      if (part == null) {
        continue; // a trailing range the 64-alignment left empty
      }
      totalBytes += part.byteLen;
      anySupplementary |= part.supplementary;
    }
    if (totalBytes > Integer.MAX_VALUE - 16) {
      throw new IllegalStateException("String extrema of column " + col + " exceed " + totalBytes + " bytes");
    }
    final int[] ids = new int[4 * n];
    final int[] offsets = new int[4 * n + 1];
    final byte[] bytes = new byte[(int) totalBytes];
    int entry = 0;
    int running = 0;
    for (final ExtremaRange part : parts) {
      if (part == null) {
        continue;
      }
      final int entries = part.ids.length;
      System.arraycopy(part.ids, 0, ids, entry, entries);
      System.arraycopy(part.bytes, 0, bytes, running, part.byteLen);
      for (int e = 0; e < entries; e++) {
        offsets[entry + e] = running;
        running += part.lengths[e];
      }
      entry += entries;
    }
    offsets[4 * n] = running;
    if (running != totalBytes) {
      throw new IllegalStateException("String extrema stitch mismatch: " + running + " of " + totalBytes + " bytes");
    }
    // A leaf whose sweep short-circuited leaves the column verdict at PRESENT, which is exactly
    // right; NONE is only recorded after every entry of every leaf was read.
    stringSupplementary[col] = anySupplementary
        ? SUPPLEMENTARY_PRESENT
        : SUPPLEMENTARY_NONE;
    final StringValueExtrema built = new StringValueExtrema(n, ids, bytes, offsets);
    final StringValueExtrema published;
    synchronized (this) {
      final StringValueExtrema existing = stringExtrema[col];
      if (existing != null) {
        published = existing;
      } else {
        final StringValueExtrema[] next = stringExtrema.clone();
        next[col] = built;
        stringExtrema = next;
        published = built;
      }
    }
    if (MEMO_DIAG) {
      System.err.printf("[memo] stringExtrema col=%d leaves=%d ranges=%d resident=%b bytes=%d ms=%.2f%n", col, n, ranges,
          resident != null, running, (System.nanoTime() - t0) / 1e6);
    }
    return published;
  }

  /**
   * The memoized value extrema of one STRING_DICT column ({@link ProjectionColumnStore#stringValueExtrema}):
   * four slots per leaf — {@link #MIN1} the smallest referenced value, {@link #MIN2} the second-smallest
   * DISTINCT one, {@link #MAX1} the largest, {@link #MAX2} the second-largest distinct one — each as
   * the leaf's dict id ({@code -1} when the leaf has no such value) with its bytes materialized in
   * one shared array, so two leaves' extrema compare without either leaf's dictionary in memory.
   * Immutable once published.
   */
  public static final class StringValueExtrema {
    public static final int MIN1 = 0;
    public static final int MIN2 = 1;
    public static final int MAX1 = 2;
    public static final int MAX2 = 3;
    private final int leafCount;
    private final int[] ids;
    private final byte[] bytes;
    private final int[] offsets;

    StringValueExtrema(final int leafCount, final int[] ids, final byte[] bytes, final int[] offsets) {
      if (ids.length != 4 * leafCount || offsets.length != 4 * leafCount + 1) {
        throw new IllegalArgumentException("Extrema table shape does not match " + leafCount + " leaves");
      }
      this.leafCount = leafCount;
      this.ids = ids;
      this.bytes = bytes;
      this.offsets = offsets;
    }

    public int leafCount() {
      return leafCount;
    }

    /** The leaf's dict id in {@code slot}, or {@code -1} when the leaf has no such value. */
    public int id(final int leaf, final int slot) {
      return ids[4 * leaf + slot];
    }

    public boolean has(final int leaf, final int slot) {
      return ids[4 * leaf + slot] >= 0;
    }

    /** Start of the slot's value in {@link #bytes()}. */
    public int offset(final int leaf, final int slot) {
      return offsets[4 * leaf + slot];
    }

    /** Length of the slot's value in {@link #bytes()} ({@code 0} for an absent slot). */
    public int length(final int leaf, final int slot) {
      final int e = 4 * leaf + slot;
      return offsets[e + 1] - offsets[e];
    }

    /** The shared value bytes, addressed through {@link #offset}/{@link #length}. Never modified. */
    public byte[] bytes() {
      return bytes;
    }
  }

  /**
   * Per-leaf extrema of a column's LONG lane — the two smallest and the two largest DISTINCT present
   * values of every leaf ({@link LongValueExtrema}) — for the ordered-long kinds and for
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL}, whose lane holds dictionary ids.
   * The numeric twin of {@link #stringValueExtrema}: the descriptor zone already gives a leaf's
   * first extremum for free, but the SECOND one is what a same-column {@code <>} predicate needs to
   * bound what remains ({@code WHERE c <> lit ORDER BY c} with {@code lit} every leaf's minimum — the
   * empty string's id, under a rank-ordered global dictionary, is exactly that at 100M rows), and no
   * descriptor carries it. Exact where the zone may be loose: read from the decoded lane itself.
   *
   * <p>
   * Data-derived and literal-independent, so it is memoized per column and shared by every query.
   * The walk reads the retained slices when the column is resident and otherwise decodes leaf ranges
   * through the fetcher and drops them — it never holds the column — fanned out like
   * {@link #stringValueExtrema}. Rowless and all-missing leaves have no slot set.
   * </p>
   *
   * @throws IllegalStateException if {@code col}'s long lane is not an order
   */
  public LongValueExtrema longValueExtrema(final int col, final ColumnSegmentFetcher fetcher) {
    if (col < 0 || col >= columnKinds.length || !ProjectionColumnScan.zonePrunableKind(columnKinds[col])) {
      throw new IllegalStateException("Column " + col + " has no ordered long lane");
    }
    if (fetcher == null) {
      throw new IllegalArgumentException("fetcher must not be null");
    }
    final LongValueExtrema hit = longExtrema[col];
    if (hit != null) {
      return hit;
    }
    final long t0 = MEMO_DIAG
        ? System.nanoTime()
        : 0L;
    final int n = directories.size();
    final ColumnSlice[] resident = columns[col];
    final int ranges = resident != null || fetcher.rangedFetchIsConcurrent()
        ? memoPassRanges(n)
        : 1;
    // Every range writes only its own leaves' four slots and its own presence words (range bounds
    // are multiples of 64 leaves, so 4 * leaf slots never share a word across ranges either).
    final long[] values = new long[4 * n];
    final long[] present = new long[(4 * n + 63) >>> 6];
    forEachMemoRange(n, ranges, (from, to, r) -> longExtremaOfRange(col, from, to, resident, fetcher, values, present));
    final LongValueExtrema built = new LongValueExtrema(n, values, present);
    final LongValueExtrema published;
    synchronized (this) {
      final LongValueExtrema existing = longExtrema[col];
      if (existing != null) {
        published = existing;
      } else {
        final LongValueExtrema[] next = longExtrema.clone();
        next[col] = built;
        longExtrema = next;
        published = built;
      }
    }
    if (MEMO_DIAG) {
      System.err.printf("[memo] longExtrema col=%d leaves=%d ranges=%d resident=%b ms=%.2f%n", col, n, ranges,
          resident != null, (System.nanoTime() - t0) / 1e6);
    }
    return published;
  }

  /**
   * The memoized long-lane extrema of one column ({@link ProjectionColumnStore#longValueExtrema}): four
   * slots per leaf in {@link StringValueExtrema}'s slot order — {@link StringValueExtrema#MIN1} the
   * smallest present value, {@link StringValueExtrema#MIN2} the second-smallest DISTINCT one,
   * {@link StringValueExtrema#MAX1} the largest, {@link StringValueExtrema#MAX2} the second-largest
   * distinct one — each flagged present or absent. Immutable once published.
   */
  public static final class LongValueExtrema {
    private final int leafCount;
    private final long[] values;
    private final long[] present;

    LongValueExtrema(final int leafCount, final long[] values, final long[] present) {
      if (values.length != 4 * leafCount || present.length != (4 * leafCount + 63) >>> 6) {
        throw new IllegalArgumentException("Extrema table shape does not match " + leafCount + " leaves");
      }
      this.leafCount = leafCount;
      this.values = values;
      this.present = present;
    }

    public int leafCount() {
      return leafCount;
    }

    /** Whether the leaf has a value in {@code slot} (a leaf with one distinct value has no second slot). */
    public boolean has(final int leaf, final int slot) {
      final int e = 4 * leaf + slot;
      return (present[e >>> 6] & 1L << (e & 63)) != 0L;
    }

    /** The leaf's value in {@code slot}; meaningful only when {@link #has} holds. */
    public long value(final int leaf, final int slot) {
      return values[4 * leaf + slot];
    }
  }

  private void longExtremaOfRange(final int col, final int from, final int to, final ColumnSlice @Nullable [] resident,
      final ColumnSegmentFetcher fetcher, final long[] values, final long[] present) {
    if (resident != null) {
      for (int leaf = from; leaf < to; leaf++) {
        longExtremaOfLeaf(resident[leaf], leaf, values, present);
      }
      return;
    }
    for (int w = from; w < to; w += MEMO_DECODE_WINDOW) {
      final int wTo = Math.min(w + MEMO_DECODE_WINDOW, to);
      final ColumnSlice[] decoded = decodeLeafSlices(col, null, w, wTo - w, fetcher);
      for (int i = 0; i < decoded.length; i++) {
        longExtremaOfLeaf(decoded[i], w + i, values, present);
      }
    }
  }

  /**
   * One leaf's contribution to {@link #longValueExtrema}: its smallest, second-smallest, largest and
   * second-largest DISTINCT present long values into slots {@code 4 * leaf + slot}. Writes only its
   * own slots and presence bits (a range owns whole 64-leaf blocks, hence whole presence words), so
   * ranges run concurrently without coordination. One pass over the lane, branch-light: the two
   * candidates per side are kept as running values and a distinct-count of how many are set.
   */
  private static void longExtremaOfLeaf(final @Nullable ColumnSlice slice, final int leaf, final long[] values,
      final long[] present) {
    if (slice == null || slice.rowCount() <= 0) {
      return;
    }
    final long[] lane = slice.numericValues();
    if (lane == null) {
      return; // no long lane: the caller sees the slots absent and treats the leaf as unbounded
    }
    final long[] presence = slice.presenceWords();
    final int rowCount = slice.rowCount();
    long min1 = Long.MAX_VALUE;
    long min2 = Long.MAX_VALUE;
    long max1 = Long.MIN_VALUE;
    long max2 = Long.MIN_VALUE;
    int distinctSeen = 0; // 0, 1, or 2+ — enough to tell whether a second slot exists
    for (int r = 0; r < rowCount; r++) {
      if (presence != null && (presence[r >>> 6] & 1L << (r & 63)) == 0L) {
        continue;
      }
      final long v = lane[r];
      if (distinctSeen == 0) {
        min1 = v;
        max1 = v;
        distinctSeen = 1;
        continue;
      }
      if (v < min1) {
        min2 = min1;
        min1 = v;
        distinctSeen = 2;
      } else if (v > min1 && v < min2) {
        min2 = v;
        distinctSeen = 2;
      }
      if (v > max1) {
        max2 = max1;
        max1 = v;
        distinctSeen = 2;
      } else if (v < max1 && v > max2) {
        max2 = v;
        distinctSeen = 2;
      }
    }
    if (distinctSeen == 0) {
      return;
    }
    final int base = 4 * leaf;
    values[base + StringValueExtrema.MIN1] = min1;
    values[base + StringValueExtrema.MAX1] = max1;
    setSlot(present, base + StringValueExtrema.MIN1);
    setSlot(present, base + StringValueExtrema.MAX1);
    if (distinctSeen == 2) {
      // With two distinct values both second slots are set: the second-smallest and the
      // second-largest exist (they may be the same value, or the other side's first).
      values[base + StringValueExtrema.MIN2] = min2;
      values[base + StringValueExtrema.MAX2] = max2;
      setSlot(present, base + StringValueExtrema.MIN2);
      setSlot(present, base + StringValueExtrema.MAX2);
    }
  }

  private static void setSlot(final long[] present, final int slot) {
    present[slot >>> 6] |= 1L << (slot & 63);
  }

  /** One contiguous leaf range's share of a {@link #stringValueExtrema} pass. */
  private static final class ExtremaRange {
    /** Four per leaf of the range ({@link StringValueExtrema} slot order), {@code -1} when absent. */
    final int[] ids;
    /** Byte length of each entry ({@code 0} when absent), parallel to {@link #ids}. */
    final int[] lengths;
    /** The entries' bytes in entry order; {@code byteLen} of them are live. */
    byte[] bytes;
    int byteLen;
    /** Whether any dictionary entry of the range holds a supplementary character. */
    boolean supplementary;

    ExtremaRange(final int leaves) {
      this.ids = new int[4 * leaves];
      this.lengths = new int[4 * leaves];
      this.bytes = new byte[Math.max(256, 64 * leaves)];
    }

    void append(final byte[] src, final int off, final int len) {
      if (byteLen + len > bytes.length) {
        bytes = Arrays.copyOf(bytes, Math.max(bytes.length << 1, byteLen + len));
      }
      System.arraycopy(src, off, bytes, byteLen, len);
      byteLen += len;
    }
  }

  /** Leaves per decode batch of a memo pass that has to fetch: bounds the bytes a worker holds at once. */
  private static final int MEMO_DECODE_WINDOW = 256;

  private ExtremaRange extremaOfRange(final int col, final int from, final int to, final ColumnSlice @Nullable [] resident,
      final ColumnSegmentFetcher fetcher) {
    final ExtremaRange part = new ExtremaRange(to - from);
    if (resident != null) {
      for (int leaf = from; leaf < to; leaf++) {
        extremaOfLeaf(resident[leaf], leaf - from, part);
      }
      return part;
    }
    for (int w = from; w < to; w += MEMO_DECODE_WINDOW) {
      final int wTo = Math.min(w + MEMO_DECODE_WINDOW, to);
      final ColumnSlice[] decoded = decodeLeafSlices(col, null, w, wTo - w, fetcher);
      for (int i = 0; i < decoded.length; i++) {
        extremaOfLeaf(decoded[i], w + i - from, part);
      }
    }
    return part;
  }

  /**
   * One leaf's contribution to {@link #stringValueExtrema}: its smallest, second-smallest, largest and
   * second-largest REFERENCED dictionary entries into the range's slots {@code 4 * local + slot}
   * ({@code -1} where the leaf has no such value), their bytes appended in slot order, and whether its
   * dictionary holds a supplementary character folded into the range's verdict. Writes only its own
   * slots and reads only its own immutable slice, so ranges run concurrently without coordination.
   */
  private static void extremaOfLeaf(final @Nullable ColumnSlice slice, final int local, final ExtremaRange part) {
    final int base = 4 * local;
    Arrays.fill(part.ids, base, base + 4, -1);
    if (slice == null || slice.rowCount() <= 0) {
      return;
    }
    final byte[] dictBytes = slice.dictBytes();
    final int[] dictOffsets = slice.dictOffsets();
    final int[] rowIds = slice.stringDictIds();
    if (dictBytes == null || dictOffsets == null || rowIds == null) {
      return; // no dict lanes: the caller sees -1 and treats the leaf as unbounded
    }
    final int dictSize = dictOffsets.length - 1;
    if (dictSize == 0) {
      return;
    }
    // Scratch is per leaf rather than reused across them: the walk runs once per column, so a
    // 128-byte bitset per leaf costs nothing, and sharing one is the only thing that would stand
    // between the ranges and running concurrently.
    final long[] referenced = new long[(dictSize + 63) >>> 6];
    final long[] presence = slice.presenceWords();
    final int rowCount = slice.rowCount();
    for (int r = 0; r < rowCount; r++) {
      if (presence != null && (presence[r >>> 6] & 1L << (r & 63)) == 0L) {
        continue;
      }
      final int id = rowIds[r];
      referenced[id >>> 6] |= 1L << (id & 63);
    }
    // One pass over this leaf's entries settles the collation gate for all of its comparisons.
    boolean supplementary = false;
    for (int i = 0; i < dictSize && !supplementary; i++) {
      supplementary =
          ProjectionIndexScan.hasFourByteUtf8(dictBytes, dictOffsets[i], dictOffsets[i + 1] - dictOffsets[i]);
    }
    part.supplementary |= supplementary;
    int min1 = -1;
    int min2 = -1;
    int max1 = -1;
    int max2 = -1;
    for (int i = 0; i < dictSize; i++) {
      if ((referenced[i >>> 6] & 1L << (i & 63)) == 0L) {
        continue;
      }
      if (min1 < 0) {
        min1 = i;
        max1 = i;
        continue;
      }
      // Two DISTINCT extrema per side: an entry equal to the current first stays out of the second
      // slot (a dictionary is not obliged to be duplicate-free), and an entry equal to the current
      // second changes nothing.
      final int cmpMin = compareDictEntries(dictBytes, dictOffsets, i, min1, supplementary);
      if (cmpMin < 0) {
        min2 = min1;
        min1 = i;
      } else if (cmpMin > 0 && (min2 < 0 || compareDictEntries(dictBytes, dictOffsets, i, min2, supplementary) < 0)) {
        min2 = i;
      }
      final int cmpMax = compareDictEntries(dictBytes, dictOffsets, i, max1, supplementary);
      if (cmpMax > 0) {
        max2 = max1;
        max1 = i;
      } else if (cmpMax < 0 && (max2 < 0 || compareDictEntries(dictBytes, dictOffsets, i, max2, supplementary) > 0)) {
        max2 = i;
      }
    }
    part.ids[base + StringValueExtrema.MIN1] = min1;
    part.ids[base + StringValueExtrema.MIN2] = min2;
    part.ids[base + StringValueExtrema.MAX1] = max1;
    part.ids[base + StringValueExtrema.MAX2] = max2;
    for (int slot = 0; slot < 4; slot++) {
      final int id = part.ids[base + slot];
      if (id < 0) {
        continue;
      }
      final int off = dictOffsets[id];
      final int len = dictOffsets[id + 1] - off;
      part.lengths[base + slot] = len;
      part.append(dictBytes, off, len);
    }
  }

  /**
   * Order of two entries of ONE dictionary under the interpreter's collation: unsigned UTF-8 bytes,
   * decoded comparison as soon as a supplementary character is in the dictionary — the gate every
   * dict kernel uses.
   */
  private static int compareDictEntries(final byte[] dictBytes, final int[] dictOffsets, final int a, final int b,
      final boolean supplementary) {
    final int aOff = dictOffsets[a];
    final int aLen = dictOffsets[a + 1] - aOff;
    final int bOff = dictOffsets[b];
    final int bLen = dictOffsets[b + 1] - bOff;
    return supplementary
        ? ProjectionIndexByteScan.compareStrSlices(dictBytes, aOff, aLen, dictBytes, bOff, bLen)
        : Arrays.compareUnsigned(dictBytes, aOff, aOff + aLen, dictBytes, bOff, bOff + bLen);
  }

  /**
   * Bitset over leaves: bit {@code leaf} set when {@code col} is ALL-PRESENT on the leaf — every row
   * carries a value (a rowless leaf vacuously). The truth costs ONE read of the column's BODY chain
   * the first time it is asked per column: the encoder writes a one-byte presence marker ahead of the
   * payload ({@link ProjectionIndexColumnSegmentCodec#bodyAllPresent}), so the pass reads the marker
   * of each verified segment and retains nothing; a resident column answers from its slices, a
   * byte-cached one from its cached segments, at no I/O. Memoized per column: the answer is a
   * property of the data, shared by every query.
   *
   * <p>
   * What it buys: the sorted scan's best-first plan may skip a leaf on its zone bound only once it
   * knows no matching row there can hide a MISSING order key (such a row obliges the whole scan to
   * decline to the interpreter). That precondition used to be established by DECODING every leaf's
   * order columns — the whole column, window by window, before a single row was compared — which is
   * what made a windowed top-k over 97,654 leaves cost seconds for ten rows.
   * </p>
   */
  public long[] allPresentLeaves(final int col, final ColumnSegmentFetcher fetcher) {
    if (col < 0 || col >= columnKinds.length) {
      throw new IllegalArgumentException("column " + col + " of " + columnKinds.length);
    }
    if (fetcher == null) {
      throw new IllegalArgumentException("fetcher must not be null");
    }
    final long[] hit = allPresentLeaves[col];
    if (hit != null) {
      return hit;
    }
    final long t0 = MEMO_DIAG
        ? System.nanoTime()
        : 0L;
    final int n = directories.size();
    final long[] bits = new long[(n + 63) >>> 6];
    final ColumnSlice[] resident = columns[col];
    final byte[][] retainedBytes = resident == null
        ? completeChainOrNull(columnBytes[col], n)
        : null;
    final String source;
    if (resident != null) {
      source = "slices";
      forEachMemoRange(n, memoPassRanges(n), (from, to, r) -> {
        for (int leaf = from; leaf < to; leaf++) {
          final ColumnSlice slice = resident[leaf];
          if (slice.rowCount() <= 0
              || ProjectionIndexColumnSegmentCodec.allPresent(slice.presenceWords(), slice.rowCount())) {
            bits[leaf >>> 6] |= 1L << (leaf & 63);
          }
        }
      });
    } else if (retainedBytes != null) {
      source = "bytes";
      forEachMemoRange(n, memoPassRanges(n), (from, to, r) -> {
        for (int leaf = from; leaf < to; leaf++) {
          if (RowGroupDescriptor.rowCount(directories.get(leaf).descriptor()) <= 0
              || ProjectionIndexColumnSegmentCodec.bodyAllPresent(retainedBytes[leaf])) {
            bits[leaf >>> 6] |= 1L << (leaf & 63);
          }
        }
      });
    } else {
      source = "fetch";
      final int segId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
      final int ranges = fetcher.rangedFetchIsConcurrent()
          ? memoPassRanges(n)
          : 1;
      forEachMemoRange(n, ranges, (from, to, r) -> {
        for (int w = from; w < to; w += MEMO_FETCH_WINDOW) {
          final int wTo = Math.min(w + MEMO_FETCH_WINDOW, to);
          final byte[][] bodies = fetchLeafSegments(null, w, wTo - w, segId,
              ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY, false, fetcher);
          for (int i = 0; i < bodies.length; i++) {
            final int leaf = w + i;
            if (RowGroupDescriptor.rowCount(directories.get(leaf).descriptor()) <= 0
                || ProjectionIndexColumnSegmentCodec.bodyAllPresent(bodies[i])) {
              bits[leaf >>> 6] |= 1L << (leaf & 63);
            }
          }
        }
      });
    }
    final long[] published;
    synchronized (this) {
      final long[] existing = allPresentLeaves[col];
      if (existing != null) {
        published = existing;
      } else {
        final long[][] next = allPresentLeaves.clone();
        next[col] = bits;
        allPresentLeaves = next;
        published = bits;
      }
    }
    if (MEMO_DIAG) {
      int set = 0;
      for (final long word : bits) {
        set += Long.bitCount(word);
      }
      System.err.printf("[memo] allPresent col=%d leaves=%d allPresent=%d source=%s ms=%.2f%n", col, n, set, source,
          (System.nanoTime() - t0) / 1e6);
    }
    return published;
  }

  /** Leaves per fetch batch of a marker pass: one ranged fetch, then the segments are dropped. */
  private static final int MEMO_FETCH_WINDOW = 1024;

  /** {@code chain} when it holds a segment for every one of {@code n} leaves, else {@code null}. */
  private static byte @Nullable [][] completeChainOrNull(final byte @Nullable [][] chain, final int n) {
    if (chain == null || chain.length != n) {
      return null;
    }
    for (final byte[] segment : chain) {
      if (segment == null) {
        return null;
      }
    }
    return chain;
  }

  /**
   * Leaf-indexed mirrors of one column's descriptor entries ({@link ProjectionColumnStore#zoneIndex}):
   * the zone {@code min}/{@code max} the encoder wrote beside each BODY segment, the column flags, and
   * whether the entry exists at all. A {@code min > max} pair is the format's all-missing marker
   * (rowless leaf, or every cell missing), never a real range. Immutable once published.
   */
  public static final class ZoneIndex {
    private final long[] min;
    private final long[] max;
    private final byte[] flags;
    private final long[] known;

    ZoneIndex(final long[] min, final long[] max, final byte[] flags, final long[] known) {
      this.min = min;
      this.max = max;
      this.flags = flags;
      this.known = known;
    }

    /** Whether the leaf's descriptor lists the column's BODY entry (the range is UNKNOWN otherwise). */
    public boolean known(final int leaf) {
      return (known[leaf >>> 6] & 1L << (leaf & 63)) != 0L;
    }

    public long min(final int leaf) {
      return min[leaf];
    }

    public long max(final int leaf) {
      return max[leaf];
    }

    public byte flags(final int leaf) {
      return flags[leaf];
    }

    /** The format's all-missing marker: no present value on the leaf, so no range to reason with. */
    public boolean allMissing(final int leaf) {
      return min[leaf] > max[leaf];
    }
  }

  /**
   * The column's zone mirrors for every leaf, gathered ONCE from the descriptors into leaf-indexed
   * arrays. {@link #columnZoneRange} answers the same question for one leaf at the price of a
   * descriptor binary search plus two dependent loads; a plan that asks it for all 97,654 leaves of a
   * 100M-row store pays tens of milliseconds per column per query for what is a constant of the
   * build. Memoized per column, published like {@link #column}.
   */
  public ZoneIndex zoneIndex(final int col) {
    if (col < 0 || col >= columnKinds.length) {
      throw new IllegalArgumentException("column " + col + " of " + columnKinds.length);
    }
    final ZoneIndex hit = zoneIndices[col];
    if (hit != null) {
      return hit;
    }
    final int n = directories.size();
    final long[] min = new long[n];
    final long[] max = new long[n];
    final byte[] flags = new byte[n];
    final long[] known = new long[(n + 63) >>> 6];
    final int segId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    forEachMemoRange(n, memoPassRanges(n), (from, to, r) -> {
      for (int leaf = from; leaf < to; leaf++) {
        final byte[] descriptor = directories.get(leaf).descriptor();
        final int entry = RowGroupDescriptor.entryIndexOf(descriptor, segId);
        if (entry < 0) {
          min[leaf] = Long.MAX_VALUE;
          max[leaf] = Long.MIN_VALUE;
          flags[leaf] = ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE;
          continue;
        }
        min[leaf] = RowGroupDescriptor.entryMin(descriptor, entry);
        max[leaf] = RowGroupDescriptor.entryMax(descriptor, entry);
        flags[leaf] = RowGroupDescriptor.entryColFlags(descriptor, entry);
        known[leaf >>> 6] |= 1L << (leaf & 63);
      }
    });
    final ZoneIndex built = new ZoneIndex(min, max, flags, known);
    synchronized (this) {
      final ZoneIndex existing = zoneIndices[col];
      if (existing != null) {
        return existing;
      }
      final ZoneIndex[] next = zoneIndices.clone();
      next[col] = built;
      zoneIndices = next;
    }
    return built;
  }

  /** Diagnostics of the memo passes ({@code -Dsirix.projDiag}), read once. */
  private static final boolean MEMO_DIAG = Boolean.getBoolean("sirix.projDiag");

  /** Below this many leaves a memo pass stays serial: the fan-out's fixed cost exceeds the work. */
  private static final int MEMO_PASS_MIN = 4096;

  /** At most one memo-pass range per this many leaves, so a range amortizes its fetch and its task. */
  private static final int MEMO_RANGE_MIN = 512;

  /**
   * How many ranges a memo pass over {@code n} leaves splits into: one below {@link #MEMO_PASS_MIN},
   * else one per {@link #MEMO_RANGE_MIN} leaves and never more than there are cores.
   */
  private static int memoPassRanges(final int n) {
    if (n < MEMO_PASS_MIN) {
      return 1;
    }
    return Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), n / MEMO_RANGE_MIN));
  }

  /** A memo pass's work on one contiguous leaf range {@code [from, to)}, the range's index beside it. */
  @FunctionalInterface
  private interface LeafRangeTask {
    void run(int from, int to, int range);
  }

  /**
   * Run {@code task} over {@code n} leaves split into {@code ranges} contiguous ranges whose bounds are
   * multiples of 64 — so two ranges never share a bitset word and can OR into one {@code long[]}
   * without coordination. One range runs on the calling thread; more fan out over the common pool,
   * the first failure rethrown on the caller after every range stopped. A range that ends up empty
   * (more ranges than 64-leaf blocks) is skipped, and its part stays {@code null}.
   */
  private static void forEachMemoRange(final int n, final int ranges, final LeafRangeTask task) {
    if (ranges <= 1) {
      task.run(0, n, 0);
      return;
    }
    final int rangeLen = ((n + ranges - 1) / ranges + 63) & ~63;
    final AtomicReference<RuntimeException> failed = new AtomicReference<>();
    IntStream.range(0, ranges).parallel().forEach(r -> {
      final int from = r * rangeLen;
      final int to = Math.min(from + rangeLen, n);
      if (from >= to || failed.get() != null) {
        return;
      }
      try {
        task.run(from, to, r);
      } catch (final RuntimeException rangeFailed) {
        failed.compareAndSet(null, rangeFailed);
      }
    });
    final RuntimeException rangeFailed = failed.get();
    if (rangeFailed != null) {
      throw rangeFailed;
    }
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
   * that a predicate over such a column is resolved to an ID before it ever reaches a kernel (see the
   * executor's leaf conversion), so the numeric evaluator the slice's shape selects is asking the
   * right question. A string literal meeting a long-lane slice is a route defect, not a slow path,
   * and {@link ProjectionColumnScan#evaluateMask} refuses it loudly rather than comparing an unset
   * long against ids.
   */
  public boolean columnSliceable(final int col) {
    return col >= 0 && col < columnKinds.length
        && (ProjectionIndexRowGroupPage.isLongLaneKind(columnKinds[col])
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET);
  }

  /** RAW bytes one segment chain spans across every leaf that carries it. */
  private long projectedSegmentChainBytes(final int segId) {
    long total = 0;
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      final byte[] descriptor = directories.get(i).descriptor();
      final int entry = RowGroupDescriptor.entryIndexOf(descriptor, segId);
      if (entry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, entry);
      }
    }
    return total;
  }

  /**
   * Bytes a {@link #recordKeys} fill costs: the raw KEYS chain plus the 8 B/row {@code long[]} it
   * DECODES INTO and retains. Unlike the column doors this counts the decoded form explicitly,
   * because that is the part that stays — a 100M-row store retains ~800 MB of keys whose raw chain is
   * a fraction of it, and charging only the chain would leave the larger half unaccounted.
   *
   * @return the projected record-key fill bytes
   */
  public long projectedRecordKeysFillBytes() {
    long total = projectedSegmentChainBytes(ProjectionIndexColumnSegmentCodec.keysColumnSegmentId());
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      total += 8L * RowGroupDescriptor.rowCount(directories.get(i).descriptor());
    }
    return total;
  }

  /** RAW bytes the BODY chain of {@code col} spans — what a raw-bytes fill retains. */
  private long projectedColumnBodyFillBytes(final int col) {
    if (col < 0 || col >= columnKinds.length) {
      return 0;
    }
    long[] cached = projectedBodyFillBytes.get();
    if (cached == null) {
      cached = new long[columnKinds.length];
      projectedBodyFillBytes.compareAndSet(null, cached);
      cached = projectedBodyFillBytes.get();
    }
    final long known = cached[col];
    if (known != 0) {
      return known;
    }
    final int bodySegId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    long total = 0;
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      final byte[] descriptor = directories.get(i).descriptor();
      final int bodyEntry = RowGroupDescriptor.entryIndexOf(descriptor, bodySegId);
      if (bodyEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, bodyEntry);
      }
    }
    // A benign same-column race writes the identical sum twice.
    cached[col] = Math.max(1, total);
    return cached[col];
  }

  /**
   * Lazily-computed per-column projected BODY-chain bytes; 0 = not yet computed (a real sum is never
   * 0). Memoized for the same reason as {@link #projectedFillBytes}, and now for a sharper one:
   * {@link #incrementalFillBytes} puts this walk on the PLANNER path, so every gated column of every
   * group or aggregate query would otherwise redo a per-leaf descriptor probe over the whole store
   * once that column's raw BODY chain is published.
   */
  private final AtomicReference<long[]> projectedBodyFillBytes = new AtomicReference<>();

  /**
   * RAW bytes a MASKED fill of {@code col} would fetch: the same BODY (+DICT) walk
   * {@link #projectedColumnFillBytes} does, restricted to the leaves the keep mask leaves standing.
   * Exact rather than scaled, because the mask names the surviving leaf set and the descriptors are
   * already in hand — and because a prune that drops only the rowless leaves of a 3.5 GB column
   * leaves 3.5 GB, which a leaf-count ratio would not reveal.
   *
   * @param col the column
   * @param keepWords bitset over leaf indices; {@code null} means every leaf survives
   * @return the projected masked fill bytes
   */
  public long projectedMaskedFillBytes(final int col, final long @Nullable [] keepWords) {
    if (keepWords == null) {
      return projectedColumnFillBytes(col);
    }
    if (col < 0 || col >= columnKinds.length) {
      return 0;
    }
    final int bodySegId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    final int dictSegId = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col);
    long total = 0;
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      final int word = i >>> 6;
      if (word >= keepWords.length || (keepWords[word] & (1L << (i & 63))) == 0) {
        continue;
      }
      final byte[] descriptor = directories.get(i).descriptor();
      final int bodyEntry = RowGroupDescriptor.entryIndexOf(descriptor, bodySegId);
      if (bodyEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, bodyEntry);
      }
      final int dictEntry = RowGroupDescriptor.entryIndexOf(descriptor, dictSegId);
      if (dictEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, dictEntry);
      }
      total += decodedColumnResidentBytes(descriptor, col, columnKinds[col]);
    }
    return total;
  }

  /**
   * Whether a WHOLE-COLUMN fill of {@code col} fits the fill budget — the size half of route
   * viability, which {@link #columnSliceable} (a KIND predicate) deliberately does not answer.
   *
   * @param col the column
   * @return {@code true} when {@link #projectedColumnFillBytes} is within the budget
   */
  public boolean columnFillWithinBudget(final int col) {
    return col >= 0 && col < columnKinds.length
        && fitsMakingRoom(incrementalFillBytes(col, projectedColumnFillBytes(col)), new int[] {col}, false);
  }

  /**
   * Whether the sliced whole-column route is VIABLE for {@code col}: the kind can be sliced AND the
   * fill either fits the budget or has already been paid for.
   *
   * <p>
   * This is the predicate a PLANNER needs, not {@link #columnSliceable} alone. Kind-sliceability says
   * the route could decode the column; it says nothing about the budget
   * {@link #column(int, ColumnSegmentFetcher)} enforces before its first fetch. A planner gating on
   * kind alone selects the sliced arm for an over-budget column, the fill then declines through the
   * budget door, and the exception escapes to a generic fail-soft catch — so the query takes the
   * SLOWEST route and trips a defect counter, instead of the whole-leaf windowed byte scan the budget
   * declined it toward. An already-filled column stays viable: its bytes are resident, so the budget
   * question is moot.
   *
   * @param col the column
   * @return {@code true} when the sliced route can actually serve this column
   */
  public boolean columnFillable(final int col) {
    return columnSliceable(col) && (columnFilled(col) || columnFillWithinBudget(col));
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
      pinResident(col); // this query is now serving from these arrays — no close may release them
      return slices;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    // BUDGET, before the first fetch (task #78). A whole-column slice fill holds every leaf's BODY
    // (and, for strings, DICT) bytes plus their decoded arrays resident AT ONCE — on a fat string
    // column at 100M rows that is ~8-10 GB and OOMed the heap before serving a row. Over the
    // budget, refuse THROUGH THE ESTABLISHED DECLINE DOOR (the same IllegalStateException callers
    // already answer by falling back to the whole-leaf byte-scan route, which now serves through
    // bounded windowed payload loads). Deliberately NOT memoized as corrupt: nothing is wrong with
    // the store — the projected size is a property of the column, recomputed cheaply, and a later
    // caller with a raised budget must be able to fill.
    final long projected = projectedColumnFillBytes(col);
    checkFillBudget(col, incrementalFillBytes(col, projected), "slice fill");
    // Fill OUTSIDE the monitor: the fetch+decode is the store's only I/O and must not
    // serialize other columns (or evidence readers) behind it. A same-column race does the
    // work twice with identical results — first publish wins.
    slices = fillColumn(col, fetcher);
    pinResident(col);
    synchronized (this) {
      final ColumnSlice[] existing = columns[col];
      if (existing != null) {
        return existing;
      }
      // Publish via a fresh array write so the unsynchronized volatile read stays safe.
      final ColumnSlice[][] next = columns.clone();
      next[col] = slices;
      columns = next;
      // Re-priced HERE, not reused from the check: the fill above ran the inner raw-BODY door,
      // which may have published and charged that chain in between. Same helper, later moment.
      final long charged = incrementalFillBytes(col, projected);
      chargedSliceBytes[col] = charged;
      retainedFillBytes.addAndGet(charged);
    }
    return slices;
  }

  /**
   * Byte budget above which {@link #column(int, ColumnSegmentFetcher)} declines a whole-column slice
   * fill. Same property and derivation as the catalog's whole-leaf eager budget
   * ({@code sirix.projection.eagerMaterializeBytes}): a fill's raw bytes live on the heap beside the
   * decoded slice arrays and the off-heap arena, so half the projection cache budget and a quarter of
   * the heap bound it from both sides.
   */
  private static final long COLUMN_FILL_BUDGET_DEFAULT = Long.getLong("sirix.projection.eagerMaterializeBytes",
      Math.min(Long.parseLong(System.getProperty("sirix.projection.cacheBytes", String.valueOf(8L << 30))) / 2,
          Runtime.getRuntime().maxMemory() / 4));

  private static volatile long columnFillBudgetBytes = COLUMN_FILL_BUDGET_DEFAULT;

  /**
   * Test seam: shrink the fill budget so a small store still exercises the decline.
   *
   * @param value the budget in bytes
   * @return the previous budget, for restoring in a finally block
   */
  public static long setColumnFillBudgetBytesForTesting(final long value) {
    final long previous = columnFillBudgetBytes;
    columnFillBudgetBytes = value;
    return previous;
  }

  /** Kill switch: {@code false} restores the static budget with retention for the store's lifetime. */
  public static final String RESIDENCY_HEADROOM_PROPERTY = "sirix.projection.residency.headroom";

  /**
   * Whether the headroom share may LOWER what a store retains (R1). Opt-IN, and measured that way.
   *
   * <p>
   * R1 shipped on by default and was reverted to opt-in on the evidence of the 100M leg it was built
   * for. With it on, the same share gates three consumers at once — this budget, the group table and
   * the grouped-distinct ceiling — and a pessimistic sample at a query boundary releases what the
   * query just filled, so the next try re-reads it. Measured at 100M on the 69.6 GB rebuild, same
   * database and code, only this flag moving: q3 (projection-aggregate) 23.4 s cold / 21.1 s hot with
   * it on against 2.4 / 0.09 with it off — hot == cold being the signature of retaining nothing — and
   * q4 ({@code COUNT(DISTINCT UserID)}) never finished, thrashing at 12.6 cores and 16.9 GB RSS with a
   * 763-second concurrent mark cycle, against 1.50 / 1.46 with it off.
   * </p>
   *
   * <p>
   * The mechanism, its scope, its pins and its witnesses all stay: a store that genuinely must bound
   * what it keeps across queries turns it on with
   * {@code -Dsirix.projection.residency.headroom=true}. What it may not do is decide, from one
   * process-wide sample, that a query should throw away the column it is about to read again — and
   * the per-consumer accounting that would make it safe (one accumulator over every claimant of the
   * share, not N independent tests against the whole of it) does not exist yet.
   * </p>
   */
  private static volatile boolean residencyHeadroom =
      Boolean.parseBoolean(System.getProperty(RESIDENCY_HEADROOM_PROPERTY, "false"));

  /**
   * Test seam for the kill switch.
   *
   * @param value {@code false} for the pre-R1 behaviour (static budget, no release)
   * @return the previous value, for restoring in a finally block
   */
  public static boolean setResidencyHeadroomForTesting(final boolean value) {
    final boolean previous = residencyHeadroom;
    residencyHeadroom = value;
    return previous;
  }

  /** Whether headroom-gated residency (R1) is in effect. */
  public static boolean residencyHeadroomEnabled() {
    return residencyHeadroom;
  }

  /**
   * The heap share the retained fills may occupy, sampled at query-scope boundaries; {@code -1}
   * before the first sample.
   *
   * <p>
   * Sampled rather than read per call for two reasons. It is CONSTANT within a query, so every
   * planner predicate and every fill door of one query price against the same figure — a budget that
   * shrank between the route decision and the route's second fill would produce exactly the mid-route
   * decline that the combined-fit rule ({@link #columnsFitWithinBudget}) exists to prevent. And the
   * sample walks the heap pools' post-collection usage, which belongs at a query boundary, not on a
   * planner path a group arm re-enters per pass.
   * </p>
   */
  private static volatile long headroomShareBytes = -1L;

  /**
   * Re-read {@link HeapHeadroom#plannedShareBytes()} — called when a query scope opens and when one
   * closes, and by tests that move the headroom seam without running a query.
   *
   * @return the new share in bytes
   */
  public static long sampleHeadroomShare() {
    final long share = HeapHeadroom.plannedShareBytes();
    headroomShareBytes = share;
    return share;
  }

  /**
   * The budget every residency decision prices against: the static per-store fill budget, and — with
   * R1 in effect — no more than the shared {@link HeapHeadroom#plannedShareBytes() headroom share}.
   *
   * <p>
   * The SAME figure gates the planner predicates and the fill doors. A planner that priced against a
   * looser budget than the door enforces would admit a resident route whose second fill then throws
   * the decline door mid-kernel; the store answers one question with one number so that cannot
   * happen. Above the budget nothing is refused that is already resident, and nothing fails: the
   * windowed lanes serve every column without retention, so the budget only ever decides whether
   * bytes are KEPT.
   * </p>
   *
   * @return the effective residency budget in bytes
   */
  public static long residencyBudgetBytes() {
    final long stat = columnFillBudgetBytes;
    if (!residencyHeadroom) {
      return stat;
    }
    long share = headroomShareBytes;
    if (share < 0L) {
      share = sampleHeadroomShare();
    }
    return Math.min(stat, share);
  }

  /**
   * RESIDENT bytes ONE column of ONE leaf decodes into, on top of the raw segment bytes it is decoded
   * from — a bit-packed long lane becomes 8 B per value plus its presence words, a boolean column two
   * presence words, and everything else nothing extra.
   *
   * <p>
   * The single place this arithmetic lives. The budget that DECLINES a fill and the cache weight that
   * ADMITS the handle are two answers to one question — what does this column cost resident — and
   * they were answered by two formulas: the weigher counted the decoded lane, the fill doors counted
   * only packed bytes. A budget that prices packed bytes while the heap holds decoded ones admits
   * roughly 8x what it believes it is admitting on a long-lane column. Both sides now call here, so
   * they cannot disagree again.
   * </p>
   *
   * @param descriptor the leaf's row-group descriptor
   * @param kind the column's kind byte
   * @return the decoded residency of that column in that leaf
   */
  public static long decodedColumnResidentBytes(final byte[] descriptor, final int col, final byte kind) {
    final int rows = RowGroupDescriptor.rowCount(descriptor);
    final long presenceBytes = ((rows + 63L) >>> 6) << 3;
    // A LAYOUT question — how many bytes does a decoded slice occupy — so it asks the layout
    // predicate. A global string column decodes to the same eight bytes per row as any other long
    // lane, and counting it as weightless would let the cache hold more than it accounted for.
    if (ProjectionIndexRowGroupPage.isLongLaneKind(kind)) {
      return ((long) rows << 3) + presenceBytes;
    }
    if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
      return presenceBytes << 1;
    }
    if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      // The terms {@link #sliceRetainedBytes} reports for a filled string slice: the per-row id
      // lane, the presence words, and the dictionary in its DECODED form. Pricing these at zero
      // would exempt the exact column kind the windowed route exists to serve.
      long bytes = ((long) rows << 2) + presenceBytes;
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        // A set column keeps a per-row counts table beside the flat element run. The run itself can
        // exceed one element per row, which no descriptor field records — one element per row is
        // the floor, so a set column is the one shape this figure can still understate.
        bytes += (long) rows << 2;
      }
      final int dictEntry =
          RowGroupDescriptor.entryIndexOf(descriptor, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col));
      if (dictEntry >= 0) {
        bytes += (long) RowGroupDescriptor.entryByteLen(descriptor, dictEntry) * DICT_DECODED_EXPANSION;
      }
      return bytes;
    }
    return 0;
  }

  /**
   * Multiple applied to a DICT segment's stored bytes to reach what a fill retains for it: an
   * FSST-compressed dictionary is DECOMPRESSED into a fresh array on decode, and a 4 B offset per
   * entry is built beside it. Neither the compression ratio nor the entry count is recorded in the
   * descriptor, so this is a deliberate over-estimate — the safe direction for a residency cap, which
   * declines a fill early rather than admitting one it cannot hold.
   */
  private static final int DICT_DECODED_EXPANSION = 4;

  /**
   * Decoded residency of a DISTINCT-IDENTITY slice: the per-row id lane and presence words, and NOT
   * the dictionary — that mode reads the 8 B/entry hash chain instead, whose stored bytes are already
   * its retained bytes.
   */
  private static long decodedIdentityResidentBytes(final byte[] descriptor) {
    final int rows = RowGroupDescriptor.rowCount(descriptor);
    return ((long) rows << 2) + (((rows + 63L) >>> 6) << 3);
  }

  /**
   * Lazily-computed per-column projected fill bytes; 0 = not yet computed (a real sum is never 0).
   */
  private final AtomicReference<long[]> projectedFillBytes = new AtomicReference<>();

  /**
   * RESIDENT bytes a full slice fill of {@code col} costs: the BODY segment's byteLen across every
   * leaf, plus the DICT segment's where the descriptor lists one, plus what those bytes DECODE INTO
   * ({@link #decodedColumnResidentBytes}) — the same figure the cache weigher charges for the same
   * column, because a fill retains both halves for the store's lifetime.
   */
  public long projectedColumnFillBytes(final int col) {
    long[] cached = projectedFillBytes.get();
    if (cached == null) {
      cached = new long[columnKinds.length];
      projectedFillBytes.compareAndSet(null, cached);
      cached = projectedFillBytes.get();
    }
    final long known = cached[col];
    if (known != 0) {
      return known;
    }
    final int bodySegId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    final int dictSegId = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col);
    long total = 0;
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      final byte[] descriptor = directories.get(i).descriptor();
      final int bodyEntry = RowGroupDescriptor.entryIndexOf(descriptor, bodySegId);
      if (bodyEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, bodyEntry);
      }
      final int dictEntry = RowGroupDescriptor.entryIndexOf(descriptor, dictSegId);
      if (dictEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, dictEntry);
      }
      total += decodedColumnResidentBytes(descriptor, col, columnKinds[col]);
    }
    // A benign same-column race writes the identical sum twice.
    cached[col] = Math.max(1, total);
    return cached[col];
  }

  /**
   * Whether {@code col}'s full slices are ALREADY filled and cached. Lets a caller that would
   * otherwise ask for the cheaper distinct-identity fill reuse what another consumer has paid for,
   * instead of fetching a second chain for the same column.
   */
  public boolean columnFilled(final int col) {
    final ColumnSlice[][] slots = columns;
    final boolean filled = col >= 0 && col < slots.length && slots[col] != null;
    if (filled) {
      // A POSITIVE residency answer is what a planner turns into a resident route, so it pins for the
      // asking query exactly like a fill does. Without this a concurrent scope's close could release
      // the column between the planner's answer and the route's first read — and the route would then
      // meet the decline door mid-kernel instead of having been sent windowed up front.
      pinResident(col);
    }
    return filled;
  }

  /**
   * Whether the KEYS chain is ALREADY decoded and retained — {@link #columnFilled} for record keys:
   * a positive answer pins the keys for the asking query, since the caller will serve from them.
   */
  public boolean recordKeysFilled() {
    final boolean filled = recordKeySlices != null;
    if (filled) {
      pinResident(keysPinSlot());
    }
    return filled;
  }

  /**
   * A fill DECLINED because its projected bytes exceed {@link #columnFillBudgetBytes} — not
   * corruption, and deliberately never memoized as such.
   *
   * <p>
   * A distinct type because the two conditions that reach a caller's {@code IllegalStateException}
   * handler mean opposite things: a corrupt or truncated segment is a defect worth counting and
   * logging, whereas this is the store REFUSING a fill it can price, so the caller should fall back
   * to the whole-leaf windowed route the budget declined it toward — quietly, and without ticking a
   * defect counter. It stays an {@code IllegalStateException} so every existing fail-soft handler
   * keeps working unchanged; handlers that distinguish declines catch this first.
   * </p>
   */
  public static final class FillBudgetExceededException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    FillBudgetExceededException(final String message) {
      super(message);
    }
  }

  /**
   * Bytes this store has RETAINED across every published fill — the cumulative half of the budget.
   *
   * <p>
   * {@code columnFillBudgetBytes} is a per-column figure, but published fills are kept for the
   * store's whole cache lifetime, so a handle's residency is the SUM over the columns a query mix
   * touches, not any one of them. Without this ledger a ten-column projection with eight columns each
   * just under the budget retains eight budgets' worth while the cache weigher charges one. Charging
   * every publish makes that weight an honest upper bound by construction.
   * </p>
   *
   * <p>
   * Every retained byte is counted exactly ONCE. A slice fill and an identity fill both reach
   * {@link #columnBytes}, which publishes and charges the BODY chain itself, so the outer door adds
   * only what it brings on top — see {@link #incrementalFillBytes}, which the budget CHECK and this
   * ledger's CHARGE both consume so the gate and the total cannot disagree about what a fill costs.
   * </p>
   */
  private final AtomicLong retainedFillBytes = new AtomicLong();

  /** Bytes retained by published fills so far — observability for the budget witness. */
  public long retainedFillBytes() {
    return retainedFillBytes.get();
  }

  /**
   * Per-slot pin counts: how many open {@link ProjectionResidencyScope}s have published or observed
   * this column (or the KEYS lane) resident. A COUNTER per slot, not a list per query: the release
   * only has to know whether anyone still holds a column, and a counter answers that in one read on a
   * path that must not allocate.
   */
  private final AtomicIntegerArray residencyPins;

  /**
   * Bytes each published lane charged to {@link #retainedFillBytes}; {@code 0} = not published.
   * Guarded by {@code this}, like the publish sites that write them.
   *
   * <p>
   * The charge is recorded rather than recomputed, because it is NOT a function of the column alone:
   * a slice fill that follows a raw-BODY fill of the same column charges only its decoded arrays
   * ({@link #incrementalFillBytes}). Releasing a lane must return exactly what that lane added, or
   * the ledger drifts from the heap in the one direction a budget cannot survive — downward.
   * </p>
   */
  private final long[] chargedSliceBytes;

  private final long[] chargedIdentityBytes;

  private final long[] chargedBodyBytes;

  private long chargedKeysBytes;

  /** Bytes returned to the heap by {@link #releaseUnpinnedOverBudget()} process-wide. */
  private static final LongAdder RESIDENCY_RELEASED_BYTES = new LongAdder();

  /** Lanes released process-wide (one per column-with-its-lanes, or the KEYS lane). */
  private static final LongAdder RESIDENCY_RELEASES = new LongAdder();

  /**
   * Kill switch: {@code false} makes a fit door refuse whenever the retained total plus the fill
   * exceeds the budget, retaining whatever was filled first for the store's lifetime.
   */
  public static final String RESIDENCY_EVICT_PROPERTY = "sirix.projection.residency.evict";

  /**
   * Whether a fit door may EVICT unpinned fills to admit the one being priced
   * ({@link #fitsMakingRoom}). Without it residency is first-come-first-served: once earlier queries'
   * columns filled the budget (a leg at 100M sat at 5.07 GB of a 5.37 GB budget from its 19th query
   * on) every later column was refused for the store's lifetime and re-decoded on every read, while
   * the columns holding the budget were never read again.
   */
  private static volatile boolean residencyEvict =
      Boolean.parseBoolean(System.getProperty(RESIDENCY_EVICT_PROPERTY, "true"));

  /**
   * Test seam for the eviction kill switch.
   *
   * @param value {@code false} for first-come-first-served retention (no fit door evicts)
   * @return the previous value, for restoring in a finally block
   */
  public static boolean setResidencyEvictForTesting(final boolean value) {
    final boolean previous = residencyEvict;
    residencyEvict = value;
    return previous;
  }

  /** The empty keep set of a KEYS-lane fill. */
  private static final int[] NO_COLUMNS = new int[0];

  /** Lanes evicted by a fit door process-wide, to make room for the fill it admitted. */
  private static final LongAdder RESIDENCY_EVICTIONS = new LongAdder();

  /** Bytes evicted by fit doors process-wide. */
  private static final LongAdder RESIDENCY_EVICTED_BYTES = new LongAdder();

  /** Test observability: lanes a fit door evicted to admit a fill, process-wide. */
  public static long residencyEvictionCount() {
    return RESIDENCY_EVICTIONS.sum();
  }

  /** Test observability: bytes fit doors evicted, process-wide. */
  public static long residencyEvictedBytes() {
    return RESIDENCY_EVICTED_BYTES.sum();
  }

  /**
   * Whether {@code needed} more retained bytes fit the budget beside what this store retains, EVICTING
   * unpinned fills to make room when they do not. The one rule every fit door prices against
   * ({@link #columnFillWithinBudget}, {@link #columnIdentityFillable}, {@link #columnsFitWithinBudget},
   * {@link #leafAccess}): a fill that fits the budget on its own is admitted, and the fills it displaces
   * are those no open query pins and the caller is not about to read — largest first, like the
   * scope-exit release, so the fewest columns are dropped.
   *
   * <p>
   * Retention is an optimisation (every route also reads through the windowed lanes), so evicting can
   * cost a re-fetch, never an answer. Nothing is evicted unless the evictable total actually makes
   * the fill fit: a dry pass sums what could go before a single lane is dropped, so a fill that could
   * never fit leaves the store exactly as it found it.
   * </p>
   *
   * @param needed the bytes the fill would charge (already-resident lanes excluded)
   * @param keep columns the caller is about to read — never evicted, even when unpinned
   * @param keepKeys whether the caller is about to read the resident KEYS lane
   * @return {@code true} when the fill fits, after any eviction it needed
   */
  private boolean fitsMakingRoom(final long needed, final int[] keep, final boolean keepKeys) {
    final long budget = residencyBudgetBytes();
    if (retainedFillBytes.get() + needed <= budget) {
      return true;
    }
    if (!residencyEvict || needed > budget) {
      return false;
    }
    long freed = 0L;
    int evictions = 0;
    synchronized (this) {
      long retained = retainedFillBytes.get();
      if (retained + needed <= budget) {
        return true;
      }
      final boolean[] kept = new boolean[columnKinds.length];
      for (final int col : keep) {
        if (col >= 0 && col < kept.length) {
          kept[col] = true;
        }
      }
      final int keysSlot = keysPinSlot();
      final boolean keysEvictable = !keepKeys && residencyPins.get(keysSlot) == 0;
      long evictable = keysEvictable ? chargedKeysBytes : 0L;
      for (int col = 0; col < columnKinds.length; col++) {
        if (!kept[col] && residencyPins.get(col) == 0) {
          evictable += chargedSliceBytes[col] + chargedIdentityBytes[col] + chargedBodyBytes[col];
        }
      }
      if (retained - evictable + needed > budget) {
        return false; // could not fit even with everything evictable gone: touch nothing
      }
      while (retained + needed > budget) {
        int bestColumn = -1;
        long bestBytes = 0L;
        for (int col = 0; col < columnKinds.length; col++) {
          if (kept[col] || residencyPins.get(col) != 0) {
            continue;
          }
          final long bytes = chargedSliceBytes[col] + chargedIdentityBytes[col] + chargedBodyBytes[col];
          if (bytes > bestBytes) {
            bestBytes = bytes;
            bestColumn = col;
          }
        }
        final boolean keys = keysEvictable && chargedKeysBytes > bestBytes;
        if (keys) {
          bestBytes = chargedKeysBytes;
          recordKeySlices = null;
          chargedKeysBytes = 0L;
        } else if (bestColumn < 0 || bestBytes <= 0L) {
          break; // the dry pass admitted this fit, so this is unreachable; fail closed if it is not
        } else {
          releaseColumnLanes(bestColumn);
        }
        retained = retainedFillBytes.addAndGet(-bestBytes);
        freed += bestBytes;
        evictions++;
      }
      if (retained + needed > budget) {
        return false;
      }
    }
    RESIDENCY_EVICTIONS.add(evictions);
    RESIDENCY_EVICTED_BYTES.add(freed);
    if (Boolean.getBoolean("sirix.projDiag")) {
      System.err.println("[proj] residency evict: " + evictions + " lane(s), " + (freed >> 20)
          + " MB returned to admit a " + (needed >> 20) + " MB fill; retained " + (retainedFillBytes.get() >> 20)
          + " MB of a " + (budget >> 20) + " MB budget");
    }
    return true;
  }

  /** Test observability: bytes released at query-scope exits process-wide. */
  public static long residencyReleasedBytes() {
    return RESIDENCY_RELEASED_BYTES.sum();
  }

  /** Test observability: released columns (and KEYS lanes) process-wide. */
  public static long residencyReleaseCount() {
    return RESIDENCY_RELEASES.sum();
  }

  /** Pin slots: one per column plus one for the store-wide KEYS lane. */
  int pinSlotCount() {
    return columnKinds.length + 1;
  }

  /** The pin slot of the store-wide KEYS lane. */
  public int keysPinSlot() {
    return columnKinds.length;
  }

  /** Pin {@code slot} for one open scope. */
  void acquirePin(final int slot) {
    residencyPins.incrementAndGet(slot);
  }

  /** Drop the pins one closing scope holds, named by the set bits of {@code bits}. */
  void dropPins(final long[] bits) {
    final int slots = pinSlotCount();
    for (int word = 0; word < bits.length; word++) {
      long remaining = bits[word];
      while (remaining != 0) {
        final int bit = Long.numberOfTrailingZeros(remaining);
        remaining &= remaining - 1;
        final int slot = (word << 6) + bit;
        if (slot < slots) {
          residencyPins.decrementAndGet(slot);
        }
      }
    }
  }

  /** Test observability: scopes currently pinning {@code slot}. */
  public int residencyPins(final int slot) {
    return slot >= 0 && slot < residencyPins.length()
        ? residencyPins.get(slot)
        : 0;
  }

  /** Pin the column (or KEYS) slot in every open query scope — see {@link ProjectionResidencyScope}. */
  private void pinResident(final int slot) {
    ProjectionResidencyScope.pin(this, slot);
  }

  /**
   * Release published fills that no open query pins, largest first, until this store's retained total
   * is back within {@link #residencyBudgetBytes()}. Called at a query-scope exit — never on a timer,
   * never from a cache listener, never while a query that observed a column resident is still open.
   *
   * <p>
   * Retention is an OPTIMISATION: every route that reads a resident column also reads it through the
   * windowed lanes ({@link #leafAccess}, {@link WindowedLeafAccess}), which retain nothing. So
   * releasing can cost a re-fetch, never an answer — and keeping instead is what made a leg's later
   * queries plan their group tables against a heap the earlier queries' fills had already taken.
   * </p>
   *
   * <p>
   * A column's lanes are released TOGETHER (slices, distinct-identity slices, raw BODY bytes). They
   * are one column's residency, priced incrementally against each other, and releasing the raw bytes
   * out from under the decoded slices they were decoded from would leave the ledger describing a
   * combination that never existed. The store-wide KEYS lane is its own unit. Fingerprint chains are
   * deliberately NOT released: they are what lets a predicate prune leaves before a single BODY byte
   * is fetched, so dropping them to save their own bytes would cost the pruning that keeps the fills
   * small — the same economics that made them charged-but-never-refused.
   * </p>
   */
  void releaseUnpinnedOverBudget() {
    if (!residencyHeadroom) {
      return; // kill switch: retain for the store's lifetime, exactly as before R1
    }
    final long budget = residencyBudgetBytes();
    if (retainedFillBytes.get() <= budget) {
      return;
    }
    long freed = 0L;
    int releases = 0;
    synchronized (this) {
      long retained = retainedFillBytes.get();
      final int keysSlot = keysPinSlot();
      while (retained > budget) {
        // Largest releasable unit first: not an LRU (no recency is consulted, and none is recorded),
        // simply the choice that reaches the budget in the fewest drops and therefore re-fetches the
        // fewest columns if they are wanted again.
        int bestColumn = -1;
        long bestBytes = 0L;
        for (int col = 0; col < columnKinds.length; col++) {
          if (residencyPins.get(col) != 0) {
            continue;
          }
          final long bytes = chargedSliceBytes[col] + chargedIdentityBytes[col] + chargedBodyBytes[col];
          if (bytes > bestBytes) {
            bestBytes = bytes;
            bestColumn = col;
          }
        }
        final boolean keys = residencyPins.get(keysSlot) == 0 && chargedKeysBytes > bestBytes;
        if (keys) {
          bestBytes = chargedKeysBytes;
        } else if (bestColumn < 0) {
          break; // everything left is pinned or already released
        }
        if (bestBytes <= 0L) {
          break;
        }
        if (keys) {
          recordKeySlices = null;
          chargedKeysBytes = 0L;
        } else {
          releaseColumnLanes(bestColumn);
        }
        retained = retainedFillBytes.addAndGet(-bestBytes);
        freed += bestBytes;
        releases++;
      }
    }
    if (freed > 0L) {
      RESIDENCY_RELEASED_BYTES.add(freed);
      RESIDENCY_RELEASES.add(releases);
      if (Boolean.getBoolean("sirix.projDiag")) {
        System.err.println("[proj] residency release: " + releases + " column(s), " + (freed >> 20)
            + " MB returned, retained " + (retainedFillBytes.get() >> 20) + " MB of a " + (budget >> 20)
            + " MB budget");
      }
    }
  }

  /** Drop every published lane of {@code col}; the caller holds the monitor and does the accounting. */
  private void releaseColumnLanes(final int col) {
    assert Thread.holdsLock(this);
    if (chargedSliceBytes[col] != 0L) {
      final ColumnSlice[][] next = columns.clone();
      next[col] = null;
      columns = next;
      chargedSliceBytes[col] = 0L;
    }
    if (chargedIdentityBytes[col] != 0L) {
      final ColumnSlice[][] next = identityColumns.clone();
      next[col] = null;
      identityColumns = next;
      chargedIdentityBytes[col] = 0L;
    }
    if (chargedBodyBytes[col] != 0L) {
      final byte[][][] next = columnBytes.clone();
      next[col] = null;
      columnBytes = next;
      chargedBodyBytes[col] = 0L;
    }
  }

  /** Whether {@code col}'s raw BODY chain is already published — and therefore already charged. */
  private boolean columnBytesFilled(final int col) {
    final byte[][][] slots = columnBytes;
    return col >= 0 && col < slots.length && slots[col] != null;
  }

  /**
   * What a fill of {@code col} priced at {@code projected} would ADD to this store's residency: the
   * gross projection minus the bytes already retained AND already charged for that same column.
   *
   * <p>
   * The one place that subtraction lives, because the budget CHECK and the ledger CHARGE have to
   * agree about it. A slice fill and an identity fill both reach {@link #columnBytes}, which
   * publishes and charges the BODY chain on its own account — so once a fused fold scan has filled
   * those raw bytes, a later slice fill of the same column adds only its decoded arrays. Pricing the
   * check at the gross figure while charging the increment made the gate refuse fills whose true
   * residency fit the budget, steering servable queries onto the slower whole-leaf route for bytes
   * that were already counted.
   * </p>
   *
   * <p>
   * Only for the doors that REUSE the published BODY arrays. A masked fill re-fetches the surviving
   * leaves into fresh arrays and a record-key fill has no column, so both are priced gross.
   * </p>
   *
   * <p>
   * Evaluated TWICE per fill, deliberately: once before the fetch to gate it, once at publish to
   * charge it. What is already retained legitimately changes in between — the fill itself runs the
   * inner raw-BODY door — so a single value captured up front would charge for bytes the inner door
   * had just accounted for.
   * </p>
   *
   * @param col the column, or negative for a store-wide fill
   * @param projected the fill's gross projection
   * @return the bytes the fill would add
   */
  private long incrementalFillBytes(final int col, final long projected) {
    final long alreadyRetained = columnBytesFilled(col)
        ? projectedColumnBodyFillBytes(col)
        : 0;
    return Math.max(0, projected - alreadyRetained);
  }

  /**
   * Refuse a fill whose bytes would not fit beside what this store already retains, after
   * {@link #fitsMakingRoom} has evicted what it may to admit it.
   *
   * @param col the column
   * @param projected the fill's projected bytes
   * @param mode names the fill mode in the decline message
   * @throws FillBudgetExceededException when the cumulative cap would be exceeded
   */
  private void checkFillBudget(final int col, final long projected, final String mode) {
    if (fitsMakingRoom(projected, col < 0 ? NO_COLUMNS : new int[] {col}, col < 0)) {
      return;
    }
    final long retained = retainedFillBytes.get();
    final long budget = residencyBudgetBytes();
    throw new FillBudgetExceededException((col < 0
        ? "The store's "
        : "Column " + col + " ") + mode + " adds " + projected + " B beside " + retained
        + " B already retained, over the " + budget + " B residency budget (min of "
        + columnFillBudgetBytes + " B sirix.projection.eagerMaterializeBytes and the "
        + (residencyHeadroom
            ? headroomShareBytes + " B heap headroom share"
            : "static budget: " + RESIDENCY_HEADROOM_PROPERTY + "=false")
        + ") — declining the fill; the caller falls back to the whole-leaf windowed route");
  }

  /** Whether {@code col}'s DISTINCT-IDENTITY slices are already filled and cached. */
  public boolean columnIdentityFilled(final int col) {
    final ColumnSlice[][] slots = identityColumns;
    final boolean filled = col >= 0 && col < slots.length && slots[col] != null;
    if (filled) {
      pinResident(col); // see columnFilled: a positive residency answer pins for the asking query
    }
    return filled;
  }

  /** Lazily-computed per-column projected DISTINCT-IDENTITY fill bytes; 0 = not yet computed. */
  private final AtomicReference<long[]> projectedIdentityFillBytes = new AtomicReference<>();

  /**
   * RAW bytes a {@link #columnDistinctIdentity} fill of {@code col} would fetch, which is a different
   * — and on a fat dictionary a far smaller — figure than {@link #projectedColumnFillBytes}: the BODY
   * chain plus the ~8 B/entry {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_DICT_HASHES} chain,
   * and the DICTIONARY only for the leaves that carry no hash segment (exactly the fallback keep-mask
   * {@code fillIdentityColumn} builds).
   *
   * <p>
   * Falls back to {@link #projectedColumnFillBytes} for a non-STRING_DICT column and for the case
   * {@code fillIdentityColumn} declines outright — NO row-bearing leaf carries hashes — because that
   * is when the identity request becomes a full fill.
   * </p>
   *
   * @param col the column
   * @return the projected identity fill bytes
   */
  public long projectedColumnIdentityFillBytes(final int col) {
    if (col < 0 || col >= columnKinds.length
        || columnKinds[col] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      return projectedColumnFillBytes(col);
    }
    long[] cached = projectedIdentityFillBytes.get();
    if (cached == null) {
      cached = new long[columnKinds.length];
      projectedIdentityFillBytes.compareAndSet(null, cached);
      cached = projectedIdentityFillBytes.get();
    }
    final long known = cached[col];
    if (known != 0) {
      return known;
    }
    final int bodySegId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    final int dictSegId = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col);
    final int hashSegId = ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(col);
    long total = 0;
    int rowLeaves = 0;
    int fallbackLeaves = 0;
    final int n = directories.size();
    for (int i = 0; i < n; i++) {
      final byte[] descriptor = directories.get(i).descriptor();
      final int bodyEntry = RowGroupDescriptor.entryIndexOf(descriptor, bodySegId);
      if (bodyEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, bodyEntry);
      }
      if (RowGroupDescriptor.rowCount(descriptor) == 0) {
        continue;
      }
      rowLeaves++;
      final int hashEntry = RowGroupDescriptor.entryIndexOf(descriptor, hashSegId);
      if (hashEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, hashEntry);
        continue;
      }
      fallbackLeaves++;
      final int dictEntry = RowGroupDescriptor.entryIndexOf(descriptor, dictSegId);
      if (dictEntry >= 0) {
        total += RowGroupDescriptor.entryByteLen(descriptor, dictEntry);
      }
    }
    for (int i = 0; i < n; i++) {
      total += decodedIdentityResidentBytes(directories.get(i).descriptor());
    }
    if (rowLeaves > 0 && fallbackLeaves == rowLeaves) {
      return projectedColumnFillBytes(col);
    }
    cached[col] = Math.max(1, total);
    return cached[col];
  }

  /**
   * Whether the WHOLE-COLUMN fills of every not-yet-resident column in {@code columns} fit the budget
   * TOGETHER — the route-level twin of {@link #columnFillWithinBudget}, which prices ONE column against
   * the remainder. Two columns that each fit the remainder need not fit together: the first fill would
   * succeed and retain its bytes, the second would throw the budget door mid-route (a whole-leaf
   * re-entry, or a decline). {@code identityColumn} ({@code -1} for none) is priced in
   * DISTINCT-IDENTITY mode, as {@link #columnIdentityFillable} prices it; a column already resident in
   * either mode costs nothing; a repeated column counts once.
   *
   * @param columns the columns a route would fill resident (predicates, keys, aggregates, conditions)
   * @param identityColumn the {@code COUNT(DISTINCT)} operand filled in identity mode, or {@code -1}
   * @return {@code true} when the combined fill fits beside what the store already retains
   */
  public boolean columnsFitWithinBudget(final int[] columns, final int identityColumn) {
    long needed = 0L;
    final boolean[] counted = new boolean[columnKinds.length];
    final StringBuilder diag = Boolean.getBoolean("sirix.projDiag") ? new StringBuilder() : null;
    for (final int col : columns) {
      if (col < 0 || col >= columnKinds.length) {
        return false;
      }
      if (counted[col] || columnFilled(col)) {
        continue;
      }
      counted[col] = true;
      final long priced;
      if (col == identityColumn) {
        priced = columnIdentityFilled(col)
            ? 0L
            : incrementalFillBytes(col, projectedColumnIdentityFillBytes(col));
      } else {
        priced = incrementalFillBytes(col, projectedColumnFillBytes(col));
      }
      needed += priced;
      if (diag != null) {
        diag.append(" col=")
            .append(col)
            .append(":kind=")
            .append(columnKinds[col])
            .append(":")
            .append(priced >> 20)
            .append("MB");
      }
    }
    final boolean fits = fitsMakingRoom(needed, columns, false);
    if (diag != null && !fits) {
      System.err.println("[store] combined fit REFUSED: needed=" + (needed >> 20) + "MB retained="
          + (retainedFillBytes.get() >> 20) + "MB budget=" + (residencyBudgetBytes() >> 20) + "MB" + diag);
    }
    return fits;
  }

  /**
   * Whether the sliced route is VIABLE for {@code col} when it will be filled in DISTINCT-IDENTITY
   * mode — the {@code COUNT(DISTINCT)} operand's mode. Asking {@link #columnFillable} instead rejects
   * a fat dictionary column on a projection the identity fill never fetches.
   *
   * @param col the column
   * @return {@code true} when the identity fill can actually serve this column
   */
  public boolean columnIdentityFillable(final int col) {
    return columnSliceable(col) && (columnFilled(col) || columnIdentityFilled(col)
        || fitsMakingRoom(incrementalFillBytes(col, projectedColumnIdentityFillBytes(col)), new int[] {col}, false));
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
      pinResident(col);
      return full[col];
    }
    final ColumnSlice[][] slots = identityColumns;
    ColumnSlice[] slices = slots[col];
    if (slices != null) {
      pinResident(col);
      return slices;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    final long projectedIdentity = projectedColumnIdentityFillBytes(col);
    checkFillBudget(col, incrementalFillBytes(col, projectedIdentity), "distinct-identity fill");
    slices = fillIdentityColumn(col, fetcher);
    if (slices == null) {
      return column(col, fetcher); // no leaf carries hashes — the whole column falls back
    }
    pinResident(col);
    synchronized (this) {
      final ColumnSlice[] existing = identityColumns[col];
      if (existing != null) {
        return existing;
      }
      final ColumnSlice[][] next = identityColumns.clone();
      next[col] = slices;
      identityColumns = next;
      final long charged = incrementalFillBytes(col, projectedIdentity);
      chargedIdentityBytes[col] = charged;
      retainedFillBytes.addAndGet(charged);
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
    } catch (final ProjectionStoreInconsistentException inconsistent) {
      // Leaves that disagree about a column's encoding are a WRITER fault, not undecodable bytes.
      // Decline the store, but do NOT record this column as corrupt: that memo is store-wide and
      // lives for the process, so one mis-dispatch would permanently disable every fast path on a
      // column whose bytes were never in question (task #50).
      throw inconsistent;
    } catch (final IllegalStateException corrupt) {
      corruptColumns[col] = 1;
      throw corrupt;
    }
    if (diag) {
      final long tDone = System.nanoTime();
      System.err.printf(
          "[proj] identity fill col=%d leaves=%d hashed=%d fallback=%d in %.1f ms | body=%.1f ms "
              + "hash+dict=%.1f ms decode=%.1f ms | t=%.1f..%.1f%n",
          col, n, rowLeaves - fallbackLeaves, fallbackLeaves, (tDone - startNanos) / 1e6, (tBody - startNanos) / 1e6,
          (tHash - tBody) / 1e6, (tDone - tHash) / 1e6, startNanos / 1e6, tDone / 1e6);
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
    // CHARGED but never REFUSED. The fingerprints are what let a predicate prune leaves before any
    // BODY/DICT byte is fetched, so refusing them to protect the budget would forfeit the pruning
    // that keeps fills under it — the economics invert. Charging them still keeps the ledger an
    // honest account of residency, and the pressure lands where it belongs: on the next column fill.
    final long projectedBloom = projectedSegmentChainBytes(ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(col));
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
      retainedFillBytes.addAndGet(projectedBloom);
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
  /**
   * The keep-masked slices of {@code col} WITHOUT a second fetch or a second budget charge when the
   * column is already resident: the retained slices behind a mask (dropped leaves yield the shared
   * {@code rowCount == 0} sentinel, exactly what {@link #columnMasked} decodes to). A column that is
   * not resident takes the masked fill. The residency decisions price a resident column's fill at
   * zero, so re-fetching it masked — and pricing the masked bytes against a budget the column's own
   * body already fills — declined q19 at 100M/8 GB ("masked slice fill adds 117 MB beside 2,118 MB
   * already retained") on every try inside a leg, while it served alone in a fresh JVM.
   *
   * @param col the column
   * @param fetcher the caller's own live fetcher (used only when the column is not resident)
   * @param keepWords the leaf keep mask, or {@code null} for every leaf
   * @return one slice per leaf
   */
  public ColumnSlice[] columnMaskedView(final int col, final ColumnSegmentFetcher fetcher,
      final long @Nullable [] keepWords) {
    if (keepWords == null) {
      return column(col, fetcher);
    }
    if (!columnFilled(col)) {
      return columnMasked(col, fetcher, keepWords);
    }
    final ColumnSlice[] resident = column(col, fetcher);
    final ColumnSlice[] view = new ColumnSlice[resident.length];
    for (int i = 0; i < resident.length; i++) {
      final int word = i >>> 6;
      view[i] = word < keepWords.length && (keepWords[word] & 1L << (i & 63)) != 0L
          ? resident[i]
          : PRUNED_SLICE;
    }
    return view;
  }

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
    // Priced INCREMENTALLY, like every residency decision: a column whose body bytes are already
    // retained (an identity fill, a previous plain fill) adds nothing the ledger has not counted, and
    // re-charging its masked projection against a budget it already fills declined q19 at 100M/8 GB.
    checkFillBudget(col, incrementalFillBytes(col, projectedMaskedFillBytes(col, keepWords)), "masked slice fill");
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
    } catch (final ProjectionStoreInconsistentException inconsistent) {
      // Leaves that disagree about a column's encoding are a WRITER fault, not undecodable bytes.
      // Decline the store, but do NOT record this column as corrupt: that memo is store-wide and
      // lives for the process, so one mis-dispatch would permanently disable every fast path on a
      // column whose bytes were never in question (task #50).
      throw inconsistent;
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
    } catch (final ProjectionStoreInconsistentException inconsistent) {
      // Leaves that disagree about a column's encoding are a WRITER fault, not undecodable bytes.
      // Decline the store, but do NOT record this column as corrupt: that memo is store-wide and
      // lives for the process, so one mis-dispatch would permanently disable every fast path on a
      // column whose bytes were never in question (task #50).
      throw inconsistent;
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
        raw += b == null
            ? 0
            : b.length;
      }
      if (dictSegments != null) {
        for (final byte[] b : dictSegments) {
          raw += b == null
              ? 0
              : b.length;
        }
      }
      final long total = FILL_DECODED_BYTES.addAndGet(decoded);
      final long rawTotal = FILL_RAW_BYTES.addAndGet(raw);
      System.err.printf(
          "[fill] col=%d kind=%d leaves=%d decoded=%.1f MB raw=%.1f MB | body=%.1f ms dict=%.1f ms "
              + "decode=%.1f ms (par=%s) | t=%.1f..%.1f | store totals: decoded=%.1f MB raw=%.1f MB%n",
          col, columnKinds[col], n, decoded / 1048576.0, raw / 1048576.0, (tBody - tEnter) / 1e6, (tDict - tBody) / 1e6,
          (tDone - tDict) / 1e6, n >= PARALLEL_DECODE_MIN, tEnter / 1e6, tDone / 1e6, total / 1048576.0,
          rawTotal / 1048576.0);
    }
    return slices;
  }

  /**
   * {@code -Dsirix.projection.fillDiag=true} reports what each column fill retains. A filled column
   * is held for the store's lifetime, so at large leaf counts the fills — not the query's own working
   * set — dominate the heap, and nothing else makes that visible.
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
    b += s.presenceWords() == null
        ? 0
        : s.presenceWords().length * 8L;
    b += s.numericValues() == null
        ? 0
        : s.numericValues().length * 8L;
    b += s.boolWords() == null
        ? 0
        : s.boolWords().length * 8L;
    b += s.stringDictIds() == null
        ? 0
        : s.stringDictIds().length * 4L;
    b += s.dictBytes() == null
        ? 0
        : s.dictBytes().length;
    b += s.dictOffsets() == null
        ? 0
        : s.dictOffsets().length * 4L;
    b += s.setCounts() == null
        ? 0
        : s.setCounts().length * 4L;
    b += s.dictHashes() == null
        ? 0
        : s.dictHashes().length * 8L;
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
   * These segments are bounded by the 512-byte segment-slot inline threshold, so their bytes are
   * captured with the directories and {@code fetchAll} is handed nothing to fetch — the whole chain
   * resolves without an overflow-page read.
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
      pinResident(col);
      return segments;
    }
    if (corruptColumns[col] != 0) {
      throw new IllegalStateException("Column " + col + " has a known-corrupt BODY segment");
    }
    final long projectedBody = projectedColumnBodyFillBytes(col);
    checkFillBudget(col, projectedBody, "raw BODY fill");
    segments = fetchColumnBytes(col, fetcher);
    pinResident(col);
    synchronized (this) {
      final byte[][] existing = columnBytes[col];
      if (existing != null) {
        return existing;
      }
      final byte[][][] next = columnBytes.clone();
      next[col] = segments;
      columnBytes = next;
      chargedBodyBytes[col] = projectedBody;
      retainedFillBytes.addAndGet(projectedBody);
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
      // A small segment lives inline in its OWN segment slot and was captured while the directory
      // was built. The descriptor never carries segment payload bytes.
      final byte[] inlineForEntry = dir.inlineBytesAt(entry);
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
   * {@link #PARALLEL_CHAIN_MIN}, else at most one worker per {@link #CHAIN_RANGE_MIN} leaves and
   * never more than there are cores. A small store keeps the single batched call it was tuned for.
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

  /**
   * Rollback switch for the parallel chain fetch
   * ({@code -Dsirix.projection.parallelChainFetch=false}).
   */
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
    // One carrier for the whole fan-out. Allocating lazily inside each worker let CAS losers discard
    // one n-entry array apiece under an inline-heavy first touch.
    final byte[][] inline = new byte[n][];
    final AtomicBoolean hasInline = new AtomicBoolean();
    final AtomicReference<RuntimeException> failed = new AtomicReference<>();
    IntStream.range(0, workers).parallel().forEach(w -> {
      final int from = w * chunk;
      final int to = Math.min(from + chunk, n);
      if (from >= to || failed.get() != null) {
        return;
      }
      try {
        collectColumnOffsetsRange(segId, offsets, optional, absent, from, to, inline, hasInline);
      } catch (final RuntimeException malformed) {
        failed.compareAndSet(null, malformed);
      }
    });
    final RuntimeException malformed = failed.get();
    if (malformed != null) {
      throw malformed;
    }
    return hasInline.get()
        ? inline
        : null;
  }

  /** One worker's leaf range of {@link #collectColumnOffsetsParallel}. */
  private void collectColumnOffsetsRange(final int segId, final long[] offsets, final boolean optional,
      final boolean[] absent, final int from, final int to, final byte[][] inlineBytes,
      final AtomicBoolean hasInline) {
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
      final byte[] inlineForEntry = dir.inlineBytesAt(entry);
      if (inlineForEntry != null) {
        inlineBytes[i] = inlineForEntry;
        hasInline.set(true);
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
      pinResident(keysPinSlot());
      return slices;
    }
    if (keysCorrupt) {
      throw new IllegalStateException("The KEYS chain has a known-corrupt segment");
    }
    final long projectedKeys = projectedRecordKeysFillBytes();
    checkFillBudget(-1, projectedKeys, "record-key fill");
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
    pinResident(keysPinSlot());
    synchronized (this) {
      slices = recordKeySlices;
      if (slices != null) {
        return slices;
      }
      recordKeySlices = decoded;
      chargedKeysBytes = projectedKeys;
      retainedFillBytes.addAndGet(projectedKeys);
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
   * (their offset is replaced by the no-page sentinel), filled from an inline segment slot, nor verified
   * — a leaf the caller has already proven irrelevant costs zero I/O and zero CPU.
   */
  private byte[][] fetchSegmentChain(final int col, final int segId, final byte segKind, final boolean optional,
      final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords) {
    final int n = directories.size();
    // Leaf order IS file order to within noise: the builder persists leaves 1..N in one
    // sequential commit, so a column's segment offsets ascend with the leaf index — no
    // explicit sort needed for read locality. One batched fetch = one read transaction.
    // A segment stored inline in its own slot carries no page; its bytes were captured with the
    // directory, so it is skipped in the offset batch (NULL_ID_LONG makes the fetcher yield null)
    // and filled afterwards. inlineBytes stays null when the whole column is referenced.
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
    } catch (final ProjectionStoreInconsistentException inconsistent) {
      // Leaves that disagree about a column's encoding are a WRITER fault, not undecodable bytes.
      // Decline the store, but do NOT record this column as corrupt: that memo is store-wide and
      // lives for the process, so one mis-dispatch would permanently disable every fast path on a
      // column whose bytes were never in question (task #50).
      throw inconsistent;
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


  // ==================== per-leaf column access (resident or windowed) ====================

  /**
   * Per-leaf access to a column store for kernels that visit ONE leaf at a time — the sorted top-k
   * scan and its kin. The resident form hands out the store's retained slices; the windowed form
   * decodes leaves per window through the caller's fetcher and keeps a handful of windows in a
   * small clock cache, so a column whose whole-column fill would exceed the heap budget (a fat
   * string column at 100M rows, ~8 GB of per-leaf dictionaries) still serves the kernel exactly,
   * at a bounded working set. Single-threaded by contract: the kernels that take it run on one
   * thread.
   */
  public interface LeafColumnAccess {
    /** The slice of {@code col} on {@code leaf}, never masked — sort keys, best-first and zone reads. */
    ColumnSlice slice(int col, int leaf);

    /**
     * The slice of predicate column {@code col} on {@code leaf}: the pruned sentinel when the zone-map
     * keep mask dropped the leaf, exactly as the masked resident fill hands it out.
     */
    ColumnSlice predicateSlice(int col, int leaf);

    /** The record keys of {@code leaf} in row order. */
    long[] recordKeys(int leaf);

    /** Whether leaves are decoded per window instead of held resident. */
    boolean windowed();
  }

  private static final LongAdder WINDOWED_LEAF_ACCESSES = new LongAdder();

  /** Test observability: how often a kernel took the windowed (non-retaining) access. */
  public static long windowedLeafAccessCount() {
    return WINDOWED_LEAF_ACCESSES.sum();
  }

  /** Leaves per decoded window of the windowed access — the LRU keeps a few of these per column. */
  public static final int LEAF_ACCESS_WINDOW = 64;

  /**
   * Resident when every one of {@code columns} (and the KEYS chain, when {@code needKeys}) fits
   * the fill budget or is already filled — the retained slices serve as always; otherwise the
   * windowed access, which retains nothing.
   */
  public LeafColumnAccess leafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords,
      final int[] columns, final boolean needKeys) {
    // The COMBINED projected fill of everything not yet resident, against what the budget has left —
    // not each column alone. Two fat columns that each fit the remainder do not fit together: the
    // first fill would succeed and the second throw the budget door mid-kernel, declining the try to
    // the interpreter, while the next try (first column now retained) would go windowed and serve.
    boolean resident = true;
    long needed = 0L;
    final boolean[] counted = new boolean[columnKinds.length];
    for (final int col : columns) {
      if (!columnSliceable(col)) {
        resident = false;
        break;
      }
      if (counted[col] || columnFilled(col)) {
        continue;
      }
      counted[col] = true;
      needed += incrementalFillBytes(col, projectedColumnFillBytes(col));
    }
    if (resident && needKeys && recordKeySlices == null) {
      needed += projectedRecordKeysFillBytes();
    }
    if (resident && !fitsMakingRoom(needed, columns, needKeys)) {
      resident = false;
    }
    if (resident) {
      // Admitted as resident: pin every lane the access will fill, so a concurrent scope's close
      // cannot release one of them between this decision and the access's first read.
      for (final int col : columns) {
        pinResident(col);
      }
      if (needKeys) {
        pinResident(keysPinSlot());
      }
      return new ResidentLeafAccess(fetcher, keepWords);
    }
    WINDOWED_LEAF_ACCESSES.increment();
    return new WindowedLeafAccess(fetcher, keepWords, LEAF_ACCESS_WINDOW, WindowedLeafAccess.DEFAULT_LEAVES_PER_COLUMN,
        false);
  }

  /** The resident access regardless of budget (tests and callers that already hold the fills). */
  public LeafColumnAccess residentLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords) {
    return new ResidentLeafAccess(fetcher, keepWords);
  }

  /** The windowed access regardless of budget (tests), with the default per-column leaf cache. */
  public LeafColumnAccess windowedLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords,
      final int windowLeaves) {
    return windowedLeafAccess(fetcher, keepWords, windowLeaves, WindowedLeafAccess.DEFAULT_LEAVES_PER_COLUMN);
  }

  /**
   * The windowed access with an explicit per-column leaf cache: a sequential kernel that consumes one
   * sub-chunk at a time needs only a couple of windows, a top-k heap wants room for its entries.
   */
  public LeafColumnAccess windowedLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords,
      final int windowLeaves, final int cacheLeaves) {
    return windowedLeafAccess(fetcher, keepWords, windowLeaves, cacheLeaves, false);
  }

  /**
   * The windowed access with, when {@code recycleSlices}, the long-lane arrays of an EVICTED slice
   * recycled into the next window's decode. That makes a slice's arrays valid only while the slice
   * is in the access's cache, so it is for callers whose consumers finish with a window before the
   * access moves {@code cacheLeaves} past it — the sliced group kernels' fill / kernel / release
   * discipline ({@link WindowedSliceArrays}). A caller that keeps slices across windows (a top-k heap
   * resolving old entries) must not ask for it.
   */
  public LeafColumnAccess windowedLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords,
      final int windowLeaves, final int cacheLeaves, final boolean recycleSlices) {
    WINDOWED_LEAF_ACCESSES.increment();
    return new WindowedLeafAccess(fetcher, keepWords, windowLeaves, cacheLeaves, recycleSlices);
  }

  /** Test observability: presence/value arrays a windowed access reused instead of allocating. */
  public static long recycledSliceArraysCount() {
    return SliceArrayPool.reusedCount();
  }

  /**
   * Count a kernel's use of a NON-RETAINING access that is not one of the store's own windowed
   * accesses — the sorted scan's per-slab leaf-set accesses, counted once per scan rather than once
   * per slab, so the witness keeps meaning "the kernel did not fill a column for this".
   */
  static void noteWindowedLeafAccess() {
    WINDOWED_LEAF_ACCESSES.increment();
  }

  /**
   * An access over an explicit SET of leaves — {@code leaves[from..to)}, ascending and distinct —
   * that decodes each column it is asked for exactly once, for exactly those leaves, in ONE batched
   * fetch per chain. The shape a best-first top-k hands each of its workers: a handful of leaves
   * scattered across the store, rarely two in the same window, so a windowed access would decode a
   * 64-leaf window per leaf and a resident one would fill the whole column for ten rows. Columns the
   * store already holds resident are read from the retained slices, captured once at construction
   * (the caller pinned what it observed resident, see {@link #columnFilled}); everything else is
   * decoded into arrays owned by this access and dropped with it. {@link LeafColumnAccess#windowed()}
   * is {@code true}: nothing is retained in the store.
   *
   * <p>
   * Not thread-safe — one instance per worker, like every access in this file.
   * </p>
   *
   * @throws IllegalArgumentException when the leaves are not strictly ascending or out of range
   */
  public LeafColumnAccess leafSetAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords,
      final int[] leaves, final int from, final int to) {
    if (fetcher == null) {
      throw new IllegalArgumentException("fetcher must not be null");
    }
    if (leaves == null || from < 0 || to > leaves.length || from > to) {
      throw new IllegalArgumentException("leaf range [" + from + ", " + to + ") of "
          + (leaves == null
              ? "null"
              : leaves.length));
    }
    final int leafCount = directories.size();
    for (int i = from; i < to; i++) {
      final int leaf = leaves[i];
      if (leaf < 0 || leaf >= leafCount) {
        throw new IllegalArgumentException("leaf " + leaf + " of " + leafCount);
      }
      if (i > from && leaves[i - 1] >= leaf) {
        throw new IllegalArgumentException("leaves must be strictly ascending: " + leaves[i - 1] + " before " + leaf);
      }
    }
    return new LeafSetAccess(fetcher, keepWords, leaves, from, to);
  }

  /**
   * Fetch, verify and hand back one chain's segments for {@code len} leaves — leaf {@code i} of the
   * batch is {@code leaves[base + i]} when {@code leaves} is given (ascending, distinct), else
   * {@code base + i} (a contiguous window). Absent optional segments (a rowless leaf's DICT) are
   * {@code null} in the result; a missing REQUIRED one throws.
   *
   * <p>
   * The offsets are collected HERE, for these leaves only: an access used to collect the chain of
   * every leaf of the store once per column (97,654 descriptor binary searches per column at 100M)
   * and every worker builds its own access, so a pruned scan that touched twelve windows still paid
   * twenty whole-column collections — ~100 ms per column per worker, the entire cost of a pruned
   * q36–q42 once the fetch itself was gone. Per batch the collection is {@code len} lookups and the
   * fetch arrays are batch-sized instead of {@code leafCount} entries zeroed per call.
   * </p>
   */
  private byte[][] fetchLeafSegments(final int @Nullable [] leaves, final int base, final int len, final int segId,
      final byte segKind, final boolean optional, final ColumnSegmentFetcher fetcher) {
    final long[] offsets = new long[len];
    byte[][] inlineBytes = null;
    boolean[] absent = null;
    boolean any = false;
    for (int i = 0; i < len; i++) {
      final int leaf = leaves == null
          ? base + i
          : leaves[base + i];
      final RowGroupDirectory dir = directories.get(leaf);
      final int entry = RowGroupDescriptor.entryIndexOf(dir.descriptor(), segId);
      if (entry < 0) {
        if (!optional) {
          throw new IllegalStateException("Descriptor of leaf " + dir.rowGroupId() + " lists no segment id " + segId);
        }
        if (absent == null) {
          absent = new boolean[len];
        }
        absent[i] = true;
        offsets[i] = Constants.NULL_ID_LONG;
        continue;
      }
      // A small segment lives inline in its OWN segment slot, captured while the directory was built.
      final byte[] inlineForEntry = dir.inlineBytesAt(entry);
      if (inlineForEntry != null) {
        if (inlineBytes == null) {
          inlineBytes = new byte[len][];
        }
        inlineBytes[i] = inlineForEntry;
        offsets[i] = Constants.NULL_ID_LONG;
      } else {
        offsets[i] = dir.columnSegmentOffsets()[entry];
        any = true;
      }
    }
    final byte[][] out = new byte[len][];
    if (any) {
      // Pruned leaves of a window are fetched with it (their offsets stay real); the kernel never
      // reads them, and the window's contiguity is what the backend's run coalescing lives on.
      try {
        fetcher.fetchRange(offsets, 0, len, out);
      } catch (final RuntimeException fetchFailed) {
        throw new IllegalStateException("Segment fetch failed for segment id " + segId + ": "
            + fetchFailed.getMessage(), fetchFailed);
      }
    }
    for (int i = 0; i < len; i++) {
      if (absent != null && absent[i]) {
        out[i] = null;
        continue;
      }
      byte[] segment = out[i];
      if (inlineBytes != null && inlineBytes[i] != null) {
        segment = inlineBytes[i];
        out[i] = segment;
      }
      final int leaf = leaves == null
          ? base + i
          : leaves[base + i];
      if (segment == null) {
        throw new IllegalStateException("Fetch returned no segment " + segId + " for leaf " + leaf);
      }
      ProjectionIndexColumnSegmentCodec.verifyColumnSegment(directories.get(leaf).descriptor(), segment, segId,
          segKind);
    }
    return out;
  }

  /**
   * Decode {@code col}'s slices for a batch of leaves (addressed as in {@link #fetchLeafSegments}):
   * the BODY chain, plus the DICT chain for the string kinds. Every leaf of the batch is decoded;
   * pruning is the predicate view's business ({@link LeafColumnAccess#predicateSlice}).
   */
  private ColumnSlice[] decodeLeafSlices(final int col, final int @Nullable [] leaves, final int base, final int len,
      final ColumnSegmentFetcher fetcher) {
    return decodeLeafSlices(col, leaves, base, len, fetcher, null);
  }

  /**
   * {@link #decodeLeafSlices(int, int[], int, int, ColumnSegmentFetcher)} with the long-lane arrays of the
   * body-only kinds taken from {@code pool} — the windowed access's recycling seam; string kinds
   * decode as before.
   */
  private ColumnSlice[] decodeLeafSlices(final int col, final int @Nullable [] leaves, final int base, final int len,
      final ColumnSegmentFetcher fetcher, final @Nullable SliceArrayPool pool) {
    if (!columnSliceable(col)) {
      throw new IllegalStateException("Column " + col + " is not sliceable");
    }
    final boolean set = columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
    final boolean string = set || columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final byte[][] bodies = fetchLeafSegments(leaves, base, len, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY, false, fetcher);
    final byte[][] dicts = string
        ? fetchLeafSegments(leaves, base, len, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col),
            ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT, true, fetcher)
        : null;
    final ColumnSlice[] decoded = new ColumnSlice[len];
    for (int i = 0; i < len; i++) {
      final int leaf = leaves == null
          ? base + i
          : leaves[base + i];
      final byte[] descriptor = directories.get(leaf).descriptor();
      final byte[] bodySeg = bodies[i];
      final byte[] dictSeg = dicts != null
          ? dicts[i]
          : null;
      decoded[i] = set
          ? ProjectionIndexColumnSegmentCodec.decodeStringSetSlice(descriptor, bodySeg, dictSeg, col)
          : string
              ? ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, bodySeg, dictSeg, col)
              : ProjectionIndexColumnSegmentCodec.decodeBodySlice(descriptor, bodySeg, col, pool);
    }
    return decoded;
  }

  /** Decode the record keys of a batch of leaves (addressed as in {@link #fetchLeafSegments}). */
  private long[][] decodeLeafKeys(final int @Nullable [] leaves, final int base, final int len,
      final ColumnSegmentFetcher fetcher) {
    final byte[][] segments = fetchLeafSegments(leaves, base, len, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.SEG_KIND_KEYS, false, fetcher);
    final long[][] decoded = new long[len][];
    for (int i = 0; i < len; i++) {
      final int leaf = leaves == null
          ? base + i
          : leaves[base + i];
      decoded[i] = ProjectionIndexColumnSegmentCodec.decodeKeysSlice(directories.get(leaf).descriptor(), segments[i]);
    }
    return decoded;
  }

  /** See {@link #leafSetAccess}. */
  private final class LeafSetAccess implements LeafColumnAccess {
    private final ColumnSegmentFetcher fetcher;
    private final long @Nullable [] keepWords;
    private final int[] leaves;
    private final int from;
    private final int to;
    /** The store's retained slices per column at construction ({@code null} where not resident). */
    private final ColumnSlice[][] retained;
    private final long @Nullable [][] retainedKeys;
    /** Per column, the set's decoded slices aligned to {@code leaves[from..to)}; built on first touch. */
    private final ColumnSlice[][] decoded = new ColumnSlice[columnKinds.length][];
    private long @Nullable [][] decodedKeys;

    LeafSetAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords, final int[] leaves,
        final int from, final int to) {
      this.fetcher = fetcher;
      this.keepWords = keepWords;
      this.leaves = leaves;
      this.from = from;
      this.to = to;
      this.retained = columns;
      this.retainedKeys = recordKeySlices;
    }

    @Override
    public boolean windowed() {
      return true;
    }

    private int position(final int leaf) {
      final int pos = Arrays.binarySearch(leaves, from, to, leaf);
      if (pos < 0) {
        throw new IllegalArgumentException("leaf " + leaf + " is not in this access's set");
      }
      return pos - from;
    }

    @Override
    public ColumnSlice slice(final int col, final int leaf) {
      final ColumnSlice[] resident = retained[col];
      if (resident != null) {
        return resident[leaf];
      }
      ColumnSlice[] set = decoded[col];
      if (set == null) {
        set = decodeLeafSlices(col, leaves, from, to - from, fetcher);
        decoded[col] = set;
      }
      return set[position(leaf)];
    }

    @Override
    public ColumnSlice predicateSlice(final int col, final int leaf) {
      if (keepWords != null && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) == 0L) {
        return PRUNED_SLICE;
      }
      return slice(col, leaf);
    }

    @Override
    public long[] recordKeys(final int leaf) {
      if (retainedKeys != null) {
        return retainedKeys[leaf];
      }
      long[][] set = decodedKeys;
      if (set == null) {
        set = decodeLeafKeys(leaves, from, to - from, fetcher);
        decodedKeys = set;
      }
      return set[position(leaf)];
    }
  }

  private final class ResidentLeafAccess implements LeafColumnAccess {
    private final ColumnSegmentFetcher fetcher;
    private final long @Nullable [] keepWords;
    private final ColumnSlice[][] byColumn = new ColumnSlice[columnKinds.length][];
    private final ColumnSlice[][] byPredicateColumn = new ColumnSlice[columnKinds.length][];
    private long @Nullable [][] keys;

    ResidentLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords) {
      this.fetcher = fetcher;
      this.keepWords = keepWords;
    }

    @Override
    public ColumnSlice slice(final int col, final int leaf) {
      ColumnSlice[] slices = byColumn[col];
      if (slices == null) {
        slices = column(col, fetcher);
        byColumn[col] = slices;
      }
      return slices[leaf];
    }

    @Override
    public ColumnSlice predicateSlice(final int col, final int leaf) {
      if (keepWords == null) {
        return slice(col, leaf);
      }
      ColumnSlice[] slices = byPredicateColumn[col];
      if (slices == null) {
        slices = columnMaskedView(col, fetcher, keepWords);
        byPredicateColumn[col] = slices;
      }
      return slices[leaf];
    }

    @Override
    public long[] recordKeys(final int leaf) {
      long[][] k = keys;
      if (k == null) {
        k = ProjectionColumnStore.this.recordKeys(fetcher);
        keys = k;
      }
      return k[leaf];
    }

    @Override
    public boolean windowed() {
      return false;
    }
  }

  private final class WindowedLeafAccess implements LeafColumnAccess {
    /**
     * Decoded leaves kept per column, LRU. A miss decodes the leaf's whole window (sequential scans
     * then touch each window once), and the capacity is well above a top-k heap's size plus a window,
     * so the heap comparisons that resolve OTHER leaves' entries hit the cache instead of re-decoding
     * a window per comparison — the pathology that made a 100M sorted scan slower than the interpreter.
     */
    static final int DEFAULT_LEAVES_PER_COLUMN = 512;
    private final int cacheLeaves;
    private final ColumnSegmentFetcher fetcher;
    private final long @Nullable [] keepWords;
    private final int windowLeaves;
    private final int leafCount = directories.size();
    @SuppressWarnings("unchecked")
    private final Int2ObjectLinkedOpenHashMap<ColumnSlice>[] leafCache =
        new Int2ObjectLinkedOpenHashMap[columnKinds.length];
    private final Int2ObjectLinkedOpenHashMap<long[]> keyCache = new Int2ObjectLinkedOpenHashMap<>();
    /** Recycles an evicted slice's long-lane arrays into the next decode; {@code null} = allocate. */
    private final @Nullable SliceArrayPool slicePool;

    WindowedLeafAccess(final ColumnSegmentFetcher fetcher, final long @Nullable [] keepWords, final int windowLeaves,
        final int cacheLeaves, final boolean recycleSlices) {
      if (windowLeaves <= 0) {
        throw new IllegalArgumentException("windowLeaves must be positive: " + windowLeaves);
      }
      if (cacheLeaves < windowLeaves) {
        throw new IllegalArgumentException("cacheLeaves " + cacheLeaves + " below one window of " + windowLeaves);
      }
      this.fetcher = fetcher;
      this.keepWords = keepWords;
      this.windowLeaves = windowLeaves;
      this.cacheLeaves = cacheLeaves;
      this.slicePool = recycleSlices
          ? new SliceArrayPool()
          : null;
    }

    @Override
    public boolean windowed() {
      return true;
    }

    private boolean pruned(final int leaf) {
      return keepWords != null && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) == 0L;
    }

    @Override
    public ColumnSlice predicateSlice(final int col, final int leaf) {
      if (leaf >= 0 && leaf < leafCount && pruned(leaf)) {
        return PRUNED_SLICE;
      }
      return slice(col, leaf);
    }

    @Override
    public ColumnSlice slice(final int col, final int leaf) {
      if (leaf < 0 || leaf >= leafCount) {
        throw new IndexOutOfBoundsException("leaf " + leaf + " of " + leafCount);
      }
      if (!columnSliceable(col)) {
        throw new IllegalStateException("Column " + col + " is not sliceable");
      }
      Int2ObjectLinkedOpenHashMap<ColumnSlice> cache = leafCache[col];
      if (cache == null) {
        cache = new Int2ObjectLinkedOpenHashMap<>(cacheLeaves * 2);
        leafCache[col] = cache;
      }
      final ColumnSlice hit = cache.getAndMoveToLast(leaf);
      if (hit != null) {
        return hit;
      }
      final int window = leaf / windowLeaves;
      final ColumnSlice[] decoded = decodeWindow(col, window);
      final int from = window * windowLeaves;
      for (int i = 0; i < decoded.length; i++) {
        cache.putAndMoveToLast(from + i, decoded[i]);
      }
      while (cache.size() > cacheLeaves) {
        // Eviction is the arrays' death under the windowed contract: a recycling access hands them to
        // its pool here and the NEXT window's decode takes them back instead of allocating.
        final ColumnSlice evicted = cache.removeFirst();
        if (slicePool != null) {
          slicePool.recycle(evicted);
        }
      }
      return decoded[leaf - from];
    }

    private ColumnSlice[] decodeWindow(final int col, final int window) {
      final int from = window * windowLeaves;
      final int to = Math.min(from + windowLeaves, leafCount);
      return decodeLeafSlices(col, null, from, to - from, fetcher, slicePool);
    }

    @Override
    public long[] recordKeys(final int leaf) {
      if (leaf < 0 || leaf >= leafCount) {
        throw new IndexOutOfBoundsException("leaf " + leaf + " of " + leafCount);
      }
      final long[] hit = keyCache.getAndMoveToLast(leaf);
      if (hit != null) {
        return hit;
      }
      final int window = leaf / windowLeaves;
      final int from = window * windowLeaves;
      final int to = Math.min(from + windowLeaves, leafCount);
      final long[][] decoded = decodeLeafKeys(null, from, to - from, fetcher);
      for (int i = from; i < to; i++) {
        keyCache.putAndMoveToLast(i, decoded[i - from]);
      }
      while (keyCache.size() > cacheLeaves) {
        keyCache.removeFirst();
      }
      return decoded[leaf - from];
    }
  }

}

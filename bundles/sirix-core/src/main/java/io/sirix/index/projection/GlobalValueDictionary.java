/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.HashAccesses;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryBlockIndexNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.ValueDictionaryRankTableNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.cache.Cache;
import io.sirix.cache.GlobalDictionaryRecordCacheKey;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.concurrent.atomic.AtomicLong;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Read access to the global projection value dictionary — the {@code id <-> value} mapping that
 * backs {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} columns.
 *
 * <h2>Why the dictionary is global</h2>
 *
 * A {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT} column carries one dictionary per
 * row group, which is the right shape when a column has a few dozen distinct values: every leaf
 * stores each value once and the ids pack into a handful of bits. It is the wrong shape when a
 * column has millions, because then a recurring value is stored once <em>per leaf</em> — hundreds
 * of copies of the same string — and the column's bytes come out roughly the size of the raw
 * strings. Worse, nothing about a per-leaf id is comparable across leaves, so group identity has to
 * be recovered by hashing the bytes back out of every leaf's dictionary.
 *
 * <p>
 * A global dictionary fixes both at once: a value is stored exactly once for the whole resource,
 * and the id it is stored under <em>is</em> its identity. Grouping becomes an integer group-by,
 * distinct-counting becomes a fold over integers, and equality against a literal becomes an integer
 * compare after a single probe.
 *
 * <h2>Node-key layout</h2>
 *
 * One sub-trie holds every column's dictionary (see
 * {@link NamePage#JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET} for why it cannot be one
 * sub-trie per column). Each column starts with one contiguous base run reserved from the offset's
 * own counter, anchored by a stable {@link ValueDictionaryHeaderNode} whose key the projection's
 * metadata records. Maintenance copy-on-writes only affected paths in immutable forward-hash and
 * reverse-id radix directories while retaining the original header anchor:
 *
 * <pre>
 *   headerKey                                  the {@link ValueDictionaryHeaderNode}
 *   header.forwardRootKey                    hash-prefix radix root
 *   header.reverseRootKey                    id-prefix radix root
 *   radix leaf                               immutable hash or value bucket
 * </pre>
 *
 * <h2>Why a run, and not a namespace computed from the column</h2>
 *
 * Partitioning the key space by {@code (projectionDefId, columnOrdinal)} with a fixed stride is the
 * obvious layout and it does not work. The indirect-page trie underneath a sub-trie adds a level
 * only when the page key being prepared is exactly the power-of-two boundary of its current height
 * ({@code KeyedTrieWriter#prepareLeafOfTree}), so keys have to be allocated densely and
 * monotonically for the trie to ever grow deep enough to address them. A strided base leaps past
 * every boundary without triggering growth, and the traversal then resolves every page key to the
 * root reference — records at keys billions apart land on the same page and overwrite each other,
 * silently. Reserving a dense run is the shape the trie actually supports, and it costs nothing:
 * the run's start is one long in the projection metadata, which already travels with the column.
 *
 * <h2>Persistent record packing</h2>
 *
 * Every dictionary append occupies the smallest possible persistent units: its record keys are a
 * dense, stride-one interval. Large individual values already use the key-value page's overflow
 * mechanism; leaving 63 empty slots between ordinary dictionary records would instead multiply
 * indirect-page and leaf-page churn without providing an ownership or versioning guarantee.
 *
 * <h2>Cost model</h2>
 *
 * Ordinary materialising methods here are a per-LITERAL or per-WINNER cost. The explicitly created
 * {@link ReadView} is the exception for operators that must interpret global ids while scanning: it
 * binds the header and name page to one revision and exposes allocation-free comparisons and the
 * two admitted substring transforms. It never exposes or copies an entry's byte array.
 */
public final class GlobalValueDictionary {

  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");

  /** Shares the projection's diagnostics switch, so one flag explains a whole route. */
  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");
  private static final AtomicInteger HFT_MAX_PROBE_UNITS = new AtomicInteger();
  private static final LongHashFunction SECONDARY_HASH = LongHashFunction.xx3();

  public static final int PERSISTENT_RECORD_STRIDE = 1;

  public static final int PERSISTENT_RECORDS_PER_PAGE = Constants.INP_REFERENCE_COUNT / PERSISTENT_RECORD_STRIDE;

  /** Answer of {@link #probe} when the dictionary provably does not hold the value. */
  public static final int ID_ABSENT = 0;

  /**
   * Answer of {@link #probe} when the dictionary cannot say. The caller must fall back to a route
   * that does not depend on the mapping rather than treat it as absent.
   */
  public static final int ID_UNKNOWN = -1;

  private static final int READ_VIEW_CACHE_SIZE = 256;

  /**
   * Reverse BUCKETS a read view retains, each covering 256 consecutive ids.
   *
   * <p>
   * A read-only transaction's dictionary record memo is a no-op, so a probe that walks from the
   * reverse root materialises three radix nodes plus the bucket before it reaches the entry — five
   * record decodes for one id. Retaining the bucket collapses that to one decode per id for any scan
   * with locality. Sixteen buckets span 4096 consecutive ids and cost sixteen references, so this is
   * bounded by the VIEW, never by the dictionary's cardinality.
   */
  private static final int READ_VIEW_BUCKET_CACHE_SIZE = 16;

  /**
   * FLOOR for both per-view tables: the smallest they may be, and what they are when the resident
   * budget is off. Named as a floor rather than a size because it is no longer either table's actual
   * length -- {@link #READ_VIEW_BLOCK_SLOTS} decides that from the budget.
   */
  private static final int READ_VIEW_TABLE_FLOOR = 16;

  /**
   * Byte budget for one view's resident decoded blocks; {@code 0} keeps the fixed 16-slot table.
   *
   * <p>
   * RESIDENCY BY FIT, never unconditional. A decoded block is up to
   * {@link ValueDictionaryValueBlockNode#MAX_BLOCK_BYTES}, so the budget divided by that bound gives
   * the number of slots the view may hold, and the table being DIRECT-MAPPED is what makes the budget
   * a real bound rather than a hope — a slot holds at most one block, so resident bytes can never
   * exceed slots times the bound, and a collision simply re-decodes through the path that already
   * exists. There is no eviction policy to get wrong because there is no eviction: the map
   * overwrites, and being wrong about what to keep costs a decode, never an answer.
   * </p>
   *
   * <p>
   * <b>DEFAULT OFF, because the shared record cache superseded it.</b> Sized from a budget this was
   * worth 24.0 us -> 0.42 us on a random point read, measured at the knee of 2,048 slots. Then
   * {@code BufferManager#getGlobalDictionaryRecordCache} began retaining decoded records ACROSS
   * transactions, which serves the same misses from one place instead of once per view — and with it
   * present the per-view table is worth 381 ns against 312 ns on the same point read, and nothing at
   * all on the 43-query leg (cold 6.786 against 6.744, hot 1.288 against 1.302, min of three legs
   * each, where the spread WITHIN each configuration is larger than the difference between them). A
   * per-view budget is also the wrong shape at scale: it is claimed once per view, so ten views would
   * claim it ten times for one dictionary, where the shared cache claims it once.
   *
   * <p>
   * The knob stays because the arithmetic behind it is still true where no shared cache is available.
   * Set it to a byte budget to restore the sized table; the budget divided by
   * {@link ValueDictionaryValueBlockNode#MAX_BLOCK_BYTES} gives the slot count, and the table being
   * DIRECT-MAPPED is what makes it a bound rather than a hope.
   * </p>
   */
  private static final long READ_VIEW_RESIDENT_BLOCK_BYTES =
      Long.getLong("sirix.projection.globalDict.residentBlockBytes", 0L);

  /** Slots the budget affords, rounded DOWN to a power of two so the index stays a mask. */
  private static final int READ_VIEW_BLOCK_SLOTS = blockSlotsForBudget(READ_VIEW_RESIDENT_BLOCK_BYTES);

  /**
   * Reverse-bucket slots, matched to the block slots.
   *
   * <p>
   * The two caches sit in series on a point read — a bucket must be resolved to learn which block
   * covers an id — so sizing only the blocks moves the cost rather than removing it. Measured: with
   * 2048 block slots and the bucket table left at 16, a random read fell from 23.9 us to 1.7 us and
   * STOPPED there, because every read still decoded its bucket. A bucket record is far smaller than a
   * block (it holds references, not values), so matching the counts costs a small fraction of the
   * block budget and is not metered separately.
   * </p>
   */
  private static final int READ_VIEW_BUCKET_SLOTS = Math.max(READ_VIEW_BUCKET_CACHE_SIZE, READ_VIEW_BLOCK_SLOTS);

  static {
    // Both tables index with `x & (SLOTS - 1)`, which is a modulo only for a power of two. A later
    // edit to the sizing arithmetic that produced, say, 3000 slots would not fail -- it would
    // silently mask into a fraction of the table and surface only as unexplained latency. The
    // constraint is cheap to state and impossible to notice once broken.
    if (Integer.bitCount(READ_VIEW_BLOCK_SLOTS) != 1 || Integer.bitCount(READ_VIEW_BUCKET_SLOTS) != 1
        || Integer.bitCount(READ_VIEW_CACHE_SIZE) != 1) {
      throw new ExceptionInInitializerError("read-view table sizes must be powers of two, got blocks="
          + READ_VIEW_BLOCK_SLOTS + " buckets=" + READ_VIEW_BUCKET_SLOTS + " slices=" + READ_VIEW_CACHE_SIZE);
    }
  }


  /** Ceiling on what one view's tables may hold, whatever the property says. */
  private static final long MAX_RESIDENT_BLOCK_BYTES = 512L << 20;

  private static int blockSlotsForBudget(final long budgetBytes) {
    if (budgetBytes <= 0L) {
      return READ_VIEW_TABLE_FLOOR;
    }
    final long affordable = budgetBytes / ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES;
    if (affordable <= READ_VIEW_TABLE_FLOOR) {
      return READ_VIEW_TABLE_FLOOR;
    }
    // Highest power of two not exceeding what the budget affords, under TWO caps. The first
    // bounds the array of references; the second bounds the RESIDENT BYTES those slots may
    // come to hold, which the first does not -- 1<<20 slots of 64 KiB blocks is 64 GiB, so a
    // mistyped property could make an absurd footprint legal while every individual bound
    // looked reasonable.
    final long byBytes = MAX_RESIDENT_BLOCK_BYTES / ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES;
    final long capped = Math.min(Math.min(affordable, 1L << 20), byBytes);
    return Integer.highestOneBit((int) capped);
  }

  private GlobalValueDictionary() {
    throw new AssertionError("no instances");
  }

  /**
   * Open a bounded reverse-dictionary view tied to the reader's current revision.
   *
   * <p>
   * The fixed direct-mapped caches retain immutable entry-node and reverse-bucket references only —
   * never a value, so the view's footprint is fixed whatever the dictionary's cardinality. A hot-loop
   * HIT performs neither a radix traversal nor an allocation. A MISS resolves through a retained
   * bucket when one is held, which removes the three radix-node decodes a walk from the root would
   * repeat for all 256 ids the bucket covers; it still decodes the entry record itself, so a miss is
   * NOT allocation-free. The view refuses an incomplete/unknown dictionary up front and checks the
   * revision before every operation, so it can never reinterpret a row id against another revision's
   * dictionary.
   * </p>
   *
   * @param headerNodeKey dictionary header key recorded by the projection column
   * @param reader reader positioned at the revision that owns the projection rows
   * @return a readable view, or {@code null} when the dictionary is absent, incomplete, or changed
   *         revision while the view was being opened
   */
  /**
   * A view over the per-segment dictionaries of ONE segment-scoped column, resolving the packed
   * {@code (segment, id)} cells such a column stores.
   *
   * <p>
   * Every consumer that resolves a CELL — materialising a value, taking a substring of one — works
   * through this untouched, because the cell says which dictionary it belongs to. Consumers indexed
   * by ID SPACE do not: a verdict bitset or a length table over a range assumes a dense id space, and
   * packed cells are not one. Those refuse; see {@link ReadView#isSegmentUnion}.
   * </p>
   *
   * @param headerKeysBySegment the dictionary anchor per segment, {@code 0} where the segment sealed
   *        none for this column
   * @return the union, or {@code null} when no segment has a readable dictionary
   */
  public static @Nullable ReadView segmentUnionReadView(final long[] headerKeysBySegment,
      final StorageEngineReader reader) {
    Objects.requireNonNull(headerKeysBySegment, "headerKeysBySegment must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    final ReadView[] perSegment = new ReadView[headerKeysBySegment.length];
    boolean any = false;
    for (int segment = 0; segment < headerKeysBySegment.length; segment++) {
      final long headerKey = headerKeysBySegment[segment];
      if (headerKey <= 0L) {
        continue;
      }
      final ReadView view = readView(headerKey, reader);
      if (view == null) {
        // One unreadable segment makes the whole column unresolvable: a cell of that segment has no
        // other dictionary it could legally take, so answering for the rest would serve some rows and
        // silently drop others.
        return null;
      }
      perSegment[segment] = view;
      any = true;
    }
    return any
        ? new ReadView(perSegment, reader.getRevisionNumber())
        : null;
  }

  public static @Nullable ReadView readView(final long headerNodeKey, final StorageEngineReader reader) {
    Objects.requireNonNull(reader, "reader must not be null");
    final int revision = reader.getRevisionNumber();
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null || !header.isDirectoryComplete()) {
      return null;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    if (reader.getRevisionNumber() != revision) {
      return null;
    }
    return new ReadView(headerNodeKey, header, revision, namePage, databaseType, reader);
  }

  /** Ids per separator-array entry; one reverse bucket, so the partition needs no spill handling. */
  private static final int VALUES_PER_INDEXED_RANGE = ValueDictionaryValueBucketNode.VALUES_PER_BUCKET;

  private static final byte[] EMPTY_SEPARATOR = new byte[0];

  /**
   * One lane's share of a SPLIT verdict sweep: the bucket range the lane owns, the slice it fills,
   * and the merge that folds that slice back into the whole-dictionary bitset.
   *
   * <h2>Why the arithmetic lives here and nowhere else</h2>
   *
   * <p>
   * Three numbers decide whether a split sweep is lossless: how long the slice is, which global
   * verdict word its element zero stands for, and how far the merge may write. They are related by
   * the id/bucket/word aliasing {@link ReadView#fillStringOpVerdict} documents — bucket {@code b}
   * owns ids {@code 256b+1 .. 256b+256} and therefore words {@code 4b .. 4b+4}, the last of which is
   * bucket {@code b+1}'s first — so getting any one of them wrong drops a row SILENTLY, at one id in
   * 256, on the path whose whole job is to decide which rows match. A second copy of the three is how
   * a caller and its regression test agree with each other while both drift from the sweep; every
   * caller therefore splits through this type instead of recomputing it.
   * </p>
   *
   * <p>
   * A lane owns its slice outright, which is what makes the parallel sweep safe: lanes OR-ing into
   * one shared array would lose one another's bits in the boundary word they share.
   * </p>
   */
  public static final class VerdictSlice {

    private static final long[] NO_WORDS = new long[0];

    private final int bucketLo;
    private final int bucketHi;
    private final int wordBase;
    private final long[] words;

    private VerdictSlice(final int bucketLo, final int bucketHi) {
      this.bucketLo = bucketLo;
      this.bucketHi = bucketHi;
      this.wordBase = bucketLo << 2;
      this.words = bucketLo >= bucketHi
          ? NO_WORDS
          : new long[((bucketHi - bucketLo) << 2) + 1];
    }

    /**
     * Lane {@code lane} of {@code lanes} over {@code buckets} buckets — an even split by bucket, so
     * that no two lanes ever fill the same bucket and every bucket is filled by one.
     *
     * @param buckets {@link ReadView#verdictBucketCount()}
     * @param lane the lane, {@code 0 <= lane < lanes}
     * @param lanes number of lanes the sweep is split into, at least one
     * @return the lane's share, possibly {@linkplain #isEmpty() empty} when there are more lanes than
     *         buckets
     * @throws IllegalArgumentException if {@code buckets} is negative, {@code lanes} is not positive,
     *         or {@code lane} is not a lane of {@code lanes}
     */
    public static VerdictSlice forLane(final int buckets, final int lane, final int lanes) {
      if (buckets < 0) {
        throw new IllegalArgumentException("buckets must not be negative: " + buckets);
      }
      if (lanes <= 0) {
        throw new IllegalArgumentException("lanes must be positive: " + lanes);
      }
      if (lane < 0 || lane >= lanes) {
        throw new IllegalArgumentException("lane " + lane + " is not one of " + lanes + " lanes");
      }
      // The multiply is done in long space: buckets * lane overflows an int at ~46k buckets a side,
      // which a 100M-row dictionary passes, and an overflowed bound would hand a lane a range that
      // silently excludes buckets no other lane covers.
      final int lo = (int) ((long) buckets * lane / lanes);
      final int hi = (int) ((long) buckets * (lane + 1) / lanes);
      return new VerdictSlice(lo, hi);
    }

    /** First bucket of the lane, inclusive. */
    public int bucketLo() {
      return bucketLo;
    }

    /** Last bucket of the lane, exclusive. */
    public int bucketHi() {
      return bucketHi;
    }

    /** Whether the lane owns no bucket at all, in which case filling and merging are no-ops. */
    public boolean isEmpty() {
      return bucketLo >= bucketHi;
    }

    /**
     * Evaluate {@code op} against this lane's buckets into the lane-owned slice.
     *
     * <p>
     * {@code view} may be — and for a parallel sweep MUST be — a view of the lane's own, since a view's
     * slice caches are single-threaded. It must be a view of the same revision and entry count the
     * split was sized from; a caller crossing views is responsible for checking that.
     * </p>
     *
     * @param view the dictionary view the lane reads through
     * @param op one of {@code EQ}, {@code NE}, {@code STR_LT/LE/GT/GE}, {@code STR_CONTAINS}
     * @param literalUtf8 the literal, UTF-8 encoded
     */
    public void fill(final ReadView view, final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
      Objects.requireNonNull(view, "view must not be null");
      if (isEmpty()) {
        return;
      }
      view.fillStringOpVerdict(op, literalUtf8, bucketLo, bucketHi, words, wordBase);
    }

    /**
     * OR this lane's slice into the whole-dictionary verdict.
     *
     * <p>
     * The write is clamped to what {@code verdict} addresses: the last bucket's slice covers the
     * boundary word of a bucket that does not exist, and the final bucket is partial whenever the entry
     * count is not a multiple of 256, so the tail of the last lane's slice legitimately describes ids
     * past the dictionary. Those words are zero — no id set them — so clamping drops nothing.
     * </p>
     *
     * @param verdict the whole-dictionary bitset, sized as {@link ReadView#newVerdict()} sizes it
     * @throws NullPointerException if {@code verdict} is null
     */
    public void mergeInto(final long[] verdict) {
      Objects.requireNonNull(verdict, "verdict must not be null");
      if (isEmpty()) {
        return;
      }
      final int limit = Math.min(words.length, verdict.length - wordBase);
      for (int w = 0; w < limit; w++) {
        verdict[wordBase + w] |= words[w];
      }
    }
  }

  /**
   * Revision-bound, fixed-memory reverse-dictionary access for scan kernels.
   *
   * <p>
   * A view is ONE kernel's scratch: its slice, bucket, block and rank-table caches are plain arrays
   * written on every miss with no publication, so a view must be confined to the thread that created
   * it — one view per worker, never one shared across a parallel fold. The dictionary records behind
   * it are immutable and shared through the record caches, so views are cheap to create per worker
   * and there is nothing to gain from sharing one.
   * </p>
   */
  public static final class ReadView {

    private final long headerNodeKey;
    private final long reverseRootKey;
    /** Root of the forward hash index, or 0 for a dictionary that is probed by binary search or not at all. */
    private final long forwardRootKey;
    private final int entryCount;
    private final int revision;
    private final @Nullable NamePage namePage;
    private final @Nullable DatabaseType databaseType;
    private final @Nullable StorageEngineReader reader;
    /**
     * Per-id SLICE cache, keyed by the id a caller passes (a MINT under a rank table): the backing
     * array a value lives in, plus its offset and length. No entry node and no copied {@code byte[]}
     * — a scan compares far more values than it emits, so a wrapper or a copy per compared id is
     * precisely the per-row garbage the packed layout removes.
     */
    private final int[] cachedIds = new int[READ_VIEW_CACHE_SIZE];
    private final byte[][] cachedBacking = new byte[READ_VIEW_CACHE_SIZE][];
    private final int[] cachedOffsets = new int[READ_VIEW_CACHE_SIZE];
    private final int[] cachedLengths = new int[READ_VIEW_CACHE_SIZE];
    /**
     * SPILL lane, same slot indexing. A spilled value stays behind its record rather than having its
     * array handed out: a record owns its bytes, and exposing them to keep one cache uniform would
     * trade the node's immutability for a convenience. Exactly one of {@code cachedBacking[slot]} and
     * {@code cachedSpills[slot]} is non-null for a resolved slot.
     */
    private final ValueDictionaryEntryNode[] cachedSpills = new ValueDictionaryEntryNode[READ_VIEW_CACHE_SIZE];
    /** Direct-mapped reverse-bucket retention; {@code -1} marks an unused slot. */
    private int @Nullable [] cachedBuckets;
    private ValueDictionaryValueBucketNode @Nullable [] cachedBucketNodes;
    /**
     * Direct-mapped retention of decoded SUB-BLOCKS, keyed by record key. A block is up to 64 KiB and
     * packs many consecutive ids, so decoding one per probe dominated the miss path; holding a few
     * costs a fixed number of references and no per-id state.
     */
    private long @Nullable [] cachedBlockKeys;
    private ValueDictionaryValueBlockNode @Nullable [] cachedBlocks;
    /**
     * Separator array over the ordered prefix, loaded ONCE per view and then kept. It is the whole
     * point of the structure: without it a binary-search probe decodes one block per step, with it one
     * block per probe, and re-reading it per probe would give back exactly what it saves.
     */
    private final long blockIndexKey;

    private @Nullable ValueDictionaryBlockIndexNode blockIndex;

    private boolean blockIndexLoaded;

    private int @Nullable [] transformedIds;
    private int @Nullable [] transformedStarts;
    private int @Nullable [] transformedLengths;
    private byte @Nullable [] transformedModes;
    private long @Nullable [] transformedValues;

    /**
     * Whether EVERY id is in collation order of its value — {@link ValueDictionaryHeaderNode#idsAreCollationOrdered},
     * the single test an ordering arm may make. While it holds, id order IS value order, so id
     * comparisons answer string comparisons with no dictionary touch at all.
     */
    private final boolean fullyOrdered;

    /**
     * Whether STORAGE is in collation order — {@link ValueDictionaryHeaderNode#isFullyOrdered}. Weaker
     * than {@link #fullyOrdered}: behind a rank table the positions are ordered while the ids are not,
     * so two ids still compare as their positions do, at the price of two table reads.
     */
    private final boolean storageOrdered;

    /**
     * Key of the first {@link ValueDictionaryRankTableNode} record, or {@code 0} when ids ARE storage
     * positions. Under a table the view's callers speak MINTS (the ids rows carry) and the reverse
     * radix speaks POSITIONS (collation order); {@link #positionOf} translates on every miss through
     * the forward run and {@link #mintAtPosition} translates back for the probe through the inverse
     * run that follows it ({@code rankTableKey + recordCount + i}).
     */
    private final long rankTableKey;

    /** Mints the table covers; a mint above it is its own position (an unordered tail appended later). */
    private final int orderedPrefixCount;

    /**
     * The table's records, both runs, fetched on first use and then held — REFERENCES into the record
     * cache, never a per-view copy. A sealed 100M segment dictionary has ~17 records of 16384 entries
     * per run; one flat {@code int[]} per view would be 1 MB times every worker of every query that
     * opens it, and an inverse built per view would be the same again per PROBE.
     */
    private ValueDictionaryRankTableNode @Nullable [] rankTable;

    /**
     * Where {@link #locate} leaves the slice of the position it resolved. A scratch, not a cache: the
     * slice cache above is keyed by MINT and filled from here, while the binary-search probe reads
     * positions straight off it without touching the cache at all.
     */
    private byte @Nullable [] locatedBacking;
    private int locatedOffset;
    private int locatedLength;
    private @Nullable ValueDictionaryEntryNode locatedSpill;

    /**
     * The per-segment views this one is a union of, or {@code null} for an ordinary single-dictionary
     * view.
     *
     * <p>
     * A SEGMENT-scoped column has one dictionary per segment, and a cell carries its segment in the
     * high 32 bits ({@link ProjectionIndexRowGroupPage#packSegmentCell}). A union view unpacks the
     * cell and delegates, so every consumer that resolves a CELL keeps working untouched. What it
     * cannot do is anything indexed by ID SPACE — a verdict bitset, a length table over a range —
     * because packed cells are not a dense id space; those refuse rather than answer, and the callers
     * that would use them already decline a column that is not resource-wide.
     * </p>
     */
    private final ReadView @Nullable [] perSegment;

    /** A union over one dictionary per segment; index is the segment, {@code null} where none. */
    private ReadView(final ReadView @Nullable [] perSegment, final int revision) {
      this.perSegment = perSegment;
      this.revision = revision;
      this.headerNodeKey = 0L;
      this.reverseRootKey = 0L;
      this.forwardRootKey = 0L;
      this.entryCount = 0;
      this.blockIndexKey = 0L;
      this.fullyOrdered = false;
      this.storageOrdered = false;
      this.rankTableKey = 0L;
      this.orderedPrefixCount = 0;
      this.namePage = null;
      this.databaseType = null;
      this.reader = null;
    }

    private ReadView(final long headerNodeKey, final ValueDictionaryHeaderNode header, final int revision,
        final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
      this.perSegment = null;
      this.blockIndexKey = header.getBlockIndexKey();
      this.fullyOrdered = header.idsAreCollationOrdered();
      this.storageOrdered = header.isFullyOrdered();
      this.rankTableKey = header.getRankTableKey();
      this.orderedPrefixCount = header.getOrderedPrefixCount();
      this.headerNodeKey = headerNodeKey;
      this.reverseRootKey = header.getReverseRootKey();
      this.forwardRootKey = header.getForwardRootKey();
      this.entryCount = header.getEntryCount();
      this.revision = revision;
      this.namePage = namePage;
      this.databaseType = databaseType;
      this.reader = reader;
    }

    /**
     * Whether id order IS collation order for every entry — the one test an ordering arm may make
     * before it compares ids as numbers ({@link #compareIds} does so itself; a plan that bounds leaves
     * by the ids in their descriptors must ask first).
     */
    public boolean fullyOrdered() {
      return fullyOrdered;
    }

    /**
     * Whether a rank table stands between the ids rows carry and storage positions. A verdict arm
     * must DECLINE such a view before asking ({@link #stringOpVerdict} refuses with an
     * {@link UnsupportedOperationException} as the backstop): the verdict is built by walking
     * storage in position order and would otherwise have to be re-indexed by mint.
     */
    public boolean hasRankTable() {
      return rankTableKey != 0L;
    }

    /** Dictionary header key this view was opened for. */
    public long headerNodeKey() {
      return headerNodeKey;
    }

    /** Resource revision whose dictionary roots and pages this view retains. */
    public int revision() {
      return revision;
    }

    /** Number of ids readable in this revision. */
    public int entryCount() {
      return entryCount;
    }

    /**
     * Per-id string lengths for the whole dictionary, indexed by id (slot 0 unused).
     *
     * <p>
     * Mode {@code STRING_LENGTH_UTF8_BYTES} is each value's stored byte length;
     * {@code STRING_LENGTH_CODE_POINTS} counts non-continuation bytes — the same derivations the
     * per-leaf dictionary kernels apply per entry, lifted to once per distinct value per query. The
     * returned table is immutable by convention and safe to share across scan workers.
     */
    public int[] lengthTable(final byte lengthMode) {
      final int[] table = new int[entryCount + 1];
      fillLengthTable(lengthMode, 1, entryCount, table);
      return table;
    }

    /**
     * Fill {@code table[fromId..toId]} with the per-id string lengths of this view, in the given mode —
     * the id-range half of {@link #lengthTable(byte)}, so callers holding one view PER WORKER can
     * derive one table over disjoint id ranges in parallel (the view's slice caches are
     * single-threaded; the table's disjoint ranges need no coordination). Ids are walked in order, so
     * every block of the range is decoded once — while ids are storage positions; behind a rank table
     * the walk is in mint order, which visits storage at random, so it is correct there and not fast.
     *
     * @param lengthMode {@link ProjectionIndexByteScan#STRING_LENGTH_UTF8_BYTES} or
     *        {@link ProjectionIndexByteScan#STRING_LENGTH_CODE_POINTS}
     * @param fromId first id to derive, at least 1
     * @param toId last id to derive, inclusive, at most {@link #entryCount()}
     * @param table the table indexed by id, at least {@code toId + 1} long
     */
    public void fillLengthTable(final byte lengthMode, final int fromId, final int toId, final int[] table) {
      if (lengthMode != ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
          && lengthMode != ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
        throw new IllegalArgumentException("not a string length mode: " + lengthMode);
      }
      if (fromId < 1 || toId > entryCount || toId >= table.length) {
        throw new IllegalArgumentException(
            "id range [" + fromId + ", " + toId + "] outside 1.." + entryCount + " or the table of " + table.length);
      }
      for (int id = fromId; id <= toId; id++) {
        final int slot = sliceSlot(id);
        final ValueDictionaryEntryNode spill = cachedSpills[slot];
        if (spill != null) {
          table[id] = lengthMode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
              ? spill.getValueLength()
              : spill.codePointLength();
          continue;
        }
        final int len = cachedLengths[slot];
        if (lengthMode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
          table[id] = len;
        } else {
          final byte[] backing = cachedBacking[slot];
          final int off = cachedOffsets[slot];
          int codePoints = 0;
          for (int b = off; b < off + len; b++) {
            if ((backing[b] & 0xC0) != 0x80) {
              codePoints++;
            }
          }
          table[id] = codePoints;
        }
      }
    }

    /**
     * Materialize the value interned under {@code id} as a {@link String}.
     *
     * <p>
     * For WINNERS only — group emission, deferred-extremum results — never for per-row work: the whole
     * point of the id lanes is that rows stay integers. Packed ids decode straight off their slice;
     * spilled ids go through the record's defensive copy, which is fine at winner cardinality.
     */
    /**
     * The view holding the dictionary a packed cell names.
     *
     * @throws IllegalStateException if the cell names a segment this index sealed nothing in — a cell
     *         that cannot be resolved is refused, never resolved against a neighbour's dictionary
     */
    private ReadView segmentViewOf(final long cell) {
      final ReadView[] segments = perSegment;
      final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
      final ReadView view = segment >= 0 && segment < segments.length
          ? segments[segment]
          : null;
      if (view == null) {
        throw new IllegalStateException("cell names segment " + segment + ", which sealed no dictionary for this"
            + " column; its rows keep their bytes and must not be resolved against another segment's ids");
      }
      return view;
    }

    /**
     * Sweep the dictionary of the segment {@code anyCellInSegment} belongs to, returning verdict bits
     * over the MINT ids that segment's rows carry.
     *
     * <p>
     * The packed-cell entry point to {@link #stringOpVerdictByMint}: a union view holds one dictionary
     * per segment, and a cell names which. Addressing by cell rather than by segment index keeps the
     * {@code perSegment} array private and reuses {@link #segmentViewOf}'s refusal — a cell naming a
     * segment that sealed nothing is refused here too, rather than swept against a neighbour.
     * </p>
     *
     * @param anyCellInSegment any packed cell of the segment to sweep
     * @param op a per-value string op
     * @param literalUtf8 the literal's UTF-8 bytes
     * @return bit {@code id} set iff the value under that id satisfies {@code op}
     */
    public long[] stringOpVerdictByMintOfCell(final long anyCellInSegment, final ProjectionIndexScan.Op op,
        final byte[] literalUtf8) {
      return perSegment == null
          ? stringOpVerdictByMint(op, literalUtf8)
          : segmentViewOf(anyCellInSegment).stringOpVerdictByMint(op, literalUtf8);
    }

    /**
     * The storage POSITION of a packed cell within its own segment's dictionary, or {@code -1} when
     * that dictionary keeps no collation-ordered storage.
     *
     * <p>
     * Why a caller wants this: a sealed segment dictionary stores its values in collation order, so
     * within ONE segment position order IS value order and an ordering question becomes an integer
     * compare — the same identity {@link #compareIds} already exploits for {@code storageOrdered}.
     * Across segments the positions mean nothing to each other, so a caller may only use this to
     * order within a segment and must compare VALUES to merge segments.
     * </p>
     */
    public int positionOfCell(final long anyCell) {
      final ReadView view = perSegment == null
          ? this
          : segmentViewOf(anyCell);
      if (!view.storageOrdered) {
        return -1;
      }
      final int id = perSegment == null
          ? (int) anyCell
          : ProjectionIndexRowGroupPage.idOfCell(anyCell);
      return id >= 1 && id <= view.entryCount
          ? view.positionOf(id)
          : -1;
    }

    /**
     * Entries in the dictionary of the segment {@code anyCell} names, or {@code -1} when that segment
     * keeps no collation-ordered storage to walk.
     */
    public int segmentEntryCount(final long anyCell) {
      final ReadView view = perSegment == null
          ? this
          : segmentViewOf(anyCell);
      return view.storageOrdered
          ? view.entryCount
          : -1;
    }

    /**
     * The MINT stored at {@code position} within the segment {@code anyCell} names.
     *
     * <p>
     * Lets a caller walk a segment's values in STORAGE order — which is collation order — while
     * addressing them by the ids rows carry. Reading values in this order decodes each block once,
     * where reading them in mint order re-decodes a block per value.
     * </p>
     */
    public int mintAtPositionOfCell(final long anyCell, final int position) {
      final ReadView view = perSegment == null
          ? this
          : segmentViewOf(anyCell);
      return view.mintAtPosition(position);
    }

    /**
     * A sequential cursor over the storage positions of the segment {@code anyCell} names, or
     * {@code null} when that dictionary keeps no collation-ordered storage to walk.
     *
     * <p>
     * The walking counterpart of {@link #mintAtPositionOfCell}: where that answers one position
     * through this view's per-mint caches, the cursor holds the block and the inverse rank-table
     * record of the position it is at and moves on only when a seek leaves them — the shape a merge
     * over every segment at once needs, which reads each block once per range and compares bytes it
     * already holds ({@link SegmentRunCursor}). The cursor shares this view's rank-table records and
     * is as thread-private as the view is.
     * </p>
     */
    public @Nullable SegmentRunCursor positionCursorOfCell(final long anyCell) {
      final ReadView view = perSegment == null
          ? this
          : segmentViewOf(anyCell);
      if (!view.storageOrdered) {
        return null;
      }
      view.ensureRevision();
      return new PositionCursor(view);
    }

    /** Whether this view resolves packed {@code (segment, id)} cells rather than bare ids. */
    public boolean isSegmentUnion() {
      return perSegment != null;
    }

    /**
     * Ids readable in the dictionary of segment {@code segment} of a union, or {@code -1} when that
     * segment sealed none for this column (its rows keep their bytes) or this is not a union.
     *
     * <p>
     * Unlike {@link #segmentEntryCount}, answered whatever the dictionary's storage order: a caller
     * sizing a per-id table needs the count, not a walkable order.
     * </p>
     */
    public int entryCountOfSegment(final int segment) {
      final ReadView[] segments = perSegment;
      if (segments == null || segment < 0 || segment >= segments.length) {
        return -1;
      }
      final ReadView view = segments[segment];
      return view == null
          ? -1
          : view.entryCount;
    }

    /**
     * The value a packed {@code (segment, id)} CELL names — the long-width entry point a
     * segment-scoped column needs.
     *
     * <p>
     * A cell does not fit in an {@code int}: the segment lives in its high 32 bits, so
     * {@code valueAsString((int) cell)} would silently truncate the segment away and resolve every
     * cell against segment 0. That is invisible while a resource has one segment and wrong the moment
     * it has two, which is why the packed path has its own signature rather than sharing the id one.
     * </p>
     */
    public @Nullable String valueOfCell(final long cell) {
      if (perSegment == null) {
        return valueAsString((int) cell);
      }
      final ReadView segmentView = segmentViewOf(cell);
      final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
      if (id < 1 || id > segmentView.entryCount()) {
        // An id its own segment's dictionary does not contain. Resolving it anyway would return
        // whatever entry the index happens to land on -- a real value, belonging to another row --
        // so the only safe answer is "cannot resolve", which makes the caller decline.
        if (PROJ_DIAG) {
          System.err.println("[segdict] cell names segment " + ProjectionIndexRowGroupPage.segmentOfCell(cell)
              + " id " + id + ", but that dictionary holds " + segmentView.entryCount() + " entries");
        }
        return null;
      }
      return segmentView.valueAsString(id);
    }

    /**
     * Whether the value a packed CELL names satisfies {@code op} against {@code literalUtf8},
     * evaluated on the stored BYTES — no {@link String} is built.
     *
     * <p>
     * The per-value half of a two-phase string predicate on a segment-scoped column, and the reason
     * such a column needs no dictionary-wide sweep: a caller memoises this per {@code (segment, id)},
     * so the byte work is paid once per distinct value the column actually REFERENCES, which is at
     * most — and usually far less than — the dictionary's size. It is also indifferent to a rank
     * table, because it addresses an id rather than a storage position.
     * </p>
     *
     * @return the verdict, or {@code null} when the cell names no entry of its segment
     */
    /**
     * A 64-bit content hash of the value a packed CELL names, computed on the STORED BYTES.
     *
     * <p>
     * What a group-by over a segment-scoped column needs and a {@link String} is the wrong way to get:
     * building one per distinct value costs about 150 bytes and a GC-visible object each, which is
     * invisible at a million distinct values and is 2.7 GB at eighteen million. Every column store
     * that dictionary-encodes strings compares and hashes bytes in place for exactly this reason.
     * </p>
     *
     * @return the hash, or 0 when the cell names no entry — a caller must treat 0 as "unresolvable"
     *         rather than as a hash, since it cannot tell them apart
     */
    public long cellHash(final long cell) {
      final ReadView view;
      final int id;
      if (perSegment == null) {
        view = this;
        id = (int) cell;
      } else {
        view = segmentViewOf(cell);
        id = ProjectionIndexRowGroupPage.idOfCell(cell);
      }
      if (id < 1 || id > view.entryCount()) {
        return 0L;
      }
      final int slot = view.sliceSlot(id);
      final ValueDictionaryEntryNode spill = view.cachedSpills[slot];
      if (spill != null) {
        final byte[] bytes = spill.getValue();
        return ProjectionIndexByteScan.fnv1a64(bytes, 0, bytes.length);
      }
      return ProjectionIndexByteScan.fnv1a64(view.cachedBacking[slot], view.cachedOffsets[slot],
          view.cachedLengths[slot]);
    }

    /**
     * The string length of the value a packed CELL names, in the given mode, read off the stored
     * bytes — no {@link String} is built.
     *
     * <p>
     * The per-cell twin of {@link #fillLengthTable}: a segment-scoped length table is derived per
     * canonical id rather than per dictionary id, and the canonicaliser that owns those ids walks the
     * segments in storage order and asks here for each cell it lands on.
     * </p>
     *
     * @param lengthMode {@link ProjectionIndexByteScan#STRING_LENGTH_UTF8_BYTES} or
     *        {@link ProjectionIndexByteScan#STRING_LENGTH_CODE_POINTS}
     * @return the length, or {@code -1} when the cell names no entry of its segment
     */
    public int valueLengthOfCell(final long cell, final byte lengthMode) {
      if (lengthMode != ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
          && lengthMode != ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
        throw new IllegalArgumentException("not a string length mode: " + lengthMode);
      }
      final ReadView view;
      final int id;
      if (perSegment == null) {
        view = this;
        id = (int) cell;
      } else {
        view = segmentViewOf(cell);
        id = ProjectionIndexRowGroupPage.idOfCell(cell);
      }
      if (id < 1 || id > view.entryCount()) {
        return -1;
      }
      final int slot = view.sliceSlot(id);
      final ValueDictionaryEntryNode spill = view.cachedSpills[slot];
      if (spill != null) {
        return lengthMode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
            ? spill.getValueLength()
            : spill.codePointLength();
      }
      final int len = view.cachedLengths[slot];
      if (lengthMode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
        return len;
      }
      final byte[] backing = view.cachedBacking[slot];
      final int off = view.cachedOffsets[slot];
      int codePoints = 0;
      for (int b = off; b < off + len; b++) {
        if ((backing[b] & 0xC0) != 0x80) {
          codePoints++;
        }
      }
      return codePoints;
    }

    public @Nullable Boolean cellMatchesStringOp(final long cell, final ProjectionIndexScan.Op op,
        final byte[] literalUtf8, final boolean literalHasSupplementary) {
      Objects.requireNonNull(op, "op must not be null");
      Objects.requireNonNull(literalUtf8, "literalUtf8 must not be null");
      final ReadView view;
      final int id;
      if (perSegment == null) {
        view = this;
        id = (int) cell;
      } else {
        view = segmentViewOf(cell);
        id = ProjectionIndexRowGroupPage.idOfCell(cell);
      }
      if (id < 1 || id > view.entryCount()) {
        return null;
      }
      final int slot = view.sliceSlot(id);
      final ValueDictionaryEntryNode spill = view.cachedSpills[slot];
      if (spill != null) {
        final byte[] bytes = spill.getValue();
        return ProjectionIndexScan.stringDictEntryMatches(bytes, 0, bytes.length, op, literalUtf8,
            literalHasSupplementary);
      }
      return ProjectionIndexScan.stringDictEntryMatches(view.cachedBacking[slot], view.cachedOffsets[slot],
          view.cachedLengths[slot], op, literalUtf8, literalHasSupplementary);
    }

    public String valueAsString(final int id) {
      final ReadView[] segments = perSegment;
      if (segments != null) {
        // An int cannot carry a cell's segment; a union view must be asked through valueOfCell.
        throw new IllegalStateException("this view resolves packed (segment, id) cells; call valueOfCell(long)");
      }
      final int slot = sliceSlot(id);
      final ValueDictionaryEntryNode spill = cachedSpills[slot];
      if (spill != null) {
        return new String(spill.getValue(), StandardCharsets.UTF_8);
      }
      return new String(cachedBacking[slot], cachedOffsets[slot], cachedLengths[slot], StandardCharsets.UTF_8);
    }

    /**
     * Evaluate one string predicate against EVERY value in this revision's dictionary, returning a
     * verdict bitset over id space: bit {@code id} (1-based, bit 0 unused) is set iff the value
     * interned under {@code id} satisfies {@code op} against {@code literalUtf8}.
     *
     * <p>
     * This is the global half of the two-phase pattern the per-leaf dictionaries already use
     * ({@code evalStringDict}): the string work runs once per DISTINCT value here, and every row group
     * afterwards answers each row with one bit test against the id it already stores. Packed ids
     * evaluate over their zero-copy {@code (backing, offset, length)} slices through the same per-entry
     * authority the leaf kernels use ({@code ProjectionIndexScan.stringDictEntryMatches}), so op
     * semantics — including the UTF-16 collation contract for the ordering ops — cannot drift between
     * the two dictionary tiers. Spilled ids evaluate through their record's own entry points, which
     * exist so the record's array never escapes.
     *
     * <p>
     * Sequential ids share sub-blocks, so the sweep runs at block-cache speed; the returned bitset is
     * immutable by convention and safe to share across scan workers.
     *
     * @param op one of {@code EQ}, {@code NE}, {@code STR_LT/LE/GT/GE}, {@code STR_CONTAINS}
     * @param literalUtf8 the literal, UTF-8 encoded
     * @return the verdict bitset, sized {@code (entryCount + 64) >> 6} words
     */
    public long[] stringOpVerdict(final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
      Objects.requireNonNull(op, "op must not be null");
      Objects.requireNonNull(literalUtf8, "literalUtf8 must not be null");
      switch (op) {
        case EQ, NE, STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> {
        }
        default -> throw new IllegalArgumentException("not a per-value string op: " + op);
      }
      refuseVerdictUnderRankTable();
      final long[] verdict = newVerdict();
      fillStringOpVerdict(op, literalUtf8, 0, verdictBucketCount(), verdict, 0);
      return verdict;
    }

    /**
     * The verdict sweep sets bit {@code position} — it walks storage — while every row carries a
     * MINT, and the bucket-to-word aliasing the split sweep is built on ({@link VerdictSlice}) does
     * not survive a permutation between the two. Until the sweep is rebuilt in rank space, a caller
     * holding a tabled dictionary must take a route that does not depend on the bitset.
     */
    private void refuseVerdictUnderRankTable() {
      if (rankTableKey != 0L) {
        throw new UnsupportedOperationException("string-op verdicts over value dictionary " + headerNodeKey
            + " are not served: it maps ids through a rank table (key " + rankTableKey
            + "), and the sweep sets bits by storage position while rows carry mints");
      }
    }

    /**
     * An empty verdict bitset for this revision, sized {@code (entryCount + 64) >> 6} words — one bit
     * per id plus the unused bit zero, which is the size every consumer of a verdict assumes.
     *
     * @return a fresh, zeroed bitset
     */
    public long[] newVerdict() {
      return new long[entryCount + 64 >>> 6];
    }

    /**
     * Lane {@code lane} of {@code lanes} over this dictionary's buckets — {@link VerdictSlice#forLane}
     * against {@link #verdictBucketCount()}, so a caller never repeats either number.
     *
     * @param lane the lane, {@code 0 <= lane < lanes}
     * @param lanes number of lanes the sweep is split into, at least one
     * @return the lane's share
     */
    public VerdictSlice verdictSlice(final int lane, final int lanes) {
      return VerdictSlice.forLane(verdictBucketCount(), lane, lanes);
    }

    /**
     * Buckets the reverse dictionary holds — the unit {@link #fillStringOpVerdict} is split on.
     *
     * @return bucket count, {@code 0} when the dictionary is empty
     */
    public int verdictBucketCount() {
      return entryCount == 0
          ? 0
          : (entryCount - 1 >>> 8) + 1;
    }

    /**
     * Fill the verdict bits for buckets {@code [bucketLo, bucketHi)} into {@code out}, whose word
     * {@code 0} is the global verdict word {@code wordBase}.
     *
     * <h2>Why a caller-owned slice and not the shared array</h2>
     *
     * Ids are 1-based and bucketed by {@code (id - 1) >>> 8}, so bucket {@code b} owns ids
     * {@code 256b+1 .. 256b+256} — which occupy verdict words {@code 4b .. 4b+4}, FIVE words, whose
     * last is also bucket {@code b+1}'s first. Adjacent buckets therefore SHARE a boundary word, and
     * two lanes OR-ing into it concurrently would lose one another's bits: a silently dropped row at
     * one id in 256, on a path whose whole job is to decide which rows match. Each lane fills its own
     * slice and the caller merges; the merge is a linear OR over 4*(bucketHi-bucketLo)+1 words.
     *
     * @param op one of {@code EQ}, {@code NE}, {@code STR_LT/LE/GT/GE}, {@code STR_CONTAINS}
     * @param literalUtf8 the literal, UTF-8 encoded
     * @param bucketLo first bucket, inclusive
     * @param bucketHi last bucket, exclusive
     * @param out destination, at least {@code 4 * (bucketHi - bucketLo) + 1} words
     * @param wordBase the global verdict word {@code out[0]} stands for, normally {@code 4*bucketLo}
     * @throws IllegalArgumentException if the range is not within the dictionary
     */
    /**
     * Evaluate {@code op} against EVERY value of this dictionary in one sequential pass, returning a
     * verdict bitset indexed by the MINT ids rows carry.
     *
     * <p>
     * The rank-aware twin of {@link #fillStringOpVerdict}, and the reason it has to exist: that one
     * indexes by storage position and therefore refuses a dictionary with a rank table, which every
     * sealed segment dictionary has. Refusing pushed the segment lane onto a per-referenced-cell
     * resolution — a random dictionary read for each distinct value a query touches, measured at 46 s
     * for one LIKE over 100M rows. Walking storage once is sequential and reads each block's packed
     * bytes in place; the rank table then says which id each position belongs to, one record lookup
     * per entry.
     * </p>
     *
     * @return bit {@code id} set iff the value under that id satisfies {@code op}
     */
    /** The numbers any refusal of a bulk route has to name to be actionable. */
    private String shape() {
      return "entryCount=" + entryCount + " orderedPrefix=" + orderedPrefixCount + " reverseRoot=" + reverseRootKey
          + " forwardRoot=" + forwardRootKey + " rankTable=" + hasRankTable() + " storageOrdered=" + storageOrdered;
    }

    public long[] stringOpVerdictByMint(final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
      Objects.requireNonNull(op, "op must not be null");
      Objects.requireNonNull(literalUtf8, "literalUtf8 must not be null");
      try {
        return sweepStringOp(op, literalUtf8);
      } catch (final RuntimeException failed) {
        // A bulk walk that lands on the wrong record must say WHICH dictionary it was walking; the
        // shape is the difference between an actionable refusal and a silent slow path.
        throw new IllegalStateException("sweep failed over " + shape() + ": " + failed, failed);
      }
    }

    private long[] sweepStringOp(final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
      final long[] verdict = newVerdict();
      final boolean litHasSupplementary =
          ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
      final int buckets = verdictBucketCount();
      for (int bucket = 0; bucket < buckets; bucket++) {
        final ValueDictionaryValueBucketNode bucketNode =
            GlobalValueDictionaryRadix.valueBucketOf(reverseRootKey, bucket, namePage, databaseType, reader);
        if (bucketNode == null) {
          throw new IllegalStateException("value dictionary bucket " + bucket + " of " + buckets
              + " is missing from revision " + revision + " (" + shape() + ")");
        }
        final int blocks = bucketNode.blockCount();
        for (int block = 0; block < blocks; block++) {
          final int blockFirstPosition = bucketNode.blockFirstId(block);
          final ValueDictionaryValueBlockNode node = GlobalValueDictionaryRadix.blockNode(bucketNode.blockKey(block),
              blockFirstPosition, namePage, databaseType, reader);
          if (node == null) {
            throw new IllegalStateException("value dictionary block " + blockFirstPosition + " is missing from"
                + " revision " + revision);
          }
          final byte[] bytes = node.rawBytes();
          final int count = node.size();
          int start = node.offsetAt(0);
          for (int index = 0; index < count; index++) {
            final int end = node.offsetAt(index + 1);
            if (ProjectionIndexScan.stringDictEntryMatches(bytes, start, end - start, op, literalUtf8,
                litHasSupplementary)) {
              final int id = mintAtPosition(blockFirstPosition + index);
              verdict[id >>> 6] |= 1L << (id & 63);
            }
            start = end;
          }
        }
        final int spills = bucketNode.spillCount();
        for (int spill = 0; spill < spills; spill++) {
          final ValueDictionaryEntryNode entry =
              GlobalValueDictionaryRadix.spillEntry(bucketNode.spillKeyAt(spill), namePage, databaseType, reader);
          if (entry == null) {
            throw new IllegalStateException("value dictionary spill for position " + bucketNode.spillId(spill)
                + " is missing from revision " + revision);
          }
          if (spillMatches(entry, op, literalUtf8)) {
            final int id = mintAtPosition(bucketNode.spillId(spill));
            verdict[id >>> 6] |= 1L << (id & 63);
          }
        }
      }
      return verdict;
    }

    public void fillStringOpVerdict(final ProjectionIndexScan.Op op, final byte[] literalUtf8, final int bucketLo,
        final int bucketHi, final long[] out, final int wordBase) {
      Objects.requireNonNull(op, "op must not be null");
      Objects.requireNonNull(literalUtf8, "literalUtf8 must not be null");
      Objects.requireNonNull(out, "out must not be null");
      switch (op) {
        case EQ, NE, STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> {
        }
        default -> throw new IllegalArgumentException("not a per-value string op: " + op);
      }
      refuseVerdictUnderRankTable();
      final int buckets = verdictBucketCount();
      if (bucketLo < 0 || bucketHi > buckets || bucketLo > bucketHi) {
        throw new IllegalArgumentException(
            "bucket range [" + bucketLo + ", " + bucketHi + ") outside the dictionary's " + buckets);
      }
      if (bucketLo == bucketHi) {
        return;
      }
      final boolean litHasSupplementary = ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
      // BLOCK-AT-A-TIME, not id-at-a-time. A per-id walk routes all entryCount values through
      // sliceSlot, whose direct-mapped slice cache MISSES on every one of them — ascending ids never
      // repeat a slot — so each value pays a revision check, two cache probes and a bucket search to
      // reach bytes the block it came from is already holding. Measured on a 275,494-entry URL
      // dictionary that machinery alone was 16.5 ms of a 25.1 ms sweep. Walking the reverse trie
      // once and reading each block's packed bytes in place removes it without changing a verdict.
      for (int bucket = bucketLo; bucket < bucketHi; bucket++) {
        final ValueDictionaryValueBucketNode bucketNode =
            GlobalValueDictionaryRadix.valueBucketOf(reverseRootKey, bucket, namePage, databaseType, reader);
        if (bucketNode == null) {
          throw new IllegalStateException(
              "global value dictionary bucket " + bucket + " is missing from revision " + revision);
        }
        final int blocks = bucketNode.blockCount();
        for (int block = 0; block < blocks; block++) {
          final int blockFirstId = bucketNode.blockFirstId(block);
          final ValueDictionaryValueBlockNode node = GlobalValueDictionaryRadix.blockNode(bucketNode.blockKey(block),
              blockFirstId, namePage, databaseType, reader);
          if (node == null) {
            throw new IllegalStateException(
                "global value dictionary block " + blockFirstId + " is missing from " + "revision " + revision);
          }
          // Read the packed bytes once; a coded block expanded them when it deserialized, and
          // re-entering through valueOffset(id) per value would re-check the id's range for nothing.
          final byte[] bytes = node.rawBytes();
          final int count = node.size();
          int start = node.offsetAt(0);
          for (int index = 0; index < count; index++) {
            final int end = node.offsetAt(index + 1);
            if (ProjectionIndexScan.stringDictEntryMatches(bytes, start, end - start, op, literalUtf8,
                litHasSupplementary)) {
              final int id = blockFirstId + index;
              out[(id >>> 6) - wordBase] |= 1L << (id & 63);
            }
            start = end;
          }
        }
        // Values too large for a block live beside them, one record each; they are rare by
        // construction, so they keep the per-record path rather than earning a bulk one.
        final int spills = bucketNode.spillCount();
        for (int spill = 0; spill < spills; spill++) {
          final ValueDictionaryEntryNode entry =
              GlobalValueDictionaryRadix.spillEntry(bucketNode.spillKeyAt(spill), namePage, databaseType, reader);
          if (entry == null) {
            throw new IllegalStateException("global value dictionary spill for id " + bucketNode.spillId(spill)
                + " is missing from revision " + revision);
          }
          if (spillMatches(entry, op, literalUtf8)) {
            final int id = bucketNode.spillId(spill);
            out[(id >>> 6) - wordBase] |= 1L << (id & 63);
          }
        }
      }
    }

    /**
     * Op dispatch for a SPILLED value, through the record's no-escape entry points. Semantics mirror
     * {@code stringDictEntryMatches} arm for arm; ordering uses {@code compareToRange}, which is UTF-16
     * collation unconditionally — the same order the byte-path arm reaches via its
     * supplementary-character fallback.
     */
    private static boolean spillMatches(final ValueDictionaryEntryNode spill, final ProjectionIndexScan.Op op,
        final byte[] literalUtf8) {
      return switch (op) {
        case EQ -> spill.valueEquals(literalUtf8, 0, literalUtf8.length);
        case NE -> !spill.valueEquals(literalUtf8, 0, literalUtf8.length);
        case STR_CONTAINS -> spill.containsNeedle(literalUtf8, 0, literalUtf8.length);
        case STR_LT -> spill.compareToRange(literalUtf8, 0, literalUtf8.length) < 0;
        case STR_LE -> spill.compareToRange(literalUtf8, 0, literalUtf8.length) <= 0;
        case STR_GT -> spill.compareToRange(literalUtf8, 0, literalUtf8.length) > 0;
        case STR_GE -> spill.compareToRange(literalUtf8, 0, literalUtf8.length) >= 0;
        default -> throw new IllegalStateException("not a per-value string op: " + op);
      };
    }

    /** Compare two ids under the query engine's UTF-16 string collation without materialisation. */
    /**
     * Order two packed CELLS by the values they name, under the dictionary's collation.
     *
     * <p>
     * Within one segment this is {@link #compareIds}, which a rank table already answers from two
     * position reads without touching a value. ACROSS segments neither id space says anything about
     * the other's, so the values themselves decide — and because the two operands come from different
     * views, their slice caches cannot alias, which is the hazard {@code compareIds} lifts locals to
     * avoid. Allocation-free either way: no {@link String} is built.
     * </p>
     */
    public int compareCells(final long leftCell, final long rightCell) {
      if (leftCell == rightCell) {
        return 0;
      }
      if (perSegment == null) {
        return compareIds((int) leftCell, (int) rightCell);
      }
      final int leftId = ProjectionIndexRowGroupPage.idOfCell(leftCell);
      final int rightId = ProjectionIndexRowGroupPage.idOfCell(rightCell);
      final ReadView left = segmentViewOf(leftCell);
      final ReadView right = segmentViewOf(rightCell);
      if (left == right) {
        return left.compareIds(leftId, rightId);
      }
      final int leftSlot = left.sliceSlot(leftId);
      final ValueDictionaryEntryNode leftSpill = left.cachedSpills[leftSlot];
      final byte[] leftBacking = left.cachedBacking[leftSlot];
      final int leftOffset = left.cachedOffsets[leftSlot];
      final int leftLength = left.cachedLengths[leftSlot];
      final int rightSlot = right.sliceSlot(rightId);
      final ValueDictionaryEntryNode rightSpill = right.cachedSpills[rightSlot];
      if (leftSpill == null) {
        return rightSpill == null
            ? ValueDictionaryEntryNode.compareUtf16Range(leftBacking, leftOffset, leftLength,
                right.cachedBacking[rightSlot], right.cachedOffsets[rightSlot], right.cachedLengths[rightSlot])
            : -rightSpill.compareToRange(leftBacking, leftOffset, leftLength);
      }
      return rightSpill == null
          ? leftSpill.compareToRange(right.cachedBacking[rightSlot], right.cachedOffsets[rightSlot],
              right.cachedLengths[rightSlot])
          : leftSpill.compareValueUtf16(rightSpill);
    }

    public int compareIds(final int leftId, final int rightId) {
      if (leftId == rightId) {
        return 0;
      }
      if (fullyOrdered) {
        // Rank-ordered dictionary: id order IS collation order (W17's witnessed identity), so the
        // comparison needs no slice resolution — the difference between an integer compare and two
        // RANDOM block loads per row (measured: pass-2 string extrema over a global operand were
        // ~54 s of q28's 60 s at 100M, dictionary 10× the record cache).
        return Integer.compare(leftId, rightId);
      }
      if (storageOrdered) {
        // Ordered storage behind a rank table: the POSITIONS are in collation order even though the
        // mints are not, so two table reads answer the comparison and no value is touched.
        return Integer.compare(positionOf(leftId), positionOf(rightId));
      }
      // Both slices are resolved BEFORE either is read: the two ids may share a cache slot, and
      // reading through a slot the second resolution has already overwritten would compare the wrong
      // value. Copying the left operand out would fix that too — and reintroduce the per-compare
      // allocation this path exists to remove — so the left triple is lifted into locals instead.
      final int leftSlot = sliceSlot(leftId);
      final byte[] leftBacking = cachedBacking[leftSlot];
      final int leftOffset = cachedOffsets[leftSlot];
      final int leftLength = cachedLengths[leftSlot];
      final ValueDictionaryEntryNode leftSpill = cachedSpills[leftSlot];
      final int rightSlot = sliceSlot(rightId);
      final byte[] rightBacking = cachedBacking[rightSlot];
      final ValueDictionaryEntryNode rightSpill = cachedSpills[rightSlot];
      if (leftSpill == null) {
        return rightSpill == null
            ? ValueDictionaryEntryNode.compareUtf16Range(leftBacking, leftOffset, leftLength, rightBacking,
                cachedOffsets[rightSlot], cachedLengths[rightSlot])
            : -rightSpill.compareToRange(leftBacking, leftOffset, leftLength);
      }
      return rightSpill == null
          ? leftSpill.compareToRange(rightBacking, cachedOffsets[rightSlot], cachedLengths[rightSlot])
          : leftSpill.compareValueUtf16(rightSpill);
    }

    /**
     * Compare the value stored at storage {@code position} to a caller-owned byte range, under the
     * same collation.
     *
     * <p>
     * The binary-search probe's inner loop, which searches STORAGE — the ordered prefix is a range of
     * positions, and under a rank table those are not the ids rows carry. It exists so that the search
     * reuses this view's bucket and decoded-block caches instead of walking the reverse radix and
     * decoding a 33 KB block from scratch for every one of its ~18 steps, which is what the stateless
     * per-id read does and what made the search 39x the hash probe when measured. It bypasses the
     * mint-keyed slice cache, whose slots it must not fill with positions. Allocation-free by the same
     * construction as {@link #compareIds}.
     * </p>
     *
     * @return negative, zero or positive as the stored value orders before, with, or after the range
     */
    int comparePositionToValue(final int position, final byte[] utf8, final int offset, final int length) {
      ensureRevision();
      if (position < 1 || position > entryCount) {
        throw new IllegalStateException("global value dictionary position " + position + " is outside revision "
            + revision + " cardinality " + entryCount);
      }
      locate(position);
      final ValueDictionaryEntryNode spill = locatedSpill;
      return spill == null
          ? ValueDictionaryEntryNode.compareUtf16Range(locatedBacking, locatedOffset, locatedLength, utf8, offset,
              length)
          : spill.compareToRange(utf8, offset, length);
    }

    /**
     * Resolve a value to its id through THIS view — the forward direction, a binary search over the
     * ordered prefix and, for an unordered tail, the forward hash index.
     *
     * <p>
     * The instance form of {@link GlobalValueDictionary#probe(long, byte[], StorageEngineReader)}: a
     * caller that interns many values (the segment lane probing a sealed generation for a tail, a
     * point predicate resolved once per segment) holds one view and pays the bucket, block, separator
     * and rank-table record fetches ONCE across all of them, where the static form pays them per call.
     * Same answers, same contract: {@link #ID_ABSENT} only when the directory is complete and provably
     * does not hold the value; a decode-only dictionary answers {@link #ID_UNKNOWN}. A view is opened
     * only over a complete directory, so completeness needs no re-check here. Under a rank table the
     * id returned is the MINT the rows carry, translated from the found position by one inverse-run
     * record read.
     * </p>
     *
     * @param utf8 the value's UTF-8 bytes
     * @param offset start of the value in {@code utf8}
     * @param length byte length of the value
     * @return the id, {@link #ID_ABSENT}, or {@link #ID_UNKNOWN}
     */
    public int probe(final byte[] utf8, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, utf8.length);
      if (entryCount == 0) {
        // A COMPLETE directory with zero entries provably holds nothing — absence is an answer, not
        // ignorance.
        return ID_ABSENT;
      }
      // The ordered prefix is probed by BINARY SEARCH over the reverse index, which is sorted by value
      // because its values were stored in collation order. A dictionary with an unordered tail must
      // try BOTH: the value may be in either half, and answering ABSENT after searching only the
      // prefix would be a wrong answer, not a slow one.
      if (orderedPrefixCount > 0) {
        final int position = searchOrderedPrefix(this, orderedPrefixCount, utf8, offset, length);
        if (position != ID_ABSENT) {
          // The search answers in STORAGE order; the id rows carry is the mint stored there, which is
          // the position itself unless a rank table stands between the two.
          return mintAtPosition(position);
        }
        if (storageOrdered) {
          return ID_ABSENT;
        }
      }
      if (forwardRootKey == 0L) {
        // Two dictionaries omit the forward index. A FULLY ORDERED one answered above, from the binary
        // search over its sorted reverse index. A DECODE-ONLY one cannot answer at all: it kept no
        // structure that maps a value to an id, which is the whole reason it is cheap to version.
        // UNKNOWN, never ABSENT -- absence is a licence to mint a new id, and minting a second id for
        // a value this dictionary already holds is exactly the silent corruption ids exist to prevent.
        return ID_UNKNOWN;
      }
      final long wanted = valueHash(utf8, offset, length);
      final long secondary = secondaryValueHash(utf8, offset, length);
      final GlobalValueDictionaryRadix.ProbeResult result = GlobalValueDictionaryRadix.probe(forwardRootKey,
          reverseRootKey, entryCount, wanted, secondary, utf8, offset, length, namePage, databaseType, reader);
      return recordProbeResult(result.id(), result.units());
    }

    /**
     * The storage POSITION of {@code id}: the rank the table holds for a covered mint, the id itself
     * when there is no table or the id lies above the table's prefix (an appended tail is stored in
     * mint order behind the ordered prefix, so there id and position coincide).
     *
     * @throws IllegalStateException if {@code id} is outside {@code 1..entryCount}, or the table
     *         record is missing, mis-shaped, or holds a rank outside the prefix — a wrong position would
     *         read the wrong value silently, and an out-of-range id would index a table record that does
     *         not exist (id 0 lands on record index 262143 through the unsigned shift)
     */
    int positionOf(final int id) {
      if (rankTableKey == 0L) {
        return id;
      }
      if (id < 1 || id > entryCount) {
        throw new IllegalStateException(
            "id " + id + " is outside value dictionary " + headerNodeKey + " (" + entryCount + " entries)");
      }
      if (id > orderedPrefixCount) {
        return id;
      }
      final int rank =
          rankTableRecord((id - 1) >>> ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT).entryOf(id);
      if (rank < 1 || rank > orderedPrefixCount) {
        throw new IllegalStateException("rank table of value dictionary " + headerNodeKey + " maps id " + id
            + " to position " + rank + ", outside its ordered prefix of " + orderedPrefixCount);
      }
      return rank;
    }

    /**
     * The id stored at storage {@code position} — the inverse of {@link #positionOf}, for the probe:
     * the binary search finds a position and the caller wants the id rows carry. One read of the
     * inverse run's record, cached like the forward run's; no per-view inverse is ever built.
     *
     * @throws IllegalStateException if {@code position} is outside {@code 1..entryCount}, or the
     *         inverse record is missing, mis-shaped, or holds a mint outside the prefix
     */
    int mintAtPosition(final int position) {
      if (rankTableKey == 0L) {
        return position;
      }
      if (position < 1 || position > entryCount) {
        throw new IllegalStateException(
            "position " + position + " is outside value dictionary " + headerNodeKey + " (" + entryCount + " entries)");
      }
      if (position > orderedPrefixCount) {
        return position;
      }
      final int records = ValueDictionaryRankTableNode.recordCountFor(orderedPrefixCount);
      final int mint =
          rankTableRecord(records + ((position - 1) >>> ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT))
              .entryOf(position);
      if (mint < 1 || mint > orderedPrefixCount) {
        throw new IllegalStateException("inverse rank table of value dictionary " + headerNodeKey + " maps position "
            + position + " to id " + mint + ", outside its ordered prefix of " + orderedPrefixCount);
      }
      return mint;
    }

    /**
     * Record {@code index} of the rank table — the forward run at {@code 0..records-1}, the inverse run
     * at {@code records..2*records-1} — fetched once per view and validated against its expected shape.
     */
    private ValueDictionaryRankTableNode rankTableRecord(final int index) {
      ValueDictionaryRankTableNode[] table = rankTable;
      if (table == null) {
        table = new ValueDictionaryRankTableNode[2 * ValueDictionaryRankTableNode.recordCountFor(orderedPrefixCount)];
        rankTable = table;
      }
      ValueDictionaryRankTableNode record = table[index];
      if (record == null) {
        record = loadRankTableRecord(headerNodeKey, rankTableKey, orderedPrefixCount, index, namePage, databaseType,
            reader);
        table[index] = record;
      }
      return record;
    }

    /**
     * The storage POSITION range that can hold {@code utf8}, narrowed by the separator array when there
     * is one. The separators were cut between positions, so the range is in position space whether or
     * not a rank table stands between positions and ids.
     *
     * <p>
     * Returns {@code (low << 32) | high} packed, because this is on the probe path and a record here
     * would allocate per probe. Without a separator array the range is the whole ordered prefix, which
     * is correct and merely slower — the array is an accelerator, never a source of truth.
     * </p>
     */
    long candidatePositionRange(final byte[] utf8, final int offset, final int length, final int boundary) {
      if (!blockIndexLoaded) {
        blockIndexLoaded = true;
        if (blockIndexKey != 0L) {
          final DataRecord record = namePage.getProjectionValueDictionaryRecord(blockIndexKey, databaseType, reader);
          if (record instanceof ValueDictionaryBlockIndexNode index) {
            blockIndex = index;
          }
        }
      }
      final ValueDictionaryBlockIndexNode index = blockIndex;
      if (index == null) {
        return ((long) 1 << 32) | (boundary & 0xFFFFFFFFL);
      }
      final int block = index.blockOf(utf8, offset, length);
      final int low = index.firstId(block);
      final int high = block + 1 < index.size()
          ? Math.min(index.firstId(block + 1) - 1, boundary)
          : boundary;
      return ((long) low << 32) | (high & 0xFFFFFFFFL);
    }

    /** Allocation-free {@code xs:integer(substring(value, start, length))}. */
    public long xsIntegerOfSubstring(final int id, final int start, final int length) {
      if (perSegment != null) {
        // Same width problem as valueAsString: a cell's segment does not survive an int. No caller
        // needs a substring of a segment-scoped column yet, and a truncating one would read segment
        // 0's dictionary for every cell.
        throw new IllegalStateException("this view resolves packed (segment, id) cells; xsIntegerOfSubstring has no cell form");
      }
      return transformed(id, start, length, (byte) 1);
    }

    /** Allocation-free order-preserving pack of a 16-byte ISO-minute substring. */
    public long packIsoMinuteSubstring(final int id, final int start, final int length) {
      if (perSegment != null) {
        // Same width problem as valueAsString: a cell's segment does not survive an int. No caller
        // needs a substring of a segment-scoped column yet, and a truncating one would read segment
        // 0's dictionary for every cell.
        throw new IllegalStateException("this view resolves packed (segment, id) cells; packIsoMinuteSubstring has no cell form");
      }
      return transformed(id, start, length, (byte) 2);
    }

    /** Materialise a validated ISO-minute substring for one emitted winner. */
    public String materializeIsoMinuteSubstring(final int id, final int start, final int length) {
      if (perSegment != null) {
        // Same width problem as valueAsString: a cell's segment does not survive an int. No caller
        // needs a substring of a segment-scoped column yet, and a truncating one would read segment
        // 0's dictionary for every cell.
        throw new IllegalStateException("this view resolves packed (segment, id) cells; materializeIsoMinuteSubstring has no cell form");
      }
      // The ONE place a value becomes a String: an emitted winner. Validated on exactly the terms
      // packIsoMinuteSubstring uses, so an inadmissible substring is refused here as it is there.
      final int slot = sliceSlot(id);
      final ValueDictionaryEntryNode spill = cachedSpills[slot];
      if (spill != null) {
        return spill.materializeAsciiSubstring(start, length);
      }
      final byte[] backing = cachedBacking[slot];
      final int offset = cachedOffsets[slot];
      final int valueLength = cachedLengths[slot];
      if (ProjectionIndexByteScan.packIsoMinuteSubstring(backing, offset, valueLength, start,
          length) == Long.MIN_VALUE) {
        throw new IllegalArgumentException("dictionary value is not an admissible ISO-minute substring");
      }
      return new String(backing, offset + start - 1, length, StandardCharsets.US_ASCII);
    }

    /**
     * Resolve {@code id} to a cache slot holding its slice, returning the slot index.
     *
     * <p>
     * A packed id yields the sub-block's own backing array with an offset and length — nothing is
     * copied and no record wrapper is built. A spilled id yields its entry record's bytes, which the
     * record already owns. Either way the cached triple is a VIEW, never a copy. The slot is keyed by
     * the id the caller passed; under a rank table the miss translates it to its storage position
     * first, so the cache never holds a position under a mint's key or the reverse.
     */
    private int sliceSlot(final int id) {
      ensureRevision();
      if (id < 1 || id > entryCount) {
        throw new IllegalStateException(
            "global value dictionary id " + id + " is outside revision " + revision + " cardinality " + entryCount);
      }
      final int slot = id & (READ_VIEW_CACHE_SIZE - 1);
      if (cachedIds[slot] == id && (cachedBacking[slot] != null || cachedSpills[slot] != null)) {
        return slot;
      }
      locate(positionOf(id));
      cachedBacking[slot] = locatedBacking;
      cachedOffsets[slot] = locatedOffset;
      cachedLengths[slot] = locatedLength;
      cachedSpills[slot] = locatedSpill;
      cachedIds[slot] = id;
      return slot;
    }

    /**
     * Resolve storage {@code position} to its slice, into the {@code located*} scratch: the reverse
     * bucket that covers it (retained direct-mapped), the block within the bucket (likewise), and the
     * value's offset and length inside the block's packed bytes — or its spill record. This is the
     * one path from a position to bytes; {@link #sliceSlot} caches its result per id and
     * {@link #comparePositionToValue} reads it in place.
     */
    private void locate(final int position) {
      final int bucket = (position - 1) >>> 8;
      final int bucketSlot = bucket & (READ_VIEW_BUCKET_SLOTS - 1);
      // Allocated on first MISS, not in the constructor. A view is built per worker and there are
      // many readView call sites per execution, so eager tables were 1.5-3 MB of garbage per
      // execution when the budget sized them large -- paid even by the queries that never resolve a
      // slice. One predictable branch on a path that fetches a record anyway costs nothing.
      if (cachedBuckets == null) {
        cachedBuckets = new int[READ_VIEW_BUCKET_SLOTS];
        Arrays.fill(cachedBuckets, -1);
        cachedBucketNodes = new ValueDictionaryValueBucketNode[READ_VIEW_BUCKET_SLOTS];
      }
      ValueDictionaryValueBucketNode bucketNode = cachedBuckets[bucketSlot] == bucket
          ? cachedBucketNodes[bucketSlot]
          : null;
      if (bucketNode == null) {
        bucketNode = GlobalValueDictionaryRadix.valueBucketOf(reverseRootKey, bucket, namePage, databaseType, reader);
        if (bucketNode == null) {
          throw new IllegalStateException(
              "global value dictionary position " + position + " is missing from revision " + revision);
        }
        cachedBucketNodes[bucketSlot] = bucketNode;
        cachedBuckets[bucketSlot] = bucket;
      }
      final long blockKey = bucketNode.blockKeyCovering(position);
      if (blockKey != 0L) {
        final int blockSlot = (int) (blockKey ^ blockKey >>> 32) & (READ_VIEW_BLOCK_SLOTS - 1);
        if (cachedBlockKeys == null) {
          cachedBlockKeys = new long[READ_VIEW_BLOCK_SLOTS];
          cachedBlocks = new ValueDictionaryValueBlockNode[READ_VIEW_BLOCK_SLOTS];
        }
        ValueDictionaryValueBlockNode block = cachedBlockKeys[blockSlot] == blockKey
            ? cachedBlocks[blockSlot]
            : null;
        if (block == null) {
          block = GlobalValueDictionaryRadix.blockNode(blockKey, position, namePage, databaseType, reader);
          cachedBlocks[blockSlot] = block;
          cachedBlockKeys[blockSlot] = blockKey;
        }
        locatedBacking = block.rawBytes();
        locatedOffset = block.valueOffset(position);
        locatedLength = block.valueLength(position);
        locatedSpill = null;
      } else {
        final long spillKey = bucketNode.spillKeyCovering(position);
        if (spillKey == 0L) {
          throw new IllegalStateException(
              "global value dictionary position " + position + " is missing from revision " + revision);
        }
        locatedSpill = GlobalValueDictionaryRadix.spillEntry(spillKey, namePage, databaseType, reader);
        locatedBacking = null;
      }
    }

    private long transformed(final int id, final int start, final int length, final byte mode) {
      ensureRevision();
      if (transformedIds == null) {
        transformedIds = new int[READ_VIEW_CACHE_SIZE];
        transformedStarts = new int[READ_VIEW_CACHE_SIZE];
        transformedLengths = new int[READ_VIEW_CACHE_SIZE];
        transformedModes = new byte[READ_VIEW_CACHE_SIZE];
        transformedValues = new long[READ_VIEW_CACHE_SIZE];
      }
      final int slot = (id * 31 + start * 17 + length * 7 + mode) & (READ_VIEW_CACHE_SIZE - 1);
      if (transformedIds[slot] == id && transformedStarts[slot] == start && transformedLengths[slot] == length
          && transformedModes[slot] == mode) {
        return transformedValues[slot];
      }
      final int valueSlot = sliceSlot(id);
      final ValueDictionaryEntryNode spill = cachedSpills[valueSlot];
      // Packed values use the SAME range functions the column kernels use, so validation — including
      // start < 1 and a negative length — is identical on both paths by construction rather than by
      // agreement. A spilled value transforms through its own record for the same reason it compares
      // through it: a record owns its bytes and does not hand them out.
      final long transformed;
      if (spill != null) {
        transformed = mode == 1
            ? spill.xsIntegerOfSubstring(start, length)
            : spill.packIsoMinuteSubstring(start, length);
      } else {
        final byte[] backing = cachedBacking[valueSlot];
        final int offset = cachedOffsets[valueSlot];
        final int valueLength = cachedLengths[valueSlot];
        transformed = mode == 1
            ? ProjectionIndexByteScan.xsIntegerOfSubstring(backing, offset, valueLength, start, length)
            : ProjectionIndexByteScan.packIsoMinuteSubstring(backing, offset, valueLength, start, length);
      }
      transformedIds[slot] = id;
      transformedStarts[slot] = start;
      transformedLengths[slot] = length;
      transformedModes[slot] = mode;
      transformedValues[slot] = transformed;
      return transformed;
    }

    private void ensureRevision() {
      final int actualRevision = reader.getRevisionNumber();
      if (actualRevision != revision) {
        throw new IllegalStateException("global value dictionary read view for revision " + revision
            + " cannot serve reader revision " + actualRevision);
      }
    }
  }

  /**
   * {@link SegmentRunCursor} over one dictionary's storage positions, on top of a {@link ReadView}.
   *
   * <p>
   * Holds the reverse bucket, the value block and the inverse rank-table record covering the
   * position it is at, each replaced only when a seek leaves it. Nothing here is keyed by mint and
   * nothing is direct-mapped: a walk in position order touches each block once, so a cache would
   * only be a cache of the block it holds anyway. Rank-table records are the VIEW's, shared with its
   * other routes; block and bucket records come through the shared dictionary record cache.
   * </p>
   */
  private static final class PositionCursor extends SegmentRunCursor {

    private final ReadView view;

    private final int entries;

    /** Index of the reverse bucket held, {@code -1} before the first seek. */
    private int bucketIndex = -1;

    private @Nullable ValueDictionaryValueBucketNode bucket;

    /** The block held, covering positions {@code [blockFirst, blockEnd)}; empty until the first seek. */
    private @Nullable ValueDictionaryValueBlockNode block;

    private int blockFirst;

    private int blockEnd;

    /** The inverse rank-table record held, covering positions {@code [inverseFirst, inverseEnd)}. */
    private @Nullable ValueDictionaryRankTableNode inverse;

    private int inverseFirst;

    private int inverseEnd;

    private PositionCursor(final ReadView view) {
      this.view = view;
      this.entries = view.entryCount;
    }

    @Override
    public void seek(final int position) {
      if (position >= blockFirst && position < blockEnd) {
        final ValueDictionaryValueBlockNode held = block;
        backing = held.rawBytes();
        offset = held.valueOffset(position);
        length = held.valueLength(position);
        spill = null;
        return;
      }
      if (position < 1 || position > entries) {
        throw new IllegalStateException("global value dictionary position " + position + " is outside revision "
            + view.revision + " cardinality " + entries);
      }
      final int bucketOf = (position - 1) >>> 8;
      ValueDictionaryValueBucketNode bucketNode = bucket;
      if (bucketNode == null || bucketOf != bucketIndex) {
        bucketNode = GlobalValueDictionaryRadix.valueBucketOf(view.reverseRootKey, bucketOf, view.namePage,
            view.databaseType, view.reader);
        if (bucketNode == null) {
          throw new IllegalStateException(
              "global value dictionary position " + position + " is missing from revision " + view.revision);
        }
        bucket = bucketNode;
        bucketIndex = bucketOf;
        loads++;
      }
      final long blockKey = bucketNode.blockKeyCovering(position);
      if (blockKey != 0L) {
        final ValueDictionaryValueBlockNode loaded =
            GlobalValueDictionaryRadix.blockNode(blockKey, position, view.namePage, view.databaseType, view.reader);
        loads++;
        block = loaded;
        blockFirst = loaded.getFirstId();
        blockEnd = blockFirst + loaded.size();
        backing = loaded.rawBytes();
        offset = loaded.valueOffset(position);
        length = loaded.valueLength(position);
        spill = null;
        return;
      }
      final long spillKey = bucketNode.spillKeyCovering(position);
      if (spillKey == 0L) {
        throw new IllegalStateException(
            "global value dictionary position " + position + " is missing from revision " + view.revision);
      }
      spill = GlobalValueDictionaryRadix.spillEntry(spillKey, view.namePage, view.databaseType, view.reader);
      loads++;
      backing = null;
      offset = 0;
      length = 0;
    }

    @Override
    public int mintAt(final int position) {
      if (position < 1 || position > entries) {
        return -1;
      }
      if (view.rankTableKey == 0L || position > view.orderedPrefixCount) {
        return position; // ids ARE positions: no table, or the unordered tail behind the prefix
      }
      ValueDictionaryRankTableNode record = inverse;
      if (record == null || position < inverseFirst || position >= inverseEnd) {
        final int records = ValueDictionaryRankTableNode.recordCountFor(view.orderedPrefixCount);
        record = view.rankTableRecord(records
            + ((position - 1) >>> ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT));
        inverse = record;
        inverseFirst = record.firstKey();
        inverseEnd = inverseFirst + record.size();
        loads++;
      }
      return record.entryOf(position);
    }
  }

  /** Blocks the warmer has decoded into the record cache; the engagement witness. */
  private static final AtomicLong WARMED_BLOCKS = new AtomicLong();

  /**
   * Blocks STILL RESIDENT when their warm pass finished — the number that says what warming bought.
   *
   * <p>
   * Separate from {@link #WARMED_BLOCKS} because they answer different questions and only the second
   * can see eviction: a pass that warms 96,000 blocks and evicts 60,000 of them reports an identical
   * warmed count to one that keeps every block. At a cardinality that fits they are equal; at one
   * that does not, the gap between them IS the finding.
   * </p>
   */
  private static final AtomicLong RESIDENT_BLOCKS = new AtomicLong();

  /**
   * @return blocks warmed into the record cache since JVM start; {@code 0} means the warmer never
   *         ran.
   */
  public static long warmedBlockCount() {
    return WARMED_BLOCKS.get();
  }

  /**
   * @return warmed blocks still resident when their pass ended; below {@link #warmedBlockCount()}
   *         means churn.
   */
  public static long residentBlockCount() {
    return RESIDENT_BLOCKS.get();
  }

  /**
   * Decodes a dictionary's value blocks into the buffer manager's record cache, ahead of the query
   * that would otherwise pay for them.
   *
   * <p>
   * <b>Why this exists.</b> A first verdict build over a 275,494-entry dictionary measured 142 ms, of
   * which 123 ms was first touch — 84 ms fetching and deserializing 1,085 block records and 39 ms
   * decoding and front-expanding them — against 19 ms of steady-state work once they are resident.
   * Every later execution pays the 19 ms. This moves the 123 ms off the query that happens to be
   * first. A prefetch of the pages alone would move only the 84 ms; a warmer has to fetch in order to
   * decode, so it moves both.
   * </p>
   *
   * <p>
   * <b>It caches values, never accessors.</b> Nothing here is retained: the walk touches records
   * through {@code NamePage}, which populates the record cache with decoded, immutable block records,
   * and the reader this runs on belongs to the caller. No {@link ReadView} is held, so no transaction
   * is pinned past its own lifetime.
   * </p>
   *
   * <p>
   * <b>Partial warmth is partial benefit, never wrongness.</b> The walk stops when it has warmed
   * {@code budgetBytes}, so a dictionary larger than the record cache warms its low ids and leaves
   * the rest; a query reaching an unwarmed block decodes it through the path that already exists.
   * Racing is safe for the same reason — a query arriving mid-warm finds some blocks resident and
   * fetches the others. A failure is swallowed for the same reason: warming is an optimisation, and a
   * resource that closed underneath a background walk must not turn into a query error.
   * </p>
   *
   * @param headerNodeKey the dictionary's header key
   * @param reader a reader the CALLER owns and outlives this call
   * @param budgetBytes decoded bytes to stop after
   * @return blocks warmed, or {@code 0} if the dictionary was unreadable
   */
  public static long warmDictionaryBlocks(final long headerNodeKey, final StorageEngineReader reader,
      final long budgetBytes) {
    Objects.requireNonNull(reader, "reader must not be null");
    // ONCE per (database, resource, revision, dictionary), claimed through the BUFFER MANAGER. The
    // caller cannot dedupe this itself: the engine builds an executor per EXECUTION, so an
    // executor-scoped guard let the walk repeat once per query — 43 times over a 43-query leg, each
    // repeat also opening and closing a read-only transaction, which showed up as a stable cold
    // regression on the earliest query. The marker belongs beside the caches it describes rather
    // than in a static, so a resource deleted and recreated with the same ids has its marker swept
    // with its data; a surviving marker would report "already warm" over an empty cache and disable
    // the warmer for the life of the process.
    final GlobalDictionaryRecordCacheKey warmKey = new GlobalDictionaryRecordCacheKey(reader.getDatabaseId(),
        reader.getResourceId(), reader.getRevisionNumber(), headerNodeKey);
    final Cache<GlobalDictionaryRecordCacheKey, Boolean> markers =
        reader.getBufferManager().getGlobalDictionaryWarmMarkers();
    if (markers.get(warmKey) != null) {
      return 0L;
    }
    // Claimed on the interface rather than through a concrete putIfAbsent, so a no-op buffer manager
    // stays a no-op. The window between the check and the put lets two callers walk the same
    // dictionary at once, which is idempotent — both decode the same immutable blocks into the same
    // keys — and costs one redundant walk in a race that only the first query per resource can hit.
    markers.put(warmKey, Boolean.TRUE);
    final ReadView view = readView(headerNodeKey, reader);
    if (view == null || view.entryCount() <= 0) {
      return 0L;
    }
    long warmed = 0L;
    long bytes = 0L;
    final LongArrayList warmedKeys = new LongArrayList();
    final int bucketCount = (view.entryCount() - 1 >>> 8) + 1;
    try {
      for (int bucket = 0; bucket < bucketCount && bytes < budgetBytes; bucket++) {
        final ValueDictionaryValueBucketNode bucketNode = GlobalValueDictionaryRadix.valueBucketOf(view.reverseRootKey,
            bucket, view.namePage, view.databaseType, reader);
        if (bucketNode == null) {
          break;
        }
        final int blocks = bucketNode.blockCount();
        for (int block = 0; block < blocks && bytes < budgetBytes; block++) {
          final ValueDictionaryValueBlockNode node = GlobalValueDictionaryRadix.blockNode(bucketNode.blockKey(block),
              bucketNode.blockFirstId(block), view.namePage, view.databaseType, reader);
          if (node == null) {
            continue;
          }
          bytes += node.rawBytes().length;
          warmedKeys.add(bucketNode.blockKey(block));
          warmed++;
        }
      }
    } catch (final RuntimeException swallowed) {
      // Best effort by contract: whatever was warmed stays warm and the query path is unaffected.
      // A resource closing under a background walk must never surface as a query error.
    }
    WARMED_BLOCKS.addAndGet(warmed);
    // What SURVIVED the pass. Re-reading each key is a cache lookup, so this costs a walk of the
    // keys and no I/O; the gap against `warmed` is the eviction the warmed count cannot see.
    final Cache<GlobalDictionaryRecordCacheKey, DataRecord> records =
        reader.getBufferManager().getGlobalDictionaryRecordCache();
    long resident = 0L;
    for (int i = 0; i < warmedKeys.size(); i++) {
      if (records.get(new GlobalDictionaryRecordCacheKey(reader.getDatabaseId(), reader.getResourceId(),
          reader.getRevisionNumber(), warmedKeys.getLong(i))) != null) {
        resident++;
      }
    }
    RESIDENT_BLOCKS.addAndGet(resident);
    return warmed;
  }

  public static long maximumKeysToReserve(final int entryCount) {
    if (entryCount < 0)
      throw new IllegalArgumentException("entryCount must not be negative");
    final long reverseBuckets = (entryCount + 255L) >>> 8;
    final long maximumRecords = 13L * entryCount + 4L * reverseBuckets;
    return 1L + Math.multiplyExact(maximumRecords, PERSISTENT_RECORD_STRIDE);
  }

  /**
   * The hash a value is indexed under in the forward directory.
   *
   * @param utf8 the value's UTF-8 bytes
   * @param off offset into {@code utf8}
   * @param len length in {@code utf8}
   * @return the value hash
   */
  public static long valueHash(final byte[] utf8, final int off, final int len) {
    return ProjectionIndexByteScan.fnv1a64(utf8, off, len);
  }

  static long secondaryValueHash(final byte[] utf8, final int off, final int len) {
    // Same xx3 function, Unsafe-free access — identical hash values, minus the per-read
    // beforeMemoryAccess() deprecation check JDK 25 charges the library's default access.
    return SECONDARY_HASH.hash(utf8, HashAccesses.BYTES, off, len);
  }

  /**
   * The database type of a reader's resource. Mirrors the single derivation point on the reader
   * ({@code NodeStorageEngineReader#databaseType}), which is package-private to the page-access
   * layer; the two must agree, or records get written under one offset and looked up under another.
   *
   * @param reader the reader whose resource is wanted
   * @return the database type
   */
  public static DatabaseType databaseTypeOf(final StorageEngineReader reader) {
    return reader.getResourceSession() instanceof JsonResourceSession
        ? DatabaseType.JSON
        : DatabaseType.XML;
  }

  /**
   * Read a dictionary's header.
   *
   * @param headerNodeKey the header's node key, as recorded in the projection's metadata
   * @param reader the reader positioned at the revision wanted
   * @return the header, or {@code null} when this revision holds no readable dictionary there
   * @throws IllegalStateException if the record at that key is not a header
   */
  public static @Nullable ValueDictionaryHeaderNode header(final long headerNodeKey, final StorageEngineReader reader) {
    if (headerNodeKey <= 0) {
      return null;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    if (!namePage.hasProjectionValueDictionary(databaseType)) {
      return null;
    }
    final DataRecord record = namePage.getProjectionValueDictionaryRecord(headerNodeKey, databaseType, reader);
    if (record == null) {
      return null;
    }
    if (!(record instanceof ValueDictionaryHeaderNode header)) {
      throw new IllegalStateException(
          "record at value dictionary header key " + headerNodeKey + " is a " + record.getKind() + ", not a header");
    }
    // An unknown layout is "no dictionary I can read", not a failure: a resource written by a newer
    // build must make an older one decline, never misparse.
    return header.getVersion() == ValueDictionaryHeaderNode.VERSION
        ? header
        : null;
  }

  /**
   * Materialise the value behind an id — the reverse direction, one record read.
   *
   * @param headerNodeKey the dictionary's header key
   * @param id the value id
   * @param reader the reader positioned at the revision wanted
   * @return the value's UTF-8 bytes, or {@code null} when the id is not stored in this revision
   */
  public static byte @Nullable [] valueBytes(final long headerNodeKey, final int id, final StorageEngineReader reader) {
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null) {
      return null;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    return valueBytes(header, id, namePage, databaseType, reader);
  }

  /** As above, with the header and page lookups already done. */
  private static byte @Nullable [] valueBytes(final ValueDictionaryHeaderNode header, final int id,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (id < 1 || id > header.getEntryCount()) {
      return null;
    }
    return GlobalValueDictionaryRadix.value(header.getReverseRootKey(),
        storagePosition(header, id, namePage, databaseType, reader), namePage, databaseType, reader);
  }

  /**
   * The storage POSITION of a live {@code id} ({@code 1..entryCount}, the caller's precondition). The
   * reverse radix is addressed by position; behind a rank table that is not the id, so the forward
   * run's record covering the id is read first -- one record fetch, served from the record cache
   * after the first -- and its rank is what the radix is asked for.
   */
  private static int storagePosition(final ValueDictionaryHeaderNode header, final int id, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    final int orderedPrefixCount = header.getOrderedPrefixCount();
    if (!header.hasRankTable() || id > orderedPrefixCount) {
      return id;
    }
    final int position = loadRankTableRecord(header.getNodeKey(), header.getRankTableKey(), orderedPrefixCount,
        (id - 1) >>> ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT, namePage, databaseType, reader)
        .entryOf(id);
    if (position < 1 || position > orderedPrefixCount) {
      throw new IllegalStateException("rank table of value dictionary " + header.getNodeKey() + " maps id " + id
          + " to position " + position + ", outside its ordered prefix of " + orderedPrefixCount);
    }
    return position;
  }

  /**
   * Fetch record {@code index} of a dictionary's rank table and refuse anything but the record the
   * seal wrote there: the table is TWO runs of {@code recordCountFor(P)} records at arithmetic keys
   * {@code rankTableKey + index} — the forward run ({@code mint -> rank}) first, the inverse run
   * ({@code rank -> mint}) behind it — record {@code i} of a run covering keys {@code 1 + i * 16384}
   * onwards, its size following from the ordered prefix. A record of another shape at that key is
   * corruption, and translating through it would read wrong values without a trace.
   */
  private static ValueDictionaryRankTableNode loadRankTableRecord(final long headerNodeKey, final long rankTableKey,
      final int orderedPrefixCount, final int index, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final int records = ValueDictionaryRankTableNode.recordCountFor(orderedPrefixCount);
    if (index < 0 || index >= 2 * records) {
      throw new IllegalStateException("rank table of value dictionary " + headerNodeKey + " has 2 x " + records
          + " records, not a record " + index);
    }
    final int withinRun = index < records
        ? index
        : index - records;
    final long key = rankTableKey + index;
    final DataRecord record = namePage.getProjectionValueDictionaryRecord(key, databaseType, reader);
    if (!(record instanceof ValueDictionaryRankTableNode table)) {
      throw new IllegalStateException("rank table record " + index + " of value dictionary " + headerNodeKey
          + " at key " + key + " is " + (record == null
              ? "missing"
              : "a " + record.getKind()));
    }
    final int expectedFirstKey = 1 + (withinRun << ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT);
    final int expectedCount = Math.min(ValueDictionaryRankTableNode.ENTRIES_PER_RECORD,
        orderedPrefixCount - (withinRun << ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT));
    if (table.firstKey() != expectedFirstKey || table.size() != expectedCount
        || table.bitsPerEntry() != ValueDictionaryRankTableNode.bitsFor(orderedPrefixCount)) {
      throw new IllegalStateException("rank table record " + index + " of value dictionary " + headerNodeKey
          + " at key " + key + " covers keys " + table.firstKey() + "+" + table.size() + " at "
          + table.bitsPerEntry() + " bits; expected " + expectedFirstKey + "+" + expectedCount + " at "
          + ValueDictionaryRankTableNode.bitsFor(orderedPrefixCount) + " bits for an ordered prefix of "
          + orderedPrefixCount);
    }
    return table;
  }

  /**
   * Materialise the value behind an id as a string.
   *
   * @param headerNodeKey the dictionary's header key
   * @param id the value id
   * @param reader the reader positioned at the revision wanted
   * @return the value, or {@code null} when the id is not stored in this revision
   */
  public static @Nullable String value(final long headerNodeKey, final int id, final StorageEngineReader reader) {
    final byte[] bytes = valueBytes(headerNodeKey, id, reader);
    return bytes == null
        ? null
        : new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Materialise several ids at once, resolving the header and page lookups a single time.
   *
   * <p>
   * The winner-materialisation path: a top-k group-by hands over the k ids it is about to return and
   * gets their strings back. Values are visited in ascending STORAGE POSITION so that values sharing
   * a record page are resolved consecutively, which is what turns k random reads into far fewer page
   * touches; the caller's order is restored through the index carried alongside. Without a rank table
   * the position is the id; behind one the ids are mints in arrival order, so each is translated
   * first (k forward-run record reads, cached) and the walk is ordered by where the values actually
   * are, not by the number the rows carry.
   *
   * @param headerNodeKey the dictionary's header key
   * @param ids the ids to resolve; not modified
   * @param reader the reader positioned at the revision wanted
   * @return the values, index-aligned to {@code ids}; an entry is {@code null} when its id is not
   *         stored in this revision (0, negative, or above the entry count), or every entry when the
   *         header is unreadable
   */
  public static @Nullable String[] values(final long headerNodeKey, final int[] ids, final StorageEngineReader reader) {
    Objects.requireNonNull(ids, "ids must not be null");
    final String[] out = new String[ids.length];
    if (ids.length == 0) {
      return out;
    }
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null) {
      return out;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    final int entryCount = header.getEntryCount();
    final int[] positions = new int[ids.length];
    final int[] order = new int[ids.length];
    for (int i = 0; i < ids.length; i++) {
      order[i] = i;
      final int id = ids[i];
      // A dead id keeps itself as the sort key: it resolves to null whatever its place in the walk.
      positions[i] = id < 1 || id > entryCount
          ? id
          : storagePosition(header, id, namePage, databaseType, reader);
    }
    sortIndicesByValue(order, positions);
    for (final int slot : order) {
      final int id = ids[slot];
      if (id < 1 || id > entryCount) {
        continue;
      }
      final byte[] bytes =
          GlobalValueDictionaryRadix.value(header.getReverseRootKey(), positions[slot], namePage, databaseType, reader);
      if (bytes != null) {
        out[slot] = new String(bytes, StandardCharsets.UTF_8);
      }
    }
    return out;
  }

  /**
   * Insertion-sort {@code order} (a permutation of {@code 0..n-1}) by {@code values[order[i]]}. The
   * arrays are k elements long, k being a query's LIMIT — insertion sort beats anything with an
   * allocation at that size.
   */
  private static void sortIndicesByValue(final int[] order, final int[] values) {
    for (int i = 1; i < order.length; i++) {
      final int slot = order[i];
      final int key = values[slot];
      int j = i - 1;
      while (j >= 0 && values[order[j]] > key) {
        order[j + 1] = order[j];
        j--;
      }
      order[j + 1] = slot;
    }
  }

  /**
   * Resolve a value to its id — the forward direction, a binary search over the directory.
   *
   * <p>
   * Answers {@link #ID_ABSENT} only when the directory is complete and provably does not hold the
   * value; a directory that was never written, one that does not cover every id, and an unreadable
   * header all answer {@link #ID_UNKNOWN}, because "I cannot see it" and "it is not there" lead to
   * opposite query results and must never be confused. A hash match is confirmed by reading the
   * candidate's value entry and comparing bytes, so a hash collision costs an extra read rather than
   * a wrong id.
   *
   * @param headerNodeKey the dictionary's header key
   * @param utf8 the value's UTF-8 bytes
   * @param reader the reader positioned at the revision wanted
   * @return the id, {@link #ID_ABSENT}, or {@link #ID_UNKNOWN}
   */
  public static int probe(final long headerNodeKey, final byte[] utf8, final StorageEngineReader reader) {
    return probe(headerNodeKey, utf8, 0, utf8.length, reader);
  }

  static int probe(final long headerNodeKey, final byte[] utf8, final int offset, final int length,
      final StorageEngineReader reader) {
    Objects.checkFromIndexSize(offset, length, utf8.length);
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null || !header.isDirectoryComplete()) {
      return ID_UNKNOWN;
    }
    if (header.getEntryCount() == 0) {
      // A COMPLETE directory with zero entries provably holds nothing — absence is an answer, not
      // ignorance. Without this the empty dictionary fell past the prefix search (no boundary) to
      // the forward-index check and answered UNKNOWN for want of an index it can never need.
      return ID_ABSENT;
    }
    if (header.getOrderedPrefixCount() > 0) {
      // ONE view for the whole search: its bucket, block, separator and rank-table caches are what
      // make the ~18 steps cost far less than 18 independent reads, and they are useless if a view is
      // built per step. The view is still built PER CALL here, which is the one-shot price of this
      // form; a caller that interns many values holds a view and calls ReadView#probe across them.
      final ReadView view = readView(headerNodeKey, reader);
      if (view == null) {
        return ID_UNKNOWN;
      }
      return view.probe(utf8, offset, length);
    }
    if (header.getForwardRootKey() == 0) {
      // No ordered prefix and no forward index: a DECODE-ONLY dictionary, which kept no structure that
      // maps a value to an id -- the whole reason it is cheap to version. UNKNOWN, never ABSENT:
      // absence is a licence to mint a new id, and minting a second id for a value this dictionary
      // already holds is exactly the silent corruption ids exist to prevent.
      return ID_UNKNOWN;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    final long wanted = valueHash(utf8, offset, length);
    final long secondary = secondaryValueHash(utf8, offset, length);
    final GlobalValueDictionaryRadix.ProbeResult result =
        GlobalValueDictionaryRadix.probe(header.getForwardRootKey(), header.getReverseRootKey(), header.getEntryCount(),
            wanted, secondary, utf8, offset, length, namePage, databaseType, reader);
    return recordProbeResult(result.id(), result.units());
  }

  /**
   * Builds the separator array over a fully ordered dictionary and returns its record key.
   *
   * <p>
   * Partitions on REVERSE BUCKET boundaries (256 ids), not on block boundaries. The two are nearly
   * the same partition, and the bucket one is total by construction — a bucket covers its ids whether
   * they are packed in blocks or spilled to their own records, so the search's within-range step
   * handles a spilled value with no special case.
   * </p>
   *
   * @return the record key of the separator array, or 0 when the dictionary is too small to index
   */
  public static long buildBlockIndex(final long headerNodeKey, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter writer, final TransactionIntentLog log) {
    final ValueDictionaryHeaderNode header = header(headerNodeKey, writer);
    // The separators are cut between storage POSITIONS, and this method reads them through the id
    // route. That is one and the same lookup only while ids ARE positions, so the seal builds the
    // index before it writes the rank table -- afterwards the id route would translate mints, and the
    // header rewrite below (which does not carry the table key) would drop the table. Refuse loudly,
    // and BEFORE the too-small-to-index return: calling this on a tabled dictionary is a programming
    // error whatever its size, and a silent 0 for a small one would hide it until the segment grew.
    if (header != null && header.hasRankTable()) {
      throw new IllegalStateException("the block index of value dictionary " + headerNodeKey
          + " must be built before its rank table (key " + header.getRankTableKey() + ") exists");
    }
    if (header == null || !header.isFullyOrdered() || header.getEntryCount() <= VALUES_PER_INDEXED_RANGE) {
      return 0L;
    }
    final int entryCount = header.getEntryCount();
    final int ranges = (entryCount + VALUES_PER_INDEXED_RANGE - 1) / VALUES_PER_INDEXED_RANGE;
    final int[] firstIds = new int[ranges];
    final int[] offsets = new int[ranges + 1];
    final byte[][] separators = new byte[ranges][];
    int totalSeparatorBytes = 0;
    for (int i = 0; i < ranges; i++) {
      final int firstId = i * VALUES_PER_INDEXED_RANGE + 1;
      firstIds[i] = firstId;
      if (i == 0) {
        separators[i] = EMPTY_SEPARATOR;
      } else {
        final byte[] previous = valueBytes(headerNodeKey, firstId - 1, writer);
        final byte[] next = valueBytes(headerNodeKey, firstId, writer);
        if (previous == null || next == null) {
          throw new IllegalStateException("the dictionary lost id " + firstId + " while its index was being built");
        }
        separators[i] = shortestSeparator(previous, next);
      }
      totalSeparatorBytes = Math.addExact(totalSeparatorBytes, separators[i].length);
      offsets[i + 1] = totalSeparatorBytes;
    }
    final byte[] packed = new byte[totalSeparatorBytes];
    for (int i = 0; i < ranges; i++) {
      System.arraycopy(separators[i], 0, packed, offsets[i], separators[i].length);
    }
    // From the reservation on, the intent log is being changed: a failure past this point leaves a
    // reserved key or an index record without the header that points at it, so the owning
    // transaction is poisoned exactly as the dictionary writer poisons its own flushes.
    try {
      final long indexKey = namePage.reserveProjectionValueDictionaryKeys(databaseType, 1L);
      namePage.putProjectionValueDictionaryRecord(
          ValueDictionaryBlockIndexNode.takeOwnership(indexKey, firstIds, packed, offsets), databaseType, writer, log);
      namePage.putProjectionValueDictionaryRecord(new ValueDictionaryHeaderNode(header.getNodeKey(),
          ValueDictionaryHeaderNode.VERSION, entryCount, header.getForwardRootKey(), header.getReverseRootKey(),
          header.getGeneration(), header.getOrderedPrefixCount(), indexKey), databaseType, writer, log);
      return indexKey;
    } catch (final RuntimeException | Error failure) {
      GlobalValueDictionaryWriter.poisonOwningTransaction(writer, failure);
      throw failure;
    }
  }

  /**
   * Attaches a rank table to a dictionary whose ordered prefix was written in collation order under
   * storage positions, and returns the table's first record key.
   *
   * <p>
   * From here on the ids the pages carry are MINTS: {@code rankByMint[m]} is the storage position of
   * mint {@code m}, and every read route translates through the table ({@link ReadView#positionOf}).
   * The seal writes the sorted values first (positions {@code 1..P}), builds the separator array
   * ({@link #buildBlockIndex}, which refuses to run after this), then calls here with the
   * permutation it recorded while sorting. Mints above {@code P} (an unordered tail) stay their own
   * positions. The permutation is persisted in BOTH directions as two runs of
   * {@link ValueDictionaryRankTableNode#recordCountFor} records at consecutive keys: {@code mint ->
   * rank} at {@code tableKey + i} (the direction every decode takes) and {@code rank -> mint} at
   * {@code tableKey + records + i} (the direction the probe takes, once per probe — persisting it
   * costs the table's bytes again, ≈ 2.5 B per distinct value, and saves an {@code int[P]} inverse
   * per probing view on the query path). Packed at {@link ValueDictionaryRankTableNode#bitsFor} bits
   * per entry, {@link ValueDictionaryRankTableNode#ENTRIES_PER_RECORD} entries per record.
   * </p>
   *
   * @param rankByMint the storage position of every mint {@code 1..P}, index 0 unused — a
   *        permutation of {@code 1..P}, checked here once so that no reader has to
   * @return the key of the first table record
   * @throws IllegalArgumentException if the header does not describe an untabled ordered prefix
   *         without a forward index, or {@code rankByMint} is not a permutation of {@code 1..P}
   */
  public static long attachRankTable(final long headerNodeKey, final int[] rankByMint, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineWriter writer, final TransactionIntentLog log) {
    Objects.requireNonNull(rankByMint, "rankByMint must not be null");
    final ValueDictionaryHeaderNode header = header(headerNodeKey, writer);
    if (header == null) {
      throw new IllegalArgumentException("no value dictionary header at key " + headerNodeKey);
    }
    final int prefix = header.getOrderedPrefixCount();
    if (prefix == 0 || header.getForwardRootKey() != 0L || header.hasRankTable()) {
      throw new IllegalArgumentException("value dictionary " + headerNodeKey
          + " cannot take a rank table: it needs an ordered prefix, no forward index and no table yet, but is "
          + header);
    }
    if (rankByMint.length != prefix + 1) {
      throw new IllegalArgumentException("rankByMint covers " + (rankByMint.length - 1) + " mints, the ordered prefix "
          + prefix);
    }
    // A repeated rank would give two mints one value and leave another value unreachable; refused
    // before a single record is written. The inverse is built in the same pass: a slot already
    // taken IS the repeated rank.
    final int[] mintByRank = new int[prefix + 1];
    for (int mint = 1; mint <= prefix; mint++) {
      final int rank = rankByMint[mint];
      if (rank < 1 || rank > prefix) {
        throw new IllegalArgumentException("mint " + mint + " has rank " + rank + " outside 1.." + prefix);
      }
      if (mintByRank[rank] != 0) {
        throw new IllegalArgumentException("rank " + rank + " is assigned to two mints (the second is " + mint + ")");
      }
      mintByRank[rank] = mint;
    }
    final int bitsPerEntry = ValueDictionaryRankTableNode.bitsFor(prefix);
    final int records = ValueDictionaryRankTableNode.recordCountFor(prefix);
    // Every validation above runs before the first reservation; from here on a failure leaves table
    // records without a header that points at them, so the owning transaction is poisoned (the
    // dictionary writer's own contract for its flushes) rather than left committable by a caller
    // that catches the exception.
    try {
      final long tableKey = namePage.reserveProjectionValueDictionaryKeys(databaseType, 2L * records);
      for (int i = 0; i < records; i++) {
        final int firstKey = 1 + (i << ValueDictionaryRankTableNode.ENTRIES_PER_RECORD_SHIFT);
        final int count = Math.min(ValueDictionaryRankTableNode.ENTRIES_PER_RECORD, prefix - (firstKey - 1));
        namePage.putProjectionValueDictionaryRecord(
            ValueDictionaryRankTableNode.pack(tableKey + i, firstKey, rankByMint, count, bitsPerEntry), databaseType,
            writer, log);
        namePage.putProjectionValueDictionaryRecord(
            ValueDictionaryRankTableNode.pack(tableKey + records + i, firstKey, mintByRank, count, bitsPerEntry),
            databaseType, writer, log);
      }
      // The forward root is carried over rather than written as the 0 the precondition guarantees: if
      // that precondition ever loosens, the header's own invariant (a table admits no forward index)
      // refuses the write instead of silently dropping the index the tail is probed through.
      namePage.putProjectionValueDictionaryRecord(new ValueDictionaryHeaderNode(header.getNodeKey(),
          ValueDictionaryHeaderNode.VERSION, header.getEntryCount(), header.getForwardRootKey(),
          header.getReverseRootKey(), header.getGeneration(), prefix, header.getBlockIndexKey(), tableKey),
          databaseType, writer, log);
      return tableKey;
    } catch (final RuntimeException | Error failure) {
      GlobalValueDictionaryWriter.poisonOwningTransaction(writer, failure);
      throw failure;
    }
  }

  /**
   * The shortest prefix of {@code next} that still orders after {@code previous}.
   *
   * <p>
   * Cut at a UTF-8 code-point boundary, because a prefix that splits a character is not a value the
   * collation can compare, and VERIFIED against the comparator before it is used — if the short form
   * does not separate, the whole value is stored. A separator array is an accelerator, so it may be
   * larger than necessary but must never be wrong.
   * </p>
   */
  static byte[] shortestSeparator(final byte[] previous, final byte[] next) {
    int common = 0;
    final int limit = Math.min(previous.length, next.length);
    while (common < limit && previous[common] == next[common]) {
      common++;
    }
    int cut = Math.min(common + 1, next.length);
    while (cut < next.length && (next[cut] & 0xC0) == 0x80) {
      cut++;
    }
    final byte[] candidate = Arrays.copyOf(next, cut);
    return ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, candidate, 0, candidate.length) < 0
        ? candidate
        : next.clone();
  }

  /**
   * Binary search for {@code utf8} over storage positions {@code 1..boundary}, which are in collation
   * order. The answer is a POSITION: it equals the id only while the dictionary keeps no rank table,
   * so the caller translates through {@link ReadView#mintAtPosition}.
   *
   * <p>
   * The comparator MUST be {@link ValueDictionaryEntryNode#compareUtf16Range} and not unsigned byte
   * order: the two differ for supplementary characters, which sort after U+E000..U+FFFF in UTF-8
   * bytes but before them in UTF-16. The rank pass sorts with a byte substitution that is provably
   * equivalent to this comparator, so searching with anything else would look up a value in an order
   * it was not stored in and answer ABSENT for a value that is present.
   * </p>
   *
   * @return the storage position, or {@link #ID_ABSENT} when the prefix provably does not hold the
   *         value
   */
  private static int searchOrderedPrefix(final ReadView view, final int boundary, final byte[] utf8, final int offset,
      final int length) {
    // The separator array narrows the search to ONE block before a single value is read; without it
    // the range is the whole prefix and every step decodes a different block.
    final long range = view.candidatePositionRange(utf8, offset, length, boundary);
    int low = (int) (range >>> 32);
    int high = (int) range;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int comparison = view.comparePositionToValue(mid, utf8, offset, length);
      if (comparison == 0) {
        return mid;
      }
      if (comparison < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return ID_ABSENT;
  }

  private static int recordProbeResult(final int result, final int probeUnits) {
    if (HFT_TELEMETRY_ENABLED) {
      int maximum = HFT_MAX_PROBE_UNITS.get();
      while (probeUnits > maximum && !HFT_MAX_PROBE_UNITS.compareAndSet(maximum, probeUnits)) {
        maximum = HFT_MAX_PROBE_UNITS.get();
      }
    }
    return result;
  }

  public static void resetProbeTelemetry() {
    HFT_MAX_PROBE_UNITS.set(0);
  }

  public static int maxProbeUnits() {
    return HFT_MAX_PROBE_UNITS.get();
  }

}

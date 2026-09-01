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
 * binds the header and name page to one revision and exposes allocation-free comparisons and the two
 * admitted substring transforms. It never exposes or copies an entry's byte array.
 */
public final class GlobalValueDictionary {

  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");
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
   * record decodes for one id. Retaining the bucket collapses that to one decode per id for any
   * scan with locality. Sixteen buckets span 4096 consecutive ids and cost sixteen references, so
   * this is bounded by the VIEW, never by the dictionary's cardinality.
   */
  private static final int READ_VIEW_BUCKET_CACHE_SIZE = 16;

  /**
   * FLOOR for both per-view tables: the smallest they may be, and what they are when the resident
   * budget is off. Named as a floor rather than a size because it is no longer either table's
   * actual length -- {@link #READ_VIEW_BLOCK_SLOTS} decides that from the budget.
   */
  private static final int READ_VIEW_TABLE_FLOOR = 16;

  /**
   * Byte budget for one view's resident decoded blocks; {@code 0} keeps the fixed 16-slot table.
   *
   * <p>
   * RESIDENCY BY FIT, never unconditional. A decoded block is up to
   * {@link ValueDictionaryValueBlockNode#MAX_BLOCK_BYTES}, so the budget divided by that bound gives
   * the number of slots the view may hold, and the table being DIRECT-MAPPED is what makes the
   * budget a real bound rather than a hope — a slot holds at most one block, so resident bytes can
   * never exceed slots times the bound, and a collision simply re-decodes through the path that
   * already exists. There is no eviction policy to get wrong because there is no eviction: the map
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
   * each, where the spread WITHIN each configuration is larger than the difference between them).
   * A per-view budget is also the wrong shape at scale: it is claimed once per view, so ten views
   * would claim it ten times for one dictionary, where the shared cache claims it once.
   *
   * <p>
   * The knob stays because the arithmetic behind it is still true where no shared cache is
   * available. Set it to a byte budget to restore the sized table; the budget divided by
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
   * STOPPED there, because every read still decoded its bucket. A bucket record is far smaller than
   * a block (it holds references, not values), so matching the counts costs a small fraction of the
   * block budget and is not metered separately.
   * </p>
   */
  private static final int READ_VIEW_BUCKET_SLOTS =
      Math.max(READ_VIEW_BUCKET_CACHE_SIZE, READ_VIEW_BLOCK_SLOTS);

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
   * <p>The fixed direct-mapped caches retain immutable entry-node and reverse-bucket references
   * only — never a value, so the view's footprint is fixed whatever the dictionary's cardinality.
   * A hot-loop HIT performs neither a radix traversal nor an allocation. A MISS resolves through a
   * retained bucket when one is held, which removes the three radix-node decodes a walk from the
   * root would repeat for all 256 ids the bucket covers; it still decodes the entry record itself,
   * so a miss is NOT allocation-free. The view refuses an incomplete/unknown dictionary
   * up front and checks the revision before every operation, so it can never reinterpret a row id
   * against another revision's dictionary.</p>
   *
   * @param headerNodeKey dictionary header key recorded by the projection column
   * @param reader reader positioned at the revision that owns the projection rows
   * @return a readable view, or {@code null} when the dictionary is absent, incomplete, or changed
   *         revision while the view was being opened
   */
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
    return new ReadView(headerNodeKey, header.getReverseRootKey(), header.getEntryCount(), revision, namePage,
        databaseType, reader, header.getBlockIndexKey(), header.isFullyOrdered());
  }

  /** Ids per separator-array entry; one reverse bucket, so the partition needs no spill handling. */
  private static final int VALUES_PER_INDEXED_RANGE = ValueDictionaryValueBucketNode.VALUES_PER_BUCKET;

  private static final byte[] EMPTY_SEPARATOR = new byte[0];

  /** Revision-bound, fixed-memory reverse-dictionary access for scan kernels. */
  public static final class ReadView {

    private final long headerNodeKey;
    private final long reverseRootKey;
    private final int entryCount;
    private final int revision;
    private final NamePage namePage;
    private final DatabaseType databaseType;
    private final StorageEngineReader reader;
    /**
     * Per-id SLICE cache: the backing array a value lives in, plus its offset and length. No entry
     * node and no copied {@code byte[]} — a scan compares far more values than it emits, so a
     * wrapper or a copy per compared id is precisely the per-row garbage the packed layout removes.
     */
    private final int[] cachedIds = new int[READ_VIEW_CACHE_SIZE];
    private final byte[][] cachedBacking = new byte[READ_VIEW_CACHE_SIZE][];
    private final int[] cachedOffsets = new int[READ_VIEW_CACHE_SIZE];
    private final int[] cachedLengths = new int[READ_VIEW_CACHE_SIZE];
    /**
     * SPILL lane, same slot indexing. A spilled value stays behind its record rather than having its
     * array handed out: a record owns its bytes, and exposing them to keep one cache uniform would
     * trade the node's immutability for a convenience. Exactly one of {@code cachedBacking[slot]}
     * and {@code cachedSpills[slot]} is non-null for a resolved slot.
     */
    private final ValueDictionaryEntryNode[] cachedSpills = new ValueDictionaryEntryNode[READ_VIEW_CACHE_SIZE];
    /** Direct-mapped reverse-bucket retention; {@code -1} marks an unused slot. */
    private int @Nullable [] cachedBuckets;
    private ValueDictionaryValueBucketNode @Nullable [] cachedBucketNodes;
    /**
     * Direct-mapped retention of decoded SUB-BLOCKS, keyed by record key. A block is up to 64 KiB
     * and packs many consecutive ids, so decoding one per probe dominated the miss path; holding a
     * few costs a fixed number of references and no per-id state.
     */
    private long @Nullable [] cachedBlockKeys;
    private ValueDictionaryValueBlockNode @Nullable [] cachedBlocks;
    /**
     * Separator array over the ordered prefix, loaded ONCE per view and then kept. It is the whole
     * point of the structure: without it a binary-search probe decodes one block per step, with it
     * one block per probe, and re-reading it per probe would give back exactly what it saves.
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
     * Whether EVERY id is in collation order of its value — {@code orderedPrefixCount == entryCount}
     * on the header, the single test an ordering arm may make. While it holds, id order IS value
     * order, so id comparisons answer string comparisons with no dictionary touch at all.
     */
    private final boolean fullyOrdered;

    private ReadView(final long headerNodeKey, final long reverseRootKey, final int entryCount, final int revision,
        final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader,
        final long blockIndexKey, final boolean fullyOrdered) {
      this.blockIndexKey = blockIndexKey;
      this.fullyOrdered = fullyOrdered;
      this.headerNodeKey = headerNodeKey;
      this.reverseRootKey = reverseRootKey;
      this.entryCount = entryCount;
      this.revision = revision;
      this.namePage = namePage;
      this.databaseType = databaseType;
      this.reader = reader;
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
      if (lengthMode != ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
          && lengthMode != ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
        throw new IllegalArgumentException("not a string length mode: " + lengthMode);
      }
      final int[] table = new int[entryCount + 1];
      for (int id = 1; id <= entryCount; id++) {
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
      return table;
    }

    /**
     * Materialize the value interned under {@code id} as a {@link String}.
     *
     * <p>
     * For WINNERS only — group emission, deferred-extremum results — never for per-row work: the
     * whole point of the id lanes is that rows stay integers. Packed ids decode straight off their
     * slice; spilled ids go through the record's defensive copy, which is fine at winner cardinality.
     */
    public String valueAsString(final int id) {
      final int slot = sliceSlot(id);
      final ValueDictionaryEntryNode spill = cachedSpills[slot];
      if (spill != null) {
        return new String(spill.getValue(), StandardCharsets.UTF_8);
      }
      return new String(cachedBacking[slot], cachedOffsets[slot], cachedLengths[slot],
          StandardCharsets.UTF_8);
    }

    /**
     * Evaluate one string predicate against EVERY value in this revision's dictionary, returning a
     * verdict bitset over id space: bit {@code id} (1-based, bit 0 unused) is set iff the value
     * interned under {@code id} satisfies {@code op} against {@code literalUtf8}.
     *
     * <p>
     * This is the global half of the two-phase pattern the per-leaf dictionaries already use
     * ({@code evalStringDict}): the string work runs once per DISTINCT value here, and every row
     * group afterwards answers each row with one bit test against the id it already stores. Packed
     * ids evaluate over their zero-copy {@code (backing, offset, length)} slices through the same
     * per-entry authority the leaf kernels use ({@code ProjectionIndexScan.stringDictEntryMatches}),
     * so op semantics — including the UTF-16 collation contract for the ordering ops — cannot drift
     * between the two dictionary tiers. Spilled ids evaluate through their record's own entry
     * points, which exist so the record's array never escapes.
     *
     * <p>
     * Sequential ids share sub-blocks, so the sweep runs at block-cache speed; the returned bitset
     * is immutable by convention and safe to share across scan workers.
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
      final long[] verdict = new long[entryCount + 64 >>> 6];
      final boolean litHasSupplementary =
          ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
      // BLOCK-AT-A-TIME, not id-at-a-time. A per-id walk routes all entryCount values through
      // sliceSlot, whose direct-mapped slice cache MISSES on every one of them — ascending ids never
      // repeat a slot — so each value pays a revision check, two cache probes and a bucket search to
      // reach bytes the block it came from is already holding. Measured on a 275,494-entry URL
      // dictionary that machinery alone was 16.5 ms of a 25.1 ms sweep. Walking the reverse trie
      // once and reading each block's packed bytes in place removes it without changing a verdict.
      final int bucketCount = (entryCount - 1 >>> 8) + 1;
      for (int bucket = 0; bucket < bucketCount; bucket++) {
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
            throw new IllegalStateException("global value dictionary block " + blockFirstId + " is missing from "
                + "revision " + revision);
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
              verdict[id >>> 6] |= 1L << (id & 63);
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
            verdict[id >>> 6] |= 1L << (id & 63);
          }
        }
      }
      return verdict;
    }

    /**
     * Op dispatch for a SPILLED value, through the record's no-escape entry points. Semantics mirror
     * {@code stringDictEntryMatches} arm for arm; ordering uses {@code compareToRange}, which is
     * UTF-16 collation unconditionally — the same order the byte-path arm reaches via its
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
     * Compare the value behind {@code id} to a caller-owned byte range, under the same collation.
     *
     * <p>
     * The binary-search probe's inner loop. It exists so that searching an ordered prefix reuses this
     * view's caches — the bucket, the decoded block and the resolved slice — instead of walking the
     * reverse radix and decoding a 33 KB block from scratch for every one of its ~18 steps, which is
     * what the stateless per-id read does and what made the search 39x the hash probe when measured.
     * Allocation-free by the same construction as {@link #compareIds}.
     * </p>
     *
     * @return negative, zero or positive as the stored value orders before, with, or after the range
     */
    public int compareIdToValue(final int id, final byte[] utf8, final int offset, final int length) {
      final int slot = sliceSlot(id);
      final ValueDictionaryEntryNode spill = cachedSpills[slot];
      return spill == null
          ? ValueDictionaryEntryNode.compareUtf16Range(cachedBacking[slot], cachedOffsets[slot], cachedLengths[slot],
              utf8, offset, length)
          : spill.compareToRange(utf8, offset, length);
    }

    /**
     * The id range that can hold {@code utf8}, narrowed by the separator array when there is one.
     *
     * <p>
     * Returns {@code (low << 32) | high} packed, because this is on the probe path and a record here
     * would allocate per probe. Without a separator array the range is the whole ordered prefix,
     * which is correct and merely slower — the array is an accelerator, never a source of truth.
     * </p>
     */
    long candidateIdRange(final byte[] utf8, final int offset, final int length, final int boundary) {
      if (!blockIndexLoaded) {
        blockIndexLoaded = true;
        if (blockIndexKey != 0L) {
          final DataRecord record =
              namePage.getProjectionValueDictionaryRecord(blockIndexKey, databaseType, reader);
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
      return transformed(id, start, length, (byte) 1);
    }

    /** Allocation-free order-preserving pack of a 16-byte ISO-minute substring. */
    public long packIsoMinuteSubstring(final int id, final int start, final int length) {
      return transformed(id, start, length, (byte) 2);
    }

    /** Materialise a validated ISO-minute substring for one emitted winner. */
    public String materializeIsoMinuteSubstring(final int id, final int start, final int length) {
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
      if (ProjectionIndexByteScan.packIsoMinuteSubstring(backing, offset, valueLength, start, length)
          == Long.MIN_VALUE) {
        throw new IllegalArgumentException("dictionary value is not an admissible ISO-minute substring");
      }
      return new String(backing, offset + start - 1, length, java.nio.charset.StandardCharsets.US_ASCII);
    }

    /**
     * Resolve {@code id} to a cache slot holding its slice, returning the slot index.
     *
     * <p>
     * A packed id yields the sub-block's own backing array with an offset and length — nothing is
     * copied and no record wrapper is built. A spilled id yields its entry record's bytes, which the
     * record already owns. Either way the cached triple is a VIEW, never a copy.
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
      final int bucket = (id - 1) >>> 8;
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
              "global value dictionary id " + id + " is missing from revision " + revision);
        }
        cachedBucketNodes[bucketSlot] = bucketNode;
        cachedBuckets[bucketSlot] = bucket;
      }
      final long blockKey = bucketNode.blockKeyCovering(id);
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
          block = GlobalValueDictionaryRadix.blockNode(blockKey, id, namePage, databaseType, reader);
          cachedBlocks[blockSlot] = block;
          cachedBlockKeys[blockSlot] = blockKey;
        }
        cachedBacking[slot] = block.rawBytes();
        cachedOffsets[slot] = block.valueOffset(id);
        cachedLengths[slot] = block.valueLength(id);
        cachedSpills[slot] = null;
      } else {
        final long spillKey = bucketNode.spillKeyCovering(id);
        if (spillKey == 0L) {
          throw new IllegalStateException(
              "global value dictionary id " + id + " is missing from revision " + revision);
        }
        cachedSpills[slot] = GlobalValueDictionaryRadix.spillEntry(spillKey, namePage, databaseType, reader);
        cachedBacking[slot] = null;
      }
      cachedIds[slot] = id;
      return slot;
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

  /** @return blocks warmed into the record cache since JVM start; {@code 0} means the warmer never ran. */
  public static long warmedBlockCount() {
    return WARMED_BLOCKS.get();
  }

  /** @return warmed blocks still resident when their pass ended; below {@link #warmedBlockCount()} means churn. */
  public static long residentBlockCount() {
    return RESIDENT_BLOCKS.get();
  }

  /**
   * Decodes a dictionary's value blocks into the buffer manager's record cache, ahead of the query
   * that would otherwise pay for them.
   *
   * <p>
   * <b>Why this exists.</b> A first verdict build over a 275,494-entry dictionary measured 142 ms,
   * of which 123 ms was first touch — 84 ms fetching and deserializing 1,085 block records and 39 ms
   * decoding and front-expanding them — against 19 ms of steady-state work once they are resident.
   * Every later execution pays the 19 ms. This moves the 123 ms off the query that happens to be
   * first. A prefetch of the pages alone would move only the 84 ms; a warmer has to fetch in order
   * to decode, so it moves both.
   * </p>
   *
   * <p>
   * <b>It caches values, never accessors.</b> Nothing here is retained: the walk touches records
   * through {@code NamePage}, which populates the record cache with decoded, immutable block
   * records, and the reader this runs on belongs to the caller. No {@link ReadView} is held, so no
   * transaction is pinned past its own lifetime.
   * </p>
   *
   * <p>
   * <b>Partial warmth is partial benefit, never wrongness.</b> The walk stops when it has warmed
   * {@code budgetBytes}, so a dictionary larger than the record cache warms its low ids and leaves
   * the rest; a query reaching an unwarmed block decodes it through the path that already exists.
   * Racing is safe for the same reason — a query arriving mid-warm finds some blocks resident and
   * fetches the others. A failure is swallowed for the same reason: warming is an optimisation, and
   * a resource that closed underneath a background walk must not turn into a query error.
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
        final ValueDictionaryValueBucketNode bucketNode =
            GlobalValueDictionaryRadix.valueBucketOf(view.reverseRootKey, bucket, view.namePage, view.databaseType,
                reader);
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
    return GlobalValueDictionaryRadix.value(header.getReverseRootKey(), id, namePage, databaseType, reader);
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
   * gets their strings back. Ids are visited in ascending order so that ids sharing a record page are
   * resolved consecutively, which is what turns k random reads into far fewer page touches; the
   * caller's order is restored through the index carried alongside.
   *
   * @param headerNodeKey the dictionary's header key
   * @param ids the ids to resolve; not modified
   * @param reader the reader positioned at the revision wanted
   * @return the values, index-aligned to {@code ids}; an entry is {@code null} when its id is not
   *         stored in this revision
   */
  public static @Nullable String[] values(final long headerNodeKey, final int[] ids, final StorageEngineReader reader) {
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
    final int[] order = new int[ids.length];
    for (int i = 0; i < ids.length; i++) {
      order[i] = i;
    }
    sortIndicesByValue(order, ids);
    for (final int slot : order) {
      final byte[] bytes = valueBytes(header, ids[slot], namePage, databaseType, reader);
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
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    // The ordered prefix is probed by BINARY SEARCH over the reverse index, which is sorted by value
    // because its ids were minted in collation order. That is what lets a rank-ordered dictionary
    // carry no forward hash index at all. A dictionary with an unordered tail must try BOTH: the
    // value may be in either half, and answering ABSENT after searching only the prefix would be a
    // wrong answer, not a slow one.
    final int boundary = header.getOrderedPrefixCount();
    if (boundary > 0) {
      // ONE view for the whole search: its bucket, block and slice caches are what make the ~18
      // steps cost far less than 18 independent reads, and they are useless if a view is built per
      // step. A caller that interns many values should hold a view across them for the same reason.
      final ReadView view = readView(headerNodeKey, reader);
      if (view == null) {
        return ID_UNKNOWN;
      }
      final int ordered = searchOrderedPrefix(view, boundary, utf8, offset, length);
      if (ordered != ID_ABSENT) {
        return ordered;
      }
      if (header.isFullyOrdered()) {
        return ID_ABSENT;
      }
    }
    if (header.getForwardRootKey() == 0) {
      // Only a fully ordered dictionary may omit the forward index, and that case returned above.
      return ID_UNKNOWN;
    }
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
   * the same partition, and the bucket one is total by construction — a bucket covers its ids
   * whether they are packed in blocks or spilled to their own records, so the search's within-range
   * step handles a spilled value with no special case.
   * </p>
   *
   * @return the record key of the separator array, or 0 when the dictionary is too small to index
   */
  public static long buildBlockIndex(final long headerNodeKey, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineWriter writer, final TransactionIntentLog log) {
    final ValueDictionaryHeaderNode header = header(headerNodeKey, writer);
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
    final long indexKey = namePage.reserveProjectionValueDictionaryKeys(databaseType, 1L);
    namePage.putProjectionValueDictionaryRecord(
        ValueDictionaryBlockIndexNode.takeOwnership(indexKey, firstIds, packed, offsets), databaseType, writer, log);
    namePage.putProjectionValueDictionaryRecord(
        new ValueDictionaryHeaderNode(header.getNodeKey(), ValueDictionaryHeaderNode.VERSION, entryCount,
            header.getForwardRootKey(), header.getReverseRootKey(), header.getGeneration(),
            header.getOrderedPrefixCount(), indexKey),
        databaseType, writer, log);
    return indexKey;
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
    final byte[] candidate = java.util.Arrays.copyOf(next, cut);
    return ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, candidate, 0, candidate.length) < 0
        ? candidate
        : next.clone();
  }

  /**
   * Binary search for {@code utf8} over ids {@code 1..boundary}, which are in collation order.
   *
   * <p>
   * The comparator MUST be {@link ValueDictionaryEntryNode#compareUtf16Range} and not unsigned byte
   * order: the two differ for supplementary characters, which sort after U+E000..U+FFFF in UTF-8
   * bytes but before them in UTF-16. The rank pass sorts with a byte substitution that is provably
   * equivalent to this comparator, so searching with anything else would look up a value in an order
   * it was not stored in and answer ABSENT for a value that is present.
   * </p>
   *
   * @return the id, or {@link #ID_ABSENT} when the prefix provably does not hold the value, or
   *         {@link #ID_UNKNOWN} when a value could not be read
   */
  private static int searchOrderedPrefix(final ReadView view, final int boundary, final byte[] utf8, final int offset,
      final int length) {
    // The separator array narrows the search to ONE block before a single value is read; without it
    // the range is the whole prefix and every step decodes a different block.
    final long range = view.candidateIdRange(utf8, offset, length, boundary);
    int low = (int) (range >>> 32);
    int high = (int) range;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int comparison = view.compareIdToValue(mid, utf8, offset, length);
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

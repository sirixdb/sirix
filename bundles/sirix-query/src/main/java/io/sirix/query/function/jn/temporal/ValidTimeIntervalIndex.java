package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.HotOrderedStore;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.AbstractJsonDBArray;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.index.interval.PostingChunkConsumer;
import io.sirix.index.interval.PostingMask;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.roaringbitmap.RelativeRangeConsumer;
import org.roaringbitmap.longlong.LongConsumer;
import java.util.function.LongFunction;

public final class ValidTimeIntervalIndex {
  private static final class Diagnostics {
    private static final boolean SCAN_DIAGNOSTICS = HotOrderedStore.scanDiagnosticsEnabled();
    private static final LongAdder CANDIDATE_WORDS_VISITED = new LongAdder();
    private static final LongAdder CANDIDATE_BITMAPS_CREATED = new LongAdder();
    private static final LongAdder RUN_MERGES = new LongAdder();
    private static final LongAdder RUN_KEYS_MERGED = new LongAdder();
  }

  public static long runMerges() {
    return Diagnostics.RUN_MERGES.sum();
  }

  public static long runKeysMerged() {
    return Diagnostics.RUN_KEYS_MERGED.sum();
  }

  public static long candidateWordsVisited() {
    return Diagnostics.CANDIDATE_WORDS_VISITED.sum();
  }

  public static long candidateBitmapsCreated() {
    return Diagnostics.CANDIDATE_BITMAPS_CREATED.sum();
  }

  @SuppressWarnings("ArrayRecordComponent")
  record Evidence(long[] members, Roaring64Bitmap unverified) {
  }

  private ValidTimeIntervalIndex() {}

  @SuppressWarnings("ArrayRecordComponent") // Carries the resolved keys without copying or value equality.
  public record RoutedKeys(long[] keys, int revision) {
  }

  public static @Nullable RoutedKeys routedKeys(final @Nullable Sequence sequence) {
    return sequence instanceof ValidTimeKeySequence keys
        ? new RoutedKeys(keys.matchingKeys(), keys.revision())
        : null;
  }

  public static @Nullable Sequence sequence(final JsonDBItem document, final Instant instant,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd) {
    return sequence(document, instant, config, strictStart, strictEnd, null);
  }

  static @Nullable ValidTimeKeySequence sequence(final JsonDBItem document, final Instant instant,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd,
      final @Nullable ValidTimeResidual residual) {
    Objects.requireNonNull(document);
    Objects.requireNonNull(instant);
    Objects.requireNonNull(config);
    if (!isIndexView(document) || isMutable(document)) {
      return null;
    }
    final IndexDef definition = findValidTimeIndex(document);
    if (definition == null) {
      return null;
    }
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, residual, definition.getID());
  }

  public static @Nullable Sequence comparisonSequence(final JsonDBItem document, final Supplier<Sequence> point,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd) {
    if (!(document instanceof Array array) || !isIndexView(document) || isMutable(document) || array.len() == 0) {
      return null;
    }
    final IndexDef definition = findValidTimeIndex(document);
    if (definition == null) {
      return null;
    }
    final var reader = document.getTrx().getStorageEngineReader();
    final JsonIndexController controller =
        (JsonIndexController) document.getResourceSession()
                                      .getRtxIndexController(document.getTrx().getRevisionNumber());
    if (!controller.isExactValidTimeArray(reader, definition, document.getNodeKey(), array.len())) {
      return null;
    }
    if (!(point.get() instanceof DateTime dateTime) || dateTime.getTimezone() == null) {
      return null;
    }
    final Instant instant = new DateTimeToInstant().convert(dateTime);
    if (!new IntervalDomain().isExact(instant)) {
      return null;
    }
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, null, definition.getID(), true);
  }

  /**
   * Exact comparison-range admission. Inexact/missing bounds or unsafe revision coverage decline
   * before evaluating query instants, retaining the original field casts and short-circuit order.
   */
  public static @Nullable Sequence comparisonRangeSequence(final JsonDBItem document,
      final Supplier<Sequence> startLimit, final Supplier<Sequence> endLimit, final ValidTimeConfig config,
      final boolean strictStart, final boolean strictEnd) {
    if (!(document instanceof Array array) || !isIndexView(document) || isMutable(document) || array.len() == 0) {
      return null;
    }
    final IndexDef definition = findValidTimeIndex(document);
    if (definition == null) {
      return null;
    }
    final var reader = document.getTrx().getStorageEngineReader();
    final JsonIndexController controller =
        document.getResourceSession().getRtxIndexController(document.getTrx().getRevisionNumber());
    if (!controller.isExactValidTimeArray(reader, definition, document.getNodeKey(), array.len())) {
      return null;
    }
    final Sequence upperValue;
    final Sequence lowerValue;
    try {
      upperValue = startLimit.get();
      lowerValue = endLimit.get();
    } catch (final QueryException unsafeProbe) {
      // The original comparisons own short-circuit order and observable errors. A probe must
      // decline if eager bound evaluation raises an error that a false first predicate can hide.
      return null;
    }
    if (!(upperValue instanceof DateTime upper) || !(lowerValue instanceof DateTime lower)
        || !zonedBounds(upper, lower)) {
      return null;
    }
    final DateTimeToInstant converter = new DateTimeToInstant();
    final Instant hi;
    final Instant lo;
    try {
      hi = converter.convert(upper);
      lo = converter.convert(lower);
    } catch (final DateTimeException | IllegalArgumentException unsupportedInstant) {
      return null;
    }
    final IntervalDomain domain = new IntervalDomain();
    if (!domain.isExact(lo) || !domain.isExact(hi) || lo.isAfter(hi)) {
      return null;
    }
    return exactRangeSequence(document, config, strictStart, strictEnd, definition, reader, controller, domain, lo, hi);
  }

  private static boolean zonedBounds(final DateTime upper, final DateTime lower) {
    return upper.getTimezone() != null && lower.getTimezone() != null;
  }

  private static Sequence exactRangeSequence(final JsonDBItem document, final ValidTimeConfig config,
      final boolean strictStart, final boolean strictEnd, final IndexDef definition, final StorageEngineReader reader,
      final JsonIndexController controller, final IntervalDomain domain, final Instant lo, final Instant hi) {
    final int revision = document.getTrx().getRevisionNumber();
    final long arrayKey = document.getNodeKey();
    final long lowerPoint = domain.point(lo);
    final long upperPoint = domain.point(hi);
    final int mode = (strictStart
        ? 1
        : 0)
        | (strictEnd
            ? 2
            : 0);
    final long[] cached = ValidTimeRangeCache.get(document.getResourceSession(), controller, revision, arrayKey,
        definition.getID(), lowerPoint, upperPoint, mode);
    if (cached != null) {
      return ValidTimeKeySequence.exactKeys(document, lo, config, definition.getID(), cached);
    }
    final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader, definition.getID(), domain);
    // Exact cohort admission proved timestamp representation; membership still excludes sibling arrays.
    final long[] matches = members(document, definition.getID(),
        collectRangeKeys(document, tree, lowerPoint, upperPoint, strictStart, strictEnd));
    ValidTimeRangeCache.put(document.getResourceSession(), controller, revision, arrayKey, definition.getID(),
        lowerPoint, upperPoint, mode, matches);
    return ValidTimeKeySequence.exactKeys(document, lo, config, definition.getID(), matches);
  }

  private static long[] collectRangeKeys(final JsonDBItem document, final RelationalIntervalTree tree,
      final long lowerPoint, final long upperPoint, final boolean strictStart, final boolean strictEnd) {
    final long maximum = document.getTrx().getMaxNodeKey();
    if (maximum >= 0 && maximum <= ValidTimeRangeKeys.MAXIMUM_KEY
        && maximum <= Math.max(65536L, ((Array) document).len() * 32L)) {
      // At most 2 MiB, and only a compact resource domain. Sparse/large resources retain the
      // output-sized collector below. The bitmap removes ties directly and emits sorted keys.
      final ValidTimeRangeKeys keys = new ValidTimeRangeKeys((int) maximum);
      tree.intersect(lowerPoint, upperPoint, keys);
      if (strictStart) {
        tree.startingAt(upperPoint, keys::remove);
      }
      if (strictEnd) {
        tree.endingAt(lowerPoint, keys::remove);
      }
      return keys.sortedKeys();
    }
    final LongArrayList keys = new LongArrayList();
    tree.intersect(lowerPoint, upperPoint, keys::add);
    final LongArrayList ties = new LongArrayList(0);
    if (strictStart) {
      tree.startingAt(upperPoint, ties::add);
    }
    if (strictEnd) {
      tree.endingAt(lowerPoint, ties::add);
    }
    final long[] sorted = keys.toLongArray();
    final long[] excluded = ties.toLongArray();
    sortRangeKeys(sorted);
    sortRangeKeys(excluded);
    int tie = 0;
    int count = 0;
    long previous = -1;
    for (final long key : sorted) {
      while (tie < excluded.length && excluded[tie] < key) {
        tie++;
      }
      if (key != previous && (tie == excluded.length || excluded[tie] != key)) {
        sorted[count++] = key;
      }
      previous = key;
    }
    return count == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, count);
  }

  private static void sortRangeKeys(final long[] keys) {
    ValidTimeKeySort.sort(keys);
  }

  public static long[] keys(final JsonDBItem document, final Instant instant, final boolean strictEnd) {
    Objects.requireNonNull(document);
    final ValidTimeConfig config =
        Objects.requireNonNull(document.getResourceSession().getResourceConfig().getValidTimeConfig());
    final IndexDef exactCohort = strictEnd && document instanceof Array && isIndexView(document) && !isMutable(document)
        && new IntervalDomain().isExact(instant)
            ? findValidTimeIndex(document)
            : null;
    if (exactCohort != null) {
      final JsonIndexController controller =
          document.getResourceSession().getRtxIndexController(document.getTrx().getRevisionNumber());
      final var reader = document.getTrx().getStorageEngineReader();
      // A known exact cohort needs no closed stab for endpoint verification. Membership still
      // excludes matching intervals in nested/sibling arrays; the proof never loads on an empty stab.
      if (controller.isKnownExactValidTimeArray(reader, exactCohort, document.getNodeKey(), ((Array) document).len())) {
        return exactCohortMembers(document, instant, exactCohort.getID());
      }
    }
    final Sequence sequence = sequence(document, instant, config, false, strictEnd);
    if (sequence != null) {
      final long[] keys = ((ValidTimeKeySequence) sequence).matchingKeys();
      if (keys.length != 0 && exactCohort != null) {
        final JsonIndexController controller =
            document.getResourceSession().getRtxIndexController(document.getTrx().getRevisionNumber());
        controller.isExactValidTimeArray(document.getTrx().getStorageEngineReader(), exactCohort, document.getNodeKey(),
            ((Array) document).len());
      }
      return keys;
    }
    final LongArrayList keys = new LongArrayList();
    try (final var iterator =
        ValidTimeFilter.linearScanSequence(document, instant, config, false, strictEnd).iterate()) {
      for (var item = iterator.next(); item != null; item = iterator.next()) {
        keys.add(((JsonDBItem) item).getNodeKey());
      }
    }
    final long[] sorted = keys.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }

  static boolean isMutable(final JsonDBItem document) {
    final var reader = document.getTrx().getStorageEngineReader();
    return reader instanceof StorageEngineWriter || reader.hasTrxIntentLog();
  }

  private static boolean isIndexView(final JsonDBItem document) {
    // Persisted membership describes a whole storage array, not a positional or object-field view.
    return !(document instanceof AbstractJsonDBArray<?>) || document instanceof JsonDBArray;
  }

  /** Filter chunk keys against companion masks; emit every candidate once in key order. */
  static Evidence candidates(final JsonDBItem document, final Instant instant, final boolean strictStart,
      final boolean strictEnd, final int indexId) {
    final IntervalDomain domain = new IntervalDomain();
    final var reader = document.getTrx().getStorageEngineReader();
    final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader, indexId, domain);
    final ChunkCandidates candidates = new ChunkCandidates(document, indexId, true);
    final long point = domain.point(instant);
    final boolean exact = domain.isExact(instant);
    tree.stabChunks(point, exact && strictEnd, candidates);
    if (exact && strictStart && candidates.hasCandidates) {
      tree.startingAtChunks(point, candidates::removeExactStarts);
    }
    return candidates.evidence();
  }

  /**
   * Members of a proven-exact cohort valid at an exact instant: one half-open chunk stab is exact
   * from keys, so neither the tie chunk at the instant nor a verification mask is read.
   */
  static long[] exactCohortMembers(final JsonDBItem document, final Instant instant, final int indexId) {
    final IntervalDomain domain = new IntervalDomain();
    final var reader = document.getTrx().getStorageEngineReader();
    final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader, indexId, domain);
    final ChunkCandidates candidates = new ChunkCandidates(document, indexId, false);
    tree.stabHalfOpenChunks(domain.point(instant), candidates);
    return candidates.evidence().members();
  }

  /** Keep the sorted keys the index registered as members of this document's array, in place. */
  private static long[] members(final JsonDBItem document, final int indexId, final long[] sorted) {
    final var reader = document.getTrx().getStorageEngineReader();
    final HotOrderedStore members = document instanceof Array
        ? ValidTimeIntervalIndexFactory.createMembershipStore(reader, indexId)
        : null;
    NodeReferences memberChunk = null;
    long chunk = -1;
    int count = 0;
    for (final long key : sorted) {
      if ((key >>> 16) != chunk) {
        chunk = key >>> 16;
        if (members != null) {
          memberChunk = members.chunk(document.getNodeKey(), 0, key);
        }
      }
      if (members == null
          ? key == document.getNodeKey()
          : memberChunk != null && memberChunk.contains(key & 0xFFFFL)) {
        sorted[count++] = key;
      }
    }
    return count == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, count);
  }

  static final class ChunkCandidates implements PostingChunkConsumer, RelativeRangeConsumer, LongConsumer {
    private static final int WORDS = 1024;
    // An eight-byte run key and an eight-byte bitmap word break even at 1024 matches.
    private static final int DENSE_THRESHOLD = WORDS;
    private final LongFunction<PostingMask> membership;
    private final LongFunction<PostingMask> verification;
    // Chunks keyed by base in ascending order: a stab touches a handful of 64K chunks, so a sorted
    // pair of arrays beats a tree and keeps the fastutil map interface monomorphic for the JIT.
    private long[] chunkBases = new long[8];
    private Chunk[] chunkValues = new Chunk[8];
    private int chunkCount;
    private int lastChunkIndex;
    private final RunBuffer runKeys = new RunBuffer();
    private final long[] scratch = new long[WORDS];
    private final int[] touchedWords = new int[WORDS];
    private int touchedCount;
    private boolean hasCandidates;
    private final long[] wordGroups = new long[WORDS / Long.SIZE];
    // Per-key selection state while a bitmap-encoded chunk streams through {@link #accept(long)}.
    private @Nullable Chunk streamChunk;
    private long streamBase;
    private boolean streamVerifyOnly;
    private int streamAdded;
    private int streamLastWord;
    private int streamWords;
    private int streamPosition;

    private static final class RunBuffer {
      private static final long[] EMPTY = new long[0];
      private long[] keys = EMPTY;
      private int size;

      /** Room for {@code extra} more keys, growing geometrically. */
      private void ensureCapacity(final int extra) {
        final int required = Math.addExact(size, extra);
        if (required > keys.length) {
          final long capacity = Math.max(16L, Math.max(required, (long) keys.length + (keys.length >> 1)));
          final int boundedCapacity = (int) Math.min(Integer.MAX_VALUE - 8L, capacity);
          if (boundedCapacity < required) {
            throw new OutOfMemoryError("Valid-time run buffer exceeds the maximum array size");
          }
          keys = Arrays.copyOf(keys, boundedCapacity);
        }
      }

      private void appendWord(final long prefix, long bits) {
        if (bits == 0) {
          return;
        }
        ensureCapacity(Long.bitCount(bits));
        final long[] output = keys;
        int position = size;
        while (bits != 0) {
          output[position++] = prefix | Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
        }
        size = position;
      }
    }

    private static final class Run {
      private final int position;
      private int limit;
      private @Nullable Run next;

      private Run(final int position, final int limit) {
        this.position = position;
        this.limit = limit;
      }
    }

    private static final class Chunk {
      private final PostingMask membership;
      private @Nullable Run first;
      private @Nullable Run last;
      private @Nullable PostingMask unverified;
      private long @Nullable [] selected;
      // An upper bound for sparse runs until union; exact for a dense container.
      private int count;

      private Chunk(final PostingMask membership) {
        this.membership = membership;
      }
    }

    /** With {@code verify} false every candidate is taken as exact and no verification mask is read. */
    private ChunkCandidates(final JsonDBItem document, final int indexId, final boolean verify) {
      final var reader = document.getTrx().getStorageEngineReader();
      final long documentKey = document.getNodeKey();
      if (document instanceof Array) {
        final HotOrderedStore members = ValidTimeIntervalIndexFactory.createMembershipStore(reader, indexId);
        membership = base -> members.chunkMask(documentKey, 0, base);
      } else {
        membership = base -> (documentKey & ~0xFFFFL) == base
            ? PostingMask.from(NodeReferences.ofSortedArray(new long[] {documentKey & 0xFFFFL}))
            : PostingMask.from(null);
      }
      if (verify) {
        final HotOrderedStore unverified = ValidTimeIntervalIndexFactory.createVerificationStore(reader, indexId);
        verification = base -> unverified.chunkMask(0, 0, base);
      } else {
        verification = _ -> PostingMask.from(null);
      }
    }

    ChunkCandidates(final LongFunction<PostingMask> membership, final LongFunction<PostingMask> verification) {
      this.membership = Objects.requireNonNull(membership);
      this.verification = Objects.requireNonNull(verification);
    }

    private void mark(final int key) {
      final int word = key >>> 6;
      if (scratch[word] == 0) {
        touchedWords[touchedCount++] = word;
      }
      scratch[word] |= 1L << key;
    }

    /**
     * Mark every key of one chunk's references: a plain loop over the packed run, or a word-wise bitmap
     * walk.
     */
    private void collect(final NodeReferences references) {
      final long[] keys = references.sortedKeysOrNull();
      if (keys != null) {
        for (final long key : keys) {
          mark((int) key);
        }
        return;
      }
      references.getNodeKeys().forAllInRange(0L, WORDS << 6, this);
    }

    @Override
    public void acceptPresent(final int relativePos) {
      mark(relativePos);
    }

    @Override
    public void acceptAbsent(final int relativePos) {}

    @Override
    public void acceptAllPresent(final int relativeFrom, final int relativeTo) {
      final int firstWord = relativeFrom >>> 6;
      final int lastWord = (relativeTo - 1) >>> 6;
      for (int word = firstWord; word <= lastWord; word++) {
        if (scratch[word] == 0) {
          touchedWords[touchedCount++] = word;
        }
      }
      PostingMask.fillRange(scratch, relativeFrom, relativeTo);
    }

    @Override
    public void acceptAllAbsent(final int relativeFrom, final int relativeTo) {}

    private Chunk chunk(final long base) {
      // Consecutive chunks of a logical key ascend, so the previous or next slot usually hits.
      final int last = lastChunkIndex;
      if (last < chunkCount && chunkBases[last] == base) {
        return chunkValues[last];
      }
      if (last + 1 < chunkCount && chunkBases[last + 1] == base) {
        lastChunkIndex = last + 1;
        return chunkValues[last + 1];
      }
      final int index = Arrays.binarySearch(chunkBases, 0, chunkCount, base);
      if (index >= 0) {
        lastChunkIndex = index;
        return chunkValues[index];
      }
      final Chunk chunk = new Chunk(membership.apply(base));
      final int insert = -index - 1;
      if (chunkCount == chunkBases.length) {
        chunkBases = Arrays.copyOf(chunkBases, chunkCount << 1);
        chunkValues = Arrays.copyOf(chunkValues, chunkCount << 1);
      }
      System.arraycopy(chunkBases, insert, chunkBases, insert + 1, chunkCount - insert);
      System.arraycopy(chunkValues, insert, chunkValues, insert + 1, chunkCount - insert);
      chunkBases[insert] = base;
      chunkValues[insert] = chunk;
      chunkCount++;
      lastChunkIndex = insert;
      return chunk;
    }

    private @Nullable Chunk chunkOrNull(final long base) {
      final int index = Arrays.binarySearch(chunkBases, 0, chunkCount, base);
      return index >= 0
          ? chunkValues[index]
          : null;
    }

    @Override
    public void accept(final long base, final NodeReferences references, final boolean verifyOnly) {
      if (!references.hasNodeKeys()) {
        return;
      }
      final long[] sorted = references.sortedKeysOrNull();
      if (sorted != null) {
        acceptSorted(base, sorted, sorted.length, verifyOnly);
        return;
      }
      final Chunk chunk = chunk(base);
      if (chunk.membership.isEmpty()) {
        return;
      }
      // A bitmap-encoded chunk streams key by key through the library's callback into the same
      // single-pass selection as a sorted run; only strict-start removal still marks scratch.
      streamChunk = chunk;
      streamBase = base;
      streamVerifyOnly = verifyOnly;
      streamAdded = 0;
      streamLastWord = -1;
      streamWords = 0;
      if (chunk.selected == null) {
        runKeys.ensureCapacity((int) Math.min(WORDS << 6, references.cardinality()));
      }
      final int start = runKeys.size;
      streamPosition = start;
      references.getNodeKeys().forEach(this);
      streamChunk = null;
      if (Diagnostics.SCAN_DIAGNOSTICS) {
        Diagnostics.CANDIDATE_WORDS_VISITED.add(streamWords);
      }
      final int added = streamAdded;
      if (added == 0) {
        return;
      }
      hasCandidates = true;
      if (chunk.selected != null) {
        chunk.count += added;
        return;
      }
      if (chunk.first == null && added >= DENSE_THRESHOLD) {
        promoteRun(chunk, runKeys.keys, start, streamPosition, added);
        return;
      }
      runKeys.size = streamPosition;
      linkRun(chunk, base, start, added);
    }

    /**
     * One streamed key of a bitmap-encoded chunk; see {@link #accept(long, NodeReferences, boolean)}.
     */
    @Override
    public void accept(final long streamed) {
      final Chunk chunk = Objects.requireNonNull(streamChunk);
      final int key = (int) streamed;
      final int word = key >>> 6;
      if (word != streamLastWord) {
        streamWords++;
        streamLastWord = word;
      }
      final long bit = 1L << key;
      if ((chunk.membership.word(word) & bit) == 0) {
        return;
      }
      PostingMask unverified = chunk.unverified;
      if (unverified == null) {
        unverified = verification.apply(streamBase);
        chunk.unverified = unverified;
      }
      if (streamVerifyOnly) {
        if (Diagnostics.SCAN_DIAGNOSTICS) {
          Diagnostics.CANDIDATE_WORDS_VISITED.increment();
        }
        if ((unverified.word(word) & bit) == 0) {
          return;
        }
      }
      final long[] selected = chunk.selected;
      if (selected == null) {
        runKeys.keys[streamPosition++] = streamBase | key;
        streamAdded++;
      } else if ((selected[word] & bit) == 0) {
        selected[word] |= bit;
        streamAdded++;
      }
    }

    /**
     * One pass over a compact ascending run: membership (and, for a tied end, verification) is tested
     * per key against the immutable masks, candidates go straight into the chunk's bitmap or run. This
     * compact path avoids scratch marking, touched-word bookkeeping and reset; bitmap-backed chunks use
     * {@link #accept(long, NodeReferences, boolean)} instead.
     */
    @Override
    public void acceptSorted(final long base, final long[] keys, final int count, final boolean verifyOnly) {
      if (count == 0) {
        return;
      }
      final Chunk chunk = chunk(base);
      final PostingMask membership = chunk.membership;
      if (membership.isEmpty()) {
        return;
      }
      final long[] selected = chunk.selected;
      int lastWord = -1;
      int wordsVisited = 0;
      int added = 0;
      if (selected == null) {
        runKeys.ensureCapacity(count);
      }
      final long[] output = runKeys.keys;
      int position = runKeys.size;
      for (int index = 0; index < count; index++) {
        final int key = (int) keys[index];
        final int word = key >>> 6;
        if (word != lastWord) {
          wordsVisited++;
          lastWord = word;
        }
        final long bit = 1L << key;
        if ((membership.word(word) & bit) == 0) {
          continue;
        }
        PostingMask unverified = chunk.unverified;
        if (unverified == null) {
          unverified = verification.apply(base);
          chunk.unverified = unverified;
        }
        if (verifyOnly) {
          if (Diagnostics.SCAN_DIAGNOSTICS) {
            Diagnostics.CANDIDATE_WORDS_VISITED.increment();
          }
          if ((unverified.word(word) & bit) == 0) {
            continue;
          }
        }
        if (selected == null) {
          output[position++] = base | key;
          added++;
        } else if ((selected[word] & bit) == 0) {
          selected[word] |= bit;
          added++;
        }
      }
      if (Diagnostics.SCAN_DIAGNOSTICS) {
        Diagnostics.CANDIDATE_WORDS_VISITED.add(wordsVisited);
      }
      if (added == 0) {
        return;
      }
      hasCandidates = true;
      if (selected != null) {
        chunk.count += added;
        return;
      }
      final int start = runKeys.size;
      if (chunk.first == null && added >= DENSE_THRESHOLD) {
        // A first run is unique by construction: promote straight from it without a merge.
        promoteRun(chunk, output, start, position, added);
        return;
      }
      runKeys.size = position;
      linkRun(chunk, base, start, added);
    }

    /** Turn one ascending run (already in the buffer, not yet linked) into the chunk's bitmap. */
    private void promoteRun(final Chunk chunk, final long[] run, final int start, final int limit, final int added) {
      if (Diagnostics.SCAN_DIAGNOSTICS) {
        Diagnostics.CANDIDATE_BITMAPS_CREATED.increment();
      }
      final long[] selected = new long[WORDS];
      for (int index = start; index < limit; index++) {
        final long key = run[index];
        selected[(int) ((key & 0xFFFFL) >>> 6)] |= 1L << key;
      }
      chunk.selected = selected;
      chunk.count = added;
    }

    /** Append a run of {@code count} fresh candidates and promote the chunk once it is dense. */
    private void linkRun(final Chunk chunk, final long base, final int start, final int count) {
      final Run run = new Run(start, runKeys.size);
      if (chunk.last == null) {
        chunk.first = run;
      } else {
        chunk.last.next = run;
      }
      chunk.last = run;
      chunk.count += count;
      if (chunk.count >= DENSE_THRESHOLD) {
        promoteOrUnion(chunk, base);
      }
    }

    /**
     * A chunk whose runs hold at least {@link #DENSE_THRESHOLD} keys: OR every run into the scratch
     * words in one plain pass, count the unique keys, and either keep the bitmap or re-emit the union
     * as a single run. The chunk is dense enough that scanning all 1024 words is proportional to its
     * keys, so no occupied-word bookkeeping is needed here.
     */
    private void promoteOrUnion(final Chunk chunk, final long base) {
      final long[] words = scratch; // all zero between accepts
      final long[] source = runKeys.keys;
      for (Run run = chunk.first; run != null; run = run.next) {
        if (Diagnostics.SCAN_DIAGNOSTICS) {
          Diagnostics.RUN_KEYS_MERGED.add(run.limit - run.position);
        }
        for (int index = run.position; index < run.limit; index++) {
          final long key = source[index];
          words[(int) ((key & 0xFFFFL) >>> 6)] |= 1L << key;
        }
      }
      int unique = 0;
      int occupied = 0;
      for (int word = 0; word < WORDS; word++) {
        final long bits = words[word];
        if (bits != 0) {
          occupied++;
          unique += Long.bitCount(bits);
        }
      }
      if (Diagnostics.SCAN_DIAGNOSTICS) {
        Diagnostics.RUN_MERGES.increment();
        Diagnostics.CANDIDATE_WORDS_VISITED.add(occupied);
      }
      if (unique >= DENSE_THRESHOLD) {
        if (Diagnostics.SCAN_DIAGNOSTICS) {
          Diagnostics.CANDIDATE_BITMAPS_CREATED.increment();
        }
        chunk.selected = words.clone();
        chunk.first = null;
        chunk.last = null;
      } else {
        // Duplicate references kept the chunk sparse: one union run replaces the list.
        final int unionStart = runKeys.size;
        for (int word = 0; word < WORDS; word++) {
          runKeys.appendWord(base | ((long) word << 6), words[word]);
        }
        chunk.first = new Run(unionStart, runKeys.size);
        chunk.last = chunk.first;
      }
      chunk.count = unique;
      Arrays.fill(words, 0L);
    }

    void removeExactStarts(final long base, final NodeReferences references, final boolean ignored) {
      final Chunk chunk = chunkOrNull(base);
      if (chunk == null || chunk.count == 0) {
        return;
      }
      collect(references);
      try {
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        final long[] selected = chunk.selected;
        if (selected != null) {
          for (int index = 0; index < touchedCount; index++) {
            final int word = touchedWords[index];
            final long removed = selected[word] & scratch[word] & ~unverified.word(word);
            if (Diagnostics.SCAN_DIAGNOSTICS) {
              Diagnostics.CANDIDATE_WORDS_VISITED.increment();
            }
            selected[word] &= ~removed;
            chunk.count -= Long.bitCount(removed);
          }
          return;
        }
        final long[] keys = runKeys.keys;
        for (Run run = chunk.first; run != null; run = run.next) {
          int position = run.position;
          for (int index = run.position; index < run.limit; index++) {
            final long key = keys[index];
            final int word = (int) ((key & 0xFFFFL) >>> 6);
            final long bit = 1L << key;
            if ((scratch[word] & bit) != 0) {
              if (Diagnostics.SCAN_DIAGNOSTICS) {
                Diagnostics.CANDIDATE_WORDS_VISITED.increment();
              }
              if ((unverified.word(word) & bit) == 0) {
                continue;
              }
            }
            keys[position++] = key;
          }
          chunk.count -= run.limit - position;
          run.limit = position;
        }
      } finally {
        resetScratch();
      }
    }

    private void resetScratch() {
      for (int index = 0; index < touchedCount; index++) {
        scratch[touchedWords[index]] = 0;
      }
      touchedCount = 0;
    }

    Evidence evidence() {
      int capacity = 0;
      for (int index = 0; index < chunkCount; index++) {
        capacity = Math.addExact(capacity, chunkValues[index].count);
      }
      final long[] keys = new long[capacity];
      final long[] source = runKeys.keys;
      final Roaring64Bitmap unverifiedKeys = new Roaring64Bitmap();
      int position = 0;
      for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
        final long chunkBase = chunkBases[chunkIndex];
        final Chunk chunk = chunkValues[chunkIndex];
        if (chunk.count == 0) {
          continue;
        }
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        final long[] selected = chunk.selected;
        if (selected != null) {
          for (int word = 0; word < WORDS; word++) {
            long bits = selected[word];
            final long prefix = chunkBase | ((long) word << 6);
            while (bits != 0) {
              keys[position++] = prefix | Long.numberOfTrailingZeros(bits);
              bits &= bits - 1;
            }
            if (!unverified.isEmpty()) {
              long unchecked = selected[word] & unverified.word(word);
              while (unchecked != 0) {
                unverifiedKeys.addLong(prefix | Long.numberOfTrailingZeros(unchecked));
                unchecked &= unchecked - 1;
              }
            }
          }
          continue;
        }
        final Run first = Objects.requireNonNull(chunk.first);
        final int start = position;
        if (first.next == null) {
          final int count = first.limit - first.position;
          System.arraycopy(source, first.position, keys, position, count);
          position += count;
        } else {
          final int groups = mergeRuns(chunk);
          for (int remaining = groups; remaining != 0; remaining &= remaining - 1) {
            final int group = Integer.numberOfTrailingZeros(remaining);
            for (long words = wordGroups[group]; words != 0; words &= words - 1) {
              final int word = (group << 6) | Long.numberOfTrailingZeros(words);
              long bits = scratch[word];
              final long prefix = chunkBase | ((long) word << 6);
              while (bits != 0) {
                keys[position++] = prefix | Long.numberOfTrailingZeros(bits);
                bits &= bits - 1;
              }
            }
          }
          clearWordGroups(groups);
        }
        if (!unverified.isEmpty()) {
          for (int index = start; index < position; index++) {
            markUnverified(keys[index], unverified, unverifiedKeys);
          }
        }
      }
      return new Evidence(position == keys.length
          ? keys
          : Arrays.copyOf(keys, position), unverifiedKeys);
    }

    private int mergeRuns(final Chunk chunk) {
      int groups = 0;
      for (Run run = chunk.first; run != null; run = run.next) {
        if (Diagnostics.SCAN_DIAGNOSTICS) {
          Diagnostics.RUN_KEYS_MERGED.add(run.limit - run.position);
        }
        groups = mergeRunWords(run, runKeys.keys, wordGroups, groups);
      }
      if (Diagnostics.SCAN_DIAGNOSTICS) {
        Diagnostics.RUN_MERGES.increment();
        for (int remaining = groups; remaining != 0; remaining &= remaining - 1) {
          Diagnostics.CANDIDATE_WORDS_VISITED.add(Long.bitCount(wordGroups[Integer.numberOfTrailingZeros(remaining)]));
        }
      }
      return groups;
    }

    private void emitWords(final long base, final int groups) {
      for (int remaining = groups; remaining != 0; remaining &= remaining - 1) {
        final int group = Integer.numberOfTrailingZeros(remaining);
        for (long words = wordGroups[group]; words != 0; words &= words - 1) {
          final int word = (group << 6) | Long.numberOfTrailingZeros(words);
          runKeys.appendWord(base | ((long) word << 6), scratch[word]);
        }
      }
    }

    private void clearWordGroups(final int groups) {
      for (int remaining = groups; remaining != 0; remaining &= remaining - 1) {
        final int group = Integer.numberOfTrailingZeros(remaining);
        for (long words = wordGroups[group]; words != 0; words &= words - 1) {
          scratch[(group << 6) | Long.numberOfTrailingZeros(words)] = 0;
        }
        wordGroups[group] = 0;
      }
    }

    private int mergeRunWords(final Run run, final long[] source, final long[] wordGroups, int groups) {
      int position = run.position;
      while (position < run.limit) {
        final long key = source[position++];
        final int word = (int) ((key & 0xFFFFL) >>> 6);
        long bits = 1L << key;
        while (position < run.limit) {
          final long next = source[position];
          if ((int) ((next & 0xFFFFL) >>> 6) != word) {
            break;
          }
          bits |= 1L << next;
          position++;
        }
        final long previous = scratch[word];
        if (previous == 0) {
          final int group = word >>> 6;
          wordGroups[group] |= 1L << word;
          groups |= 1 << group;
        }
        scratch[word] = previous | bits;
      }
      return groups;
    }
  }

  private static void markUnverified(final long key, final PostingMask unverified, final Roaring64Bitmap keys) {
    if (Diagnostics.SCAN_DIAGNOSTICS) {
      Diagnostics.CANDIDATE_WORDS_VISITED.increment();
    }
    if ((unverified.word((int) ((key & 0xFFFFL) >>> 6)) & (1L << key)) != 0) {
      keys.addLong(key);
    }
  }

  private static @Nullable IndexDef findValidTimeIndex(final JsonDBItem document) {
    final var trx = document.getTrx();
    final JsonIndexController controller = trx.getResourceSession().getRtxIndexController(trx.getRevisionNumber());
    if (controller != null) {
      for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
        if (indexDef.isValidTimeIndex()) {
          return indexDef;
        }
      }
    }
    return null;
  }
}

package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.StorageEngineWriter;
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
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import io.sirix.index.interval.PostingChunkConsumer;
import io.sirix.index.interval.PostingMask;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

public final class ValidTimeIntervalIndex {
  private static final class Diagnostics {
    private static final boolean SCAN_DIAGNOSTICS = HotOrderedStore.scanDiagnosticsEnabled();
    private static final LongAdder CANDIDATE_WORDS_VISITED = new LongAdder();
    private static final LongAdder CANDIDATE_BITMAPS_CREATED = new LongAdder();
  }

  public static long candidateWordsVisited() {
    return Diagnostics.CANDIDATE_WORDS_VISITED.sum();
  }

  public static long candidateBitmapsCreated() {
    return Diagnostics.CANDIDATE_BITMAPS_CREATED.sum();
  }

  @SuppressWarnings("ArrayRecordComponent")
  record Evidence(long[] keys, Roaring64Bitmap unverified) {
  }

  private ValidTimeIntervalIndex() {}

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
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, null, definition.getID());
  }

  public static long[] keys(final JsonDBItem document, final Instant instant, final boolean strictEnd) {
    Objects.requireNonNull(document);
    final ValidTimeConfig config =
        Objects.requireNonNull(document.getResourceSession().getResourceConfig().getValidTimeConfig());
    final Sequence sequence = sequence(document, instant, config, false, strictEnd);
    if (sequence != null) {
      return ((ValidTimeKeySequence) sequence).matchingKeys();
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

  /** Intersect compressed runs before expansion; every candidate is emitted once in key order. */
  static Evidence candidates(final JsonDBItem document, final Instant instant, final boolean strictStart,
      final boolean strictEnd, final int indexId) {
    final IntervalDomain domain = new IntervalDomain();
    final var reader = document.getTrx().getStorageEngineReader();
    final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader, indexId, domain);
    final ChunkCandidates candidates = new ChunkCandidates(document, indexId);
    final long point = domain.point(instant);
    final boolean exact = domain.isExact(instant);
    tree.stabChunks(point, exact && strictEnd, candidates);
    if (exact && strictStart && candidates.hasCandidates) {
      tree.startingAtChunks(point, candidates::removeExactStarts);
    }
    return candidates.evidence();
  }

  static final class ChunkCandidates implements PostingChunkConsumer, LongConsumer {
    private static final int WORDS = 1024;
    // An eight-byte run key and an eight-byte bitmap word break even at 1024 matches.
    private static final int DENSE_THRESHOLD = WORDS;
    private final LongFunction<PostingMask> membership;
    private final LongFunction<PostingMask> verification;
    private final Long2ObjectAVLTreeMap<Chunk> chunks = new Long2ObjectAVLTreeMap<>();
    private final RunBuffer runKeys = new RunBuffer();
    private final long[] scratch = new long[WORDS];
    private final int[] touchedWords = new int[WORDS];
    private int touchedCount;
    private boolean hasCandidates;
    private final long[] wordGroups = new long[WORDS / Long.SIZE];

    private static final class RunBuffer {
      private static final long[] EMPTY = new long[0];
      private long[] keys = EMPTY;
      private int size;

      private void appendWord(final long prefix, long bits) {
        if (bits == 0) {
          return;
        }
        final int required = Math.addExact(size, Long.bitCount(bits));
        if (required > keys.length) {
          final long capacity = Math.max(16L, Math.max(required, (long) keys.length + (keys.length >> 1)));
          final int boundedCapacity = (int) Math.min(Integer.MAX_VALUE - 8L, capacity);
          if (boundedCapacity < required) {
            throw new OutOfMemoryError("Valid-time run buffer exceeds the maximum array size");
          }
          keys = Arrays.copyOf(keys, boundedCapacity);
        }
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

    private ChunkCandidates(final JsonDBItem document, final int indexId) {
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
      final HotOrderedStore unverified = ValidTimeIntervalIndexFactory.createVerificationStore(reader, indexId);
      verification = base -> unverified.chunkMask(0, 0, base);
    }

    ChunkCandidates(final LongFunction<PostingMask> membership, final LongFunction<PostingMask> verification) {
      this.membership = Objects.requireNonNull(membership);
      this.verification = Objects.requireNonNull(verification);
    }

    @Override
    public void accept(final long key) {
      final int word = (int) (key >>> 6);
      if (scratch[word] == 0) {
        touchedWords[touchedCount++] = word;
      }
      scratch[word] |= 1L << key;
    }

    private Chunk chunk(final long base) {
      Chunk chunk = chunks.get(base);
      if (chunk != null) {
        return chunk;
      }
      chunk = new Chunk(membership.apply(base));
      chunks.put(base, chunk);
      return chunk;
    }

    @Override
    public void accept(final long base, final NodeReferences references, final boolean verifyOnly) {
      if (!references.hasNodeKeys()) {
        return;
      }
      final Chunk chunk = chunk(base);
      if (chunk.membership.isEmpty()) {
        return;
      }
      references.forEachNodeKey(this);
      try {
        if (Diagnostics.SCAN_DIAGNOSTICS) {
          Diagnostics.CANDIDATE_WORDS_VISITED.add(touchedCount);
        }
        int count = 0;
        final long[] selected = chunk.selected;
        for (int index = 0; index < touchedCount; index++) {
          final int word = touchedWords[index];
          long bits = scratch[word] & chunk.membership.word(word);
          if (bits != 0) {
            if (chunk.unverified == null) {
              chunk.unverified = verification.apply(base);
            }
            if (verifyOnly) {
              if (Diagnostics.SCAN_DIAGNOSTICS) {
                Diagnostics.CANDIDATE_WORDS_VISITED.increment();
              }
              bits &= chunk.unverified.word(word);
            }
          }
          scratch[word] = bits;
          if (selected == null) {
            count += Long.bitCount(bits);
          } else {
            count += Long.bitCount(bits & ~selected[word]);
            selected[word] |= bits;
          }
        }
        if (count == 0) {
          return;
        }
        hasCandidates = true;
        if (selected != null) {
          chunk.count += count;
          return;
        }
        if (chunk.first == null && count >= DENSE_THRESHOLD) {
          if (Diagnostics.SCAN_DIAGNOSTICS) {
            Diagnostics.CANDIDATE_BITMAPS_CREATED.increment();
          }
          chunk.selected = scratch.clone();
          chunk.count = count;
          return;
        }
        final int start = runKeys.size;
        for (int index = 0; index < touchedCount; index++) {
          final int word = touchedWords[index];
          runKeys.appendWord(base | ((long) word << 6), scratch[word]);
        }
        final Run run = new Run(start, runKeys.size);
        if (chunk.last == null) {
          chunk.first = run;
        } else {
          chunk.last.next = run;
        }
        chunk.last = run;
        chunk.count += count;
        if (chunk.count >= DENSE_THRESHOLD) {
          resetScratch();
          final int groups = mergeRuns(chunk);
          int unique = 0;
          for (int remaining = groups; remaining != 0; remaining &= remaining - 1) {
            final int group = Integer.numberOfTrailingZeros(remaining);
            for (long words = wordGroups[group]; words != 0; words &= words - 1) {
              final int word = (group << 6) | Long.numberOfTrailingZeros(words);
              unique += Long.bitCount(scratch[word]);
            }
          }
          if (unique >= DENSE_THRESHOLD) {
            if (Diagnostics.SCAN_DIAGNOSTICS) {
              Diagnostics.CANDIDATE_BITMAPS_CREATED.increment();
            }
            chunk.selected = scratch.clone();
            chunk.first = null;
            chunk.last = null;
          } else {
            final int unionStart = runKeys.size;
            emitWords(base, groups);
            chunk.first = new Run(unionStart, runKeys.size);
            chunk.last = chunk.first;
          }
          chunk.count = unique;
          clearWordGroups(groups);
        }
      } finally {
        resetScratch();
      }
    }

    void removeExactStarts(final long base, final NodeReferences references, final boolean ignored) {
      final Chunk chunk = chunks.get(base);
      if (chunk == null || chunk.count == 0) {
        return;
      }
      references.forEachNodeKey(this);
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
      for (final Chunk chunk : chunks.values()) {
        capacity = Math.addExact(capacity, chunk.count);
      }
      final long[] keys = new long[capacity];
      final long[] source = runKeys.keys;
      final Roaring64Bitmap unverifiedKeys = new Roaring64Bitmap();
      int position = 0;
      for (final var entry : chunks.long2ObjectEntrySet()) {
        final Chunk chunk = entry.getValue();
        if (chunk.count == 0) {
          continue;
        }
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        final long[] selected = chunk.selected;
        if (selected != null) {
          for (int word = 0; word < WORDS; word++) {
            long bits = selected[word];
            final long prefix = entry.getLongKey() | ((long) word << 6);
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
              final long prefix = entry.getLongKey() | ((long) word << 6);
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
      return new Evidence(position == keys.length ? keys : Arrays.copyOf(keys, position), unverifiedKeys);
    }

    private int mergeRuns(final Chunk chunk) {
      int groups = 0;
      for (Run run = chunk.first; run != null; run = run.next) {
        groups = mergeRunWords(run, runKeys.keys, wordGroups, groups);
      }
      if (Diagnostics.SCAN_DIAGNOSTICS) {
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

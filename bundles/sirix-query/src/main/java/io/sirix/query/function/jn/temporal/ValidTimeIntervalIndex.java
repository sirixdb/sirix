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
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

public final class ValidTimeIntervalIndex {
  private static final boolean SCAN_DIAGNOSTICS = HotOrderedStore.scanDiagnosticsEnabled();
  private static final LongAdder CANDIDATE_WORDS_VISITED = new LongAdder();

  public static long candidateWordsVisited() {
    return CANDIDATE_WORDS_VISITED.sum();
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
    if (exact && strictStart && !candidates.runKeys.isEmpty()) {
      tree.startingAtChunks(point, candidates::removeExactStarts);
    }
    return candidates.evidence();
  }

  static final class ChunkCandidates implements PostingChunkConsumer, LongConsumer {
    private static final int WORDS = 1024;
    private final LongFunction<PostingMask> membership;
    private final LongFunction<PostingMask> verification;
    private final Long2ObjectAVLTreeMap<Chunk> chunks = new Long2ObjectAVLTreeMap<>();
    private final LongArrayList runKeys = new LongArrayList();
    private final long[] scratch = new long[WORDS];
    private final int[] touchedWords = new int[WORDS];
    private int touchedCount;

    private static final class Run {
      private int position;
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
        final int start = runKeys.size();
        if (SCAN_DIAGNOSTICS) {
          CANDIDATE_WORDS_VISITED.add(touchedCount);
        }
        for (int index = 0; index < touchedCount; index++) {
          final int word = touchedWords[index];
          final long memberBits = scratch[word] & chunk.membership.word(word);
          if (memberBits == 0) {
            continue;
          }
          if (chunk.unverified == null) {
            chunk.unverified = verification.apply(base);
          }
          if (SCAN_DIAGNOSTICS && verifyOnly) {
            CANDIDATE_WORDS_VISITED.increment();
          }
          final long selectedBits = verifyOnly
              ? memberBits & chunk.unverified.word(word)
              : memberBits;
          long bits = selectedBits;
          final long prefix = base | ((long) word << 6);
          while (bits != 0) {
            runKeys.add(prefix | Long.numberOfTrailingZeros(bits));
            bits &= bits - 1;
          }
        }
        if (runKeys.size() != start) {
          final Run run = new Run(start, runKeys.size());
          if (chunk.last == null) {
            chunk.first = run;
          } else {
            chunk.last.next = run;
          }
          chunk.last = run;
        }
      } finally {
        resetScratch();
      }
    }

    void removeExactStarts(final long base, final NodeReferences references, final boolean ignored) {
      final Chunk chunk = chunks.get(base);
      if (chunk == null || chunk.first == null) {
        return;
      }
      references.forEachNodeKey(this);
      try {
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        final long[] keys = runKeys.elements();
        for (Run run = chunk.first; run != null; run = run.next) {
          int position = run.position;
          for (int index = run.position; index < run.limit; index++) {
            final long key = keys[index];
            final int word = (int) ((key & 0xFFFFL) >>> 6);
            final long bit = 1L << key;
            if ((scratch[word] & bit) != 0) {
              if (SCAN_DIAGNOSTICS) {
                CANDIDATE_WORDS_VISITED.increment();
              }
              if ((unverified.word(word) & bit) == 0) {
                continue;
              }
            }
            keys[position++] = key;
          }
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
      final long[] keys = new long[runKeys.size()];
      final long[] source = runKeys.elements();
      final Roaring64Bitmap unverifiedKeys = new Roaring64Bitmap();
      ObjectHeapPriorityQueue<Run> runs = null;
      int position = 0;
      for (final Chunk chunk : chunks.values()) {
        final Run first = chunk.first;
        if (first == null) {
          continue;
        }
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        if (first.next == null) {
          final int count = first.limit - first.position;
          System.arraycopy(source, first.position, keys, position, count);
          if (!unverified.isEmpty()) {
            for (int index = first.position; index < first.limit; index++) {
              markUnverified(source[index], unverified, unverifiedKeys);
            }
          }
          position += count;
          continue;
        }
        if (runs == null) {
          runs = new ObjectHeapPriorityQueue<>(
              (left, right) -> Long.compare(source[left.position], source[right.position]));
        }
        for (Run run = first; run != null; run = run.next) {
          if (run.position < run.limit) {
            runs.enqueue(run);
          }
        }
        long previous = -1;
        while (!runs.isEmpty()) {
          final Run run = runs.dequeue();
          final long key = source[run.position++];
          if (key != previous) {
            keys[position++] = key;
            if (!unverified.isEmpty()) {
              markUnverified(key, unverified, unverifiedKeys);
            }
            previous = key;
          }
          if (run.position < run.limit) {
            runs.enqueue(run);
          }
        }
      }
      return new Evidence(position == keys.length ? keys : Arrays.copyOf(keys, position), unverifiedKeys);
    }
  }

  private static void markUnverified(final long key, final PostingMask unverified, final Roaring64Bitmap keys) {
    if (SCAN_DIAGNOSTICS) {
      CANDIDATE_WORDS_VISITED.increment();
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

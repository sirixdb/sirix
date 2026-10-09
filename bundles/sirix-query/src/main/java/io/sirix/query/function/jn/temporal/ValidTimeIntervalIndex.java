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
import java.util.function.Supplier;
import java.util.function.LongConsumer;

public final class ValidTimeIntervalIndex {

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

  private static final class ChunkCandidates implements PostingChunkConsumer, LongConsumer {
    private static final int WORDS = 1024;
    private final JsonDBItem document;
    private final @Nullable HotOrderedStore members;
    private final HotOrderedStore verification;
    private final Long2ObjectAVLTreeMap<Chunk> chunks = new Long2ObjectAVLTreeMap<>();
    private final long[] scratch = new long[WORDS];
    private final int[] touchedWords = new int[WORDS];
    private int touchedCount;
    private boolean hasCandidates;

    private static final class Chunk {
      private final PostingMask membership;
      private long @Nullable [] selected;
      private @Nullable PostingMask unverified;

      private Chunk(final PostingMask membership) {
        this.membership = membership;
      }
    }

    private ChunkCandidates(final JsonDBItem document, final int indexId) {
      this.document = document;
      final var reader = document.getTrx().getStorageEngineReader();
      members = document instanceof Array
          ? ValidTimeIntervalIndexFactory.createMembershipStore(reader, indexId)
          : null;
      verification = ValidTimeIntervalIndexFactory.createVerificationStore(reader, indexId);
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
      final PostingMask membership;
      if (members != null) {
        membership = members.chunkMask(document.getNodeKey(), 0, base);
      } else {
        final long key = document.getNodeKey();
        membership = (key & ~0xFFFFL) == base
            ? PostingMask.from(NodeReferences.ofSortedArray(new long[] {key & 0xFFFFL}))
            : PostingMask.from(null);
      }
      chunk = new Chunk(membership);
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
        for (int index = 0; index < touchedCount; index++) {
          final int word = touchedWords[index];
          final long memberBits = scratch[word] & chunk.membership.word(word);
          if (memberBits == 0) {
            continue;
          }
          if (chunk.unverified == null) {
            chunk.unverified = verification.chunkMask(0, 0, base);
          }
          final long selectedBits = verifyOnly
              ? memberBits & chunk.unverified.word(word)
              : memberBits;
          if (selectedBits != 0) {
            if (chunk.selected == null) {
              chunk.selected = new long[WORDS];
            }
            chunk.selected[word] |= selectedBits;
            hasCandidates = true;
          }
        }
      } finally {
        resetScratch();
      }
    }

    private void removeExactStarts(final long base, final NodeReferences references, final boolean ignored) {
      final Chunk chunk = chunks.get(base);
      if (chunk == null || chunk.selected == null) {
        return;
      }
      references.forEachNodeKey(this);
      try {
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        for (int index = 0; index < touchedCount; index++) {
          final int word = touchedWords[index];
          chunk.selected[word] &= ~(scratch[word] & ~unverified.word(word));
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

    private Evidence evidence() {
      long cardinality = 0;
      for (final Chunk chunk : chunks.values()) {
        if (chunk.selected != null) {
          for (final long bits : chunk.selected) {
            cardinality += Long.bitCount(bits);
          }
        }
      }
      final long[] keys = new long[Math.toIntExact(cardinality)];
      final Roaring64Bitmap unverifiedKeys = new Roaring64Bitmap();
      int position = 0;
      for (final var entry : chunks.long2ObjectEntrySet()) {
        final Chunk chunk = entry.getValue();
        if (chunk.selected == null) {
          continue;
        }
        final long base = entry.getLongKey();
        final PostingMask unverified = Objects.requireNonNull(chunk.unverified);
        position = emitChunk(base, chunk.selected, keys, position);
        if (!unverified.isEmpty()) {
          for (int word = 0; word < WORDS; word++) {
            long verify = chunk.selected[word] & unverified.word(word);
            while (verify != 0) {
              unverifiedKeys.addLong(base | ((long) word << 6) | Long.numberOfTrailingZeros(verify));
              verify &= verify - 1;
            }
          }
        }
      }
      return new Evidence(keys, unverifiedKeys);
    }
  }

  private static int emitChunk(final long base, final long[] words, final long[] keys, final int offset) {
    int position = offset;
    for (int word = 0; word < words.length; word++) {
      long bits = words[word];
      final long prefix = base | ((long) word << 6);
      while (bits != 0) {
        keys[position++] = prefix | Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
      }
    }
    return position;
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

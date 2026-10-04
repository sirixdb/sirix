package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.HotOrderedStore;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;
import io.sirix.query.function.DateTimeToInstant;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class ValidTimeIntervalIndex {

  @SuppressWarnings("ArrayRecordComponent")
  record Evidence(long[] members, LongOpenHashSet unverified) {
  }

  private ValidTimeIntervalIndex() {}

  public static @Nullable Sequence sequence(final JsonDBItem document, final Instant instant,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd,
      final @Nullable Predicate<? super JsonDBObject> residual) {
    Objects.requireNonNull(document);
    Objects.requireNonNull(instant);
    Objects.requireNonNull(config);
    final IndexDef definition = findValidTimeIndex(document);
    if (definition == null) {
      return null;
    }
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, residual, definition.getID());
  }

  public static @Nullable Sequence comparisonSequence(final JsonDBItem document, final Supplier<Sequence> point,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd) {
    if (!(document instanceof Array array) || array.len() == 0) {
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
    final Sequence sequence = sequence(document, instant,
        document.getResourceSession().getResourceConfig().getValidTimeConfig(), false, strictEnd, null);
    if (sequence == null) {
      throw new IllegalStateException("No valid-time index at revision " + document.getTrx().getRevisionNumber());
    }
    return ((ValidTimeKeySequence) sequence).matchingKeys();
  }

  static Evidence readEvidence(final JsonDBItem document, final int indexId, final LongOpenHashSet closed) {
    final var reader = document.getTrx().getStorageEngineReader();
    final HotOrderedStore members = document instanceof Array
        ? ValidTimeIntervalIndexFactory.createMembershipStore(reader, indexId)
        : null;
    final HotOrderedStore verification = ValidTimeIntervalIndexFactory.createVerificationStore(reader, indexId);
    final long[] sorted = closed.toLongArray();
    Arrays.sort(sorted);
    final LongOpenHashSet unverified = new LongOpenHashSet();
    NodeReferences memberChunk = null;
    NodeReferences verificationChunk = null;
    long chunk = -1;
    int count = 0;
    for (final long key : sorted) {
      if ((key >>> 16) != chunk) {
        chunk = key >>> 16;
        if (members != null) {
          memberChunk = members.chunk(document.getNodeKey(), 0, key);
        }
        verificationChunk = verification.chunk(0, 0, key);
      }
      if (members == null
          ? key == document.getNodeKey()
          : memberChunk != null && memberChunk.contains(key & 0xFFFFL)) {
        sorted[count++] = key;
        if (verificationChunk != null && verificationChunk.contains(key & 0xFFFFL)) {
          unverified.add(key);
        }
      }
    }
    return new Evidence(count == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, count), unverified);
  }

  static LongOpenHashSet closedCandidates(final JsonDBItem document, final Instant instant, final int indexId) {
    final IntervalDomain domain = new IntervalDomain();
    final var tree =
        ValidTimeIntervalIndexFactory.createReaderTree(document.getTrx().getStorageEngineReader(), indexId, domain);
    final LongOpenHashSet closed = new LongOpenHashSet();
    tree.stab(domain.point(instant), closed::add);
    return closed;
  }

  static long[] candidates(final JsonDBItem document, final Instant instant, final boolean strictStart,
      final boolean strictEnd, final int indexId, final Evidence evidence, final LongOpenHashSet closed) {
    final IntervalDomain domain = new IntervalDomain();
    final long point = domain.point(instant);
    final boolean exactPoint = domain.isExact(instant);
    final LongOpenHashSet candidates = exactPoint && strictEnd
        ? new LongOpenHashSet()
        : closed;
    if (exactPoint && (strictStart || strictEnd)) {
      final var tree =
          ValidTimeIntervalIndexFactory.createReaderTree(document.getTrx().getStorageEngineReader(), indexId, domain);
      if (strictEnd) {
        tree.stabHalfOpen(point, candidates::add);
      }
      if (strictStart) {
        tree.startingAt(point, key -> {
          if (!evidence.unverified().contains(key)) {
            candidates.remove(key);
          }
        });
      }
      if (strictEnd && !evidence.unverified().isEmpty()) {
        final var keys = closed.iterator();
        while (keys.hasNext()) {
          final long key = keys.nextLong();
          if (evidence.unverified().contains(key)) {
            candidates.add(key);
          }
        }
      }
    }
    final long[] sorted = candidates.toLongArray();
    Arrays.sort(sorted);
    return matchingMembers(sorted, evidence.members());
  }

  private static long[] matchingMembers(final long[] sorted, final long[] members) {
    int memberIndex = 0;
    int matchCount = 0;
    for (final long key : sorted) {
      while (memberIndex < members.length && members[memberIndex] < key) {
        memberIndex++;
      }
      if (memberIndex < members.length && members[memberIndex] == key) {
        sorted[matchCount++] = key;
      }
    }
    return matchCount == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, matchCount);
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

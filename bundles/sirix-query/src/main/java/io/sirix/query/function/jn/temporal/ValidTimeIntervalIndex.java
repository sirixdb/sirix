package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.QueryException;
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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

public final class ValidTimeIntervalIndex {

  @SuppressWarnings("ArrayRecordComponent")
  record Evidence(long[] members, LongOpenHashSet unverified) {
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
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, null, definition.getID());
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
    if (!(upperValue instanceof DateTime upper) || upper.getTimezone() == null
        || !(lowerValue instanceof DateTime lower) || lower.getTimezone() == null) {
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
    final long maximum = document.getTrx().getMaxNodeKey();
    if (maximum >= 0 && maximum <= ValidTimeRangeKeys.MAXIMUM_KEY && maximum <= Math.max(65536L, array.len() * 32L)) {
      // At most 2 MiB, and only a compact resource domain. Sparse/large resources retain the
      // output-sized collector below. The bitmap removes ties directly and emits sorted keys.
      final ValidTimeRangeKeys keys = new ValidTimeRangeKeys((int) maximum);
      tree.intersect(domain.point(lo), domain.point(hi), keys);
      if (strictStart) {
        tree.startingAt(domain.point(hi), keys::remove);
      }
      if (strictEnd) {
        tree.endingAt(domain.point(lo), keys::remove);
      }
      final long[] matches = readEvidence(document, definition.getID(), keys.sortedKeys(), false).members();
      ValidTimeRangeCache.put(document.getResourceSession(), controller, revision, arrayKey, definition.getID(),
          lowerPoint, upperPoint, mode, matches);
      return ValidTimeKeySequence.exactKeys(document, lo, config, definition.getID(), matches);
    }
    final LongArrayList keys = new LongArrayList();
    tree.intersect(domain.point(lo), domain.point(hi), keys::add);
    final LongArrayList ties = new LongArrayList(0);
    if (strictStart) {
      tree.startingAt(domain.point(hi), ties::add);
    }
    if (strictEnd) {
      tree.endingAt(domain.point(lo), ties::add);
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
    // Exact cohort admission already proved every member's timestamp representation. Only
    // membership remains: the same interval index can also contain nested or sibling arrays.
    final long[] matches = readEvidence(document, definition.getID(), count == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, count), false).members();
    ValidTimeRangeCache.put(document.getResourceSession(), controller, revision, arrayKey, definition.getID(),
        lowerPoint, upperPoint, mode, matches);
    return ValidTimeKeySequence.exactKeys(document, lo, config, definition.getID(), matches);
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
        final IntervalDomain domain = new IntervalDomain();
        final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader, exactCohort.getID(), domain);
        final LongOpenHashSet keys = new LongOpenHashSet();
        tree.stabHalfOpen(domain.point(instant), keys::add);
        return readEvidence(document, exactCohort.getID(), keys).members();
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

  static Evidence readEvidence(final JsonDBItem document, final int indexId, final LongOpenHashSet closed) {
    final long[] sorted = closed.toLongArray();
    Arrays.sort(sorted);
    return readEvidence(document, indexId, sorted, true);
  }

  private static Evidence readEvidence(final JsonDBItem document, final int indexId, final long[] sorted,
      final boolean verify) {
    final var reader = document.getTrx().getStorageEngineReader();
    final HotOrderedStore members = document instanceof Array
        ? ValidTimeIntervalIndexFactory.createMembershipStore(reader, indexId)
        : null;
    final HotOrderedStore verification = verify
        ? ValidTimeIntervalIndexFactory.createVerificationStore(reader, indexId)
        : null;
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
        if (verification != null) {
          verificationChunk = verification.chunk(0, 0, key);
        }
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

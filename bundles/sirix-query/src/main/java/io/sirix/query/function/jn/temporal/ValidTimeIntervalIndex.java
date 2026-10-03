package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Revision-bound valid-time key scans. Every record carrying postings is registered in the RI-tree,
 * so a stab is the only candidate source; persisted membership postings restrict the result to the
 * document object or direct array members. Rounded/clamped/open/ambiguous bounds are re-checked
 * with the original exact predicate, and only a strict endpoint unions the records needing
 * verification, because a rounded endpoint can tie with the query millisecond.
 *
 * <p>
 * The lazy sequence resolves keys on demand and constructs only requested JSON objects.
 * </p>
 */
public final class ValidTimeIntervalIndex {

  private ValidTimeIntervalIndex() {}

  /** Return a lazy key-backed sequence, or null when no interval index exists at this revision. */
  public static @Nullable Sequence sequence(final JsonDBItem document, final Instant instant,
      final ValidTimeConfig config, final boolean strictStart, final boolean strictEnd,
      final Predicate<JsonDBObject> residual) {
    Objects.requireNonNull(document);
    Objects.requireNonNull(instant);
    Objects.requireNonNull(config);
    final var trx = document.getTrx();
    final JsonIndexController controller = trx.getResourceSession().getRtxIndexController(trx.getRevisionNumber());
    if (controller == null || findValidTimeIndex(controller) == null) {
      return null;
    }
    return new ValidTimeKeySequence(document, instant, config, strictStart, strictEnd, residual);
  }

  /**
   * A FLWOR cast predicate is index-only only when every array member has two exact, unique,
   * parseable bounds. Otherwise its original scan must run: malformed casts on non-candidates can
   * raise errors, and missing fields have different semantics from open-ended intervals.
   */
  public static boolean hasExactArrayBounds(final JsonDBItem document, final Instant instant) {
    if (!(document instanceof Array array) || !new IntervalDomain().isExact(instant)) {
      return false;
    }
    final var trx = document.getTrx();
    final JsonIndexController controller = trx.getResourceSession().getRtxIndexController(trx.getRevisionNumber());
    final IndexDef definition = controller == null
        ? null
        : findValidTimeIndex(controller);
    if (definition == null) {
      return false;
    }
    final LongOpenHashSet members = new LongOpenHashSet();
    ValidTimeIntervalIndexFactory.createMembershipStore(trx.getStorageEngineReader(), definition.getID())
                                 .scan(document.getNodeKey(), 0, 0, members::add);
    if (members.size() != array.len()) {
      return false;
    }
    final LongArrayList unordered = new LongArrayList(1);
    ValidTimeIntervalIndexFactory.createOrderStore(trx.getStorageEngineReader(), definition.getID())
                                 .scan(document.getNodeKey(), 0, 0, unordered::add);
    if (!unordered.isEmpty()) {
      return false;
    }
    final int size = members.size();
    ValidTimeIntervalIndexFactory.createVerificationStore(trx.getStorageEngineReader(), definition.getID())
                                 .scan(0, 0, 0, members::remove);
    return members.size() == size;
  }

  /** Sorted matching object keys; no JSON wrappers are created for exact millisecond intervals. */
  public static long[] keys(final JsonDBItem document, final Instant instant, final boolean strictEnd) {
    return keys(document, instant, document.getResourceSession().getResourceConfig().getValidTimeConfig(), false,
        strictEnd, null);
  }

  static long[] keys(final JsonDBItem document, final Instant instant, final ValidTimeConfig config,
      final boolean strictStart, final boolean strictEnd, final Predicate<JsonDBObject> residual) {
    Objects.requireNonNull(document);
    Objects.requireNonNull(instant);
    Objects.requireNonNull(config);
    final var trx = document.getTrx();
    final JsonIndexController controller = trx.getResourceSession().getRtxIndexController(trx.getRevisionNumber());
    final IndexDef definition = controller == null
        ? null
        : findValidTimeIndex(controller);
    if (definition == null) {
      throw new IllegalStateException("No valid-time index at revision " + trx.getRevisionNumber());
    }
    final IntervalDomain domain = new IntervalDomain();
    final var tree =
        ValidTimeIntervalIndexFactory.createReaderTree(trx.getStorageEngineReader(), definition.getID(), domain);
    final LongOpenHashSet unverified = new LongOpenHashSet();
    ValidTimeIntervalIndexFactory.createVerificationStore(trx.getStorageEngineReader(), definition.getID())
                                 .scan(0, 0, 0, unverified::add);
    final LongOpenHashSet candidates = new LongOpenHashSet();
    final long point = domain.point(instant);
    final boolean exactPoint = domain.isExact(instant);
    if (strictEnd && exactPoint) {
      tree.stabHalfOpen(point, candidates::add);
    } else {
      tree.stab(point, candidates::add);
    }
    if (strictStart && exactPoint) {
      tree.startingAt(point, candidates::remove);
    }
    if (exactPoint && (strictStart || strictEnd)) {
      // A rounded endpoint equal to the query millisecond may actually lie outside the query's
      // millisecond, so a strict integer comparison can both skip such a record (half-open stab)
      // and drop one (startingAt). The closed stab needs no union: the domain map is monotonic and
      // every record carrying postings is registered, so it already yields a superset.
      candidates.addAll(unverified);
    }
    final long[] sorted = candidates.toLongArray();
    Arrays.sort(sorted);
    int matchCount = 0;
    final long savedKey = trx.getNodeKey();
    final long itemKey = document.getNodeKey();
    final LongOpenHashSet members = new LongOpenHashSet();
    if (document instanceof Array) {
      ValidTimeIntervalIndexFactory.createMembershipStore(trx.getStorageEngineReader(), definition.getID())
                                   .scan(itemKey, 0, 0, members::add);
    } else {
      members.add(itemKey);
    }
    try {
      for (final long key : sorted) {
        if (!members.contains(key)) {
          continue;
        }
        if (!exactPoint || unverified.contains(key)) {
          if (!trx.moveTo(key) || !trx.isObject()) {
            throw new IllegalStateException("Valid-time candidate disappeared: " + key);
          }
          final JsonDBObject object = new JsonDBObject(trx, document.getCollection());
          if (!ValidTimeIndexScan.isValidAtTime(object, instant, config.getNormalizedValidFromPath(),
              config.getNormalizedValidToPath(), residual == null && strictStart, residual == null && strictEnd)
              || residual != null && !residual.test(object)) {
            continue;
          }
        }
        sorted[matchCount++] = key;
      }
    } finally {
      if (trx.getNodeKey() != savedKey) {
        trx.moveTo(savedKey);
      }
    }
    return matchCount == sorted.length
        ? sorted
        : Arrays.copyOf(sorted, matchCount);
  }

  /** Find a VALIDTIME interval index in the controller, or {@code null}. */
  private static @Nullable IndexDef findValidTimeIndex(final JsonIndexController controller) {
    for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
      if (indexDef.isValidTimeIndex()) {
        return indexDef;
      }
    }
    return null;
  }
}

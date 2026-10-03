package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

/**
 * Revision-bound valid-time key scans. Exact millisecond intervals are answered by the RI-tree;
 * persisted membership postings restrict the result to the document object or direct array members.
 * Rounded/clamped/open/ambiguous bounds use the original exact predicate. Unverified postings are
 * unioned before verification because a half-open stab may omit a rounded upper-bound tie.
 *
 * <p>
 * The lazy sequence resolves keys on demand and constructs only requested JSON objects. The eager
 * {@link #tryIndexScan} interface remains available for diagnostic/differential callers.
 * </p>
 */
public final class ValidTimeIntervalIndex {

  private ValidTimeIntervalIndex() {}

  /** The verified matching records, de-duplicated, in ascending node-key order. */
  public static final class Result {
    private final List<JsonDBItem> items;
    private final long candidatesExamined;

    Result(final List<JsonDBItem> items, final long candidatesExamined) {
      this.items = items;
      this.candidatesExamined = candidatesExamined;
    }

    public List<JsonDBItem> items() {
      return items;
    }

    /** Number of distinct candidate object keys the stab produced before final verification. */
    public long candidatesExamined() {
      return candidatesExamined;
    }
  }

  /**
   * Try to evaluate the valid-time point-in-time predicate via the interval index.
   *
   * @param document the document item (anchored at the top-level array/object node)
   * @param validTime the point in valid time to test
   * @param validTimeConfig the resource's valid-time configuration
   * @return a {@link Result} when a VALIDTIME interval index exists and was used, or {@code null}
   *         when the caller should fall back to the CAS-narrowing path or linear scan
   */
  public static @Nullable Result tryIndexScan(final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    if (document == null || validTimeConfig == null) {
      return null;
    }

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());
    if (controller == null) {
      return null;
    }

    final IndexDef intervalIndex = findValidTimeIndex(controller);
    if (intervalIndex == null) {
      return null;
    }

    final JsonDBCollection collection = document.getCollection();
    final String validFromField = validTimeConfig.getNormalizedValidFromPath();
    final String validToField = validTimeConfig.getNormalizedValidToPath();

    final IntervalDomain domain = new IntervalDomain();
    final RelationalIntervalTree tree =
        ValidTimeIntervalIndexFactory.createReaderTree(rtx.getStorageEngineReader(), intervalIndex.getID(), domain);

    // The node the linear scan treats as the document item: the first child of the document root.
    final long documentItemKey = topLevelItemNodeKey(rtx);

    // Stab: collect candidate OBJECT node keys whose mapped interval contains x. De-dup + keep order.
    final LongLinkedOpenHashSet candidateObjectKeys = new LongLinkedOpenHashSet();
    tree.stab(domain.point(validTime), candidateObjectKeys::add);

    // Restrict to the scan's domain: the document item itself, or its direct array children.
    final LongLinkedOpenHashSet inDomain = new LongLinkedOpenHashSet();
    final var it = candidateObjectKeys.iterator();
    while (it.hasNext()) {
      final long objectKey = it.nextLong();
      if (objectKey == documentItemKey || isDirectChildOf(rtx, objectKey, documentItemKey)) {
        inDomain.add(objectKey);
      }
    }

    final long candidatesExamined = inDomain.size();

    // Verify each candidate by reading BOTH fields with the exact instant predicate; build items.
    // Sort by node key for a deterministic order (matches ValidTimeIndexScan).
    final long[] sortedKeys = inDomain.toLongArray();
    Arrays.sort(sortedKeys);

    final List<JsonDBItem> items = new ArrayList<>();
    for (final long objectKey : sortedKeys) {
      if (!rtx.moveTo(objectKey) || !rtx.isObject()) {
        continue;
      }
      final JsonDBObject obj = new JsonDBObject(rtx, collection);
      if (ValidTimeIndexScan.isValidAtTime(obj, validTime, validFromField, validToField)) {
        items.add(obj);
      }
    }

    return new Result(items, candidatesExamined);
  }

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
    // A rounded end equal to the query millisecond may actually lie AFTER the query. Such
    // records must be included before exact verification, even when the half-open stab skipped them.
    candidates.addAll(unverified);
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

  private static boolean isDirectChildOf(final JsonNodeReadOnlyTrx rtx, final long objectKey, final long parentKey) {
    if (!rtx.moveTo(objectKey)) {
      return false;
    }
    return rtx.getParentKey() == parentKey;
  }

  /**
   * The node key of the top-level item the callers' linear scan operates on: the first child of the
   * document root (the top-level array or object node).
   */
  private static long topLevelItemNodeKey(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    if (!rtx.hasFirstChild()) {
      return Long.MIN_VALUE;
    }
    rtx.moveToFirstChild();
    return rtx.getNodeKey();
  }
}

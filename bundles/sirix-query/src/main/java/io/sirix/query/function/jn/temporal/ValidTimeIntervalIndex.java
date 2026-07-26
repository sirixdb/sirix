package io.sirix.query.function.jn.temporal;

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

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

/**
 * Valid-time point-in-time predicate accelerated by the persistent
 * {@link RelationalIntervalTree} interval index ({@link io.sirix.index.IndexType#VALIDTIME}).
 *
 * <p>Given a query instant {@code x}, this stabs the RI-tree at {@code domain.point(x)} — an
 * {@code O(h)}-bounded probe that streams every record OBJECT whose mapped interval contains
 * {@code x} — then RE-VERIFIES each candidate by reading its exact {@code validFrom}/{@code validTo}
 * instants and applying the identical predicate the linear scan uses
 * ({@code !x.isBefore(from) && !x.isAfter(to)}), de-duplicates, and returns the surviving objects.</p>
 *
 * <h2>Why this equals the linear scan (the correctness gate)</h2>
 * <p>{@link IntervalDomain} is monotonic, so a true match {@code lo <= x <= hi} (compared as exact
 * instants) maps to {@code map(lo) <= map(x) <= map(hi)} — every true match's fork is on the
 * root&rarr;x path, so {@code stab} is COMPLETE (no false negatives). The exact-instant
 * re-verification removes false positives (sub-millisecond ties, clamped out-of-range instants,
 * and any record whose interval the domain widened). The result is therefore exactly the set the
 * scan returns. The candidate set is additionally restricted to the same domain as the scan: the
 * top-level document item, or its direct array children.</p>
 *
 * <h2>How an interval record maps to its object</h2>
 * <p>The RI-tree stores the containing record OBJECT's node key as the reference (the builder /
 * listener register {@code (validFrom, validTo)} under the object's key), so a stab result is
 * already an object node key — no parent walk is needed (unlike the CAS-narrowing path, whose hits
 * are value-node keys).</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIntervalIndex {

  private ValidTimeIntervalIndex() {
  }

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
   * @param document        the document item (anchored at the top-level array/object node)
   * @param validTime       the point in valid time to test
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
    final JsonIndexController controller =
        rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());
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
    java.util.Arrays.sort(sortedKeys);

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

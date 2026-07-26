package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared helper that accelerates the valid-time point-in-time predicate
 * ({@code validFrom <= validTime <= validTo}) used by {@code jn:open-bitemporal} and
 * {@code jn:valid-at} with CAS index range scans, falling back to {@code null} (so the caller can
 * keep its existing linear scan) when the indexes do not exist.
 *
 * <h2>Index shape</h2>
 * <p>
 * The scan requires the PAIR of {@link Type#DATI xs:dateTime} CAS indexes over the valid-time
 * fields — the kind the store layers auto-create for valid-time resources. Candidates are the
 * UNION of two exact one-sided temporal ranges: {@code validTo >= t} on the validTo index and
 * {@code validFrom <= t} on the validFrom index.
 * </p>
 *
 * <h2>How an index hit maps to its record object</h2>
 * <p>
 * The CAS index on a valid-time path (e.g. {@code /[]/validFrom}) stores the node key of the
 * indexed valid-time value node. Two node-model shapes occur:
 * </p>
 * <ul>
 * <li><b>Fused</b> (the default under {@code fuseNamedPrimitives}): the field collapses to an
 * {@code OBJECT_NAMED_STRING} leaf that carries the value inline and is a <em>direct child of the
 * containing OBJECT record</em>. The CAS index records that leaf's own node key. Hence
 * {@code moveTo(leafKey)} then {@code getParentKey()} reaches the record OBJECT.</li>
 * <li><b>Legacy</b> (non-fused): the indexed node is the {@code STRING_VALUE}, whose parent is the
 * {@code OBJECT_KEY}, whose parent is the record OBJECT. So we walk parents until {@link
 * JsonNodeReadOnlyTrx#isObject()} holds.</li>
 * </ul>
 * <p>
 * Both shapes are handled by {@link #moveToContainingObjectKey(JsonNodeReadOnlyTrx, long)} which
 * walks up to the first ancestor that plays the OBJECT role (at most two hops).
 * </p>
 *
 * <h2>Domain equivalence with the linear scan</h2>
 * <p>
 * The linear scan inspects exactly the document item itself plus, when the document is an array,
 * its <em>direct</em> element children. To return an identical set we keep only candidate records
 * whose parent node key equals the document item's node key (direct array child) or that ARE the
 * document item. This makes the index result a subset of the scan domain by construction.
 * </p>
 *
 * <h2>Why this is provably equal to the scan (the correctness gate)</h2>
 * <p>
 * xs:dateTime keys compare temporally, so the two one-sided ranges are exact. BOTH indexes are
 * required: a record whose bound string fails the xs:dateTime cast is absent from that field's
 * index (the CAS builder skips non-castable values) and is treated as unbounded on that side by the
 * predicate — but every true match has at least one castable bound satisfying its one-sided range
 * ({@code validFrom <= t} or {@code validTo >= t}), so the UNION of the two ranges is a superset of
 * all matches. Every surviving candidate is then re-verified by reading both fields with the same
 * {@code validFrom <= t <= validTo} {@link Instant} comparison the linear scan uses, removing any
 * over-fetch (no false positives).
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIndexScan {

  private static final DateTimeToInstant DATE_TIME_TO_INSTANT = new DateTimeToInstant();

  /**
   * Result of an index-accelerated valid-time scan: the verified, de-duplicated matching records.
   */
  public static final class Result {
    private final List<JsonDBItem> items;
    private final long candidatesExamined;

    Result(List<JsonDBItem> items, long candidatesExamined) {
      this.items = items;
      this.candidatesExamined = candidatesExamined;
    }

    /** The verified matching records (each record at most once), in ascending node-key order. */
    public List<JsonDBItem> items() {
      return items;
    }

    /** Number of distinct candidate records the indexes produced before final verification. */
    public long candidatesExamined() {
      return candidatesExamined;
    }
  }

  private ValidTimeIndexScan() {
  }

  /**
   * Try to evaluate the valid-time point-in-time predicate via CAS index range scans.
   *
   * @param document the document item (anchored at the top-level array/object node)
   * @param validTime the point in valid time to test
   * @param validTimeConfig the resource's valid-time configuration
   * @return a {@link Result} when the pair of xs:dateTime CAS indexes was found and used, or
   *         {@code null} when the caller should fall back to the linear scan
   */
  public static Result tryIndexScan(final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    if (document == null || validTimeConfig == null) {
      return null;
    }

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());
    if (controller == null) {
      return null;
    }

    final JsonDBCollection collection = document.getCollection();

    final String validFromField = validTimeConfig.getNormalizedValidFromPath();
    final String validToField = validTimeConfig.getNormalizedValidToPath();

    final IndexDef validToIndex = findCasIndexForField(controller, validToField);
    final IndexDef validFromIndex = findCasIndexForField(controller, validFromField);
    if (validToIndex == null || validFromIndex == null) {
      return null;
    }

    final Atomic pointInTime = new DateTime(validTime.toString());
    final long documentItemKey = topLevelItemNodeKey(rtx);

    final Set<Long> candidateObjectKeys = new LinkedHashSet<>();
    // validTo >= t: one-sided range [t, +inf) — null max is unbounded.
    collectCandidates(rtx, controller, validToIndex, pointInTime, null, documentItemKey, candidateObjectKeys);
    // validFrom <= t: one-sided range (-inf, t] — null min is unbounded.
    collectCandidates(rtx, controller, validFromIndex, null, pointInTime, documentItemKey, candidateObjectKeys);

    return verifyCandidates(rtx, collection, candidateObjectKeys, validTime, validFromField, validToField);
  }

  /**
   * Find a CAS index of content type {@link Type#DATI} whose last path step matches {@code field}.
   *
   * <p>
   * Matching on the path's {@link Path#tail() tail} (last step name) avoids reconstructing the full
   * document-shape-dependent path string and works for any nesting where the valid-time field is
   * the leaf of the indexed path.
   * </p>
   */
  private static IndexDef findCasIndexForField(final JsonIndexController controller, final String field) {
    for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
      if (!indexDef.isCasIndex()) {
        continue;
      }
      if (!Type.DATI.equals(indexDef.getContentType())) {
        continue;
      }
      for (final Path<QNm> path : indexDef.getPaths()) {
        final QNm tail = path.tail();
        if (tail != null && field.equals(tail.getLocalName())) {
          return indexDef;
        }
      }
    }
    return null;
  }

  /**
   * Run one CAS range scan and add every candidate record OBJECT key within the linear-scan domain
   * (the document item itself or its direct array children) to {@code candidateObjectKeys}. The
   * set de-dups records hit via multiple indexed values or via both indexes.
   */
  private static void collectCandidates(final JsonNodeReadOnlyTrx rtx, final JsonIndexController controller,
      final IndexDef indexDef, final Atomic min, final Atomic max, final long documentItemKey,
      final Set<Long> candidateObjectKeys) {
    // PCR filtering: restrict to the index's own indexed path(s).
    final Set<String> paths = new LinkedHashSet<>();
    for (final Path<QNm> path : indexDef.getPaths()) {
      paths.add(path.toString());
    }

    final CASFilterRange filter = controller.createCASFilterRange(paths, min, max, true, true, new JsonPCRCollector(rtx));
    final Iterator<NodeReferences> index = controller.openCASIndex(rtx.getStorageEngineReader(), indexDef, filter);

    while (index.hasNext()) {
      final NodeReferences refs = index.next();
      final var it = refs.getNodeKeys().getLongIterator();
      while (it.hasNext()) {
        final long indexedNodeKey = it.next();
        final long objectKey = moveToContainingObjectKey(rtx, indexedNodeKey);
        if (objectKey == Long.MIN_VALUE) {
          continue;
        }
        // Domain equivalence with the linear scan: keep only direct array children (parent == the
        // array node the scan iterates) or the document item object itself.
        if (objectKey == documentItemKey || isDirectChildOf(rtx, objectKey, documentItemKey)) {
          candidateObjectKeys.add(objectKey);
        }
      }
    }
  }

  /**
   * Verify each candidate by reading BOTH fields and applying the exact instant predicate; build
   * the matching items. Sorting by node key gives a deterministic order.
   */
  private static Result verifyCandidates(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final Set<Long> candidateObjectKeys, final Instant validTime, final String validFromField,
      final String validToField) {
    final long candidatesExamined = candidateObjectKeys.size();

    final List<Long> sortedKeys = new ArrayList<>(candidateObjectKeys);
    sortedKeys.sort(Long::compareTo);

    final List<JsonDBItem> items = new ArrayList<>(sortedKeys.size());
    for (final long objectKey : sortedKeys) {
      if (!rtx.moveTo(objectKey)) {
        continue;
      }
      if (!rtx.isObject()) {
        continue;
      }
      final JsonDBObject obj = new JsonDBObject(rtx, collection);
      if (isValidAtTime(obj, validTime, validFromField, validToField)) {
        items.add(obj);
      }
    }

    return new Result(items, candidatesExamined);
  }

  /**
   * Walk up from an indexed valid-time node to the node key of its containing OBJECT record.
   *
   * <p>
   * Under fusion the indexed {@code OBJECT_NAMED_STRING} leaf is already a direct child of the
   * record OBJECT (one hop). In the legacy shape the indexed {@code STRING_VALUE} sits under an
   * {@code OBJECT_KEY} under the record OBJECT (two hops). We therefore walk parents until {@link
   * JsonNodeReadOnlyTrx#isObject()} holds, capping at a few hops as a guard.
   * </p>
   *
   * @return the containing object's node key, or {@link Long#MIN_VALUE} if none was found
   */
  static long moveToContainingObjectKey(final JsonNodeReadOnlyTrx rtx, final long indexedNodeKey) {
    if (!rtx.moveTo(indexedNodeKey)) {
      return Long.MIN_VALUE;
    }
    // At most: leaf/string-value -> object-key -> object. Cap the walk defensively.
    for (int hops = 0; hops < 4; hops++) {
      if (rtx.isObject()) {
        return rtx.getNodeKey();
      }
      if (!rtx.hasParent()) {
        return Long.MIN_VALUE;
      }
      rtx.moveToParent();
    }
    return Long.MIN_VALUE;
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

  /**
   * The exact instant predicate, identical in semantics to the linear scan and to the interval-index
   * registration: reads the configured valid-time fields off {@code obj} and tests
   * {@code validFrom <= validTime <= validTo}, where an <em>absent</em> (or unparseable) bound is
   * treated as unbounded on that side — a missing {@code validTo} is "valid from {@code validFrom}
   * onward", a missing {@code validFrom} is "valid up to {@code validTo}". A record with neither bound
   * carries no interval and never matches.
   *
   * <p>
   * Data-shape problems (absent fields, non-string values, unparseable dates) are handled explicitly
   * above; anything else — notably I/O failures while reading the fields — propagates. A previous
   * catch-all here mapped such failures to {@code false}, silently dropping records from query
   * results (e.g. under file-descriptor exhaustion every record read failed and {@code jn:valid-at}
   * returned an empty sequence instead of an error).
   * </p>
   */
  static boolean isValidAtTime(final io.brackit.query.jdm.json.Object obj, final Instant validTime,
      final String validFromField, final String validToField) {
    final Sequence validFromSeq = obj.get(new QNm(validFromField));
    final Sequence validToSeq = obj.get(new QNm(validToField));

    // Open-ended intervals: a null (absent/unparseable) bound is unbounded on that side. This mirrors
    // the interval index exactly — its writer maps a null bound to the domain min/max and registers
    // the record (ValidTimeIntervalIndexWriter.toInterval) — so all paths still return the same set.
    final Instant validFrom = validFromSeq == null ? null : parseInstant(validFromSeq);
    final Instant validTo = validToSeq == null ? null : parseInstant(validToSeq);

    if (validFrom == null && validTo == null) {
      return false;
    }
    if (validFrom != null && validTime.isBefore(validFrom)) {
      return false;
    }
    if (validTo != null && validTime.isAfter(validTo)) {
      return false;
    }
    return true;
  }

  private static Instant parseInstant(final Sequence seq) {
    if (seq instanceof DateTime dt) {
      return DATE_TIME_TO_INSTANT.convert(dt);
    }
    if (seq instanceof Str str) {
      try {
        return Instant.parse(str.stringValue());
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }
}

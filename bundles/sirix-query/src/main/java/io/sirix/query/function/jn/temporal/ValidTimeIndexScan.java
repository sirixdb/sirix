package io.sirix.query.function.jn.temporal;

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared helper that accelerates the valid-time point-in-time predicate
 * ({@code validFrom <= validTime <= validTo}) used by {@code jn:open-bitemporal} and
 * {@code jn:valid-at} with a CAS index range scan, falling back to {@code null} (so the caller can
 * keep its existing linear scan) when no suitable index exists.
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
 * The CAS values are {@link Type#STR} and compared lexicographically. The range we hand the index
 * is computed at whole-second granularity so that it is a guaranteed <em>superset</em> of every
 * record that chronologically satisfies the predicate (no false negatives), and every surviving
 * candidate is then re-verified by reading both fields with the same {@code validFrom <= t <=
 * validTo} {@link Instant} comparison the scan uses (no false positives). The bound assumes the
 * stored valid-time strings are canonical ISO-8601 UTC instants (what {@code Instant.toString()}
 * produces and what the existing {@code parseInstant} already assumes). See
 * {@link #safeLowerSecondBound(Instant)} / {@link #safeUpperSecondBound(Instant)} for the proof
 * sketch.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIndexScan {

  /** ISO instant formatter truncated to whole seconds, always WITHOUT a fractional part. */
  private static final DateTimeFormatter SECOND_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

  /** A string that sorts after any canonical ISO-8601 instant string. */
  private static final String LEX_MAX = "￿";

  private static final DateTimeToInstant DATE_TIME_TO_INSTANT = new DateTimeToInstant();

  /**
   * Result of an index-accelerated valid-time scan: the verified, de-duplicated matching records
   * plus which valid-time field's index was used (for test visibility).
   */
  public static final class Result {
    private final List<JsonDBItem> items;
    private final ValidField indexedField;
    private final long candidatesExamined;

    Result(List<JsonDBItem> items, ValidField indexedField, long candidatesExamined) {
      this.items = items;
      this.indexedField = indexedField;
      this.candidatesExamined = candidatesExamined;
    }

    /** The verified matching records (each record at most once), in ascending node-key order. */
    public List<JsonDBItem> items() {
      return items;
    }

    /** Which valid-time field's CAS index narrowed the candidates. */
    public ValidField indexedField() {
      return indexedField;
    }

    /** Number of distinct candidate records the index produced before final verification. */
    public long candidatesExamined() {
      return candidatesExamined;
    }
  }

  /** Which valid-time field a CAS index was found on / used. */
  public enum ValidField {
    VALID_FROM,
    VALID_TO
  }

  private ValidTimeIndexScan() {
  }

  /**
   * Try to evaluate the valid-time point-in-time predicate via a CAS index range scan.
   *
   * @param document the document item (anchored at the top-level array/object node)
   * @param validTime the point in valid time to test
   * @param validTimeConfig the resource's valid-time configuration
   * @return a {@link Result} when a suitable CAS index was found and used, or {@code null} when the
   *         caller should fall back to the linear scan
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

    // Prefer the validTo index: for a point-in-time query "validTo >= t" is usually the more
    // selective of the two one-sided ranges. Fall back to the validFrom index.
    final IndexDef validToIndex = findCasIndexForField(controller, validToField);
    if (validToIndex != null) {
      return runIndexScan(document, rtx, controller, collection, validToIndex, ValidField.VALID_TO, validTime,
          validFromField, validToField);
    }

    final IndexDef validFromIndex = findCasIndexForField(controller, validFromField);
    if (validFromIndex != null) {
      return runIndexScan(document, rtx, controller, collection, validFromIndex, ValidField.VALID_FROM, validTime,
          validFromField, validToField);
    }

    return null;
  }

  /**
   * Find a CAS index of content type {@link Type#STR} whose last path step matches {@code field}.
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
      // Valid-time values are stored as strings (the CAS index is created with Type.STR). Only such
      // indexes give the lexicographic ordering our bound math relies on.
      if (!Type.STR.equals(indexDef.getContentType())) {
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

  private static Result runIndexScan(final JsonDBItem document, final JsonNodeReadOnlyTrx rtx,
      final JsonIndexController controller, final JsonDBCollection collection, final IndexDef indexDef,
      final ValidField field, final Instant validTime, final String validFromField, final String validToField) {

    // PCR filtering: restrict to the index's own indexed path(s).
    final Set<String> paths = new LinkedHashSet<>();
    for (final Path<QNm> path : indexDef.getPaths()) {
      paths.add(path.toString());
    }

    final Str min;
    final Str max;
    if (field == ValidField.VALID_TO) {
      // Candidates: validTo >= validTime  ->  lexicographic [safeLower, +inf]
      min = new Str(safeLowerSecondBound(validTime));
      max = new Str(LEX_MAX);
    } else {
      // Candidates: validFrom <= validTime  ->  lexicographic [-inf, safeUpper]
      min = new Str("");
      max = new Str(safeUpperSecondBound(validTime));
    }

    final CASFilterRange filter = controller.createCASFilterRange(paths, min, max, true, true, new JsonPCRCollector(rtx));
    final Iterator<NodeReferences> index = controller.openCASIndex(rtx.getStorageEngineReader(), indexDef, filter);

    // The node the linear scan treats as the document item: the first child of the document root
    // (the top-level array or object). Compute it via explicit navigation so it does not depend on
    // the shared cursor's transient position.
    final long documentItemKey = topLevelItemNodeKey(rtx);

    // De-dup candidate record objects by node key (an object could be hit twice if the index path
    // were multi-valued; also two valid-time fields of one record would map to the same object).
    final Set<Long> candidateObjectKeys = new LinkedHashSet<>();
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

    final long candidatesExamined = candidateObjectKeys.size();

    // Verify each candidate by reading BOTH fields and applying the exact instant predicate; build
    // the matching items. Sorting by node key gives a deterministic order.
    final List<Long> sortedKeys = new ArrayList<>(candidateObjectKeys);
    sortedKeys.sort(Long::compareTo);

    final List<JsonDBItem> items = new ArrayList<>();
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

    return new Result(items, field, candidatesExamined);
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
   * Lexicographic LOWER bound (inclusive) that is &le; every stored string whose instant is &ge;
   * {@code validTime}, for the {@code validTo >= validTime} query.
   *
   * <p>
   * Proof sketch (canonical ISO-8601 UTC strings): truncating {@code validTime} down to its whole
   * second and dropping the trailing {@code 'Z'} yields a prefix {@code P = "...SS"}. Any true
   * match has a floor-second &ge; validTime's floor-second. If strictly greater, the seconds prefix
   * makes the string lexicographically &gt; P. If equal, the string is {@code "...SS.fracZ"} or
   * {@code "...SSZ"} — P is a prefix of both, so P &le; both. Hence no true match sorts below P:
   * the range admits every match (plus possibly a few same-second non-matches, removed by
   * verification).
   * </p>
   */
  static String safeLowerSecondBound(final Instant validTime) {
    return SECOND_FORMATTER.format(validTime.truncatedTo(ChronoUnit.SECONDS));
  }

  /**
   * Lexicographic UPPER bound (inclusive) that is &ge; every stored string whose instant is &le;
   * {@code validTime}, for the {@code validFrom <= validTime} query.
   *
   * <p>
   * Proof sketch (canonical ISO-8601 UTC strings): the lexicographically largest string within a
   * floor-second {@code S} is {@code S + "Z"} (no fraction), because the only characters that can
   * follow the seconds are {@code '.'} (0x2E, starts a fraction) or {@code 'Z'} (0x5A), and
   * {@code '.' < 'Z'}. Using {@code U = validTime.floorSecond + "Z"} (inclusive) therefore bounds
   * every true match: a match has floor-second &le; validTime's; if strictly less the whole
   * seconds prefix makes it &lt; U, and if equal the largest possible string is exactly {@code U}.
   * So no true match sorts above U.
   * </p>
   */
  static String safeUpperSecondBound(final Instant validTime) {
    return SECOND_FORMATTER.format(validTime.truncatedTo(ChronoUnit.SECONDS)) + "Z";
  }

  /**
   * The exact instant predicate, identical in semantics to the linear scan's check: reads both
   * configured valid-time fields off {@code obj} and tests {@code validFrom <= validTime <=
   * validTo}.
   */
  static boolean isValidAtTime(final io.brackit.query.jdm.json.Object obj, final Instant validTime,
      final String validFromField, final String validToField) {
    try {
      final Sequence validFromSeq = obj.get(new QNm(validFromField));
      final Sequence validToSeq = obj.get(new QNm(validToField));

      if (validFromSeq == null || validToSeq == null) {
        return false;
      }

      final Instant validFrom = parseInstant(validFromSeq);
      final Instant validTo = parseInstant(validToSeq);

      if (validFrom == null || validTo == null) {
        return false;
      }

      return !validTime.isBefore(validFrom) && !validTime.isAfter(validTo);
    } catch (Exception e) {
      return false;
    }
  }

  private static Instant parseInstant(final Sequence seq) {
    if (seq instanceof io.brackit.query.atomic.DateTime dt) {
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

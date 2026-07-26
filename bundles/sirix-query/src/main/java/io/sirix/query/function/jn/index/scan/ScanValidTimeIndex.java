package io.sirix.query.function.jn.index.scan;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.jn.temporal.ValidTimeFilter;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.JsonDBItem;

import java.time.Instant;

/**
 * Internal scan function over a valid-time (bitemporal) interval index. Given a document and a valid
 * time instant, returns every record OBJECT whose {@code [validFrom, validTo]} interval contains the
 * instant — the index-scan analog of {@code jn:scan-cas-index-range}, and the rewrite target the
 * optimizer ({@code JsonValidTimeStep}) emits for a plain FLWOR stabbing predicate.
 *
 * <ul>
 * <li><code>jn:scan-valid-time-index($doc as json-item(), $validTime as xs:dateTime) as json-item()*</code></li>
 * </ul>
 *
 * <p>Backs onto {@link ValidTimeIntervalIndex}: stab the Relational-Interval-Tree at
 * {@code IntervalDomain.point($validTime)}, collect candidate object node-keys, re-verify each by
 * reading its exact {@code validFrom}/{@code validTo} instants, dedup, return the surviving objects.
 * If no VALIDTIME index exists on the resource (e.g. the function is called directly rather than via
 * the optimizer), it transparently falls back to the exact linear scan so results are always
 * correct.</p>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the valid-time interval index for records valid at the given instant.",
    parameters = {"$doc", "$validTime"})
public final class ScanValidTimeIndex extends io.brackit.query.function.AbstractFunction {

  /** Valid-time interval index scan function name. */
  public static final QNm SCAN_VALID_TIME_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-valid-time-index");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  public ScanValidTimeIndex() {
    super(SCAN_VALID_TIME_INDEX,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany),
            new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.One),
            new SequenceType(AtomicType.DATI, Cardinality.One)),
        true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 2) {
      throw new QueryException(new QNm("Expected 2 arguments: document, validTime"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final Instant validTime = dateTimeToInstant.convert((DateTime) args[1]);

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();
    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();

    if (validTimeConfig == null) {
      throw new QueryException(new QNm("Resource does not have valid time configuration. "
          + "Configure valid time paths when creating the resource."));
    }

    // Fast path: the persistent interval index.
    final ValidTimeIntervalIndex.Result indexResult =
        ValidTimeIntervalIndex.tryIndexScan(document, validTime, validTimeConfig);
    if (indexResult != null) {
      return new ItemSequence(indexResult.items().toArray(new Item[0]));
    }

    // Fallback (no interval index — e.g. called directly): exact linear scan, same predicate.
    return ValidTimeFilter.linearScanSequence(document, validTime, validTimeConfig);
  }
}

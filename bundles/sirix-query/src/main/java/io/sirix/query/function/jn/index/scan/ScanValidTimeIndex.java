package io.sirix.query.function.jn.index.scan;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.jn.temporal.ValidTimeFilter;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.JsonDBItem;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Public closed scan over a valid-time interval index, with an internal optimizer comparison
 * helper. Given a document and a valid time instant, returns every record OBJECT whose
 * {@code [validFrom, validTo]} interval contains the instant — the index-scan analog of
 * {@code jn:scan-cas-index-range}.
 *
 * <ul>
 * <li><code>jn:scan-valid-time-index($doc as json-item(), $validTime as xs:dateTime) as json-item()*</code></li>
 * </ul>
 *
 * <p>
 * Backs onto {@link ValidTimeIntervalIndex}: exact millisecond intervals yield sorted keys without
 * reading timestamp fields; exceptional intervals retain exact verification. Objects are
 * constructed on demand. The optimizer's internal comparison helper preserves strictness and
 * original field casts. If no VALIDTIME index exists on the resource (e.g. the function is called
 * directly rather than via the optimizer), it transparently falls back to the exact linear scan so
 * results are always correct.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the valid-time interval index for records valid at the given instant.",
    parameters = {"$doc", "$validTime"})
public final class ScanValidTimeIndex extends AbstractFunction {

  /** Valid-time interval index scan function name. */
  public static final QNm SCAN_VALID_TIME_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-valid-time-index");

  public static final String DEFERRED_POINT = "VALID_TIME_DEFERRED_POINT";

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  public ScanValidTimeIndex() {
    super(SCAN_VALID_TIME_INDEX,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany),
            new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.One),
            new SequenceType(AtomicType.DATI, Cardinality.One)),
        true);
  }

  @Override
  public Sequence execute(final @Nullable StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 2) {
      throw new QueryException(new QNm("Expected 2 arguments for a valid-time index scan"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();
    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();

    if (validTimeConfig == null) {
      throw new QueryException(new QNm("Resource does not have valid time configuration. "
          + "Configure valid time paths when creating the resource."));
    }

    final Instant validTime = dateTimeToInstant.convert((DateTime) args[1]);
    // Fast path: the persistent interval index.
    final Sequence intervalSequence =
        ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, false, false);
    if (intervalSequence != null) {
      return intervalSequence;
    }

    // Fallback (no interval index — e.g. called directly): exact linear scan, same predicate.
    return ValidTimeFilter.linearScanSequence(document, validTime, validTimeConfig);
  }

  public static Sequence comparisonScan(final @Nullable StaticContext sctx, final QueryContext ctx,
      final @Nullable JsonDBItem document, final Supplier<Sequence> point, final @Nullable DateTime capturedPoint,
      final String from, final String to, final int mode) {
    if (mode < 0 || mode > 127) {
      throw new QueryException(new QNm("Invalid valid-time comparison mode"));
    }
    if (!(document instanceof Array) || ((Array) ValidTimeFilter.currentDocument(document)).len() == 0) {
      return new ItemSequence();
    }
    return new AbstractSequence() {
      private @Nullable Sequence selected = capturedPoint == null
          ? null
          : indexed(() -> capturedPoint);

      private @Nullable Sequence indexed(final Supplier<Sequence> value) {
        final ValidTimeConfig config = document.getResourceSession().getResourceConfig().getValidTimeConfig();
        return config != null && from.equals(config.getNormalizedValidFromPath())
            && to.equals(config.getNormalizedValidToPath())
                ? ValidTimeIntervalIndex.comparisonSequence(document, value, config, (mode & 1) != 0, (mode & 2) != 0)
                : null;
      }

      private Sequence selected() {
        if (selected == null) {
          selected = indexed(point);
          if (selected == null) {
            selected = ValidTimeFilter.comparisonScanSequence(document, point, from, to, mode, sctx, ctx);
          }
        }
        return selected;
      }

      @Override
      public boolean isRepeatable() {
        return selected != null && selected.isRepeatable();
      }

      @Override
      public @Nullable IntNumeric knownSize() {
        return selected == null
            ? null
            : selected.knownSize();
      }

      @Override
      public IntNumeric size() {
        return selected().size();
      }

      @Override
      public boolean booleanValue() {
        return selected().booleanValue();
      }

      @Override
      public @Nullable Item get(final IntNumeric position) {
        return position.cmp(Int32.ONE) < 0
            ? null
            : selected().get(position);
      }

      @Override
      public Iter iterate() {
        return new BaseIter() {
          private @Nullable Iter input;
          private boolean closed;

          @Override
          public @Nullable Item next() {
            if (closed) {
              return null;
            }
            if (input == null) {
              input = selected().iterate();
            }
            return input.next();
          }

          @Override
          public void close() {
            closed = true;
            if (input != null) {
              input.close();
            }
          }
        };
      }
    };
  }
}

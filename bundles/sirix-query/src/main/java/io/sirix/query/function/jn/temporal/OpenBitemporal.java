package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

/**
 * <p>
 * Function for bitemporal queries combining transaction time and valid time. Opens the resource at
 * a specific transaction time, filtered to records valid at the specified valid time. Supported
 * signatures are:
 * </p>
 * <ul>
 * <li><code>jn:open-bitemporal($coll as xs:string, $res as xs:string,
 *     $transactionTime as xs:dateTime, $validTime as xs:dateTime) as json-item()*</code></li>
 * </ul>
 *
 * <p>
 * This function enables true bitemporal queries by combining:
 * </p>
 * <ul>
 * <li><b>Transaction time</b>: When the data was recorded (managed by SirixDB via revisions)</li>
 * <li><b>Valid time</b>: When the data is/was/will be true in the real world</li>
 * </ul>
 *
 * <p>
 * The resource must be configured with valid time paths via
 * {@link io.sirix.access.ResourceConfiguration.Builder#validTimePaths(String, String)}.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class OpenBitemporal extends AbstractFunction {

  /**
   * Function name.
   */
  public static final QNm OPEN_BITEMPORAL = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open-bitemporal");

  public static final QNm OPEN_BITEMPORAL_SLICE =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open-bitemporal-slice");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public OpenBitemporal(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 4 && args.length != 7) {
      throw new QueryException(new QNm("Expected 4 arguments: collection, resource, transactionTime, validTime"));
    }

    final JsonDBCollection collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("Collection not found: " + ((Str) args[0]).stringValue()));
    }

    final String resourceName = ((Str) args[1]).stringValue();
    final DateTime transactionDateTime = (DateTime) args[2];
    final DateTime validDateTime = (DateTime) args[3];

    final Instant transactionTime = dateTimeToInstant.convert(transactionDateTime);
    final Instant validTime = dateTimeToInstant.convert(validDateTime);

    // Open the document at the specified transaction time (point in time)
    final JsonDBItem document = collection.getDocument(resourceName, transactionTime);
    if (document == null) {
      throw new QueryException(new QNm("Resource not found: " + resourceName));
    }

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();
    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();

    if (validTimeConfig == null) {
      throw new QueryException(new QNm("Resource does not have valid time configuration. "
          + "Configure valid time paths when creating the resource."));
    }

    if (args.length == 4) {
      return closedSequence(document, validTime, validTimeConfig);
    }
    return sliceSequence(sctx, ctx, args, validDateTime, document, validTime, validTimeConfig);
  }

  private static Sequence sliceSequence(final StaticContext sctx, final QueryContext ctx, final Sequence[] args,
      final DateTime validDateTime, final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    final String field = ((Str) args[4]).stringValue();
    final int encodedMode = ((IntNumeric) args[5]).intValue();
    if (encodedMode < 1 || encodedMode > 16) {
      throw new QueryException(new QNm("Invalid valid-time comparison mode"));
    }
    final int mode = ((encodedMode - 1) & 3) + 1;
    final boolean start = mode == 1 || mode == 2;
    final boolean strict = mode == 2 || mode == 4;
    final Sequence comparisonPoint = args[6];
    final ValidTimeResidual residual = new ValidTimeResidual(sctx, ctx, () -> comparisonPoint, field, start, strict,
        encodedMode > 8, ((encodedMode - 1) & 4) == 0
            ? start
            : !start);
    if (matchesIndexedComparison(field, validTimeConfig, start, comparisonPoint, validDateTime)) {
      final Sequence sequence = ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, start && strict,
          !start && strict, residual);
      if (sequence != null) {
        return sequence;
      }
    }

    return filterSequence(closedSequence(document, validTime, validTimeConfig), residual);
  }

  private static boolean matchesIndexedComparison(final String field, final ValidTimeConfig config, final boolean start,
      final @Nullable Sequence comparisonPoint, final DateTime validDateTime) {
    return field.equals(start
        ? config.getNormalizedValidFromPath()
        : config.getNormalizedValidToPath()) && comparisonPoint instanceof DateTime point && point.getTimezone() != null
        && point.cmp(validDateTime) == 0;
  }

  private static Sequence filterSequence(final Sequence source, final ValidTimeResidual residual) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Iter input = source.iterate();
        return new BaseIter() {
          @Override
          public @Nullable Item next() {
            Item item;
            while ((item = input.next()) != null) {
              if (residual.test(item)) {
                return item;
              }
            }
            return null;
          }

          @Override
          public void close() {
            input.close();
          }
        };
      }
    };
  }

  private static Sequence closedSequence(final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    final Sequence intervalSequence =
        ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, false, false, null);
    if (intervalSequence != null) {
      return intervalSequence;
    }
    return ValidTimeFilter.linearScanSequence(document, validTime, validTimeConfig);
  }
}

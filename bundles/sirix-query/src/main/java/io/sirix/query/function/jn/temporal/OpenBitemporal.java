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
import io.brackit.query.jdm.type.AnyItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.ResourceConfiguration;
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
 * a specific transaction time, filtered to records valid at the specified valid time. Validity is
 * half-open: {@code validFrom <= validTime < validTo}. Missing or unparseable bounds retain their
 * existing unbounded interpretation. The supported signature is:
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
 * {@link ResourceConfiguration.Builder#validTimePaths(String, String)}.
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

  /** Marks calls introduced by the optimizer; the internal function is never predefined. */
  public static final String INTERNAL_SLICE = "sirix.internalBitemporalSlice";

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

  /** Used directly by the translator after analysis of the original public call. */
  public static OpenBitemporal forSlice() {
    return new OpenBitemporal(OPEN_BITEMPORAL_SLICE,
        new Signature(SequenceType.JSON_ITEM_SEQUENCE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.One),
            new SequenceType(AtomicType.DATI, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.INR, Cardinality.One),
            new SequenceType(AnyItemType.ANY, Cardinality.ZeroOrMany)));
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
      return halfOpenSequence(document, validTime, validTimeConfig);
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
      final ValidTimeKeySequence sequence =
          ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, start && strict, true, null);
      if (sequence != null) {
        return sequence.knownSize() != null
            ? sequence
            : sequence.withResidual(residual);
      }
    }

    return filterSequence(halfOpenSequence(document, validTime, validTimeConfig), residual);
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

  private static Sequence halfOpenSequence(final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    final Sequence intervalSequence =
        ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, false, true);
    if (intervalSequence != null) {
      return intervalSequence;
    }
    return ValidTimeFilter.linearScanSequence(document, validTime, validTimeConfig, false, true);
  }
}

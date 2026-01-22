package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.ValidTimeConfig;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 * Function for retrieving the validFrom timestamp from a node. Supported signatures:
 * </p>
 * <ul>
 * <li><code>sdb:valid-from($node as json-item()) as xs:dateTime?</code></li>
 * </ul>
 *
 * <p>Returns the validFrom timestamp from the node, or empty sequence if the
 * node doesn't have valid time fields or the resource is not configured for valid time.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class GetValidFrom extends AbstractFunction {

  /**
   * Function name.
   */
  public static final QNm VALID_FROM = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "valid-from");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public GetValidFrom(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 1) {
      throw new QueryException(new QNm("Expected 1 argument: node"));
    }

    final Sequence arg = args[0];
    if (!(arg instanceof StructuredDBItem<?> item)) {
      throw new QueryException(new QNm("Argument must be a structured database item"));
    }

    // Get the valid time configuration
    final ValidTimeConfig validTimeConfig = item.getTrx().getResourceSession()
        .getResourceConfig().getValidTimeConfig();

    if (validTimeConfig == null) {
      // No valid time configuration, return empty sequence
      return null;
    }

    // If it's a JSON object, try to get the validFrom field
    if (item instanceof JsonDBItem jsonItem && jsonItem instanceof io.brackit.query.jdm.json.Object obj) {
      try {
        final var validFromField = new QNm(validTimeConfig.getNormalizedValidFromPath());
        final Sequence validFromSeq = obj.get(validFromField);

        if (validFromSeq == null) {
          return null;
        }

        return parseDateTime(validFromSeq);
      } catch (Exception e) {
        return null;
      }
    }

    return null;
  }

  /**
   * Parses a DateTime from a sequence.
   */
  private DateTime parseDateTime(Sequence seq) {
    if (seq instanceof DateTime dt) {
      return dt;
    }
    if (seq instanceof io.brackit.query.atomic.Str str) {
      try {
        final Instant instant = Instant.parse(str.stringValue());
        final String dateTime = DateTimeFormatter.ISO_INSTANT.format(instant);
        return new DateTime(dateTime);
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }
}

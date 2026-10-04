package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;

import java.time.Instant;

/**
 * <p>
 * Function for querying data by valid time. Returns all records where the valid time interval
 * contains the specified timestamp. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:valid-at($coll as xs:string, $res as xs:string, $validTime as xs:dateTime) as json-item()*</code></li>
 * </ul>
 *
 * <p>
 * This function is part of the bitemporal query support in SirixDB. It requires the resource to be
 * configured with valid time paths via
 * {@link io.sirix.access.ResourceConfiguration.Builder#validTimePaths(String, String)}.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidAt extends AbstractFunction {

  /**
   * Function name.
   */
  public static final QNm VALID_AT = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "valid-at");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public ValidAt(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("Expected 3 arguments: collection, resource, validTime"));
    }

    final JsonDBCollection collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("Collection not found: " + ((Str) args[0]).stringValue()));
    }

    final String resourceName = ((Str) args[1]).stringValue();
    final DateTime dateTime = (DateTime) args[2];
    final Instant validTime = dateTimeToInstant.convert(dateTime);

    // Get the document at the most recent revision
    final JsonDBItem document = collection.getDocument(resourceName);
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

    // Exact interval keys avoid object reads; exceptional bounds retain demand-time verification
    // against the same predicate as the fallback scan.
    final Sequence intervalSequence =
        ValidTimeIntervalIndex.sequence(document, validTime, validTimeConfig, false, false);
    if (intervalSequence != null) {
      return intervalSequence;
    }

    return ValidTimeFilter.linearScanSequence(document, validTime, validTimeConfig);
  }
}

package io.sirix.query.function.jn.io;

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
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;

import java.time.Instant;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:open($coll as xs:string, $res as xs:string, $pointInTime as xs:long) as json-item()</code>
 * </li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class DocByPointInTime extends AbstractFunction {

  /** Open function name. */
  public final static QNm OPEN = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public DocByPointInTime(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBCollection collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String resourceName = ((Str) args[1]).stringValue();
    final DateTime dateTime = (DateTime) args[2];
    final Instant pointInTime = dateTimeToInstant.convert(dateTime);

    return collection.getDocument(resourceName, pointInTime);
  }
}

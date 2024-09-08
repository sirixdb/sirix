package io.sirix.query.function.jn.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.json.JsonDBCollection;

/**
 * <p>
 * Function for dropping a collection/database. Supported signatures is:
 * </p>
 * <ul>
 * <li><code>jn:drop-database($coll as xs:string, $res as xs:string)</code>
 * </li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class DropDatabase extends AbstractFunction {

  /** Doc function name. */
  public final static QNm DROP_DATABASE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-database");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public DropDatabase(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    if (args.length != 1) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBCollection collection = (JsonDBCollection) queryContext.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    collection.delete();

    return null;
  }

  @Override
  public boolean isUpdating() {
    return true;
  }
}

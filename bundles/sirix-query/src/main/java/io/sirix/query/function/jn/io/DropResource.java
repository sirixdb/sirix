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
import io.sirix.access.ResourceConfiguration;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;

/**
 * <p>
 * Function for removing a resource within a database. Supported signature is:
 * </p>
 * <ul>
 * <li><code>jn:drop-resource($coll as xs:string, $res as xs:string)</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class DropResource extends AbstractFunction {

  /**
   * Doc function name.
   */
  public final static QNm DROP_RESOURCE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-resource");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public DropResource(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 2) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBCollection collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    JsonDBItem document = collection.getDocument(((Str) args[1]).stringValue());

    long id;
    try (final var resourceSession = document.getResourceSession()) {
      id = resourceSession.getResourceConfig().getID();
    }

    collection.remove(id);

    return null;
  }

  @Override
  public boolean isUpdating() {
    return true;
  }
}

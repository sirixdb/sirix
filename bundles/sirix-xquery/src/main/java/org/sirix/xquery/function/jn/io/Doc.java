package org.sirix.xquery.function.jn.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.rest.AuthRole;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.Roles;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:doc($coll as xs:string, $res as xs:string, $revision as xs:int?) as json-item()</code>
 * </li>
 * <li><code>jn:doc($coll as xs:string, $res as xs:string) as json-item()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class Doc extends AbstractFunction {

  /** Doc function name. */
  public final static QNm DOC = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Doc(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length < 2 || args.length > 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBCollection col = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    Roles.check(ctx, col.getName(), AuthRole.VIEW);

    final String expResName = ((Str) args[1]).stringValue();
    final int revision = FunUtil.getInt(args, 2, "revision", -1, null, false);

    return col.getDocument(expResName, revision);
  }
}

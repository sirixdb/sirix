package org.sirix.xquery.function.sdb.io;

import java.time.Instant;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>sdb:open($coll as xs:string, $res as xs:string, $pointInTime as xs:long) as xs:node</code>
 * </li>
 * <li>
 * <code>sdb:open($coll as xs:string, $res as xs:string, $pointInTime as xs:long, $updatable as xs:boolean?) as xs:node</code>
 * </li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class DocByPointInTime extends AbstractFunction {

  /** Open function name. */
  public final static QNm OPEN = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "open");

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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    if (args.length < 3 || args.length > 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }
    final DBCollection col = (DBCollection) ctx.getStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String expResName = ((Str) args[1]).stringValue();
    final long time =
        FunUtil.getLong(args, 2, "pointInTime", System.currentTimeMillis(), null, true);
    final Instant pointInTime = Instant.ofEpochMilli(time);
    final boolean updatable = FunUtil.getBoolean(args, 3, "updatable", false, false);

    return col.getDocument(pointInTime, expResName, updatable);
  }
}

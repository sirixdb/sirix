package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.XmlDBNode;

/**
 * <p>
 * Function for getting the hash of the current node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:hash($doc as xs:node) as xs:string</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetHash extends AbstractFunction {

  /** Get number of children function name. */
  public final static QNm HASH = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "hash");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetHash(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) throws QueryException {
    final XmlDBNode doc = ((XmlDBNode) args[0]);

    return new Str(String.valueOf(doc.getTrx().getHash()));
  }
}

package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for retrieving the author-id of the provided item. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:author-id($doc as xs:structured-item) as xs:string</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetAuthorID extends AbstractFunction {

  /** Get the author name. */
  public final static QNm AUTHOR_ID = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "author-id");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetAuthorID(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    final var author = doc.getTrx().getUser();

    return author.map(user -> new Str(user.getId().toString())).orElse(null);
  }
}

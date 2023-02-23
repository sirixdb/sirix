package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for retrieving the author name of the current item. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:author-name($doc as xs:structured-item) as xs:string</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetAuthorName extends AbstractFunction {

  /** Get the author name. */
  public final static QNm AUTHOR_NAME = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "author-name");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetAuthorName(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    final var author = doc.getTrx().getUser();

    return author.map(user -> new Str(user.getName())).orElse(null);
  }
}

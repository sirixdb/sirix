package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

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
  public Sequence execute(StaticContext staticContext, QueryContext queryContext, Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);

    final var author = document.getTrx().getUser();

    return author.map(user -> new Str(user.getId().toString())).orElse(null);
  }
}

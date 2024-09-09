package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

/**
 * <p>
 * Function for retrieving the number of descendants of the current node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:descendant-count($doc as xs:structured-item) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetDescendantCount extends AbstractFunction {

  /** Get number of descendants function name. */
  public final static QNm GET_DESCENDANT_COUNT = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "descendant-count");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetDescendantCount(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);

    return new Int64(document.getTrx().getDescendantCount());
  }
}

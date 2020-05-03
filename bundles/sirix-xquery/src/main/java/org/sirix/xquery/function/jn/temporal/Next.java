package org.sirix.xquery.function.jn.temporal;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.TemporalJsonDBItem;

/**
 * <p>
 * Function for selecting a node in the next revision. The parameter is the context node. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>jn:next item.getNext();($doc as json-item()) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Next extends AbstractFunction {

  /** Function name. */
  public final static QNm NEXT = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "next");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Next(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final TemporalJsonDBItem<? extends TemporalJsonDBItem<?>> item = ((TemporalJsonDBItem<?>) args[0]);

    return item.getNext();
  }
}

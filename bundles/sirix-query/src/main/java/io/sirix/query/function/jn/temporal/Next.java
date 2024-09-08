package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.json.TemporalJsonDBItem;

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
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    final TemporalJsonDBItem<? extends TemporalJsonDBItem<?>> item = ((TemporalJsonDBItem<?>) args[0]);

    return item.getNext();
  }
}

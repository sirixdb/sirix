package org.sirix.query.function.jn.temporal;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.*;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.sirix.query.json.TemporalJsonDBItem;

/**
 * <p>
 * Function for selecting a node in the future or the future-or-self. The first parameter is the
 * context node. Second parameter is if ithe current node should be included or not. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>jn:all-times($doc as json-item()) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimes extends AbstractFunction {

  /** Function name. */
  public final static QNm ALL_TIMES = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "all-times");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public AllTimes(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final TemporalJsonDBItem<? extends TemporalJsonDBItem<?>> item = ((TemporalJsonDBItem<?>) args[0]);

    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          Stream<?> s;

          @Override
          public Item next() {
            if (s == null) {
              s = item.getAllTimes();
            }
            return (Item) s.next();
          }

          @Override
          public void close() {
            if (s != null) {
              s.close();
            }
          }
        };
      }
    };
  }
}

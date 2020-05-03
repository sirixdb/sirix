package org.sirix.xquery.function.jn.temporal;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Stream;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.TemporalJsonDBItem;

/**
 * <p>
 * Function for selecting a node in the past or the past-or-self. The first parameter is the context
 * node. Second parameter is if the current node should be included or not. Supported signature is:
 * </p>
 * <ul>
 * <li><code>jn:past($doc as json-item(), $includeSelf as xs:boolean?) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Past extends AbstractFunction {

  /** Function name. */
  public final static QNm PAST = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "past");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Past(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final TemporalJsonDBItem<? extends TemporalJsonDBItem<?>> item = ((TemporalJsonDBItem<?>) args[0]);
    final boolean includeSelf = FunUtil.getBoolean(args, 1, "includeSelf", false, false);

    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          Stream<?> s;

          @Override
          public Item next() {
            if (s == null) {
              s = item.getEarlier(includeSelf);
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

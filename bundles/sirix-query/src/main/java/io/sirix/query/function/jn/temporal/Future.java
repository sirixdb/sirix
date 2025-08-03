package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.*;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.TemporalJsonDBItem;

/**
 * <p>
 * Function for selecting a node in the future or the future-or-self. The first parameter is the
 * context node. Second parameter is if the current node should be included or not. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>jn:future($doc as json-item(), $includeSelf as xs:boolean?) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Future extends AbstractFunction {

  /** Function name. */
  public final static QNm FUTURE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "future");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Future(final QNm name, final Signature signature) {
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
          Stream<?> stream;

          @Override
          public Item next() {
            if (stream == null) {
              stream = item.getFuture(includeSelf);
            }
            return (Item) stream.next();
          }

          @Override
          public void close() {
            if (stream != null) {
              stream.close();
            }
          }
        };
      }
    };
  }
}

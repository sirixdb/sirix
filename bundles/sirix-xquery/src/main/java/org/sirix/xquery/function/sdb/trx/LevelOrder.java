package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;
import org.sirix.xquery.node.XmlDBNode;
import org.sirix.xquery.stream.node.SirixNodeStream;

/**
 * <p>
 * Function for traversing in level-order. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:level-order($node as xs:node) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class LevelOrder extends AbstractFunction {

  /** Get most recent revision function name. */
  public final static QNm LEVEL_ORDER = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "level-order");

  private JsonItemFactory util;

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public LevelOrder(final QNm name, final Signature signature) {
    super(name, signature, true);

    util = new JsonItemFactory();
  }

  void setJsonUtil(JsonItemFactory util) {
    this.util = util;
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);

    final int depth = FunUtil.getInt(args, 1, "depth", Integer.MAX_VALUE, null, false);
    final var axis = LevelOrderAxis.newBuilder(item.getTrx()).filterLevel(depth).includeNonStructuralNodes().build();

    if (item instanceof XmlDBNode) {
      return getXmlNodeSequence(item, axis);
    } else if (item instanceof JsonDBItem) {
      return getJsonItemSequence(item, axis);
    } else {
      throw new IllegalStateException("Node type not supported.");
    }
  }

  private Sequence getJsonItemSequence(final StructuredDBItem<?> item, final LevelOrderAxis axis) {
    final JsonDBItem jsonItem = (JsonDBItem) item;
    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          @Override
          public Item next() {
            if (axis.hasNext()) {
              axis.nextLong();
              return util.getSequence((JsonNodeReadOnlyTrx) axis.getTrx(), jsonItem.getCollection());
            }
            return null;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private Sequence getXmlNodeSequence(final StructuredDBItem<?> item, final LevelOrderAxis axis) {
    final XmlDBNode node = (XmlDBNode) item;
    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          Stream<?> s;

          @Override
          public Item next() {
            if (s == null) {
              s = new SirixNodeStream(axis, node.getCollection());
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

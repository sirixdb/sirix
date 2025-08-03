package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.*;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.LevelOrderAxis;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.stream.node.SirixNodeStream;

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
          Stream<?> stream;

          @Override
          public Item next() {
            if (stream == null) {
              stream = new SirixNodeStream(axis, node.getCollection());
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

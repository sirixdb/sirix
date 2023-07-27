package io.sirix.query.function.xml.index;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.SortedNodeSequence;
import io.sirix.query.function.xml.XMLFun;

import java.util.Comparator;

/**
 * <p>
 * Function for sorting a sequence. This function returns the given sequence in sorted order
 * regarding the document order.
 * </p>
 * <p>
 * The signature is:
 * </p>
 * <ul>
 * <li><code>sdb:sort($sequence as node()*) as node()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SortByDocOrder extends AbstractFunction {

  /** Sort by document order name. */
  public final static QNm SORT = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "sort");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public SortByDocOrder(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final Comparator<Tuple> comparator = (o1, o2) -> ((Node<?>) o1).cmp((Node<?>) o2);

    return new SortedNodeSequence(comparator, args[0], true);
  }
}

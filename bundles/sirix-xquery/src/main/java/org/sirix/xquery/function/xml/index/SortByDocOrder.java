package org.sirix.xquery.function.xml.index;

import java.util.Comparator;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.SortedNodeSequence;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.node.Node;
import org.sirix.xquery.function.xml.XMLFun;

/**
 * <p>
 * Function for sorting a sequence. This function returns the given sequence in sorted order
 * regarding the document order.
 *
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

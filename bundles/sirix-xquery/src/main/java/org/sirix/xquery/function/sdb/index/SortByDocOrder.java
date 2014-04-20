package org.sirix.xquery.function.sdb.index;

import java.util.Comparator;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.SortedNodeSequence;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for sorting a sequence. If successful, this function returns the
 * path-index number. Otherwise it returns -1.
 * 
 * Supported signatures are:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:sort($sequence as xs:anyType*) as xs:anyType*</code>
 * </li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class SortByDocOrder extends AbstractFunction {

	/** Sort by document order name. */
	public final static QNm SORT = new QNm(SDBFun.SDB_NSURI,
			SDBFun.SDB_PREFIX, "sort");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public SortByDocOrder(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		final Comparator<Tuple> comparator = new Comparator<Tuple>() {
			@Override
			public int compare(Tuple o1, Tuple o2) {
				return ((Node<?>) o1).cmp((Node<?>) o2);
			}
		};

		return new SortedNodeSequence(comparator, args[0], true);
	}
}

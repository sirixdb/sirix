package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

/**
 * <p>
 * Function for getting the most recent revision. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:most-recent-revision($doc as xs:structured-item) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetMostRecentRevision extends AbstractFunction {

	/** Get most recent revision function name. */
	public final static QNm MOST_RECENT_REVISION = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "most-recent-revision");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public GetMostRecentRevision(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

		return new Int32(doc.getTrx().getResourceSession().getMostRecentRevisionNumber());
	}
}

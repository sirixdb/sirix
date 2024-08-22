package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

/**
 * <p>
 * Function for getting the hash of the current node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:hash($doc as xs:structured-item) as xs:string</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetHash extends AbstractFunction {

	/** Get number of children function name. */
	public final static QNm HASH = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "hash");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public GetHash(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
		final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

		return new Str(String.valueOf(doc.getTrx().getHash()));
	}
}

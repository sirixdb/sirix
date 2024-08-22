package io.sirix.query.function.xml.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;

/**
 * <p>
 * Function for getting the number of namespaces of the current node. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>xml:get-namespace-count($doc as xs:structured-item) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetNamespaceCount extends AbstractFunction {

	/** Get namespcae count function name. */
	public final static QNm GET_NAMESPACE_COUNT = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "namespace-count");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public GetNamespaceCount(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		final XmlDBNode doc = ((XmlDBNode) args[0]);

		return new Int32(doc.getTrx().getNamespaceCount());
	}
}

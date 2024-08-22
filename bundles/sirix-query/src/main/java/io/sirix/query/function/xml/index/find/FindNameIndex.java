package io.sirix.query.function.xml.index.find;

import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.Cast;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;

import java.util.Optional;

/**
 * <p>
 * Function for finding a name index. If successful, this function returns the
 * name-index number. Otherwise it returns -1.
 * </p>
 * <p>
 * Supported signatures are:
 * </p>
 * <ul>
 * <li><code>xml:find-name-index($doc as node(), $name as xs:QName) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindNameIndex extends AbstractFunction {

	/** CAS index function name. */
	public final static QNm FIND_NAME_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "find-name-index");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public FindNameIndex(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
		final XmlDBNode doc = (XmlDBNode) args[0];
		final XmlNodeReadOnlyTrx rtx = doc.getTrx();
		final XmlIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
		}

		final QNm qnm = (QNm) Cast.cast(sctx, (Atomic) args[1], Type.QNM, false);
		final Optional<IndexDef> indexDef = controller.getIndexes().findNameIndex(qnm);

		return indexDef.map(def -> new Int32(def.getID())).orElseGet(() -> new Int32(-1));
	}
}

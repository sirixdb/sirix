package io.sirix.query.function.xml.index.create;

import com.google.common.collect.ImmutableSet;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Function for creating name indexes on stored documents, optionally restricted
 * to a set of included {@code QNm}s. If successful, this function returns
 * statistics about the newly created index as an XML fragment. Supported
 * signatures are:<br>
 * <ul>
 * <li><code>xml:create-name-index($doc as node(), $include as xs:QName*) as
 * node()</code></li>
 * <li><code>xml:create-name-index($doc as node()) as node()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class CreateNameIndex extends AbstractFunction {

	/** Path index function name. */
	public final static QNm CREATE_NAME_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "create-name-index");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public CreateNameIndex(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		if (args.length != 2 && args.length != 3) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final XmlDBNode doc = ((XmlDBNode) args[0]);
		final XmlNodeReadOnlyTrx rtx = doc.getTrx();
		final XmlResourceSession manager = rtx.getResourceSession();

		final Optional<XmlNodeTrx> optionalWriteTrx = manager.getNodeTrx();
		final XmlNodeTrx wtx = optionalWriteTrx.orElseGet(() -> manager.beginNodeTrx());

		if (rtx.getRevisionNumber() < manager.getMostRecentRevisionNumber()) {
			wtx.revertTo(rtx.getRevisionNumber());
		}

		final XmlIndexController controller = wtx.getResourceSession()
				.getWtxIndexController(wtx.getRevisionNumber() - 1);

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
		}

		final Set<QNm> include = new HashSet<>();
		if (args.length > 1 && args[1] != null) {
			final Iter it = args[1].iterate();
			Item next = it.next();
			while (next != null) {
				include.add((QNm) next);
				next = it.next();
			}
		}

		final IndexDef idxDef = IndexDefs.createSelectiveNameIdxDef(include,
				controller.getIndexes().getNrOfIndexDefsWithType(IndexType.NAME), IndexDef.DbType.XML);
		try {
			controller.createIndexes(ImmutableSet.of(idxDef), wtx);
		} catch (final SirixIOException e) {
			throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
		}
		return idxDef.materialize();
	}

}

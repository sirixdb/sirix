package io.sirix.query.function.jn.index.create;

import io.sirix.query.json.JsonDBItem;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.*;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * <p>
 * Function for creating CAS indexes on stored documents, optionally restricted
 * to a set of paths and a content type. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:create-cas-index($doc as json-item(), $type as xs:string?, $paths as xs:string*) as json-item()</code></li>
 * <li><code>jn:create-cas-index($doc as json-item(), $type as xs:string?) as json-item()</code></li>
 * <li><code>jn:create-cas-index($doc as json-item()) as json-item()</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CreateCASIndex extends AbstractFunction {

	/** CAS index function name. */
	public final static QNm CREATE_CAS_INDEX = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-cas-index");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public CreateCASIndex(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
		if (args.length != 2 && args.length != 3) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final JsonDBItem doc = (JsonDBItem) args[0];
		final JsonNodeReadOnlyTrx rtx = doc.getTrx();
		final JsonResourceSession manager = rtx.getResourceSession();

		final Optional<JsonNodeTrx> optionalWriteTrx = manager.getNodeTrx();
		final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(manager::beginNodeTrx);

		if (rtx.getRevisionNumber() < manager.getMostRecentRevisionNumber()) {
			wtx.revertTo(rtx.getRevisionNumber());
		}

		final JsonIndexController controller = wtx.getResourceSession()
				.getWtxIndexController(wtx.getRevisionNumber() - 1);

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
		}

		Type type = null;
		if (args[1] != null) {
			final QNm name = new QNm(Namespaces.XS_NSURI, ((Str) args[1]).stringValue());
			type = sctx.getTypes().resolveAtomicType(name);
		}

		final Set<Path<QNm>> paths = new HashSet<>();
		if (args.length == 3 && args[2] != null) {
			final Iter it = args[2].iterate();
			Item next = it.next();
			while (next != null) {
				paths.add(Path.parse(((Str) next).stringValue(), io.brackit.query.util.path.PathParser.Type.JSON));
				next = it.next();
			}
		}

		final IndexDef idxDef = IndexDefs.createCASIdxDef(false, type, paths,
				controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS), IndexDef.DbType.JSON);
		try {
			controller.createIndexes(Set.of(idxDef), wtx);
		} catch (final SirixIOException e) {
			throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
		}

		return idxDef.materialize();
	}
}

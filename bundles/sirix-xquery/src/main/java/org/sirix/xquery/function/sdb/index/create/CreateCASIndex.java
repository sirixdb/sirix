package org.sirix.xquery.function.sdb.index.create;

import java.util.HashSet;
import java.util.Set;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.IndexController;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexDefs;
import org.sirix.index.IndexType;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Function for creating CAS indexes on stored documents, optionally restricted
 * to a set of paths and a content type. If successful, this function returns
 * statistics about the newly created index as an XML fragment. Supported
 * signatures are:</br>
 * <ul>
 * <li>
 * <code>bdb:create-cas-index($coll as xs:string, $res as xs:string, $type as xs:string?, 
 * $paths as xs:string*) as node()</code></li>
 * <li>
 * <code>bdb:create-cas-index($coll as xs:string, $res as xs:string, $type as xs:string?) 
 * as node()</code></li>
 * <li>
 * <code>bdb:create-cas-index($coll as xs:string, $res as xs:string) as node()</code>
 * </li>
 * </ul>
 * 
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 * 
 */
public final class CreateCASIndex extends AbstractFunction {

	/** CAS index function name. */
	public final static QNm CREATE_CAS_INDEX = new QNm(SDBFun.SDB_NSURI,
			SDBFun.SDB_PREFIX, "create-cas-index");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public CreateCASIndex(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		if (args.length != 2 && args.length != 3 && args.length != 4) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}
		final DBCollection col = (DBCollection) ctx.getStore().lookup(
				((Str) args[0]).stringValue());

		if (col == null) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		IndexController controller = null;
		final Iter docs = col.iterate();
		DBNode doc = (DBNode) docs.next();

		final String expResName = ((Str) args[1]).stringValue();

		try {
			while (doc != null) {
				if (doc.getTrx().getSession().getResourceConfig().getResource()
						.getName().equals(expResName)) {
					controller = doc.getTrx().getSession().getIndexController();
					break;
				}
				doc = (DBNode) docs.next();
			}
		} finally {
			docs.close();
		}

		if (!(doc.getTrx() instanceof NodeWriteTrx)) {
			throw new QueryException(new QNm("Collection must be updatable!"));
		}

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: "
					+ ((Str) args[1]).stringValue()));
		}

		Type type = null;
		if (args.length > 2 && args[2] != null) {
			final QNm name = new QNm(Namespaces.XS_NSURI,
					((Str) args[2]).stringValue());
			type = sctx.getTypes().resolveAtomicType(name);
		}

		final Set<Path<QNm>> paths = new HashSet<>();
		if (args.length == 4 && args[3] != null) {
			final Iter it = args[3].iterate();
			Item next = it.next();
			while (next != null) {
				paths.add(Path.parse(((Str) next).stringValue()));
				next = it.next();
			}
		}

		final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Optional
				.fromNullable(type), paths, controller.getIndexes()
				.getNrOfIndexDefsWithType(IndexType.CAS));
		try {
			controller.createIndexes(ImmutableSet.of(idxDef),
					(NodeWriteTrx) doc.getTrx());
		} catch (final SirixIOException e) {
			throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
		}
		return idxDef.materialize();
	}
}

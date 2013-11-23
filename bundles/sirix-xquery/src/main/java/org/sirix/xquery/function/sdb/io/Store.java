package org.sirix.xquery.function.sdb.io;

import java.io.IOException;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.node.parser.StreamSubtreeParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.type.AnyNodeType;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.ElementType;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBStore;

import com.google.common.base.Optional;

/**
 * <p>
 * Function for storing a document in a collection/database. The Supported
 * signature is:
 * </p>
 * 
 * <pre>
 * <code>sdb:store($coll as xs:string, $res as xs:string, $fragment as xs:node, $create-new as xs:boolean?) as ()</code>
 * </pre>
 * 
 * @author Johannes Lichtenberger
 * 
 */
@FunctionAnnotation(description = "Store the given fragments in a collection. "
		+ "If explicitly required or if the collection does not exist, "
		+ "a new collection will be created. ", parameters = { "$coll", "$res",
		"$fragments", "$create-new" })
public final class Store extends AbstractFunction {

	/** CAS index function name. */
	public final static QNm STORE = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX,
			"store");

	/**
	 * Constructor.
	 * 
	 * @param createNew
	 *          determines if a new collection has to be created or not
	 */
	public Store(final boolean createNew) {
		this(STORE, createNew);
	}

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the function name
	 * @param createNew
	 *          determines if a new collection has to be created or not
	 */
	public Store(final QNm name, final boolean createNew) {
		super(name, createNew ? new Signature(new SequenceType(ElementType.ELEMENT,
				Cardinality.ZeroOrOne), new SequenceType(AtomicType.STR,
				Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany))
				: new Signature(new SequenceType(ElementType.ELEMENT,
						Cardinality.ZeroOrOne), new SequenceType(AtomicType.STR,
						Cardinality.One),
						new SequenceType(AtomicType.STR, Cardinality.One),
						new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany),
						new SequenceType(AtomicType.BOOL, Cardinality.One)), true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
			throws QueryException {
		try {
			final String collName = FunUtil.getString(args, 0, "collName",
					"collection", null, true);
			final Sequence nodes = args[2];
			if (nodes == null)
				throw new QueryException(new QNm("No sequence of nodes specified!"));
			final boolean createNew = args.length == 4 ? args[3].booleanValue() : true;
			final String resName = FunUtil.getString(args, 1, "resName", "resource",
					null, createNew ? false : true);

			final DBStore store = (DBStore) ctx.getStore();
			if (createNew) {
				create(store, collName, resName, nodes);
			} else {
				try {
					final DBCollection coll = (DBCollection) store.lookup(collName);
					add(store, coll, resName, nodes);
				} catch (DocumentException e) {
					// collection does not exist
					create(store, collName, resName, nodes);
				}
			}

			return null;
		} catch (final Exception e) {
			throw new QueryException(new QNm(e.getMessage()), e);
		}
	}

	private void add(final org.brackit.xquery.xdm.Store store,
			final DBCollection coll, final String resName, final Sequence nodes)
			throws DocumentException, IOException {
		if (nodes instanceof Node) {
			final Node<?> n = (Node<?>) nodes;
			coll.add(resName, new StoreParser(n));
		} else {
			final ParserStream parsers = new ParserStream(nodes);
			try {
				for (SubtreeParser parser = parsers.next(); parser != null; parser = parsers
						.next()) {
					coll.add(resName, parser);
				}
			} finally {
				parsers.close();
			}
		}
	}

	private void create(final DBStore store, final String collName,
			final String resName, final Sequence nodes) throws DocumentException,
			IOException {
		if (nodes instanceof Node) {
			final Node<?> n = (Node<?>) nodes;
			store.create(collName, Optional.of(resName), new StoreParser(n));
		} else {
			store.create(collName, new ParserStream(nodes));
		}
	}

	private static class StoreParser extends StreamSubtreeParser {
		private final boolean intercept;

		public StoreParser(Node<?> node) throws DocumentException {
			super(node.getSubtree());
			intercept = (node.getKind() != Kind.DOCUMENT);
		}

		@Override
		public void parse(SubtreeHandler handler) throws DocumentException {
			if (intercept) {
				handler = new InterceptorHandler(handler);
			}
			super.parse(handler);
		}
	}

	private static class InterceptorHandler implements SubtreeHandler {
		private final SubtreeHandler handler;

		public InterceptorHandler(SubtreeHandler handler) {
			this.handler = handler;
		}

		public void beginFragment() throws DocumentException {
			handler.beginFragment();
			handler.startDocument();
		}

		public void endFragment() throws DocumentException {
			handler.endDocument();
			handler.endFragment();
		}

		public void startDocument() throws DocumentException {
			handler.startDocument();
		}

		public void endDocument() throws DocumentException {
			handler.endDocument();
		}

		public void text(Atomic content) throws DocumentException {
			handler.text(content);
		}

		public void comment(Atomic content) throws DocumentException {
			handler.comment(content);
		}

		public void processingInstruction(QNm target, Atomic content)
				throws DocumentException {
			handler.processingInstruction(target, content);
		}

		public void startMapping(String prefix, String uri)
				throws DocumentException {
			handler.startMapping(prefix, uri);
		}

		public void endMapping(String prefix) throws DocumentException {
			handler.endMapping(prefix);
		}

		public void startElement(QNm name) throws DocumentException {
			handler.startElement(name);
		}

		public void endElement(QNm name) throws DocumentException {
			handler.endElement(name);
		}

		public void attribute(QNm name, Atomic value) throws DocumentException {
			handler.attribute(name, value);
		}

		public void begin() throws DocumentException {
			handler.begin();
		}

		public void end() throws DocumentException {
			handler.end();
		}

		public void fail() throws DocumentException {
			handler.fail();
		}
	}

	private static class ParserStream implements Stream<SubtreeParser> {
		private final Iter it;

		public ParserStream(final Sequence locs) {
			it = locs.iterate();
		}

		@Override
		public SubtreeParser next() throws DocumentException {
			try {
				final Item i = it.next();
				if (i == null) {
					return null;
				}
				if (i instanceof Node<?>) {
					final Node<?> n = (Node<?>) i;
					return new StoreParser(n);
				} else {
					throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
							"Cannot create subtree parser for item of type: %s", i.itemType());
				}
			} catch (QueryException e) {
				throw new DocumentException(e);
			}
		}

		@Override
		public void close() {
			it.close();
		}
	}
}

package org.sirix.xquery;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Optional;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.module.Module;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.xquery.node.DBNode;

public final class SirixXQuery {
	public static final String DEBUG_CFG = "org.brackit.xquery.debug";
	public static final String DEBUG_DIR_CFG = "org.brackit.xquery.debugDir";
	public static boolean DEBUG = Cfg.asBool(DEBUG_CFG, false);
	public static String DEBUG_DIR = Cfg.asString(DEBUG_DIR_CFG, "debug/");

	private final Module module;
	private boolean prettyPrint;

	public SirixXQuery(Module module) {
		this.module = module;
	}

	public SirixXQuery(String query) throws QueryException {
		this.module = new CompileChain().compile(query);
	}

	public SirixXQuery(CompileChain chain, String query) throws QueryException {
		this.module = chain.compile(query);
	}

	public Module getModule() {
		return module;
	}

	public Sequence execute(QueryContext ctx) throws QueryException {
		return run(ctx, true);
	}

	public Sequence evaluate(QueryContext ctx) throws QueryException {
		return run(ctx, false);
	}

	private Sequence run(QueryContext ctx, boolean lazy) throws QueryException {
		Expr body = module.getBody();
		if (body == null) {
			throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
					"Module does not contain a query body.");
		}
		Sequence result = body.evaluate(ctx, new TupleImpl());

		if ((!lazy) || (body.isUpdating())) {
			// iterate possibly lazy result sequence to "pull-in" all pending
			// updates
			if ((result != null) && (!(result instanceof Item))) {
				Iter it = result.iterate();
				try {
					while (it.next() != null);
				} finally {
					it.close();
				}
			}

			ctx.applyUpdates();

			if (!ctx.getUpdateList().list().isEmpty()) {
				commit(ctx.getUpdateList().list().get(0).getTarget());
			}
		}

		return result;
	}

	private void commit(Sequence item) {
		if (item instanceof DBNode) {
			final Optional<XdmNodeWriteTrx> trx =
					((DBNode) item).getTrx().getResourceManager().getNodeWriteTrx();
			trx.ifPresent(XdmNodeWriteTrx::commit);
		}
	}

	public void serialize(QueryContext ctx, PrintStream out) throws QueryException {
		serialize(ctx, new PrintWriter(out));
	}

	public void serialize(QueryContext ctx, PrintWriter out) throws QueryException {
		Sequence result = run(ctx, true);
		if (result == null) {
			return;
		}
		StringSerializer serializer = new StringSerializer(out);
		serializer.setFormat(prettyPrint);
		serializer.serialize(result);
	}

	public void serialize(QueryContext ctx, Serializer serializer) throws QueryException {
		Sequence result = run(ctx, true);
		if (result == null) {
			return;
		}
		serializer.serialize(result);
	}

	public boolean isPrettyPrint() {
		return prettyPrint;
	}

	public SirixXQuery prettyPrint() {
		this.prettyPrint = true;
		return this;
	}
}

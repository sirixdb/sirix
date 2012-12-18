package org.sirix.xquery.compiler.optimizer;

import java.util.Map;

import javax.annotation.Nonnull;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.optimizer.Stage;
import org.brackit.xquery.compiler.optimizer.TopDownOptimizer;
import org.brackit.xquery.module.StaticContext;
import org.sirix.api.NodeReadTrx;
import org.sirix.xquery.compiler.optimizer.walker.MultiChildStep;

public class SirixOptimizer extends TopDownOptimizer {

	public SirixOptimizer(final @Nonnull Map<QNm, Str> options,
			final @Nonnull NodeReadTrx rtx) {
		super(options);
		// perform index matching as last step
		getStages().add(new Stage() {
			@Override
			public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
				ast = new MultiChildStep(sctx).walk(ast);
				return ast;
			}

		});
		getStages().add(new IndexMatching(rtx));
	}

	private static class IndexMatching implements Stage {
		private final NodeReadTrx mRtx;

		public IndexMatching(final @Nonnull NodeReadTrx rtx) {
			mRtx = rtx;
		}

		@Override
		public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
			// TODO add rules for index resolution here
			return ast;
		}
	}
}

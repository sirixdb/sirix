package org.sirix.xquery.compiler.optimizer;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.optimizer.Stage;
import org.brackit.xquery.compiler.optimizer.TopDownOptimizer;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.json.JsonDBStore;
import org.sirix.xquery.node.XmlDBStore;

import java.util.Map;

public final class SirixOptimizer extends TopDownOptimizer {

  public SirixOptimizer(final Map<QNm, Str> options, final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    super(options);
    // perform index matching as last step
    // getStages().add(new Stage() {
    // @Override
    // public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    // ast = new MultiChildStep(sctx).walk(ast);
    // return ast;
    // }
    //
    // });
    getStages().add(new IndexMatching(nodeStore));
  }

  private static class IndexMatching implements Stage {
    // private final DBStore mStore;

    public IndexMatching(final XmlDBStore store) {
      // mStore = store;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
      // TODO add rules for index resolution here
      ast.display();
      return ast;
    }
  }
}

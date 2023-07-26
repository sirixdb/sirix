package org.sirix.query.compiler.optimizer;

import java.util.Map;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.optimizer.Stage;
import org.brackit.xquery.compiler.optimizer.TopDownOptimizer;
import org.brackit.xquery.module.StaticContext;
import org.sirix.query.compiler.optimizer.walker.json.JsonCASStep;
import org.sirix.query.compiler.optimizer.walker.json.JsonObjectKeyNameStep;
import org.sirix.query.compiler.optimizer.walker.json.JsonPathStep;
import org.sirix.query.json.JsonDBStore;
import org.sirix.query.node.XmlDBStore;

public final class SirixOptimizer extends TopDownOptimizer {

  public SirixOptimizer(final Map<QNm, Str> options, final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    super(options);
    // Perform index matching as last step.
    getStages().add(new IndexMatching(nodeStore, jsonItemStore));
  }

  private static class IndexMatching implements Stage {
    private final XmlDBStore xmlNodeStore;

    private final JsonDBStore jsonItemStore;

    public IndexMatching(final XmlDBStore xmlNodestore, final JsonDBStore jsonItemStore) {
      this.xmlNodeStore = xmlNodestore;
      this.jsonItemStore = jsonItemStore;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
      ast = new JsonCASStep(jsonItemStore).walk(ast);
      ast = new JsonPathStep(jsonItemStore).walk(ast);
      ast = new JsonObjectKeyNameStep(jsonItemStore).walk(ast);

      return ast;
    }
  }
}

package io.sirix.query.compiler.optimizer;

import java.util.Map;

import io.sirix.query.compiler.optimizer.walker.json.JsonPathStep;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.compiler.optimizer.TopDownOptimizer;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JsonCASStep;
import io.sirix.query.compiler.optimizer.walker.json.JsonObjectKeyNameStep;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.XmlDBStore;

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

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

public class SirixOptimizer extends TopDownOptimizer {

  private final XmlDBStore xmlNodeStore;
  private final JsonDBStore jsonItemStore;

  public SirixOptimizer(final Map<QNm, Str> options, final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    super(options);
    this.xmlNodeStore = nodeStore;
    this.jsonItemStore = jsonItemStore;
    // Perform index matching as last step.
    getStages().add(new IndexMatching(nodeStore, jsonItemStore));
  }

  /**
   * Get the JSON database store.
   *
   * @return The JSON database store
   */
  protected JsonDBStore getJsonDBStore() {
    return jsonItemStore;
  }

  /**
   * Get the XML node store.
   *
   * @return The XML node store
   */
  protected XmlDBStore getXmlDBStore() {
    return xmlNodeStore;
  }

  /**
   * Add an optimization stage at a specific position.
   *
   * <p>This method allows subclasses (e.g., sirix-enterprise) to inject
   * custom optimization stages at any position in the optimization pipeline.</p>
   *
   * @param index Position to insert the stage (0-based)
   * @param stage The optimization stage to add
   */
  protected void addStageAt(int index, Stage stage) {
    getStages().add(index, stage);
  }

  /**
   * Add an optimization stage before the index matching stage (at the end).
   *
   * <p>This is a convenience method for adding stages that should run
   * after all other optimizations but before index matching.</p>
   *
   * @param stage The optimization stage to add
   */
  protected void addStageBeforeIndexMatching(Stage stage) {
    // Insert before last stage (IndexMatching)
    final int lastIndex = getStages().size() - 1;
    if (lastIndex >= 0) {
      getStages().add(lastIndex, stage);
    } else {
      getStages().add(stage);
    }
  }

  /**
   * Add an optimization stage at the beginning of the pipeline.
   *
   * <p>Stages added here will run first, before any other optimization.</p>
   *
   * @param stage The optimization stage to add
   */
  protected void addStageFirst(Stage stage) {
    getStages().add(0, stage);
  }

  /**
   * Get the number of optimization stages.
   *
   * @return Number of stages currently in the pipeline
   */
  protected int getStageCount() {
    return getStages().size();
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

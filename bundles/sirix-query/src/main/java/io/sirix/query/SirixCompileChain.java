package io.sirix.query;

import java.util.Map;

import io.sirix.query.compiler.optimizer.SirixOptimizer;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBStore;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.translator.BlockPipelineStrategy;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.util.Cfg;

/**
 * Compile chain for SirixDB queries.
 *
 * <p>Uses parallel (block-based) execution by default. Brackit's
 * {@link BlockPipelineStrategy} leverages ForkJoinPool-based work-stealing to parallelize FLWOR
 * expressions automatically. The strategy only activates for FLWOR PipeExpr AST nodes, so simple
 * queries incur zero overhead.
 *
 * <p>Thread-safety for parallel execution is provided by per-worker read-only transactions:
 * collections wrap raw transactions in thread-safe proxies that transparently obtain per-thread
 * cursors from the resource session's shared pool.
 *
 * @author Johannes Lichtenberger
 */
public final class SirixCompileChain extends CompileChain implements AutoCloseable {
  public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.indexrewrite", true);

  static {
    // define function namespaces and functions in these namespaces
    SDBFun.register();
    XMLFun.register();
    JNFun.register();
  }

  /** The XML node store. */
  private final XmlDBStore nodeStore;

  /** The JSON item store. */
  private final JsonDBStore jsonItemStore;

  /** Whether to use block-based parallel execution. */
  private final boolean parallel;

  /** Whether parallel output must preserve input order. */
  private final boolean ordered;

  // ---- Factory methods (parallel by default) ----

  public static SirixCompileChain create() {
    return new SirixCompileChain(null, null, true, true);
  }

  public static SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixCompileChain(nodeStore, null, true, true);
  }

  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore, true, true);
  }

  public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    return new SirixCompileChain(nodeStore, jsonStore, true, true);
  }

  /**
   * Create a parallel compile chain with ordered output.
   *
   * @param nodeStore the XML node store (or null)
   * @param jsonStore the JSON item store (or null)
   * @return a parallel compile chain
   */
  public static SirixCompileChain createParallel(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    return new SirixCompileChain(nodeStore, jsonStore, true, true);
  }

  /**
   * Create a parallel compile chain with configurable ordering.
   *
   * @param nodeStore the XML node store (or null)
   * @param jsonStore the JSON item store (or null)
   * @param ordered whether parallel output must preserve document order
   * @return a parallel compile chain
   */
  public static SirixCompileChain createParallel(final XmlDBStore nodeStore, final JsonDBStore jsonStore,
      final boolean ordered) {
    return new SirixCompileChain(nodeStore, jsonStore, true, ordered);
  }

  /**
   * Constructor.
   *
   * @param nodeStore the Sirix {@link BasicXmlDBStore}
   * @param jsonItemStore the json item store.
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    this(nodeStore, jsonItemStore, true, true);
  }

  /**
   * Full constructor with parallel execution support.
   *
   * @param nodeStore the XML node store (or null for default)
   * @param jsonItemStore the JSON item store (or null for default)
   * @param parallel whether to use block-based parallel execution
   * @param ordered whether parallel output preserves order (ignored if parallel is false)
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel,
      final boolean ordered) {
    this.nodeStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    this.jsonItemStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    this.parallel = parallel;
    this.ordered = ordered;
  }

  @Override
  protected Translator getTranslator(Map<QNm, Str> options) {
    if (parallel) {
      final BlockPipelineStrategy strategy = new BlockPipelineStrategy();
      strategy.setOrdered(ordered);
      return new SirixTranslator(options, strategy);
    }
    return new SirixTranslator(options);
  }

  @Override
  protected Optimizer getOptimizer(Map<QNm, Str> options) {
    if (!OPTIMIZE) {
      return super.getOptimizer(options);
    }
    return new SirixOptimizer(options, nodeStore, jsonItemStore);
  }

  @Override
  public void close() {
    nodeStore.close();
    jsonItemStore.close();
  }
}

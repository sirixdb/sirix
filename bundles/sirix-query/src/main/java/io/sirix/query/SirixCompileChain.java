package io.sirix.query;

import java.util.Map;

import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.compiler.optimizer.SirixOptimizer;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.translator.SirixPipelineStrategy;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.translator.BlockPipelineStrategy;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.util.Cfg;

/**
 * Compile chain for SirixDB queries.
 *
 * <p>Uses sequential execution by default. Parallel (block-based) execution is available via
 * {@link #createParallel} factory methods for read-only queries. Brackit's
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

  /** The last optimizer instance, retained to access the Mesh after compilation. */
  private SirixOptimizer lastOptimizer;

  /**
   * Optional per-chain {@link SirixVectorizedExecutor}. When non-null,
   * {@link #compile} installs it as Brackit's thread-local vectorized
   * executor for the duration of each compile call — the resulting
   * {@code Expr} tree captures the executor reference at compile time,
   * so the thread-local does not need to be live during execution.
   *
   * <p>Lazily built from {@link #autoExecutorSession} on first compile
   * and reused thereafter. {@code null} when the chain wasn't configured
   * with a session (single-shot / multi-resource workflows fall back to
   * {@link SequentialPipelineStrategy#setVectorizedExecutor}).
   */
  private volatile SirixVectorizedExecutor autoExecutor;

  /** Session for lazy executor construction; {@code null} disables the auto-wiring. */
  private final JsonResourceSession autoExecutorSession;

  // ---- Sequential (default) factory methods ----

  public static SirixCompileChain create() {
    return new SirixCompileChain(null, null, false, true);
  }

  public static SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixCompileChain(nodeStore, null, false, true);
  }

  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore, false, true);
  }

  /**
   * Convenience factory that auto-wires a {@link SirixVectorizedExecutor}
   * against the supplied resource session. Queries compiled through the
   * returned chain receive the analytical fast paths (projection byte-scan,
   * page-skip, PAX SIMD) without the caller having to register the
   * executor via {@link SequentialPipelineStrategy#setVectorizedExecutor}.
   *
   * <p>The executor reference is lazily built on the first
   * {@code compile} and torn down on {@link #close()}. Use this variant
   * for single-resource analytical workloads; for multi-resource queries
   * stay with {@link #createWithJsonStore(JsonDBStore)} and manage the
   * executor explicitly.
   *
   * @param jsonStore the JSON item store
   * @param session   the resource session analytical queries will target;
   *                  the executor uses its current most-recent revision
   */
  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore,
      final JsonResourceSession session) {
    return new SirixCompileChain(null, jsonStore, false, true, session);
  }

  public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    return new SirixCompileChain(nodeStore, jsonStore, false, true);
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
   * Create a parallel compile chain that additionally enables morsel-driven fan-out
   * for PipeExprs that fall out of the vectorized fast path. Morsel wrapping is a
   * process-wide toggle on {@link SequentialPipelineStrategy}, so enabling it here
   * affects all compile chains in the JVM until disabled.
   *
   * @param nodeStore the XML node store (or null)
   * @param jsonStore the JSON item store (or null)
   * @return a parallel compile chain with morsel fan-out enabled
   */
  public static SirixCompileChain createParallelWithMorsel(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    SequentialPipelineStrategy.setMorselEnabled(true);
    return createParallel(nodeStore, jsonStore);
  }

  /**
   * Constructor.
   *
   * @param nodeStore the Sirix {@link BasicXmlDBStore}
   * @param jsonItemStore the json item store.
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    this(nodeStore, jsonItemStore, false, true, null);
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
    this(nodeStore, jsonItemStore, parallel, ordered, null);
  }

  /**
   * Full constructor with parallel execution and optional auto-executor
   * session. When {@code autoExecutorSession} is non-null, queries
   * compiled through this chain transparently receive the vectorized
   * fast paths — see {@link #createWithJsonStore(JsonDBStore, JsonResourceSession)}.
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel,
      final boolean ordered, final JsonResourceSession autoExecutorSession) {
    this.nodeStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    this.jsonItemStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    this.parallel = parallel;
    this.ordered = ordered;
    this.autoExecutorSession = autoExecutorSession;
  }

  /**
   * Resolve the auto-executor, constructing it lazily against
   * {@link #autoExecutorSession} if not yet built. Callers must hold a
   * happens-before guarantee against concurrent construction by the
   * volatile {@link #autoExecutor} write — a second invocation observes
   * the published instance rather than rebuilding.
   */
  private SirixVectorizedExecutor ensureAutoExecutor() {
    if (autoExecutorSession == null) return null;
    SirixVectorizedExecutor exec = autoExecutor;
    if (exec != null) return exec;
    synchronized (this) {
      exec = autoExecutor;
      if (exec == null) {
        exec = new SirixVectorizedExecutor(autoExecutorSession,
            autoExecutorSession.getMostRecentRevisionNumber());
        autoExecutor = exec;
      }
    }
    return exec;
  }

  @Override
  public io.brackit.query.module.Module compile(final String query) throws io.brackit.query.QueryException {
    final SirixVectorizedExecutor exec = ensureAutoExecutor();
    if (exec == null) {
      return super.compile(query);
    }
    // Install the per-thread executor for the compile call. Brackit's
    // SequentialPipelineStrategy.tryVectorizedExpr prefers this over the
    // process-wide static; compiled VectorizedGroupByExpr nodes capture
    // the executor reference, so the thread-local doesn't need to be live
    // during execution.
    SequentialPipelineStrategy.setThreadVectorizedExecutor(exec);
    try {
      return super.compile(query);
    } finally {
      SequentialPipelineStrategy.clearThreadVectorizedExecutor();
    }
  }

  @Override
  protected Translator getTranslator(Map<QNm, Str> options) {
    if (parallel) {
      final BlockPipelineStrategy strategy = new BlockPipelineStrategy();
      strategy.setOrdered(ordered);
      return new SirixTranslator(options, strategy);
    }
    return new SirixTranslator(options, new SirixPipelineStrategy());
  }

  @Override
  protected Optimizer getOptimizer(Map<QNm, Str> options) {
    if (!OPTIMIZE) {
      return super.getOptimizer(options);
    }
    lastOptimizer = new SirixOptimizer(options, nodeStore, jsonItemStore);
    return lastOptimizer;
  }

  /**
   * Get the Mesh containing plan alternatives from the last compilation.
   *
   * @return the Mesh, or null if optimization was disabled or no compilation happened
   */
  public Mesh getMesh() {
    return lastOptimizer != null ? lastOptimizer.getMesh() : null;
  }

  /**
   * Collect histograms for fields that had cache misses during the last optimization.
   * Call this after query execution completes and all resource sessions are closed.
   * The collected histograms are stored in {@link io.sirix.query.compiler.optimizer.stats.StatisticsCatalog}
   * and benefit subsequent queries.
   */
  public void collectPendingHistograms() {
    if (lastOptimizer != null) {
      lastOptimizer.collectPendingHistograms();
    }
  }

  @Override
  public void close() {
    final SirixVectorizedExecutor exec = autoExecutor;
    if (exec != null) {
      try {
        exec.close();
      } catch (final Exception ignored) {
        // Best-effort close — don't mask store-close failures.
      }
      autoExecutor = null;
    }
    nodeStore.close();
    jsonItemStore.close();
  }
}

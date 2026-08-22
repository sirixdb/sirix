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
import io.sirix.query.scan.RevisionTrackingExecutor;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.translator.BlockPipelineStrategy;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.module.Module;
import io.brackit.query.util.Cfg;

/**
 * Compile chain for SirixDB queries.
 *
 * <p>
 * Uses sequential execution by default. Parallel (block-based) execution is available via
 * {@link #createParallel} factory methods for read-only queries. Brackit's
 * {@link BlockPipelineStrategy} leverages ForkJoinPool-based work-stealing to parallelize FLWOR
 * expressions automatically. The strategy only activates for FLWOR PipeExpr AST nodes, so simple
 * queries incur zero overhead.
 *
 * <p>
 * Thread-safety for parallel execution is provided by per-worker read-only transactions:
 * collections wrap raw transactions in thread-safe proxies that transparently obtain per-thread
 * cursors from the resource session's shared pool.
 *
 * <p>
 * Analytical queries get the vectorized fast paths without being asked to. A chain built with a
 * {@link JsonResourceSession} binds an executor to it; a chain built from a store alone reads the
 * resource off each query's own {@code jn:doc}/{@code jn:open} (see
 * {@link StoreBoundExecutorCache}). An executor a caller registered explicitly via
 * {@link SequentialPipelineStrategy#setVectorizedExecutor} always wins over both. Whether a
 * resolved executor may actually serve a given scan stays the decision of
 * {@code SirixVectorizedExecutor.acceptsSource}, so auto-wiring can only ever add speed, never
 * change an answer.
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
   * Optional per-chain {@link SirixVectorizedExecutor}. When non-null, {@link #compile} installs it
   * as Brackit's thread-local vectorized executor for the duration of each compile call — the
   * resulting {@code Expr} tree captures the executor reference at compile time, so the thread-local
   * does not need to be live during execution.
   *
   * <p>
   * Lazily built from {@link #autoExecutorSession} on first compile and reused thereafter.
   * {@code null} when the chain wasn't configured with a session (single-shot / multi-resource
   * workflows fall back to {@link SequentialPipelineStrategy#setVectorizedExecutor}).
   */
  private volatile SirixVectorizedExecutor autoExecutor;

  /** Set before teardown starts so a concurrent compile cannot publish an executor after close. */
  private volatile boolean closed;

  /** Terminal fence and bounded lazy-result cursor pool shared by every revision executor. */
  private final SirixVectorizedExecutor.ExecutionLifecycle executorLifecycle =
      new SirixVectorizedExecutor.ExecutionLifecycle();

  /** Sentinel: resolve the auto-executor revision to the session's most recent at first compile. */
  private static final int MOST_RECENT_REVISION = -1;

  /** Session for lazy executor construction; {@code null} disables the auto-wiring. */
  private final JsonResourceSession autoExecutorSession;

  /**
   * Revision the auto-executor binds to; {@link #MOST_RECENT_REVISION} resolves to the session's most
   * recent revision at first compile.
   */
  private final int autoExecutorRevision;

  /**
   * Kill switch for the store-resolved auto-wiring. On by default: the analytical fast paths were the
   * configuration that got measured, so leaving them off unless a caller named a session meant the
   * benchmarked system and the delivered one were not the same. Set
   * {@code -Dsirix.query.autoVectorize=false} to compile every query through the generic pipeline.
   */
  private static final boolean AUTO_VECTORIZE_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.query.autoVectorize", "true"));

  /**
   * Resolves an executor from the query's own {@code jn:doc}/{@code jn:open} when no session was
   * supplied, so a chain built from a store alone still gets the fast paths. {@code null} when the
   * chain is session-bound (that binding wins) or the auto-wiring is switched off.
   *
   * @see StoreBoundExecutorCache
   */
  private final StoreBoundExecutorCache storeBoundExecutors;

  // ---- Sequential (default) factory methods ----

  public static SirixCompileChain create() {
    return new SirixCompileChain(null, null, false, true);
  }

  public static SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixCompileChain(nodeStore, null, false, true);
  }

  /**
   * Chain over a JSON store, serving every resource in it. Analytical queries still get the
   * vectorized fast paths: the resource is read off each query's own {@code jn:doc}/{@code jn:open}
   * rather than named up front, so a chain that outlives many queries over many resources needs no
   * executor management from the caller. See {@link StoreBoundExecutorCache}.
   *
   * @param jsonStore the JSON item store
   */
  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore, false, true);
  }

  /**
   * Chain over a JSON store that compiles every query to the generic pipeline — no auto-wiring, no
   * vectorized substitution.
   *
   * <p>
   * Exists for the callers that treat "no executor registered" as a statement rather than an
   * omission: a differential test whose ground truth IS the generic pipeline, or an A/B harness
   * measuring what the fast paths are worth. Those callers used to get this from
   * {@link #createWithJsonStore(JsonDBStore)} by saying nothing, which auto-wiring turned into a
   * silent no-op — a differential that compares a fast path against itself proves nothing and does
   * not fail while doing so.
   *
   * @param jsonStore the JSON item store
   */
  public static SirixCompileChain createWithJsonStoreWithoutAutoWiring(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore, false, true, null, MOST_RECENT_REVISION, false);
  }

  /**
   * Convenience factory that auto-wires a {@link SirixVectorizedExecutor} against the supplied
   * resource session. Queries compiled through the returned chain receive the analytical fast paths
   * (projection byte-scan, page-skip, PAX SIMD) without the caller having to register the executor
   * via {@link SequentialPipelineStrategy#setVectorizedExecutor}.
   *
   * <p>
   * The executor reference is lazily built on the first {@code compile} and torn down on
   * {@link #close()}. Use this variant when the caller already holds the session — it saves resolving
   * one per query and works for a resource this chain's store cannot look up. For everything else
   * {@link #createWithJsonStore(JsonDBStore)} now auto-wires per query, including multi-resource
   * workloads.
   *
   * @param jsonStore the JSON item store
   * @param session the resource session analytical queries will target; the executor uses its current
   *        most-recent revision
   */
  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore, final JsonResourceSession session) {
    return new SirixCompileChain(null, jsonStore, false, true, session);
  }

  public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    return new SirixCompileChain(nodeStore, jsonStore, false, true);
  }

  /**
   * Variant of {@link #createWithJsonStore(JsonDBStore, JsonResourceSession)} that carries BOTH
   * stores (XML + JSON) and pins the auto-wired executor to an explicit revision — the shape the REST
   * layer needs: a resource-scoped request may name any committed revision, and analytical serving
   * must answer AT that revision, never a later one.
   *
   * <p>
   * The same single-resource contract applies: queries compiled through the returned chain must only
   * target the supplied session's resource — the analytical detection captures source paths, not
   * resource identity, so the caller has to prove single-resource targeting before using this factory
   * (see the REST layer's serving gate).
   *
   * @param nodeStore the XML node store
   * @param jsonStore the JSON item store
   * @param session the resource session analytical queries will target
   * @param revision the committed revision analytical queries are answered at; {@code >= 1}
   */
  public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore,
      final JsonResourceSession session, final int revision) {
    if (session == null) {
      throw new IllegalArgumentException("session must not be null");
    }
    if (revision < 1) {
      throw new IllegalArgumentException("revision must be >= 1: " + revision);
    }
    final int mostRecent = session.getMostRecentRevisionNumber();
    if (revision > mostRecent) {
      throw new IllegalArgumentException(
          "revision " + revision + " does not exist yet (most recent: " + mostRecent + ")");
    }
    return new SirixCompileChain(nodeStore, jsonStore, false, true, session, revision);
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
   * Create a parallel compile chain that additionally enables morsel-driven fan-out for PipeExprs
   * that fall out of the vectorized fast path. Morsel wrapping is a process-wide toggle on
   * {@link SequentialPipelineStrategy}, so enabling it here affects all compile chains in the JVM
   * until disabled.
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
   * Create a compile chain whose scan-shaped pipelines fan out morsel-driven, over splits of the
   * source itself.
   *
   * <p>
   * Note this is <em>not</em> {@link #createParallelWithMorsel}, despite the name overlap, and the
   * difference is why that method could never do what it claimed. Morsel wrapping lives in
   * {@link SequentialPipelineStrategy}, but {@code createParallelWithMorsel} also asks for the
   * parallel translator — and that installs {@code BlockPipelineStrategy}, which does not extend
   * {@code SequentialPipelineStrategy}. The flag was therefore set and then never consulted: the
   * chain it returns is the block-parallel chain, nothing more. This factory keeps the sequential
   * translator, which is the one that knows how to split.
   *
   * <p>
   * The fan-out replaces the pipeline expression itself, so each worker runs the whole chain — leaf
   * scan, predicates and the return expression — over its own piece of the source. Pieces come from
   * {@code SplittableSequence}; a source that cannot split iterates serially, so this is never slower
   * than the ordinary chain by more than the check.
   *
   * <p>
   * <b>Results are unordered.</b> Morsel wrapping is a process-wide toggle on
   * {@link SequentialPipelineStrategy}, so enabling it here affects every compile chain in the JVM
   * until it is disabled.
   *
   * @param nodeStore the XML node store (or null)
   * @param jsonStore the JSON item store (or null)
   * @return a sequential compile chain with morsel fan-out enabled
   */
  public static SirixCompileChain createWithMorsel(final XmlDBStore nodeStore, final JsonDBStore jsonStore) {
    SequentialPipelineStrategy.setMorselEnabled(true);
    return new SirixCompileChain(nodeStore, jsonStore);
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
   * Full constructor with parallel execution and optional auto-executor session. When
   * {@code autoExecutorSession} is non-null, queries compiled through this chain transparently
   * receive the vectorized fast paths — see
   * {@link #createWithJsonStore(JsonDBStore, JsonResourceSession)}.
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel,
      final boolean ordered, final JsonResourceSession autoExecutorSession) {
    this(nodeStore, jsonItemStore, parallel, ordered, autoExecutorSession, MOST_RECENT_REVISION);
  }

  /**
   * Full constructor with parallel execution, optional auto-executor session and an explicit executor
   * revision ({@link #MOST_RECENT_REVISION} defers resolution to the first compile).
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel,
      final boolean ordered, final JsonResourceSession autoExecutorSession, final int autoExecutorRevision) {
    this(nodeStore, jsonItemStore, parallel, ordered, autoExecutorSession, autoExecutorRevision, true);
  }

  /**
   * Full constructor, with the store-resolved auto-wiring under caller control — see
   * {@link #createWithJsonStoreWithoutAutoWiring(JsonDBStore)} for the only reason to switch it off.
   */
  private SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel,
      final boolean ordered, final JsonResourceSession autoExecutorSession, final int autoExecutorRevision,
      final boolean autoWire) {
    this.nodeStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    this.jsonItemStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    this.parallel = parallel;
    this.ordered = ordered;
    this.autoExecutorSession = autoExecutorSession;
    this.autoExecutorRevision = autoExecutorRevision;
    // A session-bound chain already knows its resource; only a store-only chain has to read it off
    // the query.
    this.storeBoundExecutors = autoWire && AUTO_VECTORIZE_ENABLED && autoExecutorSession == null
        ? new StoreBoundExecutorCache(this.jsonItemStore, executorLifecycle)
        : null;
  }

  /**
   * Resolve the auto-executor, constructing it lazily against {@link #autoExecutorSession} if not yet
   * built. Callers must hold a happens-before guarantee against concurrent construction by the
   * volatile {@link #autoExecutor} write — a second invocation observes the published instance rather
   * than rebuilding.
   */
  private SirixVectorizedExecutor ensureAutoExecutor() {
    if (autoExecutorSession == null || closed && !executorLifecycle.isEnteredByCurrentThread())
      return null;
    SirixVectorizedExecutor exec = autoExecutor;
    // A chain pinned to an explicit revision keeps one executor for its whole life; that revision
    // is immutable, so its caches can never go stale.
    if (autoExecutorRevision != MOST_RECENT_REVISION) {
      if (exec != null)
        return exec;
      synchronized (this) {
        if (closed && !executorLifecycle.isEnteredByCurrentThread()) {
          return null;
        }
        exec = autoExecutor;
        if (exec == null) {
          exec = new SirixVectorizedExecutor(autoExecutorSession, autoExecutorRevision, executorLifecycle);
          autoExecutor = exec;
        }
      }
      return exec;
    }
    // "Most recent" has to mean most recent AT EACH COMPILE, not at the first one. The executor is
    // pinned to a revision and memoises answers for it — aggregates, path statistics, decoded
    // columns — so holding on to the one built before a commit makes every later query on this
    // chain answer from the pre-commit state. It reads as a stale cache long after the write that
    // invalidated it, and only for callers that used the auto-wiring.
    int mostRecent = autoExecutorSession.getMostRecentRevisionNumber();
    if (exec != null && exec.getRevision() == mostRecent)
      return exec;
    SirixVectorizedExecutor superseded = null;
    synchronized (this) {
      if (closed && !executorLifecycle.isEnteredByCurrentThread()) {
        return null;
      }
      // A compile can wait here while another compile publishes a newer revision. Re-read after
      // acquiring the monitor so the delayed compile cannot replace that executor with the stale
      // "latest" revision it observed before waiting.
      mostRecent = autoExecutorSession.getMostRecentRevisionNumber();
      exec = autoExecutor;
      if (exec == null || exec.getRevision() != mostRecent) {
        superseded = exec;
        exec = new SirixVectorizedExecutor(autoExecutorSession, mostRecent, executorLifecycle);
        autoExecutor = exec;
      }
    }
    // Retire the one it replaces after releasing the chain monitor. Dropping the reference instead
    // leaked a worker pool per revision advance, while retiring it under the monitor lets a slow
    // warm-up teardown serialize every compile on this chain. Retirement leaves the shared
    // admission/cursor lifecycle open: old compiled scans degrade inline and old lazy results
    // rebind the chain's bounded consumer cursor to their immutable revision on demand.
    retireExecutorQuietly(superseded);
    return exec;
  }

  @Override
  public Module compile(final String query) throws QueryException {
    if (autoExecutorSession != null) {
      // Install the per-thread executor for the compile call. Brackit's
      // SequentialPipelineStrategy.tryVectorizedExpr prefers this over the
      // process-wide static; compiled VectorizedGroupByExpr nodes capture
      // the executor reference, so the thread-local doesn't need to be live
      // during execution.
      //
      // What they capture is the revision-tracking indirection, not the executor itself: the
      // capture outlives the compile, and an executor is pinned to one revision, so a query
      // compiled once and executed again after a commit would otherwise keep answering from
      // before the write.
      SequentialPipelineStrategy.setThreadVectorizedExecutor(
          new RevisionTrackingExecutor(this::ensureAutoExecutor, executorLifecycle));
      try {
        return super.compile(query);
      } finally {
        SequentialPipelineStrategy.clearThreadVectorizedExecutor();
      }
    }
    if (storeBoundExecutors == null || SequentialPipelineStrategy.getVectorizedExecutor() != null) {
      // Auto-wiring off, or the caller registered an executor of their own — theirs wins, and
      // clearing the thread-local afterwards would throw away a registration we did not make.
      return super.compile(query);
    }
    // Nothing was registered, so whatever is on the thread-local when we return is ours to remove.
    // The executor itself is installed by parse(), which is where the query first exists as a tree.
    try {
      return super.compile(query);
    } finally {
      SequentialPipelineStrategy.clearThreadVectorizedExecutor();
    }
  }

  /**
   * Parse hook: this is the earliest point at which the query exists as a tree, and it runs before
   * the analysis, optimization and translation {@link CompileChain#compile} performs — which is
   * exactly the window in which the vectorized executor has to be registered, because the translator
   * captures it into the compiled expression.
   *
   * <p>
   * Only fires for a store-only chain with nothing already registered; the resolved executor is
   * removed again by {@link #compile}'s {@code finally}. {@code parse} is reachable only from
   * {@code compile} — this class is final — so the registration cannot outlive that call.
   */
  @Override
  protected AST parse(final String query) throws QueryException {
    final AST ast = super.parse(query);
    if (storeBoundExecutors == null || SequentialPipelineStrategy.getVectorizedExecutor() != null) {
      return ast;
    }
    final StoreBoundExecutorCache.DocumentSource named = StoreBoundExecutorCache.firstDocumentSource(ast);
    // No literal jn:doc does not mean no document: a query reading `declare variable $doc external`
    // names its resource nowhere but in the binding the caller already made, which is the ordinary
    // embedding shape and used to lose the fast path outright. See BoundDocumentHint, including why
    // using a hint this query may not honour is safe — the runtime source gate re-checks the actual
    // binding per evaluation and declines to the generic pipeline on any mismatch.
    final StoreBoundExecutorCache.DocumentSource source = named != null
        ? named
        : BoundDocumentHint.peek();
    if (source == null) {
      // Nothing to bind — compile the generic pipeline. Do not eagerly resolve here: every
      // resolver-backed capability/gate below is lifecycle-admitted before it touches a session,
      // and an unavailable document simply makes that wrapper decline the fast path.
      return ast;
    }
    // The compiled expression captures the indirection, so it re-resolves the document's CURRENT
    // revision on every execution — the same thing this query's own jn:doc does — and resolves a
    // separate executor per scanned document, so a query reading two resources is accelerated on
    // both rather than only on the one it named first.
    SequentialPipelineStrategy.setThreadVectorizedExecutor(
        new RevisionTrackingExecutor(() -> storeBoundExecutors.resolve(source),
            ref -> storeBoundExecutors.resolve(ref) == null
                ? null
                : () -> storeBoundExecutors.resolve(ref),
            executorLifecycle));
    return ast;
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

  /** The store-resolved executor cache, or {@code null} when this chain does not auto-wire. */
  StoreBoundExecutorCache storeBoundExecutors() {
    return storeBoundExecutors;
  }

  /**
   * Get the Mesh containing plan alternatives from the last compilation.
   *
   * @return the Mesh, or null if optimization was disabled or no compilation happened
   */
  public Mesh getMesh() {
    return lastOptimizer != null
        ? lastOptimizer.getMesh()
        : null;
  }

  /**
   * Collect histograms for fields that had cache misses during the last optimization. Call this after
   * query execution completes and all resource sessions are closed. The collected histograms are
   * stored in {@link io.sirix.query.compiler.optimizer.stats.StatisticsCatalog} and benefit
   * subsequent queries.
   */
  public void collectPendingHistograms() {
    if (lastOptimizer != null) {
      lastOptimizer.collectPendingHistograms();
    }
  }

  @Override
  public void close() {
    if (executorLifecycle.isEnteredByCurrentThread()) {
      throw new IllegalStateException("Cannot close a Sirix compile chain from its admitted query work");
    }
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    // One fence covers cached, current and already-evicted executors. It rejects work reaching a
    // retired inline executor after terminal close and drains top-level calls admitted beforehand.
    executorLifecycle.closeAndAwait();
    final SirixVectorizedExecutor exec;
    synchronized (this) {
      // An admitted most-recent resolver is allowed to publish after terminal publication. The
      // lifecycle drain above waits for it; capture only afterwards so that late executor is both
      // closed and unlinked rather than retained by an already-closed chain.
      exec = autoExecutor;
      autoExecutor = null;
    }
    closeExecutorQuietly(exec);
    // Fence executor-owned warm-ups and pools before stores close their sessions.
    if (storeBoundExecutors != null) {
      storeBoundExecutors.close();
    }
    nodeStore.close();
    jsonItemStore.close();
  }

  /** Best-effort terminal executor close; always called after the shared chain fence. */
  private static void closeExecutorQuietly(final SirixVectorizedExecutor executor) {
    if (executor == null) {
      return;
    }
    try {
      executor.close();
    } catch (final Exception ignored) {
      // An executor teardown must not mask compilation or store-close failures.
    }
  }

  /** Best-effort executor retirement; the shared chain lifecycle deliberately remains open. */
  private static void retireExecutorQuietly(final SirixVectorizedExecutor executor) {
    if (executor == null) {
      return;
    }
    try {
      executor.retire();
    } catch (final Exception ignored) {
      // Retirement must not mask compilation. Terminal chain close still owns the shared fence.
    }
  }
}

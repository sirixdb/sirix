package io.sirix.query.scan;

import java.util.function.Function;
import java.util.function.Supplier;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.jdm.Sequence;

/**
 * A {@link VectorizedExecutor} that resolves the real, revision-pinned {@link
 * SirixVectorizedExecutor} on every call instead of holding one.
 *
 * <p><b>Why this exists.</b> The translator captures the executor OBJECT into the compiled
 * expression, so whatever is pinned in that object is pinned for the expression's whole life. A
 * {@link SirixVectorizedExecutor} is pinned to one revision and memoises answers for it, so a query
 * compiled once and executed again after a commit kept answering from before the write — while the
 * generic pipeline, whose {@code jn:doc} opens the most recent revision at EXECUTE time, moved on.
 * Re-resolving per compile (which is what fixed the first round of this bug) does nothing for a
 * caller that compiles once and executes many times, which is the normal way to use a prepared
 * query.
 *
 * <p>Capturing this indirection instead makes "most recent" mean most recent when the query RUNS,
 * which is what the pipeline it stands in for does. A source that named an explicit revision
 * resolves to that same immutable snapshot every time, so it is unaffected.
 *
 * <p>The supplier returns {@code null} when the executor can no longer be resolved — the compile
 * chain that owned it has been closed, or the resource has gone away. The last successfully
 * resolved executor is then reused: a compiled query outliving its chain keeps answering from the
 * revision it last saw rather than failing, which matters because most of these entry points are
 * substituted at translate time and have no generic pipeline to fall back to.
 *
 * <p><b>Maintenance.</b> Every method of {@link VectorizedExecutor} must be forwarded here,
 * including ones added later: an un-forwarded method silently inherits the interface default
 * ("unsupported"), which turns a working fast path off — or, at a translate-time-substituted entry
 * point, turns a working query into a failing one. {@code RevisionTrackingExecutorTest} asserts the
 * full set is overridden, so a new interface method fails the build rather than the user.
 */
public final class RevisionTrackingExecutor implements VectorizedExecutor {

  /** Resolves the executor for the current revision; {@code null} when it cannot be resolved. */
  private final Supplier<SirixVectorizedExecutor> resolver;

  /**
   * Resolves a resolver for an ARBITRARY source, so one chain can front every resource a query
   * names. {@code null} for a single-resource binding, which then answers only for its own source.
   */
  private final Function<SourceRef, Supplier<SirixVectorizedExecutor>> perSourceResolver;

  /** Last successfully resolved executor — the fallback once the resolver stops answering. */
  private volatile SirixVectorizedExecutor last;

  /**
   * Executor pinned for the evaluation currently running on this thread.
   *
   * <p>{@code RuntimeSourceGatedExpr} gates and then evaluates as two separate calls into this
   * object, and each would otherwise re-ask for "the most recent revision". A commit landing between
   * them let the gate verify the bound item against revision R — {@code servesWholeDocument}
   * compares the item's revision to the executor's — while the scan then answered from a fresh
   * executor pinned to R+1. Both revisions are legitimate snapshots; using two of them for one
   * evaluation is not. The gate pins what it admitted, and the scan that follows serves from it.
   */
  private final ThreadLocal<SirixVectorizedExecutor> pinnedForEvaluation = new ThreadLocal<>();

  /**
   * @param resolver resolves the revision-pinned executor; must not throw, and returns {@code null}
   *                 when no executor is available
   */
  public RevisionTrackingExecutor(final Supplier<SirixVectorizedExecutor> resolver) {
    this(resolver, null);
  }

  /**
   * Multi-resource variant. {@code perSourceResolver} maps any scan's source identity to a resolver
   * for THAT document, so a query reading two documents gets the fast path on both rather than only
   * on whichever one it named first.
   *
   * @param resolver          resolver for this instance's own binding — the query's first document,
   *                          and what un-annotated scans fall back to
   * @param perSourceResolver resolver factory per source; {@code null} disables multi-resource
   *                          dispatch, and a {@code null} RESULT declines that one source
   */
  public RevisionTrackingExecutor(final Supplier<SirixVectorizedExecutor> resolver,
      final Function<SourceRef, Supplier<SirixVectorizedExecutor>> perSourceResolver) {
    if (resolver == null) {
      throw new IllegalArgumentException("resolver must not be null");
    }
    this.resolver = resolver;
    this.perSourceResolver = perSourceResolver;
  }

  /**
   * The executor to serve this call, or {@code null} when none was ever resolved. Cheap: resolution
   * is a most-recent-revision read plus a map lookup, paid once per query execution rather than per
   * record.
   */
  private SirixVectorizedExecutor current() {
    final SirixVectorizedExecutor pinned = pinnedForEvaluation.get();
    if (pinned != null) {
      return pinned;
    }
    final SirixVectorizedExecutor resolved = resolver.get();
    if (resolved != null) {
      last = resolved;
      return resolved;
    }
    return last;
  }

  /** The executor most recently resolved, for tests and diagnostics; may be {@code null}. */
  public SirixVectorizedExecutor lastResolved() {
    return last;
  }

  // ==================== forwarding ====================

  @Override
  public Sequence executeGroupByCount(final QueryContext ctx, final String[] sourcePath, final String groupField)
      throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executeGroupByCount(ctx, sourcePath, groupField);
  }

  @Override
  public Sequence executeGroupByCountMulti(final QueryContext ctx, final String[] sourcePath,
      final String[] groupFields, final String[] outNames, final String countName, final PredicateNode predicate)
      throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executeGroupByCountMulti(ctx, sourcePath, groupFields, outNames, countName,
                                                               predicate);
  }

  @Override
  public Sequence executeSortedScan(final QueryContext ctx, final String[] sourcePath, final String orderField,
      final String direction) throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executeSortedScan(ctx, sourcePath, orderField, direction);
  }

  @Override
  public Sequence executeAggregate(final QueryContext ctx, final String[] sourcePath, final String func,
      final String field) throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executeAggregate(ctx, sourcePath, func, field);
  }

  @Override
  public Sequence executeCountDistinct(final QueryContext ctx, final String[] sourcePath, final String field)
      throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executeCountDistinct(ctx, sourcePath, field);
  }

  @Override
  public Sequence executePredicateCount(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate) throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executePredicateCount(ctx, sourcePath, predicate);
  }

  @Override
  public Sequence executePredicateGroupByCount(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate, final String groupField) throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executePredicateGroupByCount(ctx, sourcePath, predicate, groupField);
  }

  @Override
  public Sequence executePredicateAggregate(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate, final String func, final String field) throws QueryException {
    final SirixVectorizedExecutor exec = current();
    return exec == null ? null : exec.executePredicateAggregate(ctx, sourcePath, predicate, func, field);
  }

  @Override
  public boolean supportsSortedScan() {
    final SirixVectorizedExecutor exec = current();
    return exec != null && exec.supportsSortedScan();
  }

  @Override
  public boolean supportsMultiKeyGroupBy() {
    final SirixVectorizedExecutor exec = current();
    return exec != null && exec.supportsMultiKeyGroupBy();
  }

  @Override
  public boolean canExecute(final QueryContext ctx) {
    final SirixVectorizedExecutor exec = current();
    return exec != null && exec.canExecute(ctx);
  }

  @Override
  public boolean acceptsPredicate(final String[] sourcePath, final PredicateNode predicate) {
    final SirixVectorizedExecutor exec = current();
    return exec != null && exec.acceptsPredicate(sourcePath, predicate);
  }

  /**
   * Admit a source this chain can reach, not merely the one it is bound to. With multi-resource
   * dispatch the question is whether the store can open the named document at all — the executor
   * that will actually serve it is picked in {@link #executorForSource(SourceRef)} and answers for
   * its own resource, so identity is still checked, just against the right binding.
   */
  @Override
  public boolean acceptsSource(final SourceRef source) {
    if (perSourceResolver != null && source != null && source.kind() == SourceRef.Kind.DOCUMENT) {
      final SirixVectorizedExecutor forSource = resolveForSource(source);
      return forSource != null && forSource.acceptsSource(source);
    }
    final SirixVectorizedExecutor exec = current();
    return exec != null && exec.acceptsSource(source);
  }

  @Override
  public VectorizedExecutor executorForSource(final SourceRef source) {
    if (perSourceResolver == null || source == null || source.kind() != SourceRef.Kind.DOCUMENT) {
      return this;
    }
    final Supplier<SirixVectorizedExecutor> forSource = perSourceResolver.apply(source);
    if (forSource == null || forSource.get() == null) {
      return this;
    }
    // Still revision-tracking, and still able to dispatch, so a nested scan over a third document
    // is reached the same way.
    return new RevisionTrackingExecutor(forSource, perSourceResolver);
  }

  /** The executor bound to {@code source}, or {@code null} when the store cannot reach it. */
  private SirixVectorizedExecutor resolveForSource(final SourceRef source) {
    final Supplier<SirixVectorizedExecutor> forSource = perSourceResolver.apply(source);
    return forSource == null ? null : forSource.get();
  }

  /**
   * Runtime gate. Routed through the same per-source dispatch as the compile-time overload: brackit
   * asks HERE for a {@link SourceRef.Kind#VARIABLE} source, so resolving only the query's first
   * document would leave multi-resource dispatch dead exactly where it is decided at run time.
   *
   * <p>A VARIABLE names no document to dispatch on, so it falls to this instance — whose delegate
   * still checks the bound item's resource and revision itself.
   */
  @Override
  public boolean acceptsSource(final SourceRef source, final QueryContext ctx) {
    if (perSourceResolver != null && source != null && source.kind() == SourceRef.Kind.DOCUMENT) {
      final SirixVectorizedExecutor forSource = resolveForSource(source);
      if (forSource == null || !forSource.acceptsSource(source, ctx)) {
        return false;
      }
      pinnedForEvaluation.set(forSource);
      return true;
    }
    final SirixVectorizedExecutor exec = current();
    if (exec == null || !exec.acceptsSource(source, ctx)) {
      // Nothing will serve this evaluation, so leave no pin behind for the next one.
      pinnedForEvaluation.remove();
      return false;
    }
    pinnedForEvaluation.set(exec);
    return true;
  }
}

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
 * A {@link VectorizedExecutor} that resolves the real, revision-pinned
 * {@link SirixVectorizedExecutor} on every call instead of holding one.
 *
 * <p>
 * <b>Why this exists.</b> The translator captures the executor OBJECT into the compiled expression,
 * so whatever is pinned in that object is pinned for the expression's whole life. A
 * {@link SirixVectorizedExecutor} is pinned to one revision and memoises answers for it, so a query
 * compiled once and executed again after a commit kept answering from before the write — while the
 * generic pipeline, whose {@code jn:doc} opens the most recent revision at EXECUTE time, moved on.
 * Re-resolving per compile (which is what fixed the first round of this bug) does nothing for a
 * caller that compiles once and executes many times, which is the normal way to use a prepared
 * query.
 *
 * <p>
 * Capturing this indirection instead makes "most recent" mean most recent when the query RUNS,
 * which is what the pipeline it stands in for does. A source that named an explicit revision
 * resolves to that same immutable snapshot every time, so it is unaffected.
 *
 * <p>
 * The supplier may transiently return {@code null} when the resource cannot be resolved. The last
 * successfully resolved executor is then reused while its chain lifecycle remains open. Terminal
 * chain close is different: its shared admission fence permanently rejects the forwarded call
 * before any sequential or parallel session work can start.
 *
 * <p>
 * <b>Maintenance.</b> Every method of {@link VectorizedExecutor} must be forwarded here, including
 * ones added later: an un-forwarded method silently inherits the interface default ("unsupported"),
 * which turns a working fast path off — or, at a translate-time-substituted entry point, turns a
 * working query into a failing one. {@code RevisionTrackingExecutorTest} asserts the full set is
 * overridden, so a new interface method fails the build rather than the user.
 */
public final class RevisionTrackingExecutor implements VectorizedExecutor {

  /** Resolves the executor for the current revision; {@code null} when it cannot be resolved. */
  private final Supplier<SirixVectorizedExecutor> resolver;

  /**
   * Resolves a resolver for an ARBITRARY source, so one chain can front every resource a query names.
   * {@code null} for a single-resource binding, which then answers only for its own source.
   */
  private final Function<SourceRef, Supplier<SirixVectorizedExecutor>> perSourceResolver;

  /** Shared terminal fence; admission begins before either resolver can touch its store/session. */
  private final SirixVectorizedExecutor.ExecutionLifecycle executionLifecycle;

  /** Last successfully resolved executor — the fallback once the resolver stops answering. */
  private volatile SirixVectorizedExecutor last;

  /**
   * Executor pinned for the evaluation currently running on this thread, together with the
   * {@link QueryContext} that evaluation belongs to.
   *
   * <p>
   * {@code RuntimeSourceGatedExpr} gates and then evaluates as two separate calls into this object,
   * and each would otherwise re-ask for "the most recent revision". A commit landing between them let
   * the gate verify the bound item against revision R — {@code servesWholeDocument} compares the
   * item's revision to the executor's — while the scan then answered from a fresh executor pinned to
   * R+1. Both revisions are legitimate snapshots; using two of them for one evaluation is not. The
   * gate pins what it admitted, and the scan that follows serves from it.
   *
   * <p>
   * The pin lasts for that pair and no longer, enforced twice over. The scan that consumes it
   * releases it in a {@code finally}, so nothing survives a completed — or a failed — evaluation. And
   * the context is part of the pin, so a pin that somehow outlived its evaluation still cannot answer
   * for a LATER one: a different execution carries a different {@link QueryContext}, the identity
   * check fails, and the executor is re-resolved. Without that, a query compiled once and executed
   * repeatedly would answer from the revision of its first execution after a commit — precisely the
   * staleness this whole class exists to remove.
   *
   * <p>
   * The holder is created once per thread and mutated in place: pinning happens per evaluation, not
   * per record, but it must not put an allocation on a path that is otherwise allocation-free.
   */
  private final ThreadLocal<Pin> pinnedForEvaluation = ThreadLocal.withInitial(Pin::new);

  /** Mutable per-thread pin cell; see {@link #pinnedForEvaluation}. */
  private static final class Pin {
    private QueryContext ctx;
    private SirixVectorizedExecutor executor;

    void set(final QueryContext ctx, final SirixVectorizedExecutor executor) {
      this.ctx = ctx;
      this.executor = executor;
    }

    void clear() {
      ctx = null;
      executor = null;
    }
  }

  /**
   * @param resolver resolves the revision-pinned executor; must not throw, and returns {@code null}
   *        when no executor is available
   * @param executionLifecycle lifecycle shared by the resolver's executors and owning compile chain
   */
  public RevisionTrackingExecutor(final Supplier<SirixVectorizedExecutor> resolver,
      final SirixVectorizedExecutor.ExecutionLifecycle executionLifecycle) {
    this(resolver, null, executionLifecycle);
  }

  /**
   * Multi-resource variant. {@code perSourceResolver} maps any scan's source identity to a resolver
   * for THAT document, so a query reading two documents gets the fast path on both rather than only
   * on whichever one it named first.
   *
   * @param resolver resolver for this instance's own binding — the query's first document, and what
   *        un-annotated scans fall back to
   * @param perSourceResolver resolver factory per source; {@code null} disables multi-resource
   *        dispatch, and a {@code null} RESULT declines that one source
   * @param executionLifecycle lifecycle shared by every per-source executor and the owning chain
   */
  public RevisionTrackingExecutor(final Supplier<SirixVectorizedExecutor> resolver,
      final Function<SourceRef, Supplier<SirixVectorizedExecutor>> perSourceResolver,
      final SirixVectorizedExecutor.ExecutionLifecycle executionLifecycle) {
    if (resolver == null) {
      throw new IllegalArgumentException("resolver must not be null");
    }
    if (executionLifecycle == null) {
      throw new IllegalArgumentException("executionLifecycle must not be null");
    }
    this.resolver = resolver;
    this.perSourceResolver = perSourceResolver;
    this.executionLifecycle = executionLifecycle;
  }

  /**
   * The executor to serve this call, or {@code null} when none was ever resolved. Cheap: resolution
   * is a most-recent-revision read plus a map lookup, paid once per query execution rather than per
   * record.
   */
  private SirixVectorizedExecutor current() {
    final SirixVectorizedExecutor pinned = pinnedForEvaluation.get().executor;
    if (pinned != null) {
      return pinned;
    }
    return resolve();
  }

  /**
   * The executor to serve a call that belongs to {@code ctx}. The pin is honoured only for the
   * evaluation that created it; a pin left behind by an earlier execution is DROPPED here rather than
   * served from, which is what makes a query re-executed after a commit see the new revision.
   */
  private SirixVectorizedExecutor current(final QueryContext ctx) {
    final Pin pin = pinnedForEvaluation.get();
    final SirixVectorizedExecutor pinned = pin.executor;
    if (pinned != null) {
      if (pin.ctx == ctx) {
        return pinned;
      }
      pin.clear();
    }
    return resolve();
  }

  private SirixVectorizedExecutor resolve() {
    final SirixVectorizedExecutor resolved = resolver.get();
    if (resolved != null) {
      last = resolved;
      return resolved;
    }
    return last;
  }

  /** Resolve and locally fence a forwarded call whose chain lifecycle is already admitted. */
  private SirixVectorizedExecutor currentAndEnter(final QueryContext ctx) {
    final SirixVectorizedExecutor executor = current(ctx);
    if (executor != null) {
      executor.enterExecution();
    }
    return executor;
  }

  private void leaveAndUnpin(final SirixVectorizedExecutor executor, final QueryContext ctx) {
    try {
      if (executor != null) {
        executor.leaveExecution();
      }
    } finally {
      try {
        unpin(ctx);
      } finally {
        executionLifecycle.leave();
      }
    }
  }

  /**
   * Admit before resolution. The later executor admission is reentrant on this same lifecycle, so
   * this moves (rather than duplicates) the one global CAS already paid by a forwarded scan.
   */
  private void enterResolution() {
    try {
      executionLifecycle.enter();
    } catch (final IllegalStateException e) {
      clearPinAfterTerminalRejection();
      throw e;
    }
  }

  /**
   * Context-aware admission for a gated evaluation. If terminal publication wins between its gate
   * and scan, no scan finally block will run, so rejected admission must release that gate's pin
   * here rather than retaining its QueryContext on the worker thread indefinitely.
   */
  private void enterResolution(final QueryContext ctx) {
    try {
      executionLifecycle.enter();
    } catch (final IllegalStateException e) {
      clearPinAfterTerminalRejection();
      throw e;
    }
  }

  /** Terminal close is irreversible, so no outer evaluation can still own a useful pin. */
  private void clearPinAfterTerminalRejection() {
    if (executionLifecycle.isClosed()) {
      pinnedForEvaluation.get().clear();
    }
  }

  /**
   * Releases the pin the gate took for {@code ctx}. Called from every scan entry point in a
   * {@code finally}: the gate and the scan it admitted are one evaluation, and the pin must not
   * outlive it — not on a normal return, and not on the exception that unwinds it.
   *
   * <p>
   * A pin belonging to a different context is left alone: it belongs to an evaluation further out on
   * this thread's stack, which will release its own.
   */
  private void unpin(final QueryContext ctx) {
    final Pin pin = pinnedForEvaluation.get();
    if (pin.executor != null && pin.ctx == ctx) {
      pin.clear();
    }
  }

  /** The executor most recently resolved, for tests and diagnostics; may be {@code null}. */
  public SirixVectorizedExecutor lastResolved() {
    return last;
  }

  // ==================== forwarding ====================

  @Override
  public Sequence executeGroupByCount(final QueryContext ctx, final String[] sourcePath, final String groupField)
      throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeGroupByCount(ctx, sourcePath, groupField);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executeGroupByCountMulti(final QueryContext ctx, final String[] sourcePath,
      final String[] groupFields, final String[] outNames, final String countName, final PredicateNode predicate)
      throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeGroupByCountMulti(ctx, sourcePath, groupFields, outNames, countName, predicate);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executeSortedScan(final QueryContext ctx, final String[] sourcePath, final String orderField,
      final String direction) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeSortedScan(ctx, sourcePath, orderField, direction);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executeAggregate(final QueryContext ctx, final String[] sourcePath, final String func,
      final String field) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeAggregate(ctx, sourcePath, func, field);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executeCountDistinct(final QueryContext ctx, final String[] sourcePath, final String field)
      throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeCountDistinct(ctx, sourcePath, field);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executePredicateCount(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executePredicateCount(ctx, sourcePath, predicate);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executePredicateGroupByCount(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate, final String groupField) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executePredicateGroupByCount(ctx, sourcePath, predicate, groupField);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public Sequence executePredicateAggregate(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicate, final String func, final String field) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executePredicateAggregate(ctx, sourcePath, predicate, func, field);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public boolean supportsSortedScan() {
    enterResolution();
    try {
      final SirixVectorizedExecutor exec = current();
      return exec != null && exec.supportsSortedScan();
    } finally {
      executionLifecycle.leave();
    }
  }

  /**
   * Capability, not a promise about a particular query: {@code executeBinaryAggregate} answers
   * {@code null} for shapes it cannot serve and the generic pipeline runs instead.
   */
  @Override
  public boolean supportsBinaryAggregate() {
    enterResolution();
    try {
      final SirixVectorizedExecutor exec = current();
      return exec != null && exec.supportsBinaryAggregate();
    } finally {
      executionLifecycle.leave();
    }
  }

  @Override
  public Sequence executeBinaryAggregate(final QueryContext ctx, final String[] sourcePath, final String func,
      final String leftField, final String op, final String rightField) throws QueryException {
    enterResolution(ctx);
    SirixVectorizedExecutor exec = null;
    try {
      exec = currentAndEnter(ctx);
      return exec == null
          ? null
          : exec.executeBinaryAggregate(ctx, sourcePath, func, leftField, op, rightField);
    } finally {
      leaveAndUnpin(exec, ctx);
    }
  }

  @Override
  public boolean supportsMultiKeyGroupBy() {
    enterResolution();
    try {
      final SirixVectorizedExecutor exec = current();
      return exec != null && exec.supportsMultiKeyGroupBy();
    } finally {
      executionLifecycle.leave();
    }
  }

  /**
   * On ADMISSION this deliberately does NOT release the pin: it is the question a gated scan asks
   * immediately before the scan itself, and releasing here would send that scan back to the resolver
   * — the split-revision defect this class exists to prevent.
   *
   * <p>
   * On REFUSAL the pin is released, because nothing will follow it. The pipeline falls back to the
   * generic plan and no {@code execute*} forwarder ever runs, so the {@code finally} that normally
   * releases is never reached and the thread would keep a strong reference to the
   * {@link QueryContext} and to an executor the store's LRU may already have retired — for as long as
   * a pooled request thread happens to stay idle.
   */
  @Override
  public boolean canExecute(final QueryContext ctx) {
    enterResolution(ctx);
    try {
      final SirixVectorizedExecutor exec = current(ctx);
      // Deliberately side-effect free: this is the question a gated scan asks, not the end of an
      // evaluation, and it must NOT release the pin.
      //
      // Releasing here looked like it closed a retention leak — a gate that admits and is never
      // followed by a scan leaves the pin on the thread until the next gate or scan. But `unpin`
      // can only identify an owner by QueryContext identity, and NESTED evaluations of one query
      // share that context. A nested pipeline asking canExecute for something this backend refuses
      // would then clear the OUTER evaluation's pin; the outer forwarder finds none, re-resolves
      // through the resolver, and after an intervening commit serves from a revision its own gate
      // never verified. That is the split-revision defect this class exists to prevent, and a
      // wrong revision is strictly worse than a retained reference.
      //
      // Expressing "whose pin is this?" needs an evaluation identity — a depth counter or a
      // per-evaluation token — which the gate API does not carry. Until it does, the pin is
      // released only by the execute* forwarders' finally blocks, which DO know their own
      // evaluation ended. The residual retention is bounded (it lasts until the next gate or scan
      // on this thread) and cannot produce a wrong answer, because `current` honours a pin only
      // when its context matches by identity.
      return exec != null && exec.canExecute(ctx);
    } finally {
      executionLifecycle.leave();
    }
  }

  @Override
  public boolean acceptsPredicate(final String[] sourcePath, final PredicateNode predicate) {
    enterResolution();
    try {
      final SirixVectorizedExecutor exec = current();
      return exec != null && exec.acceptsPredicate(sourcePath, predicate);
    } finally {
      executionLifecycle.leave();
    }
  }

  /**
   * Admit a source this chain can reach, not merely the one it is bound to. With multi-resource
   * dispatch the question is whether the store can open the named document at all — the executor that
   * will actually serve it is picked in {@link #executorForSource(SourceRef)} and answers for its own
   * resource, so identity is still checked, just against the right binding.
   */
  @Override
  public boolean acceptsSource(final SourceRef source) {
    enterResolution();
    try {
      if (perSourceResolver != null && source != null && source.kind() == SourceRef.Kind.DOCUMENT) {
        final SirixVectorizedExecutor forSource = resolveForSource(source);
        return forSource != null && forSource.acceptsSource(source);
      }
      final SirixVectorizedExecutor exec = current();
      return exec != null && exec.acceptsSource(source);
    } finally {
      executionLifecycle.leave();
    }
  }

  @Override
  public VectorizedExecutor executorForSource(final SourceRef source) {
    enterResolution();
    try {
      if (perSourceResolver == null || source == null || source.kind() != SourceRef.Kind.DOCUMENT) {
        return this;
      }
      final Supplier<SirixVectorizedExecutor> forSource = perSourceResolver.apply(source);
      if (forSource == null || forSource.get() == null) {
        return this;
      }
      // Still revision-tracking, and still able to dispatch, so a nested scan over a third document
      // is reached the same way.
      return new RevisionTrackingExecutor(forSource, perSourceResolver, executionLifecycle);
    } finally {
      executionLifecycle.leave();
    }
  }

  /** The executor bound to {@code source}, or {@code null} when the store cannot reach it. */
  private SirixVectorizedExecutor resolveForSource(final SourceRef source) {
    final Supplier<SirixVectorizedExecutor> forSource = perSourceResolver.apply(source);
    return forSource == null
        ? null
        : forSource.get();
  }

  /**
   * Runtime gate. Routed through the same per-source dispatch as the compile-time overload: brackit
   * asks HERE for a {@link SourceRef.Kind#VARIABLE} source, so resolving only the query's first
   * document would leave multi-resource dispatch dead exactly where it is decided at run time.
   *
   * <p>
   * A VARIABLE names no document to dispatch on, so it falls to this instance — whose delegate still
   * checks the bound item's resource and revision itself.
   */
  @Override
  public boolean acceptsSource(final SourceRef source, final QueryContext ctx) {
    enterResolution(ctx);
    try {
      if (perSourceResolver != null && source != null && source.kind() == SourceRef.Kind.DOCUMENT) {
        final SirixVectorizedExecutor forSource = resolveForSource(source);
        if (forSource == null || !forSource.acceptsSource(source, ctx)) {
          unpin(ctx);
          return false;
        }
        pinnedForEvaluation.get().set(ctx, forSource);
        return true;
      }
      final SirixVectorizedExecutor exec = current(ctx);
      if (exec == null || !exec.acceptsSource(source, ctx)) {
        // Nothing will serve this evaluation, so leave no pin behind for the next one — but release
        // only OUR OWN, through the same ownership rule unpin() documents. Clearing unconditionally
        // would drop a pin belonging to an evaluation further out on this thread's stack, and the
        // scan that outer gate already admitted would then re-resolve through the resolver: after an
        // intervening commit that is a different revision than the one it verified.
        unpin(ctx);
        return false;
      }
      pinnedForEvaluation.get().set(ctx, exec);
      return true;
    } finally {
      executionLifecycle.leave();
    }
  }
}

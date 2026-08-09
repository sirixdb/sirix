package io.sirix.query;

import io.brackit.query.jdm.Sequence;

/**
 * The document a caller most recently bound as a query variable on this thread, so a store-only
 * {@link SirixCompileChain} can auto-wire a query that names no {@code jn:doc}.
 *
 * <h2>The gap it closes</h2>
 *
 * <p>{@link StoreBoundExecutorCache} lifts the resource out of the query text. That works for
 * {@code jn:doc('db','res')} and for nothing else — and {@code declare variable $doc external} with
 * the document bound through the {@link io.brackit.query.QueryContext} is the ordinary embedding
 * shape: bind once, compile once, run many. Such a query carries no function call to lift, so
 * auto-wiring declined and the generic pipeline answered. Measured on a 3.5M-record corpus, the
 * same query with the same answer took 705 ms that way against 1.1 ms written with a literal
 * {@code jn:doc} — auto-vectorization being ON by default is not the same as it firing.
 *
 * <p>The resource cannot be read off the query, and the {@link io.brackit.query.QueryContext} that
 * holds it does not exist at compile time — brackit compiles from a string. What DOES connect them
 * is the order the embedder works in: bind the document, then compile the query that reads it.
 * {@link SirixQueryContext#bind} records what it was handed here, and {@link SirixCompileChain}
 * consults it when the tree names nothing.
 *
 * <h2>Why a guess is safe</h2>
 *
 * <p>This only decides WHICH executor is built and asked the translate-time questions. Whether that
 * executor may serve is decided again at run time, per evaluation, by
 * {@code SirixVectorizedExecutor.acceptsSource(SourceRef, QueryContext)} against the variable's
 * ACTUAL binding — resource, revision and whole-document position all re-checked there. A hint that
 * names a different document than the query ends up reading is therefore declined at run time and
 * the generic pipeline answers, which is exactly what happens today without any hint. A stale or
 * wrong hint costs the fast path and never correctness.
 *
 * <p>Only the {@code (database, resource, revision)} triple is kept, never the item: a thread-local
 * holding a bound item would retain its read transaction — and through it a revision's pages — for
 * as long as the thread lives.
 *
 * <p>Per thread because compilation is: two threads compiling different queries against different
 * documents must not read each other's hint, and one thread doing bind-compile-bind-compile gets
 * the right one each time.
 */
final class BoundDocumentHint {

  /**
   * The last whole-document binding seen on this thread. Not cleared after a compile: a caller that
   * binds once and compiles several queries should have all of them wired, and the value is three
   * immutable fields rather than anything with a lifetime.
   */
  private static final ThreadLocal<StoreBoundExecutorCache.DocumentSource> LAST_BOUND =
      new ThreadLocal<>();

  private BoundDocumentHint() {
    throw new AssertionError("no instances");
  }

  /**
   * Record {@code sequence} if it is a whole Sirix JSON document; ignore it otherwise.
   *
   * <p>Deliberately does not clear the hint for a non-document binding. A query binding both a
   * document and a scalar parameter would otherwise lose the document to whichever bind came last,
   * and the order in which a caller binds its variables is not something this can require.
   */
  static void remember(final Sequence sequence) {
    final StoreBoundExecutorCache.DocumentSource source =
        StoreBoundExecutorCache.boundDocumentSource(sequence);
    if (source != null) {
      LAST_BOUND.set(source);
    }
  }

  /** The last whole-document binding on this thread, or {@code null} when there has been none. */
  static StoreBoundExecutorCache.DocumentSource peek() {
    return LAST_BOUND.get();
  }

  /** Forget this thread's hint — for tests, and for a caller that wants no carry-over. */
  static void clear() {
    LAST_BOUND.remove();
  }
}

package io.sirix.query.scan;

import io.brackit.query.QueryContext;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;

import java.util.Objects;

/** Provides one revision-stable Sirix executor for a translated expression evaluation. */
public interface SirixExecutorProvider extends VectorizedExecutor {

  /**
   * Acquires the executor that must serve the complete evaluation, or {@code null} when none is
   * available. The caller must close a non-null lease.
   */
  Lease acquire(QueryContext context, SourceRef source);

  /** One admitted, revision-stable executor evaluation. */
  final class Lease implements AutoCloseable {
    private final SirixVectorizedExecutor executor;
    private final Runnable release;
    private boolean closed;

    Lease(final SirixVectorizedExecutor executor, final Runnable release) {
      this.executor = Objects.requireNonNull(executor);
      this.release = Objects.requireNonNull(release);
    }

    public SirixVectorizedExecutor executor() {
      return executor;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        release.run();
      }
    }
  }
}

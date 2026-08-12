package io.sirix.query.scan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A projection worker's DECLINE must survive the fan-out.
 *
 * <h2>What this pins</h2>
 *
 * <p>
 * The projection kernels signal "not mine to serve" by throwing {@link IllegalStateException}, and
 * the callers of the parallel drivers catch exactly that to compile the generic pipeline instead. A
 * worker's throwable arrives wrapped in an {@link ExecutionException}; wrapping THAT in a plain
 * {@code RuntimeException} put the decline where no caller looks, and the fallback became a failed
 * query.
 *
 * <p>
 * It only ever failed past the fan-out threshold — below 64 row groups the drivers run inline and
 * the decline propagates unwrapped — which is why it was found on a 3.48M-record corpus and not by
 * the suite. Stated here rather than through a query because every shape that reached it has since
 * grown a route of its own: a count-distinct over a numeric column is served now, so an end-to-end
 * test would pass while proving nothing.
 */
final class ProjectionWorkerFailureTest {

  @Test
  @DisplayName("a decline wrapped by the worker pool is rethrown as a decline")
  void declineSurvivesTheWrapper() {
    final IllegalStateException decline = new IllegalStateException("groupColumn 0 is not STRING_DICT");
    final ExecutionException fromWorker = new ExecutionException(decline);
    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> SirixVectorizedExecutor.projectionWorkerFailure("parallel projection failed", fromWorker));
    assertSame(decline, thrown, "the decline must arrive at the caller as the SAME exception it "
        + "catches to fall back — a copy or a wrapper is a failed query");
  }

  @Test
  @DisplayName("a genuine failure keeps its wrapper rather than being read as a decline")
  void realFailureIsNotDowngraded() {
    final RuntimeException broken = new RuntimeException("segment decode blew up");
    final RuntimeException returned =
        SirixVectorizedExecutor.projectionWorkerFailure("parallel projection failed", new ExecutionException(broken));
    assertEquals("parallel projection failed", returned.getMessage(),
        "a failure inside a worker must not be quietly downgraded into 'the fast path "
            + "passed on it', which would answer from the generic pipeline and hide it");
  }

  @Test
  @DisplayName("a self-referential cause chain terminates")
  void selfReferentialCauseTerminates() {
    // Not hypothetical for a chain assembled across threads: a cause that points at itself would
    // spin the walk forever, inside a query.
    final RuntimeException loop = new RuntimeException("loop") {
      @Override
      public synchronized Throwable getCause() {
        return this;
      }
    };
    assertEquals("parallel projection failed",
        SirixVectorizedExecutor.projectionWorkerFailure("parallel projection failed", loop).getMessage());
  }
}

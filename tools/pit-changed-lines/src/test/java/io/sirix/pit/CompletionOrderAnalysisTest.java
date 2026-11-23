package io.sirix.pit;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;
import org.pitest.mutationtest.MutationMetaData;
import org.pitest.mutationtest.MutationResultInterceptor;
import org.pitest.mutationtest.MutationResultListener;
import org.pitest.mutationtest.build.MutationAnalysisUnit;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.util.PitError;

/** CONV-205: a cancelled completion wait must preserve the caller's cancellation signal. */
final class CompletionOrderAnalysisTest {
  @Test
  void interruptedWaitPropagatesFailureAndRestoresInterruptStatus() {
    final MutationAnalysisUnit unit = new MutationAnalysisUnit() {
      @Override
      public int priority() {
        return 0;
      }

      @Override
      public Collection<MutationDetails> mutants() {
        return List.of();
      }

      @Override
      public MutationMetaData call() {
        return new MutationMetaData(List.of());
      }
    };
    final MutationResultInterceptor interceptor = results -> results;
    final List<MutationResultListener> listeners = List.of();
    final List<MutationAnalysisUnit> units = List.of(unit);
    try (ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1)) {
      Thread.currentThread().interrupt();
      try {
        final PitError failure =
            assertThrows(PitError.class, () -> CompletionOrderAnalysis.run(executor, interceptor, listeners, units));
        assertInstanceOf(InterruptedException.class, failure.getCause());
        assertTrue(Thread.currentThread().isInterrupted(), "the caller must still observe cancellation");
      } finally {
        Thread.interrupted();
      }
    }
  }
}

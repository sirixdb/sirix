package io.sirix.pit;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import org.pitest.mutationtest.ClassMutationResults;
import org.pitest.mutationtest.MutationMetaData;
import org.pitest.mutationtest.MutationResultInterceptor;
import org.pitest.mutationtest.MutationResultListener;
import org.pitest.mutationtest.build.MutationAnalysisUnit;
import org.pitest.util.Unchecked;

/**
 * PIT 1.25.5's execution protocol, draining completed units instead of submission-order futures.
 */
public final class CompletionOrderAnalysis {
  private CompletionOrderAnalysis() {}

  /** Called by the lane's agent in place of PIT's head-of-line-blocking executor loop. */
  public static void run(final ThreadPoolExecutor executor, final MutationResultInterceptor interceptor,
      final List<MutationResultListener> listeners, final List<MutationAnalysisUnit> units) {
    for (final MutationResultListener listener : listeners) {
      listener.runStart();
    }
    final ExecutorCompletionService<MutationMetaData> completed = new ExecutorCompletionService<>(executor);
    for (final MutationAnalysisUnit unit : units) {
      completed.submit(unit);
    }
    executor.shutdown();
    try {
      for (int remaining = units.size(); remaining > 0; remaining--) {
        final MutationMetaData result = completed.take().get();
        for (final ClassMutationResults results : interceptor.modify(result.toClassResults())) {
          for (final MutationResultListener listener : listeners) {
            listener.handleMutationResult(results);
          }
        }
      }
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      throw Unchecked.translateCheckedException(error);
    } catch (final ExecutionException error) {
      throw Unchecked.translateCheckedException(error);
    }
    for (final ClassMutationResults results : interceptor.remaining()) {
      for (final MutationResultListener listener : listeners) {
        listener.handleMutationResult(results);
      }
    }
    for (final MutationResultListener listener : listeners) {
      listener.runEnd();
    }
  }
}

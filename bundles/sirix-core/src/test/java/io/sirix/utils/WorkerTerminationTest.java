package io.sirix.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class WorkerTerminationTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void requestsTheRightShutdownAndPreservesAnExistingInterrupt(final boolean cancel, final boolean interrupted)
      throws InterruptedException {
    final ExecutorService executor = mock(ExecutorService.class);
    when(executor.isTerminated()).thenReturn(true);
    when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenAnswer(
        invocation -> fail("A terminated executor has no workers left to await"));
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    try {
      WorkerTermination.shutdownAndAwait(executor, cancel);
      assertEquals(interrupted, Thread.currentThread().isInterrupted());
      if (cancel || interrupted) {
        verify(executor).shutdownNow();
        verify(executor, never()).shutdown();
      } else {
        verify(executor).shutdown();
        verify(executor, never()).shutdownNow();
      }
    } finally {
      Thread.interrupted();
    }
  }
}

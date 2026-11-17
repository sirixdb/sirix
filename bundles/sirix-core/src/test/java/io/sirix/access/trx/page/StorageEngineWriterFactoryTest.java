package io.sirix.access.trx.page;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/** NATIVE-406: the public terminal hook must reach both engine-owned pools. */
@Timeout(20)
final class StorageEngineWriterFactoryTest {
  @Test
  void terminalHookDrainsTheEnginesSharedPools() throws Exception {
    final ExecutorService sharedAppend = sharedPool("SNAPSHOT_APPEND_EXECUTOR");
    final ExecutorService sharedSerialization = sharedPool("SNAPSHOT_FLUSH_POOL");
    try (final ForkJoinPool append = new ForkJoinPool(1);
         final ForkJoinPool serialization = new ForkJoinPool(1);
         final MockedStatic<SnapshotExecutorShutdown> shutdown =
             mockStatic(SnapshotExecutorShutdown.class, CALLS_REAL_METHODS)) {
      // Substitute only the terminal process-owned resources. Both public delegation layers
      // and the drain implementation execute; other tests can still open writers afterward.
      shutdown.when(() -> SnapshotExecutorShutdown.drain(sharedAppend, sharedSerialization)).thenAnswer(call -> {
        SnapshotExecutorShutdown.drain(append, serialization);
        return null;
      });
      final Future<Integer> result = append.submit(() -> serialization.submit(() -> 42).join());

      StorageEngineWriterFactory.shutdownSharedExecutors();

      assertTrue(append.isTerminated(), "The public hook must drain append work");
      assertTrue(serialization.isTerminated(), "The public hook must retire serialization after append");
      assertEquals(42, result.get());
      StorageEngineWriterFactory.shutdownSharedExecutors();
    }
  }

  private static ExecutorService sharedPool(final String name) throws ReflectiveOperationException {
    // Inspect live resource identity, not source text; reversed or unrelated pools cannot match.
    final Field field = NodeStorageEngineWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return (ExecutorService) field.get(null);
  }
}

package io.sirix.access.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the {@link ResourceSession}.
 *
 * @author Johannes Lichtenberger
 */
public final class ResourceSessionTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @DisplayName("throw exception when multiple read-write transactions are started")
  @Test
  public void test_whenMultipleReadWriteTrxStarted_throwException() {
    final Exception exception = assertThrows(RuntimeException.class, () -> createTransactions(resourceSession -> {
      resourceSession.beginNodeTrx();
      resourceSession.beginNodeTrx();
    }));

    assertion(exception);
  }

  private void createTransactions(Consumer<JsonResourceSession> startTransactions) {
    final var resource = "resource";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.MEMORY_MAPPED)
                                                   .build());
      try (final var session = database.beginResourceSession(resource)) {
        startTransactions.accept(session);
      }
    }
  }

  private void assertion(Exception exception) {
    final var expectedMessage =
        "No read-write transaction available, please close the running read-write transaction first.";
    final var actualMessage = exception.getMessage();
    System.out.println("Actual message: " + actualMessage);
    assertTrue(actualMessage.contains(expectedMessage));
  }

  /**
   * Pre-fix this would have run strictly serially because beginNodeReadOnlyTrx was
   * {@code synchronized} on the per-session monitor; post-fix all opens proceed in parallel.
   * Either way the routine must remain race-free: every opened reader gets a unique trx ID,
   * lands in nodeTrxMap exactly once, and the activeTrxCount() observed at the end equals
   * the number of opens.
   *
   * <p>The test guards three invariants that the {@code synchronized} previously hid:
   * <ul>
   *   <li>{@code nodeTrxIDCounter} (AtomicInteger) generates unique IDs under contention.</li>
   *   <li>{@code nodeTrxMap.put} (ConcurrentMap) does not double-insert under contention.</li>
   *   <li>No thread observes a partially-constructed reader (publication safety).</li>
   * </ul>
   */
  @DisplayName("concurrent beginNodeReadOnlyTrx is race-free and produces unique IDs")
  @Test
  public void concurrentReaderOpens_areRaceFreeAndProduceUniqueIds() throws Exception {
    final int threadCount = 16;
    final int opensPerThread = 64; // 1024 total opens
    final int totalOpens = threadCount * opensPerThread;
    final var resource = "resource";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.MEMORY_MAPPED)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(resource)) {
        // Need at least one committed revision to read from.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }

        final int baseline = session.activeTrxCount();
        final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
          final CountDownLatch start = new CountDownLatch(1);
          final List<Future<List<JsonNodeReadOnlyTrx>>> futures = new ArrayList<>(threadCount);
          final AtomicInteger errorCount = new AtomicInteger();

          for (int t = 0; t < threadCount; t++) {
            futures.add(pool.submit(() -> {
              final List<JsonNodeReadOnlyTrx> opened = new ArrayList<>(opensPerThread);
              try {
                start.await();
                for (int i = 0; i < opensPerThread; i++) {
                  final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
                  assertNotNull(rtx, "begin returned null");
                  opened.add(rtx);
                }
              } catch (final Throwable th) {
                errorCount.incrementAndGet();
                th.printStackTrace();
              }
              return opened;
            }));
          }

          start.countDown();

          final List<JsonNodeReadOnlyTrx> all = new ArrayList<>(totalOpens);
          for (final Future<List<JsonNodeReadOnlyTrx>> f : futures) {
            all.addAll(f.get(60, TimeUnit.SECONDS));
          }

          assertEquals(0, errorCount.get(), "exceptions during concurrent opens");
          assertEquals(totalOpens, all.size(), "every thread must yield opensPerThread rtxs");

          // All trx IDs must be unique — collisions would imply the AtomicInteger or
          // the ConcurrentMap.put-once semantics broke under contention.
          final var ids = new java.util.HashSet<Integer>(totalOpens * 2);
          for (final JsonNodeReadOnlyTrx rtx : all) {
            assertTrue(ids.add(rtx.getId()), "duplicate trx id observed: " + rtx.getId());
          }

          assertEquals(baseline + totalOpens, session.activeTrxCount(),
              "activeTrxCount must reflect every concurrent open");

          // Close them all and verify the bookkeeping returns to baseline.
          for (final JsonNodeReadOnlyTrx rtx : all) {
            rtx.close();
          }
          assertEquals(baseline, session.activeTrxCount(),
              "activeTrxCount must return to baseline after closing every rtx");
        } finally {
          pool.shutdownNow();
          if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("executor did not terminate");
          }
        }
      }
    }
  }
}

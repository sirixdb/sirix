package io.sirix.access.trx.node;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AsyncCommitFrontierOrderingTest {

  private static final String RESOURCE = "async-commit-frontier";

  private CountDownLatch releaseHarden;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    if (releaseHarden != null) {
      releaseHarden.countDown();
    }
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
    JsonTestHelper.deleteEverything();
  }

  @Test
  void successorMaintenanceWaitsForThePredecessorCommitFrontier() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
    }

    final CountDownLatch firstHardenEntered = new CountDownLatch(1);
    final CountDownLatch secondPredecessorWait = new CountDownLatch(1);
    releaseHarden = new CountDownLatch(1);
    final AtomicBoolean blockFirstHarden = new AtomicBoolean(true);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger preCommitCalls = new AtomicInteger();
    AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
      if ("attempt".equals(stage)) {
        attempts.incrementAndGet();
      } else if ("predecessor-wait".equals(stage) && attempts.get() == 2) {
        secondPredecessorWait.countDown();
      } else if ("before-harden".equals(stage) && blockFirstHarden.getAndSet(false)) {
        firstHardenEntered.countDown();
        try {
          releaseHarden.await();
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
      }
    };

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> writes = executor.submit(() -> {
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
            final JsonResourceSession session = database.beginResourceSession(RESOURCE);
            final JsonNodeTrx wtx = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          wtx.addPreCommitHook(unused -> preCommitCalls.incrementAndGet());
          final long arrayKey = wtx.insertArrayAsFirstChild().getNodeKey();
          for (int i = 0; i < 8; i++) {
            wtx.moveTo(arrayKey);
            wtx.insertStringValueAsFirstChild("value-" + i);
          }
          wtx.commit();
        }
        return null;
      });

      assertTrue(firstHardenEntered.await(10, TimeUnit.SECONDS));
      assertTrue(secondPredecessorWait.await(10, TimeUnit.SECONDS));
      assertEquals(1, preCommitCalls.get());

      releaseHarden.countDown();
      writes.get(30, TimeUnit.SECONDS);
    } finally {
      releaseHarden.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}

package io.sirix.access;

import static org.junit.jupiter.api.Assertions.fail;

import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduces the mixed read/write use-after-free seen via REST as sporadic HTTP 500s (3 / 108k
 * requests with 16 readers + 1 committing writer): read-only transactions crashed with
 * {@code NullPointerException: "page" is null} (PageLayout access to a closed page's segment) and
 * {@code AssertionError: Type not known} (value parse through a recycled frame).
 *
 * <p>The record-page cache is shrunk so eviction churns constantly, maximizing the window in
 * which a page can be freed out from under a reader whose guard accounting went wrong. Run with
 * {@code -Dsirix.debug.memory.leaks=true} (set in the static initializer below) so a reader that
 * observes a freed page logs the closing thread's stack trace.
 */
public final class MixedReadWriteUafStressTest {

  static {
    // Must win the race with DiagnosticSettings class-init: enables the close-site capture in
    // KeyValueLeafPage so a reader observing a freed page logs WHO closed it.
    System.setProperty("sirix.debug.memory.leaks", "true");
  }

  private static final int READERS = 8;
  private static final long DURATION_MS = Long.getLong("sirix.uaf.stress.ms", 15_000);

  private long previousRecordCacheBytes;
  private long previousFragmentCacheBytes;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    // The buffer manager is GLOBAL — remember the suite's budgets so tearDown can restore
    // them. Leaving the shrunken budget in place starved every test running after this one
    // in the same JVM (eviction storms → "sustained allocator thrashing" in the HOT family).
    if (Databases.getGlobalBufferManager() instanceof io.sirix.cache.BufferManagerImpl bm) {
      previousRecordCacheBytes = bm.getRecordPageCacheMaxWeightBytes();
      previousFragmentCacheBytes = bm.getRecordPageFragmentCacheMaxWeightBytes();
    }
    // 8 record pages + 8 fragment pages of budget — the latest-revision working set does not
    // fit, so eviction runs continuously while readers traverse.
    Databases.reinitializeBufferManagerForTesting(8L * 64 * 1024, 8L * 64 * 1024);
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
    if (previousRecordCacheBytes > 0) {
      Databases.reinitializeBufferManagerForTesting(previousRecordCacheBytes, previousFragmentCacheBytes);
    }
  }

  @Test
  @Timeout(180)
  public void concurrentReadersSurviveWriterCommits() throws Exception {
    final var sb = new StringBuilder("[");
    for (int i = 0; i < 3000; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"a\":").append(i).append(",\"b\":\"v").append(i).append("\",\"c\":").append((i & 1) == 0).append('}');
    }
    sb.append(']');

    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));
      }

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong reads = new AtomicLong();
      final AtomicLong commits = new AtomicLong();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        for (int r = 0; r < READERS; r++) {
          final Thread t = new Thread(() -> {
            try {
              start.await();
              while (!stop.get() && failure.get() == null) {
                try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
                  final var axis = new DescendantAxis(rtx);
                  while (axis.hasNext()) {
                    axis.nextLong();
                    switch (rtx.getKind()) {
                      case STRING_VALUE, OBJECT_NAMED_STRING -> rtx.getValue();
                      case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> rtx.getNumberValue();
                      case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> rtx.getBooleanValue();
                      default -> {
                      }
                    }
                  }
                }
                reads.incrementAndGet();
              }
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          }, "uaf-reader-" + r);
          t.setDaemon(true);
          threads.add(t);
          t.start();
        }

        final Thread writer = new Thread(() -> {
          try {
            start.await();
            int i = 0;
            while (!stop.get() && failure.get() == null) {
              try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"w\":" + i + "}"));
              }
              commits.incrementAndGet();
              i++;
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "uaf-writer");
        writer.setDaemon(true);
        threads.add(writer);
        writer.start();

        start.countDown();
        final long deadline = System.currentTimeMillis() + DURATION_MS;
        while (System.currentTimeMillis() < deadline && failure.get() == null) {
          Thread.sleep(100);
        }
        stop.set(true);
        for (final Thread t : threads) {
          t.join(60_000);
        }
      }

      final Throwable t = failure.get();
      if (t != null) {
        t.printStackTrace();
        fail("concurrent reader/writer failed after " + reads.get() + " reads / " + commits.get() + " commits: " + t);
      }
      System.out.println("UAF stress survived: " + reads.get() + " reads, " + commits.get() + " commits");
    }
  }
}

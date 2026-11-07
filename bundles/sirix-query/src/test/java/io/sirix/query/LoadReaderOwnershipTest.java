package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.access.Databases;
import io.sirix.access.MutationGate;
import io.sirix.io.StorageType;
import io.sirix.query.json.BasicJsonDBStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(120)
final class LoadReaderOwnershipTest {
  @TempDir
  Path directory;

  @Test
  void interruptedLoadReleasesGateWhileWorkersStillOwnTheirInputs() throws Exception {
    final Path databasePath = directory.resolve("database");
    final CountDownLatch sourcesStarted = new CountDownLatch(2);
    final CountDownLatch releaseSources = new CountDownLatch(1);
    final CountDownLatch responsesFinished = new CountDownLatch(2);
    final AtomicBoolean interruptRestored = new AtomicBoolean();
    final AtomicInteger sourceRequests = new AtomicInteger();
    final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    try (final ExecutorService requests = Executors.newFixedThreadPool(2)) {
      server.setExecutor(requests);
      server.createContext("/input", exchange -> {
        if (sourceRequests.getAndIncrement() < 2) {
          exchange.sendResponseHeaders(200, 2);
          try (var body = exchange.getResponseBody()) {
            body.write("{}".getBytes(StandardCharsets.UTF_8));
          } finally {
            exchange.close();
          }
          return;
        }
        try {
          exchange.sendResponseHeaders(200, 0);
          try (var body = exchange.getResponseBody()) {
            body.write("[1,2,3,".getBytes(StandardCharsets.UTF_8));
            body.flush();
            sourcesStarted.countDown();
            try {
              releaseSources.await();
            } catch (final InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new IOException(interrupted);
            }
            body.write("4]".getBytes(StandardCharsets.UTF_8));
          }
        } finally {
          exchange.close();
          responsesFinished.countDown();
        }
      });
      server.start();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
          .location(directory)
          .storageType(StorageType.IN_MEMORY)
          .build();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        final String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/input?";
        final FutureTask<Throwable> load = new FutureTask<>(() -> {
          try {
            new Query(chain, "jn:load('database',(),('" + uri + "first','" + uri + "second'))").execute(context);
            return null;
          } catch (final Throwable failure) {
            return failure;
          } finally {
            interruptRestored.set(Thread.currentThread().isInterrupted());
          }
        });
        final Thread caller = Thread.ofPlatform().daemon().start(load);
        try {
          assertTrue(sourcesStarted.await(10, TimeUnit.SECONDS), "both inputs must reach the HTTP server");
          awaitWorkersWriting(databasePath);
          caller.interrupt();
          assertInstanceOf(QueryException.class, load.get(75, TimeUnit.SECONDS));
          assertTrue(interruptRestored.get(), "the interrupted caller must retain its interrupt status");
          assertTrue(responsesFinished.getCount() > 0, "the input must remain stalled after cancellation returns");
          try (var database = Databases.openJsonDatabase(databasePath)) {
            assertThrows(IllegalStateException.class, () -> database.removeResource("resource1"),
                "the stalled worker must still own its resource session");
          }
          try (var gate = MutationGate.acquireDatabase(databasePath)) {
            assertTrue(load.isDone(), "cancellation must release the database gate while inputs are still live");
          }
        } finally {
          releaseSources.countDown();
          assertTrue(responsesFinished.await(10, TimeUnit.SECONDS), "HTTP responses must drain after release");
          caller.join(TimeUnit.SECONDS.toMillis(10));
          assertFalse(caller.isAlive(), "the caller must finish after releasing its inputs");
        }
        awaitWorkerSessionRelease(databasePath);
      } finally {
        server.stop(0);
      }
    }
  }

  private static void awaitWorkersWriting(final Path path) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (System.nanoTime() < deadline) {
      if (Databases.existsDatabase(path)) {
        try (var database = Databases.openJsonDatabase(path)) {
          if (database.existsResource("resource1") && database.existsResource("resource2")) {
            try (var first = database.beginResourceSession("resource1");
                var second = database.beginResourceSession("resource2")) {
              if (first.hasRunningNodeWriteTrx() && second.hasRunningNodeWriteTrx()) {
                return;
              }
            }
          }
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("both workers must hold write transactions before cancellation");
  }

  private static void awaitWorkerSessionRelease(final Path path) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    try (var database = Databases.openJsonDatabase(path)) {
      for (final Path resource : database.listResources()) {
        final String name = resource.getFileName().toString();
        boolean removed = false;
        while (!removed) {
          try {
            database.removeResource(name);
            removed = true;
          } catch (final IllegalStateException activeSession) {
            if (System.nanoTime() >= deadline) {
              throw activeSession;
            }
            Thread.sleep(10);
          }
        }
      }
    }
  }
}

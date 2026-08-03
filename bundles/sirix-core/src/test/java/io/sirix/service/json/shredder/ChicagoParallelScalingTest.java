package io.sirix.service.json.shredder;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures read-transaction scaling on the Chicago open-data resource: a warm single-transaction
 * full descendant-axis traversal as the baseline, then one concurrent read-only transaction per
 * core, each running the same full traversal against the same resource session.
 *
 * <p>The shredded database is created once under the JVM's temp directory (override via
 * {@code -Dchicago.scaling.db=...}) and reused on subsequent runs. Requires
 * {@code src/test/resources/json/cityofchicago.json}; the test is skipped if the file is absent.
 * Progress and the final summary are mirrored to {@code sirix-chicago-scaling-result.txt} beside
 * it.</p>
 *
 * <p>Run with:
 * {@code ./gradlew :sirix-core:test --tests "io.sirix.service.json.shredder.ChicagoParallelScalingTest"}</p>
 */
public final class ChicagoParallelScalingTest {

  private static final Path JSON_FILE = Paths.get("src", "test", "resources", "json", "cityofchicago.json");

  /**
   * Where scratch state goes. {@code java.io.tmpdir} rather than a literal {@code /tmp}: the latter
   * is read-only in some build sandboxes, and the failure mode was a hard
   * {@code FileSystemException} that failed the whole suite for a reason unrelated to what this
   * test measures.
   */
  private static final Path SCRATCH_DIR = Paths.get(System.getProperty("java.io.tmpdir", "."));

  private static final Path DB_PATH =
      Paths.get(System.getProperty("chicago.scaling.db",
                                   SCRATCH_DIR.resolve("sirix-chicago-scaling-db").toString()));

  private static final Path RESULT_FILE =
      Paths.get(System.getProperty("chicago.scaling.result",
                                   SCRATCH_DIR.resolve("sirix-chicago-scaling-result.txt").toString()));

  private static final String RESOURCE = "shredded";

  private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();

  private record Traversal(long nanos, long nodeCount, long keySum) {
  }

  /**
   * The run's transcript, or standard output when the transcript file cannot be opened.
   *
   * <p>This is a measurement harness: failing the run because a progress log could not be written
   * reports a broken build for something that has no bearing on what is being measured. The numbers
   * still reach the console either way.
   */
  private static PrintWriter openReport() {
    try {
      return new PrintWriter(Files.newBufferedWriter(RESULT_FILE, StandardCharsets.UTF_8));
    } catch (final IOException e) {
      System.out.println("[chicago-scaling] cannot write " + RESULT_FILE + " (" + e.getMessage()
                             + "); reporting to stdout only");
      return new PrintWriter(System.out, true);
    }
  }

  @Test
  public void measureParallelDescendantAxisScaling() throws Exception {
    Assumptions.assumeTrue(Files.exists(JSON_FILE), "cityofchicago.json not present, skipping");
    Assumptions.assumeTrue(Files.isWritable(SCRATCH_DIR) || Files.exists(DB_PATH),
                           SCRATCH_DIR + " is not writable and no prepared database exists, skipping");

    try (final PrintWriter out = openReport()) {
      if (Files.exists(DB_PATH)) {
        report(out, "reusing existing database at " + DB_PATH);
      } else {
        shred(out);
      }

      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DB_PATH);
           final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Traversal warmup = traverse(session);
        report(out, String.format("warm-up: %.2fs (%d nodes, keySum=%d)",
                                  warmup.nanos() / 1e9, warmup.nodeCount(), warmup.keySum()));

        final Traversal baseline1 = traverse(session);
        report(out, String.format("baseline #1: %.2fs", baseline1.nanos() / 1e9));
        final Traversal baseline2 = traverse(session);
        report(out, String.format("baseline #2: %.2fs", baseline2.nanos() / 1e9));
        final long baselineNanos = Math.min(baseline1.nanos(), baseline2.nanos());

        final ExecutorService pool = Executors.newFixedThreadPool(PARALLELISM);
        final List<Callable<Traversal>> tasks = new ArrayList<>(PARALLELISM);
        for (int i = 0; i < PARALLELISM; i++) {
          tasks.add(() -> traverse(session));
        }
        final List<Future<Traversal>> futures = pool.invokeAll(tasks);
        pool.shutdown();

        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (final Future<Traversal> future : futures) {
          final Traversal traversal = future.get();
          assertEquals(warmup.nodeCount(), traversal.nodeCount(), "traversal node count mismatch");
          assertEquals(warmup.keySum(), traversal.keySum(), "traversal key sum mismatch");
          sum += traversal.nanos();
          min = Math.min(min, traversal.nanos());
          max = Math.max(max, traversal.nanos());
          report(out, String.format("parallel txn: %.2fs", traversal.nanos() / 1e9));
        }

        final double avg = sum / (double) PARALLELISM;
        report(out, String.format(
            "PARALLEL_SCALING_RESULT threads=%d nodes=%d single=%.2fs parallel_avg=%.2fs parallel_min=%.2fs "
                + "parallel_max=%.2fs ratio_avg=%.2fx ratio_max=%.2fx",
            PARALLELISM, warmup.nodeCount(), baselineNanos / 1e9, avg / 1e9, min / 1e9, max / 1e9,
            avg / baselineNanos, (double) max / baselineNanos));
      }
    }
  }

  private static Traversal traverse(final JsonResourceSession session) {
    try (final var rtx = session.beginNodeReadOnlyTrx()) {
      final long start = System.nanoTime();
      final Axis axis = new DescendantAxis(rtx);
      long count = 0;
      long keySum = 0;
      while (axis.hasNext()) {
        keySum += axis.nextLong();
        count++;
      }
      return new Traversal(System.nanoTime() - start, count, keySum);
    }
  }

  private static void shred(final PrintWriter out) throws IOException {
    report(out, "shredding " + JSON_FILE + " into " + DB_PATH + " ...");
    final long start = System.nanoTime();
    Databases.createJsonDatabase(new DatabaseConfiguration(DB_PATH));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DB_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(true)
                                                   .storeNodeHistory(false)
                                                   .storeChildCount(true)
                                                   .hashKind(HashType.ROLLING)
                                                   .useTextCompression(false)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .useDeweyIDs(false)
                                                   .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor()))
                                                   .build());
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE);
           final var trx = manager.beginNodeTrx((262_144 << 4) + 262_144)) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(JSON_FILE));
      }
    }
    report(out, String.format("shredding done [%.1fs]", (System.nanoTime() - start) / 1e9));
  }

  private static void report(final PrintWriter out, final String line) {
    out.println(line);
    out.flush();
    System.out.println(line);
  }
}

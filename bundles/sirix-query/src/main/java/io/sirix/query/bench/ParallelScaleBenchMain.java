package io.sirix.query.bench;

import com.google.gson.stream.JsonReader;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.service.json.shredder.ParallelJsonShredder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Non-JMH scale runner that measures <em>ingest</em> throughput of
 * {@link ParallelJsonShredder} — sharding a generated JSON dataset across {@code P} resources
 * shredded concurrently — against the single-writer baseline (one resource, one writer thread).
 *
 * <p>Both arms run through the <strong>same</strong> {@link ParallelJsonShredder#shredPartitioned}
 * entry point; the single-writer baseline is simply {@code partitions = 1, maxConcurrency = 1}. That
 * isolates the one variable under test — the number of concurrent writers/resources — from session,
 * transaction, auto-commit, serialization and resource-config differences, all of which are held
 * identical. The records are byte-for-byte the same shape as {@link ScaleBenchMain} so the per-record
 * shred cost matches the query-side benchmark.
 *
 * <p>Usage:
 * <pre>
 *   java io.sirix.query.bench.ParallelScaleBenchMain &lt;recordCount&gt; &lt;partitions&gt; [maxConcurrency=0] [mode=both|single|parallel]
 * </pre>
 * {@code maxConcurrency <= 0} uses {@code availableProcessors}. For a clean headline number, run the
 * two modes as separate JVM processes ({@code mode=single} then {@code mode=parallel}) so neither
 * benefits from the other's warm JIT.
 */
public final class ParallelScaleBenchMain {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
          "Usage: ParallelScaleBenchMain <recordCount> <partitions> [maxConcurrency=0] [mode=both|single|parallel]");
      System.exit(1);
    }
    final long recordCount = Long.parseLong(args[0]);
    final int partitions = Integer.parseInt(args[1]);
    final int maxConcurrency = args.length < 3 ? 0 : Integer.parseInt(args[2]);
    final String mode = args.length < 4 ? "both" : args[3];
    if (partitions < 1) {
      System.err.println("partitions must be >= 1");
      System.exit(1);
    }

    // Match ScaleBenchMain: lift the off-heap pool so the concurrent shreds don't tip the page cache.
    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    final var alloc = Allocators.getInstance();
    alloc.init(offheap);
    // Fast-shred defaults identical to ScaleBenchMain: no path summary, no rolling hash.
    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "131072"));
    final boolean pathSummary = Boolean.parseBoolean(System.getProperty("buildPathSummary", "false"));
    final HashType hash = HashType.fromString(System.getProperty("hashType", "NONE"));

    System.out.printf("# Records: %,d   Partitions: %d   maxConcurrency: %s   mode: %s%n",
        recordCount, partitions, maxConcurrency <= 0 ? "cores(" + Runtime.getRuntime().availableProcessors() + ")"
            : String.valueOf(maxConcurrency), mode);
    System.out.printf("# Allocator: %s   offheap=%d MB   autoCommit=%,d nodes   pathSummary=%s   hash=%s%n",
        alloc.getClass().getSimpleName(), offheap / (1L << 20), autoCommit, pathSummary, hash);

    final Function<String, ResourceConfiguration> configFactory = name -> ResourceConfiguration.newBuilder(name)
        .buildPathSummary(pathSummary)
        .hashKind(hash)
        .build();

    Double singleRecPerSec = null;
    Double parallelRecPerSec = null;

    if (mode.equals("single") || mode.equals("both")) {
      singleRecPerSec = run("single", recordCount, 1, 1, autoCommit, configFactory);
    }
    if (mode.equals("parallel") || mode.equals("both")) {
      parallelRecPerSec = run("parallel", recordCount, partitions, maxConcurrency, autoCommit, configFactory);
    }

    if (singleRecPerSec != null && parallelRecPerSec != null) {
      System.out.printf("%n# SPEEDUP (parallel / single): %.2fx   (%,.0f → %,.0f rec/s)%n",
          parallelRecPerSec / singleRecPerSec, singleRecPerSec, parallelRecPerSec);
    }
  }

  /** Run one arm: shred {@code recordCount} records split into {@code partitions} resources, timed. */
  private static double run(final String label, final long recordCount, final int partitions,
      final int maxConcurrency, final int autoCommit, final Function<String, ResourceConfiguration> configFactory)
      throws Exception {
    final Path dbDir = Files.createTempDirectory("sirix-pshred-bench-" + label + "-");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbDir));
    final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir);
    try {
      // Even split; the first (recordCount % partitions) shards carry one extra record so the totals
      // match the single-writer arm exactly.
      final long base = recordCount / partitions;
      final long remainder = recordCount % partitions;
      final List<Callable<JsonReader>> parts = new ArrayList<>(partitions);
      long nextId = 0;
      for (int p = 0; p < partitions; p++) {
        final long count = base + (p < remainder ? 1 : 0);
        final long startId = nextId;
        nextId += count;
        // Per-slice RNG seed → each partition is deterministic and independent (no shared Random
        // across writer threads); identical record shape to ScaleBenchMain via the shared generator.
        parts.add(() -> new JsonReader(new GeneratedRecordsReader(startId, count, 0x9E3779B97F4A7C15L ^ startId)));
      }

      final long t0 = System.nanoTime();
      final List<String> names =
          ParallelJsonShredder.shredPartitioned(database, parts, "records", configFactory, autoCommit, maxConcurrency);
      final long ms = (System.nanoTime() - t0) / 1_000_000L;
      final double recPerSec = recordCount * 1000.0 / Math.max(1, ms);
      System.out.printf("%-9s | partitions=%-4d | %,9d ms | %,12.0f rec/s | resources=%d%n",
          label, partitions, ms, recPerSec, names.size());
      return recPerSec;
    } finally {
      database.close();
      Databases.removeDatabase(dbDir);
    }
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import com.google.gson.stream.JsonReader;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.access.trx.node.json.BulkJsonTreeAssembler;
import io.sirix.access.trx.node.json.NdjsonAsArrayInputStream;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.ParallelJsonShredder;
import io.sirix.settings.VersioningType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

/**
 * MEASUREMENT PROBE, not a shipping loader: how does the existing partitioned parallel ingest
 * ({@link ParallelJsonShredder}, one writer per partition-resource) scale with partition count on
 * the ClickBench hits shape?
 *
 * <p>
 * This is the load-bearing question for reaching Umbra-ballpark ingestion: the tuned single-writer
 * path tops out around ~10k rows/s because one thread does everything, while ClickBench-class
 * systems ingest 100M rows in minutes by using every core. Before designing a partitioned
 * production load (query-side union, per-partition projections), this probe answers whether the
 * storage engine's shared substrate — global page caches, the off-heap frame allocator, the IO
 * layer — lets N independent writers actually run at N× or serializes them.
 *
 * <p>
 * The generator emits byte-identical disjoint slices for disjoint {@code (firstRow, rowCount)}
 * ranges under one seed, so the SAME total dataset is ingested regardless of partition count; only
 * the framing brackets per slice differ. Partitions land in resources {@code hits-0 … hits-(p-1)}.
 * No projection is armed — the probe measures the document-store substrate; per-partition
 * projections are a later design step.
 *
 * <p>
 * Usage: {@code ClickBenchParallelLoadProbe <dbDir> <totalRows> <partitions> [maxConcurrency]}
 */
public final class ClickBenchParallelLoadProbe {

  private ClickBenchParallelLoadProbe() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 3 || args.length > 4) {
      System.err.println("Usage: ClickBenchParallelLoadProbe <dbDir> <totalRows> <partitions> [maxConcurrency]");
      System.exit(2);
      return;
    }
    final Path dbDir = Path.of(args[0]);
    final long totalRows = Long.parseLong(args[1]);
    final int partitions = Integer.parseInt(args[2]);
    final int maxConcurrency = args.length == 4
        ? Integer.parseInt(args[3])
        : partitions;
    if (totalRows <= 0 || partitions <= 0 || totalRows < partitions) {
      System.err.println("need totalRows >= partitions > 0");
      System.exit(2);
      return;
    }

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    Allocators.getInstance().init(offheap);
    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "1048576"));
    final StorageType storageType =
        StorageType.fromString(System.getProperty("storageType", StorageType.FILE_CHANNEL.name()));
    final VersioningType versioningType =
        VersioningType.fromString(System.getProperty("versioningType", VersioningType.FULL.name()));
    final boolean pathSummary = Boolean.parseBoolean(System.getProperty("buildPathSummary", "true"));
    final long seed = Long.parseLong(System.getProperty("clickbench.seed", "42"));

    Files.createDirectories(dbDir);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbDir);
    Databases.createJsonDatabase(dbConfig);

    System.out.printf(Locale.ROOT,
        "# parallel-load probe: rows=%d partitions=%d concurrency=%d offheap=%d MB "
            + "autoCommit=%d storage=%s versioning=%s pathSummary=%s%n",
        totalRows, partitions, maxConcurrency, offheap / (1L << 20), autoCommit, storageType, versioningType,
        pathSummary);

    // -Dprobe.writeFile=<path>: emit the generated dataset to a file once and exit — the
    // file-based arms then measure the SHIPPING shape (real loads read files; the in-process
    // generator's char-production cost is benchmark-rig-only and inflates both arms).
    final String writeFile = System.getProperty("probe.writeFile");
    if (writeFile != null) {
      try (Reader generated = new ClickBenchHitsGenerator(0, totalRows, seed);
          Writer sink = Files.newBufferedWriter(Path.of(writeFile), StandardCharsets.UTF_8)) {
        generated.transferTo(sink);
      }
      System.out.printf(Locale.ROOT, "Wrote %d rows to %s (%d bytes)%n", totalRows, writeFile,
          Files.size(Path.of(writeFile)));
      return;
    }
    // -Dprobe.file=<path>: read the dataset from a file instead of generating in-process.
    // Single-partition only — partitioned file splitting is a later step.
    final String sourceFile = System.getProperty("probe.file");
    if (sourceFile != null && partitions != 1) {
      System.err.println("probe.file supports partitions=1 only");
      System.exit(2);
      return;
    }

    // Raw generator readers; the CURSOR arm wraps each in Gson at its use site, the BULK arm's
    // scanner consumes the chars directly — the tokenizer difference is part of what the A/B
    // measures.
    final List<Callable<Reader>> slices = new ArrayList<>(partitions);
    final long rowsPerPartition = totalRows / partitions;
    for (int i = 0; i < partitions; i++) {
      final long firstRow = i * rowsPerPartition;
      final long rowCount = i == partitions - 1
          ? totalRows - firstRow
          : rowsPerPartition;
      slices.add(sourceFile != null
          ? () -> Files.newBufferedReader(Path.of(sourceFile), StandardCharsets.UTF_8)
          : () -> new ClickBenchHitsGenerator(firstRow, rowCount, seed));
    }

    final boolean bulkAssembly = Boolean.getBoolean("probe.bulk");
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir)) {
      final long start = System.nanoTime();
      final int resourceCount;
      if (bulkAssembly) {
        // Same partitioning, resource configs, auto-commit cadence and concurrency as the cursor
        // arm — the ONLY difference is the tree BUILDER, which is exactly what the A/B measures.
        resourceCount = loadPartitionsViaBulkAssembly(database, slices, storageType, versioningType, pathSummary,
            autoCommit, maxConcurrency);
      } else {
        final List<Callable<JsonReader>> gsonSlices = new ArrayList<>(slices.size());
        for (final Callable<Reader> slice : slices) {
          gsonSlices.add(() -> new JsonReader(slice.call()));
        }
        resourceCount = ParallelJsonShredder
                                            .shredPartitioned(database, gsonSlices, "hits",
                                                name -> resourceConfig(name, storageType, versioningType, pathSummary),
                                                autoCommit, maxConcurrency, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)
                                            .size();
      }
      final double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
      System.out.printf(Locale.ROOT, "Parallel load time: %.3f s (%d resources, %.0f rows/s, builder=%s)%n", seconds,
          resourceCount, totalRows / seconds, bulkAssembly
              ? "bulk-assembly"
              : "cursor");
    }
    // Structural witness for cross-builder comparisons: node counts are dense keys, so identical
    // maxNodeKey + root child count across arms rules out dropped or duplicated records regardless
    // of how the FILE size differs (page re-touch patterns legitimately differ per builder).
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir)) {
      long maxKeySum = 0;
      long rootChildSum = 0;
      for (final String name : database.listResources()
                                       .stream()
                                       .map(Path::getFileName)
                                       .map(Object::toString)
                                       .sorted()
                                       .toList()) {
        try (JsonResourceSession session = database.beginResourceSession(name);
            var rtx = session.beginNodeReadOnlyTrx()) {
          maxKeySum += rtx.getMaxNodeKey();
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rootChildSum += rtx.getChildCount();
        }
      }
      System.out.printf(Locale.ROOT, "Structure: maxNodeKeySum=%d rootChildSum=%d%n", maxKeySum, rootChildSum);
    }
    try (var files = Files.walk(dbDir)) {
      final long bytes = files.filter(Files::isRegularFile).mapToLong(file -> {
        try {
          return Files.size(file);
        } catch (final IOException e) {
          return 0L;
        }
      }).sum();
      System.out.printf(Locale.ROOT, "Data size: %d%n", bytes);
    }
    if (Boolean.getBoolean("probe.projection")) {
      projectPartitions(dbDir);
    }
  }

  /**
   * Post-pass per-partition projection build: one {@code jn:create-projection-index} + commit per
   * resource, run sequentially so each number is an uncontended single-partition cost. Prints
   * per-partition seconds plus total and max — max approximates the parallel-build wall floor.
   */
  private static void projectPartitions(final Path dbDir) {
    final Path location = dbDir.toAbsolutePath().getParent();
    final String databaseName = dbDir.getFileName().toString();
    final List<String> resources;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir)) {
      resources = database.listResources().stream().map(Path::getFileName).map(Object::toString).sorted().toList();
    }
    if (Boolean.getBoolean("probe.projectionParallel")) {
      final long start = System.nanoTime();
      final ExecutorService pool =
          Executors.newFixedThreadPool(Math.min(resources.size(), Runtime.getRuntime().availableProcessors()));
      try {
        final List<Future<Double>> builds = new ArrayList<>(resources.size());
        for (final String resource : resources) {
          builds.add(pool.submit(() -> ClickBenchProjection.create(location, databaseName, resource)));
        }
        for (int i = 0; i < resources.size(); i++) {
          System.out.printf(Locale.ROOT, "Projection %s: %.3f s%n", resources.get(i), builds.get(i).get());
        }
      } catch (final InterruptedException | ExecutionException e) {
        throw new IllegalStateException("parallel projection build failed", e);
      } finally {
        pool.shutdown();
      }
      System.out.printf(Locale.ROOT, "Projection wall (parallel): %.3f s (%d partitions)%n",
          (System.nanoTime() - start) / 1e9, resources.size());
      return;
    }
    double total = 0;
    double max = 0;
    for (final String resource : resources) {
      final double seconds = ClickBenchProjection.create(location, databaseName, resource);
      total += seconds;
      max = Math.max(max, seconds);
      System.out.printf(Locale.ROOT, "Projection %s: %.3f s%n", resource, seconds);
    }
    System.out.printf(Locale.ROOT, "Projection total: %.3f s (max %.3f s, %d partitions)%n", total, max,
        resources.size());
  }

  private static ResourceConfiguration resourceConfig(final String name, final StorageType storageType,
      final VersioningType versioningType, final boolean pathSummary) {
    return new ResourceConfiguration.Builder(name).storageType(storageType)
                                                  .versioningApproach(versioningType)
                                                  .buildPathSummary(pathSummary)
                                                  .hashKind(HashType.NONE)
                                                  .storeNodeHistory(false)
                                                  .useDeweyIDs(false)
                                                  .build();
  }

  /**
   * One writer per partition-resource, bulk-assembly builder, cursor-arm-identical everything else.
   */
  private static int loadPartitionsViaBulkAssembly(final Database<JsonResourceSession> database,
      final List<Callable<Reader>> slices, final StorageType storageType, final VersioningType versioningType,
      final boolean pathSummary, final int autoCommit, final int maxConcurrency) throws Exception {
    final ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(maxConcurrency, slices.size())));
    try {
      final List<Future<?>> shards = new ArrayList<>(slices.size());
      for (int i = 0; i < slices.size(); i++) {
        final String name = "hits-" + i;
        final Callable<Reader> slice = slices.get(i);
        database.createResource(resourceConfig(name, storageType, versioningType, pathSummary));
        shards.add(pool.submit(() -> {
          try (JsonResourceSession session = database.beginResourceSession(name);
              JsonNodeTrx wtx = session.beginNodeTrx(autoCommit, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
            // The scanner consumes the generator's char stream DIRECTLY — no Gson tokenizer in the
            // bulk arm; the cursor arm keeps Gson, which is part of what the A/B measures.
            // -Dprobe.parallelImport=true routes through the chunked coordinator/worker pipeline
            // (P=1 in M2); default stays the sequential assembler.
            if (Boolean.getBoolean("probe.parallelImport")) {
              final String file = System.getProperty("probe.file");
              if (file != null) {
                // Byte entry: the coordinator slices/scans raw UTF-8; workers decode their own
                // chunks — the shipping shape for file loads. .gz streams decompress on the
                // feeder path; NDJSON corpora ride the array adapter, optionally row-limited.
                InputStream fileBytes = new BufferedInputStream(Files.newInputStream(Path.of(file)), 1 << 20);
                if (file.endsWith(".gz")) {
                  fileBytes = new GZIPInputStream(fileBytes, 1 << 16);
                }
                if (Boolean.getBoolean("probe.ndjson")) {
                  final long rowLimit = Long.getLong("probe.rowLimit", Long.MAX_VALUE);
                  fileBytes = new NdjsonAsArrayInputStream(fileBytes, rowLimit);
                }
                try (InputStream input = fileBytes) {
                  ParallelBulkJsonImporter.assemble(wtx, input);
                }
              } else {
                ParallelBulkJsonImporter.assemble(wtx, slice.call());
              }
            } else {
              BulkJsonTreeAssembler.assemble(wtx, slice.call());
            }
            wtx.commit();
          }
          return null;
        }));
      }
      for (final Future<?> shard : shards) {
        shard.get();
      }
      return slices.size();
    } finally {
      pool.shutdownNow();
    }
  }
}

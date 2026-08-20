package io.sirix.query.bench.clickbench;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.trx.node.HashType;
import io.sirix.cache.Allocators;
import io.sirix.io.SharedArenas;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * ClickBench loader: shreds the {@code hits} records into a SirixDB JSON resource and reports the
 * two figures the ClickBench harness asks a system for — {@code Load time} in seconds and
 * {@code Data size} in bytes.
 *
 * <pre>
 *   ClickBenchLoadMain &lt;dbDir&gt; &lt;source&gt;
 * </pre>
 * 
 * where {@code source} is any spelling {@link ClickBenchSource} accepts — a JSON array file, a
 * JSON-lines file (both optionally gzipped), or {@code generate:<rows>[:seed]} for the offline
 * synthetic dataset.
 *
 * <p>
 * Tunables, all system properties, defaulted to the fast-ingest configuration the other scale
 * benchmarks in this package use:
 * <ul>
 * <li>{@code -Dsirix.offheap.bytes} (default 24 GiB) — page buffer pool;</li>
 * <li>{@code -Dsirix.autoCommit.nodes} (default 1048576) — async-flush window in nodes. This keeps
 * roughly 1,024 record pages live per epoch and amortizes epoch rotation without introducing
 * background-flush backpressure;</li>
 * <li>{@code -DstorageType} (default FILE_CHANNEL for this benchmark) — selects the preallocated,
 * single-append-owner path that can release immutable projection payloads before the root commit.
 * MEMORY_MAPPED remains available explicitly, but its legacy physical-tail semantics cannot safely
 * prewrite rollbackable pages and therefore retain those payloads until final commit;</li>
 * <li>{@code -Dclickbench.projection} (default true) — build the projection index over the columns
 * the 43 queries touch, as part of the load. Without it the benchmark measures the row path alone
 * and no column or group-by kernel is reachable, which is what every earlier run did;</li>
 * <li>{@code -DbuildPathSummary} (defaults to {@code clickbench.projection}) — the projection
 * builder resolves its field paths through the summary, so the two cannot disagree;</li>
 * <li>{@code -DbuildPathStatistics} (default false), {@code -DhashType} (default NONE) — structures
 * the analytical queries do not need;</li>
 * <li>{@code -DstoreNodeHistory} (default false) — ClickBench reads one immutable snapshot, so its
 * load does not maintain the per-record temporal revision index. Set true only for a non-standard
 * run that exercises Sirix temporal axes;</li>
 * <li>{@code -Dclickbench.validate} (default true) — post-load type check of the first record.</li>
 * </ul>
 *
 * <p>
 * {@code Load time} includes building the projection, reported separately on its own line too:
 * DuckDB likewise builds its per-column structures while ingesting, so charging ours to query time
 * instead would be a comparison in our favour that the protocol does not allow.
 *
 * <p>
 * The validation is not ceremony: the official {@code hits.json.gz} is produced by ClickHouse,
 * whose {@code JSONEachRow} format quotes 64-bit integers by default. A quoted {@code UserID}
 * shreds as a string node, and then Q19/Q40/Q41 quietly return nothing while every other query
 * still looks plausible. Failing the load is the only way that does not turn into a wrong benchmark
 * result.
 */
public final class ClickBenchLoadMain {

  /** Columns whose exact 64-bit integer value the queries depend on. */
  private static final List<String> INT64_COLUMNS =
      List.of("WatchID", "UserID", "FUniqID", "ParamPrice", "RefererHash", "URLHash");

  /** Columns that must arrive as ISO-8601 strings for the date predicates to work. */
  private static final List<String> DATE_COLUMNS =
      List.of("EventDate", "EventTime", "ClientEventTime", "LocalEventTime");

  private ClickBenchLoadMain() {
    throw new AssertionError("no instances");
  }

  /**
   * How many records the source will deliver, for the projection's global-dictionary election.
   *
   * <p>
   * Exact for {@code generate:<rows>}; otherwise {@code -Dclickbench.expectedRows}, which for the
   * official {@code hits.json.gz} is <b>99,997,497</b>. It is not derivable from a gzip stream and
   * the election happens far too early to count, so an unhinted 100M load elects URL, Referer and
   * Title, watches their dictionaries outgrow the heap, and abandons the whole projection partway
   * through — a correct load that measures the row path. With the hint those three decline up front
   * and every other column stays indexed.
   * </p>
   */
  private static long expectedRows(final String source) {
    if (source.startsWith("generate:")) {
      final String[] parts = source.split(":");
      if (parts.length >= 2) {
        return Long.parseLong(parts[1]);
      }
    }
    return Long.getLong("clickbench.expectedRows", -1L);
  }

  /** Resolve the effective HotSpot value, rather than trusting possibly duplicated JVM arguments. */
  private static long effectiveVmOption(final String option) {
    final HotSpotDiagnosticMXBean diagnosticBean =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    if (diagnosticBean == null) {
      throw new IllegalStateException("The HFT gate requires the HotSpot diagnostic MXBean");
    }
    return Long.parseLong(diagnosticBean.getVMOption(option).getValue());
  }

  /** Resolve and validate a positive integer system property before measurement starts. */
  private static int positiveIntProperty(final String name, final int defaultValue) {
    final int value = Integer.getInteger(name, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException("-D" + name + " must be > 0, got " + value);
    }
    return value;
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: ClickBenchLoadMain <dbDir> <source>");
      System.err.println("  source: <file.json[.gz]> | <file.jsonl[.gz]> | generate:<rows>[:seed]");
      System.exit(2);
      return;
    }
    final Path dbDir = Path.of(args[0]);
    final String source = args[1];

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    final var allocator = Allocators.getInstance();
    allocator.init(offheap);

    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "1048576"));
    final StorageType storageType =
        StorageType.fromString(System.getProperty("storageType", StorageType.FILE_CHANNEL.name()));
    final boolean projection = Boolean.parseBoolean(System.getProperty("clickbench.projection", "true"));
    // One-pass by default: the projection is declared before the shred and maintained by it. The
    // second-pass route stays available (-Dclickbench.projection.incremental=false) because it is
    // the route every published number so far was measured with, and because it is what a projection
    // over an ALREADY loaded corpus has to use.
    final boolean incrementalProjection =
        Boolean.parseBoolean(System.getProperty("clickbench.projection.incremental", "true"));
    // The projection builder resolves its field paths through the path summary, so the two options
    // are not independent: a corpus loaded without a summary can never have a projection added, and
    // discovering that only when jn:create-projection-index throws costs a whole re-ingest. Default
    // the summary to whatever the projection needs, and reject the contradiction outright.
    final boolean pathSummary =
        Boolean.parseBoolean(System.getProperty("buildPathSummary", String.valueOf(projection)));
    if (projection && !pathSummary) {
      System.err.println("buildPathSummary=false cannot be combined with clickbench.projection=true — the "
          + "projection builder resolves its paths through the summary. Set one of them.");
      System.exit(2);
      return;
    }
    final boolean pathStatistics = Boolean.parseBoolean(System.getProperty("buildPathStatistics", "false"));
    final HashType hashType = HashType.fromString(System.getProperty("hashType", "NONE"));
    // The 43 ClickBench queries address the finished resource's current revision only. Maintaining
    // RECORD_TO_REVISIONS for every inserted node therefore consumes CPU, allocations and disk but
    // cannot serve a benchmark query. Keep an explicit opt-in for temporal diagnostic runs.
    final boolean storeNodeHistory = Boolean.parseBoolean(System.getProperty("storeNodeHistory", "false"));
    final long sourceExpectedRows = projection && incrementalProjection
        ? expectedRows(source)
        : -1L;
    final boolean hftTelemetry = Boolean.getBoolean("sirix.hft.telemetry");
    final int pinnedTrieScanBudget = positiveIntProperty(
        "sirix.asyncFlush.pinnedTrieSpillScanBudget", 1_024);
    final int pinnedTrieBatchCapacity = positiveIntProperty(
        "sirix.asyncFlush.pinnedTrieSpillBatchCapacity", 64);
    // Resolve these before HFT_MEASURE_START. Loading the management bean or SharedArenas class
    // inside the marked region would contaminate the very GC evidence the marker is meant to bind.
    final String hftConfiguration = hftTelemetry
        ? String.format(Locale.ROOT,
            "# HFT_CONFIG globalDict=%s autoCommitNodes=%d arenaStrategy=%s maxNewSizeBytes=%d "
                + "storage=%s projectionMode=%s expectedRows=%d pinnedTrieScanBudget=%d "
                + "pinnedTrieBatchCapacity=%d",
            System.getProperty("sirix.projection.globalDict", "auto").toLowerCase(Locale.ROOT),
            autoCommit, SharedArenas.strategy().name().toLowerCase(Locale.ROOT),
            effectiveVmOption("MaxNewSize"), storageType,
            projection
                ? incrementalProjection ? "incremental" : "second-pass"
                : "disabled",
            sourceExpectedRows, pinnedTrieScanBudget, pinnedTrieBatchCapacity)
        : null;

    Files.createDirectories(dbDir);
    System.out.printf("# ClickBench load: db=%s source=%s%n", dbDir, source);
    System.out.printf(
        "# offheap=%d MB autoCommit=%d storage=%s pathSummary=%s pathStatistics=%s hash=%s nodeHistory=%s%n",
        offheap / (1L << 20), autoCommit, storageType, pathSummary, pathStatistics, hashType, storeNodeHistory);

    long start = 0L;
    try (var store = BasicJsonDBStore.newBuilder()
                                     .location(dbDir)
                                     .storageType(storageType)
                                     .numberOfNodesBeforeAutoCommit(autoCommit)
                                     .buildPathSummary(pathSummary)
                                     .buildPathStatistics(pathStatistics)
                                     .hashType(hashType)
                                     .storeNodeHistory(storeNodeHistory)
                                     .build()) {
      try (ClickBenchSource.JacksonSource jsonSource = ClickBenchSource.openJackson(source)) {
        // Machine-readable measurement boundary for the fixed-heap HFT gate. Store/source opening
        // intentionally happens first so JVM/library startup and gzip setup cannot masquerade as
        // ingestion old-gen pressure. The end marker is emitted only after close + explicit sync.
        System.out.println("# HFT_MEASURE_START");
        if (hftConfiguration != null) {
          System.out.println(hftConfiguration);
        }
        System.out.flush();
        start = System.nanoTime();
        if (projection && incrementalProjection) {
          store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsonSource.parser(),
              ClickBenchProjection.spec(sourceExpectedRows), jsonSource.ldjson());
        } else {
          store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsonSource.parser(),
              jsonSource.ldjson());
        }
      }
    }

    // The projection index is part of LOADING, the same way DuckDB builds its own per-column
    // structures while ingesting: a run without it measures the row path and can say nothing about
    // any column or group-by kernel. Reported on its own line as well, so the ingest cost and the
    // index cost stay separable, and switchable off for a row-path A/B.
    if (!projection) {
      System.out.println("# projection: DISABLED (-Dclickbench.projection=false) — nothing will be served");
    } else if (incrementalProjection) {
      System.out.printf("# projection: columns=%d built DURING the shred (one pass)%n",
          ClickBenchProjection.PROJECTED_COLUMNS.size());
    } else {
      final double projectionSeconds = ClickBenchProjection.create(dbDir);
      System.out.printf("# projection: columns=%d built in %.3f s by a second pass%n",
          ClickBenchProjection.PROJECTED_COLUMNS.size(), projectionSeconds);
    }

    // ClickBench's own driver syncs inside the measured window so "load time" means "the data is on
    // disk"; the store's close() already flushed, this makes the page cache write-back explicit.
    sync();
    final double loadSeconds = (System.nanoTime() - start) / 1e9;
    System.out.println("# HFT_MEASURE_END");
    System.out.flush();

    final long bytes = directorySize(dbDir);
    System.out.printf("Load time: %.3f%n", loadSeconds);
    System.out.printf("Data size: %d%n", bytes);

    if (Boolean.parseBoolean(System.getProperty("clickbench.validate", "true"))) {
      validate(dbDir);
    }
    System.exit(0);
  }

  /**
   * Reads the first record back and fails loudly if the encoding is not the one the queries assume.
   *
   * @param dbDir the loaded database directory
   */
  private static void validate(final Path dbDir) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String query = ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE,
          "subsequence(for $h in $hits[] return $h, 1, 1)");
      final Sequence result = new Query(chain, query).execute(ctx);
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(result);
      }
      final String first = out.toString().trim();
      if (first.isEmpty()) {
        throw new IllegalStateException("validation failed: the resource holds no records");
      }
      final JsonElement parsed = JsonParser.parseString(first);
      if (!parsed.isJsonObject()) {
        throw new IllegalStateException("validation failed: first record is not an object: " + first);
      }
      final JsonObject record = parsed.getAsJsonObject();
      final List<String> problems = new ArrayList<>();
      for (final String column : ClickBenchSchema.COLUMNS) {
        if (!record.has(column)) {
          problems.add("missing column " + column);
        }
      }
      for (final String column : INT64_COLUMNS) {
        final JsonElement value = record.get(column);
        if (value != null && value.isJsonPrimitive() && !((JsonPrimitive) value).isNumber()) {
          problems.add(column + " is not a JSON number (" + value
              + ") — 64-bit ids must not be quoted, or Q19/Q40/Q41 silently return nothing");
        }
      }
      for (final String column : DATE_COLUMNS) {
        final JsonElement value = record.get(column);
        if (value != null && value.isJsonPrimitive() && !((JsonPrimitive) value).isString()) {
          problems.add(column + " is not an ISO-8601 string (" + value + ")");
        }
      }
      if (!problems.isEmpty()) {
        throw new IllegalStateException("ClickBench encoding validation failed:\n  " + String.join("\n  ", problems));
      }
      System.out.printf("# validation OK: %d columns, exact 64-bit ids, ISO-8601 dates%n", record.size());
    }
  }

  /** Sums every file below {@code dir}; ClickBench counts indexes and logs, i.e. everything. */
  static long directorySize(final Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).mapToLong(path -> {
        try {
          return Files.size(path);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }).sum();
    }
  }

  /** Best-effort {@code sync(1)}; ignored where it is not available. */
  private static void sync() {
    try {
      new ProcessBuilder("sync").inheritIO().start().waitFor();
    } catch (final IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.out.println("# note: could not run sync(1): " + e.getMessage());
    }
  }
}

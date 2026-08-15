package io.sirix.query.bench.clickbench;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.trx.node.HashType;
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 * <li>{@code -Dsirix.autoCommit.nodes} (default 131072) — auto-commit window in nodes;</li>
 * <li>{@code -Dclickbench.projection} (default true) — build the projection index over the columns
 * the 43 queries touch, as part of the load. Without it the benchmark measures the row path alone
 * and no column or group-by kernel is reachable, which is what every earlier run did;</li>
 * <li>{@code -DbuildPathSummary} (defaults to {@code clickbench.projection}) — the projection
 * builder resolves its field paths through the summary, so the two cannot disagree;</li>
 * <li>{@code -DbuildPathStatistics} (default false), {@code -DhashType} (default NONE) — structures
 * the analytical queries do not need;</li>
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

    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "131072"));
    final boolean projection = Boolean.parseBoolean(System.getProperty("clickbench.projection", "true"));
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

    Files.createDirectories(dbDir);
    System.out.printf("# ClickBench load: db=%s source=%s%n", dbDir, source);
    System.out.printf("# offheap=%d MB autoCommit=%d pathSummary=%s pathStatistics=%s hash=%s%n", offheap / (1L << 20),
        autoCommit, pathSummary, pathStatistics, hashType);

    final long start = System.nanoTime();
    try (var store = BasicJsonDBStore.newBuilder()
                                     .location(dbDir)
                                     .numberOfNodesBeforeAutoCommit(autoCommit)
                                     .buildPathSummary(pathSummary)
                                     .buildPathStatistics(pathStatistics)
                                     .hashType(hashType)
                                     .build()) {
      try (Reader src = ClickBenchSource.open(source); JsonReader jsonReader = new JsonReader(src)) {
        // ClickBench records nest no deeper than one object, but gson's default nesting limit is
        // conservative and the array wrapper adds a level.
        store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsonReader);
      }
    }
    double loadSeconds = (System.nanoTime() - start) / 1e9;

    // The projection index is part of LOADING, the same way DuckDB builds its own per-column
    // structures while ingesting: a run without it measures the row path and can say nothing about
    // any column or group-by kernel. Reported on its own line as well, so the ingest cost and the
    // index cost stay separable, and switchable off for a row-path A/B.
    if (projection) {
      final double projectionSeconds = ClickBenchProjection.create(dbDir);
      System.out.printf("# projection: columns=%d built in %.3f s%n", ClickBenchProjection.PROJECTED_COLUMNS.size(),
          projectionSeconds);
      loadSeconds += projectionSeconds;
    } else {
      System.out.println("# projection: DISABLED (-Dclickbench.projection=false) — nothing will be served");
    }

    // ClickBench's own driver syncs inside the measured window so "load time" means "the data is on
    // disk"; the store's close() already flushed, this makes the page cache write-back explicit.
    sync();

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

package io.sirix.query.bench.clickbench;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Runs the 43 ClickBench queries against a loaded SirixDB resource under the ClickBench protocol
 * and emits the output the ClickBench harness expects.
 *
 * <pre>
 *   ClickBenchRunMain &lt;dbDir&gt; [--tries N] [--queries 0,3,7-12] [--variant N]
 *                     [--dump DIR] [--json FILE] [--threads N] [--load-time SECONDS]
 *                     [--reuse-executor] [--comment TEXT]
 * </pre>
 *
 * <h2>What is measured</h2> Each query runs {@code --tries} times (ClickBench uses 3: try 1 is the
 * cold run, the best of tries 2 and 3 is the hot run) and every result is fully serialized, so no
 * engine-side laziness can hide work. By default a <em>fresh</em> {@link SirixVectorizedExecutor}
 * is installed for every try: the executor memoizes aggregate and group-by results by (source path,
 * predicate), and a memo hit would report a hash lookup as the hot runtime. The store, its page
 * caches and the OS page cache stay shared across tries, which is exactly what a hot run is
 * supposed to measure. Pass {@code --reuse-executor} to measure the memoized behaviour instead.
 *
 * <p>
 * {@code --dump DIR} writes each query's result to {@code DIR/qNN.jsonl}, one JSON array per result
 * row, for the differential check against DuckDB
 * ({@code bundles/sirix-query/bench/clickbench/compare-results.py}).
 */
public final class ClickBenchRunMain {

  private static final int QUERY_COUNT = 43;

  private ClickBenchRunMain() {
    throw new AssertionError("no instances");
  }

  /** The harness's command line, after parsing and validation. */
  private record Options(Path dbDir, int tries, int variant, int threads, double loadTime, boolean reuseExecutor,
      Path dumpDir, Path jsonOut, String comment, String adHoc, Set<Integer> selected, boolean buildProjection) {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: ClickBenchRunMain <dbDir> [--tries N] [--queries 0,3,7-12] "
          + "[--variant N] [--dump DIR] [--json FILE] [--threads N] [--query-file F] "
          + "[--load-time SECONDS] [--reuse-executor] [--build-projection] [--comment TEXT]");
      System.exit(2);
      return;
    }
    final Options options = parseOptions(args);

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    Allocators.getInstance().init(offheap);

    // The differential's second leg runs the interpreter, and it only really runs the interpreter if
    // the harness ALSO keeps its hands off: installing an executor explicitly overrides the
    // store-resolved auto-wiring that -Dsirix.query.autoVectorize=false switches off, so honouring
    // the kill switch here is what makes "fast path vs interpreter" an actual comparison.
    final boolean fastPaths = !"false".equalsIgnoreCase(System.getProperty("sirix.query.autoVectorize", "true"));

    // Corpora loaded before the projection became part of the load have none, and re-ingesting a
    // large one to add an index is pure waste — build it in place first, then measure as usual.
    if (options.buildProjection()) {
      final double seconds = ClickBenchProjection.create(options.dbDir());
      System.out.printf("# projection: columns=%d built in %.3f s%n", ClickBenchProjection.PROJECTED_COLUMNS.size(),
          seconds);
    }

    final double[][] timings = new double[QUERY_COUNT][options.tries()];
    for (final double[] row : timings) {
      Arrays.fill(row, Double.NaN);
    }

    System.out.printf("# ClickBench run: db=%s tries=%d variant=%d threads=%d freshExecutor=%s fastPaths=%s%n",
        options.dbDir(), options.tries(), options.variant(), options.threads(), !options.reuseExecutor(), fastPaths);

    try (var store = BasicJsonDBStore.newBuilder().location(options.dbDir()).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(ClickBenchSchema.DATABASE);
      if (collection == null) {
        throw new IllegalStateException("no database '" + ClickBenchSchema.DATABASE + "' under " + options.dbDir()
            + " — run ClickBenchLoadMain first");
      }
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(ClickBenchSchema.RESOURCE);
      final int revision = session.getMostRecentRevisionNumber();

      // Open-time catalog warm, untimed by design: every ClickBench system loads its catalog
      // metadata when the database file opens (DuckDB reads its catalog and stats then); this
      // store's equivalent is the projection handle build (directory walk + bloom blocks). The
      // segment readahead kicks here too, so it leads the first data query instead of racing it.
      final ProjectionIndexRegistry.Handle warmHandle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(), revision,
              new String[] {"[]"}, new String[] {"AdvEngineID"});
      if (warmHandle != null && warmHandle.columnStoreOrNull() != null) {
        warmHandle.kickSegmentPrefetch(Runnable::run, () -> session.beginNodeReadOnlyTrx(revision),
            trx -> ((JsonNodeReadOnlyTrx) trx).getStorageEngineReader());
      }

      SirixVectorizedExecutor shared = null;
      if (options.reuseExecutor() && fastPaths) {
        shared = new SirixVectorizedExecutor(session, revision, options.threads());
        SequentialPipelineStrategy.setVectorizedExecutor(shared);
      }
      try {
        if (options.adHoc() != null) {
          runAdHoc(options, chain, ctx, session, revision, shared, fastPaths);
          session.close();
          System.exit(0);
        }
        runSuite(options, chain, ctx, session, revision, fastPaths, timings);
      } finally {
        if (shared != null) {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          shared.close();
        }
        session.close();
      }
    }

    report(options, timings);
    System.exit(0);
  }

  /** Reads the command line; {@link #validate} checks the ranges. */
  private static Options parseOptions(final String[] args) throws IOException {
    final Path dbDir = Path.of(args[0]);
    int tries = 3;
    int variant = 0;
    int threads = Runtime.getRuntime().availableProcessors();
    double loadTime = -1.0;
    boolean reuseExecutor = false;
    Path dumpDir = null;
    Path jsonOut = null;
    String comment = null;
    String adHoc = null;
    Set<Integer> selected = null;
    boolean buildProjection = false;

    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--tries" -> tries = Integer.parseInt(requireValue(args, ++i, "--tries"));
        case "--variant" -> variant = Integer.parseInt(requireValue(args, ++i, "--variant"));
        case "--threads" -> threads = Integer.parseInt(requireValue(args, ++i, "--threads"));
        case "--load-time" -> loadTime = Double.parseDouble(requireValue(args, ++i, "--load-time"));
        case "--dump" -> dumpDir = Path.of(requireValue(args, ++i, "--dump"));
        case "--json" -> jsonOut = Path.of(requireValue(args, ++i, "--json"));
        case "--comment" -> comment = requireValue(args, ++i, "--comment");
        case "--queries" -> selected = parseSelection(requireValue(args, ++i, "--queries"));
        // A file, not a literal: the gradle wrapper task splits its argument string on whitespace,
        // and every interesting JSONiq body has spaces in it.
        case "--query-file" ->
          adHoc = Files.readString(Path.of(requireValue(args, ++i, "--query-file")), StandardCharsets.UTF_8);
        case "--reuse-executor" -> reuseExecutor = true;
        case "--build-projection" -> buildProjection = true;
        default -> throw new IllegalArgumentException("unknown option: " + args[i]);
      }
    }
    return validate(new Options(dbDir, tries, variant, threads, loadTime, reuseExecutor, dumpDir, jsonOut, comment,
        adHoc, selected, buildProjection));
  }

  private static Options validate(final Options options) throws IOException {
    if (options.tries() < 1) {
      throw new IllegalArgumentException("--tries must be >= 1");
    }
    if (options.threads() < 1) {
      throw new IllegalArgumentException("--threads must be >= 1");
    }
    if (options.dumpDir() != null) {
      Files.createDirectories(options.dumpDir());
    }
    return options;
  }

  /**
   * Escape hatch for investigating a disagreement: runs one hand-written body against the same
   * binding the catalog queries use and prints it.
   */
  private static void runAdHoc(final Options options, final SirixCompileChain chain, final SirixQueryContext ctx,
      final JsonResourceSession session, final int revision, final SirixVectorizedExecutor shared,
      final boolean fastPaths) throws IOException {
    final SirixVectorizedExecutor executor = shared != null || !fastPaths
        ? null
        : new SirixVectorizedExecutor(session, revision, options.threads());
    if (executor != null) {
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
    }
    try {
      final long t0 = System.nanoTime();
      final String serialized = execute(chain, ctx,
          ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, options.adHoc()));
      System.out.printf("# ad-hoc query took %.3f s%n", (System.nanoTime() - t0) / 1e9);
      System.out.println(serialized.length() > 4000
          ? serialized.substring(0, 4000) + "…"
          : serialized);
    } finally {
      if (executor != null) {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /** Runs the selected queries {@code --tries} times each, filling {@code timings} in place. */
  private static void runSuite(final Options options, final SirixCompileChain chain, final SirixQueryContext ctx,
      final JsonResourceSession session, final int revision, final boolean fastPaths, final double[][] timings)
      throws IOException {
    System.out.printf("%-4s | %10s | %10s | %10s | %s%n", "q", "try1(s)", "hot(s)", "rows", "note");
    for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
      if (options.selected() != null && !options.selected().contains(query.index())) {
        continue;
      }
      final String text =
          ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, query.jsoniq(options.variant()));
      String note = "";
      long rows = -1L;
      for (int t = 0; t < options.tries(); t++) {
        SirixVectorizedExecutor perTry = null;
        if (!options.reuseExecutor() && fastPaths) {
          perTry = new SirixVectorizedExecutor(session, revision, options.threads());
          SequentialPipelineStrategy.setVectorizedExecutor(perTry);
        }
        try {
          final long t0 = System.nanoTime();
          final String serialized = execute(chain, ctx, text);
          timings[query.index()][t] = (System.nanoTime() - t0) / 1e9;
          if (t == options.tries() - 1 && options.dumpDir() != null) {
            rows = dump(options.dumpDir(), query.index(), serialized);
          }
        } catch (final Exception e) {
          note = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
          break;
        } finally {
          if (perTry != null) {
            SequentialPipelineStrategy.setVectorizedExecutor(null);
            perTry.close();
          }
        }
      }
      System.out.printf("%-4d | %10s | %10s | %10s | %s%n", query.index(), format(timings[query.index()][0]),
          format(hot(timings[query.index()])), rows < 0
              ? "-"
              : Long.toString(rows),
          note);
    }
    // Which fast paths actually took the work. A route that silently declines is
    // indistinguishable from one that works — the timings alone cannot tell them apart, so a
    // "no regression" reading over a route that never engaged is the default failure mode here.
    System.out.printf(
        "# served: predicateCounts=%d groupAggregates=%d numericGroupBys=%d groupSliced=%d "
            + "sortedScans=%d predicateScans=%d valueEmissions=%d%n",
        SirixVectorizedExecutor.projectionCountsServed(), SirixVectorizedExecutor.groupAggServedCount(),
        SirixVectorizedExecutor.numericGroupByServedCount(), SirixVectorizedExecutor.groupAggSlicedServedCount(),
        SirixVectorizedExecutor.sortedScanServedCount(), SirixVectorizedExecutor.predicateScanServedCount(),
        SirixVectorizedExecutor.predicateValueEmissionsServedCount());
    System.out.printf("# chunked: lazyLoads=%d chunkMaterializations=%d eagerFallbacks=%d%n",
        ChunkedBodyConfig.lazyLoads(), ChunkedBodyConfig.chunkMaterializations(), ChunkedBodyConfig.eagerFallbacks());
  }

  /** The ClickBench output contract: a Load time line, a Data size line, then 43 timing rows. */
  private static void report(final Options options, final double[][] timings) throws IOException {
    final long dataSize = ClickBenchLoadMain.directorySize(options.dbDir());
    System.out.println();
    if (options.loadTime() >= 0) {
      System.out.printf(Locale.ROOT, "Load time: %.3f%n", options.loadTime());
    }
    System.out.printf("Data size: %d%n", dataSize);
    for (int q = 0; q < QUERY_COUNT; q++) {
      System.out.println(formatRow(timings[q]));
    }
    if (options.jsonOut() != null) {
      writeResultsJson(options.jsonOut(), timings, options.loadTime(), dataSize, options.comment());
      System.out.printf("# results JSON: %s%n", options.jsonOut().toAbsolutePath());
    }
  }

  private static String requireValue(final String[] args, final int index, final String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException(option + " needs a value");
    }
    return args[index];
  }

  /** Parses {@code 0,3,7-12} into the set of query indexes to run. */
  private static Set<Integer> parseSelection(final String spec) {
    final Set<Integer> out = new LinkedHashSet<>();
    for (final String part : spec.split(",")) {
      final String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      final int dash = trimmed.indexOf('-');
      if (dash > 0) {
        final int from = Integer.parseInt(trimmed.substring(0, dash).trim());
        final int to = Integer.parseInt(trimmed.substring(dash + 1).trim());
        if (from > to) {
          throw new IllegalArgumentException("empty range in --queries: " + trimmed);
        }
        for (int i = from; i <= to; i++) {
          out.add(checkIndex(i));
        }
      } else {
        out.add(checkIndex(Integer.parseInt(trimmed)));
      }
    }
    if (out.isEmpty()) {
      throw new IllegalArgumentException("--queries selected nothing");
    }
    return out;
  }

  private static int checkIndex(final int index) {
    if (index < 0 || index >= QUERY_COUNT) {
      throw new IllegalArgumentException("query index out of range 0.." + (QUERY_COUNT - 1) + ": " + index);
    }
    return index;
  }

  /** Executes and fully materializes the result, which is what the timing has to include. */
  private static String execute(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryText)
      throws IOException {
    final Sequence result = new Query(chain, queryText).execute(ctx);
    final StringWriter out = new StringWriter(1 << 12);
    try (PrintWriter pw = new PrintWriter(out)) {
      new StringSerializer(pw).serialize(result);
    }
    return out.toString();
  }

  /**
   * Writes one canonical JSON array per result row, which is the form {@code compare-results.py}
   * diffs against DuckDB's.
   *
   * @return the number of rows written
   */
  private static long dump(final Path dumpDir, final int index, final String serialized) throws IOException {
    final Path file = dumpDir.resolve(String.format("q%02d.jsonl", index));
    if (serialized.isBlank()) {
      // The empty sequence: a query whose HAVING or OFFSET selected nothing. An empty file is the
      // right dump — DuckDB writes zero rows for the same query.
      Files.writeString(file, "", StandardCharsets.UTF_8);
      return 0L;
    }
    long rows = 0;
    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
        JsonReader reader = new JsonReader(new StringReader(serialized))) {
      // brackit serializes a sequence as whitespace-separated values, which is exactly what a
      // lenient reader consumes one value at a time.
      reader.setStrictness(Strictness.LENIENT);
      while (reader.peek() != JsonToken.END_DOCUMENT) {
        final JsonElement item = JsonParser.parseReader(reader);
        writer.write(canonicalRow(item).toString());
        writer.write('\n');
        rows++;
      }
    }
    return rows;
  }

  /**
   * A result row as a JSON array of column values: an object contributes its values in field order
   * (which mirrors SELECT order because the queries construct them that way), anything else is a
   * single-column row.
   */
  private static JsonArray canonicalRow(final JsonElement item) {
    final JsonArray row = new JsonArray();
    if (item.isJsonObject()) {
      final JsonObject object = item.getAsJsonObject();
      for (final String key : object.keySet()) {
        row.add(canonicalCell(object.get(key)));
      }
    } else if (item.isJsonArray()) {
      for (final JsonElement element : item.getAsJsonArray()) {
        row.add(canonicalCell(element));
      }
    } else {
      row.add(canonicalCell(item));
    }
    return row;
  }

  /**
   * Renders one cell the way {@code duckdb_reference.py}'s {@code canon()} does: integers exactly
   * (JSON's single number type still carries all 64 bits as digits), fractional values rounded to six
   * significant digits so the two engines' differing last ULPs do not read as wrong answers.
   */
  private static JsonElement canonicalCell(final JsonElement cell) {
    if (cell == null || !cell.isJsonPrimitive() || !cell.getAsJsonPrimitive().isNumber()) {
      return cell;
    }
    final String text = cell.getAsJsonPrimitive().getAsString();
    if (text.indexOf('.') < 0 && text.indexOf('e') < 0 && text.indexOf('E') < 0) {
      return cell; // an integer: keep every digit
    }
    final double value = Double.parseDouble(text);
    if (!Double.isFinite(value)) {
      return new JsonPrimitive(Double.isNaN(value)
          ? "nan"
          : value > 0
              ? "inf"
              : "-inf");
    }
    return new JsonPrimitive(Double.parseDouble(String.format(Locale.ROOT, "%.6g", value)));
  }

  /** ClickBench's hot runtime: the smaller of tries 2 and 3, or NaN if either failed. */
  private static double hot(final double[] tries) {
    if (tries.length < 3) {
      return tries[tries.length - 1];
    }
    if (Double.isNaN(tries[1]) || Double.isNaN(tries[2])) {
      return Double.NaN;
    }
    return Math.min(tries[1], tries[2]);
  }

  private static String format(final double seconds) {
    return Double.isNaN(seconds)
        ? "FAILED"
        : String.format(Locale.ROOT, "%.3f", seconds);
  }

  /** One ClickBench result line: {@code [1.234, 5.678, 9.012],} with {@code null} for failures. */
  private static String formatRow(final double[] tries) {
    final StringBuilder sb = new StringBuilder(48).append('[');
    for (int i = 0; i < tries.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(Double.isNaN(tries[i])
          ? "null"
          : String.format(Locale.ROOT, "%.3f", tries[i]));
    }
    return sb.append("],").toString();
  }

  private static String firstLine(final String message) {
    if (message == null) {
      return "(no message)";
    }
    final int newline = message.indexOf('\n');
    return newline < 0
        ? message
        : message.substring(0, newline);
  }

  /** Writes the ClickBench {@code results/YYYYMMDD/<machine>.json} payload. */
  private static void writeResultsJson(final Path out, final double[][] timings, final double loadTime,
      final long dataSize, final String comment) throws IOException {
    final JsonObject root = new JsonObject();
    root.addProperty("system", "SirixDB");
    root.addProperty("date", LocalDate.now(ZoneOffset.UTC).toString());
    root.addProperty("machine", System.getProperty("clickbench.machine", "unknown"));
    root.addProperty("cluster_size", 1);
    root.addProperty("proprietary", "no");
    root.addProperty("hardware", "cpu");
    root.addProperty("tuned", "no");
    final JsonArray tags = new JsonArray();
    tags.add("Java");
    tags.add("document-oriented");
    tags.add("embedded");
    tags.add("versioned");
    root.add("tags", tags);
    if (loadTime >= 0) {
      root.addProperty("load_time", loadTime);
    } else {
      root.add("load_time", null);
    }
    root.addProperty("data_size", dataSize);
    root.add("concurrent_qps", null);
    root.add("concurrent_error_ratio", null);
    if (comment != null) {
      root.addProperty("comment", comment);
    }
    final JsonArray result = new JsonArray();
    for (final double[] tries : timings) {
      final JsonArray row = new JsonArray();
      for (final double t : tries) {
        if (Double.isNaN(t)) {
          row.add((Number) null);
        } else {
          row.add(Math.round(t * 1000.0) / 1000.0);
        }
      }
      result.add(row);
    }
    root.add("result", result);
    final Path parent = out.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(out, root + System.lineSeparator(), StandardCharsets.UTF_8);
  }
}

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
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
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

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: ClickBenchRunMain <dbDir> [--tries N] [--queries 0,3,7-12] "
          + "[--variant N] [--dump DIR] [--json FILE] [--threads N] [--query-file F] "
          + "[--load-time SECONDS] [--reuse-executor] [--comment TEXT]");
      System.exit(2);
      return;
    }
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
        default -> throw new IllegalArgumentException("unknown option: " + args[i]);
      }
    }
    if (tries < 1) {
      throw new IllegalArgumentException("--tries must be >= 1");
    }
    if (threads < 1) {
      throw new IllegalArgumentException("--threads must be >= 1");
    }
    if (dumpDir != null) {
      Files.createDirectories(dumpDir);
    }

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    Allocators.getInstance().init(offheap);

    // The differential's second leg runs the interpreter, and it only really runs the interpreter if
    // the harness ALSO keeps its hands off: installing an executor explicitly overrides the
    // store-resolved auto-wiring that -Dsirix.query.autoVectorize=false switches off, so honouring
    // the kill switch here is what makes "fast path vs interpreter" an actual comparison.
    final boolean fastPaths = !"false".equalsIgnoreCase(System.getProperty("sirix.query.autoVectorize", "true"));

    final double[][] timings = new double[QUERY_COUNT][tries];
    for (final double[] row : timings) {
      Arrays.fill(row, Double.NaN);
    }

    System.out.printf("# ClickBench run: db=%s tries=%d variant=%d threads=%d freshExecutor=%s fastPaths=%s%n", dbDir,
        tries, variant, threads, !reuseExecutor, fastPaths);

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(ClickBenchSchema.DATABASE);
      if (collection == null) {
        throw new IllegalStateException(
            "no database '" + ClickBenchSchema.DATABASE + "' under " + dbDir + " — run ClickBenchLoadMain first");
      }
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(ClickBenchSchema.RESOURCE);
      final int revision = session.getMostRecentRevisionNumber();

      SirixVectorizedExecutor shared = null;
      if (reuseExecutor && fastPaths) {
        shared = new SirixVectorizedExecutor(session, revision, threads);
        SequentialPipelineStrategy.setVectorizedExecutor(shared);
      }
      try {
        if (adHoc != null) {
          // Escape hatch for investigating a disagreement: run one hand-written body against the
          // same binding the catalog queries use, print it, and stop.
          final SirixVectorizedExecutor executor = shared != null || !fastPaths
              ? null
              : new SirixVectorizedExecutor(session, revision, threads);
          if (executor != null) {
            SequentialPipelineStrategy.setVectorizedExecutor(executor);
          }
          try {
            final long t0 = System.nanoTime();
            final String serialized = execute(chain, ctx,
                ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, adHoc));
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
          session.close();
          System.exit(0);
        }
        System.out.printf("%-4s | %10s | %10s | %10s | %s%n", "q", "try1(s)", "hot(s)", "rows", "note");
        for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
          if (selected != null && !selected.contains(query.index())) {
            continue;
          }
          final String text =
              ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, query.jsoniq(variant));
          String note = "";
          long rows = -1L;
          for (int t = 0; t < tries; t++) {
            SirixVectorizedExecutor perTry = null;
            if (!reuseExecutor && fastPaths) {
              perTry = new SirixVectorizedExecutor(session, revision, threads);
              SequentialPipelineStrategy.setVectorizedExecutor(perTry);
            }
            try {
              final long t0 = System.nanoTime();
              final String serialized = execute(chain, ctx, text);
              timings[query.index()][t] = (System.nanoTime() - t0) / 1e9;
              if (t == tries - 1 && dumpDir != null) {
                rows = dump(dumpDir, query.index(), serialized);
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
      } finally {
        if (shared != null) {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          shared.close();
        }
        session.close();
      }
    }

    final long dataSize = ClickBenchLoadMain.directorySize(dbDir);
    System.out.println();
    if (loadTime >= 0) {
      System.out.printf(Locale.ROOT, "Load time: %.3f%n", loadTime);
    }
    System.out.printf("Data size: %d%n", dataSize);
    for (int q = 0; q < QUERY_COUNT; q++) {
      System.out.println(formatRow(timings[q]));
    }
    if (jsonOut != null) {
      writeResultsJson(jsonOut, timings, loadTime, dataSize, comment);
      System.out.printf("# results JSON: %s%n", jsonOut.toAbsolutePath());
    }
    System.exit(0);
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

package io.sirix.query.bench.clickbench;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.sun.management.OperatingSystemMXBean;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.projection.HeapHeadroom;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.SirixArraySize;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Runs the 43 ClickBench queries against a loaded SirixDB resource under the ClickBench protocol
 * and emits the output the ClickBench harness expects.
 *
 * <pre>
 *   ClickBenchRunMain &lt;dbDir&gt; [--tries N] [--queries 0,3,7-12] [--variant N]
 *                     [--dump DIR] [--json FILE] [--threads N] [--load-time SECONDS]
 *                     [--reuse-executor] [--require-vectorized-serving]
 *                     [--require-generic-serving] [--comment TEXT]
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

  static final String RESULT_FORMAT_MARKER = ".clickbench-result-format";

  static final String RESULT_FORMAT_VERSION = "clickbench-jsonl-v2-lossless-float64";

  private ClickBenchRunMain() {
    throw new AssertionError("no instances");
  }

  /** Fail-closed route proof requested by a correctness campaign. */
  enum ServingProof {
    NONE,
    REQUIRE_VECTORIZED,
    REQUIRE_GENERIC
  }

  /**
   * Outcome counters that prove a query returned work from an analytical executor route.
   *
   * <p>Attempt, lookup, page-read, and decline counters are deliberately absent: any of those can
   * move before the executor returns {@code null} and the generic pipeline produces the answer.
   */
  enum ServingRoute {
    STRUCTURAL_ARRAY_SIZE("structural-array-size", SirixArraySize::storedArraySizesServedCount, false),
    PREDICATE_COUNT("predicate-count", SirixVectorizedExecutor::projectionCountsServed),
    PROJECTION_AGGREGATE("projection-aggregate", SirixVectorizedExecutor::projectionAggregatesServed),
    PROJECTION_COUNT_DISTINCT("projection-count-distinct",
        SirixVectorizedExecutor::projectionCountDistinctServedCount),
    STRING_MIN_MAX("string-min-max", SirixVectorizedExecutor::stringMinMaxServedCount),
    DOUBLE_VALUE_AGGREGATE("double-value-aggregate", SirixVectorizedExecutor::doubleValueAggregatesServed),
    BINARY_AGGREGATE("binary-aggregate", SirixVectorizedExecutor::binaryAggregatesServed),
    COMPUTED_AGGREGATE("computed-aggregate", SirixVectorizedExecutor::computedAggServedCount),
    PATH_SUMMARY_STAT("path-summary-stat", SirixVectorizedExecutor::pathSummaryStatsServed),
    GROUP_AGGREGATE("group-aggregate", SirixVectorizedExecutor::groupAggServedCount),
    CONST_GROUP_AGGREGATE("const-group-aggregate", SirixVectorizedExecutor::constGroupAggServedCount),
    NUMERIC_GROUP_BY("numeric-group-by", SirixVectorizedExecutor::numericGroupByServedCount),
    GROUP_DISTINCT("group-distinct", SirixVectorizedExecutor::groupDistinctServedCount),
    GROUP_DENSE("global-dictionary-group", SirixVectorizedExecutor::groupDenseServedCount),
    SORTED_SCAN("sorted-scan", SirixVectorizedExecutor::sortedScanServedCount),
    PREDICATE_SCAN("predicate-scan", SirixVectorizedExecutor::predicateScanServedCount),
    PREDICATE_VALUE_EMISSION("predicate-value-emission",
        SirixVectorizedExecutor::predicateValueEmissionsServedCount),
    ROW_MATERIALIZATION("row-materialization", SirixVectorizedExecutor::rowMaterializeServedCount);

    private final String label;
    private final LongSupplier counter;
    private final boolean vectorized;

    ServingRoute(final String label, final LongSupplier counter) {
      this(label, counter, true);
    }

    ServingRoute(final String label, final LongSupplier counter, final boolean vectorized) {
      this.label = label;
      this.counter = counter;
      this.vectorized = vectorized;
    }

    String label() {
      return label;
    }

    long count() {
      return counter.getAsLong();
    }

    boolean vectorized() {
      return vectorized;
    }
  }

  /** The harness's command line, after parsing and validation. */
  private record Options(Path dbDir, int tries, int variant, int threads, double loadTime, boolean reuseExecutor,
      Path dumpDir, Path jsonOut, String comment, String adHoc, Set<Integer> selected, boolean buildProjection,
      ServingProof servingProof, Set<Integer> histogramAfter) {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: ClickBenchRunMain <dbDir> [--tries N] [--queries 0,3,7-12] "
          + "[--variant N] [--dump DIR] [--json FILE] [--threads N] [--query-file F] "
          + "[--load-time SECONDS] [--reuse-executor] [--build-projection] "
          + "[--require-vectorized-serving|--require-generic-serving] [--comment TEXT] "
          + "[--histogram-after 31,35]");
      System.exit(2);
      return;
    }
    final Options options = parseOptions(args);

    // The differential's second leg runs the interpreter, and it only really runs the interpreter if
    // the harness ALSO keeps its hands off: installing an executor explicitly overrides the
    // store-resolved auto-wiring that -Dsirix.query.autoVectorize=false switches off, so honouring
    // the kill switch here is what makes "fast path vs interpreter" an actual comparison.
    final boolean fastPaths = !"false".equalsIgnoreCase(System.getProperty("sirix.query.autoVectorize", "true"));
    validateServingProofConfiguration(options.servingProof(), fastPaths, options.adHoc() != null);

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    Allocators.getInstance().init(offheap);

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

    // A serving-proof run must FAIL where a serving arm fails: the executor's fail-soft fallback would
    // otherwise answer the query through the interpreter — invisible until it finishes, which at
    // 100M rows is an hour that looks like a hang.
    final boolean strictServing = options.servingProof() == ServingProof.REQUIRE_VECTORIZED;
    SirixVectorizedExecutor.STRICT_SERVING = strictServing;
    System.out.printf("# ClickBench run: db=%s tries=%d variant=%d threads=%d freshExecutor=%s fastPaths=%s strictServing=%s%n",
        options.dbDir(), options.tries(), options.variant(), options.threads(), !options.reuseExecutor(), fastPaths,
        strictServing);

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
      if (!Boolean.getBoolean("clickbench.catalogWarm.disabled")) {
        final ProjectionIndexRegistry.Handle warmHandle =
            ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
                revision, new String[] {"[]"}, new String[] {"AdvEngineID"});
        if (warmHandle != null && warmHandle.columnStoreOrNull() != null) {
          warmHandle.kickSegmentPrefetch(Runnable::run, () -> session.beginNodeReadOnlyTrx(revision),
              trx -> ((JsonNodeReadOnlyTrx) trx).getStorageEngineReader());
        }
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
    ServingProof servingProof = ServingProof.NONE;
    Set<Integer> histogramAfter = Set.of();

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
        // Diagnostics, never timing: a live-object histogram forces a full collection, so it runs
        // after ALL tries of the named queries and prints what the heap still holds between queries.
        case "--histogram-after" -> histogramAfter = parseSelection(requireValue(args, ++i, "--histogram-after"));
        // A file, not a literal: the gradle wrapper task splits its argument string on whitespace,
        // and every interesting JSONiq body has spaces in it.
        case "--query-file" ->
          adHoc = Files.readString(Path.of(requireValue(args, ++i, "--query-file")), StandardCharsets.UTF_8);
        case "--reuse-executor" -> reuseExecutor = true;
        case "--build-projection" -> buildProjection = true;
        case "--require-vectorized-serving" ->
          servingProof = selectServingProof(servingProof, ServingProof.REQUIRE_VECTORIZED, args[i]);
        case "--require-generic-serving" ->
          servingProof = selectServingProof(servingProof, ServingProof.REQUIRE_GENERIC, args[i]);
        default -> throw new IllegalArgumentException("unknown option: " + args[i]);
      }
    }
    return validate(new Options(dbDir, tries, variant, threads, loadTime, reuseExecutor, dumpDir, jsonOut, comment,
        adHoc, selected, buildProjection, servingProof, histogramAfter));
  }

  private static ServingProof selectServingProof(final ServingProof current, final ServingProof requested,
      final String option) {
    if (current != ServingProof.NONE) {
      throw new IllegalArgumentException("route proof already selected as " + current + "; cannot also use " + option);
    }
    return requested;
  }

  private static Options validate(final Options options) throws IOException {
    if (options.tries() < 1) {
      throw new IllegalArgumentException("--tries must be >= 1");
    }
    if (options.threads() < 1) {
      throw new IllegalArgumentException("--threads must be >= 1");
    }
    if (options.dumpDir() != null) {
      prepareDumpDirectory(options.dumpDir());
      if (options.adHoc() == null) {
        invalidateSelectedDumps(options.dumpDir(), options.selected());
      }
    }
    return options;
  }

  static void validateServingProofConfiguration(final ServingProof proof, final boolean fastPaths,
      final boolean adHoc) {
    Objects.requireNonNull(proof, "proof");
    if (adHoc && proof != ServingProof.NONE) {
      throw new IllegalArgumentException("serving proof flags apply to the numbered ClickBench suite, not --query-file");
    }
    if (proof == ServingProof.REQUIRE_VECTORIZED && !fastPaths) {
      throw new IllegalArgumentException(
          "--require-vectorized-serving requires -Dsirix.query.autoVectorize=true");
    }
    if (proof == ServingProof.REQUIRE_GENERIC && fastPaths) {
      throw new IllegalArgumentException("--require-generic-serving requires -Dsirix.query.autoVectorize=false");
    }
  }

  /**
   * Establish the lossless result encoding before publishing any query file.
   *
   * <p>An unmarked non-empty directory may contain the former six-significant-digit dumps. Marking
   * that directory in place would make irreversibly rounded files look current, so it is rejected
   * rather than upgraded. A current directory may be reused for a partial diagnostic rerun because
   * every file ever published under its marker has the same lossless binary64 contract.
   */
  static void prepareDumpDirectory(final Path dumpDir) throws IOException {
    Files.createDirectories(dumpDir);
    final Path marker = dumpDir.resolve(RESULT_FORMAT_MARKER);
    if (Files.exists(marker)) {
      final String actual = Files.readString(marker, StandardCharsets.UTF_8).strip();
      if (!RESULT_FORMAT_VERSION.equals(actual)) {
        throw new IOException("unsupported ClickBench result encoding in " + marker + ": " + actual);
      }
      return;
    }
    try (var entries = Files.list(dumpDir)) {
      if (entries.findAny().isPresent()) {
        throw new IOException("refusing to mark non-empty legacy ClickBench result directory as "
            + RESULT_FORMAT_VERSION + ": " + dumpDir);
      }
    }
    Files.writeString(marker, RESULT_FORMAT_VERSION + '\n', StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
  }

  /**
   * Removes every selected final/partial result before any database or query work begins.
   *
   * <p>A failed rerun must be represented by a missing result, never by the successful output of a
   * previous run. Unselected files are retained for the explicit {@code --queries} diagnostic
   * workflow.
   */
  static void invalidateSelectedDumps(final Path dumpDir, final Set<Integer> selected) throws IOException {
    for (int index = 0; index < QUERY_COUNT; index++) {
      if (selected == null || selected.contains(index)) {
        Files.deleteIfExists(resultFile(dumpDir, index));
        Files.deleteIfExists(partialResultFile(dumpDir, index));
      }
    }
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
    final List<String> proofFailures = new ArrayList<>();
    System.out.printf("%-4s | %10s | %10s | %10s | %s%n", "q", "try1(s)", "hot(s)", "rows", "note");
    for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
      if (options.selected() != null && !options.selected().contains(query.index())) {
        continue;
      }
      final String text =
          ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, query.jsoniq(options.variant()));
      String note = "";
      long rows = -1L;
      boolean completed = true;
      final EnumSet<ServingRoute> queryRoutes = EnumSet.noneOf(ServingRoute.class);
      for (int t = 0; t < options.tries(); t++) {
        SirixVectorizedExecutor perTry = null;
        if (!options.reuseExecutor() && fastPaths) {
          perTry = new SirixVectorizedExecutor(session, revision, options.threads());
          SequentialPipelineStrategy.setVectorizedExecutor(perTry);
        }
        try {
          final long[] servingBefore = captureServingCounters();
          final long cpu0 = processCpuNanos();
          final long gcCount0 = gcCount();
          final long gcMillis0 = gcMillis();
          final long t0 = System.nanoTime();
          final String serialized = execute(chain, ctx, text);
          final double wall = (System.nanoTime() - t0) / 1e9;
          timings[query.index()][t] = wall;
          printTryResources(query.index(), t, wall, processCpuNanos() - cpu0, gcCount() - gcCount0,
              gcMillis() - gcMillis0);
          final EnumSet<ServingRoute> tryRoutes = servingDelta(servingBefore, captureServingCounters());
          queryRoutes.addAll(tryRoutes);
          final String tryProofFailure = servingProofFailureForTry(options.servingProof(), query.index(), t,
              tryRoutes);
          if (tryProofFailure != null) {
            proofFailures.add(tryProofFailure);
          }
          if (t == options.tries() - 1 && options.dumpDir() != null) {
            rows = dump(options.dumpDir(), query.index(), serialized);
          }
        } catch (final Exception e) {
          if (Boolean.getBoolean("clickbench.printStackTraces")) {
            e.printStackTrace(System.err);
          }
          note = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
          completed = false;
          break;
        } finally {
          if (perTry != null) {
            SequentialPipelineStrategy.setVectorizedExecutor(null);
            perTry.close();
          }
        }
      }
      if (!completed) {
        final String incompleteFailure = servingProofFailure(options.servingProof(), query.index(), false, queryRoutes);
        if (incompleteFailure != null) {
          proofFailures.add(incompleteFailure);
        }
      }
      final String routeNote = formatServingRoutes(fastPaths, queryRoutes);
      note = note.isEmpty()
          ? routeNote
          : note + "; " + routeNote;
      System.out.printf("%-4d | %10s | %10s | %10s | %s%n", query.index(), format(timings[query.index()][0]),
          format(hot(timings[query.index()])), rows < 0
              ? "-"
              : Long.toString(rows),
          note);
      if (options.histogramAfter().contains(query.index())) {
        printLiveClassHistogram(query.index());
      }
    }
    // Which fast paths actually took the work. A route that silently declines is
    // indistinguishable from one that works — the timings alone cannot tell them apart, so a
    // "no regression" reading over a route that never engaged is the default failure mode here.
    System.out.printf(
        "# served: structuralArraySizes=%d predicateCounts=%d projectionAggregates=%d projectionCountDistinct=%d "
            + "stringMinMax=%d doubleAggregates=%d binaryAggregates=%d computedAggregates=%d pathSummaryStats=%d "
            + "groupAggregates=%d constGroupAggregates=%d numericGroupBys=%d groupDistinct=%d denseGlobalGroups=%d "
            + "groupSliced=%d groupWindowedSlices=%d sortedScans=%d predicateScans=%d valueEmissions=%d"
            + " rowMaterializations=%d%n",
        SirixArraySize.storedArraySizesServedCount(), SirixVectorizedExecutor.projectionCountsServed(),
        SirixVectorizedExecutor.projectionAggregatesServed(),
        SirixVectorizedExecutor.projectionCountDistinctServedCount(),
        SirixVectorizedExecutor.stringMinMaxServedCount(), SirixVectorizedExecutor.doubleValueAggregatesServed(),
        SirixVectorizedExecutor.binaryAggregatesServed(), SirixVectorizedExecutor.computedAggServedCount(),
        SirixVectorizedExecutor.pathSummaryStatsServed(), SirixVectorizedExecutor.groupAggServedCount(),
        SirixVectorizedExecutor.constGroupAggServedCount(), SirixVectorizedExecutor.numericGroupByServedCount(),
        SirixVectorizedExecutor.groupDistinctServedCount(), SirixVectorizedExecutor.groupDenseServedCount(),
        SirixVectorizedExecutor.groupAggSlicedServedCount(),
        SirixVectorizedExecutor.groupWindowedSlicesCount(), SirixVectorizedExecutor.sortedScanServedCount(),
        SirixVectorizedExecutor.predicateScanServedCount(),
        SirixVectorizedExecutor.predicateValueEmissionsServedCount(),
        SirixVectorizedExecutor.rowMaterializeServedCount());
    System.out.printf("# chunked: lazyLoads=%d chunkMaterializations=%d eagerFallbacks=%d%n",
        ChunkedBodyConfig.lazyLoads(), ChunkedBodyConfig.chunkMaterializations(), ChunkedBodyConfig.eagerFallbacks());
    if (!proofFailures.isEmpty()) {
      throw new IllegalStateException("ClickBench serving proof failed:\n  " + String.join("\n  ", proofFailures));
    }
  }

  static long[] captureServingCounters() {
    final ServingRoute[] routes = ServingRoute.values();
    final long[] counters = new long[routes.length];
    for (int i = 0; i < routes.length; i++) {
      counters[i] = routes[i].count();
    }
    return counters;
  }

  static EnumSet<ServingRoute> servingDelta(final long[] before, final long[] after) {
    Objects.requireNonNull(before, "before");
    Objects.requireNonNull(after, "after");
    final ServingRoute[] routes = ServingRoute.values();
    if (before.length != routes.length || after.length != routes.length) {
      throw new IllegalArgumentException("serving counter snapshot width changed: expected " + routes.length
          + ", before=" + before.length + ", after=" + after.length);
    }
    final EnumSet<ServingRoute> served = EnumSet.noneOf(ServingRoute.class);
    for (int i = 0; i < routes.length; i++) {
      if (after[i] < before[i]) {
        throw new IllegalStateException("serving counter " + routes[i].label() + " moved backwards from "
            + before[i] + " to " + after[i]);
      }
      if (after[i] > before[i]) {
        served.add(routes[i]);
      }
    }
    return served;
  }

  static String servingProofFailure(final ServingProof proof, final int queryIndex, final boolean completed,
      final Set<ServingRoute> routes) {
    Objects.requireNonNull(proof, "proof");
    Objects.requireNonNull(routes, "routes");
    if (proof == ServingProof.NONE) {
      return null;
    }
    if (!completed) {
      return "q" + queryIndex + " did not complete every requested try";
    }
    if (proof == ServingProof.REQUIRE_VECTORIZED) {
      if (queryIndex == 0 && !routes.contains(ServingRoute.STRUCTURAL_ARRAY_SIZE)) {
        return "q0 completed without the O(1) stored-array cardinality serving counter";
      }
      if (queryIndex != 0 && routes.stream().noneMatch(ServingRoute::vectorized)) {
        return "q" + queryIndex + " completed without any outcome-level vectorized serving counter";
      }
    }
    if (proof == ServingProof.REQUIRE_GENERIC && routes.stream().anyMatch(ServingRoute::vectorized)) {
      return "q" + queryIndex + " was required to stay generic but emitted vectorized route evidence: "
          + formatServingRoutes(true, routes);
    }
    return null;
  }

  static String servingProofFailureForTry(final ServingProof proof, final int queryIndex, final int tryIndex,
      final Set<ServingRoute> routes) {
    if (tryIndex < 0) {
      throw new IllegalArgumentException("tryIndex must be non-negative: " + tryIndex);
    }
    final String failure = servingProofFailure(proof, queryIndex, true, routes);
    return failure == null
        ? null
        : failure + " on try " + (tryIndex + 1);
  }

  static String formatServingRoutes(final boolean fastPaths, final Set<ServingRoute> routes) {
    Objects.requireNonNull(routes, "routes");
    if (routes.isEmpty()) {
      return fastPaths
          ? "route=NONE"
          : "route=generic";
    }
    final StringBuilder evidence = new StringBuilder("route=");
    boolean first = true;
    for (final ServingRoute route : routes) {
      if (!first) {
        evidence.append('+');
      }
      evidence.append(route.label());
      first = false;
    }
    return evidence.toString();
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
  static long dump(final Path dumpDir, final int index, final String serialized) throws IOException {
    final Path file = resultFile(dumpDir, index);
    final Path partial = partialResultFile(dumpDir, index);
    Files.deleteIfExists(file);
    Files.deleteIfExists(partial);
    try {
      long rows = 0;
      if (serialized.isBlank()) {
        // The empty sequence: a query whose HAVING or OFFSET selected nothing. An empty file is the
        // right dump — DuckDB writes zero rows for the same query.
        Files.writeString(partial, "", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
      } else {
        try (BufferedWriter writer = Files.newBufferedWriter(partial, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
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
      }
      Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE);
      return rows;
    } catch (final IOException | RuntimeException exception) {
      try {
        Files.deleteIfExists(partial);
      } catch (final IOException cleanupFailure) {
        exception.addSuppressed(cleanupFailure);
      }
      throw exception;
    }
  }

  private static Path resultFile(final Path dumpDir, final int index) {
    return dumpDir.resolve(String.format("q%02d.jsonl", index));
  }

  private static Path partialResultFile(final Path dumpDir, final int index) {
    return dumpDir.resolve(String.format("q%02d.jsonl.partial", index));
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
   * (JSON's single number type still carries all 64 bits as digits) and finite binary64 values with
   * Gson's shortest round-trip representation. No decimal quantisation is permitted here: even a
   * one-unit error in an integer sum can become a sub-micro average at ClickBench scale.
   */
  static JsonElement canonicalCell(final JsonElement cell) {
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
    return new JsonPrimitive(value);
  }

  /**
   * The JVM's own {@code GC.class_histogram} over LIVE objects, taken in-process through the
   * diagnostic-command MBean (the sandbox this runs under hides the JVM from {@code jcmd}). Forces a
   * full collection, so the {@code Total} line is the heap the previous queries really left behind
   * — the figure every heap-derived budget (group passes, distinct ceiling, residency) should see —
   * and the top classes say who owns it. Diagnostics only; never inside a timed try.
   */
  private static void printLiveClassHistogram(final int queryIndex) {
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final ObjectName name = new ObjectName("com.sun.management:type=DiagnosticCommand");
      final String histogram = (String) server.invoke(name, "gcClassHistogram", new Object[] {new String[0]},
          new String[] {String[].class.getName()});
      final Runtime runtime = Runtime.getRuntime();
      System.err.println("# histogram after q" + queryIndex + " (live objects after a forced full GC; usedMB="
          + ((runtime.totalMemory() - runtime.freeMemory()) >> 20) + " headroom: " + HeapHeadroom.describe() + ")");
      final String[] lines = histogram.split("\n");
      final int shown = Math.min(lines.length, HISTOGRAM_LINES);
      for (int i = 0; i < shown; i++) {
        System.err.println("#   " + lines[i]);
      }
      if (lines.length > shown) {
        System.err.println("#   " + lines[lines.length - 1]);
      }
    } catch (final Exception e) {
      System.err.println("# histogram after q" + queryIndex + " unavailable: " + e);
    }
  }

  /** Header, two title lines and the largest classes; the {@code Total} line is appended separately. */
  private static final int HISTOGRAM_LINES = 45;

  private static final OperatingSystemMXBean OS_BEAN =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  private static final List<GarbageCollectorMXBean> GC_BEANS = ManagementFactory.getGarbageCollectorMXBeans();
  private static final int CORES = Runtime.getRuntime().availableProcessors();

  /** Process CPU time in nanoseconds (all threads, collector threads included); -1 when unsupported. */
  private static long processCpuNanos() {
    return OS_BEAN.getProcessCpuTime();
  }

  private static long gcCount() {
    long count = 0;
    for (final GarbageCollectorMXBean gc : GC_BEANS) {
      count += Math.max(0L, gc.getCollectionCount());
    }
    return count;
  }

  private static long gcMillis() {
    long millis = 0;
    for (final GarbageCollectorMXBean gc : GC_BEANS) {
      millis += Math.max(0L, gc.getCollectionTime());
    }
    return millis;
  }

  /**
   * One stderr line per try beside the wall time: process CPU seconds, utilisation as "busy cores" out of the
   * machine's cores, and the collector's pause count/seconds. A query whose utilisation is far below the core
   * count is WAITING (I/O, parking, a serial phase), and a pause that overlaps a wait costs no wall time — so
   * the CPU share of a profile must never be read as a wall share without this line.
   */
  private static void printTryResources(final int queryIndex, final int tryIndex, final double wallSeconds,
      final long cpuNanos, final long gcPauses, final long gcPauseMillis) {
    final double cpuSeconds = cpuNanos < 0 ? Double.NaN : cpuNanos / 1e9;
    System.err.printf(Locale.ROOT, "# q%d try %d: wall=%.3f s cpu=%.1f s util=%.1f/%d gc=%d pauses %.2f s%n",
        queryIndex, tryIndex + 1, wallSeconds, cpuSeconds, cpuSeconds / Math.max(wallSeconds, 1e-9), CORES,
        gcPauses, gcPauseMillis / 1e3);
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

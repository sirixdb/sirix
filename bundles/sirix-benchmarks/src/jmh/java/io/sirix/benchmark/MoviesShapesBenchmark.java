/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * The six shapes of the DuckDB comparison, measured warm under JMH.
 *
 * <p>Written because the hand-rolled loop in {@link PostgresBulkBench} could not settle a question
 * it was being asked: on a loaded box the same shape came back at 230 ms and at 1,192 ms across
 * runs of an identical configuration, which is enough noise to invert an A/B verdict. JMH's forks,
 * warm-up discipline and per-iteration statistics are the fix — {@code docs/BENCHMARK_DESIGN.md} R4
 * says as much, and {@link BulkQueryScanBenchmark} already applies it to four other shapes.
 *
 * <h2>The memo, which is why this is not just a copy of that benchmark</h2>
 *
 * <p>{@link SirixVectorizedExecutor} memoizes filtered counts, aggregates and group-bys by
 * (source path, predicate). Run the SAME query twice and the second answer comes from a hash map —
 * so a benchmark that repeats one query measures a map lookup, not a scan, and reports microseconds
 * for work that takes milliseconds. Every invocation therefore drops the result caches first
 * ({@code clearAggregateResultCachesForBenchmarks}), which leaves the machinery under measurement —
 * field keys, compiled predicates, page region tables, projection column slices — untouched.
 *
 * <p>The executor is installed EXPLICITLY rather than auto-wired, purely so the benchmark holds the
 * reference it needs to clear those caches; the paths measured are the same ones auto-wiring
 * reaches.
 *
 * <h2>Running it</h2>
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh \
 *       -Pjmh.includes=MoviesShapesBenchmark \
 *       -Pjmh.jvmArgs="-Dsirix.bench.store=/tmp/claude-1000/bench -Dsirix.bench.db=db-elem"
 * </pre>
 *
 * <p>Point it at a store built by {@code PostgresBulkBench ingest} in {@code single} mode. Shapes
 * S1-S4 and S6 want the projection index built ({@code jn:create-projection-index}); without it they
 * measure the storage scan instead, which is a different (and slower) answer to the same question.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// Only the heap here: the module/preview/open flags come from the gradle jmh block, and repeating
// them would hand the forked JVM the same option twice.
@Fork(value = 1, jvmArgsAppend = {"-Xmx12g"})
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
public class MoviesShapesBenchmark {

  /** Matches {@code matrix.sh}, so this and the cold harness measure the same six shapes. */
  private static final String S1 = "count(for $m in $doc[] where $m.year > 1990 return $m)";
  private static final String S2 =
      "count(for $m in $doc[] where $m.year > 1990 and $m.thumbnail_width > 200 return $m)";
  private static final String S3 =
      "sum(for $m in $doc[] return $m.thumbnail_width * $m.thumbnail_height)";
  private static final String S4 = "count(for $m in $doc[] let $y := $m.year group by $y return $y)";
  private static final String S5 =
      "count(for $m in $doc[] where some $g in $m.genres[] satisfies $g eq \"Drama\" return $m)";
  private static final String S6 =
      "count(for $m in $doc[] where $m.title eq \"Saleslady\" return $m)";

  private static final QNm DOC_VAR = new QNm("doc");
  private static final String RESOURCE = "movies";

  private BasicJsonDBStore store;
  private SirixQueryContext ctx;
  private SirixCompileChain chain;
  private JsonResourceSession session;
  private SirixVectorizedExecutor executor;

  @Setup(Level.Trial)
  public void setUp() {
    final Path location = Paths.get(System.getProperty("sirix.bench.store",
                                                       System.getProperty("java.io.tmpdir")));
    final String dbName = System.getProperty("sirix.bench.db", "db-elem");

    store = BasicJsonDBStore.newBuilder().location(location).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
    session = coll.getDatabase().beginResourceSession(RESOURCE);
    executor = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(),
                                           Runtime.getRuntime().availableProcessors());
    SequentialPipelineStrategy.setVectorizedExecutor(executor);
    ctx.bind(DOC_VAR, (Sequence) coll.getDocument());

    // Fault the resource in before anything is measured. The first pass over this corpus loads
    // every page — seconds against milliseconds warm — and JMH's warm-up would average that in
    // rather than exclude it.
    run(S1);
    run(S2);
    run(S3);
    run(S4);
    run(S5);
    run(S6);
  }

  /**
   * Drop the memoized RESULTS before each invocation, so every measured run is a scan.
   *
   * <p>Without this the benchmark reports the cost of a {@code ConcurrentHashMap.get}. Per
   * invocation rather than per iteration because JMH runs many invocations inside one iteration,
   * and only the first of them would otherwise miss.
   */
  @Setup(Level.Invocation)
  public void dropResultMemo() {
    executor.clearAggregateResultCachesForBenchmarks();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    executor.close();
    session.close();
    chain.close();
    ctx.close();
    store.close();
  }

  /** Execute and fully serialize, so a lazy sequence cannot be left unevaluated. */
  private String run(final String query) {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(sink)) {
      new Query(chain, "declare variable $doc external; " + query).serialize(ctx, ps);
    }
    return sink.toString();
  }

  @Benchmark
  public void s1FilterCountYear(final Blackhole bh) {
    bh.consume(run(S1));
  }

  @Benchmark
  public void s2Conjunction(final Blackhole bh) {
    bh.consume(run(S2));
  }

  @Benchmark
  public void s3ProductAggregate(final Blackhole bh) {
    bh.consume(run(S3));
  }

  @Benchmark
  public void s4GroupByYear(final Blackhole bh) {
    bh.consume(run(S4));
  }

  @Benchmark
  public void s5ArrayMembership(final Blackhole bh) {
    bh.consume(run(S5));
  }

  @Benchmark
  public void s6TitleLookup(final Blackhole bh) {
    bh.consume(run(S6));
  }
}

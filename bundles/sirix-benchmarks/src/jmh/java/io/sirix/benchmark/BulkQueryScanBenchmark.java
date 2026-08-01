/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

/**
 * Warm steady-state measurement of the storage scan paths behind
 * {@code docs/COMPARISON_POSTGRES_BULK.md} §4.1.
 *
 * <p>This exists because the hand-rolled timing loop in {@link PostgresBulkBench} could not measure
 * the warm path honestly, which is exactly the failure {@code docs/BENCHMARK_DESIGN.md} R4 names:
 * <em>"Use JMH for warm steady state; use a bespoke single-run harness for cold. They measure
 * opposite things and neither substitutes."</em> On this corpus the first query pass loads every
 * page — roughly 15 s against ~0.23 s per warm pass — so even forty warm iterations left the cold
 * pass at about two thirds of all profiler samples, and twice led to warm cost being attributed to
 * LZ77 decode that the warm path never executes. Loading the store in {@link Setup} and letting JMH
 * discard warm-up iterations removes that contamination structurally rather than by remembering to
 * subtract it.
 *
 * <p>The store, session and page caches are shared across iterations deliberately — that is the
 * steady state a long-lived server sees, and the counterpart to a PostgreSQL buffer pool that has
 * already faulted the table in. {@link PostgresBulkBench} remains the right tool for the COLD
 * regime, where JMH would eliminate the very effect being measured.
 *
 * <p>Point it at a store built by {@code PostgresBulkBench ingest} in {@code single} mode (the
 * query path needs one document per collection):
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh \
 *       -Pjmh.includes=BulkQueryScanBenchmark \
 *       -Pjmh.jvmArgs="-Dsirix.bench.store=/path/to/store -Dsirix.bench.db=db-name"
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(java.util.concurrent.TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xmx8g", "--add-modules", "jdk.incubator.vector",
    "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED"})
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 3)
public class BulkQueryScanBenchmark {

  /** Matches {@code PostgresBulkBench.QUERIES}, so the two harnesses measure the same shapes. */
  private static final String COUNT_ALL = "count(for $m in $doc[] return $m)";
  private static final String FILTER_COUNT_YEAR = "count(for $m in $doc[] where $m.year > 1990 return $m)";
  private static final String SUM_YEAR = "sum(for $m in $doc[] return $m.year)";
  private static final String TITLE_LOOKUP = "count(for $m in $doc[] where $m.title eq \"Saleslady\" return $m)";

  private static final QNm DOC_VAR = new QNm("doc");

  private BasicJsonDBStore store;
  private SirixQueryContext ctx;
  private SirixCompileChain chain;

  @Setup
  public void setUp() {
    final Path location = Paths.get(System.getProperty("sirix.bench.store",
        System.getProperty("java.io.tmpdir")));
    final String dbName = System.getProperty("sirix.bench.db", "db-1rev");

    store = BasicJsonDBStore.newBuilder().location(location).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
    final JsonDBItem doc = (JsonDBItem) coll.getDocument();
    ctx.bind(DOC_VAR, (Sequence) doc);

    // Fault the whole resource in before any measured iteration. Without this the first measured
    // iteration would carry the ~15 s cold page load and JMH's own warm-up would merely hide it in
    // an averaged result instead of excluding it.
    run(COUNT_ALL);
    run(FILTER_COUNT_YEAR);
    run(SUM_YEAR);
    run(TITLE_LOOKUP);
  }

  @TearDown
  public void tearDown() {
    chain.close();
    ctx.close();
    store.close();
  }

  /**
   * Execute and fully serialize, so a lazy sequence cannot be left unevaluated. Returning the text
   * to a {@link Blackhole} keeps the result observable to JMH's dead-code elimination.
   *
   * @param query the query body, without the external-variable declaration
   * @return the serialized result
   */
  private String run(final String query) {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(sink)) {
      new Query(chain, "declare variable $doc external; " + query).serialize(ctx, ps);
    }
    return sink.toString();
  }

  @Benchmark
  public void countAll(final Blackhole bh) {
    bh.consume(run(COUNT_ALL));
  }

  @Benchmark
  public void filterCountYear(final Blackhole bh) {
    bh.consume(run(FILTER_COUNT_YEAR));
  }

  @Benchmark
  public void sumYear(final Blackhole bh) {
    bh.consume(run(SUM_YEAR));
  }

  @Benchmark
  public void titleLookup(final Blackhole bh) {
    bh.consume(run(TITLE_LOOKUP));
  }
}

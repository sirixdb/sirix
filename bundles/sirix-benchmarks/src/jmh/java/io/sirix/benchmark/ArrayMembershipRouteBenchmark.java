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
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Does answering array membership FROM COLUMNS beat answering it from the records?
 *
 * <p>The question was first settled on three hand-timed runs per arm, on a box whose load average
 * was 8 with no JVM running — the same shape came back at 230 ms and at 1,192 ms under an identical
 * configuration. That is enough noise to invert an A/B verdict, and this exists so the verdict does
 * not rest on it. Both arms run in one JMH invocation with shared warm-up discipline and per-arm
 * error bars.
 *
 * <p>The route reads three columns — the element values (tagged by their enclosing array's path),
 * the field-name column and the record-ordinal column — and never rebuilds a record. Its problem is
 * not the served pages but the declining ones: a decline pays the column read AND the record page it
 * still has to read, so the arm is a win only if it serves a large enough majority.
 *
 * <p>Needs a store ingested with {@code -Dsirix.page.arrayElementStrings=true}; without the element
 * column the columnar arm has nothing to read and both arms measure the same record path.
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh \
 *       -Pjmh.includes=ArrayMembershipRouteBenchmark \
 *       -Pjmh.jvmArgs="-Dsirix.bench.store=/tmp/claude-1000/bench -Dsirix.bench.db=db-elem"
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xmx12g"})
public class ArrayMembershipRouteBenchmark {

  private static final String QUERY =
      "count(for $m in $doc[] where some $g in $m.genres[] satisfies $g eq \"Drama\" return $m)";

  private static final QNm DOC_VAR = new QNm("doc");
  private static final String RESOURCE = "movies";

  /** Which route answers the shape. A parameter, so both arms share one run's conditions. */
  @Param({"false", "true"})
  public boolean columnar;

  private BasicJsonDBStore store;
  private SirixQueryContext ctx;
  private SirixCompileChain chain;
  private JsonResourceSession session;
  private SirixVectorizedExecutor executor;

  @Setup(Level.Trial)
  public void setUp() {
    SirixVectorizedExecutor.ARRAY_CONTAINS_COLUMNAR_ENABLED = columnar;
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
    run();  // fault the resource in; the first pass loads every page
  }

  /** Every measured invocation must be a scan, not a memo hit — see MoviesShapesBenchmark. */
  @Setup(Level.Invocation)
  public void dropResultMemo() {
    executor.clearAggregateResultCachesForBenchmarks();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    SirixVectorizedExecutor.ARRAY_CONTAINS_COLUMNAR_ENABLED = false;
    executor.close();
    session.close();
    chain.close();
    ctx.close();
    store.close();
  }

  private String run() {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(sink)) {
      new Query(chain, "declare variable $doc external; " + QUERY).serialize(ctx, ps);
    }
    return sink.toString();
  }

  @Benchmark
  public void arrayMembership(final Blackhole bh) {
    bh.consume(run());
  }
}

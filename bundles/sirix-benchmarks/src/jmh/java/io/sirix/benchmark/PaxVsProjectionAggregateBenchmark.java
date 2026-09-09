/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.benchmark;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Aggregate functions served by the two vectorized storage tiers, head to head: the always-on PAX
 * page regions (the {@link SirixVectorizedExecutor} storage scan over {@code KeyValueLeafPage}
 * column regions) versus a dedicated, declared projection index (the row-group column-segment store
 * behind {@code tryProjectionAggregate}).
 *
 * <p>
 * Both arms run the SAME Brackit queries through the SAME executor; the only difference is whether
 * a covering (age, active, dept, city) projection is installed. With {@code projectionIndex=false}
 * the registry is empty and every aggregate resolves through the PAX scan; with {@code true} the
 * executor's projection lookup covers the query and the fold kernels serve it from column segments.
 * Trial setup verifies the tier actually taken via
 * {@link SirixVectorizedExecutor#predicatedAggScanCount()}.
 *
 * <p>
 * The executor memoizes aggregate RESULTS per (path, field, predicate); a naive loop would time
 * cache hits after the first invocation. Each invocation therefore clears the result caches
 * (untimed, {@link Level#Invocation}) so every sample is a real kernel scan, while both tiers keep
 * their physical-layer caches (region tables, column slices) warm — the comparison is between
 * resident storage layouts, not IO.
 *
 * <p>
 * Run with:
 * 
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=PaxVsProjectionAggregateBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Benchmark)
public class PaxVsProjectionAggregateBenchmark {

  @Param({"100000", "1000000"})
  public int recordCount;

  /** {@code true} → install the covering projection index; {@code false} → PAX scan serves. */
  @Param({"false", "true"})
  public boolean projectionIndex;

  private static final String JSON_DB = "pax-vs-proj-db";
  private static final String JSON_RESOURCE = "records.jn";
  private static final QNm DOC_VAR = new QNm("doc");

  private Path dbDir;
  private BasicJsonDBStore store;
  private SirixCompileChain chain;
  private SirixQueryContext ctx;
  private JsonResourceSession resourceSession;
  private SirixVectorizedExecutor vecExecutor;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    BenchLogging.silenceRootLogger();

    dbDir = Files.createTempDirectory("sirix-jmh-pax-vs-proj");
    store = BasicJsonDBStore.newBuilder().location(dbDir).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    try (GeneratedRecordsReader src = new GeneratedRecordsReader(recordCount);
        JsonReader jsonReader = new JsonReader(src)) {
      store.create(JSON_DB, JSON_RESOURCE, jsonReader);
    }

    final var coll = store.lookup(JSON_DB);
    resourceSession = coll.getDatabase().beginResourceSession(JSON_RESOURCE);
    if (projectionIndex) {
      final ProjectionIndexBenchSetup.BuildResult built = ProjectionIndexBenchSetup.ensureProjection(resourceSession);
      if (built.totalRows() != recordCount) {
        throw new IllegalStateException("projection rows " + built.totalRows() + " != records " + recordCount);
      }
      System.out.printf("# projection catalogued: %d row groups, %d rows%n", built.rowGroupCount(), built.totalRows());
    }

    final int latestRev = resourceSession.getMostRecentRevisionNumber();
    vecExecutor = new SirixVectorizedExecutor(resourceSession, latestRev);
    SequentialPipelineStrategy.setVectorizedExecutor(vecExecutor);

    final JsonDBCollection jsonColl = (JsonDBCollection) store.lookup(JSON_DB);
    final JsonDBItem docItem = (JsonDBItem) jsonColl.getDocument();
    ctx.bind(DOC_VAR, (Sequence) docItem);

    // Tier verification: a predicated sum must scan through the projection fold kernels
    // exactly when the projection is installed, and never otherwise.
    vecExecutor.clearAggregateResultCachesForBenchmarks();
    final long before = SirixVectorizedExecutor.predicatedAggScanCount();
    final String probeResult = runQueryOnce("sum(for $u in $doc[] where $u.age > 40 return $u.age)");
    final long delta = SirixVectorizedExecutor.predicatedAggScanCount() - before;
    // Cross-backend/one-shot-companion correctness pins — must match the one-shot bench's
    // probes at the same recordCount (the dataset is seeded).
    System.out.printf("# probe sum(age>40) = %s%n", probeResult.trim());
    System.out.printf("# probe count(age>40 and active) = %s%n",
        runQueryOnce("count(for $u in $doc[] where $u.age > 40 and $u.active return $u)").trim());
    System.out.printf("# probe sum(age) = %s%n", runQueryOnce("sum(for $u in $doc[] return $u.age)").trim());
    if (projectionIndex && delta == 0) {
      throw new IllegalStateException("projection installed but predicated aggregate was NOT projection-served");
    }
    if (!projectionIndex && delta != 0) {
      throw new IllegalStateException("no projection installed but predicated aggregate claims projection serving");
    }
    System.out.printf("# tier verified: projectionIndex=%s, projection scans during probe=%d%n", projectionIndex,
        delta);
  }

  /**
   * Every sample must be a real kernel scan: drop the executor's memoized aggregate results outside
   * the timed region. Physical caches stay warm by design.
   */
  @Setup(Level.Invocation)
  public void dropResultCaches() {
    vecExecutor.clearAggregateResultCachesForBenchmarks();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (vecExecutor != null)
      vecExecutor.close();
    if (resourceSession != null)
      resourceSession.close();
    if (chain != null)
      chain.close();
    if (store != null)
      store.close();
    Databases.removeDatabase(dbDir.resolve(JSON_DB));
  }

  private String runQueryOnce(final String body) {
    final String wrapped = "declare variable $doc external; " + body;
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    return buf.toString();
  }

  /**
   * Per-body compiled queries: this bench compares warm kernel-scan tiers, so Brackit parse/compile
   * must not sit inside the timed region (the one-shot companion deliberately keeps it there —
   * first-touch cost is its subject). The first call per body compiles during warmup; measured
   * iterations execute the cached plan.
   */
  private final Map<String, Query> compiledQueries = new HashMap<>();

  private void runQuery(final Blackhole bh, final String body) {
    final Query query =
        compiledQueries.computeIfAbsent(body, b -> new Query(chain, "declare variable $doc external; " + b));
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(query.execute(ctx));
    }
    bh.consume(buf);
  }

  // ==================== unpredicated aggregates ====================

  @Benchmark
  public void sumAge(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void avgAge(final Blackhole bh) {
    runQuery(bh, "avg(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void minMaxAge(final Blackhole bh) {
    runQuery(bh, "{\"min\": min(for $u in $doc[] return $u.age), \"max\": max(for $u in $doc[] return $u.age)}");
  }

  // ==================== predicated aggregates (the fold kernels' home turf) ====================

  @Benchmark
  public void countWhereAgeActive(final Blackhole bh) {
    runQuery(bh, "count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
  }

  @Benchmark
  public void sumWhereAge(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] where $u.age > 40 return $u.age)");
  }

  @Benchmark
  public void sumWhereAgeActive(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] where $u.age > 40 and $u.active return $u.age)");
  }

  @Benchmark
  public void sumWhereAgeBetween(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] where $u.age > 30 and $u.age < 50 return $u.age)");
  }

}

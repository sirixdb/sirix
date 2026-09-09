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
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.index.pageskip.PageSkipRegistry;
import io.sirix.index.projection.ProjectionIndexCatalog;
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

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * The ONE-SHOT companion to {@link PaxVsProjectionAggregateBenchmark}: same queries, same two
 * storage tiers, but every timed invocation is a genuinely first-touch query on a warm JVM.
 * Untimed, per invocation: the executor is closed and recreated (dropping every instance cache —
 * compiled predicates, page-skip schedules, region caches, worker transactions), the resource's
 * buffer pool is evicted ({@link BufferManager#clearAllCaches()}), and the catalog's decoded
 * projection handle is evicted. The timed query then pays predicate compilation, page loads and
 * region access on the PAX arm, and catalog hydration plus column-segment fetch and decode on the
 * projection arm — the cost profile of "a warm server answering this query for the first time".
 * Both arms read their ordinary persisted format; the benchmark has no RAM-resident
 * projection-install route.
 *
 * <p>
 * Run with:
 * 
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=PaxVsProjectionOneShotBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Benchmark)
public class PaxVsProjectionOneShotBenchmark {

  @Param({"100000", "1000000"})
  public int recordCount;

  /**
   * {@code true} → catalogued covering projection (re-cooled per invocation); {@code false} → PAX.
   */
  @Param({"false", "true"})
  public boolean projectionIndex;

  /**
   * Storage backend for the resource. {@code FILE_CHANNEL} is the buffered default; {@code IO_URING}
   * selects the sirix-enterprise O_DIRECT io_uring backend, which bypasses the OS page cache entirely
   * — reads there are storage-cold by construction, no {@link #osCold} needed. Override via
   * {@code -Pjmh.benchmarkParameters="storage=IO_URING"} with {@code -PenterpriseCoreJar=<path>}
   * supplying the provider.
   */
  @Param({"FILE_CHANNEL"})
  public String storage;

  /**
   * {@code true} → additionally drop the OS page cache ({@code sync; echo 3 >
   * /proc/sys/vm/drop_caches}, needs a privileged container) per invocation, so buffered FILE_CHANNEL
   * reads hit real storage instead of kernel memory. Untimed like the rest of {@link #goCold()}.
   * Override via {@code -Pjmh.benchmarkParameters="osCold=true"}.
   */
  @Param({"false"})
  public boolean osCold;

  private static final String JSON_DB = "pax-vs-proj-oneshot-db";
  private static final String JSON_RESOURCE = "records.jn";
  private static final QNm DOC_VAR = new QNm("doc");

  private Path dbDir;
  private BasicJsonDBStore store;
  private SirixCompileChain chain;
  private SirixQueryContext ctx;
  private JsonResourceSession resourceSession;
  private SirixVectorizedExecutor vecExecutor;
  private BufferManager bufferManager;
  private int latestRev;
  private String resourceKey;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    BenchLogging.silenceRootLogger();

    dbDir = Files.createTempDirectory("sirix-jmh-oneshot");
    // The store builder reads the backend from this property at construction; IO_URING
    // fails fast inside store.create when the enterprise provider is absent.
    System.setProperty("storageType", storage);
    store = BasicJsonDBStore.newBuilder().location(dbDir).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    try (Reader src = new GeneratedRecordsReader(recordCount); JsonReader jsonReader = new JsonReader(src)) {
      store.create(JSON_DB, JSON_RESOURCE, jsonReader);
    }

    final var coll = store.lookup(JSON_DB);
    resourceSession = coll.getDatabase().beginResourceSession(JSON_RESOURCE);
    latestRev = resourceSession.getMostRecentRevisionNumber();

    try (JsonNodeReadOnlyTrx rtx = resourceSession.beginNodeReadOnlyTrx(latestRev)) {
      bufferManager = rtx.getStorageEngineReader().getBufferManager();
    }
    resourceKey = resourceSession.getResourceConfig().getResource().toString();

    if (projectionIndex) {
      // Must run BEFORE $doc is bound: the index commit creates a new revision.
      final ProjectionIndexBenchSetup.BuildResult built = ProjectionIndexBenchSetup.ensureProjection(resourceSession);
      if (built.totalRows() != recordCount) {
        throw new IllegalStateException("projection rows " + built.totalRows() + " != records " + recordCount);
      }
      latestRev = resourceSession.getMostRecentRevisionNumber();
      System.out.printf("# catalogued projection: %d row groups, %d rows (revision now %d)%n", built.rowGroupCount(),
          built.totalRows(), latestRev);
    }

    vecExecutor = new SirixVectorizedExecutor(resourceSession, latestRev);
    SequentialPipelineStrategy.setVectorizedExecutor(vecExecutor);

    final JsonDBCollection jsonColl = (JsonDBCollection) store.lookup(JSON_DB);
    final JsonDBItem docItem = (JsonDBItem) jsonColl.getDocument();
    ctx.bind(DOC_VAR, (Sequence) docItem);

    // Tier verification, same contract as the warm-cache bench.
    final long before = SirixVectorizedExecutor.predicatedAggScanCount();
    final String probeResult = runQueryOnce("sum(for $u in $doc[] where $u.age > 40 return $u.age)");
    final long delta = SirixVectorizedExecutor.predicatedAggScanCount() - before;
    // Cross-backend correctness pin: this value must be IDENTICAL for every storage/osCold
    // combination at the same recordCount — the dataset is seeded.
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
    System.out.printf(
        "# tier verified: projectionIndex=%s storage=%s osCold=%s, " + "projection scans during probe=%d%n",
        projectionIndex, storage, osCold, delta);
  }

  /**
   * Re-cool everything a first-touch query would find cold, outside the timed region: fresh executor
   * (all instance caches and worker transactions gone), evicted buffer pool, and — on the projection
   * arm — the catalog's decoded handle.
   */
  @Setup(Level.Invocation)
  public void goCold() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (vecExecutor != null) {
      vecExecutor.close();
    }
    // The executor's worker pool is gone, but its per-thread shared read-only trxs (and the
    // page guards their last reads hold) live in the session until explicitly closed —
    // without this, every invocation leaks open trxs and pinned off-heap frames.
    resourceSession.closeSharedReadOnlyTrxs(latestRev);
    // The page-skip schedule registry is process-global; a published bitmap would hand every
    // "first-touch" invocation a prebuilt scan schedule.
    PageSkipRegistry.uninstall(resourceKey);
    bufferManager.clearAllCaches();
    if (osCold) {
      dropOsPageCache();
    }
    if (projectionIndex) {
      // The next query rebuilds from the metadata blob + directory walk and re-fetches column
      // segments through the sole persisted projection path.
      ProjectionIndexCatalog.clearCache();
    }
    vecExecutor = new SirixVectorizedExecutor(resourceSession, latestRev);
    SequentialPipelineStrategy.setVectorizedExecutor(vecExecutor);
  }

  /** Fails loudly when the container cannot drop caches — a silent no-op would fake cold IO. */
  private static void dropOsPageCache() {
    try {
      final Process p =
          new ProcessBuilder("sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches").redirectErrorStream(true).start();
      if (p.waitFor() != 0) {
        throw new IllegalStateException(
            "drop_caches exited " + p.exitValue() + " — run in a privileged container or with osCold=false");
      }
    } catch (final IOException | InterruptedException e) {
      throw new IllegalStateException("drop_caches failed", e);
    }
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

  private void runQuery(final Blackhole bh, final String body) {
    final String wrapped = "declare variable $doc external; " + body;
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    bh.consume(buf);
  }

  @Benchmark
  public void sumAge(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void countWhereAgeActive(final Blackhole bh) {
    runQuery(bh, "count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
  }

  @Benchmark
  public void sumWhereAgeActive(final Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] where $u.age > 40 and $u.active return $u.age)");
  }
}

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
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Isolates the projection catalog's ROW-GROUP DIRECTORY WALK — the one-per-(resource, revision)
 * cold cost every first projection query pays before any column is read.
 *
 * <p>Why it has its own benchmark: profiling the end-to-end cold query
 * ({@code PaxVsProjectionOneShotBenchmark} with {@code catalogued=true}) showed ~15.7&nbsp;MB
 * allocated per first-touch query at 1M rows, of which only ~0.34&nbsp;MB belongs to the scan
 * kernels — the rest is this walk. The walk is O(row groups × column segments) and runs before
 * the query's columns are even known, so it is the projection tier's scaling limit: at 1M rows
 * it is ~977 row groups, at 10M it would be ~9.8k.
 *
 * <p>The benchmark calls
 * {@link ProjectionIndexHOTStorage#readAllRowGroupDirectoriesFromColumnSegmentSlots} directly, so
 * the number is the walk alone — no catalog caching, no column fetch, no kernels. A fresh
 * read-only transaction per invocation keeps page resolution honest while leaving the buffer pool
 * warm: this measures the walk's CPU and allocation, not device I/O.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=ProjectionDirectoryWalkBenchmark \
 *     -Pjmh.profilers="gc"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
    jvmArgs = { "--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED" })
@State(Scope.Benchmark)
public class ProjectionDirectoryWalkBenchmark {

  @Param({ "100000", "1000000" })
  public int recordCount;

  private static final String JSON_DB = "proj-dirwalk-db";
  private static final String JSON_RESOURCE = "records.jn";

  private Path dbDir;
  private BasicJsonDBStore store;
  private SirixCompileChain chain;
  private SirixQueryContext ctx;
  private JsonResourceSession resourceSession;
  private int revision;
  private int defId;
  private int rowGroupCount;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    BenchLogging.silenceRootLogger();

    dbDir = Files.createTempDirectory("sirix-jmh-dirwalk");
    store = BasicJsonDBStore.newBuilder().location(dbDir).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    try (GeneratedRecordsReader src = new GeneratedRecordsReader(recordCount);
         JsonReader jsonReader = new JsonReader(src)) {
      store.create(JSON_DB, JSON_RESOURCE, jsonReader);
    }

    // Declare the projection through the real user surface so the catalog owns it.
    ProjectionIndexBenchSetup.createProjectionIndexViaQuery(chain, ctx,
        "jn:doc('" + JSON_DB + "','" + JSON_RESOURCE + "')");

    resourceSession = store.lookup(JSON_DB).getDatabase().beginResourceSession(JSON_RESOURCE);
    revision = resourceSession.getMostRecentRevisionNumber();
    defId = ProjectionIndexBenchSetup.firstProjectionDefId(resourceSession, revision);
    rowGroupCount = ProjectionIndexBenchSetup.projectionRowGroupCount(resourceSession, revision, defId);
    if (rowGroupCount <= 0) {
      throw new IllegalStateException("no persisted projection row groups found");
    }
    System.out.printf("# directory walk over %d row groups (defId=%d, %d records)%n",
        rowGroupCount, defId, recordCount);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (resourceSession != null) resourceSession.close();
    if (chain != null) chain.close();
    if (store != null) store.close();
    Databases.removeDatabase(dbDir.resolve(JSON_DB));
  }

  @Benchmark
  public void directoryWalk(final Blackhole bh) {
    try (JsonNodeReadOnlyTrx rtx = resourceSession.beginNodeReadOnlyTrx(revision)) {
      bh.consume(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
          rtx.getStorageEngineReader(), defId, rowGroupCount));
    }
  }
}

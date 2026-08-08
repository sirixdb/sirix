package io.sirix.benchmark;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Commit-latency benchmark answering: is a commit bound by serialisation or by the durability barrier?
 * Sweeps the number of changed slots per durable commit. A SMALL commit serialises few pages, so its
 * latency should sit near the fsync floor (barrier-bound); a LARGE commit serialises many pages, so its
 * latency should rise (serialisation-bound). JMH supplies the rigour the hand-rolled harness lacked:
 * warmup iterations + forks remove the cold-JIT bias, and error bars come for free.
 *
 * <p>Latency (single resource):
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*CommitLatencyBenchmark.commitDurable.*'</pre>
 * Across-resource throughput scaling (each thread owns a distinct resource, Scope.Thread):
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*CommitLatencyBenchmark.commitThroughput.*' -Pjmh.threads=8</pre>
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2,
    jvmArgs = {"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--enable-preview",
        "-Ddisable.single.threaded.check", "-Xms2g", "-Xmx6g"})
public class CommitLatencyBenchmark {

  private static final String RESOURCE = "doc";

  @Param({"1", "8", "64", "512"})
  public int touches;

  @Param({"1000000"})
  public int docBytes;

  private Path dbPath;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private long[] numberKeys;
  private int rngState = 0x9E3779B9;
  private int counter;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    // Logging is silenced race-free via src/jmh/resources/logback.xml (io.sirix -> ERROR), so the
    // per-commit DIAG chatter cannot inflate timed commits and concurrent @Setup never hits the slf4j
    // SubstituteLogger cast race.
    dbPath = Files.createTempDirectory("sirix-jmh-commit-latency");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT).storageType(StorageType.FILE_CHANNEL)
        .hashKind(HashType.NONE).buildPathSummary(false).useDeweyIDs(false).build());
    session = database.beginResourceSession(RESOURCE);
    final String doc = buildDoc(docBytes);
    long maxKey;
    try (JsonNodeTrx w = session.beginNodeTrx()) {
      w.insertSubtreeAsFirstChild(JsonShredder.createStringReader(doc));
      w.commit();
      maxKey = w.getMaxNodeKey();
    }
    final List<Long> nks = new ArrayList<>();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      for (long k = 1; k <= maxKey; k++) {
        if (rtx.moveTo(k) && rtx.isNumberValue()) {
          nks.add(k);
        }
      }
    }
    numberKeys = nks.stream().mapToLong(Long::longValue).toArray();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (session != null) {
      session.close();
    }
    if (database != null) {
      database.close();
    }
    if (dbPath != null) {
      Databases.removeDatabase(dbPath);
    }
  }

  private void oneCommit() {
    try (JsonNodeTrx w = session.beginNodeTrx()) {
      for (int i = 0; i < touches; i++) {
        rngState = rngState * 1664525 + 1013904223;
        final long key = numberKeys[Math.floorMod(rngState, numberKeys.length)];
        if (w.moveTo(key) && w.isNumberValue()) {
          w.setNumberValue(counter++);
        }
      }
      w.commit();
    }
  }

  /** Per-commit latency (ms/op): how a single durable commit's cost moves with the change size. */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void commitDurable() {
    oneCommit();
  }

  /** Aggregate commit throughput; run with -Pjmh.threads=N so each thread (= a distinct resource) writes. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void commitThroughput() {
    oneCommit();
  }

  private static String buildDoc(final int targetBytes) {
    final StringBuilder sb = new StringBuilder(targetBytes + 256);
    sb.append("{\"rows\":[");
    int k = 0;
    while (sb.length() < targetBytes - 16) {
      if (k > 0) {
        sb.append(',');
      }
      k++;
      sb.append(String.format(Locale.ROOT,
          "{\"id\":%d,\"name\":\"item-%06d\",\"qty\":%d,\"price\":%d.25,\"active\":%s}",
          k, k, k * 3, k, k % 2 == 0));
    }
    sb.append("]}");
    return sb.toString();
  }
}

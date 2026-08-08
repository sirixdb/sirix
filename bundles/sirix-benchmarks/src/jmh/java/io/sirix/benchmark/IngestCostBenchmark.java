package io.sirix.benchmark;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
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
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Per-feature ingest cost under JMH: the write-time price of node hashing, the path summary and DeweyIDs.
 * Each invocation shreds a fresh document into a fresh resource configured by {@code config}; the resource
 * is created before and removed after the timed ingest (Level.Invocation), so the database does not
 * accumulate. JMH's per-configuration forks remove the cross-config JIT-ordering bias that a single-JVM
 * sequential loop suffers (and that the hand-rolled version had to defeat by hand with interleaving).
 *
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*IngestCostBenchmark.*'</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 6, time = 2)
@Fork(value = 2,
    jvmArgs = {"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--enable-preview",
        "-Ddisable.single.threaded.check", "-Xms2g", "-Xmx8g"})
@State(Scope.Thread)
public class IngestCostBenchmark {

  @Param({"baseline", "path-summary", "rolling-hash", "dewey", "full"})
  public String config;

  @Param({"4000000"})
  public int docBytes;

  private Path dbPath;
  private Database<JsonResourceSession> database;
  private String doc;
  private int counter;
  private String resource;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    dbPath = Files.createTempDirectory("sirix-jmh-ingest");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    doc = buildDoc(docBytes);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    if (database != null) {
      database.close();
    }
    if (dbPath != null) {
      Databases.removeDatabase(dbPath);
    }
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    resource = "r" + (counter++);
    final ResourceConfiguration.Builder b = ResourceConfiguration.newBuilder(resource)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT).storageType(StorageType.FILE_CHANNEL);
    switch (config) {
      case "baseline" -> b.hashKind(HashType.NONE).useDeweyIDs(false).buildPathSummary(false).storeDiffs(false);
      case "path-summary" -> b.hashKind(HashType.NONE).useDeweyIDs(false).buildPathSummary(true).storeDiffs(false);
      case "rolling-hash" -> b.hashKind(HashType.ROLLING).useDeweyIDs(false).buildPathSummary(false).storeDiffs(false);
      case "dewey" -> b.hashKind(HashType.NONE).useDeweyIDs(true).buildPathSummary(false).storeDiffs(false);
      case "full" -> b.hashKind(HashType.ROLLING).useDeweyIDs(true).buildPathSummary(true).storeDiffs(true);
      default -> throw new IllegalArgumentException("unknown config " + config);
    }
    database.createResource(b.build());
  }

  @TearDown(Level.Invocation)
  public void tearDownInvocation() {
    database.removeResource(resource);
  }

  @Benchmark
  public void ingest() {
    try (JsonResourceSession session = database.beginResourceSession(resource);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(doc));
      wtx.commit();
    }
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

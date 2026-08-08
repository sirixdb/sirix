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
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Lock-free read scaling under JMH. One shared, fully-cached resource; each benchmark thread holds its
 * own read-only transaction (lock-free: a reader takes no write permit, only an epoch slot) and does
 * random point reads. Run with -Pjmh.threads=N to read aggregate throughput at N concurrent readers; if
 * reads are genuinely lock-free the throughput should scale with cores until memory bandwidth caps it.
 *
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*ReadScaleBenchmark.*' -Pjmh.threads=8</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 6, time = 2)
@Fork(value = 2,
    jvmArgs = {"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--enable-preview",
        "-Ddisable.single.threaded.check", "-Xms2g", "-Xmx6g"})
@State(Scope.Thread)
public class ReadScaleBenchmark {

  private static final String RESOURCE = "doc";
  private static final Object LOCK = new Object();
  private static volatile Database<JsonResourceSession> DB;
  private static volatile JsonResourceSession SESSION;
  private static volatile long MAXKEY;

  @Param({"1000000"})
  public int docBytes;

  private JsonNodeReadOnlyTrx rtx;
  private long x;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    synchronized (LOCK) {
      if (DB == null) {
        final Path path = Files.createTempDirectory("sirix-jmh-readscale");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRec(path)));
        Databases.createJsonDatabase(new DatabaseConfiguration(path));
        final Database<JsonResourceSession> db = Databases.openJsonDatabase(path);
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
            .versioningApproach(VersioningType.SLIDING_SNAPSHOT).storageType(StorageType.FILE_CHANNEL)
            .hashKind(HashType.NONE).buildPathSummary(false).useDeweyIDs(false).build());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        try (JsonNodeTrx w = session.beginNodeTrx()) {
          w.insertSubtreeAsFirstChild(JsonShredder.createStringReader(buildDoc(docBytes)));
          w.commit();
          MAXKEY = w.getMaxNodeKey();
        }
        SESSION = session;
        DB = db;
      }
    }
    rtx = SESSION.beginNodeReadOnlyTrx();
    // warm the per-thread cursor's cache view
    for (long k = 1; k <= Math.min(MAXKEY, 2048); k++) {
      rtx.moveTo(k);
    }
    x = (Thread.currentThread().threadId() + 1) * 2654435761L + 1;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (rtx != null) {
      rtx.close();
    }
  }

  @Benchmark
  public long pointRead() {
    x = x * 6364136223846793005L + 1442695040888963407L;
    final long key = 1 + Math.floorMod(x, MAXKEY);
    if (rtx.moveTo(key)) {
      return rtx.getKind().ordinal();
    }
    return 0;
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

  private static void deleteRec(final Path root) {
    try {
      if (!Files.exists(root)) {
        return;
      }
      try (var st = Files.walk(root)) {
        st.sorted(Comparator.reverseOrder()).forEach(p -> {
          try {
            Files.delete(p);
          } catch (Exception ignored) {
          }
        });
      }
    } catch (Exception ignored) {
    }
  }
}

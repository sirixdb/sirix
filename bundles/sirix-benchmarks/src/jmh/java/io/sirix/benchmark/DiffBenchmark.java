package io.sirix.benchmark;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.DiffDepth;
import io.sirix.diff.DiffFactory;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.diff.DiffObserver;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hash-guided diff cost under JMH. A document is ingested (ROLLING hash, storeDiffs off so the diff is
 * recomputed, not read from the stored delta), a revision changes {@code touches} scattered values, and
 * the per-call latency of recomputing the edit script is measured as the document size sweeps. Validates
 * (and, in the report, refines) the claim that a hash-guided diff costs the changed region not the
 * document: the prune is vertical (skip unchanged subtrees) but not horizontal (siblings are enumerated).
 *
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*DiffBenchmark.*'</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 6, time = 2)
@Fork(value = 2,
    jvmArgs = {"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--enable-preview",
        "-Ddisable.single.threaded.check", "-Xms2g", "-Xmx8g"})
@State(Scope.Benchmark)
public class DiffBenchmark {

  static final class Counter implements DiffObserver {
    long total;
    final CountDownLatch done = new CountDownLatch(1);

    public void diffListener(DiffType t, long nk, long ok, DiffDepth d) {
      total++;
    }

    public void diffDone() {
      done.countDown();
    }
  }

  private static final String RESOURCE = "doc";

  @Param({"100000", "1000000", "10000000"})
  public int docBytes;

  @Param({"10"})
  public int touches;

  private Path dbPath;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private int oldRev;
  private int newRev;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    dbPath = Files.createTempDirectory("sirix-jmh-diff");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT).storageType(StorageType.FILE_CHANNEL)
        .hashKind(HashType.ROLLING).storeDiffs(false).build());
    session = database.beginResourceSession(RESOURCE);
    long maxKey;
    try (JsonNodeTrx w = session.beginNodeTrx()) {
      w.insertSubtreeAsFirstChild(JsonShredder.createStringReader(buildDoc(docBytes)));
      w.commit();
      maxKey = w.getMaxNodeKey();
    }
    oldRev = session.getMostRecentRevisionNumber();
    final List<Long> nks = new ArrayList<>();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      for (long k = 1; k <= maxKey; k++) {
        if (rtx.moveTo(k) && rtx.isNumberValue()) {
          nks.add(k);
        }
      }
    }
    final long[] numberKeys = nks.stream().mapToLong(Long::longValue).toArray();
    long x = 12345678901L;
    try (JsonNodeTrx w = session.beginNodeTrx()) {
      for (int i = 0; i < touches; i++) {
        x ^= x << 13;
        x ^= x >>> 7;
        x ^= x << 17;
        final long key = numberKeys[(int) Math.floorMod(x, numberKeys.length)];
        if (w.moveTo(key) && w.isNumberValue()) {
          w.setNumberValue(900000 + i);
        }
      }
      w.commit();
    }
    newRev = session.getMostRecentRevisionNumber();
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

  @Benchmark
  public long recomputeDiff() throws InterruptedException {
    final Counter c = new Counter();
    final Set<DiffObserver> obs = new HashSet<>();
    obs.add(c);
    DiffFactory.invokeJsonDiff(new DiffFactory.Builder<>(session, newRev, oldRev, DiffOptimized.HASHED, obs));
    c.done.await();
    return c.total;
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

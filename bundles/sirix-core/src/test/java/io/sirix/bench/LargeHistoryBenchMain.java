package io.sirix.bench;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Large-history benchmark: builds a resource with N tiny commits (a single number-value update
 * per commit, explicit {@code wtx.commit()} — no auto-commit batching) and then measures the
 * operations whose latency could plausibly scale with the length of the revision history:
 *
 * <ul>
 *   <li>database + resource-session open</li>
 *   <li>{@code getHistory()} (full list) and {@code getHistory(100)} (most-recent page)</li>
 *   <li>{@code beginNodeReadOnlyTrx(rev)} + a 3-step read for rev 1 / N/2 / N</li>
 *   <li>{@code BasicJsonDiff.generateDiff} for (1,2) and (N-1,N)</li>
 *   <li>whole-document serialization at revision 1 and revision N</li>
 * </ul>
 *
 * Every metric is reported twice: <b>cold</b> = first run after {@link Databases#clearGlobalCaches()}
 * (sirix in-process caches dropped; the OS page cache stays warm — documented caveat) and
 * <b>warm</b> = median of {@value #WARM_ITERATIONS} subsequent runs. A JIT warm-up pass runs all
 * operations untimed first so "cold" isolates cache state, not compilation.
 *
 * <p>Usage: {@code LargeHistoryBenchMain [numCommits=10000] [workDir=<tmp>]}
 *
 * <p>Compile/run without gradle (test classpath captured in a file, one path per line is NOT
 * needed — a single colon-joined line works):
 * <pre>
 *   javac --enable-preview --release 25 --add-modules jdk.incubator.vector \
 *     -cp "$(cat /tmp/sirix-test-cp.txt)" -d /tmp/wave4-d/classes \
 *     bundles/sirix-core/src/test/java/io/sirix/bench/LargeHistoryBenchMain.java
 *   java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
 *     --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
 *     -Xms1g -Xmx4g -cp "/tmp/wave4-d/classes:$(cat /tmp/sirix-test-cp.txt)" \
 *     io.sirix.bench.LargeHistoryBenchMain 10000
 * </pre>
 */
public final class LargeHistoryBenchMain {

  private static final int WARM_ITERATIONS = 7;
  private static final int JIT_WARMUP_ITERATIONS = 3;
  private static final String INITIAL_DOC =
      "{\"counter\":0,\"label\":\"large-history-bench\",\"tags\":[\"a\",\"b\",\"c\"]}";

  /** Sink to keep results alive (defeats dead-code elimination). */
  private static volatile long blackhole;

  private record Row(String metric, double coldMs, double[] warmMs, String note) {
    double warmMedian() {
      final double[] sorted = warmMs.clone();
      Arrays.sort(sorted);
      return sorted[sorted.length / 2];
    }
  }

  @FunctionalInterface
  private interface Op {
    long run() throws Exception;
  }

  private LargeHistoryBenchMain() {
  }

  public static void main(final String[] args) throws Exception {
    final int numCommits = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
    final Path dbPath = (args.length > 1
        ? Paths.get(args[1])
        : Files.createTempDirectory("sirix-histbench-")).resolve("db");
    final String resource = "bench";

    System.out.printf(Locale.ROOT, "# LargeHistoryBench: %d commits, dbPath=%s%n", numCommits, dbPath);
    System.out.printf(Locale.ROOT, "# JVM: %s %s, availableProcessors=%d, maxHeap=%dMB%n",
                      System.getProperty("java.vendor"), System.getProperty("java.runtime.version"),
                      Runtime.getRuntime().availableProcessors(),
                      Runtime.getRuntime().maxMemory() >> 20);

    // ---------------------------------------------------------------- build phase
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    final long buildStart = System.nanoTime();
    long counterNodeKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .buildPathSummary(true)
                                                   .storeChildCount(true)
                                                   // isolation knob for commit-rate profiling: the per-record
                                                   // revision-references list grows by one per touching commit
                                                   .storeNodeHistory(Boolean.parseBoolean(
                                                       System.getProperty("bench.storeNodeHistory", "true")))
                                                   .hashKind(HashType.ROLLING)
                                                   // isolation knob: the per-commit diff-file create is the only
                                                   // remaining file creation in a growing directory
                                                   .storeDiffs(Boolean.parseBoolean(
                                                       System.getProperty("bench.storeDiffs", "true")))
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(resource)) {
        var wtx = session.beginNodeTrx();
        try {
          // Revision 1: the initial document (insertSubtreeAsFirstChild commits implicitly).
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(INITIAL_DOC));
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();   // object
          wtx.moveToFirstChild();   // object key "counter"
          wtx.moveToFirstChild();   // number value
          counterNodeKey = wtx.getNodeKey();

          // Revisions 2..N: one tiny field update per explicit commit.
          long windowStart = System.nanoTime();
          for (int i = 2; i <= numCommits; i++) {
            wtx.moveTo(counterNodeKey);
            wtx.setNumberValue(i);
            wtx.commit();
            if (i % 1000 == 0) {
              final long now = System.nanoTime();
              System.out.printf(Locale.ROOT, "#   built %6d commits  (last 1000: %.1fs, %.0f commits/s)%n",
                                i, (now - windowStart) / 1e9, 1000.0 / ((now - windowStart) / 1e9));
              if (Boolean.getBoolean("bench.clearCachesEvery1k")) {
                // isolation knob: splits "buffer-pool occupancy / sweeper pressure" from
                // algorithmic per-commit growth when chasing commit-rate decline
                Databases.clearGlobalCaches();
              }
              if (Boolean.getBoolean("bench.reopenWtxEvery1k")) {
                // isolation knob: a rate that RESETS after reopen pins the decline on
                // state accumulated inside the long-lived write transaction
                wtx.close();
                wtx = session.beginNodeTrx();
              }
              windowStart = now;
            }
          }
        } finally {
          wtx.close();
        }
      }
    }
    final double buildSeconds = (System.nanoTime() - buildStart) / 1e9;
    final long bytesOnDisk = directorySize(dbPath);
    System.out.printf(Locale.ROOT,
                      "# build: %d commits in %.1fs (%.2fms/commit avg), on-disk size=%.1f MB%n",
                      numCommits, buildSeconds, buildSeconds * 1000.0 / numCommits,
                      bytesOnDisk / (1024.0 * 1024.0));

    // ---------------------------------------------------------------- measurement phase
    final List<Row> rows = new ArrayList<>();
    final String dbName = dbPath.getFileName().toString();
    final int midRevision = numCommits / 2;

    // --- metric: open database + resource session ------------------------------------
    {
      // JIT warm-up.
      for (int i = 0; i < JIT_WARMUP_ITERATIONS; i++) {
        blackhole += openAndClose(dbPath, resource);
      }
      Databases.clearGlobalCaches();
      final double cold = timeMs(() -> openAndClose(dbPath, resource));
      final double[] warm = new double[WARM_ITERATIONS];
      for (int i = 0; i < WARM_ITERATIONS; i++) {
        warm[i] = timeMs(() -> openAndClose(dbPath, resource));
      }
      rows.add(new Row("open database+session", cold, warm, "incl. close"));
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = database.beginResourceSession(resource)) {

      final int latest = session.getMostRecentRevisionNumber();
      if (latest != numCommits) {
        throw new IllegalStateException("expected " + numCommits + " revisions, got " + latest);
      }

      measure(rows, "getHistory() full [" + latest + "]", null,
              () -> session.getHistory().size());
      measure(rows, "getHistory(100) page", null,
              () -> session.getHistory(100).size());
      measure(rows, "beginNodeReadOnlyTrx(1)+read", null,
              () -> openTrxAndRead(session, 1));
      measure(rows, "beginNodeReadOnlyTrx(" + midRevision + ")+read", null,
              () -> openTrxAndRead(session, midRevision));
      measure(rows, "beginNodeReadOnlyTrx(" + latest + ")+read", "latest",
              () -> openTrxAndRead(session, latest));
      measure(rows, "diff(1,2)", null,
              () -> new BasicJsonDiff(dbName).generateDiff(session, 1, 2).length());
      measure(rows, "diff(" + (latest - 1) + "," + latest + ")", null,
              () -> new BasicJsonDiff(dbName).generateDiff(session, latest - 1, latest).length());
      measure(rows, "serialize revision 1", null,
              () -> serializeRevision(session, 1));
      measure(rows, "serialize revision " + latest, "latest",
              () -> serializeRevision(session, latest));
    }

    // ---------------------------------------------------------------- report
    System.out.println();
    System.out.printf(Locale.ROOT, "%-38s %12s %12s   %s%n", "metric", "cold_ms", "warm_med_ms", "warm samples (ms)");
    for (final Row row : rows) {
      final StringBuilder samples = new StringBuilder();
      for (final double w : row.warmMs) {
        if (!samples.isEmpty()) {
          samples.append(' ');
        }
        samples.append(String.format(Locale.ROOT, "%.2f", w));
      }
      System.out.printf(Locale.ROOT, "%-38s %12.2f %12.2f   [%s]%s%n",
                        row.metric, row.coldMs, row.warmMedian(), samples,
                        row.note == null ? "" : "  (" + row.note + ")");
    }
    System.out.printf(Locale.ROOT, "BUILD,commits=%d,total_s=%.1f,ms_per_commit=%.2f,disk_mb=%.1f%n",
                      numCommits, buildSeconds, buildSeconds * 1000.0 / numCommits,
                      bytesOnDisk / (1024.0 * 1024.0));
    for (final Row row : rows) {
      System.out.printf(Locale.ROOT, "CSV,%s,cold_ms=%.3f,warm_med_ms=%.3f%n",
                        row.metric.replace(',', ';'), row.coldMs, row.warmMedian());
    }

    // Scaling flags: pairs that SHOULD be ~constant w.r.t. history position.
    flagRatio(rows, "beginNodeReadOnlyTrx(1)+read", "beginNodeReadOnlyTrx(" + numCommits + ")+read");
    flagRatio(rows, "diff(1,2)", "diff(" + (numCommits - 1) + "," + numCommits + ")");
    flagRatio(rows, "serialize revision 1", "serialize revision " + numCommits);
    System.out.println("# blackhole=" + blackhole);
  }

  /** clearGlobalCaches() → 1 cold run; then WARM_ITERATIONS runs, all recorded. */
  private static void measure(final List<Row> rows, final String name, final String note, final Op op)
      throws Exception {
    for (int i = 0; i < JIT_WARMUP_ITERATIONS; i++) {
      blackhole += op.run();
    }
    Databases.clearGlobalCaches();
    final double cold = timeMs(op);
    final double[] warm = new double[WARM_ITERATIONS];
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      warm[i] = timeMs(op);
    }
    rows.add(new Row(name, cold, warm, note));
  }

  private static double timeMs(final Op op) throws Exception {
    final long t0 = System.nanoTime();
    blackhole += op.run();
    return (System.nanoTime() - t0) / 1e6;
  }

  private static long openAndClose(final Path dbPath, final String resource) {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = database.beginResourceSession(resource)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  /** Open a read-only trx at the given revision and do a small real read (3 moves + value). */
  private static long openTrxAndRead(final JsonResourceSession session, final int revision) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();   // object
      final long descendants = rtx.getDescendantCount();
      rtx.moveToFirstChild();   // object key "counter"
      rtx.moveToFirstChild();   // number value
      return rtx.getNumberValue().longValue() + descendants;
    }
  }

  private static long serializeRevision(final JsonResourceSession session, final int revision) {
    final StringWriter out = new StringWriter(256);
    JsonSerializer.newBuilder(session, out, revision).build().call();
    return out.getBuffer().length();
  }

  private static void flagRatio(final List<Row> rows, final String earlyMetric, final String lateMetric) {
    final Row early = rows.stream().filter(r -> r.metric.equals(earlyMetric)).findFirst().orElse(null);
    final Row late = rows.stream().filter(r -> r.metric.equals(lateMetric)).findFirst().orElse(null);
    if (early == null || late == null) {
      return;
    }
    final double a = Math.max(early.warmMedian(), 0.0001);
    final double b = Math.max(late.warmMedian(), 0.0001);
    final double ratio = Math.max(a, b) / Math.min(a, b);
    if (ratio > 5.0) {
      System.out.printf(Locale.ROOT,
                        "SCALING FLAG: warm '%s'=%.2fms vs '%s'=%.2fms (x%.1f apart — expected ~constant)%n",
                        earlyMetric, early.warmMedian(), lateMetric, late.warmMedian(), ratio);
    }
  }

  private static long directorySize(final Path root) throws IOException {
    try (final var stream = Files.walk(root)) {
      return stream.filter(Files::isRegularFile).mapToLong(p -> {
        try {
          return Files.size(p);
        } catch (final IOException e) {
          return 0L;
        }
      }).sum();
    }
  }
}

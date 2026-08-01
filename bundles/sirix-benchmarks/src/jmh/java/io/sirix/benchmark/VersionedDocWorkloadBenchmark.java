package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.temporal.AllTimeAxis;
import io.sirix.io.StorageType;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.serialize.JsonSerializer;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * The versioned-document workloads of {@code docs/COMPARISON_POSTGRES.md} (W1-W4, W6) as proper
 * JMH benchmarks.
 *
 * <p><b>Why this exists.</b> {@link PostgresComparisonBench} times these workloads with a
 * hand-rolled loop, and got warm-up wrong: it ran ONE untimed pass and reported the median of three
 * timed ones, which were still falling steeply (96 → 80 → 73 µs/read). Every W2 figure it produced
 * was ~30 % overstated — see §0.9 of the comparison document. Warm-up, forking, dead-code
 * elimination and honest error bars are exactly what JMH does; this class is the fix, and the other
 * driver is kept only for W5 (a storage measurement, not a latency one) and for the
 * PostgreSQL-facing cross-checks.
 *
 * <p>W5 is deliberately absent: bytes on disk is not a per-operation latency and JMH has nothing to
 * add to it. Run {@code :sirix-benchmarks:postgresComparison} for that number.
 *
 * <p>The 5,001-revision history is built ONCE per trial in {@link #setup()} (~15 s), so a fork
 * amortizes it over every iteration. Keep the fork count low for that reason.
 *
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*VersionedDocWorkloadBenchmark.*'
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED",
        "-Xms1g", "-Xmx4g"})
public class VersionedDocWorkloadBenchmark {

  private static final String RESOURCE = "versioned-doc";

  /** Revisions of history the read workloads face — the comparison document's 5,001. */
  private static final int COMMITS = 5_000;

  @State(Scope.Thread)
  public static class HistoryState {

    /** {@code lean} disables path summary, child counts, hashes, node history and stored diffs. */
    @Param({"lean", "full"})
    public String config;

    private Path databasePath;
    Database<JsonResourceSession> database;
    JsonResourceSession session;

    /** Node key of the {@code counter} value — the field every commit updates. */
    long counterNodeKey;

    /** Deterministic revision sequence for the point-in-time reads. */
    final Random random = new Random(42);

    /** Monotonic counter for W1's commits, so each one genuinely changes the document. */
    private int commitCounter = COMMITS;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      ((Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(
          ch.qos.logback.classic.Level.WARN);
      databasePath = Files.createTempDirectory("sirix-w16-jmh");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);

      final ResourceConfiguration.Builder builder =
          ResourceConfiguration.newBuilder(RESOURCE)
                               .storageType(StorageType.FILE_CHANNEL)
                               .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                               .maxNumberOfRevisionsToRestore(3);
      if ("lean".equals(config)) {
        builder.hashKind(HashType.NONE)
               .storeDiffs(false)
               .storeNodeHistory(false)
               .buildPathSummary(false)
               .storeChildCount(false);
      }
      database.createResource(builder.build());
      session = database.beginResourceSession(RESOURCE);

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(PostgresComparisonBench.document()));
        wtx.commit();
      }
      counterNodeKey = locateCounterValueKey();
      for (int i = 1; i <= COMMITS; i++) {
        commitOnce(i);
      }
    }

    private long locateCounterValueKey() {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        if (!"counter".equals(rtx.getName().getLocalName())) {
          throw new IllegalStateException("expected 'counter' as the document's first member");
        }
        rtx.moveToFirstChild();
        return rtx.getNodeKey();
      }
    }

    long commitOnce(final int value) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveTo(counterNodeKey);
        wtx.setNumberValue(value);
        wtx.commit();
        return wtx.getRevisionNumber();
      }
    }

    /** W1's per-invocation unit: one durable single-field commit. */
    long commitNext() {
      return commitOnce(++commitCounter);
    }

    /** A revision drawn from the history that existed when the trial started. */
    int randomRevision() {
      return 1 + random.nextInt(COMMITS + 1);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      if (session != null) {
        session.close();
      }
      if (database != null) {
        database.close();
      }
      if (databasePath != null && Files.exists(databasePath)) {
        try (final Stream<Path> paths = Files.walk(databasePath)) {
          paths.sorted(Comparator.reverseOrder()).forEach(path -> {
            try {
              Files.deleteIfExists(path);
            } catch (final IOException ignored) {
              // best-effort temp cleanup
            }
          });
        }
      }
    }
  }

  /**
   * W1 — one durable single-field commit.
   *
   * <p>Note this GROWS the history as it runs, unlike the other probes: a commit that did not
   * commit would not be W1. Over a full run that adds tens of thousands of revisions, which is
   * why the read probes draw revisions from the range that existed at trial start.
   */
  @Benchmark
  public long w1DurableCommit(final HistoryState state) {
    return state.commitNext();
  }

  /** W2 — one random point-in-time read, fetching AND serializing the whole document. */
  @Benchmark
  public String w2RandomPointInTimeRead(final HistoryState state) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx(state.randomRevision())) {
      final StringWriter writer = new StringWriter();
      new JsonSerializer.Builder(rtx, writer).build().call();
      return writer.toString();
    }
  }

  /** W2 with the revision held FIXED — isolates per-read work from the random-revision penalty. */
  @Benchmark
  public String w2FixedRevisionRead(final HistoryState state) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx(COMMITS / 2)) {
      final StringWriter writer = new StringWriter();
      new JsonSerializer.Builder(rtx, writer).build().call();
      return writer.toString();
    }
  }

  /** W3 — list every version timestamp. */
  @Benchmark
  public int w3HistoryListing(final HistoryState state) {
    return state.session.getHistory().size();
  }

  /** W4 — one field's value across every version, via the native temporal axis. */
  @Benchmark
  public long w4FieldHistoryAxis(final HistoryState state) {
    long sum = 0;
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx()) {
      rtx.moveTo(state.counterNodeKey);
      final var axis = new AllTimeAxis<>(state.session, rtx);
      while (axis.hasNext()) {
        final JsonNodeReadOnlyTrx trx = axis.next();
        if (trx.moveTo(state.counterNodeKey)) {
          sum += (long) trx.getNumberValue().doubleValue();
        }
      }
    }
    return sum;
  }

  /** W6 — node-level semantic diff of two adjacent versions. */
  @Benchmark
  public String w6AdjacentDiff(final HistoryState state) {
    return new BasicJsonDiff(state.database.getName()).generateDiff(state.session, COMMITS / 2, COMMITS / 2 + 1);
  }
}

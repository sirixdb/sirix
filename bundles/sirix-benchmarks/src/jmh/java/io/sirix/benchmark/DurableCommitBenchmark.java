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
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Latency of the two operations the durable-commit and read-latency work targets, on the
 * versioned-document shape {@code docs/COMPARISON_POSTGRES.md} measures: a small JSON document
 * whose history grows by one single-field commit at a time.
 *
 * <p>Reported in microseconds per operation ({@link Mode#AverageTime}) because that is the number
 * under discussion — a commit's device round-trips and a read's per-transaction setup — and because
 * throughput hides the fixed per-operation costs both changes attack.
 *
 * <p>The probes are deliberately layered, so a regression lands on a specific layer. Each pair
 * differs from the one above it by exactly one thing, which is what lets a difference be
 * attributed rather than merely observed:
 * <ul>
 *   <li>{@link #durableCommit} — one durable single-field commit, the W1 unit;</li>
 *   <li>{@link #openTransactionAndClose} — open a read transaction and close it, no traversal: the
 *       per-transaction setup alone (revision-root load, channel borrow, epoch registration);</li>
 *   <li>{@link #openTransactionAndPointRead} — the same, plus ONE node read;</li>
 *   <li>{@link #pointReadOnHeldCursor} — that one node through an already-open cursor, i.e. a
 *       single {@code moveTo} with the setup subtracted;</li>
 *   <li>{@link #walkRevisionOnHeldCursor} / {@link #walkRevisionOwningTransaction} — every node of
 *       the revision, with no serializer attached: the traversal on its own, warm and cold;</li>
 *   <li>{@link #serializeThroughBorrowedCursor} / {@link #serializeOwningTransaction} — the same
 *       two cursors with the emitter added back, i.e. full-document serialization with and without
 *       a client-supplied transaction;</li>
 *   <li>{@link #serializeWithLevelLimit} — the separate limited-serializer hot loop.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*DurableCommitBenchmark.*'
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class DurableCommitBenchmark {

  private static final String RESOURCE = "durableCommit";

  /** ~2.4 KB versioned document, matching the COMPARISON_POSTGRES workload spec. */
  private static final String DOCUMENT = buildDocument();

  /** Revisions committed during setup, so reads face a real history rather than a single root. */
  private static final int PRELOADED_REVISIONS = 256;

  private static String buildDocument() {
    final StringBuilder builder = new StringBuilder(2_600);
    builder.append("{\"id\":1,\"counter\":0,\"name\":\"versioned-document\",\"tags\":[");
    for (int i = 0; i < 16; i++) {
      builder.append(i == 0 ? "" : ",").append('"').append("tag-").append(i).append('"');
    }
    builder.append("],\"payload\":{");
    for (int i = 0; i < 24; i++) {
      builder.append(i == 0 ? "" : ",")
             .append("\"field").append(i).append("\":\"")
             .append("value-").append(i).append("-0123456789abcdef")
             .append('"');
    }
    builder.append("}}");
    return builder.toString();
  }

  private static void clampLoggingForBenchmarks() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);
  }

  @State(Scope.Thread)
  public static class VersionedDocumentState {
    private Path databasePath;
    Database<JsonResourceSession> database;
    JsonResourceSession session;

    /** A long-lived cursor for the held-cursor probes; never closed between invocations. */
    JsonNodeReadOnlyTrx heldCursor;

    /** Node key of the {@code counter} value — the single field each commit updates. */
    long counterValueKey;

    /**
     * Node key of the {@code counter} MEMBER — the named record. Fused object records carry their
     * primitive inline, so this is normally the same record {@link #counterValueKey} names, which
     * is what makes the name/value probe pair a controlled comparison: same node, same
     * {@code moveTo}, differing only in which accessor is called.
     */
    long counterMemberKey;

    private int commitCounter;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();
      databasePath = Files.createTempDirectory("sirix-durable-commit-bench");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .storeDiffs(false)
                                                   .byteHandlerPipeline(
                                                       new ByteHandlerPipeline(new FFILz4Compressor()))
                                                   .build());
      session = database.beginResourceSession(RESOURCE);

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT));
        wtx.commit();
      }
      counterValueKey = locateCounterValueKey();
      for (int i = 0; i < PRELOADED_REVISIONS; i++) {
        commitOneFieldUpdate();
      }
      heldCursor = session.beginNodeReadOnlyTrx();
    }

    /** Find the value node of the {@code counter} member once, so probes need no lookup. */
    private long locateCounterValueKey() {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();      // the document's root object
        rtx.moveToFirstChild();      // its first member
        while (!"counter".equals(rtx.getName().getLocalName())) {
          if (!rtx.moveToRightSibling()) {
            throw new IllegalStateException("the benchmark document must carry a 'counter' member");
          }
        }
        counterMemberKey = rtx.getNodeKey();
        rtx.moveToFirstChild();      // the member's value
        return rtx.getNodeKey();
      }
    }

    /** One durable single-field commit — the W1 unit of work. */
    long commitOneFieldUpdate() {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveTo(counterValueKey);
        wtx.setNumberValue(++commitCounter);
        wtx.commit();
        return wtx.getRevisionNumber();
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      if (heldCursor != null && !heldCursor.isClosed()) {
        heldCursor.close();
      }
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
            } catch (final Exception ignored) {
              // Best-effort cleanup of a temp directory; a leftover file must not fail the run.
            }
          });
        }
      }
    }
  }

  @Benchmark
  public long durableCommit(final VersionedDocumentState state) {
    return state.commitOneFieldUpdate();
  }

  @Benchmark
  public void openTransactionAndPointRead(final VersionedDocumentState state, final Blackhole blackhole) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx()) {
      rtx.moveTo(state.counterValueKey);
      blackhole.consume(rtx.getNumberValue());
    }
  }

  @Benchmark
  public void pointReadOnHeldCursor(final VersionedDocumentState state, final Blackhole blackhole) {
    state.heldCursor.moveTo(state.counterValueKey);
    blackhole.consume(state.heldCursor.getNumberValue());
  }

  /**
   * Open a read transaction, read ONE object key's name, close.
   *
   * <p>The counterpart to {@link #openTransactionAndPointRead}, which reads a value instead. The
   * two differ by exactly one thing — whether the revision's NAME dictionary has to be consulted —
   * and that is the only per-transaction state a serializer touches which a bare traversal does
   * not. {@link #walkRevisionOwningTransaction} showed the traversal through a cold cursor costs
   * barely more than the open itself, so if an owning serialization is far more expensive than a
   * borrowed one, name resolution is where the difference has to live. This probe says whether it
   * does.
   */
  @Benchmark
  public void openTransactionAndReadOneName(final VersionedDocumentState state, final Blackhole blackhole) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx()) {
      rtx.moveTo(state.counterMemberKey);
      blackhole.consume(rtx.getName());
    }
  }

  /** The same single name read through the already-open cursor, i.e. with the setup subtracted. */
  @Benchmark
  public void nameReadOnHeldCursor(final VersionedDocumentState state, final Blackhole blackhole) {
    state.heldCursor.moveTo(state.counterMemberKey);
    blackhole.consume(state.heldCursor.getName());
  }

  /**
   * Transaction open and close with NO traversal beyond reaching the document root — the fixed
   * cost {@link #serializeOwningTransaction} pays over {@link #serializeThroughBorrowedCursor}.
   *
   * <p>Exists because the difference between those two probes is much larger than
   * {@link #openTransactionAndPointRead} can account for, and the two candidate explanations —
   * an expensive open, versus a first traversal through a COLD cursor (per-kind singletons still
   * unallocated, the reader's most-recently-read page slots still empty) — call for completely
   * different fixes. Subtracting this probe from that difference isolates the second.
   */
  @Benchmark
  public void openTransactionAndClose(final VersionedDocumentState state, final Blackhole blackhole) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx()) {
      blackhole.consume(rtx.moveToDocumentRoot());
    }
  }

  /**
   * Walk every node of the revision through the long-lived cursor, with no serializer attached —
   * the traversal alone: one {@code moveTo} per node plus the structural-key reads the descendant
   * axis needs to find the next one.
   */
  @Benchmark
  public long walkRevisionOnHeldCursor(final VersionedDocumentState state) {
    return walk(state.heldCursor);
  }

  /**
   * The same walk through a transaction opened and closed inside the measurement.
   *
   * <p>Paired with {@link #walkRevisionOnHeldCursor} and {@link #openTransactionAndClose} this
   * splits the gap between the two serialize probes three ways: what the open costs, what a COLD
   * cursor costs a traversal (per-kind singletons still unallocated, the reader's
   * most-recently-read page slots still empty), and what is left for the serializer's own
   * per-call state. Without the split, a difference of that size has no attributable owner.
   */
  @Benchmark
  public long walkRevisionOwningTransaction(final VersionedDocumentState state) {
    try (final JsonNodeReadOnlyTrx rtx = state.session.beginNodeReadOnlyTrx()) {
      return walk(rtx);
    }
  }

  /** Sum the visited node keys so the traversal cannot be optimized away. */
  private static long walk(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(rtx, IncludeSelf.YES);
    long checksum = 0;
    while (axis.hasNext()) {
      checksum += axis.nextLong();
    }
    return checksum;
  }

  @Benchmark
  public String serializeOwningTransaction(final VersionedDocumentState state) {
    final StringWriter writer = new StringWriter();
    new JsonSerializer.Builder(state.session, writer).build().call();
    return writer.toString();
  }

  @Benchmark
  public String serializeThroughBorrowedCursor(final VersionedDocumentState state) {
    final StringWriter writer = new StringWriter();
    new JsonSerializer.Builder(state.heldCursor, writer).build().call();
    return writer.toString();
  }

  /**
   * The LIMITED path — any {@code maxLevel}/{@code maxNodes}/{@code maxChildren} makes
   * {@link JsonSerializer} hand the work to {@code JsonLimitedSerializer}, a separate emitter with
   * its own hot loop. That is what a paginated REST read goes through, so it needs its own probe:
   * the unlimited probes above cannot see a regression (or an improvement) in it at all.
   *
   * <p>{@code maxLevel(2)} with metadata is the shape the REST layer actually requests — enough
   * levels to walk fused structural records and emit their {@code key}/{@code metadata}/{@code
   * value} envelopes, which is where that serializer does its per-node work.
   */
  @Benchmark
  public String serializeWithLevelLimit(final VersionedDocumentState state) {
    final StringWriter writer = new StringWriter();
    new JsonSerializer.Builder(state.session, writer).maxLevel(2).withMetaData(true).build().call();
    return writer.toString();
  }
}

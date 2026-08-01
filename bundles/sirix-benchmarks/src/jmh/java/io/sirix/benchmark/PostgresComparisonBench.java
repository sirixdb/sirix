package io.sirix.benchmark;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * The SirixDB half of the versioned-document comparison in {@code docs/COMPARISON_POSTGRES.md}
 * (workloads W1-W6). Kept in-tree deliberately: the original driver lived in {@code /tmp} and did
 * not survive the machine it was written on, which is why §0's re-run had to re-implement the spec
 * from prose rather than re-run the same code.
 *
 * <p>Run with {@code ./gradlew :sirix-benchmarks:postgresComparison}. Optional arguments:
 * {@code <commits> <pitReads> <config>} where {@code config} is {@code full} or {@code lean}.
 *
 * <p>The two configurations exist because several SirixDB features have per-commit cost that
 * PostgreSQL's pattern has no equivalent for: {@code full} is the defaults (path summary, child
 * counts, rolling hashes, per-node history, stored diffs), {@code lean} disables all of them.
 * Reporting only one would either overstate SirixDB (lean vs a full-featured PostgreSQL) or hide
 * what the defaults cost.
 */
public final class PostgresComparisonBench {

  private static final String RESOURCE = "versioned-doc";

  private PostgresComparisonBench() {
  }

  public static void main(final String[] args) throws Exception {
    final int commits = args.length > 0 ? Integer.parseInt(args[0]) : 5_000;
    final int pitReads = args.length > 1 ? Integer.parseInt(args[1]) : 1_000;
    final boolean lean = args.length <= 2 || !"full".equalsIgnoreCase(args[2]);
    // 4th arg: versioning strategy. SLIDING_SNAPSHOT (the default) stores each revision as
    // fragments and rebuilds a revision by combining up to maxNumberOfRevisionsToRestore of them;
    // FULL stores every revision complete, trading storage for no reconstruction on read. W2 reads
    // random revisions, so it pays that reconstruction on nearly every read.
    final VersioningType versioning =
        args.length > 3 ? VersioningType.valueOf(args[3].toUpperCase()) : VersioningType.SLIDING_SNAPSHOT;

    final String document = buildDocument();
    System.out.printf("# SirixDB versioned-document benchmark (%s config)%n", lean ? "lean" : "full");
    System.out.printf("# document bytes=%d commits=%d pitReads=%d versioning=%s%n", document.length(), commits, pitReads, versioning);

    final Path databasePath = Files.createTempDirectory("sirix-pgcmp");
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(resourceConfig(lean, versioning));
        try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
          final long counterNodeKey = runW1(session, document, commits);
          runW2(session, pitReads, commits);
          runW3(session, commits);
          runW4(session, counterNodeKey, commits);
          runW6(database, session, commits);
        }
      }
      runW5(databasePath);
    } finally {
      deleteRecursively(databasePath);
    }
  }

  private static ResourceConfiguration resourceConfig(final boolean lean, final VersioningType versioning) {
    final ResourceConfiguration.Builder builder = ResourceConfiguration.newBuilder(RESOURCE)
                                                                      .storageType(StorageType.FILE_CHANNEL)
                                                                      .versioningApproach(versioning)
                                                                      .maxNumberOfRevisionsToRestore(3);
    if (lean) {
      builder.hashKind(HashType.NONE)
             .storeDiffs(false)
             .storeNodeHistory(false)
             .buildPathSummary(false)
             .storeChildCount(false);
    }
    return builder.build();
  }

  /**
   * Deterministic ~2.4 KB document: 50 top-level fields ({@code counter} first, then strings, ints
   * and bools) plus a nested array of 20 item objects. The exact same bytes are fed to PostgreSQL.
   */
  /** The benchmark document, shared with {@link VersionedDocWorkloadBenchmark} so both use identical bytes. */
  static String document() {
    return DOCUMENT_TEXT;
  }

  private static final String DOCUMENT_TEXT = buildDocument();

  private static String buildDocument() {
    final StringBuilder json = new StringBuilder(2_600);
    json.append("{\"counter\":0");
    for (int i = 0; i < 36; i++) {
      json.append(",\"s").append(i).append("\":\"value-").append(i).append("-0123456789abcdef0123456789\"");
    }
    for (int i = 0; i < 8; i++) {
      json.append(",\"i").append(i).append("\":").append(i * 7 + 1);
    }
    for (int i = 0; i < 4; i++) {
      json.append(",\"b").append(i).append("\":").append(i % 2 == 0);
    }
    json.append(",\"items\":[");
    for (int i = 0; i < 20; i++) {
      json.append(i == 0 ? "" : ",")
          .append("{\"id\":").append(i)
          .append(",\"name\":\"item-").append(i).append("\"")
          .append(",\"qty\":").append(i * 3)
          .append("}");
    }
    json.append("]}");
    return json.toString();
  }

  /** W1: insert the document, then {@code commits} single-field updates, each its own durable commit. */
  private static long runW1(final JsonResourceSession session, final String document, final int commits) {
    final long insertStart = System.nanoTime();
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document));
      wtx.commit();
    }
    final long insertNanos = System.nanoTime() - insertStart;

    final long counterNodeKey = locateCounterValueKey(session);

    final long start = System.nanoTime();
    long windowStart = start;
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      for (int i = 1; i <= commits; i++) {
        wtx.moveTo(counterNodeKey);
        wtx.setNumberValue(i);
        wtx.commit();
        if (i % 1_000 == 0) {
          final long now = System.nanoTime();
          System.out.printf("W1 window %d-%d: %.0f commits/s%n", i - 999, i,
                            1_000 / ((now - windowStart) / 1e9));
          windowStart = now;
        }
      }
    }
    final long nanos = System.nanoTime() - start;
    System.out.printf("W1 initial insert: %.1f ms%n", insertNanos / 1e6);
    System.out.printf("W1 %d durable commits: %.2f s = %.0f commits/s (%.2f ms/commit)%n",
                      commits, nanos / 1e9, commits / (nanos / 1e9), nanos / 1e6 / commits);
    return counterNodeKey;
  }

  /** W2: random point-in-time reads, each fetching AND serializing the whole document. */
  private static void runW2(final JsonResourceSession session, final int reads, final int commits) {
    final int maxRevision = commits + 1;
    final List<Long> timings = new ArrayList<>(3);
    // EIGHT warm-up passes, not one. With a single pass the three timed passes still fell
    // 96.1 -> 79.6 -> 73.3 us/read: the median was an artifact of where JIT compilation happened to
    // stop, and it overstated the cost by ~30%. At eight they converge (75.6 -> 61.2 -> 61.0).
    // This is a hand-rolled harness doing a job JMH exists to do — see the note in
    // docs/COMPARISON_POSTGRES.md about porting these workloads.
    final int warmups = Integer.getInteger("pgcmp.w2.warmups", 8);
    for (int pass = 0; pass < warmups + 3; pass++) {
      final Random random = new Random(42);                 // same revision sequence every pass
      final long start = System.nanoTime();
      long bytes = 0;
      for (int i = 0; i < reads; i++) {
        final int revision = 1 + random.nextInt(maxRevision);
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          final StringWriter writer = new StringWriter();
          new JsonSerializer.Builder(rtx, writer).build().call();
          bytes += writer.getBuffer().length();
        }
      }
      final long nanos = System.nanoTime() - start;
      if (pass >= warmups) {
        timings.add(nanos);
      }
      if (pass == 0) {
        System.out.printf("W2 warm-up: %.1f ms (%d bytes serialized)%n", nanos / 1e6, bytes);
      }
    }
    final long median = median(timings);
    System.out.printf("W2 %d random PIT full-doc reads: %.1f ms (%.1f us/read, median of 3)%n",
                      reads, median / 1e6, median / 1e3 / reads);
    // Per-pass timings, so "is one warm-up pass enough?" is answerable from the output instead of
    // assumed. Passes that keep falling mean the JIT is still compiling and the median is an
    // artifact of where warm-up happened to stop.
    final StringBuilder passes = new StringBuilder("W2 timed passes (us/read):");
    for (final long t : timings) {
      passes.append(String.format(" %.1f", t / 1e3 / reads));
    }
    System.out.println(passes);
    // The SAME revision, read repeatedly. W2 proper picks a revision at random from 5,001, so its
    // cost includes reconstructing that revision from sliding-snapshot fragments and missing every
    // cache on the way. Holding the revision fixed removes exactly that and leaves the per-read
    // engine work — the difference between the two IS the price of random time travel.
    final List<Long> fixedTimings = new ArrayList<>(3);
    final int fixedRevision = maxRevision / 2;
    for (int pass = 0; pass < 4; pass++) {
      final long start = System.nanoTime();
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(fixedRevision)) {
          final StringWriter writer = new StringWriter();
          new JsonSerializer.Builder(rtx, writer).build().call();
        }
      }
      final long nanos = System.nanoTime() - start;
      if (pass > 0) {
        fixedTimings.add(nanos);
      }
    }
    final long fixedMedian = median(fixedTimings);
    System.out.printf("W2 FIXED revision %d, %d reads: %.1f us/read (median of 3)%n",
                      fixedRevision, reads, fixedMedian / 1e3 / reads);
    breakDownW2(session, reads, maxRevision);
  }

  /**
   * Split W2's per-read cost into transaction setup, trie traversal, and serialization.
   *
   * <p>Worth measuring rather than assuming: the trie is lock-free and a point read on an open
   * cursor costs tens of NANOseconds, so if a full-document read costs ~100 µs then navigation is
   * not the constraint and tuning it would buy nothing. This prints the three parts so the next
   * optimization targets whichever actually dominates.
   */
  private static void breakDownW2(final JsonResourceSession session, final int reads, final int maxRevision) {
    long openClose = 0;
    long traverse = 0;
    long serialize = 0;
    for (int pass = 0; pass < 2; pass++) {                 // 1 warm-up + 1 measured
      Random random = new Random(42);
      long start = System.nanoTime();
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          // open + close only
        }
      }
      openClose = System.nanoTime() - start;

      random = new Random(42);
      start = System.nanoTime();
      long nodes = 0;
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          nodes += walkSubtree(rtx);
        }
      }
      traverse = System.nanoTime() - start;

      random = new Random(42);
      start = System.nanoTime();
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          final StringWriter writer = new StringWriter();
          new JsonSerializer.Builder(rtx, writer).build().call();
        }
      }
      serialize = System.nanoTime() - start;

      // Traverse AND materialize every value, but emit nothing. This splits the remaining cost in
      // two: if it lands near the serialization figure, the time is going into turning stored
      // nodes back into Java values (decode, String construction); if it stays near the traversal
      // figure, the time is in the serializer's own formatting logic.
      random = new Random(42);
      start = System.nanoTime();
      long chars = 0;
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          final var axis = new io.sirix.axis.DescendantAxis(rtx);
          while (axis.hasNext()) {
            axis.nextLong();
            final String value = rtx.getValue();   // ONE call — calling it twice doubled this figure
            chars += value == null ? 0 : value.length();
          }
        }
      }
      final long materialize = System.nanoTime() - start;

      // Serializer CONSTRUCTION alone, no call(): a fresh Builder+JsonSerializer is built per read
      // (each one allocating its own key cache and traversal state), so if construction is
      // expensive it masquerades as per-node emit cost in the figures above.
      random = new Random(42);
      start = System.nanoTime();
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          final StringWriter writer = new StringWriter();
          final var unused = new JsonSerializer.Builder(rtx, writer).build();
          if (unused == null) {
            throw new IllegalStateException("unreachable");
          }
        }
      }
      final long construct = System.nanoTime() - start;

      // The same document through the BYTE sink. Each sink has its own verbatim fast path
      // (JsonOutputSink.tryEmitQuoted): the char sink accepts plain-ASCII runs, the byte sink any
      // escape-free UTF-8 run, emitted straight from the node's stored bytes. Worth measuring side
      // by side, because a REST/network consumer gets bytes anyway, which is also what
      // PostgreSQL's client receives.
      random = new Random(42);
      start = System.nanoTime();
      for (int i = 0; i < reads; i++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1 + random.nextInt(maxRevision))) {
          final ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
          JsonSerializer.newBuilder(session, bytes, rtx.getRevisionNumber()).build().call();
        }
      }
      final long serializeBytes = System.nanoTime() - start;

      if (pass == 1) {
        System.out.printf("W2 breakdown per read: open+close %.1f us | +full traversal %.1f us "
                          + "(%d nodes/doc) | +serialization %.1f us%n",
                          openClose / 1e3 / reads, traverse / 1e3 / reads, nodes / reads,
                          serialize / 1e3 / reads);
        System.out.printf("W2 serializer construction only (no call): %.1f us%n",
                          (construct - openClose) / 1e3 / reads);
        System.out.printf("W2 value materialization (traverse + getValue, no emit): %.1f us "
                          + "(%d chars/doc)%n", (materialize - openClose) / 1e3 / reads, chars / reads);
        System.out.printf("W2 serialization sinks: char/Appendable %.1f us | byte/OutputStream %.1f us%n",
                          (serialize - openClose) / 1e3 / reads, (serializeBytes - openClose) / 1e3 / reads);
        System.out.printf("W2 breakdown shares: setup %.0f%% | traversal %.0f%% | serialization %.0f%%%n",
                          100.0 * openClose / serialize,
                          100.0 * (traverse - openClose) / serialize,
                          100.0 * (serialize - traverse) / serialize);
      }
    }
  }

  /** Visit every node of the document subtree in document order; returns the node count. */
  private static long walkSubtree(final JsonNodeReadOnlyTrx rtx) {
    long nodes = 0;
    final var axis = new io.sirix.axis.DescendantAxis(rtx);
    while (axis.hasNext()) {
      axis.nextLong();
      nodes++;
    }
    return nodes;
  }

  /** W3: list every version timestamp. */
  private static void runW3(final JsonResourceSession session, final int commits) {
    final List<Long> timings = new ArrayList<>(3);
    for (int pass = 0; pass < 4; pass++) {
      final long start = System.nanoTime();
      // getHistory() also reports the bootstrap revision 0 that createResource commits, which is
      // not a user-visible version of the document — hence size() - 1 against the spec's count.
      final int count = session.getHistory().size() - 1;
      final long nanos = System.nanoTime() - start;
      if (pass > 0) {
        timings.add(nanos);
      }
      if (pass == 3 && count != commits + 1) {
        System.out.printf("W3 WARNING: expected %d versions, got %d%n", commits + 1, count);
      }
    }
    System.out.printf("W3 history listing (%d versions): %.2f ms (median of 3)%n",
                      commits + 1, median(timings) / 1e6);
  }

  /** W4: one field's value across every version — native temporal axis, and a manual loop. */
  private static void runW4(final JsonResourceSession session, final long counterNodeKey, final int commits) {
    final List<Long> axisTimings = new ArrayList<>(3);
    long axisSum = 0;
    for (int pass = 0; pass < 4; pass++) {
      final long start = System.nanoTime();
      long sum = 0;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveTo(counterNodeKey);
        final var axis = new AllTimeAxis<>(session, rtx);
        while (axis.hasNext()) {
          final JsonNodeReadOnlyTrx trx = axis.next();
          if (trx.moveTo(counterNodeKey)) {
            sum += (long) trx.getNumberValue().doubleValue();
          }
        }
      }
      final long nanos = System.nanoTime() - start;
      if (pass > 0) {
        axisTimings.add(nanos);
      }
      axisSum = sum;
    }

    final List<Long> loopTimings = new ArrayList<>(3);
    long loopSum = 0;
    int loopVisited = 0;
    for (int pass = 0; pass < 4; pass++) {
      final long start = System.nanoTime();
      long sum = 0;
      int visited = 0;
      for (int revision = 1; revision <= session.getMostRecentRevisionNumber(); revision++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          if (rtx.moveTo(counterNodeKey)) {
            sum += (long) rtx.getNumberValue().doubleValue();
            visited++;
          }
        }
      }
      final long nanos = System.nanoTime() - start;
      if (pass > 0) {
        loopTimings.add(nanos);
      }
      loopSum = sum;
      loopVisited = visited;
    }
    // The cross-check the comparison doc pins: the counter runs 0..commits across the history, so
    // the sum is commits*(commits+1)/2 and BOTH strategies must produce it.
    final long expectedSum = (long) commits * (commits + 1) / 2;
    System.out.printf("W4 field history: axis %.1f ms, loop %.1f ms (median of 3)%n",
                      median(axisTimings) / 1e6, median(loopTimings) / 1e6);
    System.out.printf("W4 cross-check: expected sum=%d axisSum=%d loopSum=%d loopVisited=%d%s%n",
                      expectedSum, axisSum, loopSum, loopVisited,
                      axisSum == expectedSum && loopSum == expectedSum ? "" : "   <-- MISMATCH");
  }

  /** W5: bytes on disk for the full history (apparent size = sum of file sizes). */
  private static void runW5(final Path databasePath) throws IOException {
    try (final Stream<Path> paths = Files.walk(databasePath)) {
      final long bytes = paths.filter(Files::isRegularFile).mapToLong(path -> {
        try {
          return Files.size(path);
        } catch (final IOException e) {
          return 0L;
        }
      }).sum();
      System.out.printf("W5 storage for full history: %.2f MiB (apparent)%n", bytes / (1024.0 * 1024.0));
    }
    // Preallocation deliberately leaves a zero tail at rest (the zero-fill + fsync is what keeps
    // later in-place writes journal-free), so the apparent size overstates the DATA written. Report
    // the allocated size too: sparse/zero regions the filesystem never materialised do not count.
    try {
      final Process du = new ProcessBuilder("du", "-sk", "--apparent-size", databasePath.toString()).start();
      final Process duAllocated = new ProcessBuilder("du", "-sk", databasePath.toString()).start();
      System.out.printf("W5 du apparent: %s", new String(du.getInputStream().readAllBytes()));
      System.out.printf("W5 du allocated: %s", new String(duAllocated.getInputStream().readAllBytes()));
    } catch (final IOException ignored) {
      // du is a diagnostic nicety; its absence must not fail the benchmark.
    }
    try (final Stream<Path> paths = Files.walk(databasePath)) {
      paths.filter(Files::isRegularFile).sorted(Comparator.comparingLong(path -> {
        try {
          return -Files.size(path);
        } catch (final IOException e) {
          return 0L;
        }
      })).limit(4).forEach(path -> {
        try {
          System.out.printf("W5   %8.2f MiB  %s%n", Files.size(path) / (1024.0 * 1024.0),
                            databasePath.relativize(path));
        } catch (final IOException ignored) {
          // per-file diagnostic only
        }
      });
    }
  }

  /** W6: semantic diff of two adjacent versions in the middle of the history. */
  private static void runW6(final Database<JsonResourceSession> database, final JsonResourceSession session,
      final int commits) {
    final int left = commits / 2;
    final int right = left + 1;
    final List<Long> timings = new ArrayList<>(3);
    String diff = "";
    for (int pass = 0; pass < 4; pass++) {
      final long start = System.nanoTime();
      diff = new BasicJsonDiff(database.getName()).generateDiff(session, left, right);
      final long nanos = System.nanoTime() - start;
      if (pass > 0) {
        timings.add(nanos);
      }
    }
    System.out.printf("W6 diff of versions %d/%d: %.2f ms (median of 3), %d-char patch%n",
                      left, right, median(timings) / 1e6, diff.length());
  }

  /** Node key of the {@code counter} member's value — the field every commit updates. */
  private static long locateCounterValueKey(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToFirstChild();      // the document's root object
      rtx.moveToFirstChild();      // its first member — "counter" by construction
      if (!"counter".equals(rtx.getName().getLocalName())) {
        throw new IllegalStateException("expected 'counter' as the document's first member");
      }
      rtx.moveToFirstChild();      // the member's value
      return rtx.getNodeKey();
    }
  }

  private static long median(final List<Long> values) {
    final List<Long> sorted = new ArrayList<>(values);
    sorted.sort(Comparator.naturalOrder());
    return sorted.get(sorted.size() / 2);
  }

  private static void deleteRecursively(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (final Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }
  }
}

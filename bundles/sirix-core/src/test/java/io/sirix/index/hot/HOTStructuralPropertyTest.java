/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.PageConstants;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A seeded, property-based exercise of every HOT index kind the writer serves, against the complete
 * structural invariant and a plain sorted reference after every single operation.
 *
 * <p>
 * Each case is a deterministic function of its seed: an index kind (CAS, PATH, NAME, VALIDTIME or a
 * projection store with its segment side map), one of the four versioning types, a leaf-consolidation
 * cadence, and a generated stream of puts, posting removals, blob tombstones, commits, reverts to an
 * earlier revision and cold reopens. Key generators are shaped after the inputs that produced the
 * structural defects fixed since September: sparse partial keys that differ on a few scattered bits,
 * long common prefixes with trailing-byte differences, ascending, descending and clustered runs, and
 * postings that grow one chunk to several KiB so that leaves split by bytes rather than by count.
 * </p>
 *
 * <p>
 * After every put or removal the writer's uncommitted trie must pass {@link HOTInvariantValidator}
 * (children ascending and non-overlapping, trie condition, sparse-path encoding, every stored key
 * routing back to its leaf), an in-order walk of its live slots must equal the reference's slot set
 * exactly, and point lookups must answer what the reference answers. After every commit each
 * historical revision is checked the same way through the reader, including the logical iterator;
 * a cold reopen repeats that from disk with the caches cleared. A failing seed is shrunk with
 * delta debugging and reported as a replayable stream; {@link #replay} runs such a stream.
 * </p>
 *
 * <p>
 * The default lane runs a small budget per kind. The {@code heavy} method runs an extended budget
 * over every kind, sized by {@code -Dsirix.hot.property.heavy.seeds} / {@code heavy.ops}; with
 * {@code -Dsirix.hot.property.collect=true} it records every distinct failure instead of stopping at
 * the first. {@code -Dsirix.hot.property.seed=N} pins the default lane to one seed.
 * </p>
 */
final class HOTStructuralPropertyTest {

  private static final String PROPERTY = "sirix.hot.property.";
  private static final String RESOURCE = "hot-structural-property";
  private static final int INDEX_NUMBER = 0;
  private static final int NAME_INDEX_NUMBER = PageConstants.JSON_NAME_INDEX_OFFSET;

  private static final VersioningType[] VERSIONINGS = {VersioningType.FULL, VersioningType.INCREMENTAL,
      VersioningType.DIFFERENTIAL, VersioningType.SLIDING_SNAPSHOT};

  /** Consolidation cadences a seed cycles through; {@code 0} is the production cadence. */
  private static final int[] CONSOLIDATION_INTERVALS = {64, 0, 16, 256};

  private static final int DEFAULT_SEEDS = Integer.getInteger(PROPERTY + "seeds",
      System.getProperty(PROPERTY + "seed") == null
          ? 3
          : 1);
  private static final int DEFAULT_OPS = Integer.getInteger(PROPERTY + "ops", 3_000);
  private static final long BASE_SEED = Long.getLong(PROPERTY + "seed", 1L);
  private static final long SHRINK_SECONDS = Long.getLong(PROPERTY + "shrinkSeconds", 120L);
  private static final int LOOKUP_SAMPLE = 16;
  private static final int EXACT_COMMIT_EVERY = 8;
  private static final int OLDER_REVISIONS_PER_COMMIT = 3;

  /**
   * A put that merged into its leaf in place, or a posting bit removed in place, changes no
   * structure: such a step is checked by the lookups of the key it touched, while every structural
   * handler (and every {@code fullEvery}th step regardless) is followed by the complete check.
   * {@code -Dsirix.hot.property.fullEvery=1} checks completely after every step.
   */
  private static final int FULL_CHECK_EVERY = Integer.getInteger(PROPERTY + "fullEvery", 32);
  private static final String IN_PLACE_MERGE = "merge";
  private static final String IN_PLACE_REMOVE = "h:remove-posting-bit";

  /** Where collect mode writes each shrunk failure. */
  private static final Path FAILURE_DIRECTORY =
      Path.of(System.getProperty(PROPERTY + "failureDir", System.getProperty("java.io.tmpdir")), "hot-property-failures");

  private static final byte POSTING_TOMBSTONE = (byte) 0xFE;
  private static final int BULK_STRIDE = 2;

  enum Kind {
    CAS, PATH, NAME, VALIDTIME, PROJECTION
  }

  @TempDir
  Path temporaryDirectory;

  private int caseCounter;

  @AfterEach
  void reset() {
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(0);
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("CAS index: every generated stream keeps the structural invariant and the reference answers")
  void casIndex() {
    runBudget(Kind.CAS, DEFAULT_SEEDS, DEFAULT_OPS, false);
  }

  @Test
  @DisplayName("PATH index: every generated stream keeps the structural invariant and the reference answers")
  void pathIndex() {
    runBudget(Kind.PATH, DEFAULT_SEEDS, DEFAULT_OPS, false);
  }

  @Test
  @DisplayName("NAME index: every generated stream keeps the structural invariant and the reference answers")
  void nameIndex() {
    runBudget(Kind.NAME, DEFAULT_SEEDS, DEFAULT_OPS, false);
  }

  @Test
  @DisplayName("VALIDTIME index: every generated stream keeps the structural invariant and the reference answers")
  void validTimeIndex() {
    runBudget(Kind.VALIDTIME, DEFAULT_SEEDS, DEFAULT_OPS, false);
  }

  @Test
  @DisplayName("projection store: every generated stream keeps the structural invariant and the reference answers")
  void projectionIndex() {
    runBudget(Kind.PROJECTION, DEFAULT_SEEDS, DEFAULT_OPS, false);
  }

  @Test
  @Tag("heavy")
  @DisplayName("extended budget over every index kind")
  void extendedBudgetAcrossEveryKind() {
    final int seeds = Integer.getInteger(PROPERTY + "heavy.seeds", 16);
    final int ops = Integer.getInteger(PROPERTY + "heavy.ops", 20_000);
    final boolean collect = Boolean.getBoolean(PROPERTY + "collect");
    final String kindFilter = System.getProperty(PROPERTY + "kinds");
    final List<String> failures = new ArrayList<>();
    for (final Kind kind : Kind.values()) {
      if (kindFilter != null && !kindFilter.toUpperCase().contains(kind.name())) {
        continue;
      }
      failures.addAll(runBudget(kind, seeds, ops, collect));
    }
    if (!failures.isEmpty()) {
      throw new AssertionError(failures.size() + " distinct failure(s):\n" + String.join("\n", failures));
    }
  }

  // ===== budget driver =====

  /**
   * Run {@code seeds} consecutive seeds of {@code kind}. Without {@code collect} the first failure is
   * shrunk and thrown; with it every failure is shrunk, written next to the test's temporary
   * directory and summarised in the returned list.
   */
  private List<String> runBudget(final Kind kind, final int seeds, final int ops, final boolean collect) {
    final List<String> failures = new ArrayList<>();
    final Map<String, Integer> handlers = new TreeMap<>();
    final long started = System.nanoTime();
    int maxHeight = 0;
    long storedKeys = 0;
    for (int i = 0; i < seeds; i++) {
      final long seed = BASE_SEED + i;
      final CaseConfig config = CaseConfig.forSeed(kind, seed);
      final List<Op> stream = new StreamGenerator(kind, seed, ops).generate();
      final CaseResult result = runCase(config, stream);
      result.handlers.forEach((handler, count) -> handlers.merge(handler, count, Integer::sum));
      maxHeight = Math.max(maxHeight, result.height);
      storedKeys += result.storedKeys;
      System.out.println("[hot-property] " + config.header() + ": " + stream.size() + " ops, " + result.applied
          + " applied, final slots " + result.storedKeys + ", height " + result.height + (result.failure == null
              ? ""
              : ", FAILED " + signature(result.failure)));
      if (result.failure == null) {
        continue;
      }
      final AssertionError report = shrinkAndReport(config, stream, result);
      if (!collect) {
        throw report;
      }
      final String name = "failure-" + kind.name().toLowerCase() + "-" + seed + ".txt";
      final Path file = FAILURE_DIRECTORY.resolve(name);
      try {
        Files.createDirectories(FAILURE_DIRECTORY);
        Files.writeString(file, report.getMessage(), StandardCharsets.UTF_8);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      failures.add(config.header() + " -> " + signature(result.failure) + " (" + file + ")");
      System.out.println("[hot-property] " + failures.get(failures.size() - 1));
    }
    System.out.println("[hot-property] " + kind + ": " + seeds + " seed(s) x " + ops + " ops in "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms; final slots " + storedKeys + ", max height "
        + maxHeight + "; handlers " + handlers);
    return failures;
  }

  /** Replay one stream (the format {@link Op#line()} prints) under {@code config}; throws its failure. */
  static void replay(final Kind kind, final VersioningType versioning, final int consolidationInterval,
      final Path directory, final String stream) {
    final CaseConfig config = new CaseConfig(kind, versioning, -1L, consolidationInterval);
    final List<Op> ops = Op.parseStream(stream);
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(consolidationInterval);
    try (Runner runner = new Runner(config, directory)) {
      runner.run(ops);
    } finally {
      AbstractHOTIndexWriter.setConsolidationIntervalForTesting(0);
      Databases.clearGlobalCaches();
    }
  }

  private record CaseResult(@Nullable Throwable failure, int applied, Map<String, Integer> handlers, int storedKeys,
      int height) {
  }

  private CaseResult runCase(final CaseConfig config, final List<Op> ops) {
    final Path directory = temporaryDirectory.resolve("c" + caseCounter++);
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(config.consolidationInterval);
    Runner runner = null;
    try {
      runner = new Runner(config, directory);
      runner.run(ops);
      return new CaseResult(null, runner.applied, runner.handlers, runner.driver.storedKeys(), runner.driver.height());
    } catch (final Throwable failure) {
      return new CaseResult(failure, runner == null
          ? 0
          : runner.applied, runner == null
              ? Map.of()
              : runner.handlers, 0, 0);
    } finally {
      if (runner != null) {
        runner.close();
      }
      AbstractHOTIndexWriter.setConsolidationIntervalForTesting(0);
      Databases.clearGlobalCaches();
      deleteRecursively(directory);
    }
  }

  private AssertionError shrinkAndReport(final CaseConfig config, final List<Op> stream, final CaseResult result) {
    final Throwable failure = result.failure;
    final String signature = signature(failure);
    final List<Op> failing = new ArrayList<>(stream.subList(0, Math.min(stream.size(), result.applied + 1)));
    final long started = System.nanoTime();
    final long deadline = started + TimeUnit.SECONDS.toNanos(SHRINK_SECONDS);
    final int[] replays = new int[1];
    final List<Op> shrunk = shrink(config, failing, signature, deadline, replays);
    final StringBuilder sb = new StringBuilder(512);
    sb.append("HOT structural property violated\n")
      .append(config.header())
      .append('\n')
      .append("ops=")
      .append(shrunk.size())
      .append(" (shrunk from ")
      .append(failing.size())
      .append(" in ")
      .append(replays[0])
      .append(" replays, ")
      .append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
      .append(" ms")
      .append(System.nanoTime() >= deadline
          ? ", shrink budget exhausted"
          : "")
      .append(")\nfailure=")
      .append(signature)
      .append("\nstream (replay with HOTStructuralPropertyTest.replay):\n");
    for (final Op op : shrunk) {
      sb.append(op.line()).append('\n');
    }
    return new AssertionError(sb.toString(), failure);
  }

  /** Delta debugging over the operation list; a candidate counts when it fails with the same signature. */
  private List<Op> shrink(final CaseConfig config, final List<Op> failing, final String signature, final long deadline,
      final int[] replays) {
    List<Op> current = failing;
    int granularity = 2;
    while (current.size() >= 2 && System.nanoTime() < deadline) {
      final int size = current.size();
      final int chunk = (size + granularity - 1) / granularity;
      boolean reduced = false;
      for (int start = 0; start < size && System.nanoTime() < deadline; start += chunk) {
        final List<Op> candidate = new ArrayList<>(size);
        candidate.addAll(current.subList(0, start));
        candidate.addAll(current.subList(Math.min(start + chunk, size), size));
        if (candidate.isEmpty()) {
          continue;
        }
        replays[0]++;
        final CaseResult result = runCase(config, candidate);
        if (result.failure != null && signature(result.failure).equals(signature)) {
          current = new ArrayList<>(candidate.subList(0, Math.min(candidate.size(), result.applied + 1)));
          granularity = Math.max(granularity - 1, 2);
          reduced = true;
          break;
        }
      }
      if (!reduced) {
        if (granularity >= current.size()) {
          break;
        }
        granularity = Math.min(granularity * 2, current.size());
      }
    }
    return shrinkBulkCounts(config, current, signature, deadline, replays);
  }

  /** Halve the run length of every bulk operation while the failure keeps its signature. */
  private List<Op> shrinkBulkCounts(final CaseConfig config, final List<Op> failing, final String signature,
      final long deadline, final int[] replays) {
    List<Op> current = failing;
    for (int i = 0; i < current.size() && System.nanoTime() < deadline; i++) {
      final Op op = current.get(i);
      if (op.type != Op.PUT_MANY && op.type != Op.REMOVE_MANY) {
        continue;
      }
      int count = op.n;
      while (count > 1 && System.nanoTime() < deadline) {
        final int smaller = count / 2;
        final List<Op> candidate = new ArrayList<>(current);
        candidate.set(i, op.withCount(smaller));
        replays[0]++;
        final CaseResult result = runCase(config, candidate);
        if (result.failure == null || !signature(result.failure).equals(signature)) {
          break;
        }
        count = smaller;
        current = candidate;
      }
    }
    return current;
  }

  /** A failure's identity for shrinking: its class and the first lines of its message, numbers masked. */
  static String signature(final Throwable failure) {
    final String message = String.valueOf(failure.getMessage());
    final String[] lines = message.split("\n");
    final StringBuilder sb = new StringBuilder(failure.getClass().getSimpleName()).append(": ");
    for (int i = 0; i < Math.min(2, lines.length); i++) {
      sb.append(lines[i].trim()).append(' ');
    }
    final String masked = sb.toString().replaceAll("0x[0-9a-fA-F]+|[0-9a-fA-F]{8,}|-?\\d+", "#");
    return masked.length() > 200
        ? masked.substring(0, 200)
        : masked;
  }

  private static void deleteRecursively(final Path directory) {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException ignored) {
          // best effort: the temporary directory is removed by JUnit anyway
        }
      });
    } catch (final IOException ignored) {
      // best effort
    }
  }

  // ===== case configuration and operations =====

  record CaseConfig(Kind kind, VersioningType versioning, long seed, int consolidationInterval) {
    static CaseConfig forSeed(final Kind kind, final long seed) {
      return new CaseConfig(kind, VERSIONINGS[(int) Math.floorMod(seed, VERSIONINGS.length)], seed,
          CONSOLIDATION_INTERVALS[(int) Math.floorMod(seed / VERSIONINGS.length, CONSOLIDATION_INTERVALS.length)]);
    }

    String header() {
      return "kind=" + kind + " versioning=" + versioning + " seed=" + seed + " consolidationInterval="
          + consolidationInterval;
    }
  }

  /**
   * One stream element. {@code P} puts a key (posting kinds: add node key {@code v}; projection: a
   * blob of length {@code k3} generated from seed {@code k2}), {@code R} removes it, {@code M} and
   * {@code X} put and remove the {@code n} node keys {@code v, v+2, ..., v+2(n-1)} under one key in
   * a single step (a stride-two run is an array container, two bytes per node key: how a posting
   * grows to several KiB and splits its leaf by bytes, where a consecutive run would compress to a
   * four-byte run container), {@code C}
   * commits, {@code V r} commits and reverts the next transaction to revision {@code r}, {@code O}
   * commits and reopens the database cold.
   */
  record Op(char type, long k1, long k2, long k3, long v, int n) {
    static final char PUT = 'P';
    static final char REMOVE = 'R';
    static final char PUT_MANY = 'M';
    static final char REMOVE_MANY = 'X';
    static final char COMMIT = 'C';
    static final char REVERT = 'V';
    static final char REOPEN = 'O';

    static final Op COMMIT_OP = new Op(COMMIT, 0, 0, 0, 0, 0);
    static final Op REOPEN_OP = new Op(REOPEN, 0, 0, 0, 0, 0);

    static Op put(final long k1, final long k2, final long k3, final long v) {
      return new Op(PUT, k1, k2, k3, v, 1);
    }

    static Op remove(final long k1, final long k2, final long k3, final long v) {
      return new Op(REMOVE, k1, k2, k3, v, 1);
    }

    static Op putMany(final long k1, final long k2, final long k3, final long v, final int n) {
      return new Op(PUT_MANY, k1, k2, k3, v, n);
    }

    static Op removeMany(final long k1, final long k2, final long k3, final long v, final int n) {
      return new Op(REMOVE_MANY, k1, k2, k3, v, n);
    }

    static Op revert(final int revision) {
      return new Op(REVERT, revision, 0, 0, 0, 0);
    }

    boolean isPut() {
      return type == PUT || type == PUT_MANY;
    }

    boolean mutates() {
      return type == PUT || type == REMOVE || type == PUT_MANY || type == REMOVE_MANY;
    }

    Op withCount(final int count) {
      return new Op(type, k1, k2, k3, v, count);
    }

    String line() {
      return switch (type) {
        case PUT, REMOVE -> type + " " + k1 + " " + k2 + " " + k3 + " " + v;
        case PUT_MANY, REMOVE_MANY -> type + " " + k1 + " " + k2 + " " + k3 + " " + v + " " + n;
        case REVERT -> "V " + k1;
        default -> String.valueOf(type);
      };
    }

    static Op parse(final String line) {
      final String[] fields = line.trim().split("\\s+");
      final char type = fields[0].charAt(0);
      return switch (type) {
        case PUT, REMOVE -> new Op(type, Long.parseLong(fields[1]), Long.parseLong(fields[2]),
            Long.parseLong(fields[3]), Long.parseLong(fields[4]), 1);
        case PUT_MANY, REMOVE_MANY -> new Op(type, Long.parseLong(fields[1]), Long.parseLong(fields[2]),
            Long.parseLong(fields[3]), Long.parseLong(fields[4]), Integer.parseInt(fields[5]));
        case REVERT -> new Op(type, Long.parseLong(fields[1]), 0, 0, 0, 0);
        case COMMIT, REOPEN -> new Op(type, 0, 0, 0, 0, 0);
        default -> throw new IllegalArgumentException("unknown stream line: " + line);
      };
    }

    static List<Op> parseStream(final String stream) {
      final List<Op> ops = new ArrayList<>();
      for (final String line : stream.split("\n")) {
        final String trimmed = line.trim();
        if (!trimmed.isEmpty() && trimmed.charAt(0) != '#') {
          ops.add(parse(trimmed));
        }
      }
      return ops;
    }
  }

  /** Serialized key bytes with unsigned lexicographic order, the order the trie must present. */
  private record ByteKey(byte[] bytes) implements Comparable<ByteKey> {
    @Override
    public int compareTo(final ByteKey other) {
      return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof ByteKey key && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
      return HexFormat.of().formatHex(bytes);
    }
  }

  /** A reference-model disagreement; {@code check} names which property broke. */
  static final class PropertyViolation extends AssertionError {
    private static final long serialVersionUID = 1L;

    final String check;

    PropertyViolation(final String check, final String detail) {
      super(check + ": " + detail);
      this.check = check;
    }
  }

  // ===== execution =====

  /** Drives one case's database: transactions, commits, reverts, reopens and the per-step checks. */
  private static final class Runner implements AutoCloseable {
    private final CaseConfig config;
    private final Path databasePath;
    final Driver driver;
    private final List<Object> snapshots = new ArrayList<>();
    final Map<String, Integer> handlers = new TreeMap<>();
    private Database<JsonResourceSession> database;
    private JsonResourceSession session;
    private @Nullable JsonNodeTrx wtx;
    private int commits;
    private int olderRevisionCursor;
    int applied;

    Runner(final CaseConfig config, final Path databasePath) {
      this.config = config;
      this.databasePath = databasePath;
      this.driver = newDriver(config.kind);
      if (!Databases.createJsonDatabase(new DatabaseConfiguration(databasePath))) {
        throw new IllegalStateException("could not create " + databasePath);
      }
      openStore();
      if (!database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(config.versioning).build())) {
        throw new IllegalStateException("could not create resource in " + databasePath);
      }
      session = database.beginResourceSession(RESOURCE);
      snapshots.add(driver.snapshot());
    }

    void run(final List<Op> ops) {
      for (final Op op : ops) {
        step(op);
        applied++;
      }
    }

    private void step(final Op op) {
      switch (op.type) {
        case Op.PUT, Op.REMOVE, Op.PUT_MANY, Op.REMOVE_MANY -> {
          ensureTransaction();
          final AbstractHOTIndexWriter<?> writer = driver.writer();
          writer.lastDispatchHandler = "-";
          driver.apply(op);
          final String handler = writer.lastDispatchHandler;
          if (!"-".equals(handler)) {
            handlers.merge(handler, 1, Integer::sum);
          }
          final boolean inPlace = IN_PLACE_MERGE.equals(handler) || IN_PLACE_REMOVE.equals(handler);
          driver.verifyWriterSide(op, !inPlace || applied % FULL_CHECK_EVERY == 0);
        }
        case Op.COMMIT -> commitIfOpen();
        case Op.REVERT -> {
          commitIfOpen();
          final int revision = (int) op.k1;
          if (revision >= 0 && revision < snapshots.size()) {
            wtx = session.beginNodeTrx();
            wtx.revertTo(revision);
            driver.restore(snapshots.get(revision));
            driver.open(wtx.getStorageEngineWriter());
          }
        }
        case Op.REOPEN -> {
          commitIfOpen();
          closeStore();
          Databases.clearGlobalCaches();
          openStore();
          session = database.beginResourceSession(RESOURCE);
          verifyAllRevisions(true);
        }
        default -> throw new IllegalArgumentException("unknown op " + op);
      }
    }

    private void ensureTransaction() {
      if (wtx == null) {
        wtx = session.beginNodeTrx();
        driver.open(wtx.getStorageEngineWriter());
      }
    }

    private void commitIfOpen() {
      final JsonNodeTrx open = wtx;
      if (open == null) {
        return;
      }
      open.commit();
      open.close();
      wtx = null;
      final int latest = session.getMostRecentRevisionNumber();
      while (snapshots.size() <= latest) {
        snapshots.add(driver.snapshot());
      }
      commits++;
      // The new revision every time, exactly every EXACT_COMMIT_EVERY commits; older revisions on a
      // rotating sample so a long stream with many commits stays linear while every revision is
      // revisited periodically. A cold reopen checks all of them.
      verifyRevision(latest, commits % EXACT_COMMIT_EVERY == 0, false);
      for (int i = 0; i < OLDER_REVISIONS_PER_COMMIT && latest > 1; i++) {
        final int revision = 1 + (olderRevisionCursor++ % (latest - 1));
        verifyRevision(revision, false, false);
      }
    }

    private void verifyAllRevisions(final boolean cold) {
      final int latest = snapshots.size() - 1;
      for (int revision = 1; revision <= latest; revision++) {
        verifyRevision(revision, revision == latest, cold);
      }
    }

    private void verifyRevision(final int revision, final boolean exact, final boolean cold) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        driver.verifyRevision(rtx.getStorageEngineReader(), snapshots.get(revision), exact);
      } catch (final PropertyViolation violation) {
        throw new PropertyViolation(violation.check, "revision " + revision + " of " + (snapshots.size() - 1) + (cold
            ? " (cold)"
            : "") + ": " + violation.getMessage());
      }
    }

    private void openStore() {
      database = Databases.openJsonDatabase(databasePath);
    }

    private void closeStore() {
      final JsonNodeTrx open = wtx;
      wtx = null;
      try {
        if (open != null) {
          open.rollback();
          open.close();
        }
      } finally {
        try {
          if (session != null) {
            session.close();
          }
        } finally {
          if (database != null) {
            database.close();
          }
        }
      }
    }

    @Override
    public void close() {
      try {
        closeStore();
      } catch (final RuntimeException ignored) {
        // the case already failed or finished; closing is best effort
      }
    }

    private static Driver newDriver(final Kind kind) {
      return switch (kind) {
        case CAS -> new SerializedKeyDriver<>(IndexType.CAS, INDEX_NUMBER, CASKeySerializer.INSTANCE,
            HOTStructuralPropertyTest::casKey);
        case NAME -> new SerializedKeyDriver<>(IndexType.NAME, NAME_INDEX_NUMBER, NameKeySerializer.INSTANCE,
            HOTStructuralPropertyTest::nameKey);
        case VALIDTIME -> new SerializedKeyDriver<>(IndexType.VALIDTIME, INDEX_NUMBER, ValidTimeKeySerializer.INSTANCE,
            op -> new ValidTimeKey((byte) op.k1, op.k2, op.k3));
        case PATH -> new LongKeyDriver(INDEX_NUMBER);
        case PROJECTION -> new ProjectionDriver(INDEX_NUMBER);
      };
    }
  }

  /** One index kind: applies stream operations to a writer and to its reference, and checks both. */
  private interface Driver {
    /** Bind a fresh writer to the transaction's storage engine. */
    void open(StorageEngineWriter storageEngineWriter);

    AbstractHOTIndexWriter<?> writer();

    void apply(Op op);

    Object snapshot();

    void restore(Object snapshot);

    /**
     * Writer-side checks after a mutation: the touched key's lookup always, and with {@code full}
     * the structural validator, the ordered slot walk and sampled lookups as well.
     */
    void verifyWriterSide(Op lastOp, boolean full);

    /**
     * Reader-side checks of one committed revision against the reference snapshot taken then:
     * structure, slot order and the logical iterator always; {@code exact} compares every value and
     * every lookup instead of sampling.
     */
    void verifyRevision(StorageEngineReader reader, Object snapshot, boolean exact);

    /** Stored slots and trie height seen by the most recent structural validation. */
    int storedKeys();

    int height();
  }

  // ===== posting indexes (CAS, PATH, NAME, VALIDTIME) =====

  /** Point lookup and ordered iteration over one logical view of a posting index. */
  private interface PostingLookup<K> {
    @Nullable
    NodeReferences get(K key);

    /** {@code null} when the view has no logical iterator (the writer). */
    @Nullable
    Iterator<? extends Map.Entry<K, NodeReferences>> iterator();
  }

  /**
   * The reference postings of one logical key. Immutable once built, so revision snapshots share
   * it: a mutation replaces the entry with a new one built from a copy.
   */
  private static final class Logical<K> {
    final K key;
    final LongAVLTreeSet nodeKeys;
    /** Distinct {@code nodeKey >>> 16} values, ascending: the chunk slots this key occupies. */
    final int[] chunks;

    Logical(final K key, final LongAVLTreeSet nodeKeys) {
      this.key = key;
      this.nodeKeys = nodeKeys;
      int count = 0;
      int last = -1;
      for (final long nodeKey : nodeKeys) {
        final int chunk = (int) (nodeKey >>> 16);
        if (chunk != last) {
          count++;
          last = chunk;
        }
      }
      chunks = new int[count];
      int i = 0;
      last = -1;
      for (final long nodeKey : nodeKeys) {
        final int chunk = (int) (nodeKey >>> 16);
        if (chunk != last) {
          chunks[i++] = chunk;
          last = chunk;
        }
      }
    }

    long[] sortedNodeKeys() {
      return nodeKeys.toLongArray();
    }
  }

  private abstract static class PostingDriver<K> implements Driver {
    final IndexType indexType;
    final int indexNumber;
    private TreeMap<ByteKey, Logical<K>> model = new TreeMap<>();
    private byte[] scratch = new byte[512];
    private int sampleCursor;
    private int storedKeys;
    private int height;

    @Override
    public final int storedKeys() {
      return storedKeys;
    }

    @Override
    public final int height() {
      return height;
    }

    private void noteValidation(final HOTInvariantValidator.Result result) {
      result.assertOk();
      storedKeys = result.storedKeyCount();
      height = Math.max(height, result.observedHeight());
    }

    PostingDriver(final IndexType indexType, final int indexNumber) {
      this.indexType = indexType;
      this.indexNumber = indexNumber;
    }

    abstract K keyOf(Op op);

    abstract int maxPrefixLength(K key);

    abstract int serializePrefix(K key, byte[] dest);

    abstract void writerPut(K key, long nodeKey);

    abstract boolean writerRemove(K key, long nodeKey);

    abstract PostingLookup<K> writerLookup();

    abstract PostingLookup<K> readerLookup(StorageEngineReader reader);

    final ByteKey prefixKey(final K key) {
      final int needed = maxPrefixLength(key) + HOTKeySerializer.CHUNK_IDX_BYTES;
      if (scratch.length < needed) {
        scratch = new byte[Math.max(needed, scratch.length * 2)];
      }
      final int length = serializePrefix(key, scratch);
      return new ByteKey(Arrays.copyOf(scratch, length));
    }

    @Override
    public final void apply(final Op op) {
      final K key = keyOf(op);
      final ByteKey prefix = prefixKey(key);
      final int count = Math.max(1, op.n);
      final Logical<K> before = model.get(prefix);
      final LongAVLTreeSet nodeKeys = before == null
          ? new LongAVLTreeSet()
          : new LongAVLTreeSet(before.nodeKeys);
      final int stride = count > 1
          ? BULK_STRIDE
          : 1;
      if (op.isPut()) {
        for (int i = 0; i < count; i++) {
          final long nodeKey = op.v + (long) i * stride;
          writerPut(key, nodeKey);
          nodeKeys.add(nodeKey);
        }
        model.put(prefix, new Logical<>(key, nodeKeys));
        return;
      }
      for (int i = 0; i < count; i++) {
        final long nodeKey = op.v + (long) i * stride;
        final boolean removed = writerRemove(key, nodeKey);
        final boolean expected = nodeKeys.remove(nodeKey);
        if (removed != expected) {
          throw new PropertyViolation("remove-result", "remove of node key " + nodeKey + " under " + prefix + " returned "
              + removed + ", reference says " + expected);
        }
      }
      if (nodeKeys.isEmpty()) {
        model.remove(prefix);
      } else {
        model.put(prefix, new Logical<>(key, nodeKeys));
      }
    }

    @Override
    public final Object snapshot() {
      return new TreeMap<>(model);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void restore(final Object snapshot) {
      model = new TreeMap<>((TreeMap<ByteKey, Logical<K>>) snapshot);
    }

    @Override
    public final void verifyWriterSide(final Op lastOp, final boolean full) {
      final PostingLookup<K> lookup = writerLookup();
      final K touched = keyOf(lastOp);
      checkLookup(lookup, prefixKey(touched), touched, model);
      if (!full) {
        return;
      }
      final AbstractHOTIndexWriter<?> writer = writer();
      final StorageEngineReader reader = writer.getStorageEngineReader();
      final PageReference root = writer.getRootReference();
      noteValidation(HOTInvariantValidator.validate(root, reader));
      compareSlots("writer-slot-walk", liveSlotKeys(reader, root, true), expectedSlots(model));
      sampleLookups(lookup, model, LOOKUP_SAMPLE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void verifyRevision(final StorageEngineReader reader, final Object snapshot, final boolean exact) {
      final TreeMap<ByteKey, Logical<K>> expected = (TreeMap<ByteKey, Logical<K>>) snapshot;
      final PageReference root = HOTInvariantValidator.resolveRootRef(reader, indexType, indexNumber);
      noteValidation(HOTInvariantValidator.validate(root, reader));
      compareSlots("reader-slot-walk", liveSlotKeys(reader, root, true), expectedSlots(expected));
      final PostingLookup<K> lookup = readerLookup(reader);
      final Iterator<? extends Map.Entry<K, NodeReferences>> iterator = lookup.iterator();
      if (iterator != null) {
        compareIterator(iterator, expected, exact);
      }
      if (exact) {
        for (final Map.Entry<ByteKey, Logical<K>> entry : expected.entrySet()) {
          checkLookup(lookup, entry.getKey(), entry.getValue().key, expected);
        }
      } else {
        sampleLookups(lookup, expected, LOOKUP_SAMPLE);
      }
    }

    private TreeSet<ByteKey> expectedSlots(final TreeMap<ByteKey, Logical<K>> source) {
      final TreeSet<ByteKey> slots = new TreeSet<>();
      for (final Map.Entry<ByteKey, Logical<K>> entry : source.entrySet()) {
        final byte[] prefix = entry.getKey().bytes;
        for (final int chunk : entry.getValue().chunks) {
          final byte[] composite = Arrays.copyOf(prefix, prefix.length + HOTKeySerializer.CHUNK_IDX_BYTES);
          HOTKeySerializer.writeChunkIdxBE(composite, prefix.length, chunk);
          slots.add(new ByteKey(composite));
        }
      }
      return slots;
    }

    /**
     * The logical iterator must present the reference's keys in order; {@code exact} compares every
     * posting list, otherwise its cardinality and both extremes (the exact pass runs periodically and
     * at every cold reopen).
     */
    private void compareIterator(final Iterator<? extends Map.Entry<K, NodeReferences>> iterator,
        final TreeMap<ByteKey, Logical<K>> expected, final boolean exact) {
      final Iterator<Map.Entry<ByteKey, Logical<K>>> reference = expected.entrySet().iterator();
      int position = 0;
      while (iterator.hasNext()) {
        final Map.Entry<K, NodeReferences> actual = iterator.next();
        if (!reference.hasNext()) {
          throw new PropertyViolation("reader-iterator-extra",
              "iterator yields a " + position + "th logical key but the reference holds " + expected.size());
        }
        final Map.Entry<ByteKey, Logical<K>> wanted = reference.next();
        final ByteKey actualPrefix = prefixKey(actual.getKey());
        if (!actualPrefix.equals(wanted.getKey())) {
          throw new PropertyViolation("reader-iterator-order",
              "logical key " + position + " is " + actualPrefix + ", reference expects " + wanted.getKey());
        }
        final LongAVLTreeSet wantedNodeKeys = wanted.getValue().nodeKeys;
        final NodeReferences actualNodeKeys = actual.getValue();
        if (exact) {
          final long[] wantedArray = wantedNodeKeys.toLongArray();
          final long[] actualArray = actualNodeKeys.toSortedArray();
          if (!Arrays.equals(wantedArray, actualArray)) {
            throw new PropertyViolation("reader-iterator-postings",
                "postings of " + actualPrefix + ": " + describe(actualArray) + " vs reference " + describe(wantedArray));
          }
        } else if (actualNodeKeys.cardinality() != wantedNodeKeys.size()
            || !actualNodeKeys.isPresent(wantedNodeKeys.firstLong())
            || !actualNodeKeys.isPresent(wantedNodeKeys.lastLong())) {
          throw new PropertyViolation("reader-iterator-postings", "postings of " + actualPrefix + ": "
              + actualNodeKeys.cardinality() + " node keys vs reference " + wantedNodeKeys.size());
        }
        position++;
      }
      if (reference.hasNext()) {
        throw new PropertyViolation("reader-iterator-missing",
            "iterator ended after " + position + " logical keys, reference holds " + expected.size() + "; first missing "
                + reference.next().getKey());
      }
    }

    private void sampleLookups(final PostingLookup<K> lookup, final TreeMap<ByteKey, Logical<K>> expected,
        final int sample) {
      if (expected.isEmpty()) {
        return;
      }
      final int size = expected.size();
      final int count = Math.min(sample, size);
      final int stride = Math.max(1, size / count);
      final Iterator<Map.Entry<ByteKey, Logical<K>>> iterator = expected.entrySet().iterator();
      int index = 0;
      final int offset = sampleCursor++ % stride;
      while (iterator.hasNext()) {
        final Map.Entry<ByteKey, Logical<K>> entry = iterator.next();
        if ((index++ - offset) % stride == 0) {
          checkLookup(lookup, entry.getKey(), entry.getValue().key, expected);
        }
      }
    }

    private void checkLookup(final PostingLookup<K> lookup, final ByteKey prefix, final K key,
        final TreeMap<ByteKey, Logical<K>> expected) {
      final Logical<K> logical = expected.get(prefix);
      final NodeReferences actual = lookup.get(key);
      if (logical == null) {
        if (actual != null && actual.cardinality() != 0) {
          throw new PropertyViolation("lookup-absent",
              "key " + prefix + " has no postings in the reference but the index answers " + describe(actual.toSortedArray()));
        }
        return;
      }
      final long[] wanted = logical.sortedNodeKeys();
      if (actual == null) {
        throw new PropertyViolation("lookup-missing", "key " + prefix + " answers nothing, reference holds " + describe(wanted));
      }
      final long[] got = actual.toSortedArray();
      if (!Arrays.equals(wanted, got)) {
        throw new PropertyViolation("lookup-postings", "key " + prefix + " answers " + describe(got) + ", reference holds "
            + describe(wanted));
      }
    }
  }

  /** CAS, NAME and VALIDTIME: a {@link HOTIndexWriter} over a {@link HOTKeySerializer}. */
  private static final class SerializedKeyDriver<K extends Comparable<? super K>> extends PostingDriver<K> {
    private final HOTKeySerializer<K> serializer;
    private final Function<Op, K> keyFunction;
    private HOTIndexWriter<K> hotWriter;

    SerializedKeyDriver(final IndexType indexType, final int indexNumber, final HOTKeySerializer<K> serializer,
        final Function<Op, K> keyFunction) {
      super(indexType, indexNumber);
      this.serializer = serializer;
      this.keyFunction = keyFunction;
    }

    @Override
    public void open(final StorageEngineWriter storageEngineWriter) {
      hotWriter = HOTIndexWriter.create(storageEngineWriter, serializer, indexType, indexNumber);
    }

    @Override
    public AbstractHOTIndexWriter<?> writer() {
      return hotWriter;
    }

    @Override
    K keyOf(final Op op) {
      return keyFunction.apply(op);
    }

    @Override
    int maxPrefixLength(final K key) {
      return serializer.maxSerializedLength(key);
    }

    @Override
    int serializePrefix(final K key, final byte[] dest) {
      return serializer.serialize(key, dest, 0);
    }

    @Override
    void writerPut(final K key, final long nodeKey) {
      hotWriter.indexNodeKey(key, nodeKey);
    }

    @Override
    boolean writerRemove(final K key, final long nodeKey) {
      return hotWriter.remove(key, nodeKey);
    }

    @Override
    PostingLookup<K> writerLookup() {
      final HOTIndexWriter<K> writer = hotWriter;
      return new PostingLookup<>() {
        @Override
        public @Nullable NodeReferences get(final K key) {
          return writer.get(key, SearchMode.EQUAL);
        }

        @Override
        public @Nullable Iterator<? extends Map.Entry<K, NodeReferences>> iterator() {
          return null;
        }
      };
    }

    @Override
    PostingLookup<K> readerLookup(final StorageEngineReader reader) {
      final HOTIndexReader<K> hotReader = HOTIndexReader.create(reader, serializer, indexType, indexNumber);
      return new PostingLookup<>() {
        @Override
        public @Nullable NodeReferences get(final K key) {
          return hotReader.get(key, SearchMode.EQUAL);
        }

        @Override
        public Iterator<? extends Map.Entry<K, NodeReferences>> iterator() {
          return hotReader.iterator();
        }
      };
    }
  }

  /** PATH: the primitive-long writer. */
  private static final class LongKeyDriver extends PostingDriver<Long> {
    private HOTLongIndexWriter hotWriter;

    LongKeyDriver(final int indexNumber) {
      super(IndexType.PATH, indexNumber);
    }

    @Override
    public void open(final StorageEngineWriter storageEngineWriter) {
      hotWriter = HOTLongIndexWriter.create(storageEngineWriter, IndexType.PATH, indexNumber);
    }

    @Override
    public AbstractHOTIndexWriter<?> writer() {
      return hotWriter;
    }

    @Override
    Long keyOf(final Op op) {
      return op.k1;
    }

    @Override
    int maxPrefixLength(final Long key) {
      return HOTLongKeySerializer.SERIALIZED_SIZE;
    }

    @Override
    int serializePrefix(final Long key, final byte[] dest) {
      return PathKeySerializer.INSTANCE.serialize(key, dest, 0);
    }

    @Override
    void writerPut(final Long key, final long nodeKey) {
      hotWriter.indexNodeKey(key, nodeKey);
    }

    @Override
    boolean writerRemove(final Long key, final long nodeKey) {
      return hotWriter.remove(key, nodeKey);
    }

    @Override
    PostingLookup<Long> writerLookup() {
      final HOTLongIndexWriter writer = hotWriter;
      return new PostingLookup<>() {
        @Override
        public @Nullable NodeReferences get(final Long key) {
          return writer.get(key, SearchMode.EQUAL);
        }

        @Override
        public @Nullable Iterator<? extends Map.Entry<Long, NodeReferences>> iterator() {
          return null;
        }
      };
    }

    @Override
    PostingLookup<Long> readerLookup(final StorageEngineReader reader) {
      final HOTLongIndexReader hotReader = HOTLongIndexReader.create(reader, IndexType.PATH, indexNumber);
      return new PostingLookup<>() {
        @Override
        public @Nullable NodeReferences get(final Long key) {
          return hotReader.get(key, SearchMode.EQUAL);
        }

        @Override
        public Iterator<? extends Map.Entry<Long, NodeReferences>> iterator() {
          return hotReader.iterator();
        }
      };
    }
  }

  // ===== projection store =====

  /**
   * The projection slot store: raw blobs under long slot keys, inline up to 512 bytes and spilled to
   * a side-map segment page above that, replaced in place and tombstoned.
   */
  private static final class ProjectionDriver implements Driver {
    private final int indexNumber;
    private TreeMap<ByteKey, byte[]> model = new TreeMap<>();
    private ProjectionIndexHOTStorage storage;
    private int sampleCursor;
    private int storedKeys;
    private int height;

    ProjectionDriver(final int indexNumber) {
      this.indexNumber = indexNumber;
    }

    @Override
    public int storedKeys() {
      return storedKeys;
    }

    @Override
    public int height() {
      return height;
    }

    private void noteValidation(final HOTInvariantValidator.Result result) {
      result.assertOk();
      storedKeys = result.storedKeyCount();
      height = Math.max(height, result.observedHeight());
    }

    @Override
    public void open(final StorageEngineWriter storageEngineWriter) {
      storage = new ProjectionIndexHOTStorage(storageEngineWriter, indexNumber);
    }

    @Override
    public AbstractHOTIndexWriter<?> writer() {
      return storage;
    }

    @Override
    public void apply(final Op op) {
      final long slotKey = op.k1;
      if (op.isPut()) {
        final byte[] value = blobBytes(op.k2, (int) op.k3);
        storage.putBlob(slotKey, value);
        model.put(slotKeyBytes(slotKey), value);
      } else {
        storage.tombstoneBlob(slotKey);
        model.remove(slotKeyBytes(slotKey));
      }
    }

    @Override
    public Object snapshot() {
      return new TreeMap<>(model);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void restore(final Object snapshot) {
      model = new TreeMap<>((TreeMap<ByteKey, byte[]>) snapshot);
    }

    @Override
    public void verifyWriterSide(final Op lastOp, final boolean full) {
      checkBlob(slotKeyBytes(lastOp.k1), storage.getBlob(lastOp.k1), model);
      if (!full) {
        return;
      }
      final StorageEngineReader reader = storage.getStorageEngineReader();
      final PageReference root = storage.getRootReference();
      noteValidation(HOTInvariantValidator.validate(root, reader));
      compareSlots("writer-slot-walk", liveSlotKeys(reader, root, false), new TreeSet<>(model.keySet()));
      sampleBlobs(model, slot -> storage.getBlob(slot));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void verifyRevision(final StorageEngineReader reader, final Object snapshot, final boolean exact) {
      final TreeMap<ByteKey, byte[]> expected = (TreeMap<ByteKey, byte[]>) snapshot;
      final PageReference root = HOTInvariantValidator.resolveRootRef(reader, IndexType.PROJECTION, indexNumber);
      noteValidation(HOTInvariantValidator.validate(root, reader));
      compareSlots("reader-slot-walk", liveSlotKeys(reader, root, false), new TreeSet<>(expected.keySet()));
      final Function<Long, byte @Nullable []> read = slot -> ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot);
      if (exact) {
        for (final ByteKey key : expected.keySet()) {
          checkBlob(key, read.apply(slotKeyOf(key)), expected);
        }
      } else {
        sampleBlobs(expected, read);
      }
    }

    private void sampleBlobs(final TreeMap<ByteKey, byte[]> expected, final Function<Long, byte @Nullable []> read) {
      if (expected.isEmpty()) {
        return;
      }
      final int size = expected.size();
      final int stride = Math.max(1, size / Math.min(LOOKUP_SAMPLE, size));
      final int offset = sampleCursor++ % stride;
      int index = 0;
      for (final ByteKey key : expected.keySet()) {
        if ((index++ - offset) % stride == 0) {
          checkBlob(key, read.apply(slotKeyOf(key)), expected);
        }
      }
    }

    private static void checkBlob(final ByteKey key, final byte @Nullable [] actual, final TreeMap<ByteKey, byte[]> expected) {
      final byte[] wanted = expected.get(key);
      if (wanted == null) {
        if (actual != null) {
          throw new PropertyViolation("blob-absent", "slot " + key + " is tombstoned in the reference but reads "
              + actual.length + " bytes");
        }
        return;
      }
      if (actual == null) {
        throw new PropertyViolation("blob-missing", "slot " + key + " reads nothing, reference holds " + wanted.length + " bytes");
      }
      if (!Arrays.equals(wanted, actual)) {
        throw new PropertyViolation("blob-bytes", "slot " + key + " reads " + actual.length + " bytes that differ from the "
            + wanted.length + " reference bytes");
      }
    }

    private static ByteKey slotKeyBytes(final long slotKey) {
      final byte[] bytes = new byte[HOTLongKeySerializer.SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serialize(slotKey, bytes, 0);
      return new ByteKey(bytes);
    }

    private static long slotKeyOf(final ByteKey key) {
      return PathKeySerializer.INSTANCE.deserialize(key.bytes, 0, key.bytes.length);
    }

    static byte[] blobBytes(final long seed, final int length) {
      final byte[] bytes = new byte[length];
      new SplittableRandom(seed * 0x9E3779B97F4A7C15L + length).nextBytes(bytes);
      return bytes;
    }
  }

  // ===== shared checks =====

  /** The live slot keys of the trie under {@code root}, in walk order; tombstones are skipped. */
  private static List<ByteKey> liveSlotKeys(final StorageEngineReader reader, final @Nullable PageReference root,
      final boolean postingIndex) {
    final List<ByteKey> keys = new ArrayList<>();
    if (root == null) {
      return keys;
    }
    try (HOTTrieReader trie = new HOTTrieReader(reader); HOTRangeCursor cursor = trie.range(root, null, null)) {
      while (cursor.hasNext()) {
        final HOTRangeCursor.Entry entry = cursor.next();
        final long valueLength = entry.value().byteSize();
        final boolean tombstone = postingIndex
            ? valueLength == 1 && entry.value().get(ValueLayout.JAVA_BYTE, 0) == POSTING_TOMBSTONE
            : valueLength == 0;
        if (!tombstone) {
          keys.add(new ByteKey(entry.key().toArray(ValueLayout.JAVA_BYTE)));
        }
      }
    }
    return keys;
  }

  private static void compareSlots(final String check, final List<ByteKey> actual, final TreeSet<ByteKey> expected) {
    final Iterator<ByteKey> reference = expected.iterator();
    for (int i = 0; i < actual.size(); i++) {
      final ByteKey key = actual.get(i);
      if (i > 0 && actual.get(i - 1).compareTo(key) >= 0) {
        throw new PropertyViolation(check, "slot " + i + " (" + key + ") does not sort above slot " + (i - 1) + " ("
            + actual.get(i - 1) + ")");
      }
      if (!reference.hasNext()) {
        throw new PropertyViolation(check, "slot " + i + " (" + key + ") is not in the reference, which holds "
            + expected.size() + " slots");
      }
      final ByteKey wanted = reference.next();
      if (!wanted.equals(key)) {
        throw new PropertyViolation(check, "slot " + i + " is " + key + ", reference expects " + wanted);
      }
    }
    if (reference.hasNext()) {
      throw new PropertyViolation(check, "walk ended after " + actual.size() + " slots, reference holds "
          + expected.size() + "; first missing " + reference.next());
    }
  }

  private static String describe(final long[] nodeKeys) {
    if (nodeKeys.length <= 6) {
      return Arrays.toString(nodeKeys);
    }
    return nodeKeys.length + " node keys [" + nodeKeys[0] + ".." + nodeKeys[nodeKeys.length - 1] + "]";
  }

  // ===== key functions shared by the generator and the drivers =====

  /**
   * CAS key from a stream operation: {@code k1} is the payload, {@code k2} packs the prefix length
   * (low byte), the number of payload hex digits (next byte) and the type code (next byte: 0 string,
   * 1 integer), {@code k3} the path node key.
   */
  static CASValue casKey(final Op op) {
    final int typeCode = (int) (op.k2 >>> 16) & 0xFF;
    if (typeCode == 1) {
      return new CASValue(new Int32((int) op.k1), Type.INT, op.k3);
    }
    return new CASValue(new Str(patternedString(op.k1, op.k2)), Type.STR, op.k3);
  }

  static QNm nameKey(final Op op) {
    return new QNm(patternedString(op.k1, op.k2));
  }

  /** A run of {@code x}s of the packed prefix length followed by the low packed-width hex digits of the payload. */
  static String patternedString(final long payload, final long packed) {
    final int prefixLength = (int) (packed & 0xFF);
    final int width = Math.max(1, Math.min(16, (int) ((packed >>> 8) & 0xFF)));
    final String hex = Long.toHexString(payload);
    final StringBuilder sb = new StringBuilder(prefixLength + width);
    for (int i = 0; i < prefixLength; i++) {
      sb.append('x');
    }
    for (int i = width - hex.length(); i > 0; i--) {
      sb.append('0');
    }
    sb.append(hex, Math.max(0, hex.length() - width), hex.length());
    return sb.toString();
  }

  // ===== stream generation =====

  private enum Shape {
    ASCENDING, DESCENDING, CLUSTERED, SPARSE_PARTIAL, PREFIX_TRAILING, RANDOM
  }

  /**
   * Generates one case's stream. Phases of a few dozen operations each pick a key shape, a posting
   * layout (one chunk growing by bytes, or node keys spread over chunks) and a share of removals and
   * revisits; commits follow a per-case cadence, and some commits are followed by a revert or a cold
   * reopen. Live keys are tracked so removals and revisits hit what the index holds.
   */
  static final class StreamGenerator {
    private final Kind kind;
    private final SplittableRandom random;
    private final int opCount;
    private final List<long[]> live = new ArrayList<>();
    private final List<List<long[]>> liveAtRevision = new ArrayList<>();
    private int revisions;

    StreamGenerator(final Kind kind, final long seed, final int opCount) {
      this.kind = kind;
      this.random = new SplittableRandom(seed);
      this.opCount = opCount;
    }

    List<Op> generate() {
      liveAtRevision.add(List.of());
      final List<Op> ops = new ArrayList<>(opCount + 16);
      final int cadenceScale = switch (random.nextInt(4)) {
        case 0 -> 3;
        case 1 -> 24;
        case 2 -> 160;
        default -> 900;
      };
      int untilCommit = nextCadence(cadenceScale);
      int work = 0;
      while (work < opCount) {
        final Phase phase = newPhase();
        for (int i = 0; i < phase.length && work < opCount; i++) {
          final Op op = nextMutation(phase);
          ops.add(op);
          // A bulk run of node keys costs the writer what a run of single puts would; budget it so.
          work += Math.max(1, op.n / 100);
          if (--untilCommit <= 0) {
            commit(ops);
            untilCommit = nextCadence(cadenceScale);
            final double roll = random.nextDouble();
            if (roll < 0.08 && revisions >= 1) {
              // Mostly a recent revision, so the trie keeps its accumulated structure; now and then
              // any revision at all.
              final int revision = random.nextInt(16) == 0
                  ? random.nextInt(revisions + 1)
                  : revisions - 1 - random.nextInt(Math.min(revisions, 3));
              ops.add(Op.revert(revision));
              live.clear();
              live.addAll(liveAtRevision.get(revision));
            } else if (roll < 0.20) {
              ops.add(Op.REOPEN_OP);
            }
          }
        }
      }
      commit(ops);
      ops.add(Op.REOPEN_OP);
      return ops;
    }

    private void commit(final List<Op> ops) {
      ops.add(Op.COMMIT_OP);
      revisions++;
      liveAtRevision.add(new ArrayList<>(live));
    }

    private int nextCadence(final int scale) {
      return Math.max(1, scale / 2 + random.nextInt(scale + 1));
    }

    private final class Phase {
      final Shape shape;
      final int length;
      final double removeShare;
      final double revisitShare;
      final boolean denseChunk;
      final boolean bulkPostings;
      final int chunkBase;
      final int chunkSpan;
      final long[] alphabet;
      final long[] centers;
      final long base;
      final byte store;
      final boolean bothStores;
      final int endpointSpan;
      final boolean integerType;
      final int[] bitPositions;
      final List<long[]> hotSet = new ArrayList<>(4);
      long counter;

      Phase() {
        final double shapeRoll = random.nextDouble();
        shape = shapeRoll < 0.30
            ? Shape.SPARSE_PARTIAL
            : shapeRoll < 0.50
                ? Shape.PREFIX_TRAILING
                : shapeRoll < 0.62
                    ? Shape.ASCENDING
                    : shapeRoll < 0.74
                        ? Shape.DESCENDING
                        : shapeRoll < 0.88
                            ? Shape.CLUSTERED
                            : Shape.RANDOM;
        length = 20 + random.nextInt(130);
        removeShare = switch (random.nextInt(5)) {
          case 0, 1 -> 0.0;
          case 2 -> 0.15;
          case 3 -> 0.3;
          default -> 0.8;
        };
        revisitShare = switch (random.nextInt(4)) {
          case 0, 1 -> 0.0;
          case 2 -> 0.1;
          default -> 0.6;
        };
        denseChunk = random.nextBoolean();
        bulkPostings = kind != Kind.PROJECTION && random.nextInt(10) < 4;
        chunkBase = random.nextInt(4);
        chunkSpan = 1 + random.nextInt(8);
        alphabet = new long[2 + random.nextInt(4)];
        for (int i = 0; i < alphabet.length; i++) {
          alphabet[i] = random.nextInt(256);
        }
        centers = new long[3];
        for (int i = 0; i < centers.length; i++) {
          centers[i] = random.nextInt(1 << 20);
        }
        base = random.nextLong();
        store = (byte) random.nextInt(2);
        bothStores = random.nextInt(4) == 0;
        endpointSpan = switch (random.nextInt(3)) {
          case 0 -> 4;
          case 1 -> 64;
          default -> 4096;
        };
        integerType = random.nextInt(5) == 0;
        bitPositions = new int[3 + random.nextInt(4)];
        for (int i = 0; i < bitPositions.length; i++) {
          bitPositions[i] = random.nextInt(44);
        }
        counter = random.nextInt(1 << 16);
      }
    }

    private Phase newPhase() {
      return new Phase();
    }

    private Op nextMutation(final Phase phase) {
      final double roll = random.nextDouble();
      if (!live.isEmpty() && roll < phase.removeShare) {
        final long[] victim = live.remove(random.nextInt(live.size()));
        phase.hotSet.remove(victim);
        return victim[4] > 1
            ? Op.removeMany(victim[0], victim[1], victim[2], victim[3], (int) victim[4])
            : Op.remove(victim[0], victim[1], victim[2], victim[3]);
      }
      if (!live.isEmpty() && roll < phase.removeShare + phase.revisitShare) {
        return revisit(phase);
      }
      final long[] key = freshKey(phase);
      if (phase.bulkPostings) {
        final int count = 100 + random.nextInt(2_400);
        final long[] tuple = {key[0], key[1], key[2], bulkBase(phase, count), count};
        live.add(tuple);
        if (phase.hotSet.size() < 4) {
          phase.hotSet.add(tuple);
        }
        return Op.putMany(tuple[0], tuple[1], tuple[2], tuple[3], count);
      }
      final long[] tuple = {key[0], key[1], key[2], kind == Kind.PROJECTION
          ? 0L
          : nodeKey(phase), 1L};
      live.add(tuple);
      if (phase.hotSet.size() < 4) {
        phase.hotSet.add(tuple);
      }
      return Op.put(tuple[0], tuple[1], tuple[2], tuple[3]);
    }

    /** The first of {@code count} consecutive node keys inside one chunk of the phase's layout. */
    private long bulkBase(final Phase phase, final int count) {
      final long chunk = phase.denseChunk
          ? phase.chunkBase
          : phase.chunkBase + random.nextInt(phase.chunkSpan);
      return (chunk << 16) | random.nextInt((1 << 16) - count * BULK_STRIDE);
    }

    private Op revisit(final Phase phase) {
      final long[] source = !phase.hotSet.isEmpty() && random.nextInt(3) != 0
          ? phase.hotSet.get(random.nextInt(phase.hotSet.size()))
          : live.get(random.nextInt(live.size()));
      if (kind == Kind.PROJECTION) {
        // Replace the blob: a new seed and usually a new size class, so the slot grows or shrinks.
        final long[] tuple = {source[0], random.nextLong(), blobLength(phase), 0L, 1L};
        live.removeIf(candidate -> candidate[0] == source[0]);
        live.add(tuple);
        return Op.put(tuple[0], tuple[1], tuple[2], tuple[3]);
      }
      if (phase.bulkPostings) {
        // A run of node keys in another chunk of the same key: a second slot of several KiB.
        final int count = 50 + random.nextInt(1_000);
        final long[] tuple = {source[0], source[1], source[2], bulkBase(phase, count), count};
        live.add(tuple);
        return Op.putMany(tuple[0], tuple[1], tuple[2], tuple[3], count);
      }
      // Another node key in the same chunk grows that chunk's bitmap in place.
      final long nodeKey = (source[3] & ~0xFFFFL) | random.nextInt(1 << 16);
      final long[] tuple = {source[0], source[1], source[2], nodeKey, 1L};
      live.add(tuple);
      return Op.put(tuple[0], tuple[1], tuple[2], tuple[3]);
    }

    private long nodeKey(final Phase phase) {
      final long chunk = phase.denseChunk
          ? phase.chunkBase
          : phase.chunkBase + random.nextInt(phase.chunkSpan);
      return (chunk << 16) | random.nextInt(1 << 16);
    }

    private long[] freshKey(final Phase phase) {
      return switch (kind) {
        case VALIDTIME -> validTimeKey(phase);
        case CAS -> casKey(phase);
        case NAME -> nameKey(phase);
        case PATH -> new long[] {pathKey(phase), 0L, 0L};
        case PROJECTION -> projectionKey(phase);
      };
    }

    private long[] validTimeKey(final Phase phase) {
      final long store = phase.bothStores
          ? random.nextInt(2)
          : phase.store;
      final long fork;
      final long endpoint;
      switch (phase.shape) {
        case SPARSE_PARTIAL -> {
          // The fork's most significant serialized byte is one of a few patterns; endpoints stay small.
          fork = (phase.alphabet[random.nextInt(phase.alphabet.length)] << 56) ^ Long.MIN_VALUE;
          endpoint = random.nextInt(phase.endpointSpan);
        }
        case PREFIX_TRAILING -> {
          fork = phase.base;
          endpoint = (phase.counter & ~0xFFFFL) | random.nextInt(random.nextBoolean()
              ? 256
              : 1 << 16);
        }
        case ASCENDING -> {
          fork = phase.base;
          endpoint = phase.counter++;
        }
        case DESCENDING -> {
          fork = phase.base;
          endpoint = phase.counter--;
        }
        case CLUSTERED -> {
          fork = (phase.alphabet[random.nextInt(phase.alphabet.length)] << 56) ^ Long.MIN_VALUE;
          endpoint = phase.centers[random.nextInt(phase.centers.length)] + random.nextInt(33) - 16;
        }
        default -> {
          fork = random.nextLong();
          endpoint = random.nextLong();
        }
      }
      return new long[] {store, fork, endpoint};
    }

    private long[] casKey(final Phase phase) {
      final long[] stringKey = stringKey(phase);
      final long typeCode = phase.integerType
          ? 1L
          : 0L;
      final long pathNodeKey = switch (random.nextInt(4)) {
        case 0 -> 1L;
        case 1 -> 2L;
        case 2 -> 3L;
        default -> 7L;
      };
      return new long[] {stringKey[0], stringKey[1] | (typeCode << 16), pathNodeKey};
    }

    private long[] nameKey(final Phase phase) {
      final long[] stringKey = stringKey(phase);
      return new long[] {stringKey[0], stringKey[1], 0L};
    }

    /** {@code {payload, prefixLength | width << 8}} for {@link #patternedString}. */
    private long[] stringKey(final Phase phase) {
      final long payload;
      final int prefixLength;
      final int width;
      switch (phase.shape) {
        case SPARSE_PARTIAL -> {
          // Sixteen hex digits that differ only at a few nibble positions.
          long value = phase.base;
          for (int i = 0; i < 3; i++) {
            final int nibble = phase.bitPositions[i % phase.bitPositions.length] & 0xF;
            value &= ~(0xFL << (nibble * 4));
            value |= (phase.alphabet[random.nextInt(phase.alphabet.length)] & 0xFL) << (nibble * 4);
          }
          payload = value;
          prefixLength = random.nextInt(8);
          width = 16;
        }
        case PREFIX_TRAILING -> {
          payload = random.nextInt(random.nextBoolean()
              ? 16
              : 256);
          prefixLength = 60 + random.nextInt(170);
          width = payload < 16
              ? 1
              : 2;
        }
        case ASCENDING -> {
          payload = phase.counter++;
          prefixLength = random.nextInt(4);
          width = 8;
        }
        case DESCENDING -> {
          payload = phase.counter--;
          prefixLength = random.nextInt(4);
          width = 8;
        }
        case CLUSTERED -> {
          payload = phase.centers[random.nextInt(phase.centers.length)] + random.nextInt(33) - 16;
          prefixLength = random.nextInt(4);
          width = 8;
        }
        default -> {
          payload = random.nextLong();
          prefixLength = random.nextInt(32);
          width = 1 + random.nextInt(16);
        }
      }
      return new long[] {payload, prefixLength | ((long) width << 8)};
    }

    private long pathKey(final Phase phase) {
      return switch (phase.shape) {
        case SPARSE_PARTIAL -> {
          long value = 0L;
          for (final int bit : phase.bitPositions) {
            if (random.nextBoolean()) {
              value |= 1L << bit;
            }
          }
          yield value | random.nextInt(4);
        }
        case PREFIX_TRAILING -> (phase.base & ~0xFFL) | random.nextInt(256);
        case ASCENDING -> phase.counter++;
        case DESCENDING -> phase.counter--;
        case CLUSTERED -> phase.centers[random.nextInt(phase.centers.length)] + random.nextInt(33) - 16;
        default -> random.nextLong();
      };
    }

    private long[] projectionKey(final Phase phase) {
      final long rowGroup = switch (phase.shape) {
        case SPARSE_PARTIAL -> {
          long value = 0L;
          for (final int bit : phase.bitPositions) {
            if (random.nextBoolean()) {
              value |= 1L << (bit % 23);
            }
          }
          yield 1L + value;
        }
        case PREFIX_TRAILING -> 1L + ((phase.base & 0x7FFF00L) | random.nextInt(256));
        case ASCENDING -> 1L + (phase.counter++ & 0x7FFFFFL);
        case DESCENDING -> 1L + (Math.abs(phase.counter--) & 0x7FFFFFL);
        case CLUSTERED -> 1L + Math.max(0L, phase.centers[random.nextInt(phase.centers.length)] + random.nextInt(33) - 16);
        default -> 1L + random.nextInt(1 << 24);
      };
      final long slotKind = random.nextInt(9);
      final long slotKey = (rowGroup << 16) | slotKind;
      return new long[] {slotKey, random.nextLong(), blobLength(phase)};
    }

    private long blobLength(final Phase phase) {
      final double roll = random.nextDouble();
      if (phase.denseChunk && roll < 0.6) {
        return 513 + random.nextInt(2_000);
      }
      if (roll < 0.5) {
        return 1 + random.nextInt(64);
      }
      if (roll < 0.85) {
        return 200 + random.nextInt(313);
      }
      return 513 + random.nextInt(2_000);
    }
  }
}

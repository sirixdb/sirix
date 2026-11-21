package io.sirix.benchmark;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTLongBulkIndexLoader;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.io.StorageType;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * What a HOT insert costs when it has to read a sibling leaf that only the page cache owns.
 *
 * <p>
 * A put that extends its leaf's key range compares the key with the facing extreme of the
 * neighbouring leaf, to prove that the two ranges stay disjoint. While that neighbour is untouched
 * in the transaction it belongs to the page cache, which may evict it under the read, so the writer
 * checks after the read that the leaf was not evicted. Every put of the three {@code append}
 * benchmarks takes that path; {@link #insertsInsideLoggedLeaves} is their control, the same puts
 * inside the leaf's range, where no sibling is read.
 * </p>
 *
 * <p>
 * The committed index holds groups of {@value #GROUP_KEYS} keys. Two groups do not fit one leaf, so
 * each leaf holds exactly one and has room for 255 more entries. Puts go into the even groups only,
 * round robin, so every neighbour stays a committed leaf. Each shot does the same work in every
 * fork of every build, which is what lets two builds be compared shot for shot:
 * </p>
 * <ul>
 * <li>{@link PerShotTransaction}: a shot opens a transaction, creates its writer and puts
 * {@value #BATCH} keys; the transaction is rolled back, or its commit reverted, before the next.
 * Its figures include the copy-on-write of {@value #BATCH_LEAVES} leaves and one writer per
 * shot.</li>
 * <li>{@link OneTransaction}: one transaction and one writer for the whole trial, and a shot puts
 * {@value #STEADY_PUTS_PER_LEAF} keys into every even group. Nothing but puts happens inside a
 * shot, so its allocation is the allocation of puts.</li>
 * </ul>
 *
 * <p>
 * The put path reaches its compiled plateau only after about 2.5 million puts, a hundred shots of
 * the per-shot benchmarks. A shorter warm-up compares two builds in the middle of compilation,
 * where the one with more methods to compile looks slower; keep the {@value #WARMUP_SHOTS} warm-up
 * shots.
 * </p>
 *
 * <p>
 * Gradle overrides these annotations with defaults of two warm-up and three measurement iterations,
 * below that plateau. Set both iteration properties explicitly. One Gradle invocation applies one
 * schedule to every selected benchmark, so run the steady benchmark separately:
 *
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh \
 *   -Pjmh.includes='HotWriterSiblingReadBenchmark.(appendsBesideCommittedLeaves.*|insertsInsideLoggedLeaves)$' \
 *   -Pjmh.warmupIterations=120 -Pjmh.iterations=60 -Pjmh.profilers=gc
 * ./gradlew :sirix-benchmarks:jmh \
 *   -Pjmh.includes='HotWriterSiblingReadBenchmark.steadyAppendsBesideCommittedLeaves$' \
 *   -Pjmh.warmupIterations=60 -Pjmh.iterations=60 -Pjmh.profilers=gc
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = HotWriterSiblingReadBenchmark.WARMUP_SHOTS)
@Measurement(iterations = 60)
@Fork(value = 1,
    jvmArgs = {"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--enable-preview", "-Xms2g",
        "-Xmx2g"})
public class HotWriterSiblingReadBenchmark {

  /** Warm-up shots of the per-shot benchmarks; see the class comment. */
  static final int WARMUP_SHOTS = 120;

  private static final String RESOURCE = "hot-writer-sibling-read";
  private static final int INDEX_NUMBER = 0;

  /** More than half a leaf: two groups never share one. */
  static final int GROUP_KEYS = 257;

  /** Entries a leaf holds; a group leaves {@code LEAF_ENTRIES - GROUP_KEYS} of them free. */
  private static final int LEAF_ENTRIES = 512;

  /** Groups of the per-shot index: a power of two, so the bulk-built trie splits between groups. */
  private static final int BATCH_GROUPS = 256;

  /** Leaves a per-shot batch puts into. */
  static final int BATCH_LEAVES = BATCH_GROUPS / 2;

  /** New keys a per-shot batch puts into each even group. */
  private static final int BATCH_PUTS_PER_LEAF = 200;

  /** Puts per shot of {@link PerShotTransaction}. */
  static final int BATCH = BATCH_LEAVES * BATCH_PUTS_PER_LEAF;

  /**
   * Groups of the one-transaction index. Four thousand of them are written per shot, which is what
   * makes a shot long enough to measure while {@link #STEADY_PUTS_PER_LEAF} stays small enough to
   * leave room for the many shots a fork needs (see {@link #STEADY_SHOTS}).
   */
  private static final int STEADY_GROUPS = 8192;

  /**
   * New keys one shot of {@link OneTransaction} puts into each even group: {@code STEADY_GROUPS / 2}
   * times as many puts per shot, about eight milliseconds. Shorter shots mostly measure how fast the
   * core's clock comes back up after the pause between two shots.
   */
  static final int STEADY_PUTS_PER_LEAF = 2;

  /**
   * Shots one fork of {@link OneTransaction} may run, warm-up included: the leaves have room for this
   * many. Pass {@code -wi 60 -i 60} when running this benchmark directly.
   *
   * <p>
   * The count is what makes this benchmark decide anything. With the leaves holding room for 15
   * shots, a fork measured 12 of them after 3, and in 14 of 24 pairs of one gate a fork changed level
   * inside that window -- one ran four shots at about 15 ms and then seven at about 25 ms, its paired
   * fork went from 25 ms down to 14.4 ms -- which put the arm's 95 % interval at +-4.9 %, five times
   * the one percent it was tested against. Two keys per group per shot leave room for
   * {@value #STEADY_SHOTS}, so the window can start where the other three benchmarks start.
   * </p>
   */
  static final int STEADY_SHOTS = (LEAF_ENTRIES - GROUP_KEYS) / STEADY_PUTS_PER_LEAF;

  static final int STEADY_ITERATIONS = 60;

  /**
   * Complete put schedules {@link OneTransaction} runs and rolls back before its measured transaction
   * begins. A fork's own warm-up shots are not enough for the JIT, which needs about 2.5 million
   * puts; three complete schedules are about three million.
   */
  private static final int STEADY_WARM_ROUNDS = 3;

  /** One index, bulk-built and committed as revision 1, and its open session. */
  abstract static class CommittedIndex {
    Path databasePath;
    Database<JsonResourceSession> database;
    JsonResourceSession session;

    void build(final int groups) throws IOException {
      // Memory-backed when the host offers it: a commit then measures the commit, not the disk.
      final Path shm = Path.of("/dev/shm");
      databasePath = Files.isDirectory(shm) && Files.isWritable(shm)
          ? Files.createTempDirectory(shm, "sirix-jmh-hot-writer")
          : Files.createTempDirectory("sirix-jmh-hot-writer");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .useDeweyIDs(false)
                                                   .build());
      session = database.beginResourceSession(RESOURCE);
      try (JsonNodeTrx load = session.beginNodeTrx()) {
        final HOTLongBulkIndexLoader loader =
            HOTLongIndexWriter.create(load.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER).createBulkLoader();
        for (int group = 0; group < groups; group++) {
          for (int ordinal = 0; ordinal < GROUP_KEYS; ordinal++) {
            loader.add(key(group, ordinal), 1L);
          }
        }
        loader.flush();
        load.commit();
      }
    }

    void remove() {
      session.close();
      database.close();
      Databases.removeDatabase(databasePath);
    }
  }

  /** A fresh transaction and a fresh writer for every shot, against the same committed index. */
  @State(Scope.Thread)
  public static class PerShotTransaction extends CommittedIndex {
    JsonNodeTrx transaction;
    HOTLongIndexWriter writer;
    private int revisionBeforeShot;

    @Setup(Level.Trial)
    public void buildCommittedIndex() throws IOException {
      build(BATCH_GROUPS);
    }

    @Setup(Level.Iteration)
    public void beginTransaction() {
      revisionBeforeShot = session.getMostRecentRevisionNumber();
      transaction = session.beginNodeTrx();
      writer = HOTLongIndexWriter.create(transaction.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);
    }

    @TearDown(Level.Iteration)
    public void discardTransaction() {
      transaction.rollback();
      transaction.close();
      if (session.getMostRecentRevisionNumber() != revisionBeforeShot) {
        // The shot committed its batch. Revert, so the next shot puts the same keys into the same index.
        try (JsonNodeTrx revert = session.beginNodeTrx()) {
          revert.revertTo(revisionBeforeShot);
          revert.commit();
        }
      }
    }

    @TearDown(Level.Trial)
    public void removeDatabase() {
      remove();
    }

    void putBatch(final int firstOrdinal, final long offsetInGroup) {
      final HOTLongIndexWriter index = writer;
      for (int ordinal = firstOrdinal; ordinal < firstOrdinal + BATCH_PUTS_PER_LEAF; ordinal++) {
        for (int group = 0; group < BATCH_GROUPS; group += 2) {
          index.indexNodeKey(key(group, ordinal) + offsetInGroup, 1L);
        }
      }
    }
  }

  /** One transaction and one writer for the whole trial; every shot appends the next few keys. */
  @State(Scope.Thread)
  public static class OneTransaction extends CommittedIndex {
    private JsonNodeTrx transaction;
    HOTLongIndexWriter writer;
    int nextOrdinal;

    @Setup(Level.Trial)
    public void buildCommittedIndexAndBegin() throws IOException {
      build(STEADY_GROUPS);
      for (int round = 0; round < STEADY_WARM_ROUNDS; round++) {
        begin();
        for (int shot = 0; shot < STEADY_SHOTS; shot++) {
          putShot();
        }
        transaction.rollback();
        transaction.close();
      }
      begin();
    }

    private void begin() {
      transaction = session.beginNodeTrx();
      writer = HOTLongIndexWriter.create(transaction.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);
      nextOrdinal = GROUP_KEYS;
    }

    void putShot() {
      final HOTLongIndexWriter index = writer;
      final int first = nextOrdinal;
      if (first + STEADY_PUTS_PER_LEAF > LEAF_ENTRIES) {
        throw new IllegalStateException(
            "the leaves are full: run at most " + STEADY_SHOTS + " shots per fork, warm-up included");
      }
      for (int ordinal = first; ordinal < first + STEADY_PUTS_PER_LEAF; ordinal++) {
        for (int group = 0; group < STEADY_GROUPS; group += 2) {
          index.indexNodeKey(key(group, ordinal), 1L);
        }
      }
      nextOrdinal = first + STEADY_PUTS_PER_LEAF;
    }

    @TearDown(Level.Trial)
    public void rollBackAndRemove() {
      transaction.rollback();
      transaction.close();
      remove();
    }
  }

  /** Every put extends its leaf's range upwards and reads the first key of a committed neighbour. */
  @Benchmark
  public void appendsBesideCommittedLeaves(final PerShotTransaction state) {
    state.putBatch(GROUP_KEYS, 0L);
  }

  /** {@link #appendsBesideCommittedLeaves}, then the commit of the batch. */
  @Benchmark
  public void appendsBesideCommittedLeavesThenCommit(final PerShotTransaction state) {
    state.putBatch(GROUP_KEYS, 0L);
    state.transaction.commit();
  }

  /** The control: the same number of puts, each strictly inside its leaf's range. */
  @Benchmark
  public void insertsInsideLoggedLeaves(final PerShotTransaction state) {
    state.putBatch(0, 1L);
  }

  /**
   * {@link #appendsBesideCommittedLeaves} without anything else in the shot: the leaves are already
   * the transaction's and the writer exists, so a shot is {@code STEADY_GROUPS / 2 *
   * STEADY_PUTS_PER_LEAF} puts that each read one committed neighbour.
   */
  @Benchmark
  @Warmup(iterations = STEADY_ITERATIONS)
  @Measurement(iterations = STEADY_ITERATIONS)
  public void steadyAppendsBesideCommittedLeaves(final OneTransaction state) {
    state.putShot();
  }

  /** Groups differ above bit 32, the keys of one group between bits 16 and 32. */
  private static long key(final int group, final int ordinal) {
    return ((long) group << 32) | ((long) ordinal << 16);
  }
}

package io.sirix.query.budget;

import com.sun.management.ThreadMXBean;
import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.io.StorageType;
import io.sirix.index.interval.PostingMask;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.function.jn.temporal.ValidTimeRunProbe;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Isolated
final class ValidTimeRunWorkBudgetTest {
  private static final int ROWS = 20_000;
  private static final WorkCapture WORK =
      WorkCapture.of(EngineWorkCounters.INDEX_ROOT_LOADS, EngineWorkCounters.VALID_TIME_INTERVAL_REFS,
          EngineWorkCounters.VALID_TIME_POSTING_LOOKUPS, EngineWorkCounters.VALID_TIME_POSTING_CHUNKS,
          EngineWorkCounters.VALID_TIME_MASK_HITS, QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void sparseSlicesVisitOnlyTheirOccupiedWords() throws Exception {
    final NodeReferences references = NodeReferences.ofSortedArray(new long[] {7});
    final PostingMask membership = PostingMask.from(references);
    final PostingMask verification = PostingMask.from(null);
    final WorkCapture words =
        WorkCapture.of(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, QueryWorkCounters.VALID_TIME_CANDIDATE_BITMAPS);
    for (final int chunks : new int[] {16, 1000}) {
      final var capture = words.call(() -> ValidTimeRunProbe.select(chunks, references, membership, verification));
      capture.work()
             .assertExactly(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, chunks,
                 "one selected key per chunk must inspect only its occupied word, without full bitmap scans");
      capture.work()
             .assertZero(QueryWorkCounters.VALID_TIME_CANDIDATE_BITMAPS,
                 "sparse chunks must not allocate a dense candidate bitmap");
      final long[] expected = new long[chunks];
      for (int chunk = 0; chunk < chunks; chunk++) {
        expected[chunk] = ((long) chunk << 16) | 7;
      }
      assertArrayEquals(expected, capture.result());
    }
    final long[] denseKeys = new long[65536];
    for (int key = 0; key < denseKeys.length; key++) {
      denseKeys[key] = key;
    }
    final NodeReferences dense = NodeReferences.ofSortedArray(denseKeys);
    final PostingMask denseMembership = PostingMask.from(dense);
    final var capture = words.call(() -> ValidTimeRunProbe.select(1, dense, denseMembership, verification));
    capture.work()
           .assertExactly(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, 1024,
               "the dense positive control must inspect every occupied word once");
    capture.work()
           .assertExactly(QueryWorkCounters.VALID_TIME_CANDIDATE_BITMAPS, 1,
               "the dense positive control must retain one candidate bitmap");
    assertArrayEquals(denseKeys, capture.result());
  }

  @Test
  void mergingSparseRunsVisitsOnlyOccupiedWords() throws Exception {
    final NodeReferences references = NodeReferences.ofSortedArray(new long[] {7, 65535});
    final PostingMask membership = PostingMask.from(references);
    final PostingMask verification = PostingMask.from(null);
    final int chunks = 1000;
    final var capture =
        WorkCapture.of(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS)
                   .call(() -> ValidTimeRunProbe.select(chunks, references, membership, verification, 2));
    capture.work()
           .assertBetween(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, chunks * 4, chunks * 6,
               "intersect and merge sparse runs without scanning unoccupied bitmap words");
    final long[] expected = new long[chunks * 2];
    for (int chunk = 0; chunk < chunks; chunk++) {
      expected[chunk * 2] = ((long) chunk << 16) | 7;
      expected[chunk * 2 + 1] = ((long) chunk << 16) | 65535;
    }
    assertArrayEquals(expected, capture.result());
  }

  @Test
  void adaptiveContainersPromoteOnlyAtUniqueMatchThreshold() throws Exception {
    final WorkCapture work = WorkCapture.of(QueryWorkCounters.VALID_TIME_CANDIDATE_BITMAPS);
    for (final int matches : new int[] {1023, 1024}) {
      final long[] expected = new long[matches];
      for (int key = 0; key < matches; key++) {
        expected[key] = key;
      }
      final NodeReferences references = NodeReferences.ofSortedArray(expected);
      final PostingMask membership = PostingMask.from(references);
      final PostingMask verification = PostingMask.from(null);
      // Repeating the same run crosses the cumulative count, but never its unique cardinality.
      final var capture = work.call(() -> ValidTimeRunProbe.select(1, references, membership, verification, 3));
      assertArrayEquals(expected, capture.result());
      capture.work()
             .assertExactly(QueryWorkCounters.VALID_TIME_CANDIDATE_BITMAPS, matches == 1024
                 ? 1
                 : 0, "promote at 1024 unique matches, never from duplicate references");
    }
  }

  @Test
  void sparseSliceAllocationScalesWithMatches() {
    final var threads = ManagementFactory.getThreadMXBean();
    assumeTrue(threads instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) threads;
    assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
    final NodeReferences references = NodeReferences.ofSortedArray(new long[] {7});
    final PostingMask membership = PostingMask.from(references);
    final PostingMask verification = PostingMask.from(null);
    for (int warmup = 0; warmup < 32; warmup++) {
      assertEquals(1000, ValidTimeRunProbe.select(1000, references, membership, verification).length);
    }
    for (final int chunks : new int[] {16, 1000}) {
      final long start = bean.getCurrentThreadAllocatedBytes();
      final long[] keys = ValidTimeRunProbe.select(chunks, references, membership, verification);
      final long allocated = bean.getCurrentThreadAllocatedBytes() - start;
      assertEquals(chunks, keys.length);
      assertTrue(allocated <= 32768L + chunks * 512L, "candidate allocation bytes=" + allocated);
    }
  }

  @Test
  void oneStabAcrossChunksAndNoRootReadOnRepeatedTransactions() throws Exception {
    final StringBuilder json = new StringBuilder(ROWS * 75).append('[');
    for (int row = 0; row < ROWS; row++) {
      if (row != 0) {
        json.append(',');
      }
      json.append("{\"vf\":\"2020-01-01T00:00:00Z\",\"vt\":\"2030-01-01T00:00:00Z\",\"id\":").append(row).append('}');
    }
    json.append(']');
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("budget")));
    try (var database = Databases.openJsonDatabase(directory.resolve("budget"))) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('budget','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final var collection = store.lookup("budget");
      final Instant point = Instant.parse("2024-01-01T00:00:00Z");
      final long[][] keys = new long[1][];
      final WorkReport cold =
          WORK.run(() -> keys[0] = ValidTimeIntervalIndex.keys(collection.getDocument("rows"), point, true));
      cold.assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, ROWS, "each interval must be stabbed once")
          .assertBetween(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, 1, ROWS,
              "dense run assembly must inspect only occupied words")
          .assertAtLeast(EngineWorkCounters.INDEX_ROOT_LOADS, 1, "the cold witness must read index metadata");
      assertEquals(ROWS, keys[0].length);
      assertTrue(keys[0][keys[0].length - 1] > 65535, "the fixture must cross the posting chunk boundary");
      for (int index = 1; index < keys[0].length; index++) {
        assertTrue(keys[0][index - 1] < keys[0][index], "run emission must be ordered and duplicate-free");
      }
      for (int repetition = 0; repetition < 3; repetition++) {
        final WorkReport warm = WORK.run(
            () -> assertArrayEquals(keys[0], ValidTimeIntervalIndex.keys(collection.getDocument("rows"), point, true)));
        warm.assertZero(EngineWorkCounters.INDEX_ROOT_LOADS,
            "a repeated stab in a fresh transaction must reuse index roots")
            .assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, ROWS,
                "repeat stabs must still make only one pass")
            .assertBetween(QueryWorkCounters.VALID_TIME_CANDIDATE_WORDS, 1, ROWS,
                "cached dense masks must still emit runs without rescanning full bitmaps")
            .assertZero(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS,
                "warm stabs must not decode companion chunks again")
            .assertAtLeast(EngineWorkCounters.VALID_TIME_MASK_HITS, 2, "both companion masks must be reused");
      }
    }
  }
}

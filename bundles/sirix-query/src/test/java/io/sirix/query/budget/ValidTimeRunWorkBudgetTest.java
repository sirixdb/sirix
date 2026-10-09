package io.sirix.query.budget;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ValidTimeRunWorkBudgetTest {
  private static final int ROWS = 20_000;
  private static final WorkCapture WORK = WorkCapture.of(EngineWorkCounters.INDEX_ROOT_LOADS,
      EngineWorkCounters.VALID_TIME_INTERVAL_REFS, EngineWorkCounters.VALID_TIME_POSTING_LOOKUPS,
      EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, EngineWorkCounters.VALID_TIME_MASK_HITS);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
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
            .assertZero(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS,
                "warm stabs must not decode companion chunks again")
            .assertAtLeast(EngineWorkCounters.VALID_TIME_MASK_HITS, 2, "both companion masks must be reused");
      }
    }
  }
}

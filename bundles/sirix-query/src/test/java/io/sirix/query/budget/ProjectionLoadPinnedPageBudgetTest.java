/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.page.IntentLogEpochProbe;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.io.StorageType;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Work budget for a projection bulk load: the pages its intent log leaves pinned stay bounded
 * however long the load runs, on every file backend.
 *
 * <p>
 * A projection declared at load time writes its HOT trie pages inside the load transaction. They
 * cannot be flushed like record pages, so each epoch moves them into the intent log's pinned
 * region, where every one holds an off-heap frame, and the only way out before the final commit is
 * the pre-commit spill. While that spill runs, the region holds a few dozen pages; when it does
 * not, the region grows by one page per filled leaf for as long as the load lasts, and a large load
 * exhausts the arena and dies.
 *
 * <p>
 * Nothing about a small load's <em>result</em> changes either way, which is why a suite that checks
 * results cannot see it. Measured on this fixture (parallel bulk load, 200 000 records), with the
 * spill gated on the writer's ability to write ahead of the commit, as it is, against gated on its
 * ability to <em>reclaim</em> an aborted tail, as it once was:
 *
 * <pre>
 *                      pinned peak   spilled pages
 * FILE_CHANNEL              62           537         either gate (preallocated commits reclaim)
 * MEMORY_MAPPED             62           537         gated on writing ahead
 * MEMORY_MAPPED            557             0         gated on reclaiming: nothing ever spills
 * </pre>
 *
 * The healthy peak is 53 at 50 000 records and 62 at 200 000; the broken one is 171 and 557.
 * Bounded against linear is what the budget separates, with room on both sides.
 */
@Isolated
final class ProjectionLoadPinnedPageBudgetTest {

  private static final String DATABASE = "load-budget";

  private static final String RESOURCE = "events.jn";

  /** Enough rows for some sixty epochs: a bound over a handful of epochs would prove nothing. */
  private static final int RECORDS = 200_000;

  @TempDir
  private Path root;

  @BeforeEach
  @AfterEach
  void clearProjectionState() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}: a projection bulk load spills its pinned trie pages and keeps them bounded")
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void aProjectionBulkLoadKeepsItsPinnedPagesBounded(final StorageType storageType) throws Exception {
    final IntentLogEpochProbe intentLog = new IntentLogEpochProbe();
    final WorkReport load =
        WorkCapture.of(EngineWorkCounters.INTENT_LOG).with(intentLog).run(() -> bulkLoad(storageType));

    load.assertAtLeast(intentLog.epochs(), 30,
        "the fixture no longer rotates enough intent-log epochs for a bound on the pinned region to mean anything; "
            + "raise RECORDS rather than the bound");
    // Zero here is the whole defect: a backend whose writer is asked the wrong capability question
    // refuses every spill at its gate, silently, and the load only fails once it is large enough.
    load.assertAtLeast(intentLog.spilledPages(), 1,
        "the pre-commit spill never drained a pinned trie page on " + storageType + ", so a projection load holds "
            + "every HOT leaf it writes until the final commit (the 100M load died of exactly this on "
            + "MEMORY_MAPPED, where the spill was gated on reclaimable instead of uncommitted writes)");
    // The floor proves pages were pinned at all, so the ceiling is not satisfied by an idle log.
    load.assertBetween(intentLog.pinnedPagesPeak(), 1, 160,
        "the intent log's pinned region grows with the load instead of staying bounded: measured 62 with the "
            + "spill running and 557 without it on this fixture, and every pinned page holds an off-heap frame");

    assertEquals(RECORDS, projectedRowsAfterColdReopen(),
        "the load must publish every row, spilled leaves included: the budget may only bound work, never buy "
            + "it with a wrong result");
  }

  private void bulkLoad(final StorageType storageType) {
    try (
        BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                 .location(root)
                                                 .storageType(storageType)
                                                 .hashType(HashType.NONE)
                                                 .storeNodeHistory(false)
                                                 .buildPathSummary(true)
                                                 .buildPathStatistics(false)
                                                 .build();
        StringReader input = new StringReader(dataset())) {
      store.createParallel(DATABASE, RESOURCE, input,
          new ProjectionSpec("/[]", List.of("/[]/id", "/[]/tag"), List.of("long", "string")));
    }
  }

  /** Rows the projection's descriptors publish, read through a cache-cold reopen. */
  private long projectedRowsAfterColdReopen() {
    clearProjectionState();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(root.resolve(DATABASE));
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return ProjectionIndexCatalog.countRowsFromDescriptors(session,
          session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
          new String[] {"[]"});
    }
  }

  /**
   * Built per load, not kept in a constant: a dozen megabytes would stay reachable for the whole
   * suite.
   */
  private static String dataset() {
    final StringBuilder json = new StringBuilder(RECORDS * 56).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i != 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(i)
          .append(",\"tag\":\"t")
          .append(i % 1000)
          .append("\",\"v\":")
          .append(i * 31L % 9973)
          .append(",\"s\":\"x")
          .append(i)
          .append("\"}");
    }
    return json.append(']').toString();
  }
}

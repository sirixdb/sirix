/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HOT copy-on-write write path must actually POPULATE the fragment cache.
 *
 * <p>
 * Under the default SLIDING_SNAPSHOT with {@code maxNumberOfRevisionsToRestore = 3} the
 * carry-forward fires from about a leaf's third commit onward, and it loads the versioning window
 * on every copy-on-write. Those chain fragments used to be read straight off the page reader and
 * closed again, so the next commit re-read them; a populated fragment cache after several commits
 * is the observable difference between that and reusing them.
 * </p>
 *
 * <p>
 * This asserts the cache is populated rather than a specific hit count: the number of reads is a
 * function of chain length, leaf splits and eviction timing, so pinning it would make the test a
 * change-detector rather than a regression guard.
 * </p>
 *
 * <p>
 * Scope, stated honestly: {@code loadHOTPageFragments} is shared by the write path and by versioned
 * HOT reads, so a populated cache proves the fragment cache is reached by HOT chain traversal for
 * THIS resource — not that the write path specifically reached it. The cache is cleared immediately
 * before the commit loop and the assertion matches only this resource's database/resource ids, so
 * residue from another test class in the same fork cannot make it pass.
 * </p>
 */
public final class HOTFragmentCachePopulationTest {

  /** The global buffer manager whose sweepers are parked for the duration of each test. */
  private BufferManagerImpl bufferManager;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    // Park the global ClockSweepers for the duration of the test. They sweep every 100ms and
    // reclaim precisely what this test asserts is resident: a fragment that is neither hot nor
    // guarded is evicted on the sweep after next, so the gap between the last commit and the
    // assertion is enough to empty the cache on a slow runner — which is how this test failed
    // intermittently on the Windows lane. What is under test is that HOT chain traversal
    // POPULATES the cache; whether a background reclaimer has since drained it is a different
    // property, and letting it race here only ever produced false failures.
    //
    // Fetch the manager before stopping: the first call is what constructs it and starts the
    // sweepers, so stopping before that would park nothing and the threads would start after.
    final BufferManager globalBufferManager = Databases.getGlobalBufferManager();
    assertInstanceOf(BufferManagerImpl.class, globalBufferManager,
        "the global buffer manager must expose sweeper control for this test to be deterministic");
    bufferManager = (BufferManagerImpl) globalBufferManager;
    bufferManager.stopClockSweepers();
  }

  @AfterEach
  void tearDown() {
    try {
      JsonTestHelper.deleteEverything();
      // The global buffer manager outlives the database, so leave no HOT residue for the next class.
      bufferManager.clearAllCaches();
    } finally {
      // Restart unconditionally: the sweepers are global daemons shared with every other test in
      // this fork, so a failure here must not leave eviction disabled for the rest of the run.
      bufferManager.startClockSweepers(Databases.getGlobalEpochTracker());
    }
  }

  @Test
  void slidingSnapshotCommitsPopulateTheHotFragmentCache() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final long databaseId;
    final long resourceId;
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      databaseId = trx.getStorageEngineWriter().getDatabaseId();
      resourceId = trx.getStorageEngineWriter().getResourceId();
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"items\":[{\"k0\":0}]}"));
      trx.commit();
    }

    // Start from an empty cache so nothing that ran before this point can satisfy the assertion.
    final Cache<PageReference, HOTLeafPage> fragmentCache =
        Databases.getGlobalBufferManager().getHOTLeafFragmentCache();
    fragmentCache.clear();

    // Enough commits on the same index leaf that the versioning window fills and the carry-forward
    // starts loading the chain on every copy-on-write.
    for (int i = 1; i <= 6; i++) {
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToLastChild();
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"k" + i + "\":" + i + "}"));
        trx.commit();
      }
    }

    final boolean cachedForThisResource =
        fragmentCache.asMap()
                     .keySet()
                     .stream()
                     .anyMatch(key -> key.getDatabaseId() == databaseId && key.getResourceId() == resourceId);
    assertTrue(cachedForThisResource,
        "HOT chain traversal must retain this resource's fragments for the next commit instead of "
            + "re-reading them uncached on every copy-on-write");
  }
}

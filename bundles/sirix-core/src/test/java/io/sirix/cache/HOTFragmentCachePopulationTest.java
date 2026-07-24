/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.name.NameIndexListenerFactory;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HOT copy-on-write write path must actually POPULATE the fragment cache.
 *
 * <p>Under the default SLIDING_SNAPSHOT with {@code maxNumberOfRevisionsToRestore = 3} the
 * carry-forward fires from about a leaf's third commit onward, and it loads the versioning window on
 * every copy-on-write. Those chain fragments used to be read straight off the page reader and closed
 * again, so the next commit re-read them; a populated fragment cache after several commits is the
 * observable difference between that and reusing them.</p>
 *
 * <p>This asserts the cache is populated rather than a specific hit count: the number of reads is a
 * function of chain length, leaf splits and eviction timing, so pinning it would make the test a
 * change-detector rather than a regression guard.</p>
 */
public final class HOTFragmentCachePopulationTest {

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOT() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void slidingSnapshotCommitsPopulateTheHotFragmentCache() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this test");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"k0\":0}]}"));
      trx.commit();
    }

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

    final Cache<PageReference, HOTLeafPage> fragmentCache =
        Databases.getGlobalBufferManager().getHOTLeafFragmentCache();
    assertTrue(!fragmentCache.asMap().isEmpty(),
        "the HOT write path must retain its chain fragments for the next commit instead of "
            + "re-reading them uncached on every copy-on-write");
  }
}

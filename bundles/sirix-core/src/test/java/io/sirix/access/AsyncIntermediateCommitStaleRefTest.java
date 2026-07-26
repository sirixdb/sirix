package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression tests for #1077: silent record loss under {@code KEEP_OPEN_ASYNC_FLUSH} intermediate
 * commits (and the stale-reference resolution the final commit depends on).
 *
 * <p>Monotonic inserts at the head of an array make every epoch (a) freeze the TIL, (b) flush the
 * frozen leaf pages in the background, and (c) copy-on-write the still-hot pages into the new
 * epoch. Three defects conspired to silently lose every record of an epoch:
 *
 * <ol>
 * <li>The write-transaction cursor cached the TIL's modified page instance keyed by page key and
 * validated it only via {@code isClosed()}. The superseded frozen instance must stay OPEN for the
 * background flush, so the cursor kept reading it for the whole epoch while writes went into the
 * CoW copy — each new node linked to a stale first child, orphaning everything inserted after the
 * epoch boundary.</li>
 * <li>The completed-disk-offsets map (Layer 3 stale-reference resolution) deleted entries on
 * first access and pruned entries older than two generations, so a stale CoW'd reference
 * dereferenced only by the final commit could resolve to nothing — the parent was serialized
 * with child key -1.</li>
 * <li>An epoch-straddling page's outdated flush could shadow its newer content: the snapshot
 * cleanup applied disk offsets and re-promoted containers through reference objects that a CoW
 * had already re-bound to newer entries.</li>
 * </ol>
 *
 * <p>Each test drives dozens of async epochs and then verifies every inserted record is present
 * in the committed revision (the sync variant guards against regressions on the synchronous
 * path).
 */
final class AsyncIntermediateCommitStaleRefTest {

  private static final int RECORD_COUNT = 20_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private void insertManyEpochsAndVerify(final int maxNodesPerEpoch, final AfterCommitState afterCommitState,
      final String resource) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(resource)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      try (final JsonResourceSession session = db.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx(maxNodesPerEpoch, afterCommitState)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < RECORD_COUNT; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("item-" + i);
        }
        wtx.commit();
      }
    }

    // Reopen from disk and traverse the sibling chain: a stale read at insert time (or a stale
    // reference at final commit) manifests as records missing from the committed revision.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(resource);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // the array
      final boolean[] seen = new boolean[RECORD_COUNT];
      int count = 0;
      if (rtx.hasFirstChild()) {
        rtx.moveToFirstChild();
        count++;
        seen[Integer.parseInt(rtx.getValue().substring("item-".length()))] = true;
        while (rtx.hasRightSibling()) {
          rtx.moveToRightSibling();
          count++;
          seen[Integer.parseInt(rtx.getValue().substring("item-".length()))] = true;
        }
      }
      if (count != RECORD_COUNT) {
        // Diagnostic: report which contiguous record ranges vanished.
        final StringBuilder missing = new StringBuilder("missing ranges: ");
        int start = -1;
        for (int i = 0; i <= RECORD_COUNT; i++) {
          final boolean present = i < RECORD_COUNT && seen[i];
          if (!present && i < RECORD_COUNT && start < 0) {
            start = i;
          } else if ((present || i == RECORD_COUNT) && start >= 0) {
            missing.append('[').append(start).append("..").append(i - 1).append("] ");
            start = -1;
          }
        }
        System.err.println(missing);
      }
      assertEquals(RECORD_COUNT, count,
          "every record inserted across the auto-commit epochs must be present in the committed revision");
    }
  }

  @Test
  @DisplayName("Records survive many async epochs (page-aligned rotation threshold)")
  void asyncAligned512() {
    insertManyEpochsAndVerify(512, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH, "async-512");
  }

  @Test
  @DisplayName("Records survive many async epochs (unaligned rotation threshold)")
  void asyncUnaligned300() {
    insertManyEpochsAndVerify(300, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH, "async-300");
  }

  @Test
  @DisplayName("Records survive fewer, larger async epochs")
  void asyncLarge2048() {
    insertManyEpochsAndVerify(2048, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH, "async-2048");
  }

  @Test
  @DisplayName("Synchronous KEEP_OPEN auto-commit path is unaffected")
  void syncKeepOpen512() {
    insertManyEpochsAndVerify(512, AfterCommitState.KEEP_OPEN, "sync-512");
  }
}

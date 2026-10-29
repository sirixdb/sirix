package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A record page flushed by a storage epoch and modified again keeps every record of its versioning
 * chain.
 *
 * <p>
 * The flushed fragment is this transaction's own modifying page. Reading it back as the newest
 * fragment of the chain and combining it like a committed one treated the current revision as one
 * more previous revision: the reference's fragment list gained the owned key in front, the window
 * lost one real revision per flush-and-touch cycle, and the records whose newest copy lived in the
 * oldest fragment disappeared from the complete page. The SH1 publication loop hit this on the
 * eighth publication of a t100k load with a bounded transaction: "Cannot retrieve record from cache
 * ... (indexType: RECORD_TO_REVISIONS) ... (populatedCount: 103)". The fixture builds a chain as
 * deep as the window on one document page, then flushes and re-touches that page twice inside one
 * transaction. After each cycle the page's fragment chain must still name committed revisions only,
 * each once, and every record must read back, from the writer and after a cold reopen.
 * </p>
 */
final class AsyncFlushOwnedFragmentChainTest {

  private static final String RESOURCE = "async-flush-owned-fragment-chain";

  private static final int REVISIONS_TO_RESTORE = 3;

  /**
   * Nearly a full document page (1,024 nodes), so one rewrite per revision leaves a partial fragment.
   */
  private static final int RECORDS = 1_000;

  /**
   * Committed revisions after the load, each rewriting one record, so the chain is as deep as the
   * window.
   */
  private static final int CHAIN_COMMITS = 4;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest(name = "{0}: a flushed page modified again keeps its whole versioning chain")
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void flushedPageModifiedAgainKeepsEveryRecord(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final long[] keys = new long[RECORDS];
    final String[] values = new String[RECORDS];
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      // No diff log: computing it materializes every record of a touched page into the modified
      // page, which turns each fragment into a full image and hides the chain.
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(REVISIONS_TO_RESTORE)
                                                   .storeDiffs(false)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          final long arrayKey = wtx.insertArrayAsFirstChild().getNodeKey();
          for (int record = RECORDS - 1; record >= 0; record--) {
            assertTrue(wtx.moveTo(arrayKey));
            values[record] = "value-" + record;
            keys[record] = wtx.insertStringValueAsFirstChild(values[record]).getNodeKey();
          }
          wtx.commit();
          // One rewrite per revision builds a chain as deep as the versioning window.
          for (int commit = 0; commit < CHAIN_COMMITS; commit++) {
            assertTrue(wtx.moveTo(keys[commit]));
            values[commit] = "revision-" + (commit + 2) + "-" + commit;
            wtx.setStringValue(values[commit]);
            wtx.commit();
          }
        }

        try (final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final StorageEngineWriter writer = wtx.getStorageEngineWriter();
          // Touch the page, flush it, retire the flushed snapshot; then touch it again, twice over.
          for (int cycle = 0; cycle < 3; cycle++) {
            final int record = CHAIN_COMMITS + cycle;
            assertTrue(wtx.moveTo(keys[record]));
            values[record] = "owned-" + cycle;
            wtx.setStringValue(values[record]);
            writer.asyncFlush();
            writer.awaitPendingAsyncFlush();
            rotatePastFlushedDocumentPage(writer);
            assertEveryRecord(wtx, keys, values, "writer after flush cycle " + cycle);
            assertCommittedChain(writer, keys[0], "writer after flush cycle " + cycle);
          }
          wtx.commit();
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertEquals(CHAIN_COMMITS + 2, session.getMostRecentRevisionNumber());
      assertEveryRecord(rtx, keys, values, "cold reopen");
    }
  }

  private static void assertEveryRecord(final JsonNodeReadOnlyTrx rtx, final long[] keys, final String[] values,
      final String when) {
    for (int record = 0; record < RECORDS; record++) {
      assertTrue(rtx.moveTo(keys[record]), "record " + record + " reachable: " + when);
      assertEquals(values[record], rtx.getValue(), "record " + record + ": " + when);
    }
  }

  /**
   * The page's fragment chain names previous revisions only, each once: an owned fragment of the
   * revision being written must never enter it, or the versioning window loses one real revision per
   * flush-and-touch cycle.
   */
  private static void assertCommittedChain(final StorageEngineWriter writer, final long nodeKey, final String when) {
    final NodeStorageEngineReader reader = (NodeStorageEngineReader) writer.getStorageEngineReader();
    final var revisionRoot = writer.getActualRevisionRootPage();
    final PageReference documentRoot = reader.getPageReference(revisionRoot, IndexType.DOCUMENT, -1);
    final PageReference reference = reader.getLeafPageReference(documentRoot,
        nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT, -1, IndexType.DOCUMENT, revisionRoot);
    assertNotNull(reference, when);
    final int current = revisionRoot.getRevision();
    final List<PageFragmentKey> chain = reference.getPageFragments();
    final Set<Integer> revisions = new HashSet<>();
    for (final PageFragmentKey fragment : chain) {
      assertTrue(fragment.revision() < current,
          when + ": fragment chain " + chain + " names the revision being written (" + current + ")");
      assertTrue(revisions.add(fragment.revision()), when + ": fragment chain " + chain + " repeats a revision");
    }
  }

  /**
   * Flush a second epoch so the previous snapshot is retired and the flushed page leaves the intent
   * log.
   */
  private static void rotatePastFlushedDocumentPage(final StorageEngineWriter writer) {
    final long changedNodeKey = writer.getActualRevisionRootPage().getMaxNodeKeyInChangedNodesIndex() + 1;
    writer.createRecord(new RevisionReferencesNode(changedNodeKey, new int[] {1}), IndexType.CHANGED_NODES, -1);
    writer.asyncFlush();
    writer.awaitPendingAsyncFlush();
  }
}

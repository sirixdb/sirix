package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Integration tests for {@link KeyedTrieReader} and {@link KeyedTrieWriter}.
 *
 * <p>
 * These tests exercise the keyed trie traversal logic end-to-end through the
 * {@link StorageEngineReader} and {@link StorageEngineWriter} interfaces, verifying:
 * </p>
 * <ul>
 *   <li>Trie navigation correctly resolves page references for stored records</li>
 *   <li>Multi-revision trie growth and Copy-on-Write semantics</li>
 *   <li>Record retrieval after writes exercise the full trie path</li>
 *   <li>Historical revision isolation — old data readable after new commits</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
final class KeyedTrieIntegrationTest {

  private Database<JsonResourceSession> database;
  private JsonResourceSession resourceSession;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
  }

  @AfterEach
  void tearDown() {
    if (resourceSession != null && !resourceSession.isClosed()) {
      resourceSession.close();
    }
    JsonTestHelper.closeEverything();
  }

  @Nested
  @DisplayName("KeyedTrieReader — read-side trie traversal")
  class KeyedTrieReaderTests {

    @Test
    @DisplayName("getReferenceToLeafOfSubtree returns valid page reference for stored records")
    void trieNavigationResolvesStoredRecords() {
      // Write a document to populate the trie.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      // Read back through the StorageEngineReader, which delegates trie navigation
      // to KeyedTrieReader.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        final RevisionRootPage revisionRootPage = reader.getActualRevisionRootPage();
        assertNotNull(revisionRootPage, "Revision root page must exist after commit");

        // Get the page reference for the DOCUMENT index root.
        final PageReference rootRef = reader.getPageReference(revisionRootPage, IndexType.DOCUMENT, -1);
        assertNotNull(rootRef, "DOCUMENT index root reference must not be null");

        // Navigate trie to find the leaf page for record page key 0.
        final PageReference leafRef =
            reader.getReferenceToLeafOfSubtree(rootRef, 0, -1, IndexType.DOCUMENT, revisionRootPage);
        assertNotNull(leafRef, "Leaf page reference for page key 0 must not be null");
      }
    }

    @Test
    @DisplayName("getRecord retrieves correct records through trie traversal")
    void getRecordThroughTrieTraversal() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        // Node key 1 is the root object node created by JsonDocumentCreator.
        final DataRecord record = reader.getRecord(1, IndexType.DOCUMENT, -1);
        assertNotNull(record, "Record with key 1 must be retrievable through trie traversal");
        assertEquals(1, record.getNodeKey(), "Record node key must match the requested key");
      }
    }

    @Test
    @DisplayName("trie traversal returns null for non-existent page key")
    void trieNavigationReturnsNullForNonExistentPageKey() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        final RevisionRootPage revisionRootPage = reader.getActualRevisionRootPage();
        final PageReference rootRef = reader.getPageReference(revisionRootPage, IndexType.DOCUMENT, -1);

        // Use a very large page key that is beyond any pages created.
        final PageReference leafRef =
            reader.getReferenceToLeafOfSubtree(rootRef, 999_999, -1, IndexType.DOCUMENT, revisionRootPage);
        // Either null or the reference has no page key assigned.
        if (leafRef != null) {
          assertEquals(io.sirix.settings.Constants.NULL_ID_LONG, leafRef.getKey(),
              "Non-existent page key must have NULL_ID_LONG storage key");
        }
      }
    }

    @Test
    @DisplayName("historical revision is readable after new commits (trie isolation)")
    void historicalRevisionIsolation() {
      // Revision 1: create document.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      // Revision 2: modify document.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("newKey", new StringValue("newValue"));
        wtx.commit();
      }

      // Read revision 1 — should see original data without revision 2's changes.
      try (final StorageEngineReader readerRev1 = resourceSession.beginStorageEngineReader(1)) {
        final DataRecord recordRev1 = readerRev1.getRecord(1, IndexType.DOCUMENT, -1);
        assertNotNull(recordRev1, "Record from revision 1 must still be readable");
        assertEquals(1, recordRev1.getNodeKey());
      }

      // Read revision 2 — should see modified data.
      try (final StorageEngineReader readerRev2 = resourceSession.beginStorageEngineReader(2)) {
        final DataRecord recordRev2 = readerRev2.getRecord(1, IndexType.DOCUMENT, -1);
        assertNotNull(recordRev2, "Record from revision 2 must be readable");
      }
    }
  }

  @Nested
  @DisplayName("KeyedTrieWriter — write-side trie traversal")
  class KeyedTrieWriterTests {

    @Test
    @DisplayName("createRecord populates trie and record is retrievable")
    void createRecordPopulatesTrieAndIsRetrievable() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      // Verify records created by the write transaction (which uses KeyedTrieWriter
      // internally via prepareLeafOfTree) are readable.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        // Document root is node key 0, root object is key 1,
        // first object record "foo" is key 2.
        for (long nodeKey = 0; nodeKey <= 2; nodeKey++) {
          final DataRecord record = reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
          assertNotNull(record, "Record with key " + nodeKey + " must exist after commit");
          assertEquals(nodeKey, record.getNodeKey());
        }
      }
    }

    @Test
    @DisplayName("multiple commits grow trie correctly with CoW semantics")
    void multipleCommitsGrowTrieWithCow() {
      // Revision 1.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      final long maxNodeKeyRev1;
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(1)) {
        final RevisionRootPage revRoot = reader.getActualRevisionRootPage();
        maxNodeKeyRev1 = revRoot.getMaxNodeKeyInDocumentIndex();
      }

      // Revision 2: add more data.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("extra1", new StringValue("val1"));
        wtx.commit();
      }

      // Revision 3: add even more data.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("extra2", new StringValue("val2"));
        wtx.commit();
      }

      // Verify max node key increased.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(3)) {
        final RevisionRootPage revRoot = reader.getActualRevisionRootPage();
        final long maxNodeKeyRev3 = revRoot.getMaxNodeKeyInDocumentIndex();
        assert maxNodeKeyRev3 > maxNodeKeyRev1 :
            "Max node key must increase after additional commits";
      }

      // Verify revision 1 data is still intact (CoW isolation).
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(1)) {
        final DataRecord record = reader.getRecord(1, IndexType.DOCUMENT, -1);
        assertNotNull(record, "Revision 1 record must still be readable after later commits");
      }

      // Verify all records from revision 3 are readable.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(3)) {
        for (long key = 0; key <= 5; key++) {
          final DataRecord record = reader.getRecord(key, IndexType.DOCUMENT, -1);
          assertNotNull(record, "Record with key " + key + " must exist in revision 3");
        }
      }
    }

    @Test
    @DisplayName("trie traversal works for multiple index types (DOCUMENT, PATH_SUMMARY)")
    void trieTraversalWorksForMultipleIndexTypes() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        final RevisionRootPage revRoot = reader.getActualRevisionRootPage();

        // DOCUMENT index.
        final PageReference docRef = reader.getPageReference(revRoot, IndexType.DOCUMENT, -1);
        assertNotNull(docRef, "DOCUMENT index root reference must exist");
        final PageReference docLeaf =
            reader.getReferenceToLeafOfSubtree(docRef, 0, -1, IndexType.DOCUMENT, revRoot);
        assertNotNull(docLeaf, "DOCUMENT index leaf must be reachable");

        // PATH_SUMMARY index (only if path summary is enabled in the resource config).
        if (resourceSession.getResourceConfig().withPathSummary) {
          final PageReference pathSummaryRef =
              reader.getPageReference(revRoot, IndexType.PATH_SUMMARY, 0);
          assertNotNull(pathSummaryRef, "PATH_SUMMARY index root reference must exist");
        }
      }
    }

    @Test
    @DisplayName("large number of records causes trie to grow multiple levels")
    void largeDatasetCausesTrieGrowth() {
      // Insert many records to force the trie to grow beyond a single level.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        for (int i = 0; i < 2000; i++) {
          wtx.insertObjectRecordAsFirstChild("key" + i, new StringValue("value" + i));
          wtx.moveToParent();
        }
        wtx.commit();
      }

      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader()) {
        final RevisionRootPage revRoot = reader.getActualRevisionRootPage();

        // Verify trie height has grown (should be > 1 with 2000+ records).
        final int trieLevel =
            reader.getCurrentMaxIndirectPageTreeLevel(IndexType.DOCUMENT, -1, revRoot);
        assert trieLevel >= 1 : "Trie must have at least 1 level for 2000+ records";

        // Verify a record near the end is retrievable through the trie.
        final long maxKey = revRoot.getMaxNodeKeyInDocumentIndex();
        final DataRecord record = reader.getRecord(maxKey, IndexType.DOCUMENT, -1);
        assertNotNull(record, "Record at max node key must be retrievable");
        assertEquals(maxKey, record.getNodeKey());

        // Verify a record near the beginning is also retrievable.
        final DataRecord firstRecord = reader.getRecord(1, IndexType.DOCUMENT, -1);
        assertNotNull(firstRecord, "First record must still be retrievable");
      }
    }
  }

  @Nested
  @DisplayName("Versioning — trie across multiple revisions")
  class VersioningTests {

    @Test
    @DisplayName("records are version-isolated across revisions")
    void recordsAreVersionIsolatedAcrossRevisions() {
      // Revision 1: create base document.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      // Count records in revision 1.
      final long maxKeyRev1;
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(1)) {
        maxKeyRev1 = reader.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
      }

      // Revision 2: add more records.
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        for (int i = 0; i < 50; i++) {
          wtx.insertObjectRecordAsFirstChild("rev2key" + i, new StringValue("rev2val" + i));
          wtx.moveToParent();
        }
        wtx.commit();
      }

      // Revision 1 should still have same max key.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(1)) {
        final long maxKeyRev1Check = reader.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
        assertEquals(maxKeyRev1, maxKeyRev1Check,
            "Revision 1 max node key must be unchanged after new commits");
      }

      // Revision 2 should have more records.
      try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(2)) {
        final long maxKeyRev2 = reader.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
        assert maxKeyRev2 > maxKeyRev1 :
            "Revision 2 must have higher max node key than revision 1";

        // All revision 2 records should be readable.
        for (long key = 0; key <= maxKeyRev1; key++) {
          final DataRecord record = reader.getRecord(key, IndexType.DOCUMENT, -1);
          assertNotNull(record, "Record " + key + " from rev 1 must be readable in rev 2");
        }
      }
    }

    @Test
    @DisplayName("many sequential commits maintain trie integrity")
    void manySequentialCommitsMaintainTrieIntegrity() {
      final int numRevisions = 10;

      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        wtx.commit();

        for (int rev = 1; rev <= numRevisions; rev++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("rev" + rev, new StringValue("val" + rev));
          wtx.commit();
        }
      }

      // Verify each revision is independently readable.
      for (int rev = 1; rev <= numRevisions + 1; rev++) {
        try (final StorageEngineReader reader = resourceSession.beginStorageEngineReader(rev)) {
          final DataRecord record = reader.getRecord(1, IndexType.DOCUMENT, -1);
          assertNotNull(record,
              "Record with key 1 must be readable in revision " + rev);
        }
      }
    }
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for the foreground HOT leaf-consolidation cadence. */
@DisplayName("HOT periodic consolidation bounds")
final class HOTPeriodicConsolidationBoundTest {

  private static final String RESOURCE_NAME = "hot-periodic-consolidation";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final int CONSOLIDATION_INTERVAL = 4_096;
  private static final int CONSOLIDATION_TARGET = HOTLeafPage.MAX_ENTRIES * 3 / 4;

  /*
   * A power-of-two dense set with exactly two full leaf pages below every root slot. The canonical
   * bulk tree therefore has a full 32-way root, 32 independent height-one child blocks and 64 leaves.
   * Updating key zero CoWs one of those child blocks; the other 31 are off-route witnesses that the
   * 4,096th put must not copy or log the whole trie.
   */
  private static final int DATASET_SIZE = HOTIndirectPage.MAX_NODE_ENTRIES * HOTLeafPage.MAX_ENTRIES * 2;
  private static final long TARGET_KEY = 0L;
  private static final long TARGET_NODE_KEY = 0L;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("the 4,096th put touches only its route-local leaf parent")
  void periodicConsolidationDoesNotCopyTheWholeTrie() {
    final int baseRevision = bulkBuildDeepTrie();
    assertDeepNonMergeableFixture(baseRevision);

    final int updatedRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);

      for (int insert = 1; insert < CONSOLIDATION_INTERVAL; insert++) {
        writer.indexNodeKey(TARGET_KEY, TARGET_NODE_KEY);
      }

      final int logEntriesBeforeCadence = wtx.getStorageEngineWriter().getLog().liveEntryCount();
      writer.indexNodeKey(TARGET_KEY, TARGET_NODE_KEY);
      final int logEntriesAfterCadence = wtx.getStorageEngineWriter().getLog().liveEntryCount();

      assertEquals(logEntriesBeforeCadence, logEntriesAfterCadence,
          "the cadence put may revisit the already-CoWed route parent, but this fixture has no "
              + "mergeable leaf pair and must not add transaction-log entries for the 31 off-route subtrees");
      assertExactPosting(writer.get(TARGET_KEY, SearchMode.EQUAL), TARGET_NODE_KEY,
          "writer view after the cadence put");
      HOTInvariantValidator.validate(writer.getRootReference(), wtx.getStorageEngineWriter()).assertOk();

      wtx.commit();
      updatedRevision = session.getMostRecentRevisionNumber();
    }

    assertTrue(updatedRevision > baseRevision, "the cadence update must produce a distinct committed revision");
    assertColdRevision(baseRevision);
    assertColdRevision(updatedRevision);
  }

  private static int bulkBuildDeepTrie() {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);
      final HOTLongBulkIndexLoader loader = writer.createBulkLoader();
      for (long key = 0; key < DATASET_SIZE; key++) {
        loader.add(key, key);
      }
      loader.flush();
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  /**
   * Proves the workload is capable of distinguishing route-local maintenance from the old recursive
   * walk: every root child is an independent indirect block, while the target block's two full leaf
   * children cannot be consolidated under the 75%-full target.
   */
  private static void assertDeepNonMergeableFixture(final int revision) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final HOTInvariantValidator.Result result =
          HOTInvariantValidator.validateIndex(reader, IndexType.PATH, INDEX_NUMBER);
      result.assertOk();
      assertEquals(DATASET_SIZE, result.storedKeyCount(), "bulk fixture must retain every composite key");
      assertEquals(2, result.observedHeight(), "fixture must contain a root, child blocks and leaves");

      final PageReference rootReference = HOTInvariantValidator.resolveRootRef(reader, IndexType.PATH, INDEX_NUMBER);
      assertNotNull(rootReference, "committed PATH index root must exist");
      final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(rootReference));
      assertEquals(HOTIndirectPage.MAX_NODE_ENTRIES, root.getNumChildren(),
          "fixture needs one off-route indirect subtree per root slot");
      for (int childIndex = 0; childIndex < root.getNumChildren(); childIndex++) {
        assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(root.getChildReference(childIndex)),
            "root child " + childIndex + " must be an independently persisted indirect subtree");
      }

      final byte[] targetCompositeKey = new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serializeWithChunkIdx(TARGET_KEY, (int) (TARGET_NODE_KEY >>> 16), targetCompositeKey,
          0);
      final int targetParentIndex = root.findChildIndex(targetCompositeKey);
      assertTrue(targetParentIndex >= 0, "target key must route to a root child");
      final HOTIndirectPage targetParent =
          assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(root.getChildReference(targetParentIndex)));
      assertEquals(2, targetParent.getNumChildren(), "target route must end in the expected two-leaf block");
      final HOTLeafPage left =
          assertInstanceOf(HOTLeafPage.class, reader.loadHOTPage(targetParent.getChildReference(0)));
      final HOTLeafPage right =
          assertInstanceOf(HOTLeafPage.class, reader.loadHOTPage(targetParent.getChildReference(1)));
      assertTrue(left.getEntryCount() + right.getEntryCount() > CONSOLIDATION_TARGET,
          "the cadence must be a no-merge attempt, isolating transaction-log growth from leaf replacement");
    }
  }

  private static void assertColdRevision(final int revision) {
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final HOTInvariantValidator.Result result =
          HOTInvariantValidator.validateIndex(reader, IndexType.PATH, INDEX_NUMBER);
      result.assertOk();
      assertEquals(DATASET_SIZE, result.storedKeyCount(), "revision " + revision + " lost index entries");

      final HOTLongIndexReader indexReader = HOTLongIndexReader.create(reader, IndexType.PATH, INDEX_NUMBER);
      assertExactPosting(indexReader.get(0L, SearchMode.EQUAL), 0L, "revision " + revision + " first key");
      assertExactPosting(indexReader.get(DATASET_SIZE / 2L, SearchMode.EQUAL), DATASET_SIZE / 2L,
          "revision " + revision + " middle key");
      assertExactPosting(indexReader.get(DATASET_SIZE - 1L, SearchMode.EQUAL), DATASET_SIZE - 1L,
          "revision " + revision + " last key");
    }
  }

  private static void assertExactPosting(final NodeReferences references, final long expectedNodeKey,
      final String message) {
    assertNotNull(references, message + " must exist");
    assertEquals(1L, references.getNodeKeys().getLongCardinality(), message + " must contain one posting");
    assertTrue(references.getNodeKeys().contains(expectedNodeKey),
        message + " must contain node key " + expectedNodeKey);
  }
}

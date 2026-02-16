package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chicago dataset integration tests for async auto-commit.
 * <p>
 * These tests exercise bulk import with auto-commit using the Chicago crimes
 * subset dataset (~100KB, 101 records). They verify data integrity across
 * multiple auto-commit thresholds and versioning strategies.
 * <p>
 * <b>CI:</b> Tests tagged "local-only" are disabled on CI. Run locally with:
 * {@code -Dtest.local=true} or by removing the tag filter.
 * <p>
 * Tests without the "local-only" tag use the chicago-subset.json (small) and run on CI.
 */
public final class AsyncAutoCommitChicagoTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  /**
   * Test bulk import with auto-commit at various thresholds using chicago-subset.json.
   * Each threshold triggers different numbers of intermediate commits.
   */
  @ParameterizedTest
  @ValueSource(ints = {10, 50, 100, 500, 1000})
  void chicagoSubsetWithVariousAutoCommitThresholds(final int maxNodeCount) {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Bulk import with auto-commit
      try (final var wtx = session.beginNodeTrx(maxNodeCount, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Verify data integrity
      verifyChicagoSubsetIntegrity(session);
    }
  }

  /**
   * Test that serialization output is identical regardless of auto-commit threshold.
   * This is a critical correctness property: the commit boundary must not affect the
   * logical state of the database.
   */
  @Test
  void serializationIsIdenticalAcrossThresholds() {
    // First: import without auto-commit (baseline)
    final var baselineDb = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final String baselineJson;

    try (final var session = baselineDb.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")));
      }

      baselineJson = serialize(session);
    }

    // Second: import with small auto-commit threshold
    JsonTestHelper.deleteEverything();
    final var autoCommitDb = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = autoCommitDb.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx(20, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      final String autoCommitJson = serialize(session);

      assertEquals(baselineJson, autoCommitJson,
          "Serialization must be identical regardless of auto-commit threshold");
    }
  }

  /**
   * Test with DeweyIDs enabled and auto-commit.
   * DeweyIDs add complexity because they must be maintained across commit boundaries.
   */
  @Test
  void chicagoSubsetWithDeweyIdsAndAutoCommit() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(
        JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx(50, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      verifyChicagoSubsetIntegrity(session);

      // Also verify DeweyIDs are present
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        assertNotNull(rtx.getDeweyID(), "DeweyID should be present on root object");
      }
    }
  }

  /**
   * Test with rolling hashes enabled and auto-commit.
   * Hashes must be correctly recomputed across commit boundaries.
   */
  @Test
  void chicagoSubsetWithHashesAndAutoCommit() {
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(
        JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx(50, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      verifyChicagoSubsetIntegrity(session);
    }
  }

  /**
   * Test multiple revision creation with Chicago dataset.
   * Import, update (add more data), and verify all revisions are accessible.
   */
  @Test
  void multipleRevisionsWithChicagoSubset() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Initial import with auto-commit threshold 50.
      // Auto-commit creates intermediate revisions, so the complete import
      // is at the LATEST revision, not necessarily revision 1.
      try (final var wtx = session.beginNodeTrx(50, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      final int importCompleteRevision = session.getMostRecentRevisionNumber();
      assertTrue(importCompleteRevision >= 1, "At least one revision should exist after import");

      final long importChildCount;
      try (final var rtx = session.beginNodeReadOnlyTrx(importCompleteRevision)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Import revision should have root object");
        importChildCount = rtx.getChildCount();
        assertTrue(importChildCount > 0, "Import revision root object should have children");
      }

      // Modify: add a new key to the root object
      try (final var wtx = session.beginNodeTrx(10, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("added_after_import",
            new io.sirix.access.trx.node.json.objectvalue.StringValue("modified"));
        wtx.commit();
      }

      final int modifiedRevision = session.getMostRecentRevisionNumber();
      assertTrue(modifiedRevision > importCompleteRevision,
          "Modification should create a new revision");

      final long modifiedChildCount;
      try (final var rtx = session.beginNodeReadOnlyTrx(modifiedRevision)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Modified revision should have root object");
        modifiedChildCount = rtx.getChildCount();
      }

      // Modified revision should have more children (we added one object key)
      assertTrue(modifiedChildCount > importChildCount,
          "Modified rev (" + modifiedChildCount + ") should have more children than import rev ("
              + importChildCount + ")");

      // Verify the import-complete revision is still intact
      try (final var rtx = session.beginNodeReadOnlyTrx(importCompleteRevision)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Historical import revision should have root object");
        assertEquals(importChildCount, rtx.getChildCount(),
            "Import revision should be unchanged after modification");
      }
    }
  }

  /**
   * Test with all four versioning types against chicago-subset.
   * This is the most comprehensive correctness check.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void chicagoSubsetWithAllVersioningStrategies(final VersioningType versioningType) {
    JsonTestHelper.deleteEverything();

    final DatabaseConfiguration dbConfig =
        new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile());
    Databases.createJsonDatabase(dbConfig);

    final var db = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    db.createResource(
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
            .versioningApproach(versioningType)
            .build());

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx(30, AfterCommitState.KEEP_OPEN_ASYNC)) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      verifyChicagoSubsetIntegrity(session);
    }

    db.close();
  }

  /**
   * Local-only test with a more intensive workload.
   * Disabled on CI to avoid slow builds.
   */
  @Test
  @Tag("local-only")
  @EnabledIfSystemProperty(named = "test.local", matches = "true")
  void chicagoSubsetRepeatedImportStress() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Import chicago-subset 5 times with very small commit threshold
      for (int round = 0; round < 5; round++) {
        try (final var wtx = session.beginNodeTrx(5, AfterCommitState.KEEP_OPEN_ASYNC)) {
          if (round == 0) {
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
                JsonNodeTrx.Commit.NO);
          } else {
            // For subsequent rounds, modify the existing document
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.insertObjectRecordAsFirstChild("round" + round,
                new io.sirix.access.trx.node.json.objectvalue.StringValue("data" + round));
          }
          wtx.commit();
        }
      }

      // Verify latest revision
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.getChildCount() > 0);
      }

      // Verify all 5 revisions exist
      for (int rev = 1; rev <= 5; rev++) {
        try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild(), "Revision " + rev + " should have content");
        }
      }
    }
  }

  // ============================================================================
  // TIL Rotation with Chicago data
  // ============================================================================

  @Test
  void tilRotationPreservesAllChicagoPages() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        final int tilSize = log.size();
        assertTrue(tilSize > 10, "Chicago import should create many TIL entries, got " + tilSize);

        // Rotate and verify all entries preserved
        final var rotation = log.detachAndReset();
        assertEquals(tilSize, rotation.size());
        assertEquals(tilSize, rotation.refToContainer().size());
        assertEquals(0, log.size());

        // Every entry in the identity map should have a non-null container
        for (var entry : rotation.refToContainer().entrySet()) {
          assertNotNull(entry.getKey(), "Ref must not be null");
          assertNotNull(entry.getValue(), "Container must not be null for ref");
        }

        wtx.rollback();
      }
    }
  }

  @Test
  void snapshotCreationWithChicagoData() throws Exception {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();
        final int tilSize = log.size();

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "chicago snapshot", System.currentTimeMillis(), null);

        assertNotNull(snapshot);
        assertEquals(tilSize, snapshot.size());
        assertNotNull(snapshot.revisionRootPage());
        assertEquals("chicago snapshot", snapshot.commitMessage());
        assertFalse(snapshot.isCommitComplete());

        // Count accessible entries — some root-level pages (RRP, NamePage, PathSummary, etc.)
        // are re-added to the active TIL by prepareAsyncCommitSnapshot and cleared from the snapshot.
        int accessibleEntries = 0;
        for (int i = 0; i < snapshot.size(); i++) {
          if (snapshot.getEntry(i) != null) {
            accessibleEntries++;
          }
        }
        // The vast majority of entries should be accessible (only ~6 root-level pages are cleared)
        assertTrue(accessibleEntries > tilSize - 10,
            "Most entries should be accessible, got " + accessibleEntries + " of " + tilSize);

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  private void verifyChicagoSubsetIntegrity(final JsonResourceSession session) {
    try (final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), "Should have root object");

      // Find "data" array
      boolean foundData = false;
      if (rtx.moveToFirstChild()) {
        do {
          if (rtx.getKind() == NodeKind.OBJECT_KEY && "data".equals(rtx.getName().getLocalName())) {
            foundData = true;
            assertTrue(rtx.moveToFirstChild(), "data key should have value (array)");
            final long childCount = rtx.getChildCount();
            assertEquals(101, childCount,
                "Chicago subset data array should have 101 children");
            break;
          }
        } while (rtx.moveToRightSibling());
      }

      assertTrue(foundData, "Should find 'data' key in Chicago dataset");
    }

    // Verify serialization produces valid JSON
    final String json = serialize(session);
    assertNotNull(json);
    assertTrue(json.length() > 1000);
    assertTrue(json.contains("\"data\""));
  }

  private String serialize(final JsonResourceSession session) {
    try (final var writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer).build().call();
      return writer.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


}

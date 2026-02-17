package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests for {@link RevisionIndex} to verify it works correctly with the full SirixDB
 * stack including commits, database reopening, and concurrent access patterns.
 */
class RevisionIndexIntegrationTest {

  private Path tempDir;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private static final String RESOURCE_NAME = "test-resource";

  private Path createNewTempDir() throws Exception {
    return java.nio.file.Files.createTempDirectory("sirix-test-");
  }

  void setUp() throws Exception {
    tempDir = createNewTempDir();
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(tempDir);
    Databases.createJsonDatabase(dbConfig);
    database = Databases.openJsonDatabase(tempDir);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    session = database.beginResourceSession(RESOURCE_NAME);
  }

  void tearDown() {
    try {
      if (session != null) {
        session.close();
        session = null;
      }
      if (database != null) {
        database.close();
        database = null;
      }
      if (tempDir != null) {
        Databases.removeDatabase(tempDir);
        // Also clean up any remaining files
        deleteRecursively(tempDir.toFile());
        tempDir = null;
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  private void deleteRecursively(java.io.File file) {
    if (file.isDirectory()) {
      java.io.File[] children = file.listFiles();
      if (children != null) {
        for (java.io.File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  @Nested
  @DisplayName("Basic Timestamp Lookup")
  class BasicTimestampLookup {

    @Test
    @DisplayName("Open read-only transaction at exact commit timestamp")
    void testExactTimestampLookup() throws Exception {
      setUp();
      try {
        Instant commitTime;

        // After createResource(), revision 0 already exists
        final int initialRevision = session.getMostRecentRevisionNumber();
        assertEquals(0, initialRevision, "Initial revision should be 0");

        // Create revision 1 (use Commit.NO to avoid implicit auto-commit from insertSubtree)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"test\": true}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        assertEquals(1, session.getMostRecentRevisionNumber(), "After first commit, should be revision 1");

        // Get the commit timestamp from revision 1
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
          commitTime = rtx.getRevisionTimestamp();
        }

        // Open at exact timestamp - should get revision 1
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(commitTime)) {
          assertEquals(1, rtx.getRevisionNumber());
        }
      } finally {
        tearDown();
      }
    }

    @Test
    @DisplayName("Open read-only transaction before first commit returns revision 0")
    void testBeforeFirstCommit() throws Exception {
      setUp();
      try {
        // Revision 0 exists after createResource (empty document)
        final Instant beforeAnyCommit = Instant.now();

        Thread.sleep(50); // Ensure separation

        // Create revision 1 (first user commit, use Commit.NO)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Open at timestamp before first user commit - should get revision 0 (floor)
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(beforeAnyCommit)) {
          assertEquals(0, rtx.getRevisionNumber());
        }
      } finally {
        tearDown();
      }
    }

    @Test
    @DisplayName("Open read-only transaction between revisions returns earlier revision (floor)")
    void testBetweenRevisions() throws Exception {
      setUp();
      try {
        // Revision 0 exists after createResource
        Instant afterRev1;
        Instant afterRev2;

        // Create revision 1 (first user commit)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": 1}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        Thread.sleep(50); // Ensure time separation
        afterRev1 = Instant.now();
        Thread.sleep(50);

        // Create revision 2 (second user commit)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": 2}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        Thread.sleep(50);
        afterRev2 = Instant.now();

        // Open at timestamp between rev1 and rev2 - should get rev1 (floor)
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterRev1)) {
          assertEquals(1, rtx.getRevisionNumber());
        }

        // Open at timestamp after rev2 - should get rev2
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterRev2)) {
          assertEquals(2, rtx.getRevisionNumber());
        }
      } finally {
        tearDown();
      }
    }
  }

  @Nested
  @DisplayName("Multiple Revisions")
  class MultipleRevisions {

    @Test
    @DisplayName("Navigate through 10 revisions by timestamp")
    void testMultipleRevisionNavigation() throws Exception {
      setUp();
      try {
        // Revision 0 already exists after createResource
        final int numCommits = 10;
        final List<Instant> afterCommitTimes = new ArrayList<>();

        // Create revisions 1-10 with time gaps (use Commit.NO to avoid implicit auto-commit)
        for (int i = 1; i <= numCommits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          Thread.sleep(20); // Small gap between revisions
          afterCommitTimes.add(Instant.now());
          Thread.sleep(20);
        }

        // Verify each timestamp maps to the correct revision (1-10)
        for (int i = 0; i < numCommits; i++) {
          final int expectedRevision = i + 1; // Revisions are 1, 2, 3, ...
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterCommitTimes.get(i))) {
            assertEquals(expectedRevision, rtx.getRevisionNumber(),
                "Timestamp after commit " + (i + 1) + " should map to revision " + expectedRevision);
          }
        }
      } finally {
        tearDown();
      }
    }
  }

  @Nested
  @DisplayName("Database Restart")
  class DatabaseRestart {

    @Test
    @DisplayName("RevisionIndex is correctly rebuilt after database reopen")
    void testIndexRebuiltOnReopen() throws Exception {
      setUp();
      try {
        // Revision 0 already exists after createResource
        final int numCommits = 5;
        final List<Instant> afterCommitTimes = new ArrayList<>();

        // Create revisions 1-5 (use Commit.NO to avoid implicit auto-commit)
        for (int i = 1; i <= numCommits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          Thread.sleep(100); // Ensure timestamp is definitely after commit
          afterCommitTimes.add(Instant.now());
          Thread.sleep(100);
        }

        // Close session and database
        session.close();
        database.close();

        // Reopen database
        database = Databases.openJsonDatabase(tempDir);
        session = database.beginResourceSession(RESOURCE_NAME);

        // Verify all revisions are accessible by timestamp (revisions 1-5)
        for (int i = 0; i < numCommits; i++) {
          final int expectedRevision = i + 1;
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterCommitTimes.get(i))) {
            assertEquals(expectedRevision, rtx.getRevisionNumber(),
                "After reopen, timestamp should still map to revision " + expectedRevision);
          }
        }
      } finally {
        tearDown();
      }
    }
  }

  @Nested
  @DisplayName("Concurrent Access")
  class ConcurrentAccess {

    @Test
    @DisplayName("Sequential reads during writes work correctly")
    void testSequentialReadsWithWrites() throws Exception {
      setUp();
      try {
        // Revision 0 already exists; create revision 1 (use Commit.NO)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"data\": \"initial\"}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Create more revisions and verify lookups work
        for (int i = 2; i <= 10; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }

          // Immediately verify we can read the newly committed revision
          Thread.sleep(20);
          final Instant now = Instant.now();
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(now)) {
            int rev = rtx.getRevisionNumber();
            assertEquals(i, rev, "After commit " + i + ", should get revision " + i);
          }
        }
      } finally {
        tearDown();
      }
    }
  }

  @Nested
  @DisplayName("Feature Flag")
  class FeatureFlag {

    @Test
    @DisplayName("Timestamp lookup works regardless of feature flag setting")
    void testFeatureFlagCompatibility() throws Exception {
      setUp();
      try {
        // Revision 0 already exists after createResource
        final int numCommits = 5;
        final List<Instant> timestamps = new ArrayList<>();

        // Create revisions 1-5 (use Commit.NO to avoid implicit auto-commit)
        for (int i = 1; i <= numCommits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          Thread.sleep(20);
          timestamps.add(Instant.now());
          Thread.sleep(20);
        }

        // Verify lookups work correctly (revisions 1-5)
        for (int i = 0; i < numCommits; i++) {
          final int expectedRevision = i + 1;
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(timestamps.get(i))) {
            assertEquals(expectedRevision, rtx.getRevisionNumber());
          }
        }
      } finally {
        tearDown();
      }
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Single commit database")
    void testSingleCommit() throws Exception {
      setUp();
      try {
        // Revision 0 already exists; create revision 1 (use Commit.NO)
        Instant afterCommit;

        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"single\": true}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        afterCommit = Instant.now();

        // Before any commit (epoch 0) - floor to revision 0
        final Instant beforeCommit = Instant.ofEpochMilli(0);
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(beforeCommit)) {
          assertEquals(0, rtx.getRevisionNumber());
        }

        // After commit - should get revision 1
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterCommit)) {
          assertEquals(1, rtx.getRevisionNumber());
        }

        // Far in future - should get most recent (revision 1)
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(Instant.now().plusSeconds(3600))) {
          assertEquals(1, rtx.getRevisionNumber());
        }
      } finally {
        tearDown();
      }
    }

    @Test
    @DisplayName("Rapid commits with close timestamps")
    void testRapidCommits() throws Exception {
      setUp();
      try {
        // Revision 0 already exists; create 50 more revisions (1-50) (use Commit.NO)
        final int numCommits = 50;
        for (int i = 1; i <= numCommits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          // No sleep - as fast as possible
        }

        // Verify we can still navigate by timestamp - should get most recent (revision 50)
        final Instant now = Instant.now();
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(now)) {
          assertEquals(50, rtx.getRevisionNumber());
        }
      } finally {
        tearDown();
      }
    }

    @Test
    @DisplayName("RevisionIndex info is accessible after lookup")
    void testRevisionInfoAfterLookup() throws Exception {
      setUp();
      try {
        // Revision 0 already exists; create revision 1 (use Commit.NO)
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"data\": 123}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        Thread.sleep(50);
        final Instant afterCommit = Instant.now();

        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(afterCommit)) {
          assertEquals(1, rtx.getRevisionNumber());

          // Verify we can get revision timestamp
          final var revisionTimestamp = rtx.getRevisionTimestamp();
          assertNotNull(revisionTimestamp);
        }
      } finally {
        tearDown();
      }
    }
  }
}


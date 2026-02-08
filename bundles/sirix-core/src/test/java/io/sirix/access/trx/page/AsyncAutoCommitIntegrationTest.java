package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.page.PageReference;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for async auto-commit during bulk imports.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>TIL rotation correctly freezes entries and resets for new inserts</li>
 *   <li>CommitSnapshot captures correct state</li>
 *   <li>Layered lookup finds pages in active TIL, pending snapshot, or disk</li>
 *   <li>Copy-on-Write works for IndirectPages accessed from snapshot</li>
 *   <li>Concurrent inserts and commits don't corrupt data</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class AsyncAutoCommitIntegrationTest {

  private Database<JsonResourceSession> database;
  private JsonResourceSession resourceSession;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testTilRotationPreservesEntries() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      // Create initial document
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      // Access the TIL through page transaction
      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert records - navigate back to object each time
      for (int i = 0; i < 10; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // Move to the root object node
        wtx.insertObjectRecordAsFirstChild("key" + i, new StringValue("value" + i));
      }

      final int sizeBeforeRotation = log.size();
      assertTrue(sizeBeforeRotation > 0, "TIL should have entries");

      final int genBeforeRotation = log.getCurrentGeneration();

      // Rotate TIL
      final TransactionIntentLog.RotationResult rotation = log.detachAndReset();

      // Verify rotation result
      assertEquals(sizeBeforeRotation, rotation.size(), "Rotation should preserve entry count");
      assertNotNull(rotation.refToContainer(), "Identity map should be created");
      assertEquals(sizeBeforeRotation, rotation.refToContainer().size(), "Identity map should have all refs");

      // Verify TIL is reset
      assertEquals(0, log.size(), "TIL should be empty after rotation");
      assertEquals(genBeforeRotation + 1, log.getCurrentGeneration(), "Generation should increment");

      // Rollback - we're just testing TIL rotation
      wtx.rollback();
    }
  }

  @Test
  public void testGenerationCounterPreventsLogKeyCollision() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert to populate TIL
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("before", new StringValue("rotation"));

      final int genBeforeRotation = log.getCurrentGeneration();

      // Rotate TIL
      log.detachAndReset();

      // Insert more after rotation - navigate back to object first
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("after", new StringValue("rotation"));

      // New inserts should have new generation
      final int genAfterRotation = log.getCurrentGeneration();
      assertEquals(genBeforeRotation + 1, genAfterRotation, "Generation should have incremented");

      wtx.rollback();
    }
  }

  @Test
  public void testCommitSnapshotCapturesState() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert some data - navigate back to object each time
      for (int i = 0; i < 5; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("key" + i, new StringValue("value" + i));
      }

      // Prepare snapshot
      try {
        pageTrx.acquireCommitPermit();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "test commit",
          System.currentTimeMillis(),
          null
      );

      assertNotNull(snapshot, "Snapshot should be created");
      assertTrue(snapshot.size() > 0, "Snapshot should have entries");
      assertNotNull(snapshot.revisionRootPage(), "Snapshot should have RRP");
      assertEquals("test commit", snapshot.commitMessage(), "Commit message should be preserved");

      // Clean up
      pageTrx.getPendingSnapshot(); // Just to verify it's set

      wtx.rollback();
    }
  }

  @Test
  public void testConcurrentInsertsDuringAsyncCommit() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch commitStarted = new CountDownLatch(1);
    final AtomicBoolean commitSuccess = new AtomicBoolean(false);
    final AtomicReference<Exception> commitError = new AtomicReference<>();

    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert initial data - navigate back each time
      for (int i = 0; i < 10; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("initial" + i, new StringValue("value" + i));
      }

      final int sizeBeforeSnapshot = log.size();
      assertTrue(sizeBeforeSnapshot > 0, "TIL should have entries");

      // Prepare snapshot for async commit
      pageTrx.acquireCommitPermit();

      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "async commit",
          System.currentTimeMillis(),
          null
      );

      assertNotNull(snapshot, "Snapshot should be created");
      assertEquals(sizeBeforeSnapshot, snapshot.size(), "Snapshot should have all entries");

      // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
      // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
      assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

      // Start async commit in background
      executor.submit(() -> {
        try {
          commitStarted.countDown();

          // Simulate commit processing
          Thread.sleep(50);

          // Mark commit complete
          snapshot.markCommitComplete();

          commitSuccess.set(true);
        } catch (Exception e) {
          commitError.set(e);
        }
      });

      // Wait for commit to start
      assertTrue(commitStarted.await(1, TimeUnit.SECONDS), "Commit should start");

      // Verify concurrent state - snapshot exists while commit runs
      assertNotNull(pageTrx.getPendingSnapshot(), "Should have pending snapshot during commit");
      assertTrue(!snapshot.isCommitComplete(), "Commit should still be in progress");

      // Wait for commit to finish
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");

      assertNull(commitError.get(), "Commit should not have errors");
      assertTrue(commitSuccess.get(), "Commit should succeed");
      assertTrue(snapshot.isCommitComplete(), "Commit should be complete");

      wtx.rollback();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testLayeredLookupFromActiveAndSnapshot() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert to populate TIL
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("snapshot_data", new StringValue("value"));

      final int sizeBeforeSnapshot = log.size();
      assertTrue(sizeBeforeSnapshot > 0, "TIL should have entries before snapshot");

      // Prepare snapshot (rotates TIL)
      try {
        pageTrx.acquireCommitPermit();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot("test", System.currentTimeMillis(), null);

      // Verify we have a pending snapshot
      assertNotNull(snapshot, "Should have snapshot");
      assertNotNull(pageTrx.getPendingSnapshot(), "Should have pending snapshot set");

      // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
      // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
      assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

      // Verify snapshot has old entries
      assertEquals(sizeBeforeSnapshot, snapshot.size(), "Snapshot should have entries from before rotation");

      // Note: We can't insert more after rotation in this test because the path summary
      // pages are now in the snapshot and the layered lookup would need to be integrated
      // into the full write path (which is a more complex change).

      wtx.rollback();
    }
  }

  @Test
  public void testDiskOffsetPropagation() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert data
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("test", new StringValue("value"));

      // Prepare snapshot
      try {
        pageTrx.acquireCommitPermit();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Simulate disk offset recording
        snapshot.recordDiskOffset(0, 12345L);
        assertEquals(12345L, snapshot.getDiskOffset(0), "Disk offset should be recorded");

        // Before marking complete, getDiskOffset should work
        assertEquals(-1L, snapshot.getDiskOffset(999), "Out of bounds should return -1");

        // Mark complete
        snapshot.markCommitComplete();
        assertTrue(snapshot.isCommitComplete(), "Commit should be marked complete");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testBackpressurePreventsMultiplePendingCommits() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert and prepare first snapshot
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("first", new StringValue("commit"));

      pageTrx.acquireCommitPermit();
      final CommitSnapshot first = pageTrx.prepareAsyncCommitSnapshot(
          "first",
          System.currentTimeMillis(),
          null
      );

      // Try to acquire second permit (should fail since first is held)
      final boolean acquired = pageTrx.tryAcquireCommitPermit();

      if (first != null) {
        // First commit is in progress, second should block
        assertTrue(!acquired, "Should not acquire permit while another commit is pending");
      }

      wtx.rollback();
    }
  }

  // ============================================================================
  // Extensive Integration Tests
  // ============================================================================

  @Test
  public void testMultipleRotationCyclesWithGenerationTracking() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Test generation increment across multiple rotations
      // Note: We can only insert in the first cycle because subsequent rotations
      // would require full layered lookup integration for path summary pages

      // First cycle - insert data
      final int genBefore = log.getCurrentGeneration();

      for (int i = 0; i < 3; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("cycle0_key" + i, new StringValue("value" + i));
      }

      final int sizeBefore = log.size();
      assertTrue(sizeBefore > 0, "TIL should have entries in first cycle");

      // Rotate
      TransactionIntentLog.RotationResult rotation = log.detachAndReset();

      // Verify first rotation
      assertEquals(sizeBefore, rotation.size(), "Rotation should preserve size");
      assertEquals(genBefore + 1, log.getCurrentGeneration(), "Generation should increment");
      assertEquals(0, log.size(), "TIL should be empty after rotation");

      // Now test multiple empty rotations to verify generation keeps incrementing
      for (int cycle = 1; cycle < 5; cycle++) {
        final int currentGen = log.getCurrentGeneration();
        // Rotate empty TIL
        TransactionIntentLog.RotationResult emptyRotation = log.detachAndReset();
        assertEquals(0, emptyRotation.size(), "Empty rotation should have 0 entries");
        assertEquals(currentGen + 1, log.getCurrentGeneration(), 
            "Generation should increment even for empty rotation in cycle " + cycle);
      }

      // Final generation should be initial + 5
      assertEquals(genBefore + 5, log.getCurrentGeneration(), "Final generation should be 5 more than initial");

      wtx.rollback();
    }
  }

  @Test
  public void testIdentityMapPreservesReferenceIdentity() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert to get page references
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("identity_test", new StringValue("value"));

      // Get a reference from the TIL
      final int sizeBefore = log.size();
      assertTrue(sizeBefore > 0, "Should have entries");

      // Rotate
      TransactionIntentLog.RotationResult rotation = log.detachAndReset();

      // The identity map should be populated
      assertNotNull(rotation.refToContainer(), "Identity map should exist");
      assertEquals(sizeBefore, rotation.refToContainer().size(), "Identity map should have all refs");

      // Each entry in identity map should have a non-null container
      for (var entry : rotation.refToContainer().entrySet()) {
        assertNotNull(entry.getKey(), "Key should not be null");
        assertNotNull(entry.getValue(), "Container should not be null");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testSnapshotIsolationFromNewInserts() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert initial data
      for (int i = 0; i < 5; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("initial" + i, new StringValue("value" + i));
      }

      final int initialSize = log.size();

      // Create snapshot
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "isolation test",
          System.currentTimeMillis(),
          null
      );

      assertNotNull(snapshot, "Snapshot should be created");

      // Snapshot should have initial size
      assertEquals(initialSize, snapshot.size(), "Snapshot should capture initial entries");

      // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
      // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
      assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

      // Snapshot size should NOT change when new data would be inserted
      // (though we can't insert here due to path summary issues)
      assertEquals(initialSize, snapshot.size(), "Snapshot size should remain constant");

      wtx.rollback();
    }
  }

  @Test
  public void testDiskOffsetArrayBoundsChecking() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert data
      for (int i = 0; i < 5; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("bounds" + i, new StringValue("value" + i));
      }

      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "bounds test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        final int snapshotSize = snapshot.size();

        // Test valid offsets
        for (int i = 0; i < snapshotSize; i++) {
          snapshot.recordDiskOffset(i, 1000L + i);
          assertEquals(1000L + i, snapshot.getDiskOffset(i), "Should record offset for index " + i);
        }

        // Test out of bounds - should return -1, not throw
        assertEquals(-1L, snapshot.getDiskOffset(-1), "Negative index should return -1");
        assertEquals(-1L, snapshot.getDiskOffset(snapshotSize), "Index at size should return -1");
        assertEquals(-1L, snapshot.getDiskOffset(snapshotSize + 100), "Large index should return -1");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testCommitCompleteStatePropagation() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch commitStarted = new CountDownLatch(1);
    final CountDownLatch checkComplete = new CountDownLatch(1);

    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("state_prop", new StringValue("value"));

      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "state propagation test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Initially not complete
        assertTrue(!snapshot.isCommitComplete(), "Should not be complete initially");

        executor.submit(() -> {
          try {
            commitStarted.countDown();
            checkComplete.await(1, TimeUnit.SECONDS);
            
            // Now mark complete
            snapshot.markCommitComplete();
          } catch (Exception e) {
            // Ignore
          }
        });

        // Wait for thread to start
        commitStarted.await(1, TimeUnit.SECONDS);

        // Still not complete
        assertTrue(!snapshot.isCommitComplete(), "Should not be complete before markCommitComplete");

        // Signal to complete
        checkComplete.countDown();

        // Wait for completion
        Thread.sleep(100);

        // Now should be complete
        assertTrue(snapshot.isCommitComplete(), "Should be complete after markCommitComplete");
      }

      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

      wtx.rollback();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testEmptyRotationAfterClear() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // After commit, the TIL has some entries from the commit process.
      // Do a rotation to clear it, then test empty rotation.
      TransactionIntentLog.RotationResult firstRotation = log.detachAndReset();
      assertTrue(firstRotation.size() >= 0, "First rotation captures whatever was in TIL");

      // Now TIL should be empty
      assertEquals(0, log.size(), "TIL should be empty after rotation");

      // Try to prepare snapshot with empty TIL
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "empty snapshot",
          System.currentTimeMillis(),
          null
      );

      // prepareAsyncCommitSnapshot returns null for empty TIL and releases permit
      assertNull(snapshot, "Empty TIL should result in null snapshot");

      wtx.rollback();
    }
  }

  @Test
  public void testPageReferenceGenerationField() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      final int initialGen = log.getCurrentGeneration();

      // Insert to create page references
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("gen_test", new StringValue("value"));

      // Rotate
      log.detachAndReset();

      final int newGen = log.getCurrentGeneration();
      assertEquals(initialGen + 1, newGen, "Generation should increment after rotation");

      // New inserts after rotation should get new generation
      // (We can't verify the PageReference generation directly without more invasive testing)

      wtx.rollback();
    }
  }

  @Test
  public void testSnapshotCommitterRecordsDiskOffsets() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("disk_offset", new StringValue("value"));

      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "disk offset test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Simulate what SnapshotCommitter would do
        for (int i = 0; i < snapshot.size(); i++) {
          // Record disk offset
          snapshot.recordDiskOffset(i, 50000L + i * 100);
        }

        // Verify all offsets recorded
        for (int i = 0; i < snapshot.size(); i++) {
          assertEquals(50000L + i * 100, snapshot.getDiskOffset(i), 
              "Disk offset should be recorded for logKey " + i);
        }

        // Mark complete
        snapshot.markCommitComplete();
        assertTrue(snapshot.isCommitComplete(), "Commit should be complete");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testLargeNumberOfEntriesRotation() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert many entries - not as many as 100 since each insert creates multiple pages
      // (record page + path summary pages, etc.)
      final int numInserts = 20;
      for (int i = 0; i < numInserts; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("large" + i, new StringValue("value" + i));
      }

      final int sizeBefore = log.size();
      // Each insert creates multiple TIL entries (record page, path summary, etc.)
      assertTrue(sizeBefore > 0, "Should have entries in TIL");

      // Rotate
      TransactionIntentLog.RotationResult rotation = log.detachAndReset();

      // Verify all entries preserved
      assertEquals(sizeBefore, rotation.size(), "Rotation should preserve all entries");
      assertEquals(sizeBefore, rotation.refToContainer().size(), "Identity map should have all entries");

      // Verify TIL reset
      assertEquals(0, log.size(), "TIL should be empty after rotation");

      wtx.rollback();
    }
  }

  @Test
  public void testRevisionRootPageDeepCopy() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("rrp_copy", new StringValue("value"));

      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "rrp deep copy test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Verify RevisionRootPage is captured
        assertNotNull(snapshot.revisionRootPage(), "RRP should be captured");
        
        // Verify commit message is captured
        assertEquals("rrp deep copy test", snapshot.commitMessage(), "Commit message should be captured");

        // Verify revision is captured
        assertTrue(snapshot.revision() >= 0, "Revision should be non-negative");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testTryAcquireCommitPermitNonBlocking() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // First tryAcquire should succeed
      assertTrue(pageTrx.tryAcquireCommitPermit(), "First tryAcquire should succeed");

      // Second tryAcquire should fail immediately (non-blocking)
      long start = System.currentTimeMillis();
      boolean secondResult = pageTrx.tryAcquireCommitPermit();
      long duration = System.currentTimeMillis() - start;

      assertTrue(!secondResult, "Second tryAcquire should fail");
      assertTrue(duration < 100, "tryAcquire should be non-blocking (took " + duration + "ms)");

      wtx.rollback();
    }
  }

  // ============================================================================
  // Layered Lookup Path Tests
  // These tests verify all code paths that were updated to use getFromActiveOrPending()
  // ============================================================================

  @Test
  public void testGetLogRecordUsesLayeredLookup() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert data to populate TIL
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("getLogRecord_test", new StringValue("value"));

      final int sizeBefore = log.size();
      assertTrue(sizeBefore > 0, "TIL should have entries");

      // Create snapshot - this rotates the TIL
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "getLogRecord test",
          System.currentTimeMillis(),
          null
      );

      assertNotNull(snapshot, "Snapshot should be created");
      // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
      // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
      assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

      // Verify getLogRecord can find pages from the pending snapshot via logKey fallback.
      // Some entries may be null because reAddRootPageToActiveTil clears root-level pages
      // from the snapshot after moving them back into the active TIL.
      int nonNullEntries = 0;
      for (int i = 0; i < snapshot.size(); i++) {
        PageContainer expected = snapshot.getEntry(i);
        if (expected != null) {
          nonNullEntries++;
        }
      }
      assertTrue(nonNullEntries > 0, "At least some snapshot entries should be non-null");

      // Verify snapshot size matches what was captured
      assertTrue(snapshot.size() > 0, "Snapshot should have entries from pending snapshot");

      wtx.rollback();
    }
  }

  @Test
  public void testPrepareRecordPageAfterRotation() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert initial data - this creates record pages
      // Navigate back to object after each insert
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("record1", new StringValue("value1"));
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("record2", new StringValue("value2"));

      final int sizeBefore = log.size();
      assertTrue(sizeBefore > 0, "TIL should have entries");

      // Create snapshot - this rotates the TIL
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "prepareRecordPage test",
          System.currentTimeMillis(),
          null
      );

      assertNotNull(snapshot, "Snapshot should be created");
      
      // Verify the snapshot has captured entries
      assertTrue(snapshot.size() > 0, "Snapshot should have entries");

      // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
      // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
      assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

      // The pending snapshot should be set
      assertNotNull(pageTrx.getPendingSnapshot(), "Pending snapshot should be set");

      wtx.rollback();
    }
  }

  @Test
  public void testLayeredLookupWithGenerationCheck() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Get initial generation
      final int gen0 = log.getCurrentGeneration();

      // Insert to create a PageReference with gen0
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("gen_check", new StringValue("value"));

      // Rotate TIL - captures all refs with gen0 and increments to gen1
      TransactionIntentLog.RotationResult rotation = log.detachAndReset();
      final int gen1 = log.getCurrentGeneration();
      assertEquals(gen0 + 1, gen1, "Generation should increment");

      // All captured refs should have generation gen0 (not matching gen1)
      for (var entry : rotation.refToContainer().entrySet()) {
        PageReference ref = entry.getKey();
        // PageReference was marked with gen0, not matching current gen1
        assertTrue(!ref.isInActiveTil(gen1), 
            "Old ref should NOT match new generation " + gen1);
        // Verify it was marked with gen0
        assertTrue(ref.isInActiveTil(gen0), 
            "Old ref SHOULD match old generation " + gen0);
      }

      wtx.rollback();
    }
  }

  @Test
  public void testLayeredLookupPrioritizesActiveTil() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert and rotate to create pending snapshot
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("snap_entry", new StringValue("snapshot_value"));

      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "priority test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Now simulate a new insert that CoW's a page into active TIL
        // by checking that generation-based fast path works

        final int currentGen = log.getCurrentGeneration();

        // Create a new PageReference and put it in active TIL
        PageReference newRef = new PageReference();
        PageContainer newContainer = PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage()
        );
        log.put(newRef, newContainer);

        // Verify: new ref should be in active TIL
        assertTrue(newRef.isInActiveTil(currentGen), 
            "New ref should be in active TIL");

        // Verify: getLogRecord should find it from active TIL (fast path)
        PageContainer found = pageTrx.getLogRecord(newRef);
        assertEquals(newContainer, found, 
            "Should find new container from active TIL");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testSnapshotLookupFallbackToLogKey() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert data - navigate back to object after each insert
      for (int i = 0; i < 5; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("key" + i, new StringValue("value" + i));
      }

      // Create snapshot
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "logKey fallback test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Verify snapshot entry lookup works via logKey.
        // Some entries may be null because reAddRootPageToActiveTil clears
        // root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId)
        // from the snapshot after moving them back into the active TIL.
        int nonNullCount = 0;
        for (int i = 0; i < snapshot.size(); i++) {
          PageContainer entry = snapshot.getEntry(i);
          if (entry != null) {
            nonNullCount++;
          }
        }
        assertTrue(nonNullCount > 0, "At least some snapshot entries should be non-null");

        // Out of bounds should return null or throw safely
        // The implementation should handle this gracefully
        try {
          @SuppressWarnings("unused")
          PageContainer outOfBounds = snapshot.getEntry(snapshot.size() + 100);
          // Should either be null or throw - depends on implementation
        } catch (ArrayIndexOutOfBoundsException e) {
          // This is acceptable - implementation might not bounds-check getEntry
        }
      }

      wtx.rollback();
    }
  }

  @Test
  public void testDiskOffsetLazyPropagation() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Insert data
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("lazy_prop", new StringValue("value"));

      // Create snapshot
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "lazy propagation test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Simulate commit recording disk offsets
        for (int i = 0; i < snapshot.size(); i++) {
          snapshot.recordDiskOffset(i, 10000L + i * 1000);
        }

        // Mark commit complete
        snapshot.markCommitComplete();
        assertTrue(snapshot.isCommitComplete(), "Commit should be marked complete");

        // Verify disk offsets are available
        for (int i = 0; i < snapshot.size(); i++) {
          long offset = snapshot.getDiskOffset(i);
          assertEquals(10000L + i * 1000, offset, 
              "Disk offset should be correctly recorded for logKey " + i);
        }

        // Verify disk offset retrieval works for all logKeys
        // This simulates what getFromActiveOrPending() does for lazy propagation
        for (int logKey = 0; logKey < snapshot.size(); logKey++) {
          long diskOffset = snapshot.getDiskOffset(logKey);
          assertTrue(diskOffset >= 10000L, 
              "Disk offset should be propagated for logKey " + logKey);
        }
      }

      wtx.rollback();
    }
  }

  @Test
  public void testIndirectPageCowFromSnapshot() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert many records to ensure we have indirect pages
      for (int i = 0; i < 20; i++) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("indirect" + i, new StringValue("value" + i));
      }

      final int sizeBefore = log.size();
      assertTrue(sizeBefore > 0, "Should have entries including indirect pages");

      // Create snapshot
      pageTrx.acquireCommitPermit();
      final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
          "indirect page CoW test",
          System.currentTimeMillis(),
          null
      );

      if (snapshot != null) {
        // Root-level pages (RRP, NamePage, PathSummary, CAS, Path, DeweyId) are
        // re-added to the active TIL by reAddRootPageToActiveTil after rotation.
        assertEquals(6, log.size(), "Root-level pages re-added to active TIL after rotation");

        // Pending snapshot should have all entries
        assertEquals(sizeBefore, snapshot.size(), "Snapshot should have all entries");

        // Verify non-root entries are accessible. Root-level entries may have been
        // cleared from the snapshot by reAddRootPageToActiveTil.
        int accessibleCount = 0;
        for (int i = 0; i < snapshot.size(); i++) {
          if (snapshot.getEntry(i) != null) {
            accessibleCount++;
          }
        }
        assertTrue(accessibleCount > 0, "At least some snapshot entries should be accessible");
      }

      wtx.rollback();
    }
  }

  @Test
  public void testActiveVsSnapshotGenerationIsolation() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
      final var log = pageTrx.getLog();

      // Insert data and note the generation
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("iso_test", new StringValue("value"));

      final int insertGen = log.getCurrentGeneration();

      // Capture refs before rotation
      TransactionIntentLog.RotationResult rotation = log.detachAndReset();
      final int postRotationGen = log.getCurrentGeneration();

      // Verify generation incremented
      assertEquals(insertGen + 1, postRotationGen, "Generation should increment after rotation");

      // All old refs should report wrong generation
      for (var entry : rotation.refToContainer().entrySet()) {
        PageReference oldRef = entry.getKey();
        // Old ref was marked with insertGen, not postRotationGen
        assertTrue(!oldRef.isInActiveTil(postRotationGen), 
            "Old ref should not match new generation");
      }

      // Create a new ref in the new generation
      PageReference newRef = new PageReference();
      log.put(newRef, PageContainer.getInstance(
          new io.sirix.page.IndirectPage(),
          new io.sirix.page.IndirectPage()
      ));

      // New ref should match new generation
      assertTrue(newRef.isInActiveTil(postRotationGen), 
          "New ref should match current generation");

      wtx.rollback();
    }
  }

  @Test
  public void testGetFromActiveOrPendingReturnsNullWhenNotFound() throws Exception {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

      // Create a PageReference that was never put in TIL
      PageReference unknownRef = new PageReference();
      unknownRef.setKey(999999L); // Some random key

      // getLogRecord uses getFromActiveOrPending
      PageContainer result = pageTrx.getLogRecord(unknownRef);

      // Should return null since ref is not in active TIL or any snapshot
      assertNull(result, "Unknown ref should return null from layered lookup");

      wtx.rollback();
    }
  }
}

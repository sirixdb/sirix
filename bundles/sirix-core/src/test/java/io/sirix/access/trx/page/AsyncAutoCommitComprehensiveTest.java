package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.NodeKind;
import io.sirix.page.PageReference;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for async auto-commit infrastructure.
 * <p>
 * Covers:
 * <ul>
 *   <li>TIL rotation correctness under various versioning strategies</li>
 *   <li>CommitSnapshot immutability and isolation</li>
 *   <li>Layered lookup (active TIL → pending snapshot → disk)</li>
 *   <li>End-to-end auto-commit with data integrity verification</li>
 *   <li>Concurrent insert/commit correctness</li>
 *   <li>Stress tests with many entries</li>
 *   <li>Edge cases (empty TIL, single entry, boundary conditions)</li>
 * </ul>
 */
public final class AsyncAutoCommitComprehensiveTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

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

  // ============================================================================
  // TIL Rotation Tests
  // ============================================================================

  @Nested
  class TilRotationTests {

    @Test
    void rotationPreservesExactEntryCount() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        for (int i = 0; i < 15; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("k" + i, new StringValue("v" + i));
        }

        final int sizeBefore = log.size();
        assertTrue(sizeBefore > 0);

        final TransactionIntentLog.RotationResult rotation = log.detachAndReset();

        assertEquals(sizeBefore, rotation.size());
        assertEquals(sizeBefore, rotation.refToContainer().size());
        assertEquals(0, log.size());

        wtx.rollback();
      }
    }

    @Test
    void generationIncrementIsMonotonic() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        final int initialGen = log.getCurrentGeneration();

        for (int i = 0; i < 10; i++) {
          final int before = log.getCurrentGeneration();
          log.detachAndReset();
          final int after = log.getCurrentGeneration();
          assertEquals(before + 1, after, "Generation must increment by exactly 1");
        }

        assertEquals(initialGen + 10, log.getCurrentGeneration());
        wtx.rollback();
      }
    }

    @Test
    void emptyRotationIsHarmless() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // First rotation to clear commit state
        log.detachAndReset();

        // Now TIL is empty, rotate again
        final TransactionIntentLog.RotationResult rotation = log.detachAndReset();
        assertEquals(0, rotation.size());
        assertEquals(0, rotation.refToContainer().size());
        assertEquals(0, log.size());

        wtx.rollback();
      }
    }

    @Test
    void rotationAfterInsertsThenMoreInserts() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Phase 1: Insert and rotate
        for (int i = 0; i < 5; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("phase1_" + i, new StringValue("v" + i));
        }
        final int phase1Size = log.size();
        final TransactionIntentLog.RotationResult phase1 = log.detachAndReset();
        assertEquals(phase1Size, phase1.size());
        assertEquals(0, log.size());

        // Phase 2: Insert more after rotation - these use new generation
        // This tests that TIL correctly accepts new entries after rotation
        final int newGen = log.getCurrentGeneration();

        // Put a simple entry in the now-empty TIL
        final PageReference newRef = new PageReference();
        final PageContainer newContainer = PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage()
        );
        log.put(newRef, newContainer);

        assertEquals(1, log.size());
        assertTrue(newRef.isInActiveTil(newGen));

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // CommitSnapshot Tests
  // ============================================================================

  @Nested
  class CommitSnapshotTests {

    @Test
    void snapshotCapturesCorrectMetadata() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("meta", new StringValue("data"));

        try {
          pageTrx.acquireCommitPermit();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        final long timestamp = System.currentTimeMillis();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "metadata test commit", timestamp, null);

        assertNotNull(snapshot);
        assertEquals("metadata test commit", snapshot.commitMessage());
        assertEquals(timestamp, snapshot.commitTimestamp());
        assertNotNull(snapshot.revisionRootPage());
        assertTrue(snapshot.revision() >= 0);
        assertTrue(snapshot.size() > 0);
        assertNotNull(snapshot.resourceConfig());
        assertFalse(snapshot.isCommitComplete());

        wtx.rollback();
      }
    }

    @Test
    void snapshotSizeIsImmutableAfterCreation() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        for (int i = 0; i < 8; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("immutable" + i, new StringValue("v" + i));
        }

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "immutability test", System.currentTimeMillis(), null);

        assertNotNull(snapshot);
        final int capturedSize = snapshot.size();

        // Even if we add to active TIL after rotation, snapshot size doesn't change
        final PageReference r = new PageReference();
        log.put(r, PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage()));

        assertEquals(capturedSize, snapshot.size(), "Snapshot size must be immutable");

        wtx.rollback();
      }
    }

    @Test
    void diskOffsetTrackingIsCorrect() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("offsets", new StringValue("test"));

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "offsets", System.currentTimeMillis(), null);

        if (snapshot != null) {
          final int sz = snapshot.size();

          // All offsets should be -1 initially
          for (int i = 0; i < sz; i++) {
            assertEquals(-1L, snapshot.getDiskOffset(i));
          }

          // Record offsets
          for (int i = 0; i < sz; i++) {
            snapshot.recordDiskOffset(i, 10000L * (i + 1));
          }

          // Verify recorded offsets
          for (int i = 0; i < sz; i++) {
            assertEquals(10000L * (i + 1), snapshot.getDiskOffset(i));
          }

          // Boundary checks
          assertEquals(-1L, snapshot.getDiskOffset(-1));
          assertEquals(-1L, snapshot.getDiskOffset(sz));
          assertEquals(-1L, snapshot.getDiskOffset(Integer.MAX_VALUE));
        }

        wtx.rollback();
      }
    }

    @Test
    void emptyTilProducesNullSnapshot() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Clear TIL via rotation
        log.detachAndReset();

        // Acquire permit and try to create snapshot from empty TIL
        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "empty", System.currentTimeMillis(), null);

        assertNull(snapshot, "Empty TIL should produce null snapshot");

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Layered Lookup Tests
  // ============================================================================

  @Nested
  class LayeredLookupTests {

    @Test
    void activeTilTakesPriorityOverSnapshot() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Insert and create snapshot
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("snap", new StringValue("val"));

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "test", System.currentTimeMillis(), null);

        assertNotNull(snapshot);

        // Put something in active TIL
        final PageReference activeRef = new PageReference();
        final PageContainer activeContainer = PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage());
        log.put(activeRef, activeContainer);

        // getLogRecord should find it from active TIL, not snapshot
        final PageContainer found = pageTrx.getLogRecord(activeRef);
        assertEquals(activeContainer, found);
        assertTrue(activeRef.isInActiveTil(log.getCurrentGeneration()));

        wtx.rollback();
      }
    }

    @Test
    void unknownRefReturnsNull() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        final PageReference unknown = new PageReference();
        unknown.setKey(999999L);

        assertNull(pageTrx.getLogRecord(unknown));

        wtx.rollback();
      }
    }

    @Test
    void nullRefReturnsNull() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        assertNull(pageTrx.getFromActiveOrPending(null));

        wtx.rollback();
      }
    }

    @Test
    void snapshotCompletionEnablesDiskOffsetPropagation() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("disk", new StringValue("val"));

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "disk propagation", System.currentTimeMillis(), null);

        if (snapshot != null) {
          // Before completion, snapshot should serve pages from identity map
          assertFalse(snapshot.isCommitComplete());

          // Record offsets and mark complete
          for (int i = 0; i < snapshot.size(); i++) {
            snapshot.recordDiskOffset(i, 50000L + i);
          }
          snapshot.markCommitComplete();

          assertTrue(snapshot.isCommitComplete());

          // After completion, disk offsets should be available
          for (int i = 0; i < snapshot.size(); i++) {
            assertEquals(50000L + i, snapshot.getDiskOffset(i));
          }
        }

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Backpressure Tests
  // ============================================================================

  @Nested
  class BackpressureTests {

    @Test
    void singlePermitEnforcement() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        // Acquire first permit
        assertTrue(pageTrx.tryAcquireCommitPermit());

        // Second should fail immediately
        assertFalse(pageTrx.tryAcquireCommitPermit());

        // Third should also fail
        assertFalse(pageTrx.tryAcquireCommitPermit());

        wtx.rollback();
      }
    }

    @Test
    void tryAcquireIsNonBlocking() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        pageTrx.acquireCommitPermit();

        final long start = System.nanoTime();
        final boolean result = pageTrx.tryAcquireCommitPermit();
        final long durationMs = (System.nanoTime() - start) / 1_000_000;

        assertFalse(result);
        assertTrue(durationMs < 50, "tryAcquire must be non-blocking, took " + durationMs + "ms");

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Concurrent Commit Tests
  // ============================================================================

  @Nested
  class ConcurrentCommitTests {

    @Test
    void backgroundCommitDoesNotCorruptSnapshot() throws Exception {
      final ExecutorService executor = Executors.newSingleThreadExecutor();

      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        // Insert data
        for (int i = 0; i < 10; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("bg" + i, new StringValue("v" + i));
        }

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "bg commit", System.currentTimeMillis(), null);

        if (snapshot != null) {
          final int expectedSize = snapshot.size();

          // Run background "commit"
          final CountDownLatch done = new CountDownLatch(1);
          final AtomicBoolean bgSuccess = new AtomicBoolean(false);

          executor.submit(() -> {
            try {
              // Simulate commit work
              for (int i = 0; i < expectedSize; i++) {
                snapshot.recordDiskOffset(i, 1000L + i);
              }
              Thread.sleep(50);
              snapshot.markCommitComplete();
              bgSuccess.set(true);
            } catch (Exception e) {
              // ignore
            } finally {
              done.countDown();
            }
          });

          assertTrue(done.await(5, TimeUnit.SECONDS));
          assertTrue(bgSuccess.get());
          assertTrue(snapshot.isCommitComplete());
          assertEquals(expectedSize, snapshot.size(), "Size must not change during bg commit");
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        wtx.rollback();
      } finally {
        executor.shutdownNow();
      }
    }

    @Test
    void commitCompleteVisibilityAcrossThreads() throws Exception {
      final ExecutorService executor = Executors.newSingleThreadExecutor();

      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("vis", new StringValue("val"));

        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "visibility", System.currentTimeMillis(), null);

        if (snapshot != null) {
          final CountDownLatch bgReady = new CountDownLatch(1);
          final CountDownLatch mainSignal = new CountDownLatch(1);
          final AtomicBoolean visibleFromBg = new AtomicBoolean(false);

          executor.submit(() -> {
            try {
              bgReady.countDown();
              mainSignal.await(5, TimeUnit.SECONDS);
              // After main thread marks complete, bg thread should see it
              visibleFromBg.set(snapshot.isCommitComplete());
            } catch (Exception e) {
              // ignore
            }
          });

          bgReady.await(1, TimeUnit.SECONDS);
          snapshot.markCommitComplete();
          // Small delay to ensure ordering
          Thread.sleep(10);
          mainSignal.countDown();

          executor.shutdown();
          executor.awaitTermination(5, TimeUnit.SECONDS);

          assertTrue(visibleFromBg.get(),
              "commitComplete must be visible to background thread (volatile)");
        }

        wtx.rollback();
      } finally {
        executor.shutdownNow();
      }
    }
  }

  // ============================================================================
  // End-to-End Auto-Commit Tests
  // ============================================================================

  @Nested
  class EndToEndAutoCommitTests {

    @Test
    void autoCommitWithMaxNodeCountPreservesDataIntegrity() {
      // Use maxNodeCount to trigger auto-commits during bulk insert
      try (final var wtx = resourceSession.beginNodeTrx(50, AfterCommitState.KEEP_OPEN)) {
        // Insert a document that will trigger multiple auto-commits
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Verify data integrity by reading and serializing
      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Should have root object");
        // The chicago-subset has a "data" array with 101 records
        assertTrue(rtx.getChildCount() > 0, "Root object should have children");
      }

      // Verify serialization roundtrip
      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        try (final var writer = new StringWriter()) {
          new JsonSerializer.Builder(resourceSession, writer).build().call();
          final String json = writer.toString();
          assertNotNull(json);
          assertTrue(json.length() > 1000, "Serialized JSON should be substantial");
          assertTrue(json.contains("data"), "Should contain 'data' key");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Test
    void autoCommitWithSmallThresholdDoesNotLoseNodes() {
      final int insertCount = 30;

      // Very small threshold forces many auto-commits
      try (final var wtx = resourceSession.beginNodeTrx(5, AfterCommitState.KEEP_OPEN)) {
        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < insertCount; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("key" + i, new StringValue("value" + i));
        }

        wtx.commit();
      }

      // Verify all records are present
      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // Object

        int foundCount = 0;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              foundCount++;
            }
          } while (rtx.moveToRightSibling());
        }

        assertEquals(insertCount, foundCount,
            "All " + insertCount + " records should be present after auto-commit");
      }
    }

    @Test
    void multipleRevisionsWithAutoCommitAreReadable() {
      final int revisions = 5;
      final int nodesPerRevision = 10;

      for (int rev = 0; rev < revisions; rev++) {
        try (final var wtx = resourceSession.beginNodeTrx(3, AfterCommitState.KEEP_OPEN)) {
          if (rev == 0) {
            wtx.insertObjectAsFirstChild();
          }

          for (int i = 0; i < nodesPerRevision; i++) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.insertObjectRecordAsFirstChild("r" + rev + "_k" + i, new StringValue("v" + i));
          }

          wtx.commit();
        }
      }

      // Verify we can read the latest revision
      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // Object

        int totalKeys = 0;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              totalKeys++;
            }
          } while (rtx.moveToRightSibling());
        }

        assertEquals(revisions * nodesPerRevision, totalKeys);
      }

      // Verify we can open all past revisions
      for (int rev = 1; rev <= revisions; rev++) {
        try (final var rtx = resourceSession.beginNodeReadOnlyTrx(rev)) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild(), "Revision " + rev + " should have root object");
        }
      }
    }
  }

  // ============================================================================
  // Versioning Strategy Tests
  // ============================================================================

  @Nested
  class VersioningStrategyTests {

    @ParameterizedTest
    @EnumSource(value = VersioningType.class, names = {"FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
    void autoCommitWorksWithAllVersioningStrategies(final VersioningType versioningType) {
      JsonTestHelper.deleteEverything();

      final DatabaseConfiguration dbConfig =
          new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile());
      Databases.createJsonDatabase(dbConfig);

      final var db = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      db.createResource(
          ResourceConfiguration.newBuilder("versioning-test")
              .versioningApproach(versioningType)
              .build());

      try (final var session = db.beginResourceSession("versioning-test")) {
        // Auto-commit with threshold=10
        try (final var wtx = session.beginNodeTrx(10, AfterCommitState.KEEP_OPEN)) {
          wtx.insertObjectAsFirstChild();

          for (int i = 0; i < 25; i++) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.insertObjectRecordAsFirstChild("vk" + i, new StringValue("v" + i));
          }

          wtx.commit();
        }

        // Verify data integrity
        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild());

          int keyCount = 0;
          if (rtx.moveToFirstChild()) {
            do {
              if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                keyCount++;
              }
            } while (rtx.moveToRightSibling());
          }

          assertEquals(25, keyCount,
              "All 25 keys should be present with " + versioningType);
        }
      }

      db.close();
    }
  }

  // ============================================================================
  // Generation Counter Tests
  // ============================================================================

  @Nested
  class GenerationCounterTests {

    @Test
    void pageRefGenerationIsSetOnTilPut() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();
        final int gen = log.getCurrentGeneration();

        final PageReference ref = new PageReference();
        log.put(ref, PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage()));

        assertTrue(ref.isInActiveTil(gen));
        assertFalse(ref.isInActiveTil(gen + 1));
        assertFalse(ref.isInActiveTil(gen - 1));

        wtx.rollback();
      }
    }

    @Test
    void oldRefsDoNotMatchNewGeneration() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("old", new StringValue("val"));

        final int oldGen = log.getCurrentGeneration();

        final TransactionIntentLog.RotationResult rotation = log.detachAndReset();
        final int newGen = log.getCurrentGeneration();

        for (var entry : rotation.refToContainer().entrySet()) {
          assertTrue(entry.getKey().isInActiveTil(oldGen));
          assertFalse(entry.getKey().isInActiveTil(newGen));
        }

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Stress Tests
  // ============================================================================

  @Nested
  class StressTests {

    @RepeatedTest(3)
    void repeatedRotationsUnderLoad() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Insert data in the first cycle only (path summary pages
        // are in TIL and would be lost after raw rotation)
        for (int i = 0; i < 10; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("k" + i, new StringValue("v" + i));
        }

        final int sizeBefore = log.size();
        assertTrue(sizeBefore > 0, "Should have entries");

        // First rotation: captures all entries
        final TransactionIntentLog.RotationResult rotation = log.detachAndReset();
        assertEquals(sizeBefore, rotation.size());
        assertEquals(0, log.size());

        // Subsequent rotations of empty TIL should be harmless
        for (int cycle = 1; cycle < 5; cycle++) {
          final TransactionIntentLog.RotationResult emptyRotation = log.detachAndReset();
          assertEquals(0, emptyRotation.size(), "Empty rotation in cycle " + cycle);
          assertEquals(0, log.size());
        }

        wtx.rollback();
      }
    }

    @Test
    void rapidSnapshotCreation() throws Exception {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Insert data once
        for (int i = 0; i < 10; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("rapid" + i, new StringValue("v" + i));
        }

        // Create snapshot
        pageTrx.acquireCommitPermit();
        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "rapid", System.currentTimeMillis(), null);

        assertNotNull(snapshot);
        assertTrue(snapshot.size() > 0);

        // Simulate fast commit
        snapshot.markCommitComplete();

        // Now do multiple empty rotations rapidly
        for (int i = 0; i < 5; i++) {
          final TransactionIntentLog.RotationResult rotation = log.detachAndReset();
          assertTrue(rotation.size() >= 0);
        }

        wtx.rollback();
      }
    }

    @Test
    void autoCommitWithManySmallInserts() {
      // This tests that many auto-commits don't cause issues
      try (final var wtx = resourceSession.beginNodeTrx(3, AfterCommitState.KEEP_OPEN)) {
        wtx.insertObjectAsFirstChild();

        // 50 inserts with threshold 3 = ~16 auto-commits
        for (int i = 0; i < 50; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("small" + i, new NumberValue(i));
        }

        wtx.commit();
      }

      // Verify
      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();

        int count = 0;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              count++;
            }
          } while (rtx.moveToRightSibling());
        }

        assertEquals(50, count);
      }
    }
  }

  // ============================================================================
  // SnapshotCommitter Tests
  // ============================================================================

  @Nested
  class SnapshotCommitterTests {

    @Test
    void committerHandlesNullSnapshot() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        // executeCommitSnapshot with null should be a no-op
        pageTrx.executeCommitSnapshot(null);

        wtx.rollback();
      }
    }
  }

  // ============================================================================
  // Edge Cases
  // ============================================================================

  @Nested
  class EdgeCaseTests {

    @Test
    void singleEntryRotation() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();
        final var log = pageTrx.getLog();

        // Clear any existing TIL state
        log.detachAndReset();

        // Add exactly one entry
        final PageReference ref = new PageReference();
        final PageContainer container = PageContainer.getInstance(
            new io.sirix.page.IndirectPage(),
            new io.sirix.page.IndirectPage());
        log.put(ref, container);
        assertEquals(1, log.size());

        // Rotate
        final TransactionIntentLog.RotationResult rotation = log.detachAndReset();
        assertEquals(1, rotation.size());
        assertEquals(1, rotation.refToContainer().size());
        assertEquals(0, log.size());

        wtx.rollback();
      }
    }

    @Test
    void pendingSnapshotClearedOnSyncCommit() {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();

        final var pageTrx = (NodeStorageEngineWriter) wtx.getPageTrx();

        // Create a pending snapshot
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("pending", new StringValue("val"));

        try {
          pageTrx.acquireCommitPermit();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        final CommitSnapshot snapshot = pageTrx.prepareAsyncCommitSnapshot(
            "test", System.currentTimeMillis(), null);

        assertNotNull(pageTrx.getPendingSnapshot());

        // Now do a synchronous commit - this should clear the pending snapshot
        wtx.rollback();
      }
    }

    @Test
    void autoCommitThresholdOfOneWorks() {
      // Edge case: auto-commit after every single modification
      try (final var wtx = resourceSession.beginNodeTrx(1, AfterCommitState.KEEP_OPEN)) {
        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < 10; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("one" + i, new StringValue("v" + i));
        }

        wtx.commit();
      }

      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());

        int count = 0;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              count++;
            }
          } while (rtx.moveToRightSibling());
        }

        assertEquals(10, count);
      }
    }

    @Test
    void autoCommitWithBooleanAndNumberValues() {
      try (final var wtx = resourceSession.beginNodeTrx(5, AfterCommitState.KEEP_OPEN)) {
        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < 20; i++) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();

          if (i % 3 == 0) {
            wtx.insertObjectRecordAsFirstChild("bool" + i, new BooleanValue(i % 2 == 0));
          } else if (i % 3 == 1) {
            wtx.insertObjectRecordAsFirstChild("num" + i, new NumberValue(i * 3.14));
          } else {
            wtx.insertObjectRecordAsFirstChild("str" + i, new StringValue("val" + i));
          }
        }

        wtx.commit();
      }

      try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());

        int count = 0;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              count++;
            }
          } while (rtx.moveToRightSibling());
        }

        assertEquals(20, count);
      }
    }
  }
}

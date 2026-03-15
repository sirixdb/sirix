package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for concurrent access patterns on JSON resources.
 *
 * <p>Sirix allows only ONE write transaction per resource at a time.
 * Multiple read-only transactions are allowed concurrently.</p>
 */
public final class JsonConcurrentAccessTest {

  private static final String SAMPLE_JSON = "{\"key\":\"value\",\"number\":42,\"nested\":{\"a\":1,\"b\":2}}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Open 3 read-only transactions on the same revision and verify they all read the same data.
   */
  @Test
  void testMultipleReadOnlyTrxSameRevision() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(SAMPLE_JSON));
        wtx.commit();
      }

      // Open 3 read-only transactions on the same revision
      try (final var rtx1 = session.beginNodeReadOnlyTrx();
           final var rtx2 = session.beginNodeReadOnlyTrx();
           final var rtx3 = session.beginNodeReadOnlyTrx()) {

        assertEquals(rtx1.getRevisionNumber(), rtx2.getRevisionNumber());
        assertEquals(rtx2.getRevisionNumber(), rtx3.getRevisionNumber());

        // All should see the document root with children
        rtx1.moveToDocumentRoot();
        rtx2.moveToDocumentRoot();
        rtx3.moveToDocumentRoot();

        assertTrue(rtx1.hasFirstChild());
        assertTrue(rtx2.hasFirstChild());
        assertTrue(rtx3.hasFirstChild());

        final long descendants1 = rtx1.getDescendantCount();
        final long descendants2 = rtx2.getDescendantCount();
        final long descendants3 = rtx3.getDescendantCount();

        assertEquals(descendants1, descendants2);
        assertEquals(descendants2, descendants3);

        // Verify all readers can navigate to the same first child
        assertTrue(rtx1.moveToFirstChild());
        assertTrue(rtx2.moveToFirstChild());
        assertTrue(rtx3.moveToFirstChild());

        assertEquals(rtx1.getNodeKey(), rtx2.getNodeKey());
        assertEquals(rtx2.getNodeKey(), rtx3.getNodeKey());
      }
    }
  }

  /**
   * Open a read-only transaction, then a write transaction modifies the data.
   * The read-only transaction should still see the old data (snapshot isolation).
   */
  @Test
  void testReadOnlyDuringWrite() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"));
        wtx.commit();
      }

      // Open read-only trx on revision 1
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        final int revBeforeWrite = rtx.getRevisionNumber();
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild()); // array node
        final long childCountBefore = rtx.getChildCount();

        // Now open a write trx and modify the data
        try (final var wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array node
          wtx.insertNumberValueAsLastChild(4);
          wtx.commit();
        }

        // The read-only trx should still see the old data
        assertEquals(revBeforeWrite, rtx.getRevisionNumber());
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(childCountBefore, rtx.getChildCount());
      }
    }
  }

  /**
   * A write transaction commits new data; a new read-only transaction opened after
   * the commit should see the new data.
   */
  @Test
  void testReadAfterCommit() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data and commit
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"));
        wtx.commit();
      }

      final int revAfterFirstCommit = session.getMostRecentRevisionNumber();

      // Insert more data and commit
      try (final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array node
        wtx.insertNumberValueAsLastChild(4);
        wtx.commit();
      }

      final int revAfterSecondCommit = session.getMostRecentRevisionNumber();
      assertTrue(revAfterSecondCommit > revAfterFirstCommit);

      // New read-only trx should see the latest committed data
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(revAfterSecondCommit, rtx.getRevisionNumber());
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild()); // array node
        assertEquals(4, rtx.getChildCount()); // 1, 2, 3, 4
      }
    }
  }

  /**
   * A write transaction inserts data but does NOT commit. A read-only transaction
   * opened concurrently should NOT see the uncommitted changes.
   */
  @Test
  void testReadBeforeCommit() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data and commit
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"));
        wtx.commit();
      }

      // Open write trx, insert but do NOT commit
      try (final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array
        wtx.insertNumberValueAsLastChild(4);
        // No commit here!

        // Open read-only trx — should still see only [1,2,3]
        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild()); // array
          assertEquals(3, rtx.getChildCount()); // only 1, 2, 3
        }

        // Rollback the uncommitted changes so the write trx can be closed
        wtx.rollback();
      }
    }
  }

  /**
   * Two readers on different revisions see different data.
   */
  @Test
  void testConcurrentReadersDifferentRevisions() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Revision 1: [1,2,3]
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"));
        wtx.commit();
      }

      final int rev1 = session.getMostRecentRevisionNumber();

      // Revision 2: add element 4 => [1,2,3,4]
      try (final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array node
        wtx.insertNumberValueAsLastChild(4);
        wtx.commit();
      }

      final int rev2 = session.getMostRecentRevisionNumber();
      assertTrue(rev2 > rev1);

      // Open readers on both revisions simultaneously
      try (final var rtxRev1 = session.beginNodeReadOnlyTrx(rev1);
           final var rtxRev2 = session.beginNodeReadOnlyTrx(rev2)) {

        assertEquals(rev1, rtxRev1.getRevisionNumber());
        assertEquals(rev2, rtxRev2.getRevisionNumber());

        rtxRev1.moveToDocumentRoot();
        rtxRev1.moveToFirstChild();
        assertEquals(3, rtxRev1.getChildCount());

        rtxRev2.moveToDocumentRoot();
        rtxRev2.moveToFirstChild();
        assertEquals(4, rtxRev2.getChildCount());
      }
    }
  }

  /**
   * Spawn 4 threads, each opens a read-only transaction and verifies the same data.
   */
  @Test
  void testConcurrentReaderThreadsVerifySameData() throws InterruptedException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert data
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[10,20,30,40,50]"));
        wtx.commit();
      }

      final int threadCount = 4;
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(threadCount);
      final AtomicReference<Throwable> failure = new AtomicReference<>(null);
      final long[] childCounts = new long[threadCount];

      final List<Thread> threads = new ArrayList<>(threadCount);

      for (int i = 0; i < threadCount; i++) {
        final int index = i;
        final Thread thread = new Thread(() -> {
          try {
            startLatch.await();
            try (final var rtx = session.beginNodeReadOnlyTrx()) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.moveToFirstChild()); // array node
              childCounts[index] = rtx.getChildCount();
            }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
          } finally {
            doneLatch.countDown();
          }
        });
        threads.add(thread);
        thread.start();
      }

      // Release all threads at once
      startLatch.countDown();
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads did not finish in time");

      assertNull(failure.get(), () -> "Thread failure: " + failure.get());

      // All threads should have seen the same child count
      for (int i = 0; i < threadCount; i++) {
        assertEquals(5, childCounts[i], "Thread " + i + " saw wrong child count");
      }
    }
  }

  /**
   * Close a write transaction, then open a new write transaction successfully.
   */
  @Test
  void testWriterCloseAllowsNewWriter() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // First write transaction
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"first\":true}"));
        wtx.commit();
      }

      // Second write transaction should succeed now that the first is closed
      try (final var wtx = session.beginNodeTrx()) {
        assertNotNull(wtx);
        // Verify we can read the data written by the first transaction
        wtx.moveToDocumentRoot();
        assertTrue(wtx.hasFirstChild());
      }
    }
  }

  /**
   * A read-only transaction opened before a commit still sees the pre-commit state,
   * even after the commit completes.
   */
  @Test
  void testSnapshotIsolation() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"version\":1}"));
        wtx.commit();
      }

      final int rev1 = session.getMostRecentRevisionNumber();

      // Open reader on rev1 — this is the snapshot
      try (final var rtx = session.beginNodeReadOnlyTrx(rev1)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild()); // object node
        final long descendantsBefore = rtx.getDescendantCount();

        // Now create revision 2
        try (final var wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // object
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"version\":2,\"extra\":\"data\"}"));
          wtx.commit();
        }

        // Verify that the snapshot reader still sees revision 1 data
        assertEquals(rev1, rtx.getRevisionNumber());
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(descendantsBefore, rtx.getDescendantCount());
      }
    }
  }

  /**
   * Write, commit, close, then write again, commit, close.
   * Verify both sets of changes are visible.
   */
  @Test
  void testSequentialWriteTransactions() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // First write: insert an array with [1]
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"));
        wtx.commit();
      }

      final int rev1 = session.getMostRecentRevisionNumber();

      // Verify revision 1 has 1 child in the array
      try (final var rtx = session.beginNodeReadOnlyTrx(rev1)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(1, rtx.getChildCount());
      }

      // Second write: add element 2
      try (final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array
        wtx.insertNumberValueAsLastChild(2);
        wtx.commit();
      }

      final int rev2 = session.getMostRecentRevisionNumber();
      assertTrue(rev2 > rev1);

      // Verify revision 2 has 2 children
      try (final var rtx = session.beginNodeReadOnlyTrx(rev2)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(2, rtx.getChildCount());
      }

      // Revision 1 should still show 1 child
      try (final var rtx = session.beginNodeReadOnlyTrx(rev1)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(1, rtx.getChildCount());
      }
    }
  }

  /**
   * Open both a read-only and a write transaction on the same session concurrently.
   */
  @Test
  void testReaderAndWriterOnSameSession() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"));
        wtx.commit();
      }

      // Open read-only trx and write trx simultaneously
      try (final var rtx = session.beginNodeReadOnlyTrx();
           final var wtx = session.beginNodeTrx()) {

        // Reader sees current state
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild()); // object
        final long readerDescendants = rtx.getDescendantCount();
        assertTrue(readerDescendants > 0);

        // Writer can insert new data
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // object
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"b\":2}"));
        wtx.commit();

        // Reader still sees the old number of descendants (snapshot isolation)
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(readerDescendants, rtx.getDescendantCount());
      }
    }
  }

  /**
   * Multiple readers concurrently count descendants using the DescendantAxis.
   */
  @Test
  void testMultipleReadersCountDescendants() throws InterruptedException, ExecutionException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert data with several levels of nesting
      final String json = "{\"a\":{\"b\":{\"c\":1}},\"d\":[1,2,3],\"e\":\"hello\"}";
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();
      }

      // Count descendants with a single reader first as reference
      final long expectedCount;
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        long count = 0;
        final var axis = new DescendantAxis(rtx);
        while (axis.hasNext()) {
          axis.nextLong();
          count++;
        }
        expectedCount = count;
      }

      assertTrue(expectedCount > 0, "Should have descendants");

      // Now spawn multiple threads to count descendants concurrently
      final int threadCount = 4;
      final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
      try {
        final List<Future<Long>> futures = new ArrayList<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
          futures.add(executor.submit(() -> {
            try (final var rtx = session.beginNodeReadOnlyTrx()) {
              rtx.moveToDocumentRoot();
              long count = 0;
              final var axis = new DescendantAxis(rtx);
              while (axis.hasNext()) {
                axis.nextLong();
                count++;
              }
              return count;
            }
          }));
        }

        for (int i = 0; i < threadCount; i++) {
          final long count = futures.get(i).get();
          assertEquals(expectedCount, count, "Thread " + i + " saw different descendant count");
        }
      } finally {
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  /**
   * Open a reader, then a writer modifies data. The reader must still see the original state.
   */
  @Test
  void testWriterModifiesReaderUnaffected() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE)) {

      // Insert initial data: {"name":"original","items":[1,2]}
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"name\":\"original\",\"items\":[1,2]}"));
        wtx.commit();
      }

      final int revBeforeModification = session.getMostRecentRevisionNumber();

      // Open reader on the current revision
      try (final var rtx = session.beginNodeReadOnlyTrx(revBeforeModification)) {
        // Navigate to "name" object key, then to its string value child
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild()); // object
        assertTrue(rtx.moveToFirstChild()); // first object key ("name" or "items")
        // Find the "name" key
        long nameValueKey = -1;
        do {
          if (rtx.isObjectKey() && "name".equals(rtx.getName().getLocalName())) {
            assertTrue(rtx.moveToFirstChild()); // string value
            nameValueKey = rtx.getNodeKey();
            break;
          }
        } while (rtx.moveToRightSibling());
        assertTrue(nameValueKey >= 0, "Should find 'name' key");

        final String originalValue = rtx.getValue();
        assertEquals("original", originalValue);

        // Now writer modifies the document in a new revision
        try (final var wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // object
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"extra\":\"added\"}"));
          wtx.commit();
        }

        // Reader should still see "original" at the same position
        rtx.moveTo(nameValueKey);
        assertEquals("original", rtx.getValue());
        assertEquals(revBeforeModification, rtx.getRevisionNumber());
      }

      // A new reader should see the new revision
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.getRevisionNumber() > revBeforeModification);
      }
    }
  }
}

package io.sirix.page;

import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for PageReference with global BufferManager composite keys. Validates that PageReferences
 * from different databases and resources are properly distinguished to prevent cache collisions.
 *
 * @author Johannes Lichtenberger
 */
public class PageReferenceGlobalBufferTest {

  @Test
  public void testPageReferenceEqualityWithSameDatabaseAndResource() {
    final var ref1 =
        new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(Constants.NULL_ID_INT);

    final var ref2 =
        new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(Constants.NULL_ID_INT);

    assertEquals(ref1, ref2, "PageReferences with same IDs should be equal");
    assertEquals(ref1.hashCode(), ref2.hashCode(), "Hash codes should match for equal PageReferences");
  }

  @Test
  public void testPageReferenceInequalityWithDifferentDatabases() {
    final var ref1 =
        new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(Constants.NULL_ID_INT);

    final var ref2 = new PageReference().setDatabaseId(2) // Different database ID
                                        .setResourceId(10)
                                        .setKey(100)
                                        .setLogKey(Constants.NULL_ID_INT);

    assertNotEquals(ref1, ref2, "PageReferences from different databases should NOT be equal");
    assertNotEquals(ref1.hashCode(), ref2.hashCode(),
        "Hash codes should differ for PageReferences from different databases");
  }

  @Test
  public void testPageReferenceInequalityWithDifferentResources() {
    final var ref1 =
        new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(Constants.NULL_ID_INT);

    final var ref2 = new PageReference().setDatabaseId(1)
                                        .setResourceId(20) // Different resource ID
                                        .setKey(100)
                                        .setLogKey(Constants.NULL_ID_INT);

    assertNotEquals(ref1, ref2, "PageReferences from different resources should NOT be equal");
    assertNotEquals(ref1.hashCode(), ref2.hashCode(),
        "Hash codes should differ for PageReferences from different resources");
  }

  @Test
  public void testPageReferenceEqualityWithSameOffsetDifferentDatabase() {
    // This tests the critical fix: pages at same offset in different databases
    // must NOT collide in the global buffer pool
    final var dbPageAtOffset1000 =
        new PageReference().setDatabaseId(1).setResourceId(1).setKey(1000).setLogKey(Constants.NULL_ID_INT);

    final var db2PageAtOffset1000 = new PageReference().setDatabaseId(2) // Different database
                                                       .setResourceId(1)
                                                       .setKey(1000) // SAME offset!
                                                       .setLogKey(Constants.NULL_ID_INT);

    assertNotEquals(dbPageAtOffset1000, db2PageAtOffset1000,
        "Pages at same offset in different databases MUST NOT collide");
    assertNotEquals(dbPageAtOffset1000.hashCode(), db2PageAtOffset1000.hashCode(),
        "Hash codes must differ even with same offset");
  }

  @Test
  public void testPageReferenceEqualityWithSameOffsetDifferentResource() {
    // Tests that pages at same offset in different resources don't collide
    final var resource1PageAtOffset500 =
        new PageReference().setDatabaseId(1).setResourceId(1).setKey(500).setLogKey(Constants.NULL_ID_INT);

    final var resource2PageAtOffset500 = new PageReference().setDatabaseId(1)
                                                            .setResourceId(2) // Different resource
                                                            .setKey(500) // SAME offset!
                                                            .setLogKey(Constants.NULL_ID_INT);

    assertNotEquals(resource1PageAtOffset500, resource2PageAtOffset500,
        "Pages at same offset in different resources MUST NOT collide");
    assertNotEquals(resource1PageAtOffset500.hashCode(), resource2PageAtOffset500.hashCode(),
        "Hash codes must differ even with same offset and database");
  }

  @Test
  public void testPageReferenceCopyConstructorCopiesIds() {
    final var original = new PageReference().setDatabaseId(5).setResourceId(15).setKey(200).setLogKey(10);

    final var copy = new PageReference(original);

    assertEquals(original.getDatabaseId(), copy.getDatabaseId(), "Database ID should be copied");
    assertEquals(original.getResourceId(), copy.getResourceId(), "Resource ID should be copied");
    assertEquals(original.getKey(), copy.getKey(), "Key should be copied");
    assertEquals(original.getLogKey(), copy.getLogKey(), "Log key should be copied");
    assertEquals(original, copy, "Copy should equal original");
    assertEquals(original.hashCode(), copy.hashCode(), "Hash codes should match");
  }

  @Test
  public void testPageReferenceWithLogKey() {
    // Test that logKey is also part of equality
    final var ref1 = new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(5);

    final var ref2 = new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(6); // Different log
                                                                                                      // key

    assertNotEquals(ref1, ref2, "PageReferences with different log keys should NOT be equal");
  }

  @Test
  public void testPageReferenceToString() {
    final var ref = new PageReference().setDatabaseId(7).setResourceId(42).setKey(1000).setLogKey(5);

    final String toString = ref.toString();

    assertTrue(toString.contains("databaseId=7"), "toString should include database ID");
    assertTrue(toString.contains("resourceId=42"), "toString should include resource ID");
    assertTrue(toString.contains("key=1000"), "toString should include key");
    assertTrue(toString.contains("logKey=5"), "toString should include log key");
  }

  @Test
  public void testHashCodeConsistency() {
    final var ref = new PageReference().setDatabaseId(1).setResourceId(10).setKey(100).setLogKey(Constants.NULL_ID_INT);

    final int hash1 = ref.hashCode();
    final int hash2 = ref.hashCode();

    assertEquals(hash1, hash2, "hashCode should be consistent across multiple calls");
  }

  @Test
  public void testHashCodeResetOnIdChange() {
    final var ref = new PageReference().setDatabaseId(1).setResourceId(10).setKey(100);

    final int originalHash = ref.hashCode();

    // Change database ID - should reset hash
    ref.setDatabaseId(2);
    final int newHash = ref.hashCode();

    assertNotEquals(originalHash, newHash, "Hash code should change when database ID is changed");
  }

  @Test
  public void staleIdentityCleanupCannotEraseConcurrentReplacement() throws Exception {
    final PageReference reference = new PageReference();
    final UberPage observedPage = new UberPage();
    final UberPage replacement = new UberPage();
    reference.setPage(observedPage);

    final CountDownLatch observationComplete = new CountDownLatch(1);
    final CountDownLatch allowStaleCleanup = new CountDownLatch(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<Boolean> staleCleanup = executor.submit(() -> {
        final var observed = reference.getPage();
        observationComplete.countDown();
        if (!allowStaleCleanup.await(5, TimeUnit.SECONDS)) {
          throw new AssertionError("timed out waiting to perform stale swizzle cleanup");
        }
        return reference.clearPageIfSame(observed);
      });

      assertTrue(observationComplete.await(5, TimeUnit.SECONDS));
      reference.setPage(replacement);
      allowStaleCleanup.countDown();

      assertFalse(staleCleanup.get(5, TimeUnit.SECONDS),
          "a stale exact-identity cleanup must lose to the replacement publication");
      assertSame(replacement, reference.getPage(), "the concurrent replacement must remain swizzled");
    } finally {
      allowStaleCleanup.countDown();
      executor.shutdownNow();
    }
  }
}


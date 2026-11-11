package io.sirix.query.bench.bitemporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.Databases;
import io.sirix.access.trx.page.StorageEpochTestSupport;
import io.sirix.cache.BufferManagerImpl;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/** Resolved HOT siblings must survive allocations during SH1's commit-time splits. */
@Isolated
final class BitemporalStorageEpochEvictionTest {
  @TempDir
  Path directory;

  private long previousRecordCacheBytes;
  private long previousFragmentCacheBytes;

  @AfterEach
  void restore() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
    Databases.clearGlobalCaches();
    if (previousRecordCacheBytes > 0) {
      Databases.reinitializeBufferManagerForTesting(previousRecordCacheBytes, previousFragmentCacheBytes);
    }
  }

  @Test
  void unboundedLoadMatchesColdPublicationsUnderEviction() throws Exception {
    assertColdPublicationsUnderEviction(Integer.MAX_VALUE);
  }

  @Test
  void boundedLoadMatchesColdPublicationsUnderEviction() throws Exception {
    assertColdPublicationsUnderEviction(StorageEpochTestSupport.IMPORT_BOUND);
  }

  private void assertColdPublicationsUnderEviction(final int bound) throws Exception {
    final Path events = directory.resolve("events.jsonl");
    BitemporalStorageEpochTest.generateEvents(events);
    final Path plain = directory.resolve("plain");
    assertEquals(0, BitemporalStorageEpochTest.load(plain, events, Integer.MAX_VALUE));
    final List<String> expected = BitemporalStorageEpochTest.coldPublications(plain);

    // This cache budget holds only one HOT leaf, so allocating a split can evict its siblings.
    final BufferManagerImpl bufferManager = (BufferManagerImpl) Databases.getGlobalBufferManager();
    previousRecordCacheBytes = bufferManager.getRecordPageCacheMaxWeightBytes();
    previousFragmentCacheBytes = bufferManager.getRecordPageFragmentCacheMaxWeightBytes();
    Databases.reinitializeBufferManagerForTesting(16L * 64 * 1024, 16L * 64 * 1024);
    final Path pressured = directory.resolve("pressured");
    final int rotations = BitemporalStorageEpochTest.load(pressured, events, bound);
    if (bound == Integer.MAX_VALUE) {
      assertEquals(0, rotations);
    } else {
      assertTrue(rotations > 0);
    }
    assertEquals(expected, BitemporalStorageEpochTest.coldPublications(pressured),
        "every resource at every publication after cold reopen");
  }
}

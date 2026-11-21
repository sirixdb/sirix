package io.sirix.budget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTLongBulkIndexLoader;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.settings.VersioningType;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * The sibling read of an ordinary put must cost neither an allocation nor a guard.
 *
 * <p>
 * Four groups of keys make four leaves. A put just above a group's last key extends that leaf's
 * range, so the writer compares the key with the first key of the next leaf. Next to group 0 that
 * neighbour is a committed leaf, which the page cache owns and may evict; next to group 2 it is a
 * leaf this transaction already changed, which the transaction log owns. The writer reads the
 * committed neighbour unheld and checks afterwards that it was not evicted, instead of holding it,
 * because this comparison is on the path of every such put. The two puts run the same code
 * otherwise, so their allocation must be equal, and neither may take a guard on a leaf of the page
 * cache.
 * </p>
 */
@Isolated
@DisplayName("The sibling read of an ordinary HOT put allocates nothing and takes no guard")
final class HotWriterLeafRetentionAllocationBudgetTest {

  private static final String RESOURCE = "hot-writer-leaf-retention-allocation";
  private static final int INDEX_NUMBER = 0;

  /** A power of two, so the bulk-built trie splits between the groups. */
  private static final int GROUPS = 4;

  /** More than half a leaf: two groups never share one, and every leaf has room for the puts. */
  private static final int GROUP_KEYS = 257;

  /**
   * Five passes and one check of 32 puts each add 192 entries to a leaf that starts with 257 and
   * holds 512, so no put of this test splits a leaf.
   */
  private static final int PASSES = 5;
  private static final int PUTS_PER_PASS = 32;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("a put beside a committed neighbour allocates exactly what a put beside a logged neighbour does")
  void readingACommittedNeighbourAllocatesNothingAndTakesNoGuard() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final Path path = temporaryDirectory.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(VersioningType.FULL).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx load = session.beginNodeTrx()) {
        final HOTLongBulkIndexLoader loader =
            HOTLongIndexWriter.create(load.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER).createBulkLoader();
        for (int group = 0; group < GROUPS; group++) {
          for (int ordinal = 0; ordinal < GROUP_KEYS; ordinal++) {
            loader.add(key(group, ordinal), 1L);
          }
        }
        loader.flush();
        load.commit();
      }
    }
    Databases.clearGlobalCaches();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx transaction = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(transaction.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);
      @SuppressWarnings("unchecked")
      final ShardedPageCache<HOTLeafPage> cache =
          (ShardedPageCache<HOTLeafPage>) transaction.getStorageEngineWriter().getBufferManager().getHOTLeafPageCache();
      // A put inside group 3's range hands that leaf to the transaction log.
      writer.indexNodeKey(key(3, 0) + 1L, 1L);

      final long thread = Thread.currentThread().threadId();
      int ordinal = GROUP_KEYS;
      long besideCommitted = 0;
      long besideLogged = 0;
      // Warm-up and measurement share one call site each; the last pass is the one that counts.
      for (int pass = 0; pass < PASSES; pass++) {
        besideCommitted = 0;
        besideLogged = 0;
        for (int put = 0; put < PUTS_PER_PASS; put++, ordinal++) {
          besideCommitted += putAllocatedBytes(writer, key(0, ordinal), bean, thread);
          besideLogged += putAllocatedBytes(writer, key(2, ordinal), bean, thread);
        }
      }
      assertEquals(besideLogged, besideCommitted,
          "reading the committed neighbour must not allocate (bytes over " + PUTS_PER_PASS + " puts)");

      // The puts beside group 0 loaded group 1's leaf into the page cache: that is the read. Taking a
      // guard marks a leaf as recently used, so with the marks cleared neither kind of put may leave
      // one behind.
      final HOTLeafPage committedNeighbour = cachedLeafStartingAt(cache, key(1, 0));
      assertNotNull(committedNeighbour, "the puts beside group 0 must have read group 1's committed leaf");
      clearHot(cache);
      for (int put = 0; put < PUTS_PER_PASS; put++, ordinal++) {
        writer.indexNodeKey(key(2, ordinal), 1L);
        writer.indexNodeKey(key(0, ordinal), 1L);
      }
      assertFalse(anyHot(cache), "an ordinary put must not take a guard on a leaf of the page cache");
      assertEquals(0, committedNeighbour.getGuardCount(), "no guard may outlive a put");
      assertFalse(committedNeighbour.isClosed(), "the committed neighbour is still the one the puts read");
      System.out.println("[hot-live] ordinary PATH puts after cold reopen: " + PUTS_PER_PASS
          + " puts beside committed neighbour=" + besideCommitted + " allocated bytes; beside logged neighbour="
          + besideLogged + " allocated bytes; cache leaves hot=false; committed neighbour guards=0 and still open");
      transaction.rollback();
    }
  }

  private static long putAllocatedBytes(final HOTLongIndexWriter writer, final long key, final ThreadMXBean bean,
      final long thread) {
    final long before = bean.getThreadAllocatedBytes(thread);
    writer.indexNodeKey(key, 1L);
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static @Nullable HOTLeafPage cachedLeafStartingAt(final ShardedPageCache<HOTLeafPage> cache, final long key) {
    final byte[] serialized = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(key, serialized, 0);
    for (final HOTLeafPage leaf : cache.asMap().values()) {
      if (leaf.getEntryCount() > 0 && Arrays.equals(leaf.getFirstKey(), 0, Long.BYTES, serialized, 0, Long.BYTES)) {
        return leaf;
      }
    }
    return null;
  }

  private static void clearHot(final ShardedPageCache<HOTLeafPage> cache) {
    for (final HOTLeafPage leaf : cache.asMap().values()) {
      leaf.clearHot();
    }
  }

  private static boolean anyHot(final ShardedPageCache<HOTLeafPage> cache) {
    for (final HOTLeafPage leaf : cache.asMap().values()) {
      if (leaf.isHot()) {
        return true;
      }
    }
    return false;
  }

  /** Groups differ above bit 32, the keys of one group between bits 16 and 32. */
  private static long key(final int group, final int ordinal) {
    return ((long) group << 32) | ((long) ordinal << 16);
  }
}

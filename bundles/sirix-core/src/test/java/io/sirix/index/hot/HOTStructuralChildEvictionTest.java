package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/** Commit a CAS tree, then split its leaves while synchronously evicting resolved siblings. */
@Isolated
final class HOTStructuralChildEvictionTest {
  private static final int BASE_KEYS = 4_000;
  private static final int TOTAL_KEYS = 8_000;
  private static final String RESOURCE = "structural-eviction";

  @TempDir
  Path directory;

  @Test
  void committedSiblingsSurviveSplitAllocations() {
    final long[] values = new long[TOTAL_KEYS];
    final Random random = new Random(0x484f54L);
    for (int i = 0; i < BASE_KEYS; i++) {
      values[i] = (i + 1L) << 32;
    }
    // Concentrate later inserts between the first two committed keys, leaving sibling leaves cold.
    for (int i = BASE_KEYS; i < TOTAL_KEYS; i++) {
      values[i] = (1L << 32) + i - BASE_KEYS + 1L;
    }
    for (int i = TOTAL_KEYS - 1; i > BASE_KEYS; i--) {
      final int other = BASE_KEYS + random.nextInt(i - BASE_KEYS + 1);
      final long value = values[i];
      values[i] = values[other];
      values[other] = value;
    }
    final Path path = directory.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<CASValue> writer = writer(wtx);
        final HOTBulkIndexLoader<CASValue> loader = writer.createBulkLoader();
        for (int i = 0; i < BASE_KEYS; i++) {
          loader.add(key(values[i]), i + 1L);
        }
        loader.flush();
        wtx.commit();
      }
    }

    final BufferManagerImpl previous = (BufferManagerImpl) Databases.getGlobalBufferManager();
    final long previousRecordBytes = previous.getRecordPageCacheMaxWeightBytes();
    final long previousFragmentBytes = previous.getRecordPageFragmentCacheMaxWeightBytes();
    // The complete-leaf share is smaller than a leaf. Guarded loading must still hand it off alive.
    Databases.reinitializeBufferManagerForTesting(64L * 1024, 64L * 1024);
    final AtomicInteger splits = new AtomicInteger();
    try {
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ShardedPageCache<HOTLeafPage> cache =
            (ShardedPageCache<HOTLeafPage>) wtx.getStorageEngineWriter()
                .getBufferManager().getHOTLeafPageCache();
        AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(() -> {
          splits.incrementAndGet();
          cache.evictUnderPressure();
        });
        try {
          final HOTIndexWriter<CASValue> writer = writer(wtx);
          for (int i = BASE_KEYS; i < TOTAL_KEYS; i++) {
            writer.indexNodeKey(key(values[i]), i + 1L);
          }
          assertTrue(splits.get() > 0, "exercise eviction between child resolution and integration");
          for (final HOTLeafPage leaf : cache.asMap().values()) {
            assertEquals(0, leaf.getGuardCount(), "structural guards must be released after insertion");
          }
          wtx.commit();
        } catch (final RuntimeException | Error failure) {
          wtx.rollback();
          throw failure;
        } finally {
          AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(null);
        }
      }

      Databases.clearGlobalCaches();
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        for (int revision = 1; revision <= 2; revision++) {
          try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
            final HOTIndexReader<CASValue> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
                CASKeySerializer.INSTANCE, IndexType.CAS, 0);
            final int count = revision == 1 ? BASE_KEYS : TOTAL_KEYS;
            for (int i = 0; i < count; i++) {
              final NodeReferences refs = reader.get(key(values[i]), SearchMode.EQUAL);
              assertNotNull(refs, "missing posting " + i + " at revision " + revision);
              assertEquals(1, refs.getNodeKeys().getLongCardinality());
              assertTrue(refs.contains(i + 1L));
            }
          }
        }
      }
    } finally {
      AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(null);
      Databases.reinitializeBufferManagerForTesting(previousRecordBytes, previousFragmentBytes);
    }
  }

  private static HOTIndexWriter<CASValue> writer(final JsonNodeTrx wtx) {
    return HOTIndexWriter.create(wtx.getStorageEngineWriter(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
  }

  private static CASValue key(final long value) {
    return new CASValue(new Int64(value), Type.INR, 1L);
  }
}

package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter.Interval;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeDisabledLookupCacheTest {
  private static final Interval INTERVAL = new Interval(true, 10, 20, false, 0);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void truncatedRevisionReuseReadsFreshRootsAndPostings(final VersioningType versioning) {
    assertEquals("0", System.getProperty("sirix.hotLookupCache.maxEntries"));
    final Path databasePath = directory.resolve("disabled");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (var session = database.beginResourceSession("rows")) {
        assertFalse(Databases.getGlobalBufferManager().getHOTLookupCache().isEnabled());
        final long generation = Databases.getGlobalBufferManager().getHOTLookupCache().generation();
        try (var writer = session.beginNodeTrx()) {
          writer.insertObjectAsFirstChild();
          ValidTimeIntervalIndexFactory.createIndexWriter(writer.getStorageEngineWriter(), 0, new IntervalDomain(),
              "vf", "vt").insert(1, INTERVAL);
          writer.commit();
        }
        final int survivingRevision = session.getMostRecentRevisionNumber();
        try (var writer = session.beginNodeTrx()) {
          final var index = ValidTimeIntervalIndexFactory.createIndexWriter(writer.getStorageEngineWriter(), 0,
              new IntervalDomain(), "vf", "vt");
          index.delete(1, INTERVAL);
          index.insert(2, INTERVAL);
          index.checkOrder(2, 0, 3, -1);
          writer.commit();
        }
        final int discardedRevision = session.getMostRecentRevisionNumber();
        final PostingMask[] discardedMasks;
        final HOTTrieReader discardedTrie;
        final PageReference discardedRoot;
        try (var transaction = session.beginNodeReadOnlyTrx(discardedRevision)) {
          final StorageEngineReader storage = transaction.getStorageEngineReader();
          discardedMasks = masks(storage);
          assertMasks(discardedMasks, 2, true);
          final var reader = HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var lease = ValidTimeReadCache.borrow(reader)) {
            discardedTrie = lease.trie();
            discardedRoot = Objects.requireNonNull(lease.root());
          }
        }
        try (var writer = session.createStorageEngineWriter()) {
          writer.truncateTo(survivingRevision);
        }
        assertEquals(survivingRevision, session.getMostRecentRevisionNumber());
        try (var writer = session.beginNodeTrx()) {
          final var index = ValidTimeIntervalIndexFactory.createIndexWriter(writer.getStorageEngineWriter(), 0,
              new IntervalDomain(), "vf", "vt");
          index.delete(1, INTERVAL);
          index.insert(4, INTERVAL);
          writer.commit();
        }
        assertEquals(discardedRevision, session.getMostRecentRevisionNumber());
        assertEquals(generation, Databases.getGlobalBufferManager().getHOTLookupCache().generation(),
            "the disabled lookup cache must remain the non-advancing-generation witness");
        for (int repetition = 0; repetition < 2; repetition++) {
          try (var transaction = session.beginNodeReadOnlyTrx(discardedRevision)) {
            final StorageEngineReader storage = transaction.getStorageEngineReader();
            final PostingMask[] fresh = masks(storage);
            assertMasks(fresh, 4, false);
            assertMasks(discardedMasks, 2, true);
            for (int mask = 0; mask < 4; mask++) {
              assertNotSame(discardedMasks[mask], fresh[mask]);
            }
            final var reader = HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
            try (var lease = ValidTimeReadCache.borrow(reader)) {
              assertNotSame(discardedTrie, lease.trie());
              assertNotSame(discardedRoot, lease.root());
            }
            final HotOrderedStore lower = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader);
            final HotOrderedStore members = ValidTimeIntervalIndexFactory.createMembershipStore(storage, 0);
            assertArrayEquals(new long[] {4}, Objects.requireNonNull(lower.chunk(16, 10, 0)).toSortedArray());
            assertEquals(1, lower.cardinality(16, 10));
            assertTrue(lower.hasReferences(16, 10));
            assertTrue(lower.intersects(16, 10, members, 0, 0));
            assertFalse(ValidTimeIntervalIndexFactory.createOrderStore(storage, 0).hasReferences(0, 0));
            final var tree = ValidTimeIntervalIndexFactory.createReaderTree(storage, 0, new IntervalDomain());
            final LongArrayList keys = new LongArrayList();
            tree.stab(15, keys::add);
            assertArrayEquals(new long[] {4}, keys.toLongArray());
            keys.clear();
            tree.stabChunks(15, true, (base, references, _) -> references.forEachNodeKey(key -> keys.add(base | key)));
            assertArrayEquals(new long[] {4}, keys.toLongArray());
          }
          try (var transaction = session.beginNodeReadOnlyTrx(survivingRevision)) {
            assertMasks(masks(transaction.getStorageEngineReader()), 1, false);
          }
        }
      }
    }
  }

  private static PostingMask[] masks(final StorageEngineReader storage) {
    final var reader = HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
    return new PostingMask[] {
        new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 0),
        new HotOrderedStore(ValidTimeKey.STORE_UPPER, null, reader).chunkMask(16, 20, 0),
        ValidTimeIntervalIndexFactory.createMembershipStore(storage, 0).chunkMask(0, 0, 0),
        ValidTimeIntervalIndexFactory.createVerificationStore(storage, 0).chunkMask(0, 0, 0),
        ValidTimeIntervalIndexFactory.createOrderStore(storage, 0).chunkMask(0, 0, 0)};
  }

  private static void assertMasks(final PostingMask[] masks, final long reference, final boolean unordered) {
    for (int mask = 0; mask < 4; mask++) {
      assertEquals(1L << reference, masks[mask].word(0));
    }
    assertEquals(unordered ? 1 : 0, masks[4].word(0));
  }
}

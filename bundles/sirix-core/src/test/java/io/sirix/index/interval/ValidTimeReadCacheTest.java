package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

final class ValidTimeReadCacheTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void reuseAcrossClosedTransactionsConcurrentWalksAndInvalidation() {
    final Path databasePath = directory.resolve("readers");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL).build());
      try (var session = database.beginResourceSession("rows")) {
        try (var writer = session.beginNodeTrx()) {
          writer.insertObjectAsFirstChild();
          ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, new IntervalDomain())
                                       .insert(1, 10, 20);
          writer.commit();
        }
        final HOTTrieReader parked;
        final PageReference root;
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var first = ValidTimeReadCache.borrow(reader)) {
            assertNotNull(first.root());
            try (var second = ValidTimeReadCache.borrow(reader)) {
              assertNotSame(first.trie(), second.trie(), "overlapping walks must own independent path stacks");
              parked = second.trie();
              root = Objects.requireNonNull(second.root());
            }
          }
        }
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var lease = ValidTimeReadCache.borrow(reader)) {
            assertSame(parked, lease.trie(), "a new transaction must reuse parked traversal buffers");
            assertSame(root, lease.root(), "the committed root is constant within its revision");
            final byte[] key = new byte[ValidTimeKeySerializer.KEY_BYTES + 4];
            ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(new ValidTimeKey(ValidTimeKey.STORE_LOWER, 16, 10), 0,
                key, 0);
            assertNotNull(lease.trie().get(Objects.requireNonNull(lease.root()), key),
                "a reused reader must resolve through its new transaction");
          }
        }
        Databases.clearGlobalCaches();
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var lease = ValidTimeReadCache.borrow(reader)) {
            assertNotSame(parked, lease.trie(), "an invalidated cache generation must not reuse its old roots");
          }
        }
      }
    }
  }
}

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
  void everyWalkOwnsItsReaderAndResolvesThroughItsOwnTransaction() {
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
        final HOTTrieReader first;
        final PageReference root;
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var outer = ValidTimeReadCache.borrow(reader)) {
            assertNotNull(outer.root());
            try (var inner = ValidTimeReadCache.borrow(reader)) {
              assertNotSame(outer.trie(), inner.trie(), "overlapping walks must own independent path stacks");
              assertSame(outer.root(), inner.root(), "the committed root is constant within its revision");
            }
            first = outer.trie();
            root = Objects.requireNonNull(outer.root());
          }
        }
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          try (var lease = ValidTimeReadCache.borrow(reader)) {
            // No reader is parked across transactions: re-pointing one would need a forwarding
            // StorageEngineReader, a second loaded implementation of the interface the whole cold
            // read path is compiled against.
            assertNotSame(first, lease.trie(), "a new transaction binds a reader to its own storage reader");
            assertSame(root, lease.root(), "the committed root is constant within its revision");
            final byte[] key = new byte[ValidTimeKeySerializer.KEY_BYTES + 4];
            ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(new ValidTimeKey(ValidTimeKey.STORE_LOWER, 16, 10), 0,
                key, 0);
            assertNotNull(lease.trie().get(Objects.requireNonNull(lease.root()), key),
                "a lease must resolve through its transaction");
          }
        }
      }
    }
  }
}

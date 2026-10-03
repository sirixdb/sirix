/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.Transaction;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Database close retains failed cleanup for retry")
final class DatabaseCloseStrandingTest {

  @Test
  @DisplayName("a failed store stays registered until cleanup succeeds")
  void failingResourceStoreCanRetryBeforeDeregisteringTheDatabase(@TempDir final Path tempDir) {
    final PathBasedPool<Database<?>> sessions = new PathBasedPool<>();
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(tempDir);

    final ThrowingResourceStore store = new ThrowingResourceStore();
    final LocalDatabase<JsonResourceSession, ?> database = newDatabase(dbConfig, sessions, store);

    assertTrue(sessions.containsAnyEntry(tempDir));
    assertThrows(IllegalStateException.class, database::close);
    assertTrue(database.isOpen());
    assertTrue(sessions.containsAnyEntry(tempDir));
    store.fail = false;
    database.close();
    assertFalse(database.isOpen());
    assertFalse(sessions.containsAnyEntry(tempDir));
    database.close();
  }

  @SuppressWarnings("unchecked")
  private static LocalDatabase<JsonResourceSession, ?> newDatabase(final DatabaseConfiguration dbConfig,
      final PathBasedPool<Database<?>> sessions, final ResourceStore<JsonResourceSession> store) {
    return new LocalDatabase<>(new NoOpTransactionManager(), dbConfig, sessions, store,
        new WriteLocksRegistry(), new PathBasedPool<>());
  }

  /** A store whose {@code close()} fails, standing in for any cleanup that can throw. */
  private static final class ThrowingResourceStore implements ResourceStore<JsonResourceSession> {
    private boolean fail = true;

    @Override
    public JsonResourceSession beginResourceSession(final ResourceConfiguration resourceConfig,
        final BufferManager bufferManager, final Path resourceFile) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasOpenResourceSession(final Path resourcePath) {
      return false;
    }

    @Override
    public JsonResourceSession getOpenResourceSession(final Path resourcePath) {
      return null;
    }

    @Override
    public void close() {
      if (fail) {
        throw new IllegalStateException("cleanup failed");
      }
    }

    @Override
    public boolean closeResourceSession(final Path resourceFile) {
      return false;
    }
  }

  private static final class NoOpTransactionManager implements TransactionManager {
    @Override
    public Transaction beginTransaction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TransactionManager closeTransaction(final Transaction trx) {
      return this;
    }

    @Override
    public void close() {
      // nothing to release
    }
  }
}

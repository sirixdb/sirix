/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.access;

import io.sirix.api.Transaction;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Database close retains failed cleanup for retry")
final class DatabaseCloseStrandingTest {

  /** TXN-503/615: backend failure preserves ownership; TXN-803: only successful close stops hooks. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @DisplayName("a failed backend leaves the database usable until cleanup succeeds")
  void failingBackendCanRetryBeforeClosingTheDatabase(final boolean failManager, @TempDir final Path tempDir) {
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(tempDir);

    final ThrowingResourceStore store = new ThrowingResourceStore();
    store.fail = !failManager;
    final NoOpTransactionManager manager = new NoOpTransactionManager();
    manager.fail = failManager;
    final LocalDatabase<JsonResourceSession, ?> database = newDatabase(dbConfig, store, manager);

    assertTrue(database.isOpen());
    assertSame(failManager
        ? manager.failure
        : store.failure, assertThrows(IllegalStateException.class, database::close));
    assertTrue(database.isOpen());
    assertDoesNotThrow(() -> store.getNotifications().runPreCommit(() -> {
    }));
    store.fail = false;
    manager.fail = false;
    database.close();
    assertFalse(database.isOpen());
    assertThrows(IllegalStateException.class, () -> store.getNotifications().runPreCommit(() -> {
    }));
    database.close();
  }

  @SuppressWarnings("unchecked")
  private static LocalDatabase<JsonResourceSession, ?> newDatabase(final DatabaseConfiguration dbConfig,
      final ResourceStore<JsonResourceSession> store, final TransactionManager manager) {
    return new LocalDatabase<>(manager, dbConfig, store, new WriteLocksRegistry(), new PathBasedPool<>());
  }

  /** A store whose {@code close()} fails, standing in for any cleanup that can throw. */
  private static final class ThrowingResourceStore implements ResourceStore<JsonResourceSession> {
    private boolean fail = true;
    private final IllegalStateException failure = new IllegalStateException("cleanup failed");
    private final DatabaseNotifications notifications = new DatabaseNotifications();

    @Override
    public DatabaseNotifications getNotifications() {
      return notifications;
    }

    @Override
    public void assertCanClose() {
      notifications.assertNotInCallback();
    }

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
        throw failure;
      }
    }

    @Override
    public boolean closeResourceSession(final Path resourceFile) {
      return false;
    }
  }

  private static final class NoOpTransactionManager implements TransactionManager {
    private boolean fail;
    private final IllegalStateException failure = new IllegalStateException("transaction manager cleanup failed");

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
      if (fail) {
        throw failure;
      }
      // nothing to release
    }
  }
}

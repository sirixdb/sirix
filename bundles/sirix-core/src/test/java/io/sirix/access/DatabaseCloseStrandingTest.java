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

/**
 * A database must leave the session pool when it is closed, even if the cleanup on the way out
 * fails.
 *
 * <p>{@code close()} marks the instance closed before running the cleanup that can throw, and the
 * pool deregistration came last. One failure in between left the database flagged closed but
 * still registered, and the guard at the top of {@code close()} made every later attempt return
 * immediately — so the entry survived for the life of the JVM.
 *
 * <p>The consequences run a long way from where the exception happened.
 * {@link Databases#removeDatabase} declines to delete anything while a handle is still
 * registered, so the database became permanently un-removable and its files outlived every
 * attempt to clear them. Creating a database or a resource is a silent no-op when the target
 * already exists, so the next caller at that path received the <em>previous</em> database — its
 * committed data and its persisted index definitions included — while believing it had just made
 * a fresh one. Write transactions rebind index listeners from those definitions on construction,
 * which is how a resource that never defined an index ends up paying to maintain one.
 *
 * <p>These tests drive {@link LocalDatabase} directly with a store that fails to close, because
 * that is the only way to reach the defect: on the happy path the old ordering deregisters
 * perfectly well, so an end-to-end test passes with or without the fix.
 */
@DisplayName("Database close deregisters even when cleanup fails")
final class DatabaseCloseStrandingTest {

  @Test
  @DisplayName("a store that throws on close does not strand the database in the pool")
  void failingResourceStoreStillDeregistersTheDatabase(@TempDir final Path tempDir) {
    final PathBasedPool<Database<?>> sessions = new PathBasedPool<>();
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(tempDir);

    final LocalDatabase<JsonResourceSession, ?> database = newDatabase(dbConfig, sessions,
        new ThrowingResourceStore());

    // Registration happens in the constructor.
    assertTrue(sessions.containsAnyEntry(tempDir), "the database did not register itself");

    // The failure is still reported — it is not swallowed — but it must not cost deregistration.
    assertThrows(IllegalStateException.class, database::close);

    assertFalse(sessions.containsAnyEntry(tempDir),
        "the database stayed registered after a failed close, which makes it permanently "
            + "un-removable: close() short-circuits on isClosed, so nothing ever retries");
  }

  @SuppressWarnings("unchecked")
  private static LocalDatabase<JsonResourceSession, ?> newDatabase(final DatabaseConfiguration dbConfig,
      final PathBasedPool<Database<?>> sessions, final ResourceStore<JsonResourceSession> store) {
    return new LocalDatabase<>(new NoOpTransactionManager(), dbConfig, sessions, store,
        new WriteLocksRegistry(), new PathBasedPool<>());
  }

  /** A store whose {@code close()} fails, standing in for any cleanup that can throw. */
  private static final class ThrowingResourceStore implements ResourceStore<JsonResourceSession> {
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
      throw new IllegalStateException("cleanup failed");
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

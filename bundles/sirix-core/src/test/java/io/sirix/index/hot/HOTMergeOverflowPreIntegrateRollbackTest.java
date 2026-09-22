/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The merge path's rollback-only rule covers the window <em>before</em> the integration, not only
 * the integration itself.
 *
 * <p>
 * Once {@code splitLeafPage} has run, {@code K}'s document node is written while its index entry is
 * not, so every failure of that arm — the split construction, the off-path handler, the retirement
 * of the halves, or the integration — has to poison the transaction: a caller that swallowed the
 * failure and committed would hold a document with no posting. Only the integration publishes, so a
 * marking gated on "did we reach integrate" still passes every structural-atomicity test while
 * leaving that earlier window open. This scenario fails a deterministic fault into exactly that
 * window and asserts the transaction is poisoned all the same.
 * </p>
 *
 * <p>
 * The driver is the simplest overflow this writer has: puts of one key pattern into a fresh
 * valid-time index until the root leaf overflows. The leaf is the root, so the off-path handler
 * returns {@code INTEGRATE} with no parent to fold into and the seam fires with nothing published.
 * The hook counts its invocations, so a change in leaf geometry ends the scenario rather than
 * letting it pass without reaching the window it claims.
 * </p>
 */
@ResourceLock("HOT_MERGE_OVERFLOW_BEFORE_INTEGRATE_TEST_HOOK")
final class HOTMergeOverflowPreIntegrateRollbackTest {

  private static final String RESOURCE = "merge-overflow-pre-integrate-rollback";
  private static final int INDEX_NUMBER = 0;

  /** Every key of the scenario indexes this one node. */
  private static final long SHAPE_NODE_KEY = 7L;

  /** Safety net so a geometry change ends the scenario instead of running forever. */
  private static final int MAX_PUTS = 4_000;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearHookAndCaches() {
    AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(null);
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("a merge-path overflow failing before integrate poisons the transaction until rollback")
  void overflowFailingBeforeIntegratePoisonsTheTransaction() {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final IllegalStateException sentinel = new IllegalStateException("injected before the merge-path integrate");
    final AtomicInteger hookInvocations = new AtomicInteger();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(() -> {
          hookInvocations.incrementAndGet();
          throw sentinel;
        });

        final IllegalStateException overflowFailure = assertThrows(IllegalStateException.class, () -> {
          for (int put = 0; put < MAX_PUTS; put++) {
            writer.indexNodeKey(key(put + 1L), SHAPE_NODE_KEY);
          }
        });
        assertSame(sentinel, overflowFailure, "the pre-integrate seam must preserve the original failure");
        assertEquals(1, hookInvocations.get(),
            "the scenario must reach one merge-path overflow that takes the integrate arm; without one it "
                + "covers no pre-integrate window and its shape must be re-tuned");

        final SirixIOException nextWriteFailure = assertThrows(SirixIOException.class,
            () -> writer.indexNodeKey(key(MAX_PUTS + 1L), SHAPE_NODE_KEY));
        assertSame(sentinel, nextWriteFailure.getCause(),
            "the next write through the same HOT writer must report the latched pre-integrate cause");

        final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
        assertSame(sentinel, commitFailure.getCause(),
            "commit must refuse a document whose index entry the failed overflow never wrote");

        AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(null);
        wtx.rollback();

        final HOTIndexWriter<ValidTimeKey> recoveredWriter = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        recoveredWriter.indexNodeKey(key(1L), SHAPE_NODE_KEY);
        wtx.commit();
        assertEquals(1, session.getMostRecentRevisionNumber(),
            "rollback must replace the poisoned page writer with a clean commit-capable one");
      }
    } finally {
      AbstractHOTIndexWriter.setMergeOverflowBeforeIntegrateTestHook(null);
    }
  }

  /** One pattern in key byte 1 — the serializer sign-flips the fork node — with a running endpoint. */
  private static ValidTimeKey key(final long endpoint) {
    return new ValidTimeKey(ValidTimeKey.STORE_UPPER, (0x20L << 56) ^ Long.MIN_VALUE, endpoint);
  }
}

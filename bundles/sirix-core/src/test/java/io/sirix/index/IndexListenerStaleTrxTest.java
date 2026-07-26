/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Regression test for the index-listener / stale-transaction bug
 * ([[hot-listener-rebind-bug]]): a CAS/PATH/NAME index listener bound to one write transaction's
 * storage engine + path summary must not fire on a LATER write transaction after the first closed,
 * which previously surfaced as {@code AssertionError: Transaction is already closed!} from
 * {@code CASIndexListener.listen -> PathSummaryReader -> NodeStorageEngineWriter.getRecord}.
 *
 * <p>The canonical real-world pattern: create the index in one transaction, then populate/mutate
 * the indexed data in later ones.
 */
final class IndexListenerStaleTrxTest {

  private static final String DOC = "{\"k\":[{\"v\":1},{\"v\":2}]}";

  @Test
  void casIndexListenerNotFiredOnLaterTransactionAfterFirstClosed(@TempDir final Path tempDir) {
    final Path dbPath = tempDir.resolve("idx-listener");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res").build());
      try (final JsonResourceSession session = database.beginResourceSession("res")) {
        final var indexed = io.brackit.query.util.path.Path.parse("/k/[]/v",
            io.brackit.query.util.path.PathParser.Type.JSON);

        // wtx1: insert data + create a CAS index + commit, then CLOSE.
        try (final JsonNodeTrx wtx1 = session.beginNodeTrx()) {
          wtx1.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC), JsonNodeTrx.Commit.NO);
          final var ic = session.getWtxIndexController(wtx1.getRevisionNumber());
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(indexed), 0, IndexDef.DbType.JSON);
          ic.createIndexes(Set.of(def), wtx1);
          wtx1.commit();
        }

        // wtx2 (wtx1 is closed): mutate the indexed data. A stale listener bound to wtx1's
        // now-closed engine would throw "Transaction is already closed!" here.
        assertDoesNotThrow(() -> mutateInNewTrx(session), "mutation in a later wtx must not fire a "
            + "listener bound to the first (now-closed) wtx");

        // wtx3: repeat, to exercise repeated rebinding across transactions.
        assertDoesNotThrow(() -> mutateInNewTrx(session));
      }
    }
  }

  /**
   * The clearing fix must be isolated per resource: index controllers (and their listener sets) are
   * per-resource-session, so two resources each with their own index — including overlapping open
   * write transactions — must update independently with no cross-contamination and no stale-trx.
   */
  @Test
  void perResourceIsolationAcrossOverlappingWriteTransactions(@TempDir final Path tempDir) {
    final Path dbPath = tempDir.resolve("idx-listener-multi");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("a").build());
      database.createResource(ResourceConfiguration.newBuilder("b").build());
      try (final JsonResourceSession sa = database.beginResourceSession("a");
           final JsonResourceSession sb = database.beginResourceSession("b")) {
        createIndexAndCommit(sa);
        createIndexAndCommit(sb);

        // Overlapping write transactions on the two resources, interleaved. If clearing one
        // resource's listeners affected the other (shared/global state), or a stale listener fired,
        // these mutations + commits would throw "Transaction is already closed!".
        assertDoesNotThrow(() -> {
          try (final JsonNodeTrx wa = sa.beginNodeTrx();
               final JsonNodeTrx wb = sb.beginNodeTrx()) {
            replaceSubtree(wa);
            replaceSubtree(wb);
            wa.commit();
            wb.commit();
          }
        });
        // And a second round, fresh transactions, still isolated.
        assertDoesNotThrow(() -> {
          mutateInNewTrx(sa);
          mutateInNewTrx(sb);
        });
      }
    }
  }

  private static void createIndexAndCommit(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC), JsonNodeTrx.Commit.NO);
      final var indexed = io.brackit.query.util.path.Path.parse("/k/[]/v",
          io.brackit.query.util.path.PathParser.Type.JSON);
      final var ic = session.getWtxIndexController(wtx.getRevisionNumber());
      final IndexDef def = IndexDefs.createCASIdxDef(false, Type.INR,
          Collections.singleton(indexed), 0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(def), wtx);
      wtx.commit();
    }
  }

  private static void replaceSubtree(final JsonNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    if (wtx.moveToFirstChild()) {
      wtx.remove();
    }
    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC), JsonNodeTrx.Commit.NO);
  }

  /** Remove the indexed subtree and re-insert it in a fresh write transaction (fires the listener). */
  private static void mutateInNewTrx(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      replaceSubtree(wtx);
      wtx.commit();
    }
  }
}

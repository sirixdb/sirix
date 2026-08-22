/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Transaction-lineage coverage for the process-wide load-time projection registry. */
final class ProjectionBulkLoadTransactionLifecycleTest {

  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
  }

  @AfterEach
  void tearDown() {
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void rollbackAbortsOnlyTheCurrentTransactionsOwnerAndAllowsARearm() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = projectionDefinition();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final JsonIndexController controller = controller(session, wtx.getRevisionNumber());
      final ProjectionBulkLoad first = controller.createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      assertSame(first, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      wtx.rollback();

      assertTrue(first.isFinished(), "rollback must retire the listener-owned streaming builder");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(ProjectionBulkLoad.anyActive(), "rollback leaked an ACTIVE projection owner");

      final JsonIndexController reboundController = controller(session, wtx.getRevisionNumber());
      final ProjectionBulkLoad retry =
          reboundController.createProjectionIndexAtLoadStart(indexDef, wtx, -1L);
      assertNotSame(first, retry);
      assertSame(retry, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));

      wtx.rollback();
      assertTrue(retry.isFinished());
      assertFalse(ProjectionBulkLoad.anyActive());
    }
  }

  @Test
  void revertAbortsTheOwnerBeforeReplacingItsPageWriter() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = projectionDefinition();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final ProjectionBulkLoad load =
          controller(session, wtx.getRevisionNumber()).createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      assertSame(load, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      wtx.revertTo(0);

      assertTrue(load.isFinished(), "revertTo must not leave a builder bound to the discarded writer");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(ProjectionBulkLoad.anyActive(), "revertTo leaked an ACTIVE projection owner");
    }
  }

  @Test
  void cleanCloseAfterAnIntermediateMaintenanceEpochAbortsTheOwner() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = projectionDefinition();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final JsonIndexController controller = controller(session, wtx.getRevisionNumber());
      final ProjectionBulkLoad load = controller.createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      // This is the exact listener signal used by an intermediate auto-commit. Keeping this epoch
      // node-clean exercises close()'s clean-close branch; load-owned page/index state is deliberately
      // not represented by AbstractNodeTrxImpl.modificationCount.
      controller.applyPendingIndexMaintenance(false);
      assertSame(load, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(load.isFinished(), "an intermediate epoch must not finalize the streaming build");

      wtx.close();

      assertTrue(load.isFinished(), "clean close must abort state that cannot outlive its transaction");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(ProjectionBulkLoad.anyActive(), "clean close leaked an ACTIVE projection owner");
    }
  }

  @Test
  void successfulCountBasedIntermediateCommitKeepsTheSameOwnerActive() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx(1)) {
      final IndexDef indexDef = projectionDefinition();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final ProjectionBulkLoad load =
          controller(session, wtx.getRevisionNumber()).createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      // The second mutation rotates the one-mutation predecessor before it runs. reInstantiateIndexes
      // creates a new listener, which must resolve the SAME owner through the stable transaction token.
      final long arrayKey = wtx.insertArrayAsFirstChild().getNodeKey();
      wtx.moveTo(arrayKey);
      wtx.insertObjectAsFirstChild();

      assertEquals(1, session.getMostRecentRevisionNumber(), "the fixture did not cross an auto-commit epoch");
      assertSame(load, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(load.isFinished(), "an intermediate commit finalized the load-time projection");

      wtx.rollback();
      assertTrue(load.isFinished());
      assertFalse(ProjectionBulkLoad.anyActive());
    }
  }

  @Test
  void anUnrelatedWriteTransactionCannotResolveOrAbortTheOwner() {
    final var ownerDatabase =
        JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    final var unrelatedDatabase =
        JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH2.getFile());
    try (final var ownerSession = ownerDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
        final var unrelatedSession = unrelatedDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
        final var ownerWtx = ownerSession.beginNodeTrx();
        final var unrelatedWtx = unrelatedSession.beginNodeTrx()) {
      final IndexDef indexDef = projectionDefinition();
      final String ownerResourceKey = ownerSession.getResourceConfig().getResource().toString();
      final ProjectionBulkLoad owner =
          controller(ownerSession, ownerWtx.getRevisionNumber())
              .createProjectionIndexAtLoadStart(indexDef, ownerWtx, -1L);

      // Pass the owner's exact registry key deliberately: the owner-token check, not merely a
      // different resource lookup, must reject this transaction.
      assertNull(ProjectionBulkLoad.active(ownerResourceKey, INDEX_NUMBER, unrelatedWtx));

      final JsonIndexController unrelatedController =
          controller(unrelatedSession, unrelatedWtx.getRevisionNumber());
      unrelatedController.createIndexListeners(Set.of(indexDef), unrelatedWtx);
      unrelatedController.applyPendingIndexMaintenance(false);
      unrelatedWtx.close();

      assertSame(owner, ProjectionBulkLoad.active(ownerResourceKey, INDEX_NUMBER, ownerWtx),
          "an unrelated listener lifecycle aborted or captured the owner");
      assertFalse(owner.isFinished());

      ownerWtx.rollback();
      assertTrue(owner.isFinished());
      assertFalse(ProjectionBulkLoad.anyActive());
    }
  }

  @Test
  void droppingTheDefinitionAbortsItsExactLoadOwner() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = projectionDefinition();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final JsonIndexController controller = controller(session, wtx.getRevisionNumber());
      final ProjectionBulkLoad load =
          controller.createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      assertSame(load, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      controller.dropIndexes(Set.of(indexDef), wtx);

      assertTrue(load.isFinished(), "dropping a projection must retire its external streaming state");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      assertFalse(ProjectionBulkLoad.anyActive(), "dropIndexes leaked an ACTIVE projection owner");
      assertNull(controller.getIndexes().getIndexDef(INDEX_NUMBER, indexDef.getType()));

      wtx.rollback();
    }
  }

  private static IndexDef projectionDefinition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER,
        IndexDef.DbType.JSON);
  }

  private static JsonIndexController controller(final io.sirix.api.json.JsonResourceSession session,
      final int revision) {
    return (JsonIndexController) session.getWtxIndexController(revision);
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fail-closed publication coverage for caller-caught explicit projection-build failures. */
final class ProjectionIndexBuilderFailureAtomicityTest {

  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader("[{\"value\":1},{\"value\":2}]"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void postWalkFailurePoisonsCallerOwnedTransactionBeforePartialTreeCanCommit() {
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);
    final RuntimeException injected = new RuntimeException("injected post-walk projection-build failure");
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // IN-HOOK witness. The seam is the only moment at which the builder's state can be observed
      // without writing: once the failure has poisoned the transaction, ProjectionIndexHOTStorage's
      // constructor legitimately prepares a writable page and therefore MUST fail closed, so
      // constructing one afterwards observes the rollback-only guard rather than the build.
      final boolean[] hookRan = new boolean[1];
      final RuntimeException thrown =
          assertThrows(RuntimeException.class, () -> ProjectionIndexBuilder.buildAndPersistWithPostWalkHook(definition,
              wtx.getPathSummary(), wtx, wtx.getStorageEngineWriter(), false, storage -> {
                hookRan[0] = true;
                // ACCUMULATED, not finalized: at this seam the row group is readable through the
                // live bulk accumulator / read-through, and the HOT tree has NOT been spliced yet.
                assertNotNull(storage.getRowGroupFromColumnSegmentSlots(1),
                    "the seam must fire after a real row group was accumulated and readable");
                assertNull(storage.getBlob(0), "metadata must not be published before the post-walk seam");
                throw injected;
              }));
      assertTrue(hookRan[0], "the post-walk seam never fired — every assertion inside it is vacuous");
      assertSame(injected, thrown, "cleanup must preserve the build failure itself");

      // Fail-closed, asserted WITHOUT attempting a write: the caller that swallowed the build error
      // still cannot commit the partial projection state, and the refusal carries the original cause.
      final SirixIOException rollbackOnly =
          assertThrows(SirixIOException.class, wtx.getStorageEngineWriter()::assertTransactionWritable);
      assertSame(injected, rollbackOnly.getCause(),
          "a caller that catches the build error must still be unable to commit partial projection state");

      // And the guard is what stops publication: a storage construction after the poison prepares a
      // writable page and must be refused with the SAME cause, never quietly succeed.
      final SirixIOException refusedAfterPoison = assertThrows(SirixIOException.class,
          () -> new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER));
      assertSame(injected, refusedAfterPoison.getCause(),
          "the post-poison refusal must carry the original build failure, not mask it");
      wtx.rollback();
    }
  }
}

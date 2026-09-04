/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class JsonProjectionPathSummaryLifecycleTest {

  private static final String RESOURCE = "projection-path-summary-guard";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0} rejects a persisted projection on a summary-less resource")
  @EnumSource(VersioningType.class)
  void coldReopenFailsBeforeMutationWhenPersistedProjectionHasNoPathSummary(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef definition = projectionDefinition();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(false)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(10)
                                                              .build()));

      // A normal row-store write/reopen remains valid when no projection is catalogued.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        wtx.commit();
      }

      // Validate the complete request before the shared builder can catalogue or write even the
      // standard index that precedes the invalid projection definition in iteration order.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        final IndexDef pathDefinition = IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON);
        final Set<IndexDef> mixedDefinitions = new LinkedHashSet<>(List.of(pathDefinition, definition));

        final IllegalStateException failure =
            assertThrows(IllegalStateException.class, () -> controller.createIndexes(mixedDefinitions, wtx));

        assertTrue(failure.getMessage().contains("buildPathSummary=true"));
        assertNull(controller.getIndexes().getIndexDef(pathDefinition.getID(), pathDefinition.getType()),
            "projection preflight failure leaked the preceding path definition");
        assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()),
            "projection preflight failure leaked its own definition");
      }

      // Model a definition persisted by the former fail-open lifecycle. Public projection creation
      // already rejects this resource shape; direct catalogue insertion is the compatibility seam
      // needed to prove that cold rebinding cannot silently serve a stale projection.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx(AfterCommitState.CLOSE)) {
        session.getWtxIndexController(wtx.getRevisionNumber()).getIndexes().add(definition);
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final IllegalStateException failure = assertThrows(IllegalStateException.class, session::beginNodeTrx);
      assertTrue(failure.getMessage().contains("Cannot bind incremental maintenance"));
      assertTrue(failure.getMessage().contains("buildPathSummary=true"));
    }
  }

  @Test
  void listenerBindingAlsoFailsClosedWhenConfiguredPathSummaryIsUnavailable() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef definition = projectionDefinition();
    controller.getIndexes().add(definition);
    final JsonNodeTrx wtx = mock(JsonNodeTrx.class);
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(RESOURCE).buildPathSummary(true).build();
    when(wtx.getResourceSession()).thenReturn(session);
    when(session.getResourceConfig()).thenReturn(config);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexListeners(Set.of(definition), wtx));

    assertTrue(failure.getMessage().contains("path summary is unavailable"));
    assertNotNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
    verify(wtx, never()).getStorageEngineWriter();
  }

  private static IndexDef projectionDefinition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
  }
}

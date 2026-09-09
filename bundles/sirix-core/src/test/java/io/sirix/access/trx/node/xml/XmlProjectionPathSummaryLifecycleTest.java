/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.xml;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionBulkLoad;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class XmlProjectionPathSummaryLifecycleTest {

  private static final String RESOURCE = "projection-path-summary-guard";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalState() {
    ProjectionBulkLoad.clearActive();
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0} rejects a persisted XML projection on a summary-less resource")
  @EnumSource(VersioningType.class)
  void coldReopenFailsBeforeMutationWhenPersistedProjectionHasNoPathSummary(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    assertTrue(Databases.createXmlDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef definition = projectionDefinition(0, "/records/record");

    try (Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(false)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(10)
                                                              .build()));
      try (XmlResourceSession session = database.beginResourceSession(RESOURCE);
          XmlNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertElementAsFirstChild(new QNm("root"));
        wtx.commit();
      }
    }

    // A summary-less XML row store remains writable after a genuinely cold reopen when it has no
    // projection definition. Projection creation must then reject atomically before cataloguing it.
    Databases.clearGlobalCaches();
    try (Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        XmlResourceSession session = database.beginResourceSession(RESOURCE);
        XmlNodeTrx wtx = session.beginNodeTrx()) {
      final XmlIndexController controller = (XmlIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> controller.createIndexes(Set.of(definition), wtx));
      assertTrue(failure.getMessage().contains("buildPathSummary=true"));
      assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()),
          "rejected projection creation leaked a catalog definition");
    }

    // Model a definition persisted by the former fail-open lifecycle. Public projection creation
    // rejects this shape, so direct catalogue insertion is the compatibility seam for cold binding.
    try (Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        XmlResourceSession session = database.beginResourceSession(RESOURCE);
        XmlNodeTrx wtx = session.beginNodeTrx(AfterCommitState.CLOSE)) {
      session.getWtxIndexController(wtx.getRevisionNumber()).getIndexes().add(definition);
      wtx.commit();
    }

    Databases.clearGlobalCaches();
    try (Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
      final IllegalStateException failure = assertThrows(IllegalStateException.class, session::beginNodeTrx);
      assertTrue(failure.getMessage().contains("Cannot bind incremental maintenance"));
      assertTrue(failure.getMessage().contains("buildPathSummary=true"));
    }
  }

  @Test
  void listenerBindingAlsoFailsClosedWhenConfiguredPathSummaryIsUnavailable() {
    final XmlIndexController controller = new XmlIndexController();
    final IndexDef definition = projectionDefinition(0, "/records/record");
    controller.getIndexes().add(definition);
    final XmlNodeTrx wtx = mock(XmlNodeTrx.class);
    final XmlResourceSession session = mock(XmlResourceSession.class);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(RESOURCE).buildPathSummary(true).build();
    when(wtx.getResourceSession()).thenReturn(session);
    when(session.getResourceConfig()).thenReturn(config);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexListeners(Set.of(definition), wtx));

    assertTrue(failure.getMessage().contains("path summary is unavailable"));
    assertNotNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
    verify(wtx, never()).getStorageEngineWriter();
  }

  @Test
  void secondArmFailureRollsBackEveryOwnedLoadAndNewCatalogDefinition() {
    final Path databasePath = temporaryDirectory.resolve("two-definition-arm-rollback");
    assertTrue(Databases.createXmlDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef firstDefinition = projectionDefinition(0, "/missing-a/record");
    final IndexDef secondDefinition = projectionDefinition(1, "/missing-b/record");
    final Set<IndexDef> definitions = new LinkedHashSet<>(List.of(firstDefinition, secondDefinition));

    try (Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .build()));
      try (XmlResourceSession session = database.beginResourceSession(RESOURCE);
          XmlNodeTrx wtx = session.beginNodeTrx()) {
        final String resourceKey = session.getResourceConfig().getResource().toString();
        final XmlIndexController controller =
            (XmlIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        final ProjectionBulkLoad preexistingSecond = ProjectionBulkLoad.begin(secondDefinition, resourceKey, wtx,
            wtx.getPathSummary(), wtx.getStorageEngineWriter(), -1L);
        try {
          assertThrows(IllegalStateException.class,
              () -> controller.createProjectionIndexesAtLoadStart(definitions, wtx, -1L));

          assertNull(ProjectionBulkLoad.active(resourceKey, firstDefinition.getID(), wtx),
              "the first owner created by the failed arm transaction leaked");
          assertSame(preexistingSecond, ProjectionBulkLoad.active(resourceKey, secondDefinition.getID(), wtx),
              "rollback must not abort the owner that forced the second begin failure");
          assertNull(controller.getIndexes().getIndexDef(firstDefinition.getID(), firstDefinition.getType()),
              "the first newly published definition leaked");
          assertNull(controller.getIndexes().getIndexDef(secondDefinition.getID(), secondDefinition.getType()),
              "the second newly published definition leaked");
        } finally {
          preexistingSecond.abort();
        }
        assertFalse(ProjectionBulkLoad.anyActive(), "the failed arm left an ACTIVE owner residue");
        wtx.rollback();
      }
    }
  }

  private static IndexDef projectionDefinition(final int id, final String rootPath) {
    return IndexDefs.createProjectionIdxDef(parse(rootPath, PathParser.Type.XML),
        List.of(parse(rootPath + "/value", PathParser.Type.XML)), List.of(Type.LON), id, IndexDef.DbType.XML);
  }
}

package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

final class IndexCatalogueCacheTest {

  @Test
  void jsonRevertedCommitsRetainBoundedCatalogues(@TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertBoundedCatalogues(database, IndexDef.DbType.JSON);
    }
  }

  @Test
  void xmlRevertedCommitsRetainBoundedCatalogues(@TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      assertBoundedCatalogues(database, IndexDef.DbType.XML);
    }
  }

  private static <R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> void assertBoundedCatalogues(
      final Database<? extends ResourceSession<R, W>> database, final IndexDef.DbType type) throws Exception {
    database.createResource(
        ResourceConfiguration.newBuilder("catalogue").storageType(StorageType.FILE_CHANNEL).build());
    try (final ResourceSession<R, W> session = database.beginResourceSession("catalogue");
        final ResourceSession<R, W> sibling = database.beginResourceSession("catalogue");
        final W trx = session.beginNodeTrx()) {
      final AbstractResourceSession<R, W> resourceSession = (AbstractResourceSession<R, W>) session;
      final AbstractResourceSession<R, W> siblingSession = (AbstractResourceSession<R, W>) sibling;
      final Field cacheField = AbstractResourceSession.class.getDeclaredField("parsedIndexCatalogues");
      cacheField.setAccessible(true);
      final Map<?, ?> retained = (Map<?, ?>) requireNonNull(cacheField.get(resourceSession));
      session.getWtxIndexController(trx.getRevisionNumber())
             .createIndexes(Set.of(IndexDefs.createNameIdxDef(0, type)), trx);
      trx.commit();
      trx.commit();
      for (int cycle = 0; cycle < 128; cycle++) {
        trx.revertTo(1);
        trx.commit();
        assertTrue(retained.size() <= 64, "parsed catalogue retention grew with reverted commits");
        assertTrue(session.getRtxIndexController(session.getMostRecentRevisionNumber()).containsIndex(IndexType.NAME));
      }
      assertEquals(64, retained.size());
      final int latestRevision = session.getMostRecentRevisionNumber();
      for (int revision = 1; revision < latestRevision; revision++) {
        final Indexes historical = new Indexes();
        siblingSession.restoreIndexCatalogue(revision, historical);
        assertEquals(1, historical.getNrOfIndexDefsWithType(IndexType.NAME));
        assertTrue(retained.size() <= 64);
      }
      assertEquals(64, retained.size());
      try (final MockedStatic<IndexController> controller = mockStatic(IndexController.class, CALLS_REAL_METHODS)) {
        final Indexes latest = new Indexes();
        resourceSession.restoreIndexCatalogue(latestRevision, latest);
        assertEquals(1, latest.getNrOfIndexDefsWithType(IndexType.NAME));
        trx.commit();
        controller.verify(() -> IndexController.deserialize(any(InputStream.class)), never());
      }
      final Path indexes =
          session.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
      assertFalse(Files.exists(indexes.resolve((latestRevision + 1) + ".xml")));
    }
  }
}

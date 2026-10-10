package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PathSummaryRecordPageGuardTest {
  @TempDir
  File directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void pathResolutionPreservesTheSharedReadersRecordPin(final VersioningType versioning) throws Exception {
    final var databasePath = directory.toPath().resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .buildPathSummary(true)
                                                              .storeDiffs(false)
                                                              .build()));
      try (final JsonResourceSession session = database.beginResourceSession("resource")) {
        final long sentinelKey;
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          final String json = "[{\"title\":\"value\"}," + "\"padding\",".repeat(1100) + "\"sentinel\"]";
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToLastChild());
          sentinelKey = trx.getNodeKey();
          trx.commit();
        }
        try (final StorageEngineReader reader = session.createStorageEngineReader(1);
            final PathSummaryReader summary = PathSummaryReader.getInstance(reader, session)) {
          final ValueNode sentinel =
              assertInstanceOf(ValueNode.class, reader.getRecord(sentinelKey, IndexType.DOCUMENT, -1));
          final NodeStorageEngineReader recordReader = (NodeStorageEngineReader) reader;
          final KeyValueLeafPage page = recordReader.getCurrentPage();
          assertNotNull(page);
          assertTrue(page.getPageKey() > 0, "the sentinel must be on a different page from the path-summary root");
          final int guards = page.getGuardCount();
          final Path<QNm> path = Path.parse("/[]/title", PathParser.Type.JSON);
          for (int lookup = 0; lookup < 2; lookup++) {
            assertFalse(summary.getPCRsForPath(path).isEmpty(), "resolve the path on both a miss and a hit");
            assertSame(page, recordReader.getCurrentPage());
            assertEquals(guards, page.getGuardCount());
            assertEquals("sentinel", sentinel.getValue());
          }
        }
      }
    }
  }
}

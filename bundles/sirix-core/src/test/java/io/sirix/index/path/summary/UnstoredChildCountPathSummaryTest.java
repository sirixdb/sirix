package io.sirix.index.path.summary;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class UnstoredChildCountPathSummaryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void childLinksEstablishPathSummaryEmptinessAfterReopen(final VersioningType versioning) {
    assertChildExistenceAfterReopen(versioning, false);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void storedChildCountControlAfterReopen(final VersioningType versioning) {
    assertChildExistenceAfterReopen(versioning, true);
  }

  private void assertChildExistenceAfterReopen(final VersioningType versioning, final boolean storeChildCount) {
    final Path databasePath = directory.resolve("counts");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .storeChildCount(storeChildCount)
                                                   .buildPathSummary(true)
                                                   .build());
      try (var session = database.beginResourceSession("resource"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"numbers\":[{\"v\":1}]}"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath);
        var session = database.beginResourceSession("resource");
        var summary = session.openPathSummary()) {
      summary.moveToDocumentRoot();
      assertTrue(summary.hasFirstChild());
      assertTrue(summary.moveToFirstChild());
      // The named array's path has descendants even when its child count is not persisted.
      assertTrue(summary.hasFirstChild());
      assertTrue(summary.hasChildren());
      while (summary.hasFirstChild()) {
        assertTrue(summary.hasChildren());
        assertTrue(summary.moveToFirstChild());
      }
      assertFalse(summary.hasChildren());
      assertFalse(summary.moveToFirstChild());
      summary.moveToDocumentRoot();
      assertTrue(summary.hasChildren());
    }
  }
}

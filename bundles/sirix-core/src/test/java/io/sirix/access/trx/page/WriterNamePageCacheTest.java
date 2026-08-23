/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Focused lifecycle gates for the writer-local current-revision {@link NamePage}. */
final class WriterNamePageCacheTest {

  private static final String RESOURCE = "writer-name-page-cache";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void jsonKindsReuseOnePageAndFullRotationAndRollbackInvalidateIt() {
    final Path databasePath = temporaryDirectory.resolve("json-database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resourceConfiguration());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();

        NamePage currentEpochPage = writer.cachedCurrentNamePageForTesting();
        final NodeKind[] jsonKinds =
            {NodeKind.OBJECT_NAMED_OBJECT, NodeKind.OBJECT_NAMED_ARRAY, NodeKind.OBJECT_NAMED_BOOLEAN,
                NodeKind.OBJECT_NAMED_NUMBER, NodeKind.OBJECT_NAMED_STRING, NodeKind.OBJECT_NAMED_NULL};
        for (final NodeKind kind : jsonKinds) {
          writer.createNameKey("json-" + kind.name(), kind);
          if (currentEpochPage == null) {
            currentEpochPage = writer.cachedCurrentNamePageForTesting();
            assertNotNull(currentEpochPage);
          } else {
            assertSame(currentEpochPage, writer.cachedCurrentNamePageForTesting(),
                "all JSON named kinds share the current revision's dictionary page");
          }
        }

        writer.asyncFlush();
        assertNull(writer.cachedCurrentNamePageForTesting(),
            "a full TIL rotation must not carry a structural-page pointer into the next epoch");
        writer.awaitPendingAsyncFlush();

        writer.createNameKey("after-rotation", NodeKind.OBJECT_NAMED_STRING);
        assertNotNull(writer.cachedCurrentNamePageForTesting(),
            "the next epoch must resolve its own current-revision NamePage");

        wtx.rollback();
        assertNull(writer.cachedCurrentNamePageForTesting(),
            "rollback must sever the retired writer's structural-page pointer");

        final NodeStorageEngineWriter replacementWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        assertNotSame(writer, replacementWriter);
        assertNull(replacementWriter.cachedCurrentNamePageForTesting());
        replacementWriter.createNameKey("after-rollback", NodeKind.OBJECT_NAMED_STRING);
        assertNotNull(replacementWriter.cachedCurrentNamePageForTesting());
        wtx.rollback();
        assertNull(replacementWriter.cachedCurrentNamePageForTesting());
      }
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void xmlDictionariesReuseOneCurrentRevisionPage() {
    final Path databasePath = temporaryDirectory.resolve("xml-database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));

    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(resourceConfiguration());
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
          final XmlNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();

        NamePage currentEpochPage = writer.cachedCurrentNamePageForTesting();
        final NodeKind[] xmlKinds =
            {NodeKind.ELEMENT, NodeKind.ATTRIBUTE, NodeKind.NAMESPACE, NodeKind.PROCESSING_INSTRUCTION};
        for (final NodeKind kind : xmlKinds) {
          writer.createNameKey("xml-" + kind.name(), kind);
          if (currentEpochPage == null) {
            currentEpochPage = writer.cachedCurrentNamePageForTesting();
            assertNotNull(currentEpochPage);
          } else {
            assertSame(currentEpochPage, writer.cachedCurrentNamePageForTesting(),
                "all XML dictionaries are anchored in the same current-revision NamePage");
          }
        }

        wtx.rollback();
        assertNull(writer.cachedCurrentNamePageForTesting());
      }
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void commitAndCloseSeverRetiredWriterPointers() {
    final Path databasePath = temporaryDirectory.resolve("commit-database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resourceConfiguration());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final NodeStorageEngineWriter successorWriter;
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          final NodeStorageEngineWriter committingWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
          committingWriter.keyForName("committed-name", NodeKind.OBJECT_NAMED_NUMBER);
          assertNotNull(committingWriter.cachedCurrentNamePageForTesting());
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"committed-name\":1}"),
              JsonNodeTrx.Commit.NO);
          assertNotNull(committingWriter.cachedCurrentNamePageForTesting());

          wtx.commit();
          assertNull(committingWriter.cachedCurrentNamePageForTesting(),
              "successful hardening and writer close must sever the committed epoch's pointer");

          successorWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
          assertNotSame(committingWriter, successorWriter);
          assertNull(successorWriter.cachedCurrentNamePageForTesting());
          successorWriter.keyForName("committed-name", NodeKind.OBJECT_NAMED_NUMBER);
          assertNotNull(successorWriter.cachedCurrentNamePageForTesting());
        }

        assertNull(successorWriter.cachedCurrentNamePageForTesting(),
            "close must sever the last structural-page pointer even for a clean writer");
      }
    }
  }

  private static ResourceConfiguration resourceConfiguration() {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .buildPathSummary(false)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }
}

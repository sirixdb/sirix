package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the {@link FileChannelStorage} backend.
 *
 * <p>Covers the full write-read-verify cycle at both the low-level I/O layer and the high-level
 * JSON document layer to ensure data integrity and durability through the FileChannel backend.</p>
 */
class FileChannelStorageTest {

  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("sirix-filechannel-test-");
  }

  @AfterEach
  void tearDown() {
    if (tempDir != null) {
      try {
        Databases.removeDatabase(tempDir);
      } catch (final Exception ignored) {
        // best-effort cleanup
      }
      deleteRecursively(tempDir.toFile());
    }
  }

  private void deleteRecursively(final java.io.File file) {
    if (file.isDirectory()) {
      final java.io.File[] children = file.listFiles();
      if (children != null) {
        for (final java.io.File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  // ---------------------------------------------------------------------------
  // Low-level I/O tests
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Low-Level I/O: UberPage write and read")
  class LowLevelIO {

    private ResourceConfiguration resourceConfig;

    @BeforeEach
    void setUpResource() throws IOException {
      // Set resourcePath directly (setDatabaseConfiguration is package-private in io.sirix.access).
      // FileChannelStorage resolves: resourcePath / data / sirix.data
      final Path dataDir = tempDir.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath());
      Files.createDirectories(dataDir);
      resourceConfig = ResourceConfiguration.newBuilder("shredded")
          .storageType(StorageType.FILE_CHANNEL)
          .build();
      resourceConfig.resourcePath = tempDir;
    }

    @Test
    @DisplayName("exists() returns false on a fresh, empty storage")
    void testExistsReturnsFalseInitially() {
      final IOStorage storage = createStorage();
      try {
        assertFalse(storage.exists(), "A fresh storage with no writes must report exists() == false");
      } finally {
        storage.close();
      }
    }

    @Test
    @DisplayName("Write UberPage, then read back with same Writer instance")
    void testWriteAndReadBackSameInstance() {
      final IOStorage storage = createStorage();
      try {
        final PageReference pageRef = new PageReference();
        final UberPage page = new UberPage();
        pageRef.setPage(page);
        final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

        try (final Writer writer = storage.createWriter()) {
          final PageReference readBack =
              writer.writeUberPageReference(resourceConfig, pageRef, page, bytes).readUberPageReference();

          assertNotNull(readBack.getPage(), "Read-back page must not be null");
          assertEquals(page.getRevisionCount(), ((UberPage) readBack.getPage()).getRevisionCount(),
              "Revision count must match after same-instance read-back");
        }

        assertTrue(storage.exists(), "Storage must report exists() == true after a write");
      } finally {
        storage.close();
      }
    }

    @Test
    @DisplayName("Write UberPage, close writer, read back with new Reader instance")
    void testWriteCloseAndReadWithNewReader() {
      final IOStorage storage = createStorage();
      try {
        final PageReference pageRef = new PageReference();
        final UberPage page = new UberPage();
        pageRef.setPage(page);
        final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

        try (final Writer writer = storage.createWriter()) {
          writer.writeUberPageReference(resourceConfig, pageRef, page, bytes);
        }

        // Read back with a brand-new reader
        try (final Reader reader = storage.createReader()) {
          final PageReference readRef = reader.readUberPageReference();
          assertNotNull(readRef.getPage(), "Page read via new Reader must not be null");
          assertTrue(readRef.getKey() >= 0, "Page key from Reader must be a valid offset");
          assertEquals(page.getRevisionCount(), ((UberPage) readRef.getPage()).getRevisionCount(),
              "Revision count must survive a close-and-reopen cycle");
        }
      } finally {
        storage.close();
      }
    }

    @Test
    @DisplayName("Write, truncate, verify storage is empty, then write again")
    void testTruncateAndRewrite() {
      final IOStorage storage = createStorage();
      try {
        final PageReference pageRef = new PageReference();
        final UberPage page = new UberPage();
        pageRef.setPage(page);
        final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

        // First write
        try (final Writer writer = storage.createWriter()) {
          writer.writeUberPageReference(resourceConfig, pageRef, page, bytes);
        }
        assertTrue(storage.exists(), "Storage must exist after first write");

        // Truncate
        try (final Writer writer = storage.createWriter()) {
          writer.truncate();
        }
        assertFalse(storage.exists(), "Storage must not exist after truncate");

        // Write again after truncation
        final PageReference pageRef2 = new PageReference();
        final UberPage page2 = new UberPage();
        pageRef2.setPage(page2);

        try (final Writer writer = storage.createWriter()) {
          writer.writeUberPageReference(resourceConfig, pageRef2, page2, bytes);
        }
        assertTrue(storage.exists(), "Storage must exist after re-write post-truncate");

        try (final Reader reader = storage.createReader()) {
          final PageReference readRef = reader.readUberPageReference();
          assertNotNull(readRef.getPage(), "Page must be readable after truncate-and-rewrite");
          assertEquals(page2.getRevisionCount(), ((UberPage) readRef.getPage()).getRevisionCount());
        }
      } finally {
        storage.close();
      }
    }

    @Test
    @DisplayName("Close storage and reopen it; data must survive")
    void testCloseAndReopenStorage() {
      final int expectedRevisionCount;

      // Write with first storage instance
      {
        final IOStorage storage = createStorage();
        try {
          final PageReference pageRef = new PageReference();
          final UberPage page = new UberPage();
          pageRef.setPage(page);
          expectedRevisionCount = page.getRevisionCount();
          final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

          try (final Writer writer = storage.createWriter()) {
            writer.writeUberPageReference(resourceConfig, pageRef, page, bytes);
          }
        } finally {
          storage.close();
        }
      }

      // Read with second storage instance
      {
        final IOStorage storage = createStorage();
        try {
          assertTrue(storage.exists(), "Reopened storage must still report data exists");
          try (final Reader reader = storage.createReader()) {
            final PageReference readRef = reader.readUberPageReference();
            assertNotNull(readRef.getPage());
            assertEquals(expectedRevisionCount, ((UberPage) readRef.getPage()).getRevisionCount(),
                "Revision count must survive a full close-and-reopen of storage");
          }
        } finally {
          storage.close();
        }
      }
    }

    private IOStorage createStorage() {
      return new FileChannelStorage(resourceConfig, Caffeine.newBuilder().buildAsync(),
          new RevisionIndexHolder());
    }
  }

  // ---------------------------------------------------------------------------
  // High-level integration tests through the full JSON stack
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("High-Level: Full JSON document lifecycle via FILE_CHANNEL storage")
  class HighLevelIntegration {

    private static final String RESOURCE_NAME = "test-resource";

    @Test
    @DisplayName("Insert JSON document, commit, and read back via FILE_CHANNEL backend")
    void testInsertAndReadBack() {
      Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .storageType(StorageType.FILE_CHANNEL)
            .build());

        // Write
        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"name\":\"sirix\",\"version\":1}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read back and verify
        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertEquals(1, rtx.getRevisionNumber(), "Should be at revision 1 after one commit");
          assertTrue(rtx.moveToDocumentRoot(), "Must be able to navigate to document root");
          assertTrue(rtx.moveToFirstChild(), "Document root must have a first child (the object)");
        }
      }
    }

    @Test
    @DisplayName("Multiple commits produce correct revision numbers via FILE_CHANNEL backend")
    void testMultipleCommits() {
      Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .storageType(StorageType.FILE_CHANNEL)
            .build());

        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          // Commit 1
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader("{\"rev\":1}"), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }

          // Commit 2
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader("{\"rev\":2}"), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }

          // Commit 3
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader("{\"rev\":3}"), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }

          assertEquals(3, session.getMostRecentRevisionNumber(),
              "After three user commits the most recent revision number must be 3");

          // Verify each revision is independently readable
          for (int rev = 1; rev <= 3; rev++) {
            try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              assertEquals(rev, rtx.getRevisionNumber(),
                  "Reading revision " + rev + " must yield that revision number");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Data survives database close and reopen via FILE_CHANNEL backend")
    void testCloseAndReopenDatabase() {
      Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));

      // Phase 1: write and close
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .storageType(StorageType.FILE_CHANNEL)
            .build());

        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"persistent\":true}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }

      // Phase 2: reopen and verify
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertEquals(1, rtx.getRevisionNumber(),
              "After reopen the revision number must still be 1");
          assertTrue(rtx.moveToDocumentRoot(), "Must navigate to document root after reopen");
          assertTrue(rtx.moveToFirstChild(), "Object child must exist after reopen");
        }
      }
    }

    @Test
    @DisplayName("StorageType.getStorage() correctly instantiates FILE_CHANNEL backend")
    void testStorageTypeFactory() {
      Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        final ResourceConfiguration config = ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .storageType(StorageType.FILE_CHANNEL)
            .build();
        db.createResource(config);

        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          final ResourceConfiguration actual = session.getResourceConfig();
          assertEquals(StorageType.FILE_CHANNEL, actual.storageType,
              "The persisted resource configuration must use FILE_CHANNEL storage type");
        }
      }
    }
  }
}

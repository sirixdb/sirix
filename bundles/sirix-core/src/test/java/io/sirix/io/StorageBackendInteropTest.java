package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cross-backend format checks: `docs/DISK_FORMAT.md` §4 claims FILE_CHANNEL (default) and
 * MEMORY_MAPPED share ONE on-disk format. This test proves it end-to-end — a resource written
 * by either backend opens, verifies (superblock incl. resource UUID, page checksums), reads
 * identically, and accepts further commits under the other backend. It also proves the
 * resource-UUID cross-link catches wrong-file mixups on the real open path, and that the
 * unavailable/removed backends (FILE, IO_URING, S3) fail fast with actionable errors instead of
 * misreading anything.
 */
public final class StorageBackendInteropTest {

  private static final String RESOURCE = "interop-resource";
  private static final String JSON = "{\"name\":\"sirix\",\"backends\":[\"fc\",\"mm\"],\"n\":42}";

  @TempDir
  private Path tempDir;

  @BeforeEach
  @AfterEach
  public void cleanState() throws IOException {
    Databases.clearGlobalCaches();
    if (Files.exists(tempDir)) {
      try (Stream<Path> paths = Files.walk(tempDir)) {
        paths.sorted(Comparator.reverseOrder()).filter(p -> !p.equals(tempDir)).forEach(p -> p.toFile().delete());
      }
    }
  }

  @Test
  @DisplayName("Resource written via FILE_CHANNEL opens and grows via MEMORY_MAPPED")
  public void fileChannelResourceOpensViaMemoryMapped() throws IOException {
    writtenWithOpensWith(StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED);
  }

  @Test
  @DisplayName("Resource written via MEMORY_MAPPED opens and grows via FILE_CHANNEL")
  public void memoryMappedResourceOpensViaFileChannel() throws IOException {
    writtenWithOpensWith(StorageType.MEMORY_MAPPED, StorageType.FILE_CHANNEL);
  }

  private void writtenWithOpensWith(final StorageType writer, final StorageType reader) throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));
    final String goldenJson;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(writer).build());
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(JSON), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        goldenJson = serialize(session);
      }
    }

    // Cold "process": swap the persisted backend in ressetting.obj — the files themselves are
    // the shared format and must be bit-compatible.
    Databases.clearGlobalCaches();
    swapPersistedBackend(writer, reader);

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        assertEquals(reader, session.getResourceConfig().storageType,
            "test wiring: the persisted backend swap must be in effect");
        assertEquals(goldenJson, serialize(session),
            "revision 1 must read back byte-identically under " + reader);
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertObjectRecordAsFirstChild("addedBy", new StringValue(reader.name()));
          wtx.commit();
        }
        assertEquals(2, session.getMostRecentRevisionNumber(),
            "the other backend must be able to COMMIT on top of the shared format");
      }
    }
  }

  @Test
  @DisplayName("Resource-UUID cross-link rejects a swapped-in foreign data file on both backends")
  public void foreignDataFileFailsUuidCheck() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(tempDir));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
      for (final String name : new String[] {"res-a", "res-b"}) {
        db.createResource(ResourceConfiguration.newBuilder(name).storageType(StorageType.FILE_CHANNEL).build());
        try (final JsonResourceSession session = db.beginResourceSession(name);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(JSON), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    Databases.clearGlobalCaches();
    final Path dataA = findFile("res-a", "sirix.data");
    final Path dataB = findFile("res-b", "sirix.data");
    Files.copy(dataB, dataA, StandardCopyOption.REPLACE_EXISTING);

    for (final StorageType backend : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      Databases.clearGlobalCaches();
      swapPersistedBackendOf("res-a", backend);
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(tempDir)) {
        try (final JsonResourceSession session = db.beginResourceSession("res-a")) {
          serialize(session);
          fail("opening res-a with res-b's data file must fail the UUID cross-check under " + backend);
        } catch (final SirixIOException | SirixUsageException e) {
          assertTrue(messageChain(e).contains("resource UUID mismatch"),
              backend + ": expected a resource-UUID mismatch error but got: " + messageChain(e));
        }
      }
    }
  }

  @Test
  @DisplayName("Removed/absent backends fail fast with actionable errors")
  public void unavailableBackendsFailFast() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("unused").build();
    final UnsupportedOperationException file =
        assertThrows(UnsupportedOperationException.class, () -> StorageType.FILE.getInstance(config));
    assertTrue(file.getMessage().contains("FILE_CHANNEL"));
    final SirixIOException iouring =
        assertThrows(SirixIOException.class, () -> StorageType.IO_URING.getInstance(config));
    assertTrue(iouring.getMessage().contains("io_uring"));
    final SirixIOException s3 = assertThrows(SirixIOException.class, () -> StorageType.S3.getInstance(config));
    assertTrue(s3.getMessage().contains("S3"));
  }

  // ==================== helpers ====================

  private static String serialize(final JsonResourceSession session) throws IOException {
    final StringWriter writer = new StringWriter();
    final JsonSerializer serializer = new JsonSerializer.Builder(session, writer).build();
    serializer.call();
    return writer.toString();
  }

  private void swapPersistedBackend(final StorageType from, final StorageType to) throws IOException {
    final Path config = findFile(RESOURCE, "ressetting.obj");
    final String json = Files.readString(config);
    assertTrue(json.contains("\"" + from.name() + "\""), "config must persist the writer backend");
    Files.writeString(config, json.replace("\"" + from.name() + "\"", "\"" + to.name() + "\""));
  }

  private void swapPersistedBackendOf(final String resource, final StorageType to) throws IOException {
    final Path config = findFile(resource, "ressetting.obj");
    final String json = Files.readString(config)
        .replace("\"" + StorageType.FILE_CHANNEL.name() + "\"", "\"" + to.name() + "\"")
        .replace("\"" + StorageType.MEMORY_MAPPED.name() + "\"", "\"" + to.name() + "\"");
    Files.writeString(config, json);
  }

  private Path findFile(final String resource, final String fileName) throws IOException {
    try (Stream<Path> paths = Files.walk(tempDir)) {
      return paths.filter(p -> p.getFileName().toString().equals(fileName)
              && p.toString().contains(resource))
          .findFirst()
          .orElseThrow(() -> new AssertionError(fileName + " not found for resource " + resource));
    }
  }

  private static String messageChain(final Throwable throwable) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      sb.append(t.getMessage()).append(" | ");
    }
    return sb.toString();
  }
}

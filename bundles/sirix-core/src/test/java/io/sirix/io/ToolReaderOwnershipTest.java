package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRevisionPrunedException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

/** TXN-613: a failed isolated open leaves no owned native file handles behind. */
@EnabledOnOs(OS.LINUX) // /proc exposes actual per-file handle ownership, without process-wide count races.
final class ToolReaderOwnershipTest {
  @TempDir
  Path directory;
  private ResourceConfiguration config;

  @BeforeEach
  void createStore() {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession("resource");
          final JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.commit();
      }
    }
    config = ResourceConfiguration.deserializeForTool(databasePath.resolve("resources/resource"));
    assertEquals(0, openHandles());
  }

  @Test
  void everyFileOpenFailureReleasesEarlierAcquisitions() {
    final AtomicInteger acquisitions = new AtomicInteger();
    try (final MockedStatic<FileChannel> channels = mockStatic(FileChannel.class, invocation -> {
      if (invocation.getMethod().getName().equals("open")) {
        acquisitions.incrementAndGet();
      }
      return invocation.callRealMethod();
    })) {
      openAndClose(config);
    }
    assertEquals(0, openHandles());
    // Inject at each external acquisition, including overload delegation. The oracle is the real
    // operating-system handle set, not the number or ordering of calls made by the implementation.
    for (int failure = 1; failure <= acquisitions.get(); failure++) {
      final int failAt = failure;
      final AtomicInteger opens = new AtomicInteger();
      try (final MockedStatic<FileChannel> channels = mockStatic(FileChannel.class, invocation -> {
        if (invocation.getMethod().getName().equals("open") && opens.incrementAndGet() == failAt) {
          throw new IOException("injected file-open failure");
        }
        return invocation.callRealMethod();
      })) {
        assertThrows(SirixIOException.class, () -> openAndClose(config));
      }
      assertEquals(0, openHandles(), "failed open " + failure + " leaked an earlier channel");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void byteHandlerFactoryFailureReleasesChannels(final int failAt) {
    final AtomicInteger instances = new AtomicInteger();
    final ByteHandler handler = new ByteHandler() {
      @Override
      public OutputStream serialize(final OutputStream stream) {
        return stream;
      }

      @Override
      public InputStream deserialize(final InputStream stream) {
        return stream;
      }

      @Override
      public ByteHandler getInstance() {
        if (instances.incrementAndGet() == failAt) {
          throw new IllegalStateException("injected handler-factory failure");
        }
        return this;
      }
    };
    final ResourceConfiguration failing = ResourceConfiguration.newBuilder("resource")
                                                               .resourceUuid(config.resourceUuid)
                                                               .byteHandlerPipeline(new ByteHandlerPipeline(handler))
                                                               .build();
    failing.resourcePath = config.resourcePath;
    assertThrows(IllegalStateException.class, () -> openAndClose(failing));
    assertEquals(0, openHandles(), "handler initialization must not leak already acquired channels");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void unreadableRevisionEndpointClosesTheInitializedReader(final int revision) throws IOException {
    final Path revisions = config.resourcePath.resolve("data/sirix.revisions");
    final ByteBuffer tombstone = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE);
    IOStorage.writePrunedRevisionRecord(tombstone, 0L);
    tombstone.flip();
    try (final FileChannel channel = FileChannel.open(revisions, StandardOpenOption.WRITE)) {
      while (tombstone.hasRemaining()) {
        channel.write(tombstone, IOStorage.revisionsFileOffset(revision) + tombstone.position());
      }
    }
    assertThrows(SirixRevisionPrunedException.class, () -> openAndClose(config));
    assertEquals(0, openHandles(), "revision validation failure must close the fully initialized reader");
  }

  private static void openAndClose(final ResourceConfiguration configuration) {
    final InterruptedFirstCommitRecovery.ReaderAndUberPage opened =
        InterruptedFirstCommitRecovery.openReaderForTool(configuration);
    try (final Reader reader = opened.reader()) {
      assertEquals(1, opened.uberPage().getRevisionNumber());
    }
  }

  private int openHandles() {
    int count = 0;
    try (final Stream<Path> descriptors = Files.list(Path.of("/proc/self/fd"))) {
      for (final Path descriptor : descriptors.toList()) {
        try {
          if (Files.readSymbolicLink(descriptor).startsWith(config.resourcePath)) {
            count++;
          }
        } catch (final NoSuchFileException closedConcurrently) {
          // Unrelated JVM descriptors can close while /proc is being enumerated.
        }
      }
    } catch (final IOException failure) {
      throw new AssertionError("Cannot inspect operating-system resource ownership", failure);
    }
    return count;
  }
}

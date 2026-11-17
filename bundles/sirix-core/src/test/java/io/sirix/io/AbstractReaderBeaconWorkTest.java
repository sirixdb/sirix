package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Beacon-attempt accounting follows the calling thread, including failed primary attempts. */
class AbstractReaderBeaconWorkTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"FILE_CHANNEL,false", "FILE_CHANNEL,true", "MEMORY_MAPPED,false", "MEMORY_MAPPED,true"})
  void beaconWorkIsLocalToCallingThread(final StorageType backend, final boolean secondaryOnly) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final ResourceConfiguration config;
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").storageType(backend).build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        config = session.getResourceConfig();
        writer.commit();
      }
    }
    if (secondaryOnly) {
      final Path dataFile = config.resourcePath.resolve("data").resolve(IOStorage.FILENAME);
      final ByteBuffer slot = ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES);
      try (final var channel = FileChannel.open(dataFile, StandardOpenOption.WRITE)) {
        while (slot.hasRemaining()) {
          channel.write(slot, IOStorage.PRIMARY_BEACON_OFFSET + slot.position());
        }
        channel.force(true);
      }
    }
    // DISK_FORMAT.md's crash-window contract requires primary then secondary when the primary is
    // corrupt.
    final long attempts = secondaryOnly
        ? 2
        : 1;
    final long before = AbstractReader.beaconReads();
    assertEquals(1, InterruptedFirstCommitRecovery.probeUberPage(config).getRevisionNumber());
    assertEquals(attempts, AbstractReader.beaconReads() - before);
    final long afterOpening = AbstractReader.beaconReads();
    try (final var executor = Executors.newSingleThreadExecutor()) {
      assertEquals(attempts, executor.submit(() -> {
        final long workerBefore = AbstractReader.beaconReads();
        assertEquals(1, InterruptedFirstCommitRecovery.probeUberPage(config).getRevisionNumber());
        return AbstractReader.beaconReads() - workerBefore;
      }).get().longValue());
    }
    assertEquals(afterOpening, AbstractReader.beaconReads(), "another thread must not add work to this opener");
    assertEquals(1, InterruptedFirstCommitRecovery.probeUberPage(config).getRevisionNumber());
    assertEquals(attempts, AbstractReader.beaconReads() - afterOpening);
  }
}

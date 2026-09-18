/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.Encryptor;
import io.sirix.io.bytepipe.FFILz4Compressor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sorted-view build's spill through a resource's real spill target: its directory, its byte
 * handlers and its cleanup when the resource is opened.
 */
final class ProjectionSortedRunSpillTest {

  private static final long BUDGET = 64L << 10;
  private static final byte[] MARKER = "PLAINTEXT-SORT-KEY-MARKER".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path temporaryDirectory;

  @Test
  void lz4ResourceRoundTripsCompressedAndStoredRunBlocksExactly() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("lz4-spill");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("lz4")
                               .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor()))
                               .build()));
      createPlainResources(database);
      final ResourceConfiguration lz4 = ResourceConfiguration.deserialize(resourcePath(databasePath, "lz4"));
      assertTrue(lz4.byteHandlePipeline.supportsMemorySegments());
      final ProjectionSortedRunSpill spill = ProjectionSortedRunSpill.forResource(lz4);
      assertEquals(lz4.getResource().toAbsolutePath().normalize().resolve("projection-sort-spill"), spill.directory());
      final ProjectionSortedRunAccumulator spilled =
          new ProjectionSortedRunAccumulator(SortedScanFixtures.GROUP_VALUE, BUDGET, spill);
      final List<byte[]> keys = appendUntilSpilled(spilled, 3, false);
      assertEquals(keys.size(), spilled.rowCount());
      final Path runs = spilled.runDirectory();
      assertNotNull(runs);
      assertTrue(runs.startsWith(spill.directory()), "runs live under the resource's own spill directory");
      if (FFILz4Compressor.isNativeAvailable()) {
        assertTrue(runBytes(runs) < plainBytes(keys) / 2, "compressible runs must be stored compressed");
      }
      assertPersistsLikeAResidentBuild(database, spilled, keys);
      assertFalse(Files.exists(runs), "a merged build deletes its runs and its build directory");
    }
  }

  /**
   * An {@code Encryptor} pipeline takes the stream path. The pipeline is the one an encrypted
   * resource carries, keyed by the key the byte-handler tests use; a resource created with it cannot
   * be bootstrapped by this build, so the spill target is given that pipeline directly.
   */
  @Test
  void encryptorPipelineSpillsNoPlaintextKeyAndReadsItsRunsBack() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("encrypted-spill");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      createPlainResources(database);
      final ByteHandlerPipeline encrypted =
          new ByteHandlerPipeline(new Encryptor(Path.of("src", "test", "resources", "resourceName")));
      assertFalse(encrypted.supportsMemorySegments(), "an Encryptor takes the stream path");
      final Path spillDirectory = temporaryDirectory.resolve("encrypted-resource").resolve("projection-sort-spill");
      final ProjectionSortedRunAccumulator spilled = new ProjectionSortedRunAccumulator(SortedScanFixtures.GROUP_VALUE,
          BUDGET, new ProjectionSortedRunSpill(spillDirectory, encrypted));
      final List<byte[]> keys = appendUntilSpilled(spilled, 3, true);
      final Path runs = spilled.runDirectory();
      assertNotNull(runs);
      assertTrue(runs.startsWith(spillDirectory.toAbsolutePath().normalize()));
      final byte[] plain = concatenated(keys);
      assertTrue(indexOf(plain, MARKER) >= 0, "the fixture's keys carry the marker");
      try (Stream<Path> files = Files.list(runs)) {
        final List<Path> runFiles = files.toList();
        assertEquals(3, runFiles.size());
        for (final Path run : runFiles) {
          final byte[] stored = Files.readAllBytes(run);
          assertTrue(stored.length > 0);
          assertEquals(-1, indexOf(stored, MARKER), "a spilled run must not hold a plaintext sort key: " + run);
        }
      }
      assertPersistsLikeAResidentBuild(database, spilled, keys);
      assertFalse(Files.exists(runs));
    }
  }

  @Test
  void openingAResourceRemovesOrphanedRunsButKeepsLiveBuilds() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("orphans");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final Path live;
    final Path deadProcess;
    final Path deadThisProcess;
    final Path stray;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final ProjectionSortedRunSpill spill = ProjectionSortedRunSpill.forResource(
          ResourceConfiguration.deserialize(resourcePath(databasePath, "resource")));
      live = spill.createBuildDirectory();
      Files.writeString(live.resolve("run-0.keys"), "live");
      deadProcess = Files.createDirectories(spill.directory().resolve("build-" + Long.MAX_VALUE + "-3"));
      Files.writeString(deadProcess.resolve("run-0.keys"), "orphan");
      deadThisProcess =
          Files.createDirectories(spill.directory().resolve("build-" + ProcessHandle.current().pid() + "-999999999"));
      Files.writeString(deadThisProcess.resolve("run-1.keys"), "orphan");
      stray = Files.writeString(spill.directory().resolve("stray.tmp"), "orphan");
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource")) {
      assertEquals("resource", session.getResourceConfig().getResource().getFileName().toString());
      assertFalse(Files.exists(deadProcess), "runs of a build whose process died are removed");
      assertFalse(Files.exists(deadThisProcess), "an unregistered build of this process is no longer live");
      assertFalse(Files.exists(stray), "anything else in the spill directory is removed");
      assertTrue(Files.exists(live.resolve("run-0.keys")), "a live build of this JVM is kept");
    } finally {
      Files.deleteIfExists(live.resolve("run-0.keys"));
      Files.deleteIfExists(live);
      ProjectionSortedRunSpill.retireBuildDirectory(live);
    }
  }

  @Test
  void unwritableSpillDirectoryFailsTheBuildNamingIt() throws IOException {
    final Path notADirectory = Files.writeString(temporaryDirectory.resolve("spill-is-a-file"), "x");
    final ProjectionSortedRunAccumulator run = new ProjectionSortedRunAccumulator(SortedScanFixtures.GROUP_VALUE,
        BUDGET, new ProjectionSortedRunSpill(notADirectory.resolve("resource"), new ByteHandlerPipeline()));
    final SirixIOException failure = assertThrows(SirixIOException.class, () -> appendUntilSpilled(run, 1, false));
    assertTrue(failure.getMessage().contains(notADirectory.resolve("resource").toString()), failure.getMessage());
    assertTrue(failure.getMessage().contains(ProjectionSortedRunSpill.SPILL_DIRECTORY_PROPERTY), failure.getMessage());
    run.release();
  }

  @Test
  void releasingABuildDeletesItsRunsAndItsBuildDirectory() throws IOException {
    final Path spillDirectory = temporaryDirectory.resolve("released");
    final ProjectionSortedRunAccumulator run = new ProjectionSortedRunAccumulator(SortedScanFixtures.GROUP_VALUE,
        BUDGET, new ProjectionSortedRunSpill(spillDirectory, new ByteHandlerPipeline()));
    appendUntilSpilled(run, 2, false);
    final Path runs = run.runDirectory();
    assertNotNull(runs);
    try (Stream<Path> files = Files.list(runs)) {
      assertTrue(files.count() >= 2);
    }
    run.release();
    assertFalse(Files.exists(runs));
    try (Stream<Path> leftovers = Files.list(spillDirectory)) {
      assertEquals(0, leftovers.count());
    }
  }

  /**
   * Append compressible keys until {@code runs} runs have spilled. The key whose append triggered the
   * last spill is then the only resident key: persisting spills it as a one-key run, a block too
   * small for LZ4 to shrink, which it therefore stores uncompressed.
   */
  private static List<byte[]> appendUntilSpilled(final ProjectionSortedRunAccumulator run, final int runs,
      final boolean marked) {
    final List<byte[]> keys = new ArrayList<>();
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    long record = 1;
    while (run.spilledRunCount() < runs) {
      final byte[] group = ((marked
          ? "PLAINTEXT-SORT-KEY-MARKER-"
          : "group-") + (record % 97)).getBytes(StandardCharsets.UTF_8);
      writer.reset();
      writer.appendUtf8(group, 0, group.length);
      writer.appendLong(record * 1_000L);
      writer.appendRecordKey(record++);
      run.append(writer.bytesRef(), writer.length());
      keys.add(writer.copyKey());
    }
    return keys;
  }

  private static void createPlainResources(final Database<JsonResourceSession> database) {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("spilled").build()));
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resident").build()));
  }

  /**
   * Persist the spilled build and a resident build of the same keys; both views must be identical.
   */
  private static void assertPersistsLikeAResidentBuild(final Database<JsonResourceSession> database,
      final ProjectionSortedRunAccumulator spilled, final List<byte[]> keys) {
    final ProjectionSortedRunAccumulator resident = new ProjectionSortedRunAccumulator(SortedScanFixtures.GROUP_VALUE,
        Long.MAX_VALUE, new ProjectionSortedRunSpill(Path.of("unused"), new ByteHandlerPipeline()));
    for (final byte[] key : keys) {
      resident.append(key, key.length);
    }
    assertEquals(0, resident.spilledRunCount());
    try (JsonResourceSession spilledSession = database.beginResourceSession("spilled");
        JsonResourceSession residentSession = database.beginResourceSession("resident")) {
      try (JsonNodeTrx spilledWriter = spilledSession.beginNodeTrx();
          JsonNodeTrx residentWriter = residentSession.beginNodeTrx()) {
        assertEquals(keys.size(),
            spilled.persist(new ProjectionIndexHOTStorage(spilledWriter.getStorageEngineWriter(), 0)));
        assertEquals(keys.size(),
            resident.persist(new ProjectionIndexHOTStorage(residentWriter.getStorageEngineWriter(), 0)));
        spilledWriter.commit();
        residentWriter.commit();
      }
      try (JsonNodeReadOnlyTrx spilledReader = spilledSession.beginNodeReadOnlyTrx();
          JsonNodeReadOnlyTrx residentReader = residentSession.beginNodeReadOnlyTrx()) {
        final ProjectionSortedDirectory.Accessor fromSpill =
            ProjectionSortedDirectory.open(spilledReader.getStorageEngineReader(), 0);
        final ProjectionSortedDirectory.Accessor fromHeap =
            ProjectionSortedDirectory.open(residentReader.getStorageEngineReader(), 0);
        assertNotNull(fromSpill);
        assertNotNull(fromHeap);
        final ProjectionSortedDirectory.Accessor.Cursor left = fromSpill.first();
        final ProjectionSortedDirectory.Accessor.Cursor right = fromHeap.first();
        int visited = 0;
        while (right.isValid()) {
          assertTrue(left.isValid());
          assertArrayEquals(right.copyKey(), left.copyKey());
          left.advance();
          right.advance();
          visited++;
        }
        assertFalse(left.isValid());
        assertEquals(keys.size(), visited);
      }
    } finally {
      spilled.release();
      resident.release();
    }
  }

  private static Path resourcePath(final Path databasePath, final String resource) {
    return databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resource).normalize();
  }

  private static long runBytes(final Path runs) throws IOException {
    long bytes = 0;
    try (Stream<Path> files = Files.list(runs)) {
      for (final Path run : files.toList()) {
        bytes += Files.size(run);
      }
    }
    return bytes;
  }

  private static long plainBytes(final List<byte[]> keys) {
    long bytes = 0;
    for (final byte[] key : keys) {
      bytes += key.length + Short.BYTES;
    }
    return bytes;
  }

  private static byte[] concatenated(final List<byte[]> keys) {
    final byte[] all = new byte[(int) plainBytes(keys)];
    int at = 0;
    for (final byte[] key : keys) {
      System.arraycopy(key, 0, all, at, key.length);
      at += key.length + Short.BYTES;
    }
    return all;
  }

  private static int indexOf(final byte[] haystack, final byte[] needle) {
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      if (Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
        return i;
      }
    }
    return -1;
  }
}

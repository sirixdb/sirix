package io.sirix.access;

import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FormatRepairBoundaryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void derivedRepairRefusesForeignFormatsWithoutChangingFiles(final StorageType backend) throws IOException {
    for (final String site : new String[] {"settings", "data", "revisions", "data-missing-settings",
        "revisions-missing-settings", "uber"}) {
      final Path database = createStore("derived-" + backend + "-" + site, backend, true);
      final Path fixture = resourcePath(database, "fixture");
      final Path derived = resourcePath(database, "source" + DerivedIndexMaintainer.SUFFIX);
      Files.move(fixture, derived);
      if (site.equals("settings")) {
        final Path settings = derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath());
        final String original = Files.readString(settings);
        final String patched = original.replace("\"binaryEncoding\":\"V0\"", "\"binaryEncoding\":\"V7\"");
        assertTrue(!original.equals(patched));
        Files.writeString(settings, patched);
      } else if (site.equals("uber")) {
        final Path data = file(derived, "data");
        final byte[] bytes = Files.readAllBytes(data);
        final ByteBuffer slot = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        final int offset = (int) IOStorage.PRIMARY_BEACON_OFFSET;
        final int length = slot.getInt(offset);
        bytes[offset + Integer.BYTES + 1] = 1;
        slot.order(ByteOrder.BIG_ENDIAN).putLong(offset + Integer.BYTES + length,
            LongHashFunction.xx3().hashBytes(bytes, offset + Integer.BYTES, length));
        Files.write(data, bytes);
      } else {
        patchHeader(file(derived, site.startsWith("data") ? "data" : "revisions"), "version");
        if (site.endsWith("missing-settings")) {
          Files.writeString(derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()), "{}");
        }
      }
      final byte[] dataBefore = Files.readAllBytes(file(derived, "data"));
      final byte[] revisionsBefore = Files.readAllBytes(file(derived, "revisions"));
      final byte[] settingsBefore = Files.readAllBytes(
          derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()));
      Databases.clearGlobalCaches();
      try (final var opened = Databases.openJsonDatabase(database)) {
        final RuntimeException refusal = assertThrows(RuntimeException.class, () -> {
          try (final JsonResourceSession ignored = opened.beginResourceSession("source")) {
          }
        });
        assertTrue(messageChain(refusal).contains("unsupported format version"), messageChain(refusal));
        assertTrue(messageChain(refusal).contains(site.equals("settings") ? "V7" : "1"), messageChain(refusal));
      }
      assertArrayEquals(dataBefore, Files.readAllBytes(file(derived, "data")));
      assertArrayEquals(revisionsBefore, Files.readAllBytes(file(derived, "revisions")));
      assertArrayEquals(settingsBefore, Files.readAllBytes(
          derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath())));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void bootstrapRecoveryChecksBothFileIdentitiesBeforeTruncation(final StorageType backend) throws IOException {
    for (final String role : new String[] {"data", "revisions"}) {
      for (final String defect : new String[] {"version", "uuid", "role", "stride"}) {
        final Path database = createStore("bootstrap-" + backend + "-" + role + "-" + defect, backend, false);
        final Path resource = resourcePath(database, "fixture");
        final Path data = file(resource, "data");
        final Path revisions = file(resource, "revisions");
        final byte[] dataImage = Files.readAllBytes(data);
        Arrays.fill(dataImage, (int) IOStorage.PRIMARY_BEACON_OFFSET, dataImage.length, (byte) 0);
        Files.write(data, dataImage);
        Files.write(revisions, Arrays.copyOf(Files.readAllBytes(revisions), (int) IOStorage.REVISIONS_RECORDS_START));
        patchHeader(file(resource, role), defect);
        final byte[] dataBefore = Files.readAllBytes(data);
        final byte[] revisionsBefore = Files.readAllBytes(revisions);
        Databases.clearGlobalCaches();
        try (final var opened = Databases.openJsonDatabase(database)) {
          final RuntimeException refusal = assertThrows(RuntimeException.class, () -> {
            try (final JsonResourceSession ignored = opened.beginResourceSession("fixture")) {
            }
          });
          if (defect.equals("version")) {
            assertTrue(messageChain(refusal).contains("unsupported format version"), messageChain(refusal));
          }
        }
        assertArrayEquals(dataBefore, Files.readAllBytes(data), role + " " + defect);
        assertArrayEquals(revisionsBefore, Files.readAllBytes(revisions), role + " " + defect);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void zeroDataHeaderCannotHideForeignRevisionsFormat(final StorageType backend) throws IOException {
    final Path database = createStore("zero-header-" + backend, backend, false);
    final Path resource = resourcePath(database, "fixture");
    final Path data = file(resource, "data");
    final Path revisions = file(resource, "revisions");
    Files.write(data, new byte[(int) IOStorage.DATA_REGION_START + 1]);
    Files.write(revisions, Arrays.copyOf(Files.readAllBytes(revisions), (int) IOStorage.REVISIONS_RECORDS_START));
    patchHeader(revisions, "version");
    final byte[] dataBefore = Files.readAllBytes(data);
    final byte[] revisionsBefore = Files.readAllBytes(revisions);
    Databases.clearGlobalCaches();
    try (final var opened = Databases.openJsonDatabase(database)) {
      final RuntimeException refusal = assertThrows(RuntimeException.class, () -> opened.beginResourceSession("fixture"));
      assertTrue(messageChain(refusal).contains("unsupported format version"), messageChain(refusal));
    }
    assertArrayEquals(dataBefore, Files.readAllBytes(data));
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisions));
  }

  private Path createStore(final String name, final StorageType backend, final boolean derived) {
    final Path database = directory.resolve(name);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final var opened = Databases.openJsonDatabase(database)) {
      if (derived) {
        assertTrue(opened.createResource(ResourceConfiguration.newBuilder("source").storageType(backend)
            .useConventionalValidTimePaths().build()));
      }
      assertTrue(opened.createResource(ResourceConfiguration.newBuilder("fixture").storageType(backend)
          .aggregateKernel(derived).build()));
      if (derived) {
        try (final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    return database;
  }

  private static void patchHeader(final Path file, final String defect) throws IOException {
    final byte[] bytes = Files.readAllBytes(file);
    final ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    switch (defect) {
      case "version" -> header.putInt(8, 1);
      case "uuid" -> header.putLong(40, header.getLong(40) ^ 1L);
      case "role" -> header.put(12, (byte) (header.get(12) ^ 1));
      case "stride" -> header.putInt(20, 0);
      default -> throw new IllegalArgumentException(defect);
    }
    header.putLong(56, LongHashFunction.xx3().hashBytes(bytes, 0, 56));
    Files.write(file, bytes);
  }

  private static Path resourcePath(final Path database, final String name) {
    return database.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);
  }

  private static Path file(final Path resource, final String role) {
    return resource.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve(role.equals("data") ? IOStorage.FILENAME : IOStorage.REVISIONS_FILENAME);
  }

  private static String messageChain(final Throwable failure) {
    final StringBuilder message = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      message.append(cause.getMessage()).append('\n');
    }
    return message.toString();
  }
}

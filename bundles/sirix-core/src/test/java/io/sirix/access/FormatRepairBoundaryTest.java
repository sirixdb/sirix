package io.sirix.access;

import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.io.filechannel.FileChannelWriter;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.io.StorageType;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import java.util.HexFormat;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FormatRepairBoundaryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void derivedRepairRefusesForeignFormatsWithoutChangingFiles(final StorageType backend) throws IOException {
    for (final String site : new String[] {"settings", "data", "revisions", "data-missing-settings",
        "revisions-missing-settings", "uber", "root-missing-settings", "historical-root-missing-settings",
        "settings-after-unknown", "settings-after-unknown-truncated"}) {
      final Path database = createStore("derived-" + backend + "-" + site, backend, true);
      final Path fixture = resourcePath(database, "fixture");
      final Path derived = resourcePath(database, "source" + DerivedIndexMaintainer.SUFFIX);
      Files.move(fixture, derived);
      if (site.startsWith("settings")) {
        final Path settings = derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath());
        final String original = Files.readString(settings);
        final String patched = original.replace("\"binaryEncoding\":\"V0\"", "\"binaryEncoding\":\"V7\"");
        assertTrue(!original.equals(patched));
        if (site.equals("settings-after-unknown-truncated")) {
          Files.writeString(settings, "{\"unexpected\":{},\"binaryEncoding\":\"V7\",");
        } else {
          Files.writeString(settings, site.equals("settings-after-unknown")
              ? "{\"unexpected\":{\"nested\":[1,2]}," + patched.substring(1)
              : patched);
        }
      } else if (site.endsWith("root-missing-settings")) {
        patchRoot(derived, site.startsWith("historical") ? 0 : 1);
        Files.writeString(derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()), "{}");
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
      final Map<String, String> before = snapshot(derived);
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
        assertTrue(messageChain(refusal).contains(site.startsWith("settings") ? "V7" : "1"), messageChain(refusal));
      }
      assertEquals(before, snapshot(derived), site);
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
  void beaconlessForeignEnvelopePreventsBootstrapTruncation(final StorageType backend) throws IOException {
    for (final boolean zeroHeader : new boolean[] {false, true}) {
      final Path database = createStore("beaconless-" + backend + "-" + zeroHeader, backend, false);
      final Path resource = resourcePath(database, "fixture");
      patchRoot(resource, 0);
      final byte[] data = Files.readAllBytes(file(resource, "data"));
      Arrays.fill(data, zeroHeader ? 0 : (int) IOStorage.PRIMARY_BEACON_OFFSET,
          (int) IOStorage.DATA_REGION_START, (byte) 0);
      Files.write(file(resource, "data"), data);
      Files.write(file(resource, "revisions"),
          Arrays.copyOf(Files.readAllBytes(file(resource, "revisions")), (int) IOStorage.REVISIONS_RECORDS_START));
      final Map<String, String> before = snapshot(resource);
      Databases.clearGlobalCaches();
      try (final var opened = Databases.openJsonDatabase(database)) {
        final RuntimeException failure = assertThrows(RuntimeException.class,
            () -> opened.beginResourceSession("fixture"));
        assertTrue(messageChain(failure).contains("unsupported format version 1"), messageChain(failure));
      }
      assertEquals(before, snapshot(resource));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void validHistoricalRootIsCheckedBeforeCleanupOrHealing(final StorageType backend) throws IOException {
    for (final boolean tornLatest : new boolean[] {false, true}) {
      final Path database = createStore("historical-" + backend + "-" + tornLatest, backend, false);
      final Path resource = resourcePath(database, "fixture");
      try (final var opened = Databases.openJsonDatabase(database);
          final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
        writer.commit();
        writer.commit();
        writer.commit();
      }
      patchRoot(resource, 1);
      if (tornLatest) {
        final byte[] records = Files.readAllBytes(file(resource, "revisions"));
        Arrays.fill(records, (int) IOStorage.revisionsFileOffset(3),
            (int) IOStorage.revisionsFileOffset(4), (byte) 0);
        Files.write(file(resource, "revisions"), records);
      }
      final Path spill = resource.resolve("projection-sort-spill/build-999999999-orphan");
      Files.createDirectories(spill);
      Files.writeString(spill.resolve("run.bin"), "must survive refusal");
      final Map<String, String> before = snapshot(resource);
      Databases.clearGlobalCaches();
      try (final var opened = Databases.openJsonDatabase(database)) {
        final RuntimeException failure = assertThrows(RuntimeException.class,
            () -> opened.beginResourceSession("fixture"));
        assertTrue(messageChain(failure).contains("unsupported format version 1"), messageChain(failure));
      }
      assertEquals(before, snapshot(resource));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void derivedRepairStillRebuildsWhenNoForeignSettingsVersionIsReadable(final StorageType backend)
      throws IOException {
    for (final String version : new String[] {"\"V0\"", "null", "", "absent"}) {
      final Path database = createStore("readable-settings-" + backend + "-" + version.replace('"', '_'), backend, true);
      final Path derived = resourcePath(database, "source" + DerivedIndexMaintainer.SUFFIX);
      Files.move(resourcePath(database, "fixture"), derived);
      Files.writeString(derived.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()),
          version.equals("absent") ? "{}" : "{\"unexpected\":{},\"binaryEncoding\":" + version + "}");
      Databases.clearGlobalCaches();
      try (final var opened = Databases.openJsonDatabase(database);
          final var session = opened.beginResourceSession("source")) {
        assertEquals(0, session.getMostRecentRevisionNumber());
      }
      assertEquals(StorageType.FILE_CHANNEL, ResourceConfiguration.deserialize(derived).storageType);
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

  /** FMT-401: foreign envelopes must leave every resource file unchanged, even during recovery. */
  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void foreignRootRefusedBeforeRecoverySideEffects(final StorageType backend) throws IOException {
    for (final String crash : new String[] {"orphan-spill", "commit-marker", "torn-record", "bootstrap"}) {
      final Path database = createStore("foreign-root-" + backend + "-" + crash, backend, false);
      final Path resource = resourcePath(database, "fixture");
      final int revision = crash.equals("bootstrap") ? 0 : 1;
      if (revision == 1) {
        try (final var opened = Databases.openJsonDatabase(database);
            final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
          writer.commit();
        }
      }
      patchRoot(resource, revision);
      if (crash.equals("orphan-spill")) {
        final Path spill = resource.resolve("projection-sort-spill/build-999999999-orphan");
        Files.createDirectories(spill);
        Files.writeString(spill.resolve("run.bin"), "must survive refusal");
      } else if (crash.equals("commit-marker")) {
        Files.write(file(resource, "data"), new byte[4096], StandardOpenOption.APPEND);
        Files.writeString(resource.resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
            .resolve(".commit"), "");
      } else {
        final byte[] records = Files.readAllBytes(file(resource, "revisions"));
        final int start = (int) IOStorage.revisionsFileOffset(revision);
        Arrays.fill(records, start, start + IOStorage.REVISIONS_FILE_RECORD_SIZE, (byte) 0);
        Files.write(file(resource, "revisions"), records);
        if (revision == 0) {
          final byte[] bytes = Files.readAllBytes(file(resource, "data"));
          for (final long slot : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
            Arrays.fill(bytes, (int) slot + IOStorage.REVISION_RECORD_TAIL_LOG_SLOT_OFFSET,
                (int) slot + IOStorage.BEACON_SLOT_BYTES, (byte) 0);
          }
          Files.write(file(resource, "data"), bytes);
        }
      }
      final Map<String, String> before = snapshot(resource);
      Databases.clearGlobalCaches();
      try (final var opened = Databases.openJsonDatabase(database)) {
        final RuntimeException refusal = assertThrows(RuntimeException.class, () -> {
          try (final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
            writer.commit();
          }
        }, crash);
        assertTrue(messageChain(refusal).contains("unsupported format version"), messageChain(refusal));
      }
      assertEquals(before, snapshot(resource), crash);
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void discoversAllSalvageBeforeHealingAnyRecord(final StorageType backend) throws IOException {
    final Path database = createStore("mixed-salvage-" + backend, backend, false);
    final Path resource = resourcePath(database, "fixture");
    try (final var opened = Databases.openJsonDatabase(database);
        final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
      writer.commit();
      writer.commit();
      writer.commit();
    }
    patchRoot(resource, 2);
    final byte[] records = Files.readAllBytes(file(resource, "revisions"));
    Arrays.fill(records, (int) IOStorage.revisionsFileOffset(1), (int) IOStorage.revisionsFileOffset(4), (byte) 0);
    Files.write(file(resource, "revisions"), records);
    final Map<String, String> before = snapshot(resource);
    Databases.clearGlobalCaches();
    try (final var opened = Databases.openJsonDatabase(database)) {
      final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
        try (final var ignored = opened.beginResourceSession("fixture")) {
        }
      });
      assertTrue(messageChain(failure).contains("unsupported format version"), messageChain(failure));
    }
    assertEquals(before, snapshot(resource), "an earlier compatible record must not heal before foreign refusal");
  }

  @Test
  void directRepairsValidateFileAndRootFormats() throws IOException {
    for (final String repair : new String[] {"heal", "truncate"}) {
      for (final String site : new String[] {"data", "revisions", "root"}) {
        final Path database = createStore("direct-" + repair + "-" + site, StorageType.FILE_CHANNEL, false);
        try (final var opened = Databases.openJsonDatabase(database);
            final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
          writer.commit();
        }
        final Path resource = resourcePath(database, "fixture");
        final ResourceConfiguration config = ResourceConfiguration.deserialize(resource);
        if (site.equals("root")) {
          patchRoot(resource, 1);
        } else {
          patchHeader(file(resource, site), "version");
        }
        if (repair.equals("heal")) {
          final byte[] records = Files.readAllBytes(file(resource, "revisions"));
          final int offset = (int) IOStorage.revisionsFileOffset(1);
          Arrays.fill(records, offset, offset + IOStorage.REVISIONS_FILE_RECORD_SIZE, (byte) 0);
          Files.write(file(resource, "revisions"), records);
        } else {
          Files.write(file(resource, "data"), new byte[4096], StandardOpenOption.APPEND);
        }
        final Map<String, String> before = snapshot(resource);
        final AsyncCache<Integer, RevisionFileData> cache = Caffeine.newBuilder().buildAsync();
        try (final FileChannel data = FileChannel.open(file(resource, "data"), StandardOpenOption.READ,
                StandardOpenOption.WRITE);
            final FileChannel revisions = FileChannel.open(file(resource, "revisions"), StandardOpenOption.READ,
                StandardOpenOption.WRITE);
            final FileChannelReader reader = new FileChannelReader(data, revisions,
                new ByteHandlerPipeline(config.byteHandlePipeline), SerializationType.DATA, new PagePersister(),
                cache.synchronous())) {
          final RuntimeException failure;
          if (repair.equals("heal")) {
            failure = assertThrows(RuntimeException.class, () -> reader.getRevisionFileData(1), repair + " " + site);
          } else {
            try (final FileChannelWriter writer = new FileChannelWriter(data, revisions, data, SerializationType.DATA,
                new PagePersister(), cache, new RevisionIndexHolder(), reader, false, false,
                file(resource, "revisions"), config.resourceUuid.getMostSignificantBits(),
                config.resourceUuid.getLeastSignificantBits())) {
              failure = assertThrows(RuntimeException.class, () -> writer.truncateTo(1), repair + " " + site);
            }
          }
          assertTrue(messageChain(failure).contains("unsupported format version"), messageChain(failure));
        }
        assertEquals(before, snapshot(resource), repair + " " + site);
      }
    }
  }

  private static Map<String, String> snapshot(final Path resource) throws IOException {
    final Map<String, String> files = new TreeMap<>();
    final MessageDigest hash;
    try {
      hash = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException unavailable) {
      throw new AssertionError(unavailable);
    }
    try (final var paths = Files.walk(resource)) {
      for (final Path path : paths.filter(Files::isRegularFile).toList()) {
        files.put(resource.relativize(path).toString(), Files.size(path) + ":" + HexFormat.of().formatHex(hash.digest(Files.readAllBytes(path))));
      }
    }
    return files;
  }

  private static void patchRoot(final Path resource, final int revision) throws IOException {
    final byte[] bytes = Files.readAllBytes(file(resource, "data"));
    final byte[] records = Files.readAllBytes(file(resource, "revisions"));
    final ByteBuffer data = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffer index = ByteBuffer.wrap(records).order(ByteOrder.LITTLE_ENDIAN);
    final int record = (int) IOStorage.revisionsFileOffset(revision);
    final long offset = index.getLong(record);
    final int start = Math.toIntExact(offset);
    final int length = data.getInt(start);
    bytes[start + 5] = 1;
    final long hash = LongHashFunction.xx3().hashBytes(bytes, start + 4, length);
    final long timestamp = index.getLong(record + 8);
    index.putLong(record + 16, IOStorage.revisionRecordChecksum(offset, timestamp, hash));
    index.putLong(record + 24, hash);
    for (final long slot : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
      final int base = (int) slot + IOStorage.tailLogEntryOffsetInSlot(revision);
      if (data.getInt(base) == revision && data.getLong(base + 8) == offset) {
        data.putLong(base + 24, index.getLong(record + 16));
        data.putLong(base + 32, hash);
        data.putLong(base + 40, LongHashFunction.xx3().hashBytes(bytes, base, 40));
      }
    }
    Files.write(file(resource, "data"), bytes);
    Files.write(file(resource, "revisions"), records);
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

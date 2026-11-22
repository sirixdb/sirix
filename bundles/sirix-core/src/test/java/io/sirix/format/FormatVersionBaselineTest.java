package io.sirix.format;

import io.sirix.BinaryEncodingVersion;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.IOStorage;
import io.sirix.io.Superblock;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FMT-401 ordinary-open coverage: these fixtures exercise foreign superblock layout versions,
 * page-envelope versions and persisted {@code binaryEncoding} settings. Their ordinary opens
 * refuse before mutation with a plain unsupported-format-version error, without naming a pre-1.0
 * development format. Page decoding fixtures cover FILE_CHANNEL and MEMORY_MAPPED. The repair and
 * recovery boundaries are enforced as well, through the shared compatibility discovery their
 * destructive paths must pass; FormatRepairBoundaryTest covers those combinations. FMT-401 owns
 * the requirement and records what is still missing there (the FMT-208 corpus).
 *
 * <p>
 * FMT-102: the development-era acceptances of a zero resource UUID and a zero revisions record
 * stride are gone, on both the settings and the superblock side.
 *
 * <p>
 * Every fixture here re-checksums the header it patches, so a refusal is the version/identity rule
 * talking and never the checksum catching an incidental corruption.
 */
final class FormatVersionBaselineTest {

  private static final String RESOURCE = "resource";

  @TempDir
  Path directory;

  @Test
  void unsupportedSuperblockLayoutVersionIsRefusedBeforeAnyMutation() throws IOException {
    final String message = refuseUnsupportedLayoutVersion();
    assertTrue(message.contains("unsupported format version"), message);
    assertTrue(message.contains(Integer.toString(Superblock.LAYOUT_VERSION + 1)), message);
  }

  @Test
  void zeroResourceUuidInTheSuperblockIsRefusedBeforeAnyMutation() throws IOException {
    assertTrue(refuseZeroSuperblockUuid().contains("zero resource UUID"));
  }

  @Test
  void zeroRevisionsRecordStrideIsRefusedBeforeAnyMutation() throws IOException {
    assertTrue(refuseZeroRevisionsStride().contains("revisions record stride 0"));
  }

  @Test
  void unsupportedSettingsFormatVersionIsRefusedBeforeAnyMutation() throws IOException {
    final String message = refuseUnsupportedSettingsVersion();
    assertTrue(message.contains("unsupported format version"), message);
    assertTrue(message.contains("V7"), message);
  }

  @Test
  void absentResourceUuidSettingIsRefusedBeforeAnyMutation() throws IOException {
    final Path databasePath = createStore("absent-uuid-setting");
    final String settings = Files.readString(settingsFile(databasePath));
    final int start = settings.indexOf(",\"resourceUuid\":\"");
    assertTrue(start >= 0, settings);
    final int end = settings.indexOf('"', settings.indexOf(':', start) + 2);
    patchSettings(databasePath, settings.substring(start, end + 1), "");

    final String message = assertRefusedWithoutMutating(databasePath);
    assertTrue(message.contains("resourceUuid"), message);
  }

  @Test
  void zeroResourceUuidSettingIsRefusedBeforeAnyMutation() throws IOException {
    final Path databasePath = createStore("zero-uuid-setting");
    final String settings = Files.readString(settingsFile(databasePath));
    final int start = settings.indexOf("\"resourceUuid\":\"") + "\"resourceUuid\":\"".length();
    patchSettings(databasePath, settings.substring(start, start + 36), "00000000-0000-0000-0000-000000000000");

    final String message = assertRefusedWithoutMutating(databasePath);
    assertTrue(message.contains("zero resource UUID"), message);
  }

  /** The settings API refuses a non-identity before it can ever reach a superblock. */
  @Test
  void theBuilderRefusesAnAbsentOrZeroResourceIdentity() {
    final ResourceConfiguration.Builder builder = ResourceConfiguration.newBuilder(RESOURCE);
    assertThrows(NullPointerException.class, () -> builder.resourceUuid(null));
    assertThrows(IllegalArgumentException.class, () -> builder.resourceUuid(new UUID(0L, 0L)));
    final UUID identity = UUID.randomUUID();
    assertEquals(identity, builder.resourceUuid(identity).build().resourceUuid);
  }

  @Test
  void foreignPageFormatVersionsAreRefusedThroughStoreDecoding() throws IOException {
    for (final String message : refusePageFormatVersions()) {
      assertTrue(message.contains("unsupported format version"), message);
    }
  }

  /**
   * The engine must not detect, name or special-case a pre-1.0 development format: a refusal says
   * which version it found and nothing about betas, migration or re-importing.
   */
  @Test
  void noRefusalNamesAPre10Format() throws IOException {
    final List<String> collected = new ArrayList<>();
    collected.add(refuseUnsupportedLayoutVersion());
    collected.add(refuseZeroSuperblockUuid());
    collected.add(refuseZeroRevisionsStride());
    collected.add(refuseUnsupportedSettingsVersion());
    collected.addAll(refusePageFormatVersions());

    assertEquals(20, collected.size(), "settings, identities, layout and both page decoding paths");
    for (final String message : collected) {
      final String normalized = message.toLowerCase(Locale.ROOT);
      for (final String forbidden : new String[] {"beta", "pre-1.0", "re-import", "migrat", "upgrade", "convert"}) {
        assertFalse(normalized.contains(forbidden), "refusal names a pre-1.0 format: " + message);
      }
    }
  }

  // ==================== refusals ====================

  private String refuseUnsupportedLayoutVersion() throws IOException {
    final Path databasePath = createStore("layout-version");
    patchHeader(dataFile(databasePath), header -> header.putInt(8, Superblock.LAYOUT_VERSION + 1));
    return assertRefusedWithoutMutating(databasePath);
  }

  private String refuseZeroSuperblockUuid() throws IOException {
    final Path databasePath = createStore("zero-uuid");
    patchHeader(dataFile(databasePath), header -> {
      header.putLong(40, 0L);
      header.putLong(48, 0L);
    });
    return assertRefusedWithoutMutating(databasePath);
  }

  private String refuseZeroRevisionsStride() throws IOException {
    final Path databasePath = createStore("zero-stride");
    patchHeader(revisionsFile(databasePath), header -> header.putInt(20, 0));
    return assertRefusedWithoutMutating(databasePath);
  }

  private String refuseUnsupportedSettingsVersion() throws IOException {
    final Path databasePath = createStore("settings-version");
    patchSettings(databasePath, "\"binaryEncoding\":\"" + BinaryEncodingVersion.V0.name() + "\"",
        "\"binaryEncoding\":\"V7\"");
    return assertRefusedWithoutMutating(databasePath);
  }

  private List<String> refusePageFormatVersions() throws IOException {
    final List<String> messages = new ArrayList<>(16);
    for (final StorageType backend : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      for (final int candidate : new int[] {1, 127, 128, 255}) {
        for (final boolean uber : new boolean[] {true, false}) {
          final Path databasePath = createStore("page-" + backend + "-" + candidate + "-" + uber, backend);
          final byte[] data = Files.readAllBytes(dataFile(databasePath));
          final ByteBuffer bytes = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
          if (uber) {
            final int offset = (int) IOStorage.PRIMARY_BEACON_OFFSET;
            final int length = bytes.getInt(offset);
            data[offset + Integer.BYTES + 1] = (byte) candidate;
            bytes.order(ByteOrder.BIG_ENDIAN).putLong(offset + Integer.BYTES + length,
                LongHashFunction.xx3().hashBytes(data, offset + Integer.BYTES, length));
          } else {
            final byte[] revisions = Files.readAllBytes(revisionsFile(databasePath));
            final ByteBuffer records = ByteBuffer.wrap(revisions).order(ByteOrder.LITTLE_ENDIAN);
            final int record = (int) IOStorage.revisionsFileOffset(1);
            final int offset = Math.toIntExact(records.getLong(record));
            final int length = bytes.getInt(offset);
            data[offset + Integer.BYTES + 1] = (byte) candidate;
            final long hash = LongHashFunction.xx3().hashBytes(data, offset + Integer.BYTES, length);
            records.putLong(record + 24, hash);
            final byte[] identity = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(offset).putLong(records.getLong(record + 8)).putLong(hash).array();
            records.putLong(record + 16, LongHashFunction.xx3().hashBytes(identity));
            Files.write(revisionsFile(databasePath), revisions);
          }
          Files.write(dataFile(databasePath), data);
          final String message = assertRefusedWithoutMutating(databasePath);
          assertTrue(message.contains("unsupported format version"), message);
          assertTrue(message.contains(Byte.toString((byte) candidate)), message);
          messages.add(message);
        }
      }
    }
    return messages;
  }

  // ==================== fixtures ====================

  /** A committed single-resource store on the default backend. */
  private Path createStore(final String name) {
    return createStore(name, StorageType.FILE_CHANNEL);
  }

  private Path createStore(final String name, final StorageType backend) {
    final Path databasePath = directory.resolve(name);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(backend)
          .byteHandlerPipeline(new ByteHandlerPipeline()).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final var trx = session.beginNodeTrx()) {
        trx.commit();
      }
    }
    Databases.clearGlobalCaches();
    return databasePath;
  }

  /**
   * Opens the store and asks for a writer — the first mutation the engine would perform. Returns
   * the refusal message after proving both persistent files are byte-for-byte what they were.
   */
  private String assertRefusedWithoutMutating(final Path databasePath) throws IOException {
    final byte[] dataBefore = Files.readAllBytes(dataFile(databasePath));
    final byte[] revisionsBefore = Files.readAllBytes(revisionsFile(databasePath));

    Databases.clearGlobalCaches();
    // Every stage is inside the refusal: opening the database, opening the resource and asking for
    // a writer are all before the first byte a mutation would write.
    final RuntimeException refusal = assertThrows(RuntimeException.class, () -> {
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final var trx = session.beginNodeTrx()) {
        trx.commit();
      }
    });

    assertArrayEquals(dataBefore, Files.readAllBytes(dataFile(databasePath)), "sirix.data was mutated");
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile(databasePath)),
        "sirix.revisions was mutated");

    return messageChain(refusal);
  }

  /** Applies a header edit at offset 0, restoring the superblock's own XXH3 over bytes [0, 56). */
  private static void patchHeader(final Path file, final HeaderEdit edit) throws IOException {
    final byte[] bytes = Files.readAllBytes(file);
    final ByteBuffer header = ByteBuffer.wrap(bytes, 0, Superblock.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    edit.apply(header);
    header.putLong(56, LongHashFunction.xx3().hashBytes(bytes, 0, 56));
    Files.write(file, bytes);
  }

  private static void patchSettings(final Path databasePath, final String target, final String replacement)
      throws IOException {
    final Path settings = settingsFile(databasePath);
    final String content = Files.readString(settings);
    assertTrue(content.contains(target), content);
    Files.writeString(settings, content.replace(target, replacement));
  }

  private static Path resourcePath(final Path databasePath) {
    return databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(RESOURCE);
  }

  private static Path settingsFile(final Path databasePath) {
    return resourcePath(databasePath).resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath());
  }

  private static Path dataFile(final Path databasePath) {
    return resourcePath(databasePath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                    .resolve(IOStorage.FILENAME);
  }

  private static Path revisionsFile(final Path databasePath) {
    return resourcePath(databasePath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                     .resolve(IOStorage.REVISIONS_FILENAME);
  }

  private static String messageChain(final Throwable throwable) {
    final StringBuilder messages = new StringBuilder();
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    return messages.toString();
  }

  @FunctionalInterface
  private interface HeaderEdit {
    void apply(ByteBuffer header);
  }
}

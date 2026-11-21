package io.sirix.format;

import io.sirix.BinaryEncodingVersion;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Superblock;
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
 * FMT-401: 1.0 is the only on-disk format. A store carrying any other format version — in the
 * superblock layout version, in a page envelope or in the persisted {@code binaryEncoding} setting
 * — is refused before any mutation with a plain unsupported-format-version error, and nothing in
 * the refusal detects or names a pre-1.0 development format.
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
    assertTrue(refuseUnsupportedLayoutVersion().contains("unsupported layout version"));
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
  void everyPageFormatVersionButTheBaselineIsRefusedWithAPlainMessage() {
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

    assertEquals(259, collected.size(), "every refusal the engine can produce for a foreign version");
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

  /** Every page-envelope version byte but the baseline, each naming the value it found. */
  private static List<String> refusePageFormatVersions() {
    final List<String> messages = new ArrayList<>(255);
    for (int candidate = 1; candidate < 256; candidate++) {
      final byte version = (byte) candidate;
      final String message =
          assertThrows(IllegalStateException.class, () -> BinaryEncodingVersion.fromByte(version)).getMessage();
      assertTrue(message.contains(Byte.toString(version)), message);
      messages.add(message);
    }
    return messages;
  }

  // ==================== fixtures ====================

  /** A committed single-resource store on the default backend. */
  private Path createStore(final String name) {
    final Path databasePath = directory.resolve(name);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
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
    final SirixIOException refusal = assertThrows(SirixIOException.class, () -> {
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

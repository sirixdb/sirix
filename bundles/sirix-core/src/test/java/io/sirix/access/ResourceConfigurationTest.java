package io.sirix.access;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ResourceConfigurationTest {
  @TempDir
  Path directory;

  @Test
  void newResourcesDefaultToSaltedPages() {
    assertTrue(ResourceConfiguration.newBuilder("default").build().saltRecordPages);
    assertTrue(ResourceConfiguration.newBuilder("enabled").saltRecordPages(true).build().saltRecordPages);
    assertFalse(ResourceConfiguration.newBuilder("fixture").saltRecordPages(false).build().saltRecordPages);
  }

  @Test
  void bothModesSurviveSettingsRoundTrips() throws Exception {
    for (final boolean enabled : new boolean[] {false, true}) {
      final ResourceConfiguration config = persist(enabled);
      final JsonObject settings = JsonParser.parseString(Files.readString(config.getConfigFile())).getAsJsonObject();
      assertEquals(enabled, settings.get("saltRecordPages").getAsBoolean());
      assertEquals(enabled, ResourceConfiguration.deserialize(config.getResource()).saltRecordPages);
    }
  }

  @Test
  void oldSettingsDefaultToSaltedPages() throws Exception {
    final ResourceConfiguration config = persist(false);
    final JsonObject settings = JsonParser.parseString(Files.readString(config.getConfigFile())).getAsJsonObject();
    settings.remove("saltRecordPages");
    Files.writeString(config.getConfigFile(), settings.toString());
    assertTrue(ResourceConfiguration.deserialize(config.getResource()).saltRecordPages);
  }

  @Test
  void invalidSaltSettingsFailAtOpen() throws Exception {
    final ResourceConfiguration config = persist(true);
    final JsonObject settings = JsonParser.parseString(Files.readString(config.getConfigFile())).getAsJsonObject();
    for (final String invalid : new String[] {"[]", "{}", "null"}) {
      settings.add("saltRecordPages", JsonParser.parseString(invalid));
      Files.writeString(config.getConfigFile(), settings.toString());
      assertThrows(RuntimeException.class, () -> ResourceConfiguration.deserialize(config.getResource()));
    }
  }

  @Test
  void saltModesAndLegacyDefaultsCoexistWithOptionalFieldsAndExtensions() throws Exception {
    final String[] optionalFields =
        {"stringCompressionType", "deweyIdSiblingDistance", "verifyChecksumsOnRead", "hashAlgorithm"};
    for (int mode = 0; mode < 3; mode++) {
      final ResourceConfiguration config = persist(mode == 2);
      final JsonObject original = JsonParser.parseString(Files.readString(config.getConfigFile())).getAsJsonObject();
      for (int omitted = 0; omitted < 16; omitted++) {
        for (int order = 0; order < 2; order++) {
          final JsonObject settings = original.deepCopy();
          settings.remove("saltRecordPages");
          for (int bit = 0; bit < optionalFields.length; bit++) {
            if ((omitted & (1 << bit)) != 0) {
              settings.remove(optionalFields[bit]);
            }
          }
          if (mode != 0 && order == 0) {
            settings.addProperty("saltRecordPages", mode == 2);
          }
          settings.add("extensions",
              JsonParser.parseString("{\"saltRecordPages\":false,\"future\":[1,{\"saltRecordPages\":true},null]}"));
          if (mode != 0 && order == 1) {
            settings.addProperty("saltRecordPages", mode == 2);
          }
          Files.writeString(config.getConfigFile(), settings.toString());
          assertEquals(mode != 1, ResourceConfiguration.deserialize(config.getResource()).saltRecordPages);
        }
      }
    }
  }

  @Test
  void saltSettingDoesNotRelaxStrictUnknownFieldRefusal() throws Exception {
    final ResourceConfiguration config = persist(false);
    final JsonObject settings = JsonParser.parseString(Files.readString(config.getConfigFile())).getAsJsonObject();
    settings.add("extensions", JsonParser.parseString("{\"future\":[null,{}]}"));
    settings.addProperty("unknownSaltFormat", true);
    Files.writeString(config.getConfigFile(), settings.toString());
    assertTrue(assertThrows(SirixIOException.class,
        () -> ResourceConfiguration.deserialize(config.getResource())).getMessage().contains("unknownSaltFormat"));
  }

  @Test
  void toolResourceReadDoesNotSeedDatabaseMetadataForAReplacementCopy() throws Exception {
    final ResourceConfiguration resource = persist(false);
    final DatabaseConfiguration database = DatabaseConfiguration.deserializeForTool(directory).setDatabaseId(11);
    DatabaseConfiguration.serialize(database);
    assertEquals(11, ResourceConfiguration.deserializeForTool(resource.getResource()).getDatabaseId());

    // TXN-613: a copied path can be replaced with equal-size metadata at the same timestamp.
    // dbsetting.obj is the owned persisted configuration contract, not implementation source.
    final Path settingsFile = database.getConfigFile();
    final FileTime timestamp = Files.getLastModifiedTime(settingsFile);
    final long size = Files.size(settingsFile);
    final JsonObject replacement = JsonParser.parseString(Files.readString(settingsFile)).getAsJsonObject();
    replacement.addProperty("databaseId", 22);
    Files.writeString(settingsFile, replacement.toString());
    assertEquals(size, Files.size(settingsFile), "the fixture must model an equal-size replacement copy");
    Files.setLastModifiedTime(settingsFile, timestamp);

    assertEquals(22, DatabaseConfiguration.deserialize(directory).getDatabaseId(),
        "an isolated tool read must not publish stale metadata into the later database-open path");
  }

  private ResourceConfiguration persist(final boolean enabled) throws Exception {
    final DatabaseConfiguration database = new DatabaseConfiguration(directory).setDatabaseType(DatabaseType.JSON);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("resource").saltRecordPages(enabled).build();
    config.setDatabaseConfiguration(database);
    Files.createDirectories(config.getResource());
    ResourceConfiguration.serialize(config);
    return config;
  }
}

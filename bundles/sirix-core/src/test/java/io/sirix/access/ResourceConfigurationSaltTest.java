package io.sirix.access;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ResourceConfigurationSaltTest {
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

  private ResourceConfiguration persist(final boolean enabled) throws Exception {
    final DatabaseConfiguration database = new DatabaseConfiguration(directory).setDatabaseType(DatabaseType.JSON);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("resource").saltRecordPages(enabled).build();
    config.setDatabaseConfiguration(database);
    Files.createDirectories(config.getResource());
    ResourceConfiguration.serialize(config);
    return config;
  }
}

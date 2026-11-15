package io.sirix.access;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.exception.SirixIOException;
import io.sirix.settings.StringCompressionType;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class ResourceSettingsExtensionsTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void unknownTailAndNestedFieldsNameTheField() throws Exception {
    final Path resource = resource();
    final JsonObject settings = read(resource);
    settings.addProperty("future-format", true);
    write(resource, settings);
    assertTrue(
        assertThrows(SirixIOException.class, () -> ResourceConfiguration.deserialize(resource)).getMessage()
                                                                                               .contains(
                                                                                                   "future-format"));
    settings.remove("future-format");
    final JsonObject validTime = new JsonObject();
    validTime.addProperty("future-valid-time", true);
    settings.add("validTimeConfig", validTime);
    write(resource, settings);
    assertTrue(assertThrows(SirixIOException.class,
        () -> ResourceConfiguration.deserialize(resource)).getMessage().contains("future-valid-time"));
    settings.remove("validTimeConfig");
    settings.getAsJsonObject("revisioning").addProperty("future-versioning", true);
    write(resource, settings);
    assertTrue(assertThrows(SirixIOException.class,
        () -> ResourceConfiguration.deserialize(resource)).getMessage().contains("future-versioning"));
  }

  @Test
  public void optionalFieldsMayBeOmittedAndExtensionsSkipNestedValues() throws Exception {
    final Path resource = resource();
    final JsonObject settings = read(resource);
    settings.remove("stringCompressionType");
    settings.remove("deweyIdSiblingDistance");
    settings.remove("verifyChecksumsOnRead");
    settings.remove("hashAlgorithm");
    settings.add("extensions", JsonParser.parseString("{\"future\":[1,{\"nested\":true},null]}"));
    write(resource, settings);
    assertFalse(ResourceConfiguration.deserialize(resource).withPathStatistics);
    settings.addProperty("extensions", true);
    write(resource, settings);
    assertTrue(
        assertThrows(SirixIOException.class, () -> ResourceConfiguration.deserialize(resource)).getMessage()
                                                                                               .contains("extensions"));
  }

  @Test
  public void everyOptionalFieldOmissionKeepsLaterValuesAligned() throws Exception {
    final Path resource = resource();
    final JsonObject original = read(resource);
    final String[] fields =
        {"stringCompressionType", "deweyIdSiblingDistance", "verifyChecksumsOnRead", "hashAlgorithm"};
    for (int mask = 0; mask < 16; mask++) {
      final JsonObject settings = original.deepCopy();
      settings.addProperty(fields[0], "FSST");
      settings.addProperty(fields[1], 32);
      settings.addProperty(fields[2], false);
      settings.addProperty("pathStatistics", true);
      for (int bit = 0; bit < fields.length; bit++) {
        if ((mask & (1 << bit)) != 0) {
          settings.remove(fields[bit]);
        }
      }
      settings.add("extensions", JsonParser.parseString("{\"unknown\":{\"required-looking-name\":[null,false,3]}}"));
      write(resource, settings);
      final ResourceConfiguration config = ResourceConfiguration.deserialize(resource);
      assertEquals((mask & 1) == 0
          ? StringCompressionType.FSST
          : StringCompressionType.NONE, config.stringCompressionType);
      assertEquals((mask & 2) == 0
          ? 32
          : 16, config.deweyIdSiblingDistance);
      assertEquals((mask & 4) != 0, config.verifyChecksumsOnRead);
      assertTrue(config.withPathStatistics);
    }
  }

  private Path resource() throws Exception {
    final Path databasePath = temporary.getRoot().toPath().resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").build());
    }
    return databasePath.resolve("resources/resource");
  }

  private static JsonObject read(final Path resource) throws Exception {
    return JsonParser.parseString(Files.readString(resource.resolve("ressetting.obj"))).getAsJsonObject();
  }

  private static void write(final Path resource, final JsonObject settings) throws Exception {
    Files.writeString(resource.resolve("ressetting.obj"), settings.toString());
  }
}

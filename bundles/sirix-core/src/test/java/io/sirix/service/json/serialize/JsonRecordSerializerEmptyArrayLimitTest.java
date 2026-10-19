package io.sirix.service.json.serialize;

import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class JsonRecordSerializerEmptyArrayLimitTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void metadataRecordLimitEndingOnEmptyArrayProducesValidJson(final VersioningType versioning) {
    final Path databasePath = directory.resolve("counts");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("stored").storageType(StorageType.FILE_CHANNEL)
          .versioningApproach(versioning).storeChildCount(true).build());
      try (var session = database.beginResourceSession("stored")) {
        try (var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
              "{\"array\":[0,1,2],\"empty\":[],\"object\":{\"x\":1}}"), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final StringWriter output = new StringWriter();
        JsonRecordSerializer.newBuilder(session, 2, output).withNodeKeyAndChildCountMetaData(true).build().call();
        final var json = assertDoesNotThrow(() -> JsonParser.parseString(output.toString()), output::toString);
        final var records = json.getAsJsonObject().getAsJsonArray("value");
        assertEquals(2, records.size());
        assertEquals("array", records.get(0).getAsJsonObject().get("key").getAsString());
        assertEquals("empty", records.get(1).getAsJsonObject().get("key").getAsString());
        assertEquals(0, records.get(1).getAsJsonObject().getAsJsonArray("value").size());
      }
    }
  }
}

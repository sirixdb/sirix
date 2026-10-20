package io.sirix.service.json.serialize;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import io.sirix.utils.ChildCounts;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class UnstoredChildCountSerializationTest {
  private static final String JSON = "{\"array\":[0,1,2],\"empty\":[],\"object\":{\"x\":1}}";

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void serializersAndExactCountsAgreeAfterReopen(final VersioningType versioning) {
    final Path databasePath = directory.resolve("counts");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final boolean stored : new boolean[] {true, false}) {
        final String resource = stored
            ? "stored"
            : "omitted";
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(versioning)
                                                     .hashKind(HashType.NONE)
                                                     .storeChildCount(stored)
                                                     .build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(JSON), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
      }
    }
    try (var database = Databases.openJsonDatabase(databasePath);
        var stored = database.beginResourceSession("stored");
        var omitted = database.beginResourceSession("omitted")) {
      assertEquals(JsonParser.parseString(JSON), serialize(omitted, 0));
      for (int mode = 0; mode < 4; mode++) {
        assertEquals(serialize(stored, mode), serialize(omitted, mode), "serialization mode " + mode);
      }
      try (var reader = omitted.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        final long objectKey = reader.getNodeKey();
        assertEquals(3, ChildCounts.get(reader));
        assertEquals(objectKey, reader.getNodeKey());
        assertTrue(reader.moveToFirstChild());
        // The field can be a fused named array or a legacy object key.
        if (!reader.isArray()) {
          assertTrue(reader.moveToFirstChild());
        }
        final long arrayKey = reader.getNodeKey();
        assertEquals(3, ChildCounts.get(reader));
        assertEquals(arrayKey, reader.getNodeKey());
      }
    }
  }

  private static JsonElement serialize(final JsonResourceSession session, final int mode) {
    final var output = new StringWriter();
    switch (mode) {
      case 0 -> JsonSerializer.newBuilder(session, output).build().call();
      case 1 -> JsonSerializer.newBuilder(session, output).withNodeKeyAndChildCountMetaData(true).build().call();
      case 2 -> JsonLimitedSerializer.newBuilder(session, output)
                                     .maxChildren(2)
                                     .withNodeKeyAndChildCountMetaData(true)
                                     .build()
                                     .call();
      case 3 ->
        JsonRecordSerializer.newBuilder(session, 2, output).withNodeKeyAndChildCountMetaData(true).build().call();
      default -> throw new IllegalArgumentException("Unknown mode: " + mode);
    }
    return assertDoesNotThrow(() -> JsonParser.parseString(output.toString()), () -> "mode " + mode + ": " + output);
  }
}

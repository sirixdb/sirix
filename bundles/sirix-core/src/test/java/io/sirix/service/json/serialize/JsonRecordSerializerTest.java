package io.sirix.service.json.serialize;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class JsonRecordSerializerTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json", "jsonRecordSerializer");

  /**
   * Reflects the {@code sirix.json.fuseNamedPrimitives} system property at class-load
   * time. When fusion is on, the shredder collapses each object primitive field into a
   * single {@code OBJECT_NAMED_*} record, so persisted descendantCount and nodeKey
   * values shift. We pair every metadata-sensitive fixture with a {@code *-fused.json}
   * sibling and pick between them via {@link #expectedFor(String)}.
   */
  private static final boolean FUSE_NAMED_PRIMITIVES =
      true;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Resolves the expected-JSON fixture for the current fusion mode: returns the
   * {@code *-fused.json} sibling of {@code baseFileName} when fusion is enabled and the
   * fused sibling exists on disk, otherwise the legacy file.
   */
  private static Path expectedFor(final String baseFileName) {
    if (FUSE_NAMED_PRIMITIVES) {
      final int dot = baseFileName.lastIndexOf('.');
      final String fusedName = baseFileName.substring(0, dot) + "-fused" + baseFileName.substring(dot);
      final Path fusedPath = JSON.resolve(fusedName);
      if (Files.exists(fusedPath)) {
        return fusedPath;
      }
    }
    return JSON.resolve(baseFileName);
  }

  @Test
  public void serializeArray() {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [{},"bla",{"foo":"bar"},null,[]]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer = new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).build();
        jsonRecordSerializer.call();

        final var expected = """
               [{},"bla",{"foo":"bar"}]
            """.strip();

        assertEquals(expected, stringWriter.toString());
      }
    }
  }

  @Test
  public void serializeObject() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer = new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).build();
      jsonRecordSerializer.call();

      final var expected = """
              {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello"}
          """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }

  @Test
  public void serializeObjectWithPagination() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      // iter#32 fusion: "bar" OBJECT_KEY moved from nodeKey=7 -> 6 because foo's primitive
      // children ("bar","null",2.33 ARRAY-value triple) collapse the OBJECT_KEY level above
      // each numeric-array element. Pagination returns its right siblings with parent wrapper.
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).startNodeKey(6).build();
      jsonRecordSerializer.call();

      // Pagination mode returns parent wrapper with array of siblings after "bar": "baz" and "tada"
      final var expected = """
              {"value":[{"baz":"hello"},{"tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}]}
          """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }

  @Test
  public void serializePaginationWithNoRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      // iter#32 fusion: "tada" OBJECT_KEY (last child of root) moved from key 15 -> 10
      // because each primitive-valued field collapses one OBJECT_KEY level.
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).startNodeKey(10).build();
      jsonRecordSerializer.call();

      // Pagination mode with no siblings returns parent wrapper with empty array
      final var expected = "{\"value\":[]}";

      assertEquals(expected, stringWriter.toString());
    }
  }

  @Test
  public void serializeObjectWithMaxLevel() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer = new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(1).build();
      jsonRecordSerializer.call();

      final var expected = """
              {"foo":[],"bar":{},"baz":"hello"}
          """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }

  @Test
  public void serializeObjectWithMaxLevelAndMetaData() throws IOException {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(1).withMetaData(true).build();
      jsonRecordSerializer.call();

      final var expected = Files.readString(expectedFor("serializeObjectWithMaxLevelAndMetaData.json"));

      assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaData1() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(
          ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [{},"bla",{"foo":{"bar": "baz"}},null,[]]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer =
            new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(1).withMetaData(true).build();
        jsonRecordSerializer.call();

        final var expected = Files.readString(expectedFor("serializeArrayWithMaxLevelAndMetaData1.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaData2() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(
          ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [[],"foo",null,[],{}]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer =
            new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(1).withMetaData(true).build();
        jsonRecordSerializer.call();

        final var expected = Files.readString(JSON.resolve("serializeArrayWithMaxLevelAndMetaData2.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaData3() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(
          ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [{},"bla",{"foo":{"bar": "baz"}},null,[]]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer =
            new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(2).withMetaData(true).build();
        jsonRecordSerializer.call();

        final var expected = Files.readString(expectedFor("serializeArrayWithMaxLevelAndMetaData3.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaDataAndLastTopLevelNode() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(
          ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [{},"bla",{"foo":{"bar": "baz"}},null,[]]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer = new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).startNodeKey(4)
                                                                                                  .maxLevel(1)
                                                                                                  .withMetaData(true)
                                                                                                  .build();
        jsonRecordSerializer.call();

        final var expected =
            Files.readString(expectedFor("serializeArrayWithMaxLevelAndMetaDataAndLastTopLevelNode.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializePaginationWithNoRightSiblingAndMetaData() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      // iter#32 fusion: "tada" OBJECT_KEY (last child of root) moved from key 15 -> key 10
      // because the four primitive-valued fields (foo[], bar{}, baz, plus the array's
      // elements) collapse the OBJECT_KEY level above each.
      final var jsonRecordSerializer = new JsonRecordSerializer.Builder(resmgr, 3, stringWriter)
                                                                                                .startNodeKey(10)
                                                                                                .maxLevel(2)
                                                                                                .withNodeKeyAndChildCountMetaData(
                                                                                                    true)
                                                                                                .build();
      jsonRecordSerializer.call();

      // Pagination mode with no siblings returns parent metadata wrapper with empty array
      // Parent is root object (nodeKey=1) with 4 children
      final var result = stringWriter.toString();
      assertTrue(result.startsWith("{\"metadata\":{\"nodeKey\":1"));
      assertTrue(result.contains("\"childCount\":4"));
      assertTrue(result.endsWith(",\"value\":[]}"));
    }
  }
}

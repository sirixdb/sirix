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

public final class JsonRecordSerializerTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json", "jsonRecordSerializer");

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
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
  public void serializeObjectWithLastTopLevelNodeKey() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).lastTopLevelNodeKey(7).build();
      jsonRecordSerializer.call();

      final var expected = """
              {"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
          """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }

  @Test
  public void serializeObjectWithLastTopLevelNodeKeyAndNoRightSibling() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).lastTopLevelNodeKey(15).build();
      jsonRecordSerializer.call();

      final var expected = """
              {}
          """.strip();

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

      final var expected = Files.readString(JSON.resolve("serializeObjectWithMaxLevelAndMetaData.json"));

      assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaData1() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

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

        final var expected = Files.readString(JSON.resolve("serializeArrayWithMaxLevelAndMetaData1.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaData2() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

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
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

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

        final var expected = Files.readString(JSON.resolve("serializeArrayWithMaxLevelAndMetaData3.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }

  @Test
  public void serializeArrayWithMaxLevelAndMetaDataAndLastTopLevelNode() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).hashKind(HashType.ROLLING).build());

      try (final var resmgr = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = resmgr.beginNodeTrx()) {
        final var json = """
                [{},"bla",{"foo":{"bar": "baz"}},null,[]]
            """.strip();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));

        final var stringWriter = new StringWriter();
        final var jsonRecordSerializer =
            new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).lastTopLevelNodeKey(4).maxLevel(1).withMetaData(true).build();
        jsonRecordSerializer.call();

        final var expected = Files.readString(JSON.resolve("serializeArrayWithMaxLevelAndMetaDataAndLastTopLevelNode.json"));

        assertEquals(expected, stringWriter.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000"));
      }
    }
  }
}
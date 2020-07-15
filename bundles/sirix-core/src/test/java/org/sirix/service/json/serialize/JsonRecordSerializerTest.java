package org.sirix.service.json.serialize;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.Databases;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class JsonRecordSerializerTest {

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void serializeObject() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var resmgr = database.openResourceManager(JsonTestHelper.RESOURCE)) {
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
  public void serializeObjectWithMaxLevel() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var resmgr = database.openResourceManager(JsonTestHelper.RESOURCE)) {
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
  public void serializeObjectWithMaxLevelAndMetaData() {
    JsonTestHelper.createTestDocument();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var resmgr = database.openResourceManager(JsonTestHelper.RESOURCE)) {
      final var stringWriter = new StringWriter();
      final var jsonRecordSerializer =
          new JsonRecordSerializer.Builder(resmgr, 3, stringWriter).maxLevel(1).withMetaData(true).build();
      jsonRecordSerializer.call();

      final var expected = """
              {"metadata":{"nodeKey":1,"hash":"afc35014ae4a659c92ed4a99d1f35adb","type":"OBJECT","descendantCount":24,"childCount":4},"value":[{"key":"foo","metadata":{"nodeKey":2,"hash":"f63408ac19f84f635e2140a2a38dcaa2","type":"OBJECT_KEY","descendantCount":4},"value":{"metadata":{"nodeKey":3,"hash":"a709e167ce057fa71d7265aa0b0442fb","type":"ARRAY","descendantCount":3,"childCount":3},"value":[]},"key":"bar","metadata":{"nodeKey":7,"hash":"1e18013511712fbf0c45d36e766b12b4","type":"OBJECT_KEY","descendantCount":5},"value":{"metadata":{"nodeKey":8,"hash":"377c094c796c9cfd92e827ea27036683","type":"OBJECT","descendantCount":4,"childCount":2},"value":{}},"key":"baz","metadata":{"nodeKey":13,"hash":"b4dc5154b109389b469d2c1ace9337eb","type":"OBJECT_KEY","descendantCount":1},"value":{"metadata":{"nodeKey":14,"hash":"a5e5a7e7781375a358b2e752310ec785","type":"OBJECT_STRING_VALUE"},"value":"hello"}}]}
          """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }
}
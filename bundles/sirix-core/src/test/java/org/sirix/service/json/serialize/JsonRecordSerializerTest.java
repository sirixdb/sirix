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
         final var resmgr = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var rtx = resmgr.beginNodeReadOnlyTrx()) {
      final var stringWriter = new StringWriter();
      JsonRecordSerializer.serialize(rtx, 3, stringWriter);

      final var expected = """
          {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello"}
      """.strip();

      assertEquals(expected, stringWriter.toString());
    }
  }
}
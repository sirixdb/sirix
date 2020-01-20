package org.sirix.service.json.serializer;

import org.junit.Test;
import org.sirix.service.json.serialize.StringValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public final class StringEscaperTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Test
  public void test() throws IOException {
    final var escaped = StringValue.escape(Files.readString(JSON.resolve("strange-value.json")));

    System.out.println(Files.readString(JSON.resolve("strange-value.json")));
    System.out.println(escaped);

    assertEquals("\\\" bla \\n\\r", escaped);
  }
}

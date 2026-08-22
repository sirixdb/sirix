package io.sirix.query.bench.clickbench;

import com.fasterxml.jackson.core.JsonToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClickBenchSourceTest {

  @TempDir
  private Path temporaryDirectory;

  @Test
  void jacksonSourceDetectsArrayLdjsonGzipAndBomWithoutConsumingInput() throws IOException {
    final Path array = temporaryDirectory.resolve("array.json");
    Files.writeString(array, "  [{\"id\":1}]", StandardCharsets.UTF_8);
    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(array.toString())) {
      assertFalse(source.ldjson());
      assertEquals(JsonToken.START_ARRAY, source.parser().nextToken());
    }

    final Path ldjson = temporaryDirectory.resolve("hits.json.gz");
    try (OutputStream output = new GZIPOutputStream(Files.newOutputStream(ldjson))) {
      output.write("{\"id\":1}\n{\"id\":2}\n".getBytes(StandardCharsets.UTF_8));
    }
    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(ldjson.toString())) {
      assertTrue(source.ldjson());
      assertEquals(JsonToken.START_OBJECT, source.parser().nextToken());
    }

    final Path bomArray = temporaryDirectory.resolve("bom-array.json");
    final byte[] body = "[0]".getBytes(StandardCharsets.UTF_8);
    final byte[] bytes = new byte[3 + body.length];
    bytes[0] = (byte) 0xEF;
    bytes[1] = (byte) 0xBB;
    bytes[2] = (byte) 0xBF;
    System.arraycopy(body, 0, bytes, 3, body.length);
    Files.write(bomArray, bytes);
    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(bomArray.toString())) {
      assertFalse(source.ldjson());
      assertEquals(JsonToken.START_ARRAY, source.parser().nextToken());
    }
  }

  @Test
  void generatedSourceUsesArrayFraming() throws IOException {
    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson("generate:2:7")) {
      assertFalse(source.ldjson());
      assertEquals(JsonToken.START_ARRAY, source.parser().nextToken());
    }
  }

  @Test
  void arrayDetectionConsumesUnboundedLeadingWhitespace() throws IOException {
    final String body = " ".repeat(8_192) + "[{\"id\":1}]";
    final Path array = temporaryDirectory.resolve("long-leading-whitespace.json");
    Files.writeString(array, body, StandardCharsets.UTF_8);

    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(array.toString())) {
      assertFalse(source.ldjson());
      assertEquals(JsonToken.START_ARRAY, source.parser().nextToken());
    }
    try (Reader source = ClickBenchSource.open(array.toString())) {
      assertEquals("[{\"id\":1}]", sourceToString(source));
    }
  }

  @Test
  void legacyLdjsonAdapterSkipsWhitespaceOnlyLines() throws IOException {
    final Path ldjson = temporaryDirectory.resolve("whitespace-lines.jsonl");
    Files.writeString(ldjson, "{\"id\":1}\n \t \n\t{\"id\":2}\n", StandardCharsets.UTF_8);

    try (Reader source = ClickBenchSource.open(ldjson.toString())) {
      assertEquals("[{\"id\":1},{\"id\":2}]", sourceToString(source));
    }
  }

  private static String sourceToString(final Reader source) throws IOException {
    final StringBuilder value = new StringBuilder();
    final char[] buffer = new char[256];
    int read;
    while ((read = source.read(buffer)) != -1) {
      value.append(buffer, 0, read);
    }
    return value.toString();
  }
}

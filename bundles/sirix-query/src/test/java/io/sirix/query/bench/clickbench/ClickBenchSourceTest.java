package io.sirix.query.bench.clickbench;

import com.fasterxml.jackson.core.JsonToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @Test
  void officialLdjsonSchemaIsNormalizedBeforeIngestion() throws IOException {
    String record;
    try (Reader generated = ClickBenchSource.open("generate:1:7")) {
      final String array = sourceToString(generated).trim();
      record = array.substring(1, array.length() - 1);
    }
    for (final String column : ClickBenchSchema.COLUMNS) {
      if (ClickBenchSchema.typeOf(column) == ClickBenchSchema.ColumnType.LONG) {
        record = Pattern.compile("(\\\"" + column + "\\\":)(-?[0-9]+)").matcher(record).replaceFirst("$1\\\"$2\\\"");
      } else if (ClickBenchSchema.typeOf(column) == ClickBenchSchema.ColumnType.DATETIME) {
        record = Pattern.compile("(\\\"" + column + "\\\":\\\"[0-9]{4}-[0-9]{2}-[0-9]{2})T")
                        .matcher(record)
                        .replaceFirst("$1 ");
      }
    }

    final Path officialStyle = temporaryDirectory.resolve("official-hits.json.gz");
    try (OutputStream output = new GZIPOutputStream(Files.newOutputStream(officialStyle))) {
      output.write(record.getBytes(StandardCharsets.UTF_8));
      output.write('\n');
    }

    final ClickBenchSource.SourceValidation validation =
        ClickBenchSource.validateFirstRecord(officialStyle.toString(), true);
    assertEquals(ClickBenchSchema.COLUMNS.size(), validation.columns());
    assertEquals(6L, validation.normalizedLongValues());
    assertEquals(3L, validation.normalizedTimestamps());
    assertThrows(IOException.class, () -> ClickBenchSource.validateFirstRecord(officialStyle.toString(), false));

    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(officialStyle.toString(), true)) {
      final var parser = source.parser();
      assertEquals(JsonToken.START_OBJECT, parser.nextToken());
      boolean sawUserId = false;
      boolean sawEventTime = false;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String name = parser.currentName();
        final JsonToken value = parser.nextToken();
        if ("UserID".equals(name)) {
          assertEquals(JsonToken.VALUE_NUMBER_INT, value);
          parser.getLongValue(); // exact signed-long access must be available to the shredder
          sawUserId = true;
        } else if ("EventTime".equals(name)) {
          assertEquals(JsonToken.VALUE_STRING, value);
          assertEquals('T', parser.getTextCharacters()[parser.getTextOffset() + 10]);
          sawEventTime = true;
        }
      }
      assertTrue(sawUserId);
      assertTrue(sawEventTime);
    }
  }

  @Test
  void parallelInputNormalizesEveryRecordWithoutMaterializingAnotherFile() throws IOException {
    String record;
    try (Reader generated = ClickBenchSource.open("generate:1:7")) {
      final String array = sourceToString(generated).trim();
      record = array.substring(1, array.length() - 1);
    }
    for (final String column : ClickBenchSchema.COLUMNS) {
      if (ClickBenchSchema.typeOf(column) == ClickBenchSchema.ColumnType.LONG) {
        record = Pattern.compile("(\\\"" + column + "\\\":)(-?[0-9]+)").matcher(record).replaceFirst("$1\\\"$2\\\"");
      } else if (ClickBenchSchema.typeOf(column) == ClickBenchSchema.ColumnType.DATETIME) {
        record = Pattern.compile("(\\\"" + column + "\\\":\\\"[0-9]{4}-[0-9]{2}-[0-9]{2})T")
                        .matcher(record)
                        .replaceFirst("$1 ");
      }
    }
    final Path sourcePath = temporaryDirectory.resolve("parallel-official.jsonl");
    Files.writeString(sourcePath, record + '\n' + record + '\n', StandardCharsets.UTF_8);

    try (InputStream source = ClickBenchSource.openParallelInput(sourcePath.toString(), false)) {
      final String framed = new String(source.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(framed.endsWith(",]"), framed.substring(Math.max(0, framed.length() - 40)));
    }

    final String normalized;
    try (InputStream source = ClickBenchSource.openParallelInput(sourcePath.toString(), true)) {
      normalized = new String(source.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertTrue(normalized.startsWith("[{") && normalized.endsWith("]"));
    assertFalse(normalized.contains("\"WatchID\":\""));
    assertFalse(normalized.contains("\"UserID\":\""));
    assertTrue(Pattern.compile("\\\"WatchID\\\":-?[0-9]+").matcher(normalized).find());
    assertFalse(Pattern.compile("\\\"EventTime\\\":\\\"[0-9-]{10} ").matcher(normalized).find());
    assertEquals(2, Pattern.compile("\\\"EventTime\\\":\\\"[0-9-]{10}T").split(normalized, -1).length - 1);
  }

  @Test
  void normalizationLeavesAlreadyNumericLongTokensExact() throws IOException {
    try (ClickBenchSource.JacksonSource raw = ClickBenchSource.openJackson("generate:3:17");
        ClickBenchSource.JacksonSource normalized = ClickBenchSource.openJackson("generate:3:17", true)) {
      JsonToken token;
      while ((token = raw.parser().nextToken()) != null) {
        assertEquals(token, normalized.parser().nextToken());
        if (token == JsonToken.VALUE_NUMBER_INT) {
          final String field = raw.parser().currentName();
          if (field != null && ClickBenchSchema.typeOf(field) == ClickBenchSchema.ColumnType.LONG) {
            assertEquals(raw.parser().getLongValue(), normalized.parser().getLongValue(), field);
          }
        }
      }
    }
  }

  @Test
  void parallelNormalizerRetriesZeroLengthSourceReadsWithoutGrowingTheStack() throws IOException {
    final byte[] expected;
    try (Reader generated = ClickBenchSource.open("generate:1:31")) {
      expected = sourceToString(generated).getBytes(StandardCharsets.UTF_8);
    }
    final ByteArrayInputStream delegate = new ByteArrayInputStream(expected);
    final InputStream stuttering = new InputStream() {
      private int emptyReads = 4_096;

      @Override
      public int read() {
        return delegate.read();
      }

      @Override
      public int read(final byte[] bytes, final int offset, final int length) {
        if (emptyReads-- > 0) {
          return 0;
        }
        return delegate.read(bytes, offset, length);
      }
    };

    try (InputStream normalized = new ClickBenchSchemaInputStream(stuttering)) {
      assertEquals(new String(expected, StandardCharsets.UTF_8),
          new String(normalized.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void normalizationRejectsATypeViolationAfterThePreflightRecord() throws IOException {
    final String json;
    try (Reader generated = ClickBenchSource.open("generate:2:17")) {
      json = sourceToString(generated);
    }
    final String marker = "\"CounterID\":";
    final int first = json.indexOf(marker);
    final int second = json.indexOf(marker, first + marker.length());
    final int valueStart = second + marker.length();
    final int valueEnd = json.indexOf(',', valueStart);
    final String malformed =
        json.substring(0, valueStart) + '"' + json.substring(valueStart, valueEnd) + '"' + json.substring(valueEnd);
    final Path sourcePath = temporaryDirectory.resolve("late-type-violation.json");
    Files.writeString(sourcePath, malformed, StandardCharsets.UTF_8);

    try (ClickBenchSource.JacksonSource source = ClickBenchSource.openJackson(sourcePath.toString(), true)) {
      assertThrows(IOException.class, () -> {
        while (source.parser().nextToken() != null) {
          // Consume the complete parser stream; the malformed value is in record two.
        }
      });
    }
    try (InputStream source = ClickBenchSource.openParallelInput(sourcePath.toString(), true)) {
      assertThrows(IOException.class, source::readAllBytes);
    }
  }

  @Test
  void preflightRejectsInvalidCalendarAndClockValues() throws IOException {
    final String json;
    try (Reader generated = ClickBenchSource.open("generate:1:23")) {
      json = sourceToString(generated);
    }
    final Path invalidDate = temporaryDirectory.resolve("invalid-date.json");
    Files.writeString(invalidDate, replaceStringField(json, "EventDate", "2013-02-30"), StandardCharsets.UTF_8);
    assertThrows(IOException.class, () -> ClickBenchSource.validateFirstRecord(invalidDate.toString(), true));

    final Path invalidTime = temporaryDirectory.resolve("invalid-time.json");
    Files.writeString(invalidTime, replaceStringField(json, "EventTime", "2013-07-15T99:00:00"),
        StandardCharsets.UTF_8);
    assertThrows(IOException.class, () -> ClickBenchSource.validateFirstRecord(invalidTime.toString(), true));
  }

  private static String replaceStringField(final String json, final String field, final String replacement) {
    final String marker = '"' + field + "\":\"";
    final int valueStart = json.indexOf(marker) + marker.length();
    final int valueEnd = json.indexOf('"', valueStart);
    return json.substring(0, valueStart) + replacement + json.substring(valueEnd);
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

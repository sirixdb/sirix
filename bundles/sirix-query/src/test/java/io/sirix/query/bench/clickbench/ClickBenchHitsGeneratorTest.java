package io.sirix.query.bench.clickbench;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.sirix.query.bench.clickbench.ClickBenchSchema.ColumnType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the properties the ClickBench port depends on: the generated stream is JSON in the exact
 * shape {@link ClickBenchSchema} declares, it is reproducible and shardable, the literals
 * Q19/Q40/Q41 select actually occur, and 64-bit ids survive the round trip through JSON text as
 * exact digits.
 */
final class ClickBenchHitsGeneratorTest {

  private static final long SEED = 20130701L;

  /** Big enough for the rarest planted literal (1 in 5000) to show up many times over. */
  private static final int PLANTED_LITERAL_ROWS = 200_000;

  /** Chunk size for the 200k-row scan, so no single generated string gets huge. */
  private static final int PLANTED_LITERAL_CHUNK = 10_000;

  private static final Pattern INTEGER_TOKEN = Pattern.compile("-?\\d+");
  private static final Pattern ISO_DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
  private static final Pattern ISO_DATE_TIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}");

  @Test
  @DisplayName("every record parses as JSON and carries all 105 columns in schema order")
  void everyRecordCarriesAllColumnsInSchemaOrder() {
    final JsonArray records = parse(generate(0, 2_000, SEED));

    assertEquals(2_000, records.size(), "record count");
    final List<String> expected = ClickBenchSchema.COLUMNS;
    assertEquals(105, expected.size(), "the schema itself must hold 105 columns");
    for (int i = 0; i < records.size(); i++) {
      final JsonObject record = records.get(i).getAsJsonObject();
      assertEquals(expected.size(), record.size(), "column count of record " + i);
      assertEquals(expected, new ArrayList<>(record.keySet()), "column names/order of record " + i);
    }
  }

  @Test
  @DisplayName("each column's JSON value has the type the schema declares")
  void columnValuesHaveTheDeclaredType() {
    final JsonArray records = parse(generate(0, 500, SEED));

    for (int i = 0; i < records.size(); i++) {
      final JsonObject record = records.get(i).getAsJsonObject();
      for (final String column : ClickBenchSchema.COLUMNS) {
        final JsonElement element = record.get(column);
        assertTrue(element.isJsonPrimitive(), column + " must be a primitive in record " + i);
        final JsonPrimitive value = element.getAsJsonPrimitive();
        final String where = column + " in record " + i;
        switch (ClickBenchSchema.typeOf(column)) {
          case INT -> {
            assertTrue(value.isNumber(), where + " must be a JSON number");
            final String text = value.getAsString();
            assertTrue(INTEGER_TOKEN.matcher(text).matches(), where + " must be an integer: " + text);
            assertEquals(text, Integer.toString(value.getAsInt()), where + " must fit int32");
          }
          case LONG -> {
            assertTrue(value.isNumber(), where + " must be a JSON number");
            final String text = value.getAsString();
            assertTrue(INTEGER_TOKEN.matcher(text).matches(), where + " must be an integer: " + text);
            assertEquals(text, Long.toString(value.getAsLong()), where + " must round-trip as int64");
          }
          case STRING -> assertTrue(value.isString(), where + " must be a JSON string");
          case DATE -> {
            assertTrue(value.isString(), where + " must be a JSON string");
            assertTrue(ISO_DATE.matcher(value.getAsString()).matches(),
                where + " must be YYYY-MM-DD: " + value.getAsString());
          }
          case DATETIME -> {
            assertTrue(value.isString(), where + " must be a JSON string");
            assertTrue(ISO_DATE_TIME.matcher(value.getAsString()).matches(),
                where + " must be YYYY-MM-DDTHH:MM:SS: " + value.getAsString());
          }
        }
      }
    }
  }

  @Test
  @DisplayName("the same (firstRow, rowCount, seed) yields byte-identical output")
  void generationIsDeterministic() {
    assertEquals(generate(0, 1_000, SEED), generate(0, 1_000, SEED));
    assertEquals(generate(7_777, 333, SEED), generate(7_777, 333, SEED));
    assertNotEquals(generate(0, 1_000, SEED), generate(0, 1_000, SEED + 1), "the seed must matter");
    assertNotEquals(generate(0, 10, SEED), generate(10, 10, SEED), "the row index must matter");
  }

  @Test
  @DisplayName("rows [0,n) equal rows [0,k) followed by rows [k,n) — a sharded ingest sees one dataset")
  void splitGenerationEqualsWholeGeneration() {
    final int n = 5_000;
    final int k = 1_777;

    final String whole = body(generate(0, n, SEED));
    final String head = body(generate(0, k, SEED));
    final String tail = body(generate(k, n - k, SEED));

    assertEquals(whole, head + ",\n" + tail, "split generation must reproduce the whole stream");
  }

  @Test
  @DisplayName("the literals Q19/Q40/Q41 select occur in 200k rows")
  void plantedLiteralsOccur() {
    final String userId = "\"UserID\":" + ClickBenchHitsGenerator.PLANTED_USER_ID;
    final String refererHash = "\"RefererHash\":" + ClickBenchHitsGenerator.PLANTED_REFERER_HASH;
    final String urlHash = "\"URLHash\":" + ClickBenchHitsGenerator.PLANTED_URL_HASH;
    int userIdHits = 0;
    int refererHashHits = 0;
    int urlHashHits = 0;

    for (long firstRow = 0; firstRow < PLANTED_LITERAL_ROWS; firstRow += PLANTED_LITERAL_CHUNK) {
      final String chunk = generate(firstRow, PLANTED_LITERAL_CHUNK, SEED);
      userIdHits += count(chunk, userId);
      refererHashHits += count(chunk, refererHash);
      urlHashHits += count(chunk, urlHash);
    }

    assertTrue(userIdHits > 0, "Q19's UserID literal never occurred in " + PLANTED_LITERAL_ROWS + " rows");
    assertTrue(refererHashHits > 0, "Q40's RefererHash literal never occurred");
    assertTrue(urlHashHits > 0, "Q41's URLHash literal never occurred");
    // The rates are approximate, but an order of magnitude off means the planting logic broke.
    assertTrue(userIdHits >= 10, "expected ~40 planted UserIDs, got " + userIdHits);
    assertTrue(refererHashHits >= 50, "expected ~200 planted RefererHashes, got " + refererHashHits);
    assertTrue(urlHashHits >= 100, "expected ~400 planted URLHashes, got " + urlHashHits);
  }

  @Test
  @DisplayName("EventTime, ClientEventTime and LocalEventTime all fall on EventDate")
  void timestampsAgreeWithEventDate() {
    final JsonArray records = parse(generate(0, 5_000, SEED));

    for (int i = 0; i < records.size(); i++) {
      final JsonObject record = records.get(i).getAsJsonObject();
      final String eventDate = record.get("EventDate").getAsString();
      assertTrue(
          eventDate.compareTo(ClickBenchHitsGenerator.FIRST_EVENT_DATE) >= 0
              && eventDate.compareTo(ClickBenchHitsGenerator.LAST_EVENT_DATE) <= 0,
          "EventDate outside the generated month in record " + i + ": " + eventDate);
      for (final String column : List.of("EventTime", "ClientEventTime", "LocalEventTime")) {
        final String timestamp = record.get(column).getAsString();
        assertEquals(eventDate, timestamp.substring(0, 10),
            column + " day differs from EventDate in " + "record " + i + ": " + timestamp);
      }
    }
  }

  @Test
  @DisplayName("64-bit columns keep their exact digits in the JSON text — no doubles, no exponents")
  void sixtyFourBitValuesRoundTripAsDigits() {
    final String text = generate(0, 2_000, SEED);
    final JsonArray records = parse(text);

    assertFalse(text.contains("e+") || text.contains("E+") || text.contains("e-") || text.contains("E-"),
        "the output must not contain scientific notation");
    boolean sawWideValue = false;
    for (int i = 0; i < records.size(); i++) {
      final JsonObject record = records.get(i).getAsJsonObject();
      for (final String column : ClickBenchSchema.COLUMNS) {
        if (ClickBenchSchema.typeOf(column) != ColumnType.LONG) {
          continue;
        }
        final long value = record.get(column).getAsLong();
        final String digits = Long.toString(value);
        assertEquals(digits, record.get(column).getAsString(), column + " lost its exact digits in record " + i);
        assertTrue(text.contains("\"" + column + "\":" + digits),
            column + " digits " + digits + " are not in the emitted text of record " + i);
        sawWideValue |= digits.length() >= 18;
      }
    }
    assertTrue(sawWideValue, "no id wide enough to prove the int64 round trip");
  }

  @Test
  @DisplayName("the reader honours its contract: empty range, single-char reads, close")
  void readerContract() throws IOException {
    assertEquals("[]", generate(0, 0, SEED), "an empty range must still be a JSON array");

    final StringBuilder oneByOne = new StringBuilder();
    try (Reader reader = new ClickBenchHitsGenerator(0, 3, SEED)) {
      int c;
      while ((c = reader.read()) != -1) {
        oneByOne.append((char) c);
      }
      assertEquals(-1, reader.read(), "a drained reader must stay drained");
    }
    assertEquals(generate(0, 3, SEED), oneByOne.toString(), "single-char reads must match bulk reads");

    final Reader closed = new ClickBenchHitsGenerator(0, 3, SEED);
    closed.close();
    assertThrows(IOException.class, closed::read, "a closed reader must fail");

    assertThrows(IllegalArgumentException.class, () -> new ClickBenchHitsGenerator(-1, 1, SEED));
    assertThrows(IllegalArgumentException.class, () -> new ClickBenchHitsGenerator(0, -1, SEED));
    assertThrows(IllegalArgumentException.class, () -> new ClickBenchHitsGenerator(Long.MAX_VALUE, 2, SEED));
  }

  @Test
  @DisplayName("the schema exposes the create.sql types the DuckDB reference needs")
  void schemaExposesDuckdbTypes() {
    assertEquals("WatchID", ClickBenchSchema.COLUMNS.getFirst());
    assertEquals("CLID", ClickBenchSchema.COLUMNS.getLast());
    assertEquals(ColumnType.LONG, ClickBenchSchema.typeOf("UserID"));
    assertEquals(ColumnType.DATE, ClickBenchSchema.typeOf("EventDate"));
    assertEquals(ColumnType.DATETIME, ClickBenchSchema.typeOf("EventTime"));
    assertEquals(ColumnType.STRING, ClickBenchSchema.typeOf("HitColor"));
    assertEquals("VARCHAR(255)", ClickBenchSchema.duckdbType("UserAgentMinor"));
    assertThrows(IllegalArgumentException.class, () -> ClickBenchSchema.typeOf("NoSuchColumn"));
    assertThrows(IllegalArgumentException.class, () -> ClickBenchSchema.duckdbType("NoSuchColumn"));

    final JsonObject spec = JsonParser.parseString(ClickBenchSchema.duckdbColumnSpecJson()).getAsJsonObject();
    assertEquals(ClickBenchSchema.COLUMNS.size(), spec.size(), "the DuckDB spec must cover every column");
    assertEquals(ClickBenchSchema.COLUMNS, new ArrayList<>(spec.keySet()), "spec order must be schema order");
    for (final String column : ClickBenchSchema.COLUMNS) {
      assertEquals(ClickBenchSchema.duckdbType(column), spec.get(column).getAsString(), column);
    }
  }

  private static JsonArray parse(final String text) {
    final JsonElement parsed = JsonParser.parseString(text);
    assertTrue(parsed.isJsonArray(), "the generator must emit one JSON array");
    return parsed.getAsJsonArray();
  }

  /** Reads the whole generated stream; the tests are small enough to hold it in memory. */
  private static String generate(final long firstRow, final long rowCount, final long seed) {
    final StringBuilder text = new StringBuilder(1 << 20);
    final char[] chunk = new char[1 << 16];
    try (Reader reader = new ClickBenchHitsGenerator(firstRow, rowCount, seed)) {
      int read;
      while ((read = reader.read(chunk, 0, chunk.length)) != -1) {
        text.append(chunk, 0, read);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return text.toString();
  }

  /** The records of a generated array, i.e. the text without the framing brackets. */
  private static String body(final String array) {
    assertTrue(array.startsWith("[") && array.endsWith("]"), "not a JSON array: " + array.substring(0, 16));
    return array.substring(1, array.length() - 1);
  }

  private static int count(final String haystack, final String needle) {
    int hits = 0;
    for (int at = haystack.indexOf(needle); at >= 0; at = haystack.indexOf(needle, at + needle.length())) {
      hits++;
    }
    return hits;
  }
}

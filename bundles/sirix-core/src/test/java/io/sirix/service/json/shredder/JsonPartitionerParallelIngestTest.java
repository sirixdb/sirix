/*
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonPartitioner.Format;
import io.sirix.service.json.shredder.JsonPartitioner.Plan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of the composition {@link JsonPartitioner} exists for: plan a split, hand the
 * readers to {@link ParallelJsonShredder}, and get back shards that — read in index order and
 * concatenated — hold exactly the records the input held, in the input's order.
 *
 * <p>{@link JsonPartitionerTest} proves the plan's readers reproduce the record sequence; this proves
 * the records survive the trip through the shredder into real SirixDB resources. Nothing else asserts
 * that the two halves fit together, and a mismatch there would corrupt an ingest rather than fail it.
 *
 * @author Johannes Lichtenberger
 */
final class JsonPartitionerParallelIngestTest {

  private static final Path DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final Function<String, ResourceConfiguration> CONFIG =
      name -> ResourceConfiguration.newBuilder(name).build();

  /** Split even small fixtures: the production floor would collapse them into one partition. */
  private static final long NO_MINIMUM = 1L;

  @TempDir
  private Path tempDir;

  private Database<JsonResourceSession> database;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DB_PATH));
    database = Databases.openJsonDatabase(DB_PATH);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.close();
    }
    JsonTestHelper.deleteEverything();
  }

  @Test
  void aRootArrayIngestsIntoShardsThatConcatenateBackToTheInput() throws IOException {
    assertIngestPreservesRecords(arrayOfObjects(200), 5, Format.ARRAY);
  }

  @Test
  void concatenatedRecordsIngestIntoShardsThatConcatenateBackToTheInput() throws IOException {
    final StringBuilder json = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      json.append("{\"id\":").append(i).append(",\"name\":\"record-").append(i).append("\"}\n");
    }

    assertIngestPreservesRecords(json.toString(), 4, Format.NEWLINE_DELIMITED);
  }

  @Test
  void aWrappedRecordsArrayIngestsIntoShardsThatConcatenateBackToTheInput() throws IOException {
    final String json = "{\"meta\":{\"count\":200},\"data\":{\"children\":" + arrayOfObjects(200) + "}}";

    assertIngestPreservesRecords(json, 6, Format.NESTED_ARRAY);
  }

  /**
   * Records whose values carry separators and escapes — the payload most likely to be mis-cut — must
   * come back byte-identical after a round trip through the shards.
   */
  @Test
  void recordsHoldingSeparatorsAndEscapesSurviveTheIngest() throws IOException {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 120; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(i).append(",\"tricky\":\"a,b],[c{}\\\"d\\\\e\",\"uni\":\"日本語😀\"}");
    }
    json.append(']');

    assertIngestPreservesRecords(json.toString(), 5, Format.ARRAY);
  }

  @Test
  void anUnsplittableDocumentStillIngestsAsOneShard() throws IOException {
    // No array anywhere, so there is no records array to split on.
    final String json = "{\"a\":1,\"b\":{\"c\":2},\"d\":\"e\"}";
    final Path file = write(json);
    final Plan plan = JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, null);

    assertEquals(Format.SINGLE_DOCUMENT, plan.format());

    final List<String> names =
        ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard", CONFIG, 0, 0);

    assertEquals(1, names.size());
    assertEquals(JsonParser.parseString(json), JsonParser.parseString(serialize(names.getFirst())));
  }

  /**
   * Pins the documented — and easy to be surprised by — semantics of {@link Format#NESTED_ARRAY}: the
   * shards hold the <em>records</em>, and the wrapper object's sibling members are not ingested at
   * all. A caller who needs the metadata has to read it separately; this is the same trade DuckDB
   * makes when a {@code json_path} selects the records out of a wrapper.
   */
  @Test
  void aWrappedRecordsArrayIngestsTheRecordsAndNotTheWrappersOtherMembers() throws IOException {
    final String json = "{\"meta\":{\"count\":3,\"source\":\"unit-test\"},\"data\":[{\"a\":1},{\"b\":2},{\"c\":3}]}";
    final Path file = write(json);
    final Plan plan = JsonPartitioner.plan(file, 3, Format.AUTO, NO_MINIMUM, null);

    assertEquals(Format.NESTED_ARRAY, plan.format());
    assertEquals("data", plan.recordsField());

    final List<String> names =
        ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard", CONFIG, 0, 0);

    final List<JsonElement> ingested = new ArrayList<>();
    for (final String name : names) {
      JsonParser.parseString(serialize(name)).getAsJsonArray().forEach(ingested::add);
    }

    assertEquals(JsonParser.parseString("[{\"a\":1},{\"b\":2},{\"c\":3}]").getAsJsonArray().asList(), ingested);
    assertTrue(ingested.stream().noneMatch(e -> e.toString().contains("unit-test")),
        "the wrapper's sibling members are deliberately not ingested");
  }

  // ==================== Helpers ====================

  /**
   * Plan a split, shred it in parallel, and assert the shards concatenate back to the input's record
   * sequence — same records, same order, no losses or duplicates.
   */
  private void assertIngestPreservesRecords(final String json, final int partitions, final Format expectedFormat)
      throws IOException {
    final Path file = write(json);
    final Plan plan = JsonPartitioner.plan(file, partitions, Format.AUTO, NO_MINIMUM, null);

    assertEquals(expectedFormat, plan.format());
    assertTrue(plan.isPartitioned(), "fixture must actually split, otherwise this proves nothing");

    final List<String> names =
        ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard", CONFIG, 0, 0);

    assertEquals(plan.partitions().size(), names.size());
    assertEquals(plan.partitions().size(), database.listResources().size());

    final List<JsonElement> ingested = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      assertEquals("shard-" + i, names.get(i), "partition i must land in resource shard-i");
      final JsonArray shard = JsonParser.parseString(serialize(names.get(i))).getAsJsonArray();
      shard.forEach(ingested::add);
    }

    assertEquals(expectedRecords(json, plan), ingested,
        "the shards, concatenated in index order, must hold exactly the input's records");
    assertEquals(plan.recordCount(), ingested.size());
  }

  /** The record sequence of the input, derived without the partitioner. */
  private static List<JsonElement> expectedRecords(final String json, final Plan plan) {
    final List<JsonElement> expected = new ArrayList<>();
    switch (plan.format()) {
      case ARRAY -> JsonParser.parseString(json).getAsJsonArray().forEach(expected::add);
      case NESTED_ARRAY -> {
        JsonElement cursor = JsonParser.parseString(json);
        for (final String step : plan.recordsField().split("\\.")) {
          cursor = cursor.getAsJsonObject().get(step);
        }
        cursor.getAsJsonArray().forEach(expected::add);
      }
      case NEWLINE_DELIMITED -> {
        for (final String line : json.split("\n")) {
          if (!line.isBlank()) {
            expected.add(JsonParser.parseString(line));
          }
        }
      }
      case SINGLE_DOCUMENT -> expected.add(JsonParser.parseString(json));
      case AUTO -> throw new AssertionError("a plan never resolves to AUTO");
    }
    return expected;
  }

  private String serialize(final String resourceName) {
    try (final JsonResourceSession session = database.beginResourceSession(resourceName)) {
      final StringWriter writer = new StringWriter();
      new JsonSerializer.Builder(session, writer).build().call();
      return writer.toString();
    }
  }

  private Path write(final String json) throws IOException {
    final Path file = Files.createTempFile(tempDir, "ingest", ".json");
    Files.writeString(file, json, StandardCharsets.UTF_8);
    return file;
  }

  private static String arrayOfObjects(final int count) {
    final StringBuilder json = new StringBuilder(count * 48);
    json.append('[');
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(i).append(",\"name\":\"record-").append(i).append("\",\"tags\":[\"a\",\"b\"]}");
    }
    return json.append(']').toString();
  }
}

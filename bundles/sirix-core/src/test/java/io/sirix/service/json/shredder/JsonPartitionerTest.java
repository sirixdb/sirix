/*
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.JsonElement;
import com.google.gson.Strictness;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.sirix.exception.SirixIOException;
import io.sirix.service.json.shredder.JsonPartitioner.Format;
import io.sirix.service.json.shredder.JsonPartitioner.Partition;
import io.sirix.service.json.shredder.JsonPartitioner.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@link JsonPartitioner}: layout detection, record-aligned cutting, and the round-trip
 * property that matters most — <em>reading every partition in index order must reproduce the input's
 * record sequence exactly</em>. A partitioner that loses, duplicates or reorders a record would
 * silently corrupt an ingest, so nearly every case asserts that property rather than the cut offsets.
 *
 * @author Johannes Lichtenberger
 */
final class JsonPartitionerTest {

  /** Split even tiny fixtures: the production floor would collapse them into one partition. */
  private static final long NO_MINIMUM = 1L;

  @TempDir
  private Path tempDir;

  // ==================== Layout detection ====================

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "[{\"a\":1},{\"b\":2}]                     | ARRAY",
      "[1,2,3]                                   | ARRAY",
      "[]                                        | ARRAY",
      "{\"a\":1}{\"b\":2}                        | NEWLINE_DELIMITED",
      "{\"a\":1,\"b\":{\"c\":2}}                 | SINGLE_DOCUMENT",
      "42                                        | SINGLE_DOCUMENT",
      "\"hello\"                                 | SINGLE_DOCUMENT"})
  void detectsLayout(final String json, final Format expected) throws IOException {
    assertEquals(expected, plan(json, 4).format());
  }

  @Test
  void detectsNewlineDelimitedLayout() throws IOException {
    assertEquals(Format.NEWLINE_DELIMITED, plan("{\"a\":1}\n{\"b\":2}\n", 4).format());
    assertEquals(Format.NEWLINE_DELIMITED, plan("[1,2]\n[3,4]", 4).format());
    // Structural detection, not line-based: records may span lines.
    assertEquals(Format.NEWLINE_DELIMITED, plan("{\n \"a\":1\n}\n{\n \"b\":2\n}", 4).format());
  }

  @Test
  void detectsAWrappedRecordsArray() throws IOException {
    assertEquals(Format.NESTED_ARRAY, plan("{\"data\":" + paddedRecords(3) + "}", 4).format());
    assertEquals(Format.NESTED_ARRAY, plan("{\"o\":{\"inner\":" + paddedRecords(3) + "}}", 4).format());
  }

  @Test
  void picksTheLargestArrayAsTheRecordsArray() throws IOException {
    final Plan plan = plan("{\"small\":[1],\"records\":" + paddedRecords(3) + "}", 4);

    assertEquals(Format.NESTED_ARRAY, plan.format());
    assertEquals("records", plan.recordsField());
    assertEquals(3L, plan.recordCount());
  }

  @Test
  void reportsTheDottedPathOfANestedRecordsArray() throws IOException {
    final Plan plan = plan("{\"kind\":\"Listing\",\"data\":{\"children\":" + paddedRecords(2) + "}}", 4);

    assertEquals(Format.NESTED_ARRAY, plan.format());
    assertEquals("data.children", plan.recordsField());
  }

  @Test
  void honoursAnExplicitlyRequestedRecordsPath() throws IOException {
    final Path file = write("{\"small\":[{\"a\":1}],\"big\":[{\"b\":1},{\"c\":2},{\"d\":3}]}");

    final Plan plan = JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, "small");

    assertEquals(Format.NESTED_ARRAY, plan.format());
    assertEquals("small", plan.recordsField());
    assertEquals(1L, plan.recordCount());
  }

  @Test
  void rejectsARequestedRecordsPathThatDoesNotExist() throws IOException {
    final Path file = write("{\"data\":[{\"a\":1},{\"b\":2}]}");

    final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, "absent"));
    assertTrue(failure.getMessage().contains("absent"), failure.getMessage());
  }

  /**
   * Splitting on a wrapped array ingests that array's elements and nothing else, so detection alone
   * must not choose it for a document where the array is incidental — doing so would silently discard
   * the bulk of the file. An explicitly named path still wins: the caller asserted the intent.
   */
  @Test
  void autoRefusesAWrappedArrayThatIsNotEssentiallyTheWholeDocument() throws IOException {
    final String json = "{\"important\":\"" + "M".repeat(4000) + "\",\"tiny\":[1,2,3]}";
    final Path file = write(json);

    final Plan detected = JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, null);
    assertEquals(Format.SINGLE_DOCUMENT, detected.format(),
        "a 0.1%-of-the-file array must not be treated as the records array");

    final Plan requested = JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, "tiny");
    assertEquals(Format.NESTED_ARRAY, requested.format(), "an explicit path bypasses the coverage bound");
    assertEquals(3L, requested.recordCount());
  }

  @Test
  void autoAcceptsAWrappedArrayThatDominatesTheDocument() throws IOException {
    final Plan plan = plan("{\"meta\":{\"n\":3},\"data\":" + paddedRecords(3) + "}", 4);

    assertEquals(Format.NESTED_ARRAY, plan.format());
    assertEquals("data", plan.recordsField());
    assertEquals(3L, plan.recordCount());
  }

  @Test
  void reportsNoRecordsFieldForFlatLayouts() throws IOException {
    assertNull(plan("[{\"a\":1},{\"b\":2}]", 4).recordsField());
    assertNull(plan("{\"a\":1}\n{\"b\":2}", 4).recordsField());
  }

  // ==================== Round-trip ====================

  @ParameterizedTest
  @ValueSource(strings = {
      "[{\"a\":1},{\"b\":[1,2,3]},{\"c\":{\"d\":\"x\"}},{\"e\":null},{\"f\":true}]",
      "[1,2,3,4,5,6,7]",
      "[\"a\",\"b\",\"c\",\"d\"]",
      "[[1,[2,3]],[4],[[5,6],7]]",
      "[\n  {\"a\": 1},\n  {\"b\": 2},\n  {\"c\": 3}\n]",
      "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n",
      "{\"a\":1}{\"b\":2}{\"c\":3}",
      "{\n \"a\":1\n}\n{\n \"b\":2\n}",
      "[1,2]\n[3,4]\n[5,6]",
      "{\"meta\":{\"n\":3},\"data\":[{\"a\":1},{\"b\":2},{\"c\":3}]}",
      "{\"o\":{\"p\":{\"q\":[{\"a\":1},{\"b\":2},{\"c\":3}]}}}"})
  void readingEveryPartitionReproducesTheRecordSequence(final String json) throws IOException {
    for (int partitions = 1; partitions <= 6; partitions++) {
      assertRoundTrip(json, partitions);
    }
  }

  /**
   * Separators inside string literals are the classic way a naive splitter corrupts data: a comma, a
   * bracket or a quote in a value must never end a record.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "[{\"a\":\"x,y\"},{\"b\":\"],[\"},{\"c\":\"}{\"}]",
      "[{\"a\":\"back\\\\slash\"},{\"b\":\"quote\\\"inside\"},{\"c\":1}]",
      "[{\"a\":\"trailing backslash pair \\\\\\\\\"},{\"b\":2}]",
      "{\"a\":\"newline\\nin string\"}\n{\"b\":2}",
      "[{\"kü\":\"ößü\"},{\"emoji\":\"😀\"},{\"c\":\"日本語\"}]",
      "{\"d\":[{\"a\":\"],[\"},{\"b\":\"x,y\"},{\"c\":3}]}"})
  void separatorsInsideStringLiteralsDoNotSplitRecords(final String json) throws IOException {
    for (int partitions = 2; partitions <= 4; partitions++) {
      assertRoundTrip(json, partitions);
    }
  }

  /**
   * A UTF-8 byte-order mark is neither content nor whitespace. Scanned as content it reads as a bare
   * top-level scalar, which turns a BOM-prefixed array into "two top-level values" — detected as a
   * concatenated-record stream and split so the mark itself surfaces as a spurious leading record.
   */
  @Test
  void aByteOrderMarkIsNotMistakenForARecord() throws IOException {
    final Path file = writeBytes("﻿[{\"a\":1},{\"b\":2},{\"c\":3}]");

    final Plan plan = JsonPartitioner.plan(file, 3, Format.AUTO, NO_MINIMUM, null);

    assertEquals(Format.ARRAY, plan.format());
    assertEquals(3L, plan.recordCount());
    assertEquals(records("[{\"a\":1},{\"b\":2},{\"c\":3}]"), readAll(file, plan));
  }

  @Test
  void aByteOrderMarkIsNotMistakenForARecordInConcatenatedInput() throws IOException {
    final Path file = writeBytes("﻿{\"a\":1}\n{\"b\":2}\n{\"c\":3}");

    final Plan plan = JsonPartitioner.plan(file, 3, Format.AUTO, NO_MINIMUM, null);

    assertEquals(Format.NEWLINE_DELIMITED, plan.format());
    assertEquals(3L, plan.recordCount());
    assertEquals(records("{\"a\":1}\n{\"b\":2}\n{\"c\":3}"), readAll(file, plan));
  }

  /**
   * A closing quote ends a top-level value just as a closing bracket does. It is neither whitespace
   * nor a depth change, so without explicit handling {@code "a""b"} reads as a single value and every
   * record after the first is dropped without a word.
   */
  @ParameterizedTest
  @ValueSource(strings = {"\"a\"\"b\"", "\"a\"\"b\"\"c\"", "\"a\"5", "\"a\" \"b\"", "\"a\"{\"b\":2}"})
  void aClosingQuoteTerminatesATopLevelValue(final String json) throws IOException {
    final Plan plan = plan(json, 4);

    assertEquals(Format.NEWLINE_DELIMITED, plan.format());
    assertEquals(2L + (json.equals("\"a\"\"b\"\"c\"") ? 1L : 0L), plan.recordCount());
    assertRoundTrip(json, 4);
  }

  /**
   * Gson's reader accepts comments, so a commented input reaches the partitioner — but the byte
   * scanner does not model them, and cutting at a comma inside a comment would slice a record in two.
   *
   * <p>Refusing is the only safe answer. Degrading to a whole-file {@code SINGLE_DOCUMENT} partition
   * looks safer and is not: such a partition is handed to the shredder verbatim, and reading a
   * concatenated-record file as a single JSON value stops after the first record — trading a loud
   * failure for silent data loss.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "[{\"a\":1}, /* [x], y */ {\"b\":2}, {\"c\":3}]",
      "[{\"a\":1},\n// hi, there\n{\"b\":2}]",
      "[{\"a\":1}, // note ]\n {\"b\":2}]",
      "// generated by exporter\n{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"})
  void commentedInputIsRefusedRatherThanCutBlindly(final String json) throws IOException {
    final Path file = write(json);

    final SirixIOException failure =
        assertThrows(SirixIOException.class, () -> JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, null));
    assertTrue(failure.getMessage().contains("comment"), failure.getMessage());
  }

  /**
   * Gson's reader is lenient and accepts single-quoted strings, so the scanner must know them too.
   * Otherwise a separator inside {@code 'a,b'} reads as real structure and the cut lands mid-literal —
   * which either fails the whole parallel ingest or, when both fragments happen to parse, corrupts the
   * records silently.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,x','c','d']",
      "[{'k':'v,w'},{'k2':'],['},{\"k3\":\"z\"}]",
      "['a b','c d']",
      "['it\\'s escaped','next']"})
  void singleQuotedStringsAreNeverCutThrough(final String json) throws IOException {
    for (int partitions = 1; partitions <= 4; partitions++) {
      assertRoundTrip(json, partitions);
    }
  }

  @Test
  void singleQuotedConcatenatedRecordsKeepTheirContentIntact() throws IOException {
    // The silent-corruption shape: without single-quote support the splicer inserts a comma inside
    // each literal, and the whole plan reads back with no error as ["a ,b", "c ,d"].
    assertRoundTrip("'a b'\n'c d'\n", 1);
    assertRoundTrip("'a b'\n'c d'\n", 3);
  }

  @Test
  void carriageReturnLineEndingsSplitLikeAnyOtherWhitespace() throws IOException {
    assertRoundTrip("[{\"a\":1},\r\n{\"b\":2},\r\n{\"c\":3}]", 3);
    assertRoundTrip("{\"a\":1}\r\n{\"b\":2}\r\n{\"c\":3}", 3);
  }

  /**
   * The 5-argument overload lets a caller assert the layout, which takes a different route through
   * {@code plan} — no probe, only one collector built. Every other test goes through {@code AUTO}, so
   * without this the explicit branches ship unexercised.
   */
  @Test
  void anExplicitlyRequestedFormatReproducesTheRecordSequence() throws IOException {
    assertRoundTripWithFormat("[{\"a\":1},{\"b\":2},{\"c\":3}]", Format.ARRAY, null, 3L);
    assertRoundTripWithFormat("{\"a\":1}\n{\"b\":2}\n{\"c\":3}", Format.NEWLINE_DELIMITED, null, 3L);
    // An explicit NESTED_ARRAY needs no coverage bound and no probe-driven choice when named.
    assertRoundTripWithFormat("{\"meta\":1,\"data\":[{\"a\":1},{\"b\":2}]}", Format.NESTED_ARRAY, "data", 2L);
    // Named path absent: the request must fail rather than quietly fall back.
    final Path file = write("{\"data\":[{\"a\":1}]}");
    assertThrows(IllegalArgumentException.class,
        () -> JsonPartitioner.plan(file, 3, Format.NESTED_ARRAY, NO_MINIMUM, "nope"));
  }

  /**
   * Bare top-level scalars are terminated by whitespace rather than by a bracket or a quote — a
   * different branch of the scanner's value tracking, and the only one no other round-trip exercises.
   */
  @ParameterizedTest
  @ValueSource(strings = {"1 2 3", "true false null", "1\n2\n3\n", "1.5 -2e3 0"})
  void bareTopLevelScalarsSplitOnWhitespace(final String json) throws IOException {
    for (int partitions = 1; partitions <= 4; partitions++) {
      assertRoundTrip(json, partitions);
    }
  }

  @Test
  void multiByteCodePointsSurviveThePartitionBoundary() throws IOException {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 200; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"k\":\"日本語のテキスト😀").append(i).append("\"}");
    }
    json.append(']');

    assertRoundTrip(json.toString(), 8);
  }

  // ==================== Cutting behaviour ====================

  @Test
  void neverEmitsMorePartitionsThanRequested() throws IOException {
    final String json = arrayOfObjects(500);

    for (int requested = 1; requested <= 16; requested++) {
      final Plan plan = plan(json, requested);
      assertTrue(plan.partitions().size() <= requested,
          "asked for " + requested + " but got " + plan.partitions().size());
    }
  }

  @Test
  void partitionsAreContiguousAndOrdered() throws IOException {
    final Plan plan = plan(arrayOfObjects(500), 6);

    long previousEnd = -1L;
    for (int i = 0; i < plan.partitions().size(); i++) {
      final Partition partition = plan.partitions().get(i);
      assertEquals(i, partition.index());
      assertTrue(partition.byteLength() > 0L, "partition " + i + " is empty");
      if (previousEnd >= 0L) {
        // Consecutive partitions abut, save for the single separator byte the cut consumed.
        assertTrue(partition.startOffset() - previousEnd <= 1L,
            "gap between partitions " + (i - 1) + " and " + i);
        assertTrue(partition.startOffset() >= previousEnd, "partitions " + (i - 1) + " and " + i + " overlap");
      }
      previousEnd = partition.endOffsetExclusive();
    }
  }

  @Test
  void recordCountsSumToThePlansTotal() throws IOException {
    final Plan plan = plan(arrayOfObjects(500), 7);

    long sum = 0L;
    for (final Partition partition : plan.partitions()) {
      sum += partition.recordCount();
    }
    assertEquals(500L, plan.recordCount());
    assertEquals(plan.recordCount(), sum);
  }

  /**
   * A record-less tail — everything after a trailing comma, or trailing whitespace — must not become
   * a partition. Shredding it would create a whole resource whose entire content is an empty array.
   */
  @Test
  void aRecordLessTailDoesNotBecomeAPartition() throws IOException {
    for (final String json : new String[] {"[{\"a\":1},{\"b\":2},]", "[{\"a\":1},{\"b\":2}   ]",
        "[{\"a\":1},{\"b\":2},   ]"}) {
      final Plan plan = plan(json, 3);

      assertEquals(2L, plan.recordCount(), json);
      for (final Partition partition : plan.partitions()) {
        assertTrue(partition.recordCount() > 0L, () -> "record-less partition emitted for " + json);
      }
    }
  }

  @Test
  void anEmptyArrayStillYieldsItsSolePartitionEvenThoughItHoldsNoRecord() throws IOException {
    final Plan plan = plan("[]", 4);

    assertEquals(1, plan.partitions().size());
    assertEquals(0L, plan.recordCount());
  }

  @Test
  void theMinimumPartitionSizeSuppressesTinyShards() throws IOException {
    final Path file = write(arrayOfObjects(500));

    final Plan bounded = JsonPartitioner.plan(file, 16, Format.AUTO, Long.MAX_VALUE / 4, null);

    assertEquals(1, bounded.partitions().size());
    assertFalse(bounded.isPartitioned());
  }

  @Test
  void aSingleRequestedPartitionStillCoversEveryRecord() throws IOException {
    final Path file = write(arrayOfObjects(50));

    final Plan plan = JsonPartitioner.plan(file, 1, Format.AUTO, NO_MINIMUM, null);

    assertEquals(1, plan.partitions().size());
    assertEquals(Format.ARRAY, plan.format());
    assertEquals(50L, plan.recordCount());
  }

  /**
   * Asking for one partition must not degrade into "hand back the raw file": reading a concatenated
   * -record file as a single JSON value stops after the first record, so a shortcut there would drop
   * data silently.
   */
  @Test
  void aSingleRequestedPartitionDoesNotCollapseConcatenatedRecords() throws IOException {
    final Plan plan = plan("{\"a\":1}\n{\"b\":2}\n{\"c\":3}", 1);

    assertEquals(Format.NEWLINE_DELIMITED, plan.format());
    assertEquals(3L, plan.recordCount());
    assertRoundTrip("{\"a\":1}\n{\"b\":2}\n{\"c\":3}", 1);
  }

  @Test
  void anExplicitSingleDocumentFormatSkipsTheScan() throws IOException {
    final Path file = write(arrayOfObjects(50));

    final Plan plan = JsonPartitioner.plan(file, 4, Format.SINGLE_DOCUMENT, NO_MINIMUM, null);

    assertEquals(Format.SINGLE_DOCUMENT, plan.format());
    assertEquals(1, plan.partitions().size());
    assertEquals(0L, plan.partitions().getFirst().startOffset());
    assertEquals(Files.size(file), plan.partitions().getFirst().endOffsetExclusive());
  }

  // ==================== Degenerate inputs ====================

  @Test
  void anEmptyFileYieldsOneEmptyPartition() throws IOException {
    final Plan plan = plan("", 4);

    assertEquals(Format.SINGLE_DOCUMENT, plan.format());
    assertEquals(1, plan.partitions().size());
    assertEquals(0L, plan.recordCount());
    assertEquals(0L, plan.partitions().getFirst().byteLength());
  }

  @Test
  void aWhitespaceOnlyFileReportsNoRecords() throws IOException {
    final Plan plan = plan("   \n\t  ", 4);

    assertEquals(1, plan.partitions().size());
    assertEquals(0L, plan.recordCount());
  }

  @Test
  void anEmptyArrayYieldsOnePartitionWithNoRecords() throws IOException {
    final Plan plan = plan("[]", 4);

    assertEquals(Format.ARRAY, plan.format());
    assertEquals(1, plan.partitions().size());
    assertEquals(0L, plan.recordCount());
  }

  @Test
  void aSingleElementArrayIsNotSplit() throws IOException {
    final Plan plan = plan("[{\"a\":1}]", 4);

    assertEquals(1, plan.partitions().size());
    assertEquals(1L, plan.recordCount());
  }

  @ParameterizedTest
  @ValueSource(strings = {"[{\"a\":1}", "{\"a\":1", "[1,2,3", "{\"a\":\"unterminated"})
  void unbalancedInputIsRejectedRatherThanSplitWrongly(final String json) throws IOException {
    final Path file = write(json);

    final SirixIOException failure =
        assertThrows(SirixIOException.class, () -> JsonPartitioner.plan(file, 4, Format.AUTO, NO_MINIMUM, null));
    assertNotNull(failure.getMessage());
  }

  // ==================== Argument validation ====================

  @Test
  void rejectsANonPositivePartitionCount() throws IOException {
    final Path file = write("[{\"a\":1}]");

    assertThrows(IllegalArgumentException.class, () -> JsonPartitioner.plan(file, 0));
    assertThrows(IllegalArgumentException.class, () -> JsonPartitioner.plan(file, -1));
  }

  @Test
  void rejectsNullArguments() throws IOException {
    final Path file = write("[{\"a\":1}]");

    assertThrows(NullPointerException.class, () -> JsonPartitioner.plan(null, 4));
    assertThrows(NullPointerException.class, () -> JsonPartitioner.plan(file, 4, null, NO_MINIMUM, null));
    assertThrows(NullPointerException.class, () -> JsonPartitioner.reader(file, null));
    assertThrows(NullPointerException.class,
        () -> JsonPartitioner.reader(null, new Partition(0, 0L, 1L, 1L, false, false)));
  }

  @Test
  void rejectsAnInconsistentPartition() {
    assertThrows(IllegalArgumentException.class, () -> new Partition(-1, 0L, 1L, 1L, false, false));
    assertThrows(IllegalArgumentException.class, () -> new Partition(0, -1L, 1L, 1L, false, false));
    assertThrows(IllegalArgumentException.class, () -> new Partition(0, 5L, 1L, 1L, false, false));
    assertThrows(IllegalArgumentException.class, () -> new Partition(0, 0L, 1L, -1L, false, false));
    // Splicing separators only makes sense for content that is presented as an array.
    assertThrows(IllegalArgumentException.class, () -> new Partition(0, 0L, 1L, 1L, false, true));
  }

  // ==================== Reader plumbing ====================

  @Test
  void readersMatchThePartitionsOneForOne() throws Exception {
    final Path file = write(arrayOfObjects(500));
    final Plan plan = JsonPartitioner.plan(file, 5, Format.AUTO, NO_MINIMUM, null);

    final List<Callable<JsonReader>> readers = plan.readers();

    assertEquals(plan.partitions().size(), readers.size());
    for (final Callable<JsonReader> factory : readers) {
      try (final JsonReader reader = factory.call()) {
        assertEquals(JsonToken.BEGIN_ARRAY, reader.peek());
      }
    }
  }

  @Test
  void closingAReaderReleasesTheChannel() throws Exception {
    final Path file = write(arrayOfObjects(20));
    final Plan plan = JsonPartitioner.plan(file, 3, Format.AUTO, NO_MINIMUM, null);

    // Opening and closing every partition repeatedly must not leak descriptors.
    for (int round = 0; round < 200; round++) {
      for (final Partition partition : plan.partitions()) {
        JsonPartitioner.reader(file, partition).close();
      }
    }
  }

  @Test
  void partitionedReadersIsEquivalentToPlanThenReaders() throws IOException {
    final Path file = write(arrayOfObjects(500));

    assertEquals(JsonPartitioner.plan(file, 4).partitions().size(),
        JsonPartitioner.partitionedReaders(file, 4).size());
  }

  // ==================== Helpers ====================

  /**
   * Assert the round-trip property: concatenating the records read from every partition, in index
   * order, reproduces the input's own record sequence.
   */
  private void assertRoundTrip(final String json, final int partitions) throws IOException {
    final Path file = write(json);
    final Plan plan = JsonPartitioner.plan(file, partitions, Format.AUTO, NO_MINIMUM, null);

    final List<JsonElement> read = new ArrayList<>();
    long declaredRecords = 0L;
    for (final Partition partition : plan.partitions()) {
      declaredRecords += partition.recordCount();
      try (final JsonReader reader = JsonPartitioner.reader(file, partition)) {
        final JsonElement parsed = JsonParser.parseReader(reader);
        if (plan.format() == Format.SINGLE_DOCUMENT) {
          read.add(parsed);
        } else {
          parsed.getAsJsonArray().forEach(read::add);
        }
      }
    }

    final List<JsonElement> expected = recordsOf(json, plan);
    assertEquals(expected, read, () -> "record sequence differs for " + partitions + " partition(s) of " + json);
    assertEquals(expected.size(), declaredRecords,
        () -> "declared record count differs for " + partitions + " partition(s) of " + json);
    assertTrue(plan.partitions().size() <= partitions);
  }

  /** Round-trip a plan made with an explicitly asserted layout rather than a detected one. */
  private void assertRoundTripWithFormat(final String json, final Format format, final String recordsPath,
      final long expectedRecords) throws IOException {
    final Path file = write(json);
    final Plan plan = JsonPartitioner.plan(file, 3, format, NO_MINIMUM, recordsPath);

    assertEquals(format, plan.format());
    assertEquals(expectedRecords, plan.recordCount(), json);
    assertEquals(recordsOf(json, plan), readAll(file, plan), json);
  }

  /** The record sequence the input holds, derived independently of the partitioner. */
  private static List<JsonElement> recordsOf(final String json, final Plan plan) throws IOException {
    final List<JsonElement> records = new ArrayList<>();
    switch (plan.format()) {
      case ARRAY -> JsonParser.parseString(json).getAsJsonArray().forEach(records::add);
      case NESTED_ARRAY -> {
        JsonElement cursor = JsonParser.parseString(json);
        for (final String step : plan.recordsField().split("\\.")) {
          cursor = cursor.getAsJsonObject().get(step);
        }
        cursor.getAsJsonArray().forEach(records::add);
      }
      case NEWLINE_DELIMITED -> {
        final JsonReader reader = new JsonReader(new StringReader(json));
        reader.setStrictness(Strictness.LENIENT);
        while (reader.peek() != JsonToken.END_DOCUMENT) {
          records.add(JsonParser.parseReader(reader));
        }
      }
      case SINGLE_DOCUMENT -> records.add(JsonParser.parseString(json));
      case AUTO -> throw new AssertionError("a plan never resolves to AUTO");
    }
    return records;
  }

  private Plan plan(final String json, final int partitions) throws IOException {
    return JsonPartitioner.plan(write(json), partitions, Format.AUTO, NO_MINIMUM, null);
  }

  private Path write(final String json) throws IOException {
    final Path file = Files.createTempFile(tempDir, "partitioner", ".json");
    Files.writeString(file, json, StandardCharsets.UTF_8);
    return file;
  }

  /** Write raw UTF-8 bytes, so a leading {@code U+FEFF} lands on disk as a real byte-order mark. */
  private Path writeBytes(final String json) throws IOException {
    final Path file = Files.createTempFile(tempDir, "partitioner", ".json");
    Files.write(file, json.getBytes(StandardCharsets.UTF_8));
    return file;
  }

  /** Every record of every partition of {@code plan}, in index order. */
  private static List<JsonElement> readAll(final Path file, final Plan plan) throws IOException {
    final List<JsonElement> read = new ArrayList<>();
    for (final Partition partition : plan.partitions()) {
      try (final JsonReader reader = JsonPartitioner.reader(file, partition)) {
        final JsonElement parsed = JsonParser.parseReader(reader);
        if (plan.format() == Format.SINGLE_DOCUMENT) {
          read.add(parsed);
        } else {
          parsed.getAsJsonArray().forEach(read::add);
        }
      }
    }
    return read;
  }

  /** The record sequence of a BOM-free equivalent of the input, parsed independently. */
  private static List<JsonElement> records(final String json) throws IOException {
    final List<JsonElement> expected = new ArrayList<>();
    final JsonReader reader = new JsonReader(new StringReader(json));
    reader.setStrictness(Strictness.LENIENT);
    while (reader.peek() != JsonToken.END_DOCUMENT) {
      final JsonElement value = JsonParser.parseReader(reader);
      if (value.isJsonArray() && json.stripLeading().startsWith("[")) {
        value.getAsJsonArray().forEach(expected::add);
      } else {
        expected.add(value);
      }
    }
    return expected;
  }

  /** A records array padded so it dominates its wrapper, as every real-world export's does. */
  private static String paddedRecords(final int count) {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"i\":").append(i).append(",\"pad\":\"").append("p".repeat(200)).append("\"}");
    }
    return json.append(']').toString();
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

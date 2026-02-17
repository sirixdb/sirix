/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.settings.VersioningType;
import io.sirix.utils.LogWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.checkerframework.org.apache.commons.lang3.time.StopWatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link JacksonJsonShredder}.
 * 
 * <p>
 * These tests verify that the Jackson-based shredder produces identical results to the Gson-based
 * {@link JsonShredder}. Each test case validates:
 * <ul>
 * <li>Correct parsing of JSON structures</li>
 * <li>Proper node insertion into Sirix</li>
 * <li>Accurate serialization back to equivalent JSON</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class JacksonJsonShredderTest {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper logger = new LogWrapper(LoggerFactory.getLogger(JacksonJsonShredderTest.class));

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  // ==================== Basic Tests ====================

  @Test
  public void testParserCreation() throws IOException {
    try (final var parser = JacksonJsonShredder.createStringParser("\"test\"")) {
      parser.nextToken();
      assertEquals("test", parser.getText());
    }
  }

  @Test
  public void testEmptyObject() throws IOException {
    testRoundTrip("{}");
  }

  @Test
  public void testEmptyArray() throws IOException {
    testRoundTrip("[]");
  }

  @Test
  public void testSimpleString() throws IOException {
    testStringComparison("string.json");
  }

  @Test
  public void testSimpleBoolean() throws IOException {
    testStringComparison("boolean.json");
  }

  @Test
  public void testSimpleNumber() throws IOException {
    testStringComparison("number.json");
  }

  @Test
  public void testSimpleNull() throws IOException {
    testStringComparison("null.json");
  }

  // ==================== Array Tests ====================

  @Test
  public void testArray() throws IOException {
    test("array.json");
  }

  @Test
  public void testArrayAsLastChild() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(1);
      try (final var insertParser = JacksonJsonShredder.createStringParser("[]")) {
        trx.insertSubtreeAsLastChild(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = """
            ["foo",null,[],true,1.22,[]]""";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testArrayAsLeftSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(3);
      try (final var insertParser = JacksonJsonShredder.createStringParser("[]")) {
        trx.insertSubtreeAsLeftSibling(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",[],null,[],true,1.22]";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testArrayAsRightSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      try (final var insertParser = JacksonJsonShredder.createStringParser("[]")) {
        trx.insertSubtreeAsRightSibling(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,[],[],true,1.22]";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  // ==================== Object Tests ====================

  @Test
  public void testObjectAsLastChild() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(1);
      try (final var insertParser = JacksonJsonShredder.createStringParser("{\"foo\":null}")) {
        trx.insertSubtreeAsLastChild(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,[],true,1.22,{\"foo\":null}]";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testObjectAsLeftSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      try (final var insertParser = JacksonJsonShredder.createStringParser("{\"foo\":null}")) {
        trx.insertSubtreeAsLeftSibling(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = """
            ["foo",null,{"foo":null},[],true,1.22]""";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testObjectAsRightSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      try (final var insertParser = JacksonJsonShredder.createStringParser("{\"foo\":null}")) {
        trx.insertSubtreeAsRightSibling(insertParser);
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = """
            ["foo",null,[],{"foo":null},true,1.22]""";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  // ==================== Complex Structure Tests ====================

  @Test
  public void testComplex1() throws IOException {
    test("complex1.json");
  }

  @Test
  public void testComplex2() throws IOException {
    test("complex2.json");
  }

  @Test
  public void testBlockChain() throws IOException {
    test("blockchain.json");
  }

  @Test
  public void testBlockchainLatestBlock() throws IOException {
    test("blockchain-latestblock.json");
  }

  @Test
  public void testBusinessServiceProviders() throws IOException {
    test("business-service-providers.json");
  }

  @Test
  public void testABCLocationStations() throws IOException {
    test("abc-location-stations.json");
  }

  @Test
  public void testTradeAPIs() throws IOException {
    test("trade-apis.json");
  }

  @Test
  public void testTestDocument() throws IOException {
    test("test.json");
  }

  @Test
  public void testLarge() throws IOException {
    test("large-file.json");
  }

  @Test
  public void testCvx() throws IOException {
    test("CVX.json");
  }

  @Test
  public void testMovies() throws IOException {
    test("movies.json");
  }

  @Test
  public void testLinux() throws IOException {
    test("linux.json");
  }

  @Test
  public void testCopperFieldBook() throws IOException {
    test("copperfield-book.json");
  }

  @Test
  public void testLaureate() throws IOException {
    test("laureate.json");
  }

  @Test
  public void testRedditAll() throws IOException {
    test("reddit-all.json");
  }

  @Disabled("Duplicate keys")
  @Test
  public void testHistoricalEventsEnglish() throws IOException {
    test("historical-events-english.json");
  }

  // ==================== Edge Case Tests ====================

  @Test
  public void testNestedEmptyStructures() throws IOException {
    testRoundTrip("{\"a\":{},\"b\":[],\"c\":{\"d\":[]}}");
  }

  @Test
  public void testDeeplyNested() throws IOException {
    testRoundTrip("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":\"deep\"}}}}}}");
  }

  @Test
  public void testMixedArray() throws IOException {
    testRoundTrip("[1,\"two\",true,null,{\"key\":\"value\"},[1,2,3]]");
  }

  @Test
  public void testSpecialCharacters() throws IOException {
    testRoundTrip("{\"special\":\"line1\\nline2\\ttab\\\"quote\\\\\"}");
  }

  @Test
  public void testUnicode() throws IOException {
    testRoundTrip("{\"unicode\":\"Hello, 世界! 🌍\"}");
  }

  @Test
  public void testLargeNumber() throws IOException {
    testRoundTrip("{\"bigInt\":9999999999999999999,\"bigDec\":123.456789012345678901234567890}");
  }

  @Test
  public void testNegativeNumbers() throws IOException {
    testRoundTrip("{\"negative\":-42,\"negFloat\":-3.14}");
  }

  @Test
  public void testScientificNotation() throws IOException {
    testRoundTrip("{\"sci1\":1.23e10,\"sci2\":1.23E-10}");
  }

  // ==================== Performance Test ====================

  @Disabled("Manual performance test")
  @Test
  public void testPerformanceComparison() throws IOException {
    final var jsonPath = JSON.resolve("large-file.json");

    // Warm up
    for (int i = 0; i < 3; i++) {
      runGsonShredder(jsonPath);
      runJacksonShredder(jsonPath);
    }

    // Benchmark Gson
    long gsonTotal = 0;
    for (int i = 0; i < 5; i++) {
      JsonTestHelper.closeEverything();
      long start = System.nanoTime();
      runGsonShredder(jsonPath);
      gsonTotal += System.nanoTime() - start;
    }
    JsonTestHelper.closeEverything();

    // Benchmark Jackson
    long jacksonTotal = 0;
    for (int i = 0; i < 5; i++) {
      JsonTestHelper.closeEverything();
      long start = System.nanoTime();
      runJacksonShredder(jsonPath);
      jacksonTotal += System.nanoTime() - start;
    }

    logger.info("Gson average: " + (gsonTotal / 5_000_000) + " ms");
    logger.info("Jackson average: " + (jacksonTotal / 5_000_000) + " ms");
    logger.info("Speedup: " + String.format("%.2fx", (double) gsonTotal / jacksonTotal));
  }

  @Disabled("Large file test - run manually")
  @Test
  public void testShredderAndTraverseChicago() {
    logger.info("start");
    final var jsonPath = JSON.resolve("cityofchicago.json");
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createResourceWithJackson(jsonPath, database);
    }
  }

  private void createResourceWithJackson(Path jsonPath, Database<JsonResourceSession> database) {
    var stopWatch = new StopWatch();
    stopWatch.start();
    database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                 .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                 .buildPathSummary(true)
                                                 .storeDiffs(true)
                                                 .storeNodeHistory(false)
                                                 .storeChildCount(true)
                                                 .hashKind(HashType.ROLLING)
                                                 .useTextCompression(false)
                                                 .storageType(StorageType.MEMORY_MAPPED)
                                                 .useDeweyIDs(false)
                                                 .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor()))
                                                 .build());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx((262_144 << 3));
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      trx.insertSubtreeAsFirstChild(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + "s].");
  }

  // ==================== Helper Methods ====================

  private void runGsonShredder(Path jsonPath) {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
    }
  }

  private void runJacksonShredder(Path jsonPath) throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
    }
  }

  private void testRoundTrip(String json) throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(json)) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals(json, writer.toString(), true);
      }
    }
  }

  private void testStringComparison(String jsonFile) throws IOException {
    final var jsonPath = JSON.resolve(jsonFile);
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath);
        final Writer writer = new StringWriter()) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
      final var actual = writer.toString();
      assertEquals(expected, actual);
    }
  }

  private void test(String jsonFile) throws IOException {
    final var jsonPath = JSON.resolve(jsonFile);
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createFileParser(jsonPath);
        final Writer writer = new StringWriter()) {
      final var shredder =
          new JacksonJsonShredder.Builder(trx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
      final var actual = writer.toString();
      JSONAssert.assertEquals(expected, actual, true);
    }
  }
}


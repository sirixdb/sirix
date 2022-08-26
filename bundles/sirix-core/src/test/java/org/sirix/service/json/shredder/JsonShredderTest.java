package org.sirix.service.json.shredder;

import org.checkerframework.org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.Axis;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.io.StorageType;
import org.sirix.service.InsertPosition;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.settings.VersioningType;
import org.sirix.utils.LogWrapper;
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

import static org.junit.Assert.assertEquals;

public final class JsonShredderTest {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper logger = new LogWrapper(LoggerFactory.getLogger(JsonShredder.class));

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    try (final var jsonStringReader = JsonShredder.createStringReader("test")) {
      assertEquals("test", jsonStringReader.nextString());
    }
  }

  @Ignore
  @Test
  public void testChicagoDescendantAxis() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {
      var stopWatch = new StopWatch();
      logger.info("start");
      stopWatch.start();
      logger.info("Max node key: " + rtx.getMaxNodeKey());
      Axis axis = new DescendantAxis(rtx);

      int count = 0;

      while (axis.hasNext()) {
        final var nodeKey = axis.nextLong();
        if (count % 50_000_000L == 0) {
          logger.info("nodeKey: " + nodeKey);
        }
        count++;
      }

      logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS)  + " s].");

      stopWatch = new StopWatch();
      stopWatch.start();

      logger.info("start");
      axis = new PostOrderAxis(rtx);

      count = 0;

      while (axis.hasNext()) {
        final var nodeKey = axis.nextLong();
        if (count % 50_000_000L == 0) {
          logger.info("nodeKey: " + nodeKey);
        }
        count++;
      }

      logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + " s].");
    }
  }

  // TODO: JMH test
  // JVM flags: -XX:+UseShenandoahGC -Xlog:gc -XX:+UnlockExperimentalVMOptions -XX:+AlwaysPreTouch -XX:+UseLargePages -XX:-UseBiasedLocking -XX:+DisableExplicitGC -XX:+PrintCompilation -XX:ReservedCodeCacheSize=1000m -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:EliminateAllocationArraySizeLimit=1024
  @Ignore
  @Test
  public void testChicago() {
    logger.info("start");
    final var jsonPath = JSON.resolve("cityofchicago.json");
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createResource(jsonPath, database);
      database.removeResource(JsonTestHelper.RESOURCE);

      createResource(jsonPath, database);
      database.removeResource(JsonTestHelper.RESOURCE);

      createResource(jsonPath, database);
      database.removeResource(JsonTestHelper.RESOURCE);

      createResource(jsonPath, database);
      database.removeResource(JsonTestHelper.RESOURCE);

      createResource(jsonPath, database);
      database.removeResource(JsonTestHelper.RESOURCE);
    }
  }

  private void createResource(Path jsonPath, Database<JsonResourceSession> database) {
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
                                                 .build());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx(262_144 << 1)) {
      trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath));
    }

    logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + " s].");
  }

  @Test
  public void testLarge() throws IOException {
    test("CVX.json");
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

  @Test
  public void testArray() throws IOException {
    test("array.json");
  }

  @Test
  public void testArrayAsLastChild() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(1);
      trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("[]"));

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,[],true,1.22,[]]";
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
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(3);
      trx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("[]"));

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
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("[]"));

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,[],[],true,1.22]";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testObjectAsLastChild() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(1);
      trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"foo\":null}"));

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
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      trx.insertSubtreeAsLeftSibling(JsonShredder.createStringReader("{\"foo\":null}"));

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,{\"foo\":null},[],true,1.22]";
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
         final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }

      trx.moveTo(4);
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\":null}"));

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        final var expected = "[\"foo\",null,[],{\"foo\":null},true,1.22]";
        final var actual = writer.toString();
        JSONAssert.assertEquals(expected, actual, true);
      }
    }
  }

  @Test
  public void testBoolean() throws IOException {
    testStringComparison("boolean.json");
  }

  @Test
  public void testString() throws IOException {
    testStringComparison("string.json");
  }

  @Test
  public void testNumber() throws IOException {
    testStringComparison("number.json");
  }

  @Test
  public void testNull() throws IOException {
    testStringComparison("null.json");
  }

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
  public void testBusinessServiceProviders() throws IOException {
    test("business-service-providers.json");
  }

  @Test
  public void testABCLocationStations() throws IOException {
    test("abc-location-stations.json");
  }

  @Ignore
  @Test
  public void testHistoricalEventsEnglish() throws IOException {
    test("historical-events-english.json");
  }

  @Test
  public void testTradeAPIs() throws IOException {
    test("trade-apis.json");
  }

  @Test
  public void testTestDocument() throws IOException {
    test("test.json");
  }

  private void testStringComparison(String jsonFile) throws IOException {
    final var jsonPath = JSON.resolve(jsonFile);
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
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
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx,
                                                    JsonShredder.createFileReader(jsonPath),
                                                    InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      final var expected = Files.readString(jsonPath, StandardCharsets.UTF_8);
      final var actual = writer.toString();
      JSONAssert.assertEquals(expected, actual, true);
    }
  }
}

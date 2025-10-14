package io.sirix.service.json.shredder;

import com.google.gson.stream.JsonToken;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.LZ4Compressor;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.settings.VersioningType;
import io.sirix.utils.LogWrapper;
import org.checkerframework.org.apache.commons.lang3.time.StopWatch;
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
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class JsonShredderTest {

  /**
   * {@link LogWrapper} reference.
   */
  private static final LogWrapper logger = new LogWrapper(LoggerFactory.getLogger(JsonShredderTest.class));

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  private static final int NUMBER_OF_PROCESSORS = 5;

  private static final ExecutorService THREAD_POOL =
      Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    try (final var jsonStringReader = JsonShredder.createStringReader("test")) {
      assertEquals("test", jsonStringReader.nextString());
    }
  }

  @Disabled
  @Test
  public void testChicagoDescendantAxisParallel() throws InterruptedException {
//    if (Files.notExists(PATHS.PATH1.getFile())) {
//      logger.info("start");
//      final var jsonPath = JSON.resolve("cityofchicago.json");
//      Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
//      try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
//        createResource(jsonPath, database, false);
//      }
//    }
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);

    final var callableList = new ArrayList<Callable<Object>>(NUMBER_OF_PROCESSORS);

    for (int i = 0; i < NUMBER_OF_PROCESSORS; i++) {
      int finalNumber = i;
      callableList.add(Executors.callable(() -> {
        try {
          switch (finalNumber) {
            case 1:
              Thread.sleep(1_000);
            case 2:
              Thread.sleep(2_000);
            case 3:
              Thread.sleep(3_000);
            case 4:
              Thread.sleep(4_000);
          }
        } catch (InterruptedException _) {
        }

        final var rtx = session.beginNodeReadOnlyTrx();

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

        logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + "s].");

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

        logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS)+ "s].");
      }));
    }

    THREAD_POOL.invokeAll(callableList);
    THREAD_POOL.shutdown();
    THREAD_POOL.awaitTermination(20, TimeUnit.MINUTES);
  }

  @Disabled
  @Test
  public void testChicagoDescendantAxis() {
//    if (Files.notExists(PATHS.PATH1.getFile())) {
//      logger.info("start");
//      final var jsonPath = JSON.resolve("cityofchicago.json");
//      Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
//      try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
//        createResource(jsonPath, database, false);
//      }
//    }
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

      logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + "s].");

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

      logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + "s].");
    }
  }

  // TODO: JMH test
  // Use Shenandoah or ZGC
  // JVM flags: -XX:+UseShenandoahGC -Xlog:gc -XX:+UnlockExperimentalVMOptions -XX:+AlwaysPreTouch -XX:+UseLargePages -XX:+DisableExplicitGC -XX:MaxDirectMemorySize=8g -XX:+PrintCompilation -XX:ReservedCodeCacheSize=1000m -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:EliminateAllocationArraySizeLimit=1024
  @Test
  public void testShredderAndTraverseChicago() {
    logger.info("start");
    final var jsonPath = JSON.resolve("cityofchicago.json");
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()).setMaxSegmentAllocationSize(3L * (1L << 30))); // 3GB
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createResource(jsonPath, database, false);
      //      database.removeResource(JsonTestHelper.RESOURCE);
      //
      //      createResource(jsonPath, database);
      //      database.removeResource(JsonTestHelper.RESOURCE);
      //
      //      createResource(jsonPath, database);
      //      database.removeResource(JsonTestHelper.RESOURCE);
      //
      //      createResource(jsonPath, database);
      //      database.removeResource(JsonTestHelper.RESOURCE);
      //
      //      createResource(jsonPath, database);
    }
  }

  private void createResource(Path jsonPath, Database<JsonResourceSession> database, boolean doTraverse) {
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
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .useDeweyIDs(false)
                                                 .byteHandlerPipeline(new ByteHandlerPipeline(new LZ4Compressor()))
                                                 .build());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx(262_144 << 3)) {
      trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath));

      if (doTraverse) {
        trx.moveToDocumentRoot();
        logger.info("Max node key: " + trx.getMaxNodeKey());

        Axis axis = new DescendantAxis(trx);

        int count = 0;

        while (axis.hasNext()) {
          axis.nextLong();

          if (count % 5_000_000L == 0) {
            logger.info("node: " + axis.getTrx().getNode());
          }
          count++;
        }
      }
    }

    logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + "s].");
  }

  @Disabled
  @Test
  public void testParseChicago() throws IOException {
    final var stopWatch = new StopWatch();
    stopWatch.start();
    try (final var reader = JsonShredder.createFileReader(JSON.resolve("cityofchicago.json"))) {
      while (reader.peek() != JsonToken.END_DOCUMENT) {
        final var nextToken = reader.peek();

        switch (nextToken) {
          case BEGIN_OBJECT -> reader.beginObject();
          case NAME -> reader.nextName();
          case END_OBJECT -> reader.endObject();
          case BEGIN_ARRAY -> reader.beginArray();
          case END_ARRAY -> reader.endArray();
          case STRING, NUMBER -> reader.nextString();
          case BOOLEAN -> reader.nextBoolean();
          case NULL -> reader.nextNull();
          // Node kind not known.
          default -> throw new AssertionError("Unexpected token: " + nextToken);
        }
      }
    }
    System.out.println("Done in " + stopWatch.getTime(TimeUnit.MILLISECONDS) + "ms");
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
  public void testBlockchainLatestBlock() throws IOException {
    test("blockchain-latestblock.json");
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

  @Disabled("Duplicate keys")
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

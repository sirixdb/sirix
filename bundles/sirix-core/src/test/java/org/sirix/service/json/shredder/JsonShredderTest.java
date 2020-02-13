package org.sirix.service.json.shredder;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

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
import org.sirix.api.Database;
import org.sirix.axis.DescendantAxis;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.service.xml.shredder.InsertPosition;
import org.skyscreamer.jsonassert.JSONAssert;

public final class JsonShredderTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Ignore
  @Test
  public void testChicagoDescendantAxis() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var rtx = manager.beginNodeReadOnlyTrx()) {
      final var axis = new DescendantAxis(rtx);

      axis.forEach((unused) ->
                   {
                   });

      System.out.println("done");
    }
  }

  @Ignore
  @Test
  public void testChicago() {
    try {
      final var jsonPath = JSON.resolve("cityofchicago.json");
      Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
      try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
//                                                     .buildPathSummary(true)
                                                     .hashKind(HashType.NONE)
                                                     .useTextCompression(false)
                                                     .build());
        try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
            final var trx = manager.beginNodeTrx()) {
//          final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath), InsertPosition.AS_FIRST_CHILD).build();
//          shredder.call();
//          trx.commit();
          trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath));

          System.out.println();
        }
      }
    } catch (Error e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLinux() throws IOException {
    test("linux.json");
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
  public void testArrayAsRightSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
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
  public void testObjectAsRightSibling() throws IOException {
    final var jsonPath = JSON.resolve("array.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
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
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
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
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
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

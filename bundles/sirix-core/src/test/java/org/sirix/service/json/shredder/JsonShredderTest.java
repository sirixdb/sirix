package org.sirix.service.json.shredder;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.exception.SirixException;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.utils.JsonDocumentCreator;

public final class JsonShredderTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  private static final String COMPLEX_JSON_1 =
      "{\"problems\":[{\"Diabetes\":[{\"medications\":[{\"medicationsClasses\":[{\"className\":[{\"associatedDrug\":[{\"name\":\"asprin\",\"dose\":\"\",\"strength\":\"500 mg\"}],\"associatedDrug#2\":[{\"name\":\"somethingElse\",\"dose\":\"\",\"strength\":\"500 mg\"}]}],\"className2\":[{\"associatedDrug\":[{\"name\":\"asprin\",\"dose\":\"\",\"strength\":\"500 mg\"}],\"associatedDrug#2\":[{\"name\":\"somethingElse\",\"dose\":\"\",\"strength\":\"500 mg\"}]}]}]}],\"labs\":[{\"missing_field\":\"missing_value\"}]}],\"Asthma\":[{}]}]}";

  private static final String COMPLEX_JSON_2 =
      "{\"medications\":[{\"aceInhibitors\":[{\"name\":\"lisinopril\",\"strength\":\"10 mg Tab\",\"dose\":\"1 tab\",\"route\":\"PO\",\"sig\":\"daily\",\"pillCount\":\"#90\",\"refills\":\"Refill 3\"}],\"antianginal\":[{\"name\":\"nitroglycerin\",\"strength\":\"0.4 mg Sublingual Tab\",\"dose\":\"1 tab\",\"route\":\"SL\",\"sig\":\"q15min PRN\",\"pillCount\":\"#30\",\"refills\":\"Refill 1\"}],\"anticoagulants\":[{\"name\":\"warfarin sodium\",\"strength\":\"3 mg Tab\",\"dose\":\"1 tab\",\"route\":\"PO\",\"sig\":\"daily\",\"pillCount\":\"#90\",\"refills\":\"Refill 3\"}],\"betaBlocker\":[{\"name\":\"metoprolol tartrate\",\"strength\":\"25 mg Tab\",\"dose\":\"1 tab\",\"route\":\"PO\",\"sig\":\"daily\",\"pillCount\":\"#90\",\"refills\":\"Refill 3\"}],\"diuretic\":[{\"name\":\"furosemide\",\"strength\":\"40 mg Tab\",\"dose\":\"1 tab\",\"route\":\"PO\",\"sig\":\"daily\",\"pillCount\":\"#90\",\"refills\":\"Refill 3\"}],\"mineral\":[{\"name\":\"potassium chloride ER\",\"strength\":\"10 mEq Tab\",\"dose\":\"1 tab\",\"route\":\"PO\",\"sig\":\"daily\",\"pillCount\":\"#90\",\"refills\":\"Refill 3\"}]}],\"labs\":[{\"name\":\"Arterial Blood Gas\",\"time\":\"Today\",\"location\":\"Main Hospital Lab\"},{\"name\":\"BMP\",\"time\":\"Today\",\"location\":\"Primary Care Clinic\"},{\"name\":\"BNP\",\"time\":\"3 Weeks\",\"location\":\"Primary Care Clinic\"},{\"name\":\"BUN\",\"time\":\"1 Year\",\"location\":\"Primary Care Clinic\"},{\"name\":\"Cardiac Enzymes\",\"time\":\"Today\",\"location\":\"Primary Care Clinic\"},{\"name\":\"CBC\",\"time\":\"1 Year\",\"location\":\"Primary Care Clinic\"},{\"name\":\"Creatinine\",\"time\":\"1 Year\",\"location\":\"Main Hospital Lab\"},{\"name\":\"Electrolyte Panel\",\"time\":\"1 Year\",\"location\":\"Primary Care Clinic\"},{\"name\":\"Glucose\",\"time\":\"1 Year\",\"location\":\"Main Hospital Lab\"},{\"name\":\"PT/INR\",\"time\":\"3 Weeks\",\"location\":\"Primary Care Clinic\"},{\"name\":\"PTT\",\"time\":\"3 Weeks\",\"location\":\"Coumadin Clinic\"},{\"name\":\"TSH\",\"time\":\"1 Year\",\"location\":\"Primary Care Clinic\"}],\"imaging\":[{\"name\":\"Chest X-Ray\",\"time\":\"Today\",\"location\":\"Main Hospital Radiology\"},{\"name\":\"Chest X-Ray\",\"time\":\"Today\",\"location\":\"Main Hospital Radiology\"},{\"name\":\"Chest X-Ray\",\"time\":\"Today\",\"location\":\"Main Hospital Radiology\"}]}";

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.getResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(JSON.resolve("test.json")),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      assertEquals(JsonDocumentCreator.JSON, writer.toString());
    }
  }

  @Test
  public void testComplex1() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.getResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(JSON.resolve("complex1.json")),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      assertEquals(COMPLEX_JSON_1, writer.toString());
    }
  }

  @Test
  public void testComplex2() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.getResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(JSON.resolve("complex2.json")),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      assertEquals(COMPLEX_JSON_2, writer.toString());
    }
  }
}

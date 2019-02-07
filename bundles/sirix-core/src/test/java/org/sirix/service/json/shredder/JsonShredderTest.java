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
import org.sirix.service.xml.shredder.Insert;
import org.sirix.utils.JsonDocumentCreator;

public final class JsonShredderTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  private static final String COMPLEX_JSON =
      "{\"problems\":[{\"Diabetes\":[{\"medications\":[{\"medicationsClasses\":[{\"className\":[{\"associatedDrug\":[{\"name\":\"asprin\",\"dose\":\"\",\"strength\":\"500 mg\"}],\"associatedDrug#2\":[{\"name\":\"somethingElse\",\"dose\":\"\",\"strength\":\"500 mg\"}]}],\"className2\":[{\"associatedDrug\":[{\"name\":\"asprin\",\"dose\":\"\",\"strength\":\"500 mg\"}],\"associatedDrug#2\":[{\"name\":\"somethingElse\",\"dose\":\"\",\"strength\":\"500 mg\"}]}]}]}],\"labs\":[{\"missing_field\":\"missing_value\"}]}],\"Asthma\":[{}]}]}";

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
          Insert.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      System.out.println(writer.toString());
      assertEquals(JsonDocumentCreator.JSON, writer.toString());
    }
  }

  @Test
  public void testComplex() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.getResourceManager(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(JSON.resolve("complex.json")),
          Insert.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      System.out.println(writer.toString());
      assertEquals(COMPLEX_JSON, writer.toString());
    }
  }
}

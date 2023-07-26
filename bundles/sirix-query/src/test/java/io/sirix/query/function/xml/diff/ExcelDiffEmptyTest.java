package io.sirix.query.function.xml.diff;

import static java.util.stream.Collectors.toList;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.XQuery;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.diff.service.FMSEImport;
import io.sirix.exception.SirixException;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.serialize.XmlSerializer.XmlSerializerBuilder;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.shredder.XmlShredder;
import junit.framework.TestCase;

public final class ExcelDiffEmptyTest extends TestCase {

  private static final Path RESOURCES = Paths.get("src", "test", "resources");

  private static final Path XML_SHEETS = RESOURCES.resolve("sheets");

  static {
    XMLUnit.setIgnoreComments(true);
    XMLUnit.setIgnoreWhitespace(true);
  }

  @Override
  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @Override
  @After
  public void tearDown() throws SirixException, IOException {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testSheetsSecond() throws Exception {
    test(XML_SHEETS);
  }

  /**
   * Test a folder of XML files.
   *
   * @param folder path to the files
   * @throws Exception if any exception occurs
   */
  private void test(final Path folder) throws Exception {
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      XmlResourceSession resource = database.beginResourceSession(XmlTestHelper.RESOURCE);
      Predicate<Path> fileNameFilter = path -> path.getFileName().toString().endsWith(".xml");
      final List<Path> list = Files.list(folder).filter(fileNameFilter).collect(toList());

      // Sort files list according to file names.
      list.sort((first, second) -> {
        final String firstName =
            first.getFileName().toString().substring(0, first.getFileName().toString().indexOf('.'));
        final String secondName =
            second.getFileName().toString().substring(0, second.getFileName().toString().indexOf('.'));

        if (Integer.parseInt(firstName) < Integer.parseInt(secondName)) {
          return -1;
        } else if (Integer.parseInt(firstName) > Integer.parseInt(secondName)) {
          return +1;
        } else {
          return 0;
        }
      });

      boolean first = true;

      // Shredder files.
      for (final Path file : list) {
        if (file.getFileName().toString().endsWith(".xml")) {
          if (first) {
            first = false;
            try (final XmlNodeTrx wtx = resource.beginNodeTrx();
                final FileInputStream fis = new FileInputStream(file.toFile())) {
              final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis),
                  InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
              shredder.call();
            }
          } else {
            FMSEImport.main(
                new String[] {PATHS.PATH1.getFile().toAbsolutePath().toString(), file.toAbsolutePath().toString()});
          }

          resource.close();
          resource = database.beginResourceSession(XmlTestHelper.RESOURCE);

          final OutputStream out = new ByteArrayOutputStream();
          final XmlSerializer serializer = new XmlSerializerBuilder(resource, out).build();
          serializer.call();
          final StringBuilder sBuilder = XmlTestHelper.readFile(file, false);

          final Diff diff = new Diff(sBuilder.toString(), out.toString());
          final DetailedDiff detDiff = new DetailedDiff(diff);
          @SuppressWarnings("unchecked")
          final List<Difference> differences = detDiff.getAllDifferences();
          for (final Difference difference : differences) {
            System.err.println("***********************");
            System.err.println(difference);
            System.err.println("***********************");
          }

          assertTrue("pieces of XML are similar " + diff, diff.similar());
          assertTrue("but are they identical? " + diff, diff.identical());
        }
      }
    }

    // Initialize query context and store.
    final var database = PATHS.PATH1.getFile();

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = database.getFileName().toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq = "xml:diff('" + dbName + "','" + resName + "',1,2)";

      final XQuery query = new XQuery(SirixCompileChain.createWithNodeStore(store), xq);

      try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        query.serialize(ctx, new PrintStream(out));
        final String content = new String(out.toByteArray(), StandardCharsets.UTF_8);
        out.reset();

        assertEquals("", content);
      }
    }
  }
}

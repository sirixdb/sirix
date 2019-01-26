package org.sirix.diff.algorithm;

import static java.util.stream.Collectors.toList;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLTestCase;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.XdmTestHelper;
import org.sirix.XdmTestHelper.PATHS;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.diff.service.FMSEImport;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * Test the FMSE implementation.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class FMSETest extends XMLTestCase {
  private static final Path RESOURCES = Paths.get("src", "test", "resources");

  private static final Path XMLINSERTFIRST = RESOURCES.resolve("revXMLsInsert");

  private static final Path XMLINSERTSECOND = RESOURCES.resolve("revXMLsInsert1");

  private static final Path XMLINSERTTHIRD = RESOURCES.resolve("revXMLsInsert2");

  private static final Path XMLDELETEFIRST = RESOURCES.resolve("revXMLsDelete");

  private static final Path XMLDELETESECOND = RESOURCES.resolve("revXMLsDelete1");

  private static final Path XMLDELETETHIRD = RESOURCES.resolve("revXMLsDelete2");

  private static final Path XMLDELETEFOURTH = RESOURCES.resolve("revXMLsDelete3");

  private static final Path XMLSAMEFIRST = RESOURCES.resolve("revXMLsSame");

  private static final Path XMLSAMESECOND = RESOURCES.resolve("revXMLsSame1");

  private static final Path XMLALLFIRST = RESOURCES.resolve("revXMLsAll");

  private static final Path XMLALLSECOND = RESOURCES.resolve("revXMLsAll1");

  private static final Path XMLALLTHIRD = RESOURCES.resolve("revXMLsAll2");

  private static final Path XMLALLFOURTH = RESOURCES.resolve("revXMLsAll3");

  private static final Path XMLALLFIFTH = RESOURCES.resolve("revXMLsAll4");

  private static final Path XMLALLSIXTH = RESOURCES.resolve("revXMLsAll5");

  private static final Path XMLALLSEVENTH = RESOURCES.resolve("revXMLsAll6");

  private static final Path XMLALLEIGHTH = RESOURCES.resolve("revXMLsAll7");

  private static final Path XMLALLNINETH = RESOURCES.resolve("revXMLsAll8");

  private static final Path XMLALLTENTH = RESOURCES.resolve("revXMLsAll9");

  private static final Path XMLALLELEVENTH = RESOURCES.resolve("revXMLsAll10");

  private static final Path XMLLINGUISTICS = RESOURCES.resolve("linguistics");

  static {
    XMLUnit.setIgnoreComments(true);
    XMLUnit.setIgnoreWhitespace(true);
  }

  @Override
  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
  }

  @Override
  @After
  public void tearDown() throws SirixException, IOException {
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testAllFirst() throws Exception {
    test(XMLALLFIRST);
  }

  @Test
  public void testAllSecond() throws Exception {
    test(XMLALLSECOND);
  }

  @Test
  public void testAllThird() throws Exception {
    test(XMLALLTHIRD);
  }

  @Test
  public void testAllFourth() throws Exception {
    test(XMLALLFOURTH);
  }

  @Test
  public void testAllFifth() throws Exception {
    test(XMLALLFIFTH);
  }

  @Test
  public void testAllSixth() throws Exception {
    test(XMLALLSIXTH);
  }

  @Test
  public void testAllSeventh() throws Exception {
    test(XMLALLSEVENTH);
  }

  @Test
  public void testAllEigth() throws Exception {
    test(XMLALLEIGHTH);
  }

  @Test
  public void testAllNineth() throws Exception {
    test(XMLALLNINETH);
  }

  @Test
  public void testAllTenth() throws Exception {
    test(XMLALLTENTH);
  }

  @Test
  public void testAllEleventh() throws Exception {
    test(XMLALLELEVENTH);
  }

  @Test
  public void testDeleteFirst() throws Exception {
    test(XMLDELETEFIRST);
  }

  @Test
  public void testDeleteSecond() throws Exception {
    test(XMLDELETESECOND);
  }

  @Test
  public void testDeleteThird() throws Exception {
    test(XMLDELETETHIRD);
  }

  @Test
  public void testDeleteFourth() throws Exception {
    test(XMLDELETEFOURTH);
  }

  @Test
  public void testSameFirst() throws Exception {
    test(XMLSAMEFIRST);
  }

  @Test
  public void testSameSecond() throws Exception {
    test(XMLSAMESECOND);
  }

  @Test
  public void testInsertFirst() throws Exception {
    test(XMLINSERTFIRST);
  }

  @Test
  public void testInsertSecond() throws Exception {
    test(XMLINSERTSECOND);
  }

  @Test
  public void testInsertThird() throws Exception {
    test(XMLINSERTTHIRD);
  }

  @Test
  public void testLinguistics() throws Exception {
    test(XMLLINGUISTICS);
  }

  /**
   * Test a folder of XML files.
   *
   * @param folder path to the files
   * @throws Exception if any exception occurs
   */
  private void test(final Path folder) throws Exception {
    try (var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      XdmResourceManager resource = database.getResourceManager(XdmTestHelper.RESOURCE);
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
            try (final XdmNodeTrx wtx = resource.beginNodeTrx();
                final FileInputStream fis = new FileInputStream(file.toFile())) {
              final XMLShredder shredder = new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis),
                  Insert.ASFIRSTCHILD).commitAfterwards().build();
              shredder.call();
            }
          } else {
            FMSEImport.main(
                new String[] {PATHS.PATH1.getFile().toAbsolutePath().toString(), file.toAbsolutePath().toString()});
          }

          resource.close();
          resource = database.getResourceManager(XdmTestHelper.RESOURCE);

          final OutputStream out = new ByteArrayOutputStream();
          final XMLSerializer serializer = new XMLSerializerBuilder(resource, out).build();
          serializer.call();
          final StringBuilder sBuilder = XdmTestHelper.readFile(file, false);

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
  }
}

/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.shredder;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
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
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;

/**
 * Test XMLUpdateShredder.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class XMLUpdateShredderTest extends XMLTestCase {
  private static final Path RESOURCES = Paths.get("src", "test", "resources");

  private static final Path XMLINSERTFIRST = RESOURCES.resolve("revXMLsInsert");

  private static final Path XMLINSERTSECOND = RESOURCES.resolve("revXMLsInsert1");

  private static final Path XMLINSERTTHIRD = RESOURCES.resolve("revXMLsInsert2");

  private static final Path XMLDELETEFIRST = RESOURCES.resolve("revXMLsDelete");

  private static final Path XMLDELETESECOND = RESOURCES.resolve("revXMLsDelete1");

  private static final Path XMLDELETETHIRD = RESOURCES.resolve("revXMLsDelete2");

  private static final Path XMLDELETEFOURTH = RESOURCES.resolve("revXMLsDelete3");

  private static final Path XMLSAME = RESOURCES.resolve("revXMLsSame");

  private static final Path XMLALLSECOND = RESOURCES.resolve("revXMLsAll1");

  private static final Path XMLALLFOURTH = RESOURCES.resolve("revXMLsAll3");

  private static final Path XMLALLFIFTH = RESOURCES.resolve("revXMLsAll4");

  private static final Path XMLALLSEVENTH = RESOURCES.resolve("revXMLsAll6");

  // private static final String XMLLINGUISTICS = RESOURCES + File.separator +
  // "linguistics";

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
  public void tearDown() throws SirixException {
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testSame() throws Exception {
    test(XMLSAME);
  }

  @Test
  public void testInsertsFirst() throws Exception {
    test(XMLINSERTFIRST);
  }

  @Test
  public void testInsertsSecond() throws Exception {
    test(XMLINSERTSECOND);
  }

  @Test
  public void testInsertsThird() throws Exception {
    test(XMLINSERTTHIRD);
  }

  @Test
  public void testDeletesFirst() throws Exception {
    test(XMLDELETEFIRST);
  }

  @Test
  public void testDeletesSecond() throws Exception {
    test(XMLDELETESECOND);
  }

  @Test
  public void testDeletesThird() throws Exception {
    test(XMLDELETETHIRD);
  }

  @Test
  public void testDeletesFourth() throws Exception {
    test(XMLDELETEFOURTH);
  }

  // @Test
  // public void testAllFirst() throws Exception {
  // test(XMLALLFIRST);
  // }

  @Test
  public void testAllSecond() throws Exception {
    test(XMLALLSECOND);
  }

  // /** Not working anymore due to text merging on deletes. */
  // @Ignore
  // @Test
  // public void testAllThird() throws Exception {
  // test(XMLALLTHIRD);
  // }

  @Test
  public void testAllFourth() throws Exception {
    test(XMLALLFOURTH);
  }

  @Test
  public void testAllFifth() throws Exception {
    test(XMLALLFIFTH);
  }

  // /** Not working anymore due to text merging on deletes. */
  // @Ignore
  // @Test
  // public void testAllSixth() throws Exception {
  // test(XMLALLSIXTH);
  // }

  @Test
  public void testAllSeventh() throws Exception {
    test(XMLALLSEVENTH);
  }

  // /** Not working anymore due to text merging on deletes. */
  // @Ignore
  // @Test
  // public void testAllEighth() throws Exception {
  // test(XMLALLEIGHTH);
  // }
  //
  // /** Not working anymore due to text merging on deletes. */
  // @Ignore
  // @Test
  // public void testAllNineth() throws Exception {
  // test(XMLALLNINETH);
  // }

  // @Test
  // public void testLinguistics() throws Exception {
  // test(XMLLINGUISTICS);
  // }

  private void test(final Path folder) throws Exception {
    final var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XdmTestHelper.RESOURCE, PATHS.PATH1.getConfig()).build());
    final XdmResourceManager manager = database.getResourceManager(XdmTestHelper.RESOURCE);
    int i = 2;
    final List<Path> files =
        Files.list(folder).filter(file -> file.getFileName().endsWith(".xml")).collect(Collectors.toList());

    // Sort files array according to file names.
    files.sort((first, second) -> {
      final String firstName =
          first.getFileName().toString().substring(0, second.getFileName().toString().indexOf('.'));
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
    for (final Path file : files) {
      if (file.getFileName().toString().endsWith(".xml")) {
        try (final XdmNodeTrx wtx = manager.beginNodeTrx();
            final FileInputStream fis = new FileInputStream(file.toFile())) {
          if (first) {
            final XMLShredder shredder =
                new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis), Insert.ASFIRSTCHILD).commitAfterwards()
                                                                                                    .build();
            shredder.call();
            first = false;
          } else {
            @SuppressWarnings("deprecation")
            final XMLUpdateShredder shredder = new XMLUpdateShredder(wtx, XMLShredder.createFileReader(fis),
                Insert.ASFIRSTCHILD, file, ShredderCommit.COMMIT);
            shredder.call();
          }
          assertEquals(i, wtx.getRevisionNumber());

          i++;

          final OutputStream out = new ByteArrayOutputStream();
          final XMLSerializer serializer = new XMLSerializerBuilder(manager, out).prettyPrint().build();
          serializer.call();
          final StringBuilder sBuilder = XdmTestHelper.readFile(file, false);

          final Diff diff = new Diff(sBuilder.toString(), out.toString());
          final DetailedDiff detDiff = new DetailedDiff(diff);
          @SuppressWarnings("unchecked")
          final List<Difference> differences = detDiff.getAllDifferences();
          for (final Difference difference : differences) {
            System.out.println("***********************");
            System.out.println(difference);
            System.out.println("***********************");
          }

          assertTrue("pieces of XML are similar " + diff, diff.similar());
          assertTrue("but are they identical? " + diff, diff.identical());
        }
      }
    }
  }
}

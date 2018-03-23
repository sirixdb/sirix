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

import static org.junit.Assert.assertEquals;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.StartElement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.shredder.WikipediaImport.DateBy;

/**
 * Test WikipediaImport.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class WikipediaImportTest {

  /** Wikipedia file. */
  public static final Path WIKIPEDIA = Paths.get("src", "test", "resources", "testWikipedia.xml");

  /** Wikipedia expected file. */
  public static final Path EXPECTED =
      Paths.get("src", "test", "resources", "testWikipediaExpected.xml");

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    TestHelper.closeEverything();
  }

  @Test
  public void testWikipediaImport() throws Exception {
    Databases.truncateDatabase(new DatabaseConfiguration(PATHS.PATH2.getFile()));

    // Create necessary element nodes.
    final String NSP_URI = "";
    final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
    final StartElement timestamp = eventFactory.createStartElement(
        new QName(NSP_URI, "timestamp", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement page = eventFactory.createStartElement(
        new QName(NSP_URI, "page", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement rev = eventFactory.createStartElement(
        new QName(NSP_URI, "revision", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement id = eventFactory.createStartElement(
        new QName(NSP_URI, "id", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement text = eventFactory.createStartElement(
        new QName(NSP_URI, "text", XMLConstants.DEFAULT_NS_PREFIX), null, null);

    // Create list.
    final List<StartElement> list = new LinkedList<StartElement>();
    list.add(timestamp);
    list.add(page);
    list.add(rev);
    list.add(id);
    list.add(text);

    // Invoke import.
    new WikipediaImport(WIKIPEDIA, PATHS.PATH2.getFile()).importData(DateBy.HOURS, list);
    XMLSerializer.main(
        PATHS.PATH2.getFile().toAbsolutePath().toString(),
        PATHS.PATH3.getFile().toAbsolutePath().toString());

    final StringBuilder actual = TestHelper.readFile(PATHS.PATH3.getFile().toAbsolutePath(), false);
    final StringBuilder expected = TestHelper.readFile(EXPECTED, false);
    assertEquals("XML files match", expected.toString(), actual.toString());
  }
}

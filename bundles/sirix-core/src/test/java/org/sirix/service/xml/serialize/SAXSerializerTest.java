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

package org.sirix.service.xml.serialize;

import org.custommonkey.xmlunit.XMLTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.utils.XmlDocumentCreator;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

import java.io.IOException;

/**
 * Test SAXSerializer.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class SAXSerializerTest extends XMLTestCase {
  private Holder holder;

  @Override
  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @Override
  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testSAXSerializer() throws SirixException, SAXException, IOException {

    final StringBuilder sbuf = new StringBuilder();
    final ContentHandler contHandler = new XMLFilterImpl() {

      @Override
      public void startDocument() {
        sbuf.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
      }

      @Override
      public void startElement(final String uri, final String localName, final String qName,
          final Attributes atts) throws SAXException {
        sbuf.append("<" + qName);

        for (int i = 0; i < atts.getLength(); i++) {
          sbuf.append(" " + atts.getQName(i));
          sbuf.append("=\"" + atts.getValue(i) + "\"");
        }

        sbuf.append(">");
      }

      // @Override
      // public void startPrefixMapping(final String prefix, final String uri) throws SAXException {
      // strBuilder.append(" " + prefix + "=\"" + uri + "\"");
      // };

      @Override
      public void endElement(final String uri, final String localName, final String qName)
          throws SAXException {
        sbuf.append("</" + qName + ">");
      }

      @Override
      public void characters(final char[] ch, final int start, final int length)
          throws SAXException {
        for (int i = start; i < start + length; i++) {
          sbuf.append(ch[i]);
        }
      }
    };

    final SAXSerializer serializer = new SAXSerializer(holder.getResourceManager(), contHandler,
        holder.getResourceManager().getMostRecentRevisionNumber());
    serializer.call();
    assertXMLEqual(XmlDocumentCreator.XML, sbuf.toString());
  }
}

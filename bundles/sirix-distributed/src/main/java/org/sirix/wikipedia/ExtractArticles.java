/**
 * Copyright (c) 2010, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */
package org.sirix.wikipedia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;

import org.apache.xerces.parsers.SAXParser;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.sirix.utils.LogWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;


/**
 * <h1>ExtractArticles</h1>
 * 
 * <p>
 * Provides a SAX based mechanism to extract the first N Wikipedia articles found in the XML document. The
 * threshold value N can be set by the user or defaults to 10.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class ExtractArticles extends XMLFilterImpl {

  /**
   * Logger for determining the log level.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ExtractArticles.class);

  /**
   * Log wrapper for better output.
   */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LOGGER);

  /** Number of articles to extract. */
  private transient int mArtNr = 50;

  /** Articles parsed so far. */
  private transient int mArtParsed;

  /** Root node of wikipedia dump file. */
  private static final QName WIKIROOT = new QName(XMLConstants.NULL_NS_URI, "mediawiki",
    XMLConstants.DEFAULT_NS_PREFIX);

  /** System independent path separator. */
  private static final String SEP = System.getProperty("file.separator");

  /** File to write SAX output to. */
  private static final File TARGET = new File("target" + SEP + "wikipedia.xml");

  /** Start of computation in milliseconds. */
  private static long start;

  /**
   * Constructor.
   * 
   * @param parent
   *          an XMLReader instance {@link XMLReader}
   */
  public ExtractArticles(final XMLReader parent) {
    super(parent);
  }

  /**
   * Constructor.
   * 
   * @param paramArtNr
   *          Number of articles to extract
   */
  public ExtractArticles(final int paramArtNr) {
    mArtNr = paramArtNr;
  }

  @Override
  public void startElement(final String paramURI, final String paramLocalName, final String paramQName,
    final Attributes paramAtts) throws SAXException {

    if (paramQName.equalsIgnoreCase("page")) {
      mArtParsed++;
    }

    if (mArtParsed <= mArtNr) {
      super.startElement(paramURI, paramLocalName, paramQName, paramAtts);
    } else {
      // After mArtNr articles have been parsed close root element / end document and exit.
      super.endElement(WIKIROOT.getNamespaceURI(), WIKIROOT.getLocalPart(), WIKIROOT.getPrefix() + ":"
        + WIKIROOT.getLocalPart());
      super.endDocument();
      System.out.println("done in " + (System.nanoTime() - start) / 1_000_000_000 + " seconds.");
      System.exit(0);
    }
  }

  /**
   * Main method.
   * 
   * @param paramArgs
   *          First param specifies the Wikipedia dump to parse.
   */
  public static void main(final String[] paramArgs) {
    if (paramArgs.length != 1) {
      new IllegalStateException("First param must be the wikipedia dump!");
    }

    start = System.nanoTime();

    System.out.print("Start extracting articles... ");

    final String wikiDump = new File(paramArgs[0]).getAbsolutePath();
    final XMLReader parser = new ExtractArticles(new SAXParser());

    if (parser != null) {
      try {
        TARGET.delete();
        TARGET.createNewFile();
        final XMLSerializer printer = new XMLSerializer(new FileWriter(TARGET), new OutputFormat());
        parser.setContentHandler(printer);
        parser.parse(wikiDump);
      } catch (final IOException | SAXException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }
}

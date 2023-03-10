/*
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

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Axis;
import org.sirix.api.Database;
import org.sirix.api.NodeTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.diff.algorithm.fmse.DefaultNodeComparisonFactory;
import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.service.InsertPosition;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Import sorted Wikipedia revisions. Precondition is a file, which is produced from a Hadoop job.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class WikipediaImport implements Import<StartElement> {

  /**
   * {@link LogWrapper} reference.
   */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(WikipediaImport.class));

  /**
   * StAX parser {@link XMLEventReader}.
   */
  private transient XMLEventReader mReader;

  /**
   * Resource manager instance.
   */
  private final XmlResourceSession mResourceManager;

  /**
   * sirix {@link NodeTrx}.
   */
  private transient XmlNodeTrx mWtx;

  /**
   * {@link XMLEvent}s which specify the page metadata.
   */
  private transient Deque<XMLEvent> mPageEvents;

  /**
   * Determines if page has been found in sirix storage.
   */
  private transient boolean mFound;

  /**
   * String value of text-element.
   */
  private transient String mIdText;

  /**
   * Determines if StAX parser is currently parsing revision metadata.
   */
  private transient boolean mIsRev;

  /**
   * Timestamp of each revision as a simple String.
   */
  private transient String mTimestamp;

  /**
   * Sirix {@link Database}.
   */
  private final Database<XmlResourceSession> mDatabase;

  /**
   * Revision-timespan by date.
   */
  public enum DateBy {
    /**
     * By seconds.
     */
    SECONDS,

    /**
     * By minutes.
     */
    MINUTES,

    /**
     * By hours.
     */
    HOURS,

    /**
     * By days.
     */
    DAYS,

    /**
     * By months.
     */
    MONTHS
  }

  /**
   * Constructor.
   *
   * @param xmlFile       The XML file to import.
   * @param sirixDatabase The sirix destination storage directory.
   */
  public WikipediaImport(final Path xmlFile, final Path sirixDatabase) throws SirixException {
    mPageEvents = new ArrayDeque<>();
    final XMLInputFactory xmlif = XMLInputFactory.newInstance();
    xmlif.setProperty(XMLInputFactory.SUPPORT_DTD, false);

    try {
      mReader = xmlif.createXMLEventReader(new FileInputStream(requireNonNull(xmlFile.toFile())));
    } catch (XMLStreamException | FileNotFoundException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    final DatabaseConfiguration config = new DatabaseConfiguration(sirixDatabase);
    Databases.createXmlDatabase(config);
    mDatabase = Databases.openXmlDatabase(sirixDatabase);
    mDatabase.createResource(new ResourceConfiguration.Builder("shredded").build());
    mResourceManager = mDatabase.beginResourceSession("shredded");
    mWtx = mResourceManager.beginNodeTrx();
  }

  /**
   * Import data.
   *
   * @param dateRange <p>
   *                  Date range, the following values are possible:
   *                  </p>
   *                  <dl>
   *                  <dt>h</dt>
   *                  <dd>hourly revisions</dd>
   *                  <dt>d</dt>
   *                  <dd>daily revisions</dd>
   *                  <dt>w</dt>
   *                  <dd>weekly revisions (currently unsupported)</dd>
   *                  <dt>m</dt>
   *                  <dd>monthly revisions</dd>
   *                  </dl>
   * @param data      <p>
   *                  List of {@link StartElement}s with the following meaning:
   *                  </p>
   *                  <dl>
   *                  <dt>Zero index</dt>
   *                  <dd>Timestamp start tag {@link StartElement}.</dd>
   *                  <dt>First index</dt>
   *                  <dd>Page start tag {@link StartElement}.</dd>
   *                  <dt>Second index</dt>
   *                  <dd>Revision start tag {@link StartElement}.</dd>
   *                  <dt>Third index</dt>
   *                  <dd>Page-ID start tag {@link StartElement}.</dd>
   *                  <dt>Fourth index</dt>
   *                  <dd>Revision text start tag {@link StartElement}.</dd>
   *                  </dl>
   */
  @Override
  public void importData(final DateBy dateRange, final List<StartElement> data) {
    // Some checks.
    requireNonNull(dateRange);
    requireNonNull(data);
    checkArgument(data.size() == 5, "data must have 5 elements!");

    final StartElement timestamp = data.get(0);
    final StartElement page = data.get(1);
    final StartElement rev = data.get(2);
    final StartElement id = data.get(3);
    // final StartElement text = pData.get(4);

    // Initialize variables.
    mFound = false;
    mIsRev = false;
    boolean isFirst = true;

    try {
      while (mReader.hasNext()) {
        final XMLEvent event = mReader.nextEvent();

        if (isWhitespace(event)) {
          continue;
        }

        // Add event to page or revision metadata list if it's not a
        // whitespace.
        mPageEvents.add(event);

        switch (event.getEventType()) {
          case XMLStreamConstants.START_ELEMENT:
            if (checkStAXStartElement(event.asStartElement(), rev)) {
              // StAX parser in rev metadata.
              isFirst = false;
              mIsRev = true;
            } else {
              parseStartTag(event, timestamp, page, rev, id, dateRange);
            }

            break;
          case XMLStreamConstants.END_ELEMENT:
            if (event.asEndElement().getName().equals(page.getName()) && !isFirst) {
              // StAX parser is located at the end of an article/page.
              mIsRev = false;

              if (mFound) {
                try {
                  updateShredder();
                } catch (final IOException e) {
                  LOGWRAPPER.error(e.getMessage(), e);
                }
              } else {
                // Move wtx to end of file and append page.
                mWtx.moveToDocumentRoot();
                final boolean hasFirstChild = mWtx.hasFirstChild();
                if (hasFirstChild) {
                  moveToLastPage(page);
                  assert mWtx.getName().getLocalName().equals(page.getName().getLocalPart());
                }

                XmlShredder shredder = null;
                if (hasFirstChild) {
                  // Shredder as child.
                  shredder = new XmlShredder.Builder(mWtx,
                                                     XmlShredder.createQueueReader(mPageEvents),
                                                     InsertPosition.AS_RIGHT_SIBLING).build();
                } else {
                  // Shredder as right sibling.
                  shredder = new XmlShredder.Builder(mWtx,
                                                     XmlShredder.createQueueReader(mPageEvents),
                                                     InsertPosition.AS_FIRST_CHILD).build();
                }

                shredder.call();
                mPageEvents = new ArrayDeque<>();
              }
            }
            break;
          default:
        }
      }

      mWtx.commit();
      mWtx.close();
      mResourceManager.close();
      mDatabase.close();
    } catch (final XMLStreamException | SirixException | IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Update shredder.
   */
  private void updateShredder() throws SirixException, IOException, XMLStreamException {
    final Path path = Files.createTempDirectory("sdbtmp");
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(path);
    Databases.createXmlDatabase(dbConf);
    try (var db = Databases.openXmlDatabase(path)) {
      db.createResource(new ResourceConfiguration.Builder("wiki").build());
      try (XmlResourceSession resourceManager = db.beginResourceSession("wiki")) {
        if (mPageEvents.peek() != null && mPageEvents.peek().isStartElement() && !mPageEvents.peek()
                                                                                             .asStartElement()
                                                                                             .getName()
                                                                                             .getLocalPart()
                                                                                             .equals("root")) {
          mPageEvents.addFirst(XMLEventFactory.newInstance().createStartElement(new QName("root"), null, null));
          mPageEvents.addLast(XMLEventFactory.newInstance().createEndElement(new QName("root"), null));
        }
        final XmlNodeTrx wtx = resourceManager.beginNodeTrx();
        final XmlShredder shredder = new XmlShredder.Builder(wtx,
                                                             XmlShredder.createQueueReader(mPageEvents),
                                                             InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
        wtx.close();
        mPageEvents = new ArrayDeque<>();
        final XmlNodeReadOnlyTrx rtx = resourceManager.beginNodeReadOnlyTrx();
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        final long nodeKey = mWtx.getNodeKey();
        try (final FMSE fmse = FMSE.createInstance(new DefaultNodeComparisonFactory())) {
          fmse.diff(mWtx, rtx);
        }
        mWtx.moveTo(nodeKey);
        rtx.close();
      }
    }
    Databases.removeDatabase(path);
  }

  /**
   * Parses a start tag.
   *
   * @param startTagEvent current StAX {@link XMLEvent}
   * @param timestamp     timestamp start tag {@link StartElement}
   * @param wikiPage      wikipedia page start tag {@link StartElement}
   * @param revision      revision start tag {@link StartElement}
   * @param pageID        page-ID start tag {@link StartElement}
   * @param dateRange     date range, the following values are possible:
   *                      <dl>
   *                      <dt>h</dt>
   *                      <dd>hourly revisions</dd>
   *                      <dt>d</dt>
   *                      <dd>daily revisions</dd>
   *                      <dt>w</dt>
   *                      <dd>weekly revisions (currently unsupported)</dd>
   *                      <dt>m</dt>
   *                      <dd>monthly revisions</dd>
   *                      </dl>
   * @throws XMLStreamException In case of any XML parsing errors.
   * @throws SirixException     In case of any sirix errors.
   */
  private void parseStartTag(final XMLEvent startTagEvent, final StartElement timestamp, final StartElement wikiPage,
      final StartElement revision, final StartElement pageID, final DateBy dateRange)
      throws XMLStreamException, SirixException {
    XMLEvent event = startTagEvent;

    if (checkStAXStartElement(event.asStartElement(), pageID)) {
      event = mReader.nextEvent();
      mPageEvents.add(event);

      if (!mIsRev) {
        mIdText = event.asCharacters().getData();
      }
    } else if (checkStAXStartElement(event.asStartElement(), timestamp)) {
      // Timestamp start tag found.
      event = mReader.nextEvent();
      mPageEvents.add(event);

      final String currTimestamp = event.asCharacters().getData();

      // Timestamp.
      if (mTimestamp == null) {
        mTimestamp = parseTimestamp(dateRange, currTimestamp);
      } else if (!parseTimestamp(dateRange, currTimestamp).equals(mTimestamp)) {
        mTimestamp = parseTimestamp(dateRange, currTimestamp);
        mWtx.commit();
        mWtx.close();
        mWtx = mResourceManager.beginNodeTrx();
      }

      assert mIdText != null;

      // Search for existing page.
      final QName page = wikiPage.getName();
      final QName id = pageID.getName();
      final String query = "//" + qNameToString(page) + "[fn:string(" + qNameToString(id) + ") = '" + mIdText + "']";
      mWtx.moveToDocumentRoot();
      final Axis axis = new XPathAxis(mWtx, query);

      mFound = false; // Determines if page is found in shreddered file.
      int resCounter = 0; // Counts found page.
      long key = mWtx.getNodeKey();
      while (axis.hasNext()) {
        axis.next();

        // Page is found.
        mFound = true;

        // Make sure no more than one page with a unique id is found.
        resCounter++;
        assert resCounter == 1;

        // Make sure the transaction is on the page element found.
        assert mWtx.getName().getLocalName().equals(wikiPage.getName().getLocalPart());
        key = mWtx.getNodeKey();
      }
      mWtx.moveTo(key);
    }
  }

  /**
   * Parses a given Timestamp-String to extract time interval which is used (simple String value to
   * improve performance).
   *
   * @param dateRange date Range which is used for revisioning
   * @param timestamp the timestamp to parse
   * @return parsed and truncated String
   */
  private String parseTimestamp(final DateBy dateRange, final String timestamp) {
    final StringBuilder sb = new StringBuilder();

    switch (dateRange) {
      case SECONDS:
        sb.append(timestamp);
        break;
      case HOURS:
        final String[] splittedHour = timestamp.split(":");
        sb.append(splittedHour[0]);
        sb.append(":");
        sb.append(splittedHour[1]);
        break;
      case DAYS:
        final String[] splittedDay = timestamp.split("T");
        sb.append(splittedDay[0]);
        break;
      case MINUTES:
        throw new UnsupportedOperationException("Not supported right now!");
      case MONTHS:
        final String[] splittedMonth = timestamp.split("-");
        sb.append(splittedMonth[0]);
        sb.append("-");
        sb.append(splittedMonth[1]);
        break;
      default:
        throw new IllegalStateException("Date range not known!");
    }

    return sb.toString();
  }

  /**
   * Determines if the current event is a whitespace event.
   *
   * @param event {@link XMLEvent} to check.
   * @return true if it is whitespace, otherwise false.
   */
  private boolean isWhitespace(final XMLEvent event) {
    return event.isCharacters() && event.asCharacters().isWhiteSpace();
  }

  /**
   * Get prefix:localname or localname String representation of a qName.
   *
   * @param name the full qualified name
   * @return string representation
   */
  private static String qNameToString(final QName name) {
    final StringBuilder retVal = new StringBuilder();
    if ("".equals(name.getPrefix())) {
      retVal.append(name.getLocalPart());
    } else {
      retVal.append(name.getPrefix()).append(":").append(name.getLocalPart());
    }
    return retVal.toString();
  }

  /**
   * Moves {@link NodeTrx} to last shreddered article/page.
   *
   * @param page {@link StartElement} page
   */
  private void moveToLastPage(final StartElement page) {
    assert page != null;
    // All subsequent shredders, move cursor to the end.
    mWtx.moveToFirstChild();
    mWtx.moveToFirstChild();

    assert mWtx.getKind() == NodeKind.ELEMENT;
    assert mWtx.getName().getLocalName().equals(page.getName().getLocalPart());
    while (mWtx.hasRightSibling()) {
      mWtx.moveToRightSibling();
    }
    assert mWtx.getKind() == NodeKind.ELEMENT;
    assert mWtx.getName().getLocalName().equals(page.getName().getLocalPart());
  }

  /**
   * Check if start element of two StAX parsers match.
   *
   * @param startTag StartTag of the StAX parser, where it is currently (the "real" StAX parser over
   *                 the whole document)
   * @param elem     StartTag to check against
   * @return {@code true} if start elements match, {@code false} otherwise
   * @throws XMLStreamException handling XML Stream Exception
   */
  private static boolean checkStAXStartElement(final StartElement startTag, final StartElement elem)
      throws XMLStreamException {
    assert startTag != null && elem != null;
    boolean retVal = false;
    if (startTag.getEventType() == XMLStreamConstants.START_ELEMENT && startTag.getName().equals(elem.getName())) {
      // Check attributes.
      boolean foundAtts = false;
      boolean hasAtts = false;
      for (final Iterator<?> itStartTag = startTag.getAttributes(); itStartTag.hasNext(); ) {
        hasAtts = true;
        final Attribute attStartTag = (Attribute) itStartTag.next();
        for (final Iterator<?> itElem = elem.getAttributes(); itElem.hasNext(); ) {
          final Attribute attElem = (Attribute) itElem.next();
          if (attStartTag.getName().equals(attElem.getName())) {
            foundAtts = true;
            break;
          }
        }

        if (!foundAtts) {
          break;
        }
      }
      if (!hasAtts) {
        foundAtts = true;
      }

      // Check namespaces.
      boolean foundNamesps = false;
      boolean hasNamesps = false;
      for (final Iterator<?> itStartTag = startTag.getNamespaces(); itStartTag.hasNext(); ) {
        hasNamesps = true;
        final Namespace nsStartTag = (Namespace) itStartTag.next();
        for (final Iterator<?> itElem = elem.getNamespaces(); itElem.hasNext(); ) {
          final Namespace nsElem = (Namespace) itElem.next();
          if (nsStartTag.getName().equals(nsElem.getName())) {
            foundNamesps = true;
            break;
          }
        }

        if (!foundNamesps) {
          break;
        }
      }
      if (!hasNamesps) {
        foundNamesps = true;
      }

      // Check if qname, atts and namespaces are the same.
      if (foundAtts && foundNamesps) {
        retVal = true;
      } else {
        retVal = false;
      }
    }
    return retVal;
  }

  /**
   * Main method.
   *
   * @param args Arguments (path to xml-file /.
   * @throws SirixException if anything within sirix fails
   */
  public static void main(final String[] args) throws SirixException {
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: WikipediaImport path/to/xmlFile path/to/SirixStorage");
    }

    LOGWRAPPER.info("Importing wikipedia...");
    final long start = System.nanoTime();

    final Path xml = Paths.get(args[0]);
    final Path resource = Paths.get(args[1]);
    Databases.removeDatabase(resource);

    // Create necessary element nodes.
    final String NSP_URI = "http://www.mediawiki.org/xml/export-0.5/";
    final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
    final StartElement timestamp =
        eventFactory.createStartElement(new QName(NSP_URI, "timestamp", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement page =
        eventFactory.createStartElement(new QName(NSP_URI, "page", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement rev =
        eventFactory.createStartElement(new QName(NSP_URI, "revision", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement id =
        eventFactory.createStartElement(new QName(NSP_URI, "id", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement text =
        eventFactory.createStartElement(new QName(NSP_URI, "text", XMLConstants.DEFAULT_NS_PREFIX), null, null);

    // Create list.
    final List<StartElement> list = List.of(timestamp, page, rev, id, text);

    // Invoke import.
    new WikipediaImport(xml, resource).importData(DateBy.HOURS, list);

    LOGWRAPPER.info(" done in " + (System.nanoTime() - start) / 1_000_000_000 + "[s].");
  }
}

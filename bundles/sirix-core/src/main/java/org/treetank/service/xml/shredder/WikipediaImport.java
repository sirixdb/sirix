/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.treetank.service.xml.shredder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.LoggerFactory;
import org.treetank.access.Database;
import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IAxis;
import org.treetank.api.IDatabase;
import org.treetank.api.INodeReadTrx;
import org.treetank.api.ISession;
import org.treetank.api.INodeWriteTrx;
import org.treetank.diff.algorithm.fmse.FMSE;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.ElementNode;
import org.treetank.service.xml.xpath.XPathAxis;
import org.treetank.utils.LogWrapper;

/**
 * <h1>WikipediaImport</h1>
 * 
 * <p>
 * Import sorted Wikipedia revisions. Precondition is a file, which is produced from a Hadoop job.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class WikipediaImport implements IImport<StartElement> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(WikipediaImport.class));

  /** Temporal path to shredder the events into a temporal database. */
  private static final Path TMP_PATH = Paths.get("target", "tnk", "tmp");

  /** StAX parser {@link XMLEventReader}. */
  private transient XMLEventReader mReader;

  /** Treetank {@link ISession}. */
  private final ISession mSession;

  /** Treetank {@link INodeWriteTrx}. */
  private transient INodeWriteTrx mWtx;

  /** {@link XMLEvent}s which specify the page metadata. */
  private transient Deque<XMLEvent> mPageEvents;

  /** Determines if page has been found in Treetank storage. */
  private transient boolean mFound;

  /** String value of text-element. */
  private transient String mIdText;

  /** Determines if StAX parser is currently parsing revision metadata. */
  private transient boolean mIsRev;

  /** Timestamp of each revision as a simple String. */
  private transient String mTimestamp;

  /** Treetank {@link IDatabase}. */
  private final IDatabase mDatabase;

  public enum EDateBy {
    SECONDS,

    MINUTES,

    HOURS,

    DAYS,

    MONTHS
  }

  /**
   * Constructor.
   * 
   * @param pXMLFile
   *          The XML file to import.
   * @param pTTDir
   *          The Treetank destination storage directory.
   * 
   */
  public WikipediaImport(final File pXMLFile, final File pTTDir) throws AbsTTException {
    mPageEvents = new ArrayDeque<>();
    final XMLInputFactory xmlif = XMLInputFactory.newInstance();
    try {
      mReader = xmlif.createXMLEventReader(new FileInputStream(checkNotNull(pXMLFile)));
    } catch (XMLStreamException | FileNotFoundException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    final DatabaseConfiguration config = new DatabaseConfiguration(pTTDir);
    Database.createDatabase(config);
    mDatabase = Database.openDatabase(pTTDir);
    mDatabase.createResource(new ResourceConfiguration.Builder("shredded", config).useCompression(false)
      .build());
    mSession = mDatabase.getSession(new SessionConfiguration.Builder("shredded").build());
    mWtx = mSession.beginNodeWriteTrx();
  }

  /**
   * Import data.
   * 
   * @param pDateRange
   *          <p>
   *          Date range, the following values are possible:
   *          </p>
   *          <dl>
   *          <dt>h</dt>
   *          <dd>hourly revisions</dd>
   *          <dt>d</dt>
   *          <dd>daily revisions</dd>
   *          <dt>w</dt>
   *          <dd>weekly revisions (currently unsupported)</dd>
   *          <dt>m</dt>
   *          <dd>monthly revisions</dd>
   *          </dl>
   * 
   * @param pData
   *          <p>
   *          List of {@link StartElement}s with the following meaning:
   *          </p>
   *          <dl>
   *          <dt>Zero index</dt>
   *          <dd>Timestamp start tag {@link StartElement}.</dd>
   *          <dt>First index</dt>
   *          <dd>Page start tag {@link StartElement}.</dd>
   *          <dt>Second index</dt>
   *          <dd>Revision start tag {@link StartElement}.</dd>
   *          <dt>Third index</dt>
   *          <dd>Page-ID start tag {@link StartElement}.</dd>
   *          <dt>Fourth index</dt>
   *          <dd>Revision text start tag {@link StartElement}.</dd>
   *          </dl>
   */
  @Override
  public void importData(final EDateBy pDateRange, final List<StartElement> pData) {
    // Some checks.
    checkNotNull(pDateRange);
    checkNotNull(pData);
    checkArgument(pData.size() == 5, "paramData must have 5 elements!");

    final StartElement timestamp = pData.get(0);
    final StartElement page = pData.get(1);
    final StartElement rev = pData.get(2);
    final StartElement id = pData.get(3);
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
            parseStartTag(event, timestamp, page, rev, id, pDateRange);
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
              final boolean hasFirstChild = mWtx.getStructuralNode().hasFirstChild();
              if (hasFirstChild) {
                moveToLastPage(page);
                assert mWtx.getQNameOfCurrentNode().equals(page.getName());
              }

              XMLShredder shredder = null;
              if (hasFirstChild) {
                // Shredder as child.
                shredder =
                  new XMLShredder(mWtx, XMLShredder.createQueueReader(mPageEvents), EInsert.ASRIGHTSIBLING,
                    EShredderCommit.NOCOMMIT);
              } else {
                // Shredder as right sibling.
                shredder =
                  new XMLShredder(mWtx, XMLShredder.createQueueReader(mPageEvents), EInsert.ASFIRSTCHILD,
                    EShredderCommit.NOCOMMIT);
              }

              shredder.call();
              mPageEvents.clear();
            }
          }
          break;
        default:
        }
      }

      mWtx.commit();
      mWtx.close();
      mSession.close();
      mDatabase.close();
    } catch (final XMLStreamException | AbsTTException | IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /** Update shredder. */
  private void updateShredder() throws AbsTTException, IOException, XMLStreamException {
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(TMP_PATH.toFile());
    Database.truncateDatabase(dbConf);
    Database.createDatabase(dbConf);
    final IDatabase db = Database.openDatabase(TMP_PATH.toFile());
    db.createResource(new ResourceConfiguration.Builder("wiki", dbConf).useCompression(false).build());
    final ISession session = db.getSession(new SessionConfiguration.Builder("wiki").build());
    if (mPageEvents.peek().isStartElement()
      && !mPageEvents.peek().asStartElement().getName().getLocalPart().equals("root")) {
      mPageEvents.addFirst(XMLEventFactory.newInstance().createStartElement(new QName("root"), null, null));
      mPageEvents.addLast(XMLEventFactory.newInstance().createEndElement(new QName("root"), null));
    }
    final INodeWriteTrx wtx = session.beginNodeWriteTrx();
    final XMLShredder shredder =
      new XMLShredder(wtx, XMLShredder.createQueueReader(mPageEvents), EInsert.ASFIRSTCHILD);
    shredder.call();
    wtx.close();
    mPageEvents = new ArrayDeque<>();
    final INodeReadTrx rtx = session.beginNodeReadTrx();
    rtx.moveToFirstChild();
    rtx.moveToFirstChild();
    final long nodeKey = mWtx.getNode().getNodeKey();
    final FMSE fmse = new FMSE();
    fmse.diff(mWtx, rtx);
    mWtx.moveTo(nodeKey);
    rtx.close();
    session.close();
    db.close();
  }

  /**
   * Parses a start tag.
   * 
   * @param pEvent
   *          Current StAX {@link XMLEvent}.
   * @param pTimestamp
   *          Timestamp start tag {@link StartElement}.
   * @param pPage
   *          Page start tag {@link StartElement}.
   * @param pRev
   *          Revision start tag {@link StartElement}.
   * @param pID
   *          Page-ID start tag {@link StartElement}.
   * @param pDateRange
   *          Date range, the following values are possible:
   *          <dl>
   *          <dt>h</dt>
   *          <dd>hourly revisions</dd>
   *          <dt>d</dt>
   *          <dd>daily revisions</dd>
   *          <dt>w</dt>
   *          <dd>weekly revisions (currently unsupported)</dd>
   *          <dt>m</dt>
   *          <dd>monthly revisions</dd>
   *          </dl>
   * @throws XMLStreamException
   *           In case of any XML parsing errors.
   * @throws AbsTTException
   *           In case of any Treetank errors.
   */
  private void parseStartTag(final XMLEvent pEvent, final StartElement pTimestamp, final StartElement pPage,
    final StartElement pRev, final StartElement pID, final EDateBy pDateRange) throws XMLStreamException,
    AbsTTException {
    XMLEvent event = pEvent;

    if (checkStAXStartElement(event.asStartElement(), pID)) {
      event = mReader.nextEvent();
      mPageEvents.add(event);

      if (!mIsRev) {
        mIdText = event.asCharacters().getData();
      }
    } else if (checkStAXStartElement(event.asStartElement(), pTimestamp)) {
      // Timestamp start tag found.
      event = mReader.nextEvent();
      mPageEvents.add(event);

      final String currTimestamp = event.asCharacters().getData();

      // Timestamp.
      if (mTimestamp == null) {
        mTimestamp = parseTimestamp(pDateRange, currTimestamp);
      } else if (!parseTimestamp(pDateRange, currTimestamp).equals(mTimestamp)) {
        mTimestamp = parseTimestamp(pDateRange, currTimestamp);
        mWtx.commit();
        mWtx.close();
        mWtx = mSession.beginNodeWriteTrx();
      }

      assert mIdText != null;

      // Search for existing page.
      final QName page = pPage.getName();
      final QName id = pID.getName();
      final String query =
        "//" + qNameToString(page) + "[fn:string(" + qNameToString(id) + ") = '" + mIdText + "']";
      mWtx.moveToDocumentRoot();
      final IAxis axis = new XPathAxis(mWtx, query);

      mFound = false; // Determines if page is found in shreddered file.
      int resCounter = 0; // Counts found page.
      long key = mWtx.getNode().getNodeKey();
      while (axis.hasNext()) {
        axis.next();

        // Page is found.
        mFound = true;

        // Make sure no more than one page with a unique id is found.
        resCounter++;
        assert resCounter == 1;

        // Make sure the transaction is on the page element found.
        assert mWtx.getQNameOfCurrentNode().equals(pPage.getName());
        key = mWtx.getNode().getNodeKey();
      }
      mWtx.moveTo(key);
    }
  }

  /**
   * Parses a given Timestamp-String to extract time interval which is used
   * (simple String value to improve performance).
   * 
   * @param pDateRange
   *          date Range which is used for revisioning
   * @param pTimestamp
   *          the timestamp to parse
   * @return parsed and truncated String
   */
  private String parseTimestamp(final EDateBy pDateRange, final String pTimestamp) {
    final StringBuilder sb = new StringBuilder();

    switch (pDateRange) {
    case SECONDS:
      sb.append(pTimestamp);
      break;
    case HOURS:
      final String[] splittedHour = pTimestamp.split(":");
      sb.append(splittedHour[0]);
      sb.append(":");
      sb.append(splittedHour[1]);
      break;
    case DAYS:
      final String[] splittedDay = pTimestamp.split("T");
      sb.append(splittedDay[0]);
      break;
    case MINUTES:
      throw new UnsupportedOperationException("Not supported right now!");
    case MONTHS:
      final String[] splittedMonth = pTimestamp.split("-");
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
   * @param pEvent
   *          {@link XMLEvent} to check.
   * @return true if it is whitespace, otherwise false.
   */
  private boolean isWhitespace(final XMLEvent pEvent) {
    return pEvent.isCharacters() && pEvent.asCharacters().isWhiteSpace();
  }

  /**
   * Get prefix:localname or localname String representation of a qName.
   * 
   * @param pQName
   *          the full qualified name
   * @return string representation
   */
  private static String qNameToString(final QName pQName) {
    final StringBuilder retVal = new StringBuilder();
    if ("".equals(pQName.getPrefix())) {
      retVal.append(pQName.getLocalPart());
    } else {
      retVal.append(pQName.getPrefix()).append(":").append(pQName.getLocalPart());
    }
    return retVal.toString();
  }

  /**
   * Moves {@link INodeWriteTrx} to last shreddered article/page.
   * 
   * @param pPage
   *          {@link StartElement} page
   */
  private void moveToLastPage(final StartElement pPage) {
    assert pPage != null;
    // All subsequent shredders, move cursor to the end.
    mWtx.moveToFirstChild();
    mWtx.moveToFirstChild();

    assert mWtx.getNode().getKind() == ENode.ELEMENT_KIND;
    assert mWtx.getQNameOfCurrentNode().equals(pPage.getName());
    while (((ElementNode)mWtx.getNode()).hasRightSibling()) {
      mWtx.moveToRightSibling();
    }
    assert mWtx.getNode().getKind() == ENode.ELEMENT_KIND;
    assert mWtx.getQNameOfCurrentNode().equals(pPage.getName());
  }

  /**
   * Check if start element of two StAX parsers match.
   * 
   * @param mStartTag
   *          StartTag of the StAX parser, where it is currently (the "real"
   *          StAX parser over the whole document).
   * @param mElem
   *          StartTag to check against.
   * @return {@code true} if start elements match, {@code false} otherwise
   * @throws XMLStreamException
   *           handling XML Stream Exception
   */
  private static boolean checkStAXStartElement(final StartElement mStartTag, final StartElement mElem)
    throws XMLStreamException {
    assert mStartTag != null && mElem != null;
    boolean retVal = false;
    if (mStartTag.getEventType() == XMLStreamConstants.START_ELEMENT
      && mStartTag.getName().equals(mElem.getName())) {
      // Check attributes.
      boolean foundAtts = false;
      boolean hasAtts = false;
      for (final Iterator<?> itStartTag = mStartTag.getAttributes(); itStartTag.hasNext();) {
        hasAtts = true;
        final Attribute attStartTag = (Attribute)itStartTag.next();
        for (final Iterator<?> itElem = mElem.getAttributes(); itElem.hasNext();) {
          final Attribute attElem = (Attribute)itElem.next();
          if (attStartTag.getName().equals(attElem.getName())
            && attStartTag.getName().equals(attElem.getName())) {
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
      for (final Iterator<?> itStartTag = mStartTag.getNamespaces(); itStartTag.hasNext();) {
        hasNamesps = true;
        final Namespace nsStartTag = (Namespace)itStartTag.next();
        for (final Iterator<?> itElem = mElem.getNamespaces(); itElem.hasNext();) {
          final Namespace nsElem = (Namespace)itElem.next();
          if (nsStartTag.getName().equals(nsElem.getName()) && nsStartTag.getName().equals(nsElem.getName())) {
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
   * @param args
   *          Arguments.
   * @throws AbsTTException
   *           if anything within Treetank fails
   */
  public static void main(final String... args) throws AbsTTException {
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: WikipediaImport path/to/xmlFile path/to/TTStorage");
    }

    LOGWRAPPER.info("Importing wikipedia...");
    final long start = System.nanoTime();

    final File xml = new File(args[0]);
    final File tnk = new File(args[1]);
    Database.truncateDatabase(new DatabaseConfiguration(tnk));

    // Create necessary element nodes.
    final String NSP_URI = "http://www.mediawiki.org/xml/export-0.5/";
    final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
    final StartElement timestamp =
      eventFactory.createStartElement(new QName(NSP_URI, "timestamp", XMLConstants.DEFAULT_NS_PREFIX), null,
        null);
    final StartElement page =
      eventFactory.createStartElement(new QName(NSP_URI, "page", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement rev =
      eventFactory.createStartElement(new QName(NSP_URI, "revision", XMLConstants.DEFAULT_NS_PREFIX), null,
        null);
    final StartElement id =
      eventFactory.createStartElement(new QName(NSP_URI, "id", XMLConstants.DEFAULT_NS_PREFIX), null, null);
    final StartElement text =
      eventFactory.createStartElement(new QName(NSP_URI, "text", XMLConstants.DEFAULT_NS_PREFIX), null, null);

    // Create list.
    final List<StartElement> list = new LinkedList<StartElement>();
    list.add(timestamp);
    list.add(page);
    list.add(rev);
    list.add(id);
    list.add(text);

    // Invoke import.
    new WikipediaImport(xml, tnk).importData(EDateBy.HOURS, list);

    LOGWRAPPER.info(" done in " + (System.nanoTime() - start) / 1_000_000_000 + "[s].");
  }
}

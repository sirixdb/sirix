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

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.xml.ElementNode;
import org.sirix.service.InsertPosition;
import org.sirix.service.ShredderCommit;
import org.sirix.settings.Constants;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.*;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class appends a given {@link XMLStreamReader} to a {@link XmlNodeTrx} . The content of the
 * stream is added as a subtree. Based on an enum which identifies the point of insertion, the
 * subtree is either added as first child or as right sibling.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class XmlShredder extends AbstractShredder implements Callable<Long> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XmlShredder.class));

  /** {@link XmlNodeTrx}. */
  private final XmlNodeTrx wtx;

  /** {@link XMLEventReader}. */
  private final XMLEventReader reader;

  /** Determines if changes are going to be commit right after shredding. */
  private final ShredderCommit commit;

  /** Insertion position. */
  private final InsertPosition insert;

  /** Determines if comments should be included. */
  private final boolean includeComments;

  /** Determines if processing instructions should be included. */
  private final boolean includePIs;

  /**
   * Builder to build an {@link XmlShredder} instance.
   */
  public static class Builder {

    /** {@link XmlNodeTrx} implementation. */
    private final XmlNodeTrx wtx;

    /** {@link XMLEventReader} implementation. */
    private final XMLEventReader reader;

    /** Insertion position. */
    private final InsertPosition insert;

    /** Determines if comments should be included. */
    private boolean includeComments = true;

    /** Determines if processing instructions should be included. */
    private boolean includePIs = true;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    /**
     * Constructor.
     *
     * @param wtx {@link XmlNodeTrx} implementation
     * @param reader {@link XMLEventReader} implementation
     * @param insert insertion position
     */
    public Builder(final XmlNodeTrx wtx, final XMLEventReader reader, final InsertPosition insert) {
      this.wtx = checkNotNull(wtx);
      this.reader = checkNotNull(reader);
      this.insert = checkNotNull(insert);
    }

    /**
     * Include comments or not (default: yes).
     *
     * @param include include comments
     * @return this builder instance
     */
    public Builder includeComments(final boolean include) {
      includeComments = include;
      return this;
    }

    /**
     * Include processing instructions or not (default: yes).
     *
     * @param include processing instructions
     * @return this builder instance
     */
    public Builder includePIs(final boolean include) {
      includePIs = include;
      return this;
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public Builder commitAfterwards() {
      commit = ShredderCommit.COMMIT;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link XmlShredder} instance
     */
    public XmlShredder build() {
      return new XmlShredder(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder reference
   */
  private XmlShredder(final Builder builder) {
    super(builder.wtx, builder.insert);
    wtx = builder.wtx;
    reader = builder.reader;
    insert = builder.insert;
    includeComments = builder.includeComments;
    includePIs = builder.includePIs;
    commit = builder.commit;
  }

  /**
   * Invoking the shredder.
   *
   * @throws SirixException if any kind of sirix exception which has occured
   * @return revision of file
   */
  @Override
  public Long call() {
    final long revision = wtx.getRevisionNumber();
    insertNewContent();
    commit.commit(wtx);
    return revision;
  }

  /**
   * Insert new content based on a StAX parser {@link XMLStreamReader}.
   *
   * @throws SirixException if something went wrong while inserting
   */
  private void insertNewContent() {
    try {
      boolean firstElement = true;
      int level = 0;
      QName rootElement = null;
      boolean endElemReached = false;
      final StringBuilder sBuilder = new StringBuilder();
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (reader.hasNext() && !endElemReached) {
        final XMLEvent event = reader.nextEvent();

        switch (event.getEventType()) {
          case XMLStreamConstants.START_ELEMENT:
            level++;
            addNewElement(event.asStartElement());
            if (firstElement) {
              firstElement = false;
              insertedRootNodeKey = wtx.getNodeKey();
              rootElement = event.asStartElement().getName();
            }
            break;
          case XMLStreamConstants.END_ELEMENT:
            level--;
            if (level == 0 && rootElement != null && rootElement.equals(event.asEndElement().getName())) {
              endElemReached = true;
            }
            final QName name = event.asEndElement().getName();
            processEndTag(new QNm(name.getNamespaceURI(), name.getPrefix(), name.getLocalPart()));
            break;
          case XMLStreamConstants.CHARACTERS:
            if (reader.peek().getEventType() == XMLStreamConstants.CHARACTERS) {
              sBuilder.append(event.asCharacters().getData().trim());
            } else {
              sBuilder.append(event.asCharacters().getData().trim());
              processText(sBuilder.toString());
              sBuilder.setLength(0);
            }
            break;
          case XMLStreamConstants.COMMENT:
            if (includeComments) {
              processComment(((Comment) event).getText());
            }
            break;
          case XMLStreamConstants.PROCESSING_INSTRUCTION:
            if (includePIs) {
              final ProcessingInstruction pi = (ProcessingInstruction) event;
              processPI(pi.getData(), pi.getTarget());
            }
            break;
          default:
            // Node kind not known.
        }
      }

      wtx.moveTo(insertedRootNodeKey);
    } catch (final XMLStreamException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Add a new element node.
   *
   * @param event the current event from the StAX parser
   * @throws SirixException if adding {@link ElementNode} fails
   */
  private void addNewElement(final StartElement event) throws SirixException {
    assert event != null;
    final QName qName = event.getName();
    final QNm name = new QNm(qName.getNamespaceURI(), qName.getPrefix(), qName.getLocalPart());
    processStartTag(name);

    // Parse namespaces.
    for (final Iterator<?> it = event.getNamespaces(); it.hasNext();) {
      final Namespace namespace = (Namespace) it.next();
      wtx.insertNamespace(new QNm(namespace.getNamespaceURI(), namespace.getPrefix(), ""));
      wtx.moveToParent();
    }

    // Parse attributes.
    for (final Iterator<?> it = event.getAttributes(); it.hasNext();) {
      final Attribute attribute = (Attribute) it.next();
      final QName attName = attribute.getName();
      wtx.insertAttribute(new QNm(attName.getNamespaceURI(), attName.getPrefix(), attName.getLocalPart()),
                          attribute.getValue());
      wtx.moveToParent();
    }
  }

  /**
   * Main method.
   *
   * @param args input and output files
   * @throws XMLStreamException if the XML stream isn't valid
   * @throws IOException if an I/O error occurs
   * @throws SirixException if a Sirix error occurs
   */
  public static void main(final String... args) throws SirixException, IOException, XMLStreamException {
    if (args.length != 2 && args.length != 3) {
      throw new IllegalArgumentException("Usage: XMLShredder XMLFile Database [true/false] (shredder comment|PI)");
    }
    LOGWRAPPER.info("Shredding '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final Path target = Paths.get(args[1]);
    final DatabaseConfiguration config = new DatabaseConfiguration(target);
    Databases.removeDatabase(target);
    Databases.createXmlDatabase(config);

    try (final var db = Databases.openXmlDatabase(target)) {
      db.createResource(new ResourceConfiguration.Builder("shredded").build());
      try (final XmlResourceSession resMgr = db.beginResourceSession("shredded");
           final XmlNodeTrx wtx = resMgr.beginNodeTrx();
           final FileInputStream fis = new FileInputStream(Paths.get(args[0]).toFile())) {
        final XMLEventReader reader = createFileReader(fis);
        final boolean includeCoPI = args.length == 3 && Boolean.parseBoolean(args[2]);
        final XmlShredder shredder =
            new XmlShredder.Builder(wtx, reader, InsertPosition.AS_FIRST_CHILD).commitAfterwards()
                                                                               .includeComments(includeCoPI)
                                                                               .includePIs(includeCoPI)
                                                                               .build();
        shredder.call();
      }
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1000000 + " ms].");
  }

  /**
   * Create a new {@link XMLEventReader} instance on a file.
   *
   * @param fis the file input stream
   * @return an {@link XMLEventReader}
   * @throws SirixException if creating the xml event reader fails.
   */
  public static XMLEventReader createFileReader(final FileInputStream fis) {
    checkNotNull(fis);
    final XMLInputFactory factory = XMLInputFactory.newInstance();
    setProperties(factory);
    try {
      return factory.createXMLEventReader(fis);
    } catch (XMLStreamException e) {
      throw new SirixException(e.getMessage(), e);
    }
  }

  private static void setProperties(final XMLInputFactory factory) {
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
  }

  /**
   * Create a new {@link XMLEventReader} instance on a string.
   *
   * @param xmlString the XML file as a string to parse
   * @return an {@link XMLEventReader}
   * @throws SirixException if creating the xml event reader fails.
   */
  public static XMLEventReader createStringReader(final String xmlString) {
    checkNotNull(xmlString);
    final XMLInputFactory factory = XMLInputFactory.newInstance();
    setProperties(factory);
    try {
      final InputStream in = new ByteArrayInputStream(xmlString.getBytes(Constants.DEFAULT_ENCODING));
      return factory.createXMLEventReader(in);
    } catch (XMLStreamException e) {
      throw new SirixException(e.getMessage(), e);
    }
  }

  /**
   * Create a new StAX reader based on a List of {@link XMLEvent}s.
   *
   * @param events {@link XMLEvent}s
   * @return an {@link XMLEventReader}
   * @throws IOException if I/O operation fails
   * @throws XMLStreamException if any parsing error occurs
   */
  public static XMLEventReader createQueueReader(final Queue<XMLEvent> events) throws IOException, XMLStreamException {
    return new QueueEventReader(checkNotNull(events));
  }
}

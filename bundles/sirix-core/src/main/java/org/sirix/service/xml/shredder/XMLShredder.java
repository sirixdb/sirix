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

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Comment;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.ProcessingInstruction;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.xdm.ElementNode;
import org.sirix.settings.Constants;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * This class appends a given {@link XMLStreamReader} to a {@link XdmNodeWriteTrx} . The content of
 * the stream is added as a subtree. Based on an enum which identifies the point of insertion, the
 * subtree is either added as first child or as right sibling.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class XMLShredder extends AbstractShredder implements Callable<Long> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XMLShredder.class));

  /** {@link XdmNodeWriteTrx}. */
  protected final XdmNodeWriteTrx mWtx;

  /** {@link XMLEventReader}. */
  protected final XMLEventReader mReader;

  /** Determines if changes are going to be commit right after shredding. */
  private final ShredderCommit mCommit;

  /** Insertion position. */
  protected final Insert mInsert;

  /** Determines if comments should be included. */
  private boolean mIncludeComments;

  /** Determines if processing instructions should be included. */
  private boolean mIncludePIs;

  /**
   * Builder to build an {@link XMLShredder} instance.
   */
  public static class Builder {

    /** {@link XdmNodeWriteTrx} implementation. */
    private final XdmNodeWriteTrx mWtx;

    /** {@link XMLEventReader} implementation. */
    private final XMLEventReader mReader;

    /** Insertion position. */
    private final Insert mInsert;

    /** Determines if comments should be included. */
    private boolean mIncludeComments = true;

    /** Determines if processing instructions should be included. */
    private boolean mIncludePIs = true;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit mCommit = ShredderCommit.NOCOMMIT;

    /**
     * Constructor.
     *
     * @param wtx {@link XdmNodeWriteTrx} implementation
     * @param reader {@link XMLEventReader} implementation
     * @param insert insertion position
     */
    public Builder(final XdmNodeWriteTrx wtx, final XMLEventReader reader, final Insert insert) {
      mWtx = checkNotNull(wtx);
      mReader = checkNotNull(reader);
      mInsert = checkNotNull(insert);
    }

    /**
     * Include comments or not (default: yes).
     *
     * @param include include comments
     * @return this builder instance
     */
    public Builder includeComments(final boolean include) {
      mIncludeComments = include;
      return this;
    }

    /**
     * Include processing instructions or not (default: yes).
     *
     * @param nclude processing instructions
     * @return this builder instance
     */
    public Builder includePIs(final boolean include) {
      mIncludePIs = include;
      return this;
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public Builder commitAfterwards() {
      mCommit = ShredderCommit.COMMIT;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link XMLShredder} instance
     */
    public XMLShredder build() {
      return new XMLShredder(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder reference
   */
  private XMLShredder(final Builder builder) {
    super(builder.mWtx, builder.mInsert);
    mWtx = builder.mWtx;
    mReader = builder.mReader;
    mInsert = builder.mInsert;
    mIncludeComments = builder.mIncludeComments;
    mIncludePIs = builder.mIncludePIs;
    mCommit = builder.mCommit;
  }

  /**
   * Invoking the shredder.
   *
   * @throws SirixException if any kind of sirix exception which has occured
   * @return revision of file
   */
  @Override
  public Long call() throws SirixException {
    final long revision = mWtx.getRevisionNumber();
    insertNewContent();
    mCommit.commit(mWtx);
    return revision;
  }

  /**
   * Insert new content based on a StAX parser {@link XMLStreamReader}.
   *
   * @throws SirixException if something went wrong while inserting
   */
  protected final void insertNewContent() throws SirixException {
    try {
      boolean firstElement = true;
      int level = 0;
      QName rootElement = null;
      boolean endElemReached = false;
      final StringBuilder sBuilder = new StringBuilder();
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (mReader.hasNext() && !endElemReached) {
        final XMLEvent event = mReader.nextEvent();

        switch (event.getEventType()) {
          case XMLStreamConstants.START_ELEMENT:
            level++;
            addNewElement(event.asStartElement());
            if (firstElement) {
              firstElement = false;
              insertedRootNodeKey = mWtx.getNodeKey();
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
            if (mReader.peek().getEventType() == XMLStreamConstants.CHARACTERS) {
              sBuilder.append(event.asCharacters().getData().trim());
            } else {
              sBuilder.append(event.asCharacters().getData().trim());
              processText(sBuilder.toString());
              sBuilder.setLength(0);
            }
            break;
          case XMLStreamConstants.COMMENT:
            if (mIncludeComments) {
              processComment(((Comment) event).getText());
            }
            break;
          case XMLStreamConstants.PROCESSING_INSTRUCTION:
            if (mIncludePIs) {
              final ProcessingInstruction pi = (ProcessingInstruction) event;
              processPI(pi.getData(), pi.getTarget());
            }
            break;
          default:
            // Node kind not known.
        }
      }

      mWtx.moveTo(insertedRootNodeKey);
    } catch (final XMLStreamException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Add a new element node.
   *
   * @param pLeftSiblingKeyStack stack used to determine if the new element has to be inserted as a
   *        right sibling or as a new child (in the latter case is NULL on top of the stack)
   * @param event the current event from the StAX parser
   * @return the modified stack
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
      mWtx.insertNamespace(new QNm(namespace.getNamespaceURI(), namespace.getPrefix(), ""));
      mWtx.moveToParent();
    }

    // Parse attributes.
    for (final Iterator<?> it = event.getAttributes(); it.hasNext();) {
      final Attribute attribute = (Attribute) it.next();
      final QName attName = attribute.getName();
      mWtx.insertAttribute(new QNm(attName.getNamespaceURI(), attName.getPrefix(), attName.getLocalPart()),
          attribute.getValue());
      mWtx.moveToParent();
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
    Databases.createDatabase(config);

    try (final Database db = Databases.openDatabase(target)) {
      db.createResource(new ResourceConfiguration.Builder("shredded", config).build());
      try (final XdmResourceManager resMgr = db.getXdmResourceManager("shredded");
          final XdmNodeWriteTrx wtx = resMgr.beginNodeWriteTrx();
          final FileInputStream fis = new FileInputStream(Paths.get(args[0]).toFile())) {
        final XMLEventReader reader = createFileReader(fis);
        final boolean includeCoPI = args.length == 3
            ? Boolean.parseBoolean(args[2])
            : false;
        final XMLShredder shredder =
            new XMLShredder.Builder(wtx, reader, Insert.ASFIRSTCHILD).commitAfterwards()
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
  public static synchronized XMLEventReader createFileReader(final FileInputStream fis) {
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
  public static synchronized XMLEventReader createStringReader(final String xmlString) {
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
  public static synchronized XMLEventReader createQueueReader(final Queue<XMLEvent> events)
      throws IOException, XMLStreamException {
    return new QueueEventReader(checkNotNull(events));
  }
}

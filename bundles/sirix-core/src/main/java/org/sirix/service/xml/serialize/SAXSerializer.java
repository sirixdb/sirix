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

package org.sirix.service.xml.serialize;

import java.io.File;
import java.io.IOException;

import javax.xml.namespace.QName;

import org.xml.sax.*;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.access.PageWriteTrx;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.node.ElementNode;
import org.sirix.utils.XMLToken;

/**
 * <h1>SaxSerializer</h1>
 * 
 * <p>
 * Generates SAX events from a sirix database.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SAXSerializer extends AbsSerializer implements XMLReader {

  /** SAX content handler. */
  private transient ContentHandler mContHandler;

  /**
   * Constructor.
   * 
   * @param paramSession
   *          sirix session {@link ISession}.
   * @param paramHandler
   *          SAX ContentHandler {@link ContentHandler}.
   * @param paramVersions
   *          Revisions to serialize.
   */
  public SAXSerializer(final ISession paramSession, final ContentHandler paramHandler,
    final long... paramVersions) {
    super(paramSession, paramVersions);
    mContHandler = paramHandler;
  }

  /** {@inheritDoc} */
  @Override
  protected void emitStartElement(final INodeReadTrx rtx) {
    switch (rtx.getNode().getKind()) {
    case ROOT_KIND:
      break;
    case ELEMENT_KIND:
      generateElement(rtx);
      break;
    case TEXT_KIND:
      generateText(rtx);
      break;
    default:
      throw new UnsupportedOperationException("Node kind not supported by sirix!");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void emitEndElement(final INodeReadTrx rtx) {
    final QName qName = rtx.getQNameOfCurrentNode();
    final String mURI = qName.getNamespaceURI();
    try {
      mContHandler.endElement(mURI, qName.getLocalPart(), PageWriteTrx.buildName(qName));
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void emitStartManualElement(final long revision) {
    final AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "revision", "tt", "", Long.toString(revision));
    try {
      mContHandler.startElement("", "tt", "tt", atts);
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }

  }

  /** {@inheritDoc} */
  @Override
  protected void emitEndManualElement(final long revision) {
    try {
      mContHandler.endElement("", "tt", "tt");
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * Generate a start element event.
   * 
   * @param paramRtx
   *          Read Transaction
   */
  private void generateElement(final INodeReadTrx paramRtx) {
    final AttributesImpl atts = new AttributesImpl();
    final long key = paramRtx.getNode().getNodeKey();

    try {
      // Process namespace nodes.
      for (int i = 0, namesCount = ((ElementNode)paramRtx.getNode()).getNamespaceCount(); i < namesCount; i++) {
        paramRtx.moveToNamespace(i);
        final QName qName = paramRtx.getQNameOfCurrentNode();
        mContHandler.startPrefixMapping(qName.getPrefix(), qName.getNamespaceURI());
        final String mURI = qName.getNamespaceURI();
        if (qName.getLocalPart().length() == 0) {
          // if (qName.getPrefix() == null || qName.getPrefix() == "") {
          atts.addAttribute(mURI, "xmlns", "xmlns", "CDATA", mURI);
        } else {
          atts.addAttribute(mURI, "xmlns", "xmlns:" + paramRtx.getQNameOfCurrentNode().getLocalPart(),
            "CDATA", mURI);
        }
        paramRtx.moveTo(key);
      }

      // Process attributes.
      for (int i = 0, attCount = ((ElementNode)paramRtx.getNode()).getAttributeCount(); i < attCount; i++) {
        paramRtx.moveToAttribute(i);
        final QName qName = paramRtx.getQNameOfCurrentNode();
        final String mURI = qName.getNamespaceURI();
        atts.addAttribute(mURI, qName.getLocalPart(), PageWriteTrx.buildName(qName), paramRtx
          .getTypeOfCurrentNode(), paramRtx.getValueOfCurrentNode());
        paramRtx.moveTo(key);
      }

      // Create SAX events.
      final QName qName = paramRtx.getQNameOfCurrentNode();
      mContHandler.startElement(qName.getNamespaceURI(), qName.getLocalPart(), PageWriteTrx.buildName(qName),
        atts);

      // Empty elements.
      if (!((ElementNode)paramRtx.getNode()).hasFirstChild()) {
        mContHandler.endElement(qName.getNamespaceURI(), qName.getLocalPart(), PageWriteTrx.buildName(qName));
      }
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * Generate a text event.
   * 
   * @param mRtx
   *          Read Transaction.
   */
  private void generateText(final INodeReadTrx paramRtx) {
    try {
      mContHandler.characters(XMLToken.escape(paramRtx.getValueOfCurrentNode()).toCharArray(), 0, paramRtx
        .getValueOfCurrentNode().length());
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * Main method.
   * 
   * @param args
   *          args[0] specifies the path to the TT-storage from which to
   *          generate SAX events.
   * @throws Exception
   *           handling sirix exception
   */
  public static void main(final String... args) throws Exception {

    final DatabaseConfiguration config = new DatabaseConfiguration(new File(args[0]));
    Database.createDatabase(config);
    final IDatabase database = Database.openDatabase(new File(args[0]));
    database.createResource(new ResourceConfiguration.Builder("shredded", config).build());
    final ISession session = database.getSession(new SessionConfiguration.Builder("shredded").build());

    final DefaultHandler defHandler = new DefaultHandler();

    final SAXSerializer serializer = new SAXSerializer(session, defHandler);
    serializer.call();

    session.close();
  }

  @Override
  protected void emitStartDocument() {
    try {
      mContHandler.startDocument();
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  @Override
  protected void emitEndDocument() {
    try {
      mContHandler.endDocument();
    } catch (final SAXException exc) {
      exc.printStackTrace();
    }
  }

  /* Implements XMLReader method. */
  @Override
  public ContentHandler getContentHandler() {
    return mContHandler;
  }

  /* Implements XMLReader method. */
  @Override
  public DTDHandler getDTDHandler() {
    return null;
  }

  /* Implements XMLReader method. */
  @Override
  public EntityResolver getEntityResolver() {
    return null;
  }

  /* Implements XMLReader method. */
  @Override
  public ErrorHandler getErrorHandler() {
    return null;
  }

  /* Implements XMLReader method. */
  @Override
  public boolean getFeature(final String mName) throws SAXNotRecognizedException, SAXNotSupportedException {
    return false;
  }

  /* Implements XMLReader method. */
  @Override
  public Object getProperty(final String mName) throws SAXNotRecognizedException, SAXNotSupportedException {
    return null;
  }

  /* Implements XMLReader method. */
  @Override
  public void parse(final InputSource mInput) throws IOException, SAXException {
    throw new UnsupportedOperationException("Not supported by sirix!");
  }

  /* Implements XMLReader method. */
  @Override
  public void parse(final String mSystemID) throws IOException, SAXException {
    emitStartDocument();
    try {
      super.call();
    } catch (final Exception exc) {
      exc.printStackTrace();
    }
    emitEndDocument();
  }

  /* Implements XMLReader method. */
  @Override
  public void setContentHandler(final ContentHandler paramContentHandler) {
    mContHandler = paramContentHandler;
  }

  /* Implements XMLReader method. */
  @Override
  public void setDTDHandler(final DTDHandler paramHandler) {
    throw new UnsupportedOperationException("Not supported by sirix!");
  }

  /* Implements XMLReader method. */
  @Override
  public void setEntityResolver(final EntityResolver paramResolver) {
    throw new UnsupportedOperationException("Not supported by sirix!");

  }

  /* Implements XMLReader method. */
  @Override
  public void setErrorHandler(final ErrorHandler paramHandler) {
    throw new UnsupportedOperationException("Not supported by sirix!");
  }

  /* Implements XMLReader method. */
  @Override
  public void setFeature(final String paramName, final boolean paramValue) throws SAXNotRecognizedException,
    SAXNotSupportedException {
    throw new UnsupportedOperationException("Not supported by sirix!");
  }

  /* Implements XMLReader method. */
  @Override
  public void setProperty(final String paramName, final Object paramValue) throws SAXNotRecognizedException,
    SAXNotSupportedException {
    throw new UnsupportedOperationException("Not supported by sirix!");
  }
}

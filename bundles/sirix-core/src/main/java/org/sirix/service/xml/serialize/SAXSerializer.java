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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.access.DatabaseImpl;
import org.sirix.access.Utils;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

/**
 * <h1>SaxSerializer</h1>
 * 
 * <p>
 * Generates SAX events from a Sirix database/resource.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SAXSerializer extends AbsSerializer implements XMLReader {

	/** {@link LogWrapper} reference. */
	private final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(SAXSerializer.class));

	/** SAX content handler. */
	private ContentHandler mContHandler;

	/**
	 * Constructor.
	 * 
	 * @param pSession
	 *          Sirix {@link Session}
	 * @param pHandler
	 *          SAX {@link ContentHandler}
	 * @param pRevision
	 *          revision to serialize
	 * @param pRevisions
	 *          further revisions to serialize
	 */
	public SAXSerializer(@Nonnull final Session pSession,
			@Nonnull final ContentHandler pHandler, @Nonnegative final int pRevision,
			final int... pRevisions) {
		super(pSession, pRevision, pRevisions);
		mContHandler = pHandler;
	}

	@Override
	protected void emitStartElement(@Nonnull final NodeReadTrx pRtx) {
		switch (pRtx.getKind()) {
		case DOCUMENT_ROOT:
			break;
		case ELEMENT:
			generateElement(pRtx);
			break;
		case TEXT:
			generateText(pRtx);
			break;
		case COMMENT:
			generateComment(pRtx);
			break;
		case PROCESSING:
			generatePI(pRtx);
			break;
		default:
			throw new UnsupportedOperationException(
					"Node kind not supported by sirix!");
		}
	}

	@Override
	protected void emitEndElement(@Nonnull final NodeReadTrx pRtx) {
		final QName qName = pRtx.getName();
		final String mURI = qName.getNamespaceURI();
		try {
			mContHandler.endPrefixMapping(qName.getPrefix());
			mContHandler.endElement(mURI, qName.getLocalPart(),
					Utils.buildName(qName));
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitStartManualElement(@Nonnegative final long pRevision) {
		final AttributesImpl atts = new AttributesImpl();
		atts.addAttribute("", "revision", "tt", "", Long.toString(pRevision));
		try {
			mContHandler.startElement("", "tt", "tt", atts);
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}

	}

	@Override
	protected void emitEndManualElement(@Nonnegative final long pRevision) {
		try {
			mContHandler.endElement("", "tt", "tt");
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generates a comment event.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generateComment(final @Nonnull NodeReadTrx pRtx) {
		try {
			final char[] content = pRtx.getValue().toCharArray();
			mContHandler.characters(content, 0, content.length);
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generate a processing instruction event.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generatePI(final @Nonnull NodeReadTrx pRtx) {
		try {
			mContHandler.processingInstruction(pRtx.getName().getLocalPart(),
					pRtx.getValue());
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generate a start element event.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generateElement(@Nonnull final NodeReadTrx pRtx) {
		final AttributesImpl atts = new AttributesImpl();
		final long key = pRtx.getNodeKey();

		try {
			// Process namespace nodes.
			for (int i = 0, namesCount = pRtx.getNamespaceCount(); i < namesCount; i++) {
				pRtx.moveToNamespace(i);
				final QName qName = pRtx.getName();
				mContHandler.startPrefixMapping(qName.getPrefix(),
						qName.getNamespaceURI());
				final String mURI = qName.getNamespaceURI();
				if (qName.getLocalPart().length() == 0) {
					// if (qName.getPrefix() == null || qName.getPrefix() == "") {
					atts.addAttribute(mURI, "xmlns", "xmlns", "CDATA", mURI);
				} else {
					atts.addAttribute(mURI, "xmlns", "xmlns:"
							+ pRtx.getName().getLocalPart(), "CDATA", mURI);
				}
				pRtx.moveTo(key);
			}

			// Process attributes.
			for (int i = 0, attCount = pRtx.getAttributeCount(); i < attCount; i++) {
				pRtx.moveToAttribute(i);
				final QName qName = pRtx.getName();
				final String mURI = qName.getNamespaceURI();
				atts.addAttribute(mURI, qName.getLocalPart(), Utils.buildName(qName),
						pRtx.getType(), pRtx.getValue());
				pRtx.moveTo(key);
			}

			// Create SAX events.
			final QName qName = pRtx.getName();
			mContHandler.startElement(qName.getNamespaceURI(), qName.getLocalPart(),
					Utils.buildName(qName), atts);

			// Empty elements.
			if (!pRtx.hasFirstChild()) {
				mContHandler.endElement(qName.getNamespaceURI(), qName.getLocalPart(),
						Utils.buildName(qName));
			}
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generate a text event.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generateText(@Nonnull final NodeReadTrx pRtx) {
		try {
			mContHandler.characters(XMLToken.escapeContent(pRtx.getValue())
					.toCharArray(), 0, pRtx.getValue().length());
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Main method.
	 * 
	 * @param args
	 *          args[0] specifies the path to the TT-storage from which to
	 *          generate SAX events.
	 * @throws Exception
	 *           handling Sirix exceptions
	 */
	public static void main(final String... args) throws Exception {
		final DatabaseConfiguration config = new DatabaseConfiguration(new File(
				args[0]));
		DatabaseImpl.createDatabase(config);
		final Database database = DatabaseImpl.openDatabase(new File(args[0]));
		database.createResource(new ResourceConfiguration.Builder("shredded",
				config).build());
		try (final Session session = database
				.getSession(new SessionConfiguration.Builder("shredded").build())) {
			final DefaultHandler defHandler = new DefaultHandler();
			final SAXSerializer serializer = new SAXSerializer(session, defHandler,
					session.getLastRevisionNumber());
			serializer.call();
		}
	}

	@Override
	protected void emitStartDocument() {
		try {
			mContHandler.startDocument();
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitEndDocument() {
		try {
			mContHandler.endDocument();
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
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
	public boolean getFeature(final String mName)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		return false;
	}

	/* Implements XMLReader method. */
	@Override
	public Object getProperty(final String pName)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		return null;
	}

	/* Implements XMLReader method. */
	@Override
	public void parse(final InputSource mInput) throws IOException, SAXException {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}

	/* Implements XMLReader method. */
	@Override
	public void parse(final String pSystemID) throws IOException, SAXException {
		emitStartDocument();
		try {
			super.call();
		} catch (final SirixException e) {
			LOGGER.error(e.getMessage(), e);
		}
		emitEndDocument();
	}

	/* Implements XMLReader method. */
	@Override
	public void setContentHandler(final ContentHandler pContentHandler) {
		mContHandler = checkNotNull(pContentHandler);
	}

	/* Implements XMLReader method. */
	@Override
	public void setDTDHandler(final DTDHandler pHandler) {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}

	/* Implements XMLReader method. */
	@Override
	public void setEntityResolver(final EntityResolver pResolver) {
		throw new UnsupportedOperationException("Not supported by sirix!");

	}

	/* Implements XMLReader method. */
	@Override
	public void setErrorHandler(final ErrorHandler pHandler) {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}

	/* Implements XMLReader method. */
	@Override
	public void setFeature(final String pName, final boolean pValue)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}

	/* Implements XMLReader method. */
	@Override
	public void setProperty(final String pName, final Object pValue)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}
}

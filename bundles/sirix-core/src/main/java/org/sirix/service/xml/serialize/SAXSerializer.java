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

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Databases;
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
public final class SAXSerializer extends AbstractSerializer implements
		XMLReader {

	/** {@link LogWrapper} reference. */
	private final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(SAXSerializer.class));

	/** SAX content handler. */
	private ContentHandler mContHandler;

	/**
	 * Constructor.
	 * 
	 * @param session
	 *          Sirix {@link Session}
	 * @param handler
	 *          SAX {@link ContentHandler}
	 * @param revision
	 *          revision to serialize
	 * @param revisions
	 *          further revisions to serialize
	 */
	public SAXSerializer(final @Nonnull Session session,
			final @Nonnull ContentHandler handler, final @Nonnegative int revision,
			final int... revisions) {
		super(session, revision, revisions);
		mContHandler = handler;
	}

	@Override
	protected void emitStartElement(final @Nonnull NodeReadTrx rtx) {
		switch (rtx.getKind()) {
		case DOCUMENT:
			break;
		case ELEMENT:
			generateElement(rtx);
			break;
		case TEXT:
			generateText(rtx);
			break;
		case COMMENT:
			generateComment(rtx);
			break;
		case PROCESSING_INSTRUCTION:
			generatePI(rtx);
			break;
		default:
			throw new UnsupportedOperationException(
					"Node kind not supported by sirix!");
		}
	}

	@Override
	protected void emitEndElement(final @Nonnull NodeReadTrx rtx) {
		final QNm qName = rtx.getName();
		final String mURI = qName.getNamespaceURI();
		try {
			mContHandler.endPrefixMapping(qName.getPrefix());
			mContHandler.endElement(mURI, qName.getLocalName(),
					Utils.buildName(qName));
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitStartManualElement(final @Nonnegative long revision) {
		final AttributesImpl atts = new AttributesImpl();
		atts.addAttribute("", "revision", "tt", "", Long.toString(revision));
		try {
			mContHandler.startElement("", "tt", "tt", atts);
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}

	}

	@Override
	protected void emitEndManualElement(final @Nonnegative long revision) {
		try {
			mContHandler.endElement("", "tt", "tt");
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generates a comment event.
	 * 
	 * @param rtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generateComment(final @Nonnull NodeReadTrx rtx) {
		try {
			final char[] content = rtx.getValue().toCharArray();
			mContHandler.characters(content, 0, content.length);
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generate a processing instruction event.
	 * 
	 * @param rtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generatePI(final @Nonnull NodeReadTrx rtx) {
		try {
			mContHandler.processingInstruction(rtx.getName().getLocalName(),
					rtx.getValue());
		} catch (final SAXException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * Generate a start element event.
	 * 
	 * @param rtx
	 *          {@link NodeReadTrx} implementation
	 */
	private void generateElement(final @Nonnull NodeReadTrx rtx) {
		final AttributesImpl atts = new AttributesImpl();
		final long key = rtx.getNodeKey();

		try {
			// Process namespace nodes.
			for (int i = 0, namesCount = rtx.getNamespaceCount(); i < namesCount; i++) {
				rtx.moveToNamespace(i);
				final QNm qName = rtx.getName();
				mContHandler.startPrefixMapping(qName.getPrefix(),
						qName.getNamespaceURI());
				final String mURI = qName.getNamespaceURI();
				if (qName.getPrefix() == null || qName.getPrefix().length() == 0) {
					atts.addAttribute(mURI, "xmlns", "xmlns", "CDATA", mURI);
				} else {
					atts.addAttribute(mURI, "xmlns", "xmlns:"
							+ rtx.getName().getPrefix(), "CDATA", mURI);
				}
				rtx.moveTo(key);
			}

			// Process attributes.
			for (int i = 0, attCount = rtx.getAttributeCount(); i < attCount; i++) {
				rtx.moveToAttribute(i);
				final QNm qName = rtx.getName();
				final String mURI = qName.getNamespaceURI();
				atts.addAttribute(mURI, qName.getLocalName(), Utils.buildName(qName),
						rtx.getType(), rtx.getValue());
				rtx.moveTo(key);
			}

			// Create SAX events.
			final QNm qName = rtx.getName();
			mContHandler.startElement(qName.getNamespaceURI(), qName.getLocalName(),
					Utils.buildName(qName), atts);

			// Empty elements.
			if (!rtx.hasFirstChild()) {
				mContHandler.endElement(qName.getNamespaceURI(), qName.getLocalName(),
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
	private void generateText(final @Nonnull NodeReadTrx pRtx) {
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
	 * @throws SirixException
	 *           if any Sirix exception occurs
	 */
	public static void main(final String... args) throws SirixException {
		final DatabaseConfiguration config = new DatabaseConfiguration(new File(
				args[0]));
		Databases.createDatabase(config);
		final Database database = Databases.openDatabase(new File(args[0]));
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
	public boolean getFeature(String name) throws SAXNotRecognizedException,
			SAXNotSupportedException {
		throw new SAXNotSupportedException();
	}

	/* Implements XMLReader method. */
	@Override
	public Object getProperty(String name) throws SAXNotRecognizedException,
			SAXNotSupportedException {
		throw new SAXNotSupportedException();
	}

	/* Implements XMLReader method. */
	@Override
	public void parse(InputSource input) throws IOException, SAXException {
		throw new UnsupportedOperationException();
	}

	/* Implements XMLReader method. */
	@Override
	public void parse(String systemID) throws IOException, SAXException {
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
	public void setContentHandler(final @Nonnull ContentHandler contentHandler) {
		mContHandler = checkNotNull(contentHandler);
	}

	/* Implements XMLReader method. */
	@Override
	public void setDTDHandler(DTDHandler handler) {
		throw new UnsupportedOperationException();
	}

	/* Implements XMLReader method. */
	@Override
	public void setEntityResolver(EntityResolver resolver) {
		throw new UnsupportedOperationException();

	}

	/* Implements XMLReader method. */
	@Override
	public void setErrorHandler(ErrorHandler handler) {
		throw new UnsupportedOperationException();
	}

	/* Implements XMLReader method. */
	@Override
	public void setFeature(String name, boolean value)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new SAXNotSupportedException();
	}

	/* Implements XMLReader method. */
	@Override
	public void setProperty(String name, final Object value)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new SAXNotSupportedException();
	}
}

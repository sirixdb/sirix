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

package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
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

import org.sirix.access.DatabaseImpl;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.ElementNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * This class appends a given {@link XMLStreamReader} to a {@link NodeWriteTrx}
 * . The content of the stream is added as a subtree. Based on an enum which
 * identifies the point of insertion, the subtree is either added as first child
 * or as right sibling.
 * 
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLShredder extends AbsShredder implements Callable<Long> {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(XMLShredder.class));

	/** {@link NodeWriteTrx}. */
	protected final NodeWriteTrx mWtx;

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

		/** {@link NodeWriteTrx} implementation. */
		private final NodeWriteTrx mWtx;

		/** {@link XMLEventReader} implementation. */
		private final XMLEventReader mReader;

		/** Insertion position. */
		private final Insert mInsert;

		/** Determines if comments should be included. */
		private boolean mIncludeComments = true;

		/** Determines if processing instructions should be included. */
		private boolean mIncludePIs = true;

		/**
		 * Determines if after shredding the transaction should be immediately
		 * commited.
		 */
		private ShredderCommit mCommit = ShredderCommit.NOCOMMIT;

		/**
		 * Constructor.
		 * 
		 * @param pWtx
		 *          {@link NodeWriteTrx} implementation
		 * @param pReader
		 *          {@link XMLEventReader} implementation
		 * @param pInsert
		 *          insertion position
		 */
		public Builder(@Nonnull final NodeWriteTrx pWtx,
				@Nonnull final XMLEventReader pReader, @Nonnull final Insert pInsert) {
			mWtx = checkNotNull(pWtx);
			mReader = checkNotNull(pReader);
			mInsert = checkNotNull(pInsert);
		}

		/**
		 * Include comments or not (default: yes).
		 * 
		 * @param pInclude
		 *          include comments
		 * @return this builder instance
		 */
		public Builder includeComments(final boolean pInclude) {
			mIncludeComments = pInclude;
			return this;
		}

		/**
		 * Include processing instructions or not (default: yes).
		 * 
		 * @param pInclude
		 *          processing instructions
		 * @return this builder instance
		 */
		public Builder includePIs(final boolean pInclude) {
			mIncludePIs = pInclude;
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
	 * @param pBuilder
	 *          builder reference
	 */
	private XMLShredder(final @Nonnull Builder pBuilder) {
		super(pBuilder.mWtx, pBuilder.mInsert);
		mWtx = pBuilder.mWtx;
		mReader = pBuilder.mReader;
		mInsert = pBuilder.mInsert;
		mIncludeComments = pBuilder.mIncludeComments;
		mIncludePIs = pBuilder.mIncludePIs;
		mCommit = pBuilder.mCommit;
	}

	/**
	 * Invoking the shredder.
	 * 
	 * @throws SirixException
	 *           if any kind of sirix exception which has occured
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
	 * @throws SirixException
	 *           if something went wrong while inserting
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
					if (level == 0 && rootElement != null
							&& rootElement.equals(event.asEndElement().getName())) {
						endElemReached = true;
					}
					processEndTag(event.asEndElement().getName());
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
	 * @param pLeftSiblingKeyStack
	 *          stack used to determine if the new element has to be inserted as a
	 *          right sibling or as a new child (in the latter case is NULL on top
	 *          of the stack)
	 * @param pEvent
	 *          the current event from the StAX parser
	 * @return the modified stack
	 * @throws SirixException
	 *           if adding {@link ElementNode} fails
	 */
	private void addNewElement(@Nonnull final StartElement pEvent)
			throws SirixException {
		assert pEvent != null;
		final QName name = pEvent.getName();
		processStartTag(name);

		// Parse namespaces.
		for (final Iterator<?> it = pEvent.getNamespaces(); it.hasNext();) {
			final Namespace namespace = (Namespace) it.next();
			mWtx.insertNamespace(new QName(namespace.getNamespaceURI(), "", namespace
					.getPrefix()));
			mWtx.moveToParent();
		}

		// Parse attributes.
		for (final Iterator<?> it = pEvent.getAttributes(); it.hasNext();) {
			final Attribute attribute = (Attribute) it.next();
			mWtx.insertAttribute(attribute.getName(), attribute.getValue());
			mWtx.moveToParent();
		}
	}

	/**
	 * Main method.
	 * 
	 * @param pArgs
	 *          input and output files
	 * @throws XMLStreamException
	 *           if the XML stream isn't valid
	 * @throws IOException
	 *           if an I/O error occurs
	 * @throws SirixException
	 *           if a Sirix error occurs
	 */
	public static void main(final String... pArgs) throws SirixException,
			IOException, XMLStreamException {
		if (pArgs.length != 2 && pArgs.length != 3) {
			throw new IllegalArgumentException(
					"Usage: XMLShredder XMLFile Database [true/false] (shredder comment|PI)");
		}
		LOGWRAPPER.info("Shredding '" + pArgs[0] + "' to '" + pArgs[1] + "' ... ");
		final long time = System.nanoTime();
		final File target = new File(pArgs[1]);
		final DatabaseConfiguration config = new DatabaseConfiguration(target);
		DatabaseImpl.truncateDatabase(config);
		DatabaseImpl.createDatabase(config);
		final Database db = DatabaseImpl.openDatabase(target);
		db.createResource(new ResourceConfiguration.Builder("shredded", config)
				.build());
		final Session session = db.getSession(new SessionConfiguration.Builder(
				"shredded").build());
		final NodeWriteTrx wtx = session.beginNodeWriteTrx();
		final XMLEventReader reader = createFileReader(new File(pArgs[0]));
		final boolean includeCoPI = pArgs.length == 3 ? Boolean
				.parseBoolean(pArgs[2]) : false;
		final XMLShredder shredder = new XMLShredder.Builder(wtx, reader,
				Insert.ASFIRSTCHILD).commitAfterwards()
				.includeComments(includeCoPI).includePIs(includeCoPI).build();
		shredder.call();
		wtx.close();
		session.close();
		db.close();
		LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1000000 + " ms].");
	}

	/**
	 * Create a new StAX reader on a file.
	 * 
	 * @param pFile
	 *          the XML file to parse
	 * @return an {@link XMLEventReader}
	 * @throws IOException
	 *           if I/O operation fails
	 * @throws XMLStreamException
	 *           if any parsing error occurs
	 */
	public static synchronized XMLEventReader createFileReader(
			@Nonnull final File pFile) throws IOException, XMLStreamException {
		checkNotNull(pFile);
		final XMLInputFactory factory = XMLInputFactory.newInstance();
		factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
		factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
		final InputStream in = new FileInputStream(pFile);
		return factory.createXMLEventReader(in);
	}

	/**
	 * Create a new StAX reader on a string.
	 * 
	 * @param pString
	 *          the XML file as a string to parse
	 * @return an {@link XMLEventReader}
	 * @throws IOException
	 *           if I/O operation fails
	 * @throws XMLStreamException
	 *           if any parsing error occurs
	 */
	public static synchronized XMLEventReader createStringReader(
			@Nonnull final String pString) throws IOException, XMLStreamException {
		checkNotNull(pString);
		final XMLInputFactory factory = XMLInputFactory.newInstance();
		factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
		factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
		final InputStream in = new ByteArrayInputStream(pString.getBytes());
		return factory.createXMLEventReader(in);
	}

	/**
	 * Create a new StAX reader based on a List of {@link XMLEvent}s.
	 * 
	 * @param pEvents
	 *          {@link XMLEvent}s
	 * @return an {@link XMLEventReader}
	 * @throws IOException
	 *           if I/O operation fails
	 * @throws XMLStreamException
	 *           if any parsing error occurs
	 */
	public static synchronized XMLEventReader createQueueReader(
			@Nonnull final Queue<XMLEvent> pEvents) throws IOException,
			XMLStreamException {
		return new QueueEventReader(checkNotNull(pEvents));
	}
}

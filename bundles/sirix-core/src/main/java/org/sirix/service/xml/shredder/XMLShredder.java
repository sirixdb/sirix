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
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.node.ElementNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * This class appends a given {@link XMLStreamReader} to a {@link INodeWriteTrx}
 * . The content of the stream is added as a subtree. Based on an enum which
 * identifies the point of insertion, the subtree is either added as first child
 * or as right sibling.
 * 
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class XMLShredder extends AbsShredder implements Callable<Long> {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(XMLShredder.class));

	/** {@link INodeWriteTrx}. */
	protected final INodeWriteTrx mWtx;

	/** {@link XMLEventReader}. */
	protected final XMLEventReader mReader;

	/** Append as first child or not. */
	protected final EInsert mFirstChildAppend;

	/** Determines if changes are going to be commit right after shredding. */
	private final EShredderCommit mCommit;

	/**
	 * Normal constructor to invoke a shredding process on a existing
	 * {@link NodeWriteTrx}.
	 * 
	 * @param pWtx
	 *          where the new XML Fragment should be placed
	 * @param pReader
	 *          of the XML Fragment
	 * @param pAddAsFirstChild
	 *          if the insert is occuring on a node in an existing tree.
	 *          <code>false</code> is not possible when wtx is on root node.
	 * @throws IllegalStateException
	 *           if insertasfirstChild && updateOnly is both true OR if wtx is not
	 *           pointing to doc-root and updateOnly= true
	 */
	public XMLShredder(@Nonnull final INodeWriteTrx pWtx,
			@Nonnull final XMLEventReader pReader,
			@Nonnull final EInsert pAddAsFirstChild) {
		this(pWtx, pReader, pAddAsFirstChild, EShredderCommit.COMMIT);
	}

	/**
	 * Normal constructor to invoke a shredding process on a existing
	 * {@link NodeWriteTrx}.
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} where the new XML Fragment should be placed
	 * @param pReader
	 *          {@link XMLEventReader} to parse the xml fragment, which should be
	 *          inserted
	 * @param pAddAsFirstChild
	 *          determines if the insert is occuring on a node in an existing
	 *          tree. <code>false</code> is not possible when wtx is on root node
	 * @param pCommit
	 *          determines if inserted nodes should be commited right afterwards
	 * @throws SirixUsageException
	 *           if insertasfirstChild && updateOnly is both true OR if wtx is not
	 *           pointing to doc-root and updateOnly= true
	 */
	public XMLShredder(@Nonnull final INodeWriteTrx pWtx,
			@Nonnull final XMLEventReader pReader,
			@Nonnull final EInsert pAddAsFirstChild,
			@Nonnull final EShredderCommit pCommit) {
		super(pWtx, pAddAsFirstChild);
		mWtx = pWtx; // Checked for null in AbsShredder.
		mReader = checkNotNull(pReader);
		mFirstChildAppend = pAddAsFirstChild; // Checked for null in AbsShredder.
		mCommit = checkNotNull(pCommit);
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
		// try {
		boolean firstElement = true;
		int level = 0;
		QName rootElement = null;
		boolean endElemReached = false;
		final StringBuilder sBuilder = new StringBuilder();
		long insertedRootNodeKey = -1;

		System.out.println("STARTEEED!");

		// Iterate over all nodes.
		try {
			while (mReader.hasNext() && !endElemReached) {
				final XMLEvent event = mReader.nextEvent();

				switch (event.getEventType()) {
				case XMLStreamConstants.START_ELEMENT:
					level++;
					addNewElement(event.asStartElement());
					if (firstElement) {
						firstElement = false;
						insertedRootNodeKey = mWtx.getNode().getNodeKey();
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
				default:
					// Node kind not known.
				}
			}
		} catch (final Exception e) {
			System.out.println("BLABLABLABLA");
			e.printStackTrace();
		} catch (final OutOfMemoryError e) {
			System.out.println("ERROOOOR");
			e.printStackTrace();
			System.out.println(e.getCause());
			System.out.println(e);
			throw new IllegalStateException(e);
		}

		System.out.println("DOOOONE!");

		mWtx.moveTo(insertedRootNodeKey);
		// } catch (final XMLStreamException e) {
		// throw new SirixIOException(e);
		// }
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
	 * @throws Exception
	 *           if any exception occurs
	 */
	public static void main(final String... pArgs) {
		if (pArgs.length != 2) {
			throw new IllegalArgumentException("Usage: XMLShredder XMLFile Database");
		}
		try {
			System.out.println("shredding");
			LOGWRAPPER
					.info("Shredding '" + pArgs[0] + "' to '" + pArgs[1] + "' ... ");
			final long time = System.nanoTime();
			final File target = new File(pArgs[1]);
			System.out.println("shredding");
			final DatabaseConfiguration config = new DatabaseConfiguration(target);
			Database.truncateDatabase(config);
			System.out.println("shredding");
			Database.createDatabase(config);
			System.out.println("shredding");
			final IDatabase db = Database.openDatabase(target);
			db.createResource(new ResourceConfiguration.Builder("shredded", config)
					.build());
			final ISession session = db.getSession(new SessionConfiguration.Builder(
					"shredded").build());
			final INodeWriteTrx wtx = session.beginNodeWriteTrx();
			final XMLEventReader reader = createFileReader(new File(pArgs[0]));
			System.out.println("shredding!!!!!!!!!!");
			final XMLShredder shredder = new XMLShredder(wtx, reader,
					EInsert.ASFIRSTCHILD);
			shredder.call();
			System.out.println("DONE DONE shredding!!!!!!!!!!");
			wtx.close();
			System.out.println("CLOSE wtx DONE shredding!!!!!!!!!!");
			session.close();
			System.out.println("CLOSE session DONE shredding!!!!!!!!!!");
			db.close();
			System.out.println("CLOSE database DONE shredding!!!!!!!!!!");

			LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1000000
					+ " ms].");
		} catch (final Exception e) {
			System.out.println("BLABLA");
			e.printStackTrace();
		}

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

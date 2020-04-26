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
package org.sirix.wikipedia.hadoop;

import java.io.IOException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sirix.utils.LogWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * <p>
 * Appends values to an output file.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLRecordWriter<K, V> extends RecordWriter<K, V> {

	/** Logger. */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(XMLRecordWriter.class);

	/**
	 * Log wrapper {@link LogWrapper}.
	 */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(LOGGER);

	/** Writer to write XML events {@link XMLEventWriter}. */
	private transient XMLEventWriter mWriter;

	/** Factory to create events {@link XMLEventFactory}. */
	private transient XMLEventFactory mEventFactory;

	/** Full qualified name of root element {@link QName}. */
	private transient StartElement mRootElem;

	/**
	 * Constructor.
	 * 
	 * @param paramWriter
	 *          Instance of {@link XMLEventWriter}.
	 * @param paramRootElem
	 *          Root element.
	 * @throws IOException
	 *           In case any I/O operation fails.
	 * @throws XMLStreamException
	 *           In case any error occurs while creating events.
	 */
	public XMLRecordWriter(final XMLEventWriter paramWriter,
			final StartElement paramRootElem) throws IOException, XMLStreamException {
		mWriter = paramWriter;
		mEventFactory = XMLEventFactory.newInstance();
		mRootElem = paramRootElem;
		mWriter.add(mRootElem);
	}

	@Override
	public synchronized void close(final TaskAttemptContext paramContext)
			throws IOException, InterruptedException {
		try {
			mWriter.add(mEventFactory.createEndElement(mRootElem.getName(), null));
			mWriter.flush();
		} catch (final XMLStreamException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	public synchronized void write(final K paramKey, final V paramValue)
			throws IOException, InterruptedException {
		if (paramValue instanceof XMLEvent) {
			final XMLEvent[] events = (XMLEvent[]) paramValue;
			for (final XMLEvent event : events) {
				try {
					mWriter.add(event);
				} catch (final XMLStreamException e) {
					LOGWRAPPER.error(e.getMessage(), e);
				}
			}
		}
	}

}

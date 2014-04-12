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

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.sirix.utils.LogWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>XMLOutputFormat</h1>
 * 
 * <p>
 * Outputs only values and appends them
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public class XMLOutputFormat<K, V> extends FileOutputFormat<K, V> {

	/** Logger. */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(XMLOutputFormat.class);

	/**
	 * Log wrapper {@link LogWrapper}.
	 */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(LOGGER);

	// /** Root element {@link QName}. */
	// private final transient StartElement mRoot;
	//
	// /**
	// * Empty constructor.
	// *
	// * @param paramRootElem
	// * Root element.
	// */
	// public XMLOutputFormat(final StartElement paramRootElem) {
	// mRoot = paramRootElem;
	// }

	/**
	 * Default constructor.
	 */
	public XMLOutputFormat() {
		// To make Checkstyle happy.
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(
			final TaskAttemptContext paramContext) throws IOException,
			InterruptedException {
		final Path file = FileOutputFormat.getOutputPath(paramContext);
		final FileSystem fs = file.getFileSystem(paramContext.getConfiguration());
		final FSDataOutputStream out = fs.create(file);
		final XMLOutputFactory factory = XMLOutputFactory.newInstance();
		XMLEventWriter writer = null;
		XMLRecordWriter<K, V> recordWriter = null;
		final StartElement root = XMLEventFactory.newFactory().createStartElement(
				paramContext.getConfiguration().get("root"), null, null);
		try {
			writer = factory.createXMLEventWriter(out);
			recordWriter = new XMLRecordWriter<K, V>(writer, root);
		} catch (final XMLStreamException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return recordWriter;
	}

}

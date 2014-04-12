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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * <h1>InputFormat</h1>
 * 
 * <p>
 * Custom input format, which splits XML files. Key is a date/timestamp, value
 * the according subtree of a matching elemt, which is configurable.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLInputFormat extends FileInputFormat<DateWritable, Text> {

	static {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
	}

	/**
	 * Empty constructor.
	 */
	public XMLInputFormat() {
		// Default constructor.
	}

	@Override
	protected boolean isSplitable(final JobContext pContext, final Path pFilename) {
		return false;
	}

	@Override
	public RecordReader<DateWritable, Text> createRecordReader(
			final InputSplit paramSplit, final TaskAttemptContext paramContext)
			throws IOException, InterruptedException {
		final RecordReader<DateWritable, Text> reader = new XMLRecordReader();
		reader.initialize(paramSplit, paramContext);
		return reader;
	}
}

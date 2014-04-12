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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.custommonkey.xmlunit.XMLTestCase;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.utils.Files;

public final class TestSortWiki extends XMLTestCase {

	private static final File INPUT = new File("src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "testInput.xml");

	private static final File OUTPUT = new File(TestHelper.PATHS.PATH3.getFile()
			.getParentFile().getAbsolutePath());

	private static final File EXPECTED = new File("src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "testExpected.xml");

	@Override
	@Before
	public void setUp() throws Exception {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
		XMLUnit.setIgnoreWhitespace(true);
	}

	//
	// @Override
	// @After
	// public void tearDown() throws Exception {
	// }

	@Test
	public void test() throws Exception {
		final Job job = new Job(new Configuration());
		job.setJarByClass(this.getClass());
		job.setJobName(this.getClass().getName());

		// Map output.
		job.setMapOutputKeyClass(DateWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce output.
		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(XMLMap.class);
		job.setReducerClass(XMLReduce.class);

		job.setInputFormatClass(XMLInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		final Configuration config = job.getConfiguration();
		config.set("timestamp", "timestamp");
		config.set("page", "page");
		config.set("record_element_name", "revision");
		config.set("namespace_prefix", "");
		config.set("namespace_URI", "");
		config.set("root", "mediawiki");

		if (OUTPUT.exists()) {
			Files.recursiveRemove(OUTPUT.toPath());
		}

		FileInputFormat.setInputPaths(job, new Path(INPUT.getAbsolutePath()));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT.getAbsolutePath()));

		assertTrue("Job completed without any exceptions",
				job.waitForCompletion(false));
		final StringBuilder output = TestHelper.readFile(new File(OUTPUT,
				"part-r-00000"), false);
		final StringBuilder expected = TestHelper.readFile(EXPECTED, false);
		output.insert(0, "<root>").append("</root>");
		System.out.println(output.toString());
		System.out.println(expected.toString());
		assertXMLEqual("XML files are at least similar", expected.toString(),
				output.toString());
	}
}

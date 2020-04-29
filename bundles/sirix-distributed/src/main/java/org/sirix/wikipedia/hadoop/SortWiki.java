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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sirix.exception.SirixIOException;
import org.sirix.utils.Files;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * 
 * <p>
 * Sort Wikipedia pages meta history dump according to the timestamps of the
 * revisions.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SortWiki extends Configured implements Tool {

	static {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
	}

	/**
	 * {@link LogWrapper} used for logging.
	 */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DateWritable.class));

	/**
	 * Main method.
	 * 
	 * @param args
	 *          program arguments
	 * @throws Exception
	 *           if an exception occurs while running Hadoop
	 */
	public static void main(final String[] args) throws Exception {
		final long start = System.nanoTime();
		LOGWRAPPER.info("Running...");
		final int res = ToolRunner.run(new Configuration(), new SortWiki(), args);
		LOGWRAPPER.info("Result: " + res);
		LOGWRAPPER.info("Done in " + (System.nanoTime() - start) / 1_000_000_000
				+ "s");
	}

	@Override
	public int run(final String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		final Job job = new Job(getConf());
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
		config.set("namespace_URI", "http://www.mediawiki.org/xml/export-0.4/");
		config.set("root", "mediawiki");

		// Debug settings.
		config.set("mapred.job.tracker", "local");
		config.set("fs.default.name", "local");

		// First delete target directory.
		try {
			Files.recursiveRemove(new File(args[1]).toPath());
		} catch (final SirixIOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		final boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}

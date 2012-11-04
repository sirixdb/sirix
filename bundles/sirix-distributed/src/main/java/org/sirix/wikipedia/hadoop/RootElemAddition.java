package org.sirix.wikipedia.hadoop;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Add a root-element to the sorted wikipedia-document.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class RootElemAddition {
	
	/** Wikipedia XML document. */
	private static final File WIKIPEDIA_DOC = new File(new StringBuilder(
			System.getProperty("user.home")).append(File.separator)
			.append("Desktop").append(File.separator)
			.append("wiki-articles-sorted.xml").toString());
	
	/**
	 * Main method.
	 * 
	 * @param args
	 *          args[0]: input file
	 * @throws IOException
	 * 				if an I/O error occured
	 */
	public static void main(final String[] args) throws IOException {
		final FileWriter writer = new FileWriter(WIKIPEDIA_DOC);
		writer
				.write("<mediawiki xmlns='http://www.mediawiki.org/xml/export-0.5/'>");
		final FileReader reader = new FileReader(args[0]);
		for (int c, i = 0; (c = reader.read()) != -1; i++) {
			writer.write(c);
			if (i % 1_000_000 == 0) {
				writer.flush();
			}
		}
		reader.close();
		writer.write("</mediawiki>");
		writer.flush();
		writer.close();
	}
}

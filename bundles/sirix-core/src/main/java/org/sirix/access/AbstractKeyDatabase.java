package org.sirix.access;

import java.io.File;

import org.sirix.access.conf.DatabaseConfiguration;

/**
 * Abstract class for holding common data for all key databases involved in
 * encryption process. Each instance of this class stores the data in a place
 * related to the {@link DatabaseConfiguration} at a different subfolder.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public abstract class AbstractKeyDatabase {

	/**
	 * Place to store the data.
	 */
	protected final File place;

	/**
	 * Counter to give every instance a different place.
	 */
	private static int counter;

	/**
	 * Constructor with the place to store the data.
	 * 
	 * @param file
	 *          {@link File} which holds the place to store the data.
	 */
	protected AbstractKeyDatabase(final File file) {
		place = new File(file, new StringBuilder(
				DatabaseConfiguration.Paths.KEYSELECTOR.getFile().getName())
				.append(File.separator).append(counter).toString());
		place.mkdirs();
		counter++;
	}

}

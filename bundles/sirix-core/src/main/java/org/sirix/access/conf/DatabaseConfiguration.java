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

package org.sirix.access.conf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.annotation.Nullable;

import org.sirix.exception.SirixIOException;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * <h1>Database Configuration</h1>
 *
 * <p>
 * Represents a configuration of a database. Includes all settings which have to
 * be made during the creation of the database.
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 */
public final class DatabaseConfiguration {

	/**
	 * Paths for a {@link org.DatabaseImpl.Database}. Each
	 * {@link org.DatabaseImpl.Database} has the same folder layout.
	 */
	public enum Paths {

		/** File to store db settings. */
		CONFIGBINARY(new File("dbsetting.obj"), false),
		/** File to store encryption db settings. */
		KEYSELECTOR(new File("keyselector"), true),
		/** File to store the data. */
		DATA(new File("resources"), true),
		/** Lock file. */
		LOCK(new File(".lock"), false);

		/** Location of the file. */
		private final File mFile;

		/** Is the location a folder or no? */
		private final boolean mIsFolder;

		/**
		 * Constructor.
		 *
		 * @param file
		 *          to be set
		 * @param isFolder
		 *          determines if the file is a folder instead
		 */
		private Paths(final File file, final boolean isFolder) {
			mFile = checkNotNull(file);
			mIsFolder = isFolder;
		}

		/**
		 * Getting the file for the kind.
		 *
		 * @return the file to the kind
		 */
		public File getFile() {
			return mFile;
		}

		/**
		 * Check if file is denoted as folder or not.
		 *
		 * @return boolean if file is folder
		 */
		public boolean isFolder() {
			return mIsFolder;
		}

		/**
		 * Checking a structure in a folder to be equal with the data in this enum.
		 *
		 * @param file
		 *          to be checked
		 * @return -1 if less folders are there, 0 if the structure is equal to the
		 *         one expected, 1 if the structure has more folders
		 */
		public static int compareStructure(final File file) {
			checkNotNull(file);
			int existing = 0;
			for (final Paths paths : values()) {
				final File currentFile = new File(file, paths.getFile().getName());
				if (currentFile.exists()
						&& !Paths.LOCK.getFile().getName().equals(currentFile.getName())) {
					existing++;
				}
			}
			return existing - values().length + 1;
		}
	}

	// STATIC STANDARD FIELDS
	/** Identification for string. */
	public static final String BINARY = "0.1.0";
	// END STATIC STANDARD FIELDS

	/** Binary version of storage. */
	private final String mBinaryVersion;

	/** Path to file. */
	private final File mFile;

	/** Maximum unique resource ID. */
	private long mMaxResourceID;

	/**
	 * Constructor with the path to be set.
	 *
	 * @param file
	 *          file to be set
	 */
	public DatabaseConfiguration(final File file) {
		mBinaryVersion = BINARY;
		mFile = file.getAbsoluteFile();
	}

	/**
	 * Set unique maximum resource ID.
	 *
	 * @param id
	 *          maximum resource ID
	 * @return this {@link DatabaseConfiguration} reference
	 */
	public DatabaseConfiguration setMaximumResourceID(final long id) {
		checkArgument(id >= 0, "pID must be >= 0!");
		mMaxResourceID = id;
		return this;
	}

	/**
	 * Set unique maximum resource ID.
	 *
	 * @return maximum resource ID
	 */
	public long getMaxResourceID() {
		return mMaxResourceID;
	}

	/**
	 * Getting the database file.
	 *
	 * @return the database file
	 */
	public File getFile() {
		return mFile;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("File", mFile)
				.add("Binary Version", mBinaryVersion).toString();
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof DatabaseConfiguration) {
			final DatabaseConfiguration other = (DatabaseConfiguration) obj;
			return Objects.equal(mFile, other.mFile)
					&& Objects.equal(mBinaryVersion, other.mBinaryVersion);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mFile, mBinaryVersion);
	}

	/**
	 * Get the configuration file.
	 *
	 * @return configuration file
	 */
	public File getConfigFile() {
		return new File(mFile, Paths.CONFIGBINARY.getFile().getName());
	}

	/**
	 * Serializing a {@link DatabaseConfiguration} to a json file.
	 *
	 * @param config
	 *          to be serialized
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static void serialize(final DatabaseConfiguration config)
			throws SirixIOException {
		try (final FileWriter fileWriter = new FileWriter(config.getConfigFile());
				final JsonWriter jsonWriter = new JsonWriter(fileWriter);) {
			jsonWriter.beginObject();
			final String filePath = config.mFile.getAbsolutePath();
			jsonWriter.name("file").value(filePath);
			jsonWriter.name("ID").value(config.mMaxResourceID);
			jsonWriter.endObject();
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	/**
	 * Generate a DatabaseConfiguration out of a file.
	 *
	 * @param file
	 *          where the DatabaseConfiguration lies in as json
	 * @return a new {@link DatabaseConfiguration} class
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static DatabaseConfiguration deserialize(final File file)
			throws SirixIOException {
		try (final FileReader fileReader = new FileReader(new File(file,
				Paths.CONFIGBINARY.getFile().getName()));
				final JsonReader jsonReader = new JsonReader(fileReader);) {
			jsonReader.beginObject();
			final String fileName = jsonReader.nextName();
			assert fileName.equals("file");
			final File dbFile = new File(jsonReader.nextString());
			final String IDName = jsonReader.nextName();
			assert IDName.equals("ID");
			final int ID = jsonReader.nextInt();
			jsonReader.endObject();
			return new DatabaseConfiguration(dbFile).setMaximumResourceID(ID);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}
}

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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.HashKind;
import org.sirix.access.SessionImpl;
import org.sirix.exception.SirixIOException;
import org.sirix.io.EStorage;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.DeflateCompressor;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.settings.ERevisioning;

import com.google.common.base.Objects;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * <h1>ResourceConfiguration</h1>
 * 
 * <p>
 * Holds the settings for a resource which acts as a base for session that can
 * not change. This includes all settings which are persistent. Each
 * {@link ResourceConfiguration} is furthermore bound to one fixed database
 * denoted by a related {@link DatabaseConfiguration}.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 */
public final class ResourceConfiguration {
	
	/**
	 * Paths for a {@link SessionImpl}. Each resource has the same folder layout.
	 */
	public enum Paths {

		/** Folder for storage of data. */
		Data(new File("data"), true),

		/** Folder for transaction log. */
		TransactionLog(new File("log"), true),

		/** File to store the resource settings. */
		ConfigBinary(new File("ressetting.obj"), false);

		/** Location of the file. */
		private final File mFile;

		/** Is the location a folder or no? */
		private final boolean mIsFolder;

		/**
		 * Constructor.
		 * 
		 * @param pFile
		 *          to be set
		 * @param pIsFolder
		 *          to be set.
		 */
		private Paths(final @Nonnull File pFile, final boolean pIsFolder) {
			mFile = checkNotNull(pFile);
			mIsFolder = checkNotNull(pIsFolder);
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
		 * @throws NullPointerException
		 *           if {@code pFile} is {@code null}
		 */
		public static int compareStructure(final @Nonnull File file) {
			int existing = 0;
			for (final Paths paths : values()) {
				final File currentFile = new File(file, paths.getFile().getName());
				if (currentFile.exists()) {
					existing++;
				}
			}
			return existing - values().length;
		}
	}

	/** Indexes to use. */
	public enum EIndexes {
		/** Path summary index. */
		PATH,

		/** Value index. */
		VALUE,

		/** No index. */
		NONE
	}

	// FIXED STANDARD FIELDS
	/** Standard storage. */
	public static final EStorage STORAGE = EStorage.File;

	/** Standard Versioning Approach. */
	public static final ERevisioning VERSIONING = ERevisioning.INCREMENTAL;

	/** Type of hashing. */
	public static final HashKind HASHKIND = HashKind.Rolling;

	/** Versions to restore. */
	public static final int VERSIONSTORESTORE = 3;

	/** Indexes to use. */
	public static final Set<EIndexes> INDEXES = EnumSet.of(EIndexes.PATH);
	// END FIXED STANDARD FIELDS

	// MEMBERS FOR FIXED FIELDS
	/** Type of Storage (File, BerkeleyDB). */
	public final EStorage mStorage;

	/** Kind of revisioning (Full, Incremental, Differential). */
	public final ERevisioning mRevisionKind;

	/** Kind of integrity hash (rolling, postorder). */
	public final HashKind mHashKind;

	/** Number of revisions to restore a complete set of data. */
	public final int mRevisionsToRestore;

	/** Byte handler pipeline. */
	public final ByteHandlePipeline mByteHandler;

	/** Path for the resource to be associated. */
	public final File mPath;

	/** DatabaseConfiguration for this {@link ResourceConfiguration}. */
	public final DatabaseConfiguration mDBConfig;

	/** Determines if text-compression should be used or not (default is true). */
	public final boolean mCompression;

	/** Indexes to use. */
	public final Set<EIndexes> mIndexes;

	/** Unique ID. */
	private long mID;

	// END MEMBERS FOR FIXED FIELDS

	/**
	 * Convenience constructor using the standard settings.
	 * 
	 * @param builder
	 *          {@link Builder} reference
	 */
	private ResourceConfiguration(
			final @Nonnull ResourceConfiguration.Builder builder) {
		mStorage = builder.mType;
		mByteHandler = builder.mByteHandler;
		mRevisionKind = builder.mRevisionKind;
		mHashKind = builder.mHashKind;
		mRevisionsToRestore = builder.mRevisionsToRestore;
		mDBConfig = builder.mDBConfig;
		mCompression = builder.mCompression;
		mIndexes = builder.mIndexes;
		mPath = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.Data.getFile().getName()),
				builder.mResource);
	}

	/**
	 * Set a unique ID.
	 * 
	 * @param id
	 *          the ID to set
	 * @return this instance
	 */
	public ResourceConfiguration setID(final @Nonnegative long id) {
		checkArgument(id >= 0, "pID must be >= 0!");
		mID = id;
		return this;
	}
	
	/**
	 * Get the unique ID.
	 * 
	 * @return the unique resource ID
	 */
	public long getID() {
		return mID;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mStorage, mRevisionKind, mHashKind, mPath,
				mDBConfig);
	}

	@Override
	public final boolean equals(final Object obj) {
		if (obj instanceof ResourceConfiguration) {
			final ResourceConfiguration other = (ResourceConfiguration) obj;
			return Objects.equal(mStorage, other.mStorage)
					&& Objects.equal(mRevisionKind, other.mRevisionKind)
					&& Objects.equal(mHashKind, other.mHashKind)
					&& Objects.equal(mPath, other.mPath)
					&& Objects.equal(mDBConfig, other.mDBConfig);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("Resource", mPath)
				.add("Type", mStorage).add("Revision", mRevisionKind)
				.add("HashKind", mHashKind).toString();
	}

	/**
	 * Get resource.
	 * 
	 * @return resource
	 */
	public File getResource() {
		return mPath;
	}

	/**
	 * Get the configuration file.
	 * 
	 * @return configuration file
	 */
	public File getConfigFile() {
		return new File(mPath, Paths.ConfigBinary.getFile().getName());
	}

	/**
	 * JSON names.
	 */
	private static final String[] JSONNAMES = { "revisioning",
			"revisioningClass", "numbersOfRevisiontoRestore", "byteHandlerClasses",
			"storageKind", "hashKind", "compression", "dbConfig", "ID" };

	/**
	 * Serialize the configuration.
	 * 
	 * @param pConfig
	 *          configuration to serialize
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static void serialize(final @Nonnull ResourceConfiguration pConfig)
			throws SirixIOException {
		final File configFile = pConfig.getConfigFile();
		try (final FileWriter fileWriter = new FileWriter(configFile);
				final JsonWriter jsonWriter = new JsonWriter(fileWriter);) {
			jsonWriter.beginObject();
			// Versioning.
			jsonWriter.name(JSONNAMES[0]);
			jsonWriter.beginObject();
			jsonWriter.name(JSONNAMES[1]).value(pConfig.mRevisionKind.name());
			jsonWriter.name(JSONNAMES[2]).value(pConfig.mRevisionsToRestore);
			jsonWriter.endObject();
			// ByteHandlers.
			final ByteHandlePipeline byteHandler = pConfig.mByteHandler;
			jsonWriter.name(JSONNAMES[3]);
			jsonWriter.beginArray();
			for (final ByteHandler handler : byteHandler.getComponents()) {
				jsonWriter.value(handler.getClass().getName());
			}
			jsonWriter.endArray();
			// Storage type.
			jsonWriter.name(JSONNAMES[4]).value(pConfig.mStorage.name());
			// Hashing type.
			jsonWriter.name(JSONNAMES[5]).value(pConfig.mHashKind.name());
			// Text compression.
			jsonWriter.name(JSONNAMES[6]).value(pConfig.mCompression);
			// Indexes.
			jsonWriter.name(JSONNAMES[7]);
			jsonWriter.beginArray();
			for (final EIndexes index : pConfig.mIndexes) {
				jsonWriter.value(index.name());
			}
			jsonWriter.endArray();
			// ID.
			jsonWriter.name(JSONNAMES[8]).value(pConfig.mID);
			jsonWriter.endObject();
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}

		// Database config.
		DatabaseConfiguration.serialize(pConfig.mDBConfig);
	}

	/**
	 * Deserializing a Resourceconfiguration from a JSON-file from the persistent
	 * storage. The order is important and the reader is passed through the
	 * objects as visitor.
	 * 
	 * @param pFile
	 *          where the resource lies in.
	 * @return a complete {@link ResourceConfiguration} instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static ResourceConfiguration deserialize(final @Nonnull File pFile)
			throws SirixIOException {
		try {
			final File configFiler = new File(pFile, Paths.ConfigBinary.getFile()
					.getName());
			final FileReader fileReader = new FileReader(configFiler);
			final JsonReader jsonReader = new JsonReader(fileReader);
			jsonReader.beginObject();
			// Versioning.
			String name = jsonReader.nextName();
			assert name.equals(JSONNAMES[0]);
			jsonReader.beginObject();
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[1]);
			final ERevisioning revisioning = ERevisioning.valueOf(jsonReader
					.nextString());
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[2]);
			final int revisionToRestore = jsonReader.nextInt();
			jsonReader.endObject();
			// ByteHandlers.
			final List<ByteHandler> handlerList = new ArrayList<>();
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[3]);
			jsonReader.beginArray();
			while (jsonReader.hasNext()) {
				final Class<?> handlerClazz = Class.forName(jsonReader.nextString());
				final Constructor<?> handlerCons = handlerClazz.getConstructors()[0];
				handlerList.add((ByteHandler) handlerCons.newInstance());
			}
			jsonReader.endArray();
			final ByteHandlePipeline pipeline = new ByteHandlePipeline(
					handlerList.toArray(new ByteHandler[handlerList.size()]));
			// Storage type.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[4]);
			final EStorage storage = EStorage.valueOf(jsonReader.nextString());
			// Hashing type.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[5]);
			final HashKind hashing = HashKind.valueOf(jsonReader.nextString());
			// Text compression.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[6]);
			final boolean compression = jsonReader.nextBoolean();
			// Indexes.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[7]);
			final List<EIndexes> listIndexes = new ArrayList<>();
			jsonReader.beginArray();
			while (jsonReader.hasNext()) {
				listIndexes.add(EIndexes.valueOf(jsonReader.nextString()));
			}
			final Set<EIndexes> indexes = EnumSet.copyOf(listIndexes);
			jsonReader.endArray();
			// Unique ID.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[8]);
			final int ID = jsonReader.nextInt();
			jsonReader.endObject();
			jsonReader.close();
			fileReader.close();

			// Deserialize database config.
			final DatabaseConfiguration dbConfig = DatabaseConfiguration
					.deserialize(pFile.getParentFile().getParentFile());

			// Builder.
			final ResourceConfiguration.Builder builder = new ResourceConfiguration.Builder(
					pFile.getName(), dbConfig);
			builder.setByteHandlerPipeline(pipeline);
			builder.setHashKind(hashing);
			builder.setIndexes(indexes);
			builder.setRevisionKind(revisioning);
			builder.setRevisionsToRestore(revisionToRestore);
			builder.setType(storage);
			builder.useCompression(compression);

			// Deserialized instance.
			final ResourceConfiguration config = new ResourceConfiguration(builder);
			return config.setID(ID);
		} catch (IOException | ClassNotFoundException | IllegalArgumentException
				| InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new SirixIOException(e);
		}
	}

	/**
	 * Builder class for generating new {@link ResourceConfiguration} instance.
	 */
	public static final class Builder {

		/** Type of Storage (File, Berkeley). */
		private EStorage mType = STORAGE;

		/** Kind of revisioning (Incremental, Differential). */
		private ERevisioning mRevisionKind = VERSIONING;

		/** Kind of integrity hash (rolling, postorder). */
		private HashKind mHashKind = HASHKIND;

		/** Number of revisions to restore a complete set of data. */
		private int mRevisionsToRestore = VERSIONSTORESTORE;

		/** Resource for this session. */
		private final String mResource;

		/** Resource for this session. */
		private final DatabaseConfiguration mDBConfig;

		/** Determines if text-compression should be used or not (default is true). */
		private boolean mCompression;

		/** Indexes to use. */
		private Set<EIndexes> mIndexes = INDEXES;

		/** Byte handler pipeline. */
		private ByteHandlePipeline mByteHandler = new ByteHandlePipeline(
				new DeflateCompressor());

		/**
		 * Constructor, setting the mandatory fields.
		 * 
		 * @param pResource
		 *          the name of the resource, must to be set.
		 * @param pConfig
		 *          the related {@link DatabaseConfiguration}, must to be set.
		 */
		public Builder(final @Nonnull String pResource,
				final @Nonnull DatabaseConfiguration pConfig) {
			mResource = checkNotNull(pResource);
			mDBConfig = checkNotNull(pConfig);
		}

		/**
		 * Set the storage type.
		 * 
		 * @param pType
		 *          storage type to use
		 * @return reference to the builder object
		 */
		public Builder setType(final @Nonnull EStorage pType) {
			mType = checkNotNull(pType);
			return this;
		}

		/**
		 * Set the indexes to use.
		 * 
		 * @param pIndexes
		 *          indexes to use
		 * @return reference to the builder object
		 */
		public Builder setIndexes(final @Nonnull Set<EIndexes> pIndexes) {
			mIndexes = checkNotNull(pIndexes);
			return this;
		}

		/**
		 * Set the revisioning algorithm to use.
		 * 
		 * @param pRevKind
		 *          revisioning algorithm to use
		 * @return reference to the builder object
		 */
		public Builder setRevisionKind(final @Nonnull ERevisioning pRevKind) {
			mRevisionKind = checkNotNull(pRevKind);
			return this;
		}

		/**
		 * Set the hash kind to use for the nodes.
		 * 
		 * @param pHash
		 *          hash kind to use
		 * @return reference to the builder object
		 */
		public Builder setHashKind(final @Nonnull HashKind pHash) {
			mHashKind = checkNotNull(pHash);
			return this;
		}

		/**
		 * Set the byte handler pipeline.
		 * 
		 * @param pByteHandler
		 *          byte handler pipeline
		 * @return reference to the builder object
		 */
		public Builder setByteHandlerPipeline(
				final @Nonnull ByteHandlePipeline pByteHandler) {
			mByteHandler = checkNotNull(pByteHandler);
			return this;
		}

		/**
		 * Set the number of revisions to restore after the last full dump.
		 * 
		 * @param pRevToRestore
		 *          number of revisions to restore
		 * @return reference to the builder object
		 */
		public Builder setRevisionsToRestore(@Nonnegative final int pRevToRestore) {
			checkArgument(pRevToRestore > 0, "pRevisionsToRestore must be > 0!");
			mRevisionsToRestore = pRevToRestore;
			return this;
		}

		/**
		 * Determines if text-compression should be used or not.
		 * 
		 * @param pCompression
		 *          use text compression or not (default: yes)
		 * @return reference to the builder object
		 */
		public Builder useCompression(final boolean pCompression) {
			mCompression = pCompression;
			return this;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("Type", mType)
					.add("RevisionKind", mRevisionKind).add("HashKind", mHashKind)
					.toString();
		}

		/**
		 * Building a new {@link ResourceConfiguration} with immutable fields.
		 * 
		 * @return a new {@link ResourceConfiguration} instance
		 */
		public ResourceConfiguration build() {
			return new ResourceConfiguration(this);
		}
	}
}

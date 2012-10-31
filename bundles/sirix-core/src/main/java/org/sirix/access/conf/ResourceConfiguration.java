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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.HashKind;
import org.sirix.access.SessionImpl;
import org.sirix.exception.SirixIOException;
import org.sirix.io.StorageType;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.DeflateCompressor;
import org.sirix.node.NodePersistenter;
import org.sirix.node.interfaces.RecordPersistenter;
import org.sirix.settings.Revisioning;

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
 * @author Johannes Lichtenberger
 */
public final class ResourceConfiguration {

	/**
	 * Paths for a {@link SessionImpl}. Each resource has the same folder layout.
	 */
	public enum Paths {

		/** Folder for storage of data. */
		DATA(new File("data"), true),

		/** Folder for transaction log. */
		TRANSACTION_LOG(new File("log"), true),

		/** File to store the resource settings. */
		CONFIG_BINARY(new File("ressetting.obj"), false);

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
	public enum Indexes {
		/** Path summary index. */
		PATH,

		/** Value index. */
		VALUE,

		/** No index. */
		NONE
	}

	// FIXED STANDARD FIELDS
	/** Standard storage. */
	public static final StorageType STORAGE = StorageType.FILE;

	/** Standard Versioning Approach. */
	public static final Revisioning VERSIONING = Revisioning.DIFFERENTIAL;

	/** Type of hashing. */
	public static final HashKind HASHKIND = HashKind.ROLLING;

	/** Versions to restore. */
	public static final int VERSIONSTORESTORE = 3;

	/** Indexes to use. */
	public static final EnumSet<Indexes> INDEXES = EnumSet.of(Indexes.PATH);

	/** Persistenter for records. */
	public static final RecordPersistenter PERSISTENTER = new NodePersistenter();
	// END FIXED STANDARD FIELDS

	// MEMBERS FOR FIXED FIELDS
	/** Type of Storage (File, BerkeleyDB). */
	public final StorageType mStorage;

	/** Kind of revisioning (Full, Incremental, Differential). */
	public final Revisioning mRevisionKind;

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
	public final EnumSet<Indexes> mIndexes;

	/** Persistents records / commonly nodes. */
	public final RecordPersistenter mPersistenter;

	/** Unique ID. */
	private long mID;

	/** Determines if dewey IDs should be stored or not. */
	public final boolean mDeweyIDsStored;

	// END MEMBERS FOR FIXED FIELDS

	/**
	 * Get a new builder instance.
	 * 
	 * @param resource
	 *          the name of the resource
	 * @param config
	 *          the related {@link DatabaseConfiguration}
	 * @throws NullPointerException
	 *           if {@code resource} or {@code config} is {@code null}
	 * @return {@link Builder} instance
	 */
	public static Builder builder(final @Nonnull String resource,
			final @Nonnull DatabaseConfiguration config) {
		return new Builder(resource, config);
	}

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
		mDeweyIDsStored = builder.mUseDeweyIDs;
		mPath = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.DATA.getFile().getName()),
				builder.mResource);
		mPersistenter = builder.mPersistenter;
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
		return new File(mPath, Paths.CONFIG_BINARY.getFile().getName());
	}

	/**
	 * JSON names.
	 */
	private static final String[] JSONNAMES = { "revisioning",
			"revisioningClass", "numbersOfRevisiontoRestore", "byteHandlerClasses",
			"storageKind", "hashKind", "compression", "dbConfig", "resourceID",
			"deweyIDsStored", "persistenter" };

	/**
	 * Serialize the configuration.
	 * 
	 * @param config
	 *          configuration to serialize
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static void serialize(final @Nonnull ResourceConfiguration config)
			throws SirixIOException {
		final File configFile = config.getConfigFile();
		try (final FileWriter fileWriter = new FileWriter(configFile);
				final JsonWriter jsonWriter = new JsonWriter(fileWriter);) {
			jsonWriter.beginObject();
			// Versioning.
			jsonWriter.name(JSONNAMES[0]);
			jsonWriter.beginObject();
			jsonWriter.name(JSONNAMES[1]).value(config.mRevisionKind.name());
			jsonWriter.name(JSONNAMES[2]).value(config.mRevisionsToRestore);
			jsonWriter.endObject();
			// ByteHandlers.
			final ByteHandlePipeline byteHandler = config.mByteHandler;
			jsonWriter.name(JSONNAMES[3]);
			jsonWriter.beginArray();
			for (final ByteHandler handler : byteHandler.getComponents()) {
				jsonWriter.value(handler.getClass().getName());
			}
			jsonWriter.endArray();
			// Storage type.
			jsonWriter.name(JSONNAMES[4]).value(config.mStorage.name());
			// Hashing type.
			jsonWriter.name(JSONNAMES[5]).value(config.mHashKind.name());
			// Text compression.
			jsonWriter.name(JSONNAMES[6]).value(config.mCompression);
			// Indexes.
			jsonWriter.name(JSONNAMES[7]);
			jsonWriter.beginArray();
			for (final Indexes index : config.mIndexes) {
				jsonWriter.value(index.name());
			}
			jsonWriter.endArray();
			// ID.
			jsonWriter.name(JSONNAMES[8]).value(config.mID);
			// Dewey IDs stored or not.
			jsonWriter.name(JSONNAMES[9]).value(config.mDeweyIDsStored);
			// Persistenter.
			jsonWriter.name(JSONNAMES[10]).value(
					config.mPersistenter.getClass().getName());
			jsonWriter.endObject();
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}

		// Database config.
		DatabaseConfiguration.serialize(config.mDBConfig);
	}

	/**
	 * Deserializing a Resourceconfiguration from a JSON-file from the persistent
	 * storage. The order is important and the reader is passed through the
	 * objects as visitor.
	 * 
	 * @param file
	 *          where the resource lies in.
	 * @return a complete {@link ResourceConfiguration} instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public static ResourceConfiguration deserialize(final @Nonnull File file)
			throws SirixIOException {
		try {
			final File configFiler = new File(file, Paths.CONFIG_BINARY.getFile()
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
			final Revisioning revisioning = Revisioning.valueOf(jsonReader
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
			final StorageType storage = StorageType.valueOf(jsonReader.nextString());
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
			final List<Indexes> listIndexes = new ArrayList<>();
			jsonReader.beginArray();
			while (jsonReader.hasNext()) {
				listIndexes.add(Indexes.valueOf(jsonReader.nextString()));
			}
			final EnumSet<Indexes> indexes = EnumSet.copyOf(listIndexes);
			jsonReader.endArray();
			// Unique ID.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[8]);
			final int ID = jsonReader.nextInt();
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[9]);
			final boolean deweyIDsStored = jsonReader.nextBoolean();
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[10]);
			final Class<?> persistenterClazz = Class.forName(jsonReader.nextString());
			final Constructor<?> persistenterConstr = persistenterClazz
					.getConstructors()[0];
			final RecordPersistenter persistenter = (RecordPersistenter) persistenterConstr
					.newInstance();
			jsonReader.endObject();
			jsonReader.close();
			fileReader.close();

			// Deserialize database config.
			final DatabaseConfiguration dbConfig = DatabaseConfiguration
					.deserialize(file.getParentFile().getParentFile());

			// Builder.
			final ResourceConfiguration.Builder builder = new ResourceConfiguration.Builder(
					file.getName(), dbConfig);
			builder.setByteHandlerPipeline(pipeline).setHashKind(hashing)
					.setIndexes(indexes).setRevisionKind(revisioning)
					.setRevisionsToRestore(revisionToRestore).setType(storage)
					.useTextCompression(compression).setPersistenter(persistenter);
			if (deweyIDsStored) {
				builder.useDeweyIDs(true);
			}

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
		private StorageType mType = STORAGE;

		/** Kind of revisioning (Incremental, Differential). */
		private Revisioning mRevisionKind = VERSIONING;

		/** Kind of integrity hash (rolling, postorder). */
		private HashKind mHashKind = HASHKIND;

		/** Number of revisions to restore a complete set of data. */
		private int mRevisionsToRestore = VERSIONSTORESTORE;

		/** Record/Node persistenter. */
		private RecordPersistenter mPersistenter = PERSISTENTER;

		/** Resource for this session. */
		private final String mResource;

		/** Resource for this session. */
		private final DatabaseConfiguration mDBConfig;

		/** Determines if text-compression should be used or not (default is true). */
		private boolean mCompression = true;

		/** Indexes to use. */
		private EnumSet<Indexes> mIndexes = INDEXES;

		/** Byte handler pipeline. */
		private ByteHandlePipeline mByteHandler = new ByteHandlePipeline(
				new DeflateCompressor());

		/** Determines if DeweyIDs should be used or not. */
		private boolean mUseDeweyIDs = false;

		/**
		 * Constructor, setting the mandatory fields.
		 * 
		 * @param resource
		 *          the name of the resource
		 * @param config
		 *          the related {@link DatabaseConfiguration}
		 * @throws NullPointerException
		 *           if {@code resource} or {@code config} is {@code null}
		 */
		public Builder(final @Nonnull String resource,
				final @Nonnull DatabaseConfiguration config) {
			mResource = checkNotNull(resource);
			mDBConfig = checkNotNull(config);
		}

		/**
		 * Set the storage type.
		 * 
		 * @param type
		 *          storage type to use
		 * @return reference to the builder object
		 */
		public Builder setType(final @Nonnull StorageType type) {
			mType = checkNotNull(type);
			return this;
		}

		public Builder setPersistenter(
				final @Nonnull RecordPersistenter persistenter) {
			mPersistenter = checkNotNull(persistenter);
			return this;
		}

		/**
		 * Set the indexes to use.
		 * 
		 * @param indexes
		 *          indexes to use
		 * @return reference to the builder object
		 */
		public Builder setIndexes(final @Nonnull EnumSet<Indexes> indexes) {
			mIndexes = checkNotNull(indexes);
			return this;
		}

		/**
		 * Set the revisioning algorithm to use.
		 * 
		 * @param revKind
		 *          revisioning algorithm to use
		 * @return reference to the builder object
		 */
		public Builder setRevisionKind(final @Nonnull Revisioning revKind) {
			mRevisionKind = checkNotNull(revKind);
			return this;
		}

		/**
		 * Set the hash kind to use for the nodes.
		 * 
		 * @param hash
		 *          hash kind to use
		 * @return reference to the builder object
		 */
		public Builder setHashKind(final @Nonnull HashKind hash) {
			mHashKind = checkNotNull(hash);
			return this;
		}

		/**
		 * Set the byte handler pipeline.
		 * 
		 * @param byteHandler
		 *          byte handler pipeline
		 * @return reference to the builder object
		 */
		public Builder setByteHandlerPipeline(
				final @Nonnull ByteHandlePipeline byteHandler) {
			mByteHandler = checkNotNull(byteHandler);
			return this;
		}

		/**
		 * Set the number of revisions to restore after the last full dump.
		 * 
		 * @param revToRestore
		 *          number of revisions to restore
		 * @return reference to the builder object
		 */
		public Builder setRevisionsToRestore(final @Nonnegative int revToRestore) {
			checkArgument(revToRestore > 0, "pRevisionsToRestore must be > 0!");
			mRevisionsToRestore = revToRestore;
			return this;
		}

		/**
		 * Determines if DeweyIDs should be stored or not.
		 * 
		 * @return reference to the builder object
		 */
		public Builder useDeweyIDs(final boolean useDeweyIDs) {
			mUseDeweyIDs = useDeweyIDs;
			return this;
		}

		/**
		 * Determines if text-compression should be used or not.
		 * 
		 * @param compression
		 *          use text compression or not (default: yes)
		 * @return reference to the builder object
		 */
		public Builder useTextCompression(final boolean compression) {
			mCompression = compression;
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

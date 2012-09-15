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

import org.sirix.access.EHashKind;
import org.sirix.access.Session;
import org.sirix.exception.TTIOException;
import org.sirix.io.EStorage;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.DeflateCompressor;
import org.sirix.io.bytepipe.IByteHandler;
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
	 * Paths for a {@link Session}. Each resource has the same folder layout.
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
		private Paths(@Nonnull final File pFile, final boolean pIsFolder) {
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
		 * @param pFile
		 *          to be checked
		 * @return -1 if less folders are there, 0 if the structure is equal to the
		 *         one expected, 1 if the structure has more folders
		 */
		public static int compareStructure(final File pFile) {
			int existing = 0;
			for (final Paths paths : values()) {
				final File currentFile = new File(pFile, paths.getFile().getName());
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
	public static final EHashKind HASHKIND = EHashKind.Rolling;

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
	public final EHashKind mHashKind;

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

	// END MEMBERS FOR FIXED FIELDS

	/**
	 * Convenience constructor using the standard settings.
	 * 
	 * @param pBuilder
	 *          {@link Builder} reference
	 */
	private ResourceConfiguration(
			final @Nonnull ResourceConfiguration.Builder pBuilder) {
		mStorage = pBuilder.mType;
		mByteHandler = pBuilder.mByteHandler;
		mRevisionKind = pBuilder.mRevisionKind;
		mHashKind = pBuilder.mHashKind;
		mRevisionsToRestore = pBuilder.mRevisionsToRestore;
		mDBConfig = pBuilder.mDBConfig;
		mCompression = pBuilder.mCompression;
		mIndexes = pBuilder.mIndexes;
		mPath = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.Data.getFile().getName()),
				pBuilder.mResource);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mStorage, mRevisionKind, mHashKind, mPath,
				mDBConfig);
	}

	@Override
	public final boolean equals(final Object pObj) {
		if (this == pObj) {
			return true;
		}
		if (pObj instanceof ResourceConfiguration) {
			final ResourceConfiguration other = (ResourceConfiguration) pObj;
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
			"storageKind", "hashKind", "compression", "dbConfig" };

	/**
	 * Serialize the configuration.
	 * 
	 * @param pConfig
	 *          configuration to serialize
	 * @throws TTIOException
	 *           if an I/O error occurs
	 */
	public static void serialize(final @Nonnull ResourceConfiguration pConfig)
			throws TTIOException {
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
			for (final IByteHandler handler : byteHandler.getComponents()) {
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
			jsonWriter.endObject();
		} catch (final IOException e) {
			throw new TTIOException(e);
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
	 * @throws TTIOException
	 *           if an I/O error occurs
	 */
	public static ResourceConfiguration deserialize(final @Nonnull File pFile)
			throws TTIOException {
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
			final List<IByteHandler> handlerList = new ArrayList<>();
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[3]);
			jsonReader.beginArray();
			while (jsonReader.hasNext()) {
				final Class<?> handlerClazz = Class.forName(jsonReader.nextString());
				final Constructor<?> handlerCons = handlerClazz.getConstructors()[0];
				handlerList.add((IByteHandler) handlerCons.newInstance());
			}
			jsonReader.endArray();
			final ByteHandlePipeline pipeline = new ByteHandlePipeline(
					handlerList.toArray(new IByteHandler[handlerList.size()]));
			// Storage type.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[4]);
			final EStorage storage = EStorage.valueOf(jsonReader.nextString());
			// Hashing type.
			name = jsonReader.nextName();
			assert name.equals(JSONNAMES[5]);
			final EHashKind hashing = EHashKind.valueOf(jsonReader.nextString());
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
			return new ResourceConfiguration(builder);
		} catch (IOException | ClassNotFoundException | IllegalArgumentException
				| InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new TTIOException(e);
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
		private EHashKind mHashKind = HASHKIND;

		/** Number of revisions to restore a complete set of data. */
		private int mRevisionsToRestore = VERSIONSTORESTORE;

		/** Resource for the this session. */
		private final String mResource;

		/** Resource for the this session. */
		private final DatabaseConfiguration mDBConfig;

		/** Determines if text-compression should be used or not (default is true). */
		private boolean mCompression = true;

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
		public Builder(@Nonnull final String pResource,
				@Nonnull final DatabaseConfiguration pConfig) {
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
		public Builder setType(@Nonnull final EStorage pType) {
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
		public Builder setIndexes(@Nonnull final Set<EIndexes> pIndexes) {
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
		public Builder setRevisionKind(@Nonnull final ERevisioning pRevKind) {
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
		public Builder setHashKind(@Nonnull final EHashKind pHash) {
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

/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnegative;
import org.sirix.access.trx.node.HashType;
import org.sirix.exception.SirixIOException;
import org.sirix.io.StorageType;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.ByteHandlerKind;
import org.sirix.io.bytepipe.SnappyCompressor;
import org.sirix.node.NodePersistenterImpl;
import org.sirix.node.interfaces.RecordPersister;
import org.sirix.settings.VersioningType;
import com.google.common.base.MoreObjects;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Holds the settings for a resource which acts as a base for session that can not change. This
 * includes all settings which are persistent. Each {@link ResourceConfiguration} is furthermore
 * bound to one fixed database denoted by a related {@link DatabaseConfiguration}.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class ResourceConfiguration {

  /**
   * Paths in a resource. Each resource has the same folder layout.
   */
  public enum ResourcePaths {

    /** Folder for storage of data. */
    DATA(Paths.get("data"), true),

    /** Folder for the transaction log. */
    TRANSACTION_INTENT_LOG(Paths.get("log"), true),

    /** File to store the resource settings. */
    CONFIG_BINARY(Paths.get("ressetting.obj"), false),

    /** File to store index definitions. */
    INDEXES(Paths.get("indexes"), true),

    /** Folder to store the encryption key. */
    ENCRYPTION_KEY(Paths.get("encryption"), true);

    /** Location of the file. */
    private final Path mPath;

    /** Is the location a folder or no? */
    private final boolean mIsFolder;

    /**
     * Constructor.
     *
     * @param path the path
     * @param isFolder determines if the path denotes a filer or not
     */
    ResourcePaths(final Path path, final boolean isFolder) {
      mPath = path;
      mIsFolder = isFolder;
    }

    /**
     * Getting the path.
     *
     * @return the path
     */
    public Path getPath() {
      return mPath;
    }

    /**
     * Check if file is denoted as folder or not.
     *
     * @return {@code true} if file is a folder, {@code false} otherwise
     */
    public boolean isFolder() {
      return mIsFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this enum.
     *
     * @param file to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to the one expected, 1 if the
     *         structure has more folders
     * @throws NullPointerException if {@code file} is {@code null}
     */
    public static int compareStructure(final Path file) {
      int existing = 0;
      for (final ResourcePaths paths : values()) {
        final Path currentFile = file.resolve(paths.getPath());
        if (Files.exists(currentFile)) {
          existing++;
        }
      }
      return existing - values().length;
    }
  }

  // FIXED STANDARD FIELDS
  /** Standard storage. */
  private static final StorageType STORAGE = StorageType.FILE;

  /** Standard versioning approach. */
  private static final VersioningType VERSIONING = VersioningType.SLIDING_SNAPSHOT;

  /** Type of hashing. */
  private static final HashType HASHKIND = HashType.ROLLING;

  /** Versions to restore. */
  private static final int VERSIONSTORESTORE = 3;

  /** Persistenter for records. */
  private static final RecordPersister PERSISTENTER = new NodePersistenterImpl();

  // END FIXED STANDARD FIELDS

  // MEMBERS FOR FIXED FIELDS
  /** Type of Storage (File, BerkeleyDB). */
  public final StorageType storageType;

  /** Kind of revisioning (Full, Incremental, Differential). */
  public final VersioningType revisioningType;

  /** Kind of integrity hash (rolling, postorder). */
  public final HashType hashType;

  /** Number of revisions to restore a complete set of data. */
  public final int numberOfRevisionsToRestore;

  /** Byte handler pipeline. */
  public final ByteHandlePipeline byteHandlePipeline;

  /** Path for the resource to be associated. */
  public Path resourcePath;

  /** DatabaseConfiguration for this {@link ResourceConfiguration}. */
  private DatabaseConfiguration databaseConfig;

  /** Determines if text-compression should be used or not (default is true). */
  public final boolean useTextCompression;

  /** Determines if a path summary should be build and kept up to date or not. */
  public final boolean withPathSummary;

  /** Persistents records / commonly nodes. */
  public final RecordPersister recordPersister;

  /** Unique ID. */
  private long id;

  /** Determines if dewey IDs are generated and stored or not. */
  public final boolean areDeweyIDsStored;

  /** The hash function used for hashing nodes. */
  public final HashFunction nodeHashFunction;

  /** The name of the resource. */
  private String resourceName;

  // END MEMBERS FOR FIXED FIELDS

  /**
   * Get a new builder instance.
   *
   * @param resource the name of the resource
   * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
   * @return {@link Builder} instance
   */
  public static Builder newBuilder(final String resource) {
    return new Builder(resource);
  }

  /**
   * Convenience constructor using the standard settings.
   *
   * @param builder {@link Builder} reference
   */
  private ResourceConfiguration(final ResourceConfiguration.Builder builder) {
    storageType = builder.mType;
    byteHandlePipeline = builder.mByteHandler;
    revisioningType = builder.mRevisionKind;
    hashType = builder.mHashKind;
    numberOfRevisionsToRestore = builder.mRevisionsToRestore;
    useTextCompression = builder.mCompression;
    withPathSummary = builder.mPathSummary;
    areDeweyIDsStored = builder.mUseDeweyIDs;
    recordPersister = builder.mPersistenter;
    resourceName = builder.mResource;
    nodeHashFunction = builder.mHashFunction;
  }

  ResourceConfiguration setDatabaseConfiguration(final DatabaseConfiguration config) {
    databaseConfig = checkNotNull(config);
    resourcePath =
        databaseConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resourceName);
    return this;
  }

  /**
   * Set a unique ID.
   *
   * @param id the ID to set
   * @return this instance
   */
  public ResourceConfiguration setID(final @Nonnegative long id) {
    checkArgument(id >= 0, "The ID must be >= 0!");
    this.id = id;
    return this;
  }

  /**
   * Get the unique ID.
   *
   * @return the unique resource ID
   */
  public long getID() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageType, revisioningType, hashType, resourcePath, databaseConfig);
  }

  @Override
  public final boolean equals(final Object obj) {
    if (!(obj instanceof ResourceConfiguration))
      return false;

    final ResourceConfiguration other = (ResourceConfiguration) obj;
    return Objects.equals(storageType, other.storageType) && Objects.equals(revisioningType, other.revisioningType)
        && Objects.equals(hashType, other.hashType) && Objects.equals(resourcePath, other.resourcePath)
        && Objects.equals(databaseConfig, other.databaseConfig);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("Resource", resourcePath)
                      .add("Type", storageType)
                      .add("Revision", revisioningType)
                      .add("HashKind", hashType)
                      .toString();
  }

  /**
   * Get the resource.
   *
   * @return the resource
   */
  public Path getResource() {
    return resourcePath;
  }

  /**
   * Get the resource name.
   *
   * @return the resource name
   */
  public String getName() {
    return resourceName;
  }

  /**
   * Get the resource name.
   *
   * @return the resource name
   */
  public String getResourceName() {
    return resourceName;
  }

  /**
   * Get the configuration file.
   *
   * @return configuration file
   */
  public Path getConfigFile() {
    return resourcePath.resolve(ResourcePaths.CONFIG_BINARY.getPath());
  }

  /**
   * JSON names.
   */
  private static final String[] JSONNAMES =
      {"revisioning", "revisioningClass", "numbersOfRevisiontoRestore", "byteHandlerClasses", "storageKind", "hashKind",
          "hashFunction", "compression", "pathSummary", "resourceID", "deweyIDsStored", "persistenter"};

  /**
   * Serialize the configuration.
   *
   * @param config configuration to serialize
   * @throws SirixIOException if an I/O error occurs
   */
  public static void serialize(final ResourceConfiguration config) throws SirixIOException {
    final Path configFile = config.getConfigFile();
    try (final FileWriter fileWriter = new FileWriter(configFile.toFile());
        final JsonWriter jsonWriter = new JsonWriter(fileWriter)) {
      jsonWriter.beginObject();
      // Versioning.
      jsonWriter.name(JSONNAMES[0]);
      jsonWriter.beginObject();
      jsonWriter.name(JSONNAMES[1]).value(config.revisioningType.name());
      jsonWriter.name(JSONNAMES[2]).value(config.numberOfRevisionsToRestore);
      jsonWriter.endObject();
      // ByteHandlers.
      final ByteHandlePipeline byteHandler = config.byteHandlePipeline;
      jsonWriter.name(JSONNAMES[3]);
      jsonWriter.beginArray();
      for (final ByteHandler handler : byteHandler.getComponents()) {
        ByteHandlerKind.getKind(handler.getClass()).serialize(handler, jsonWriter);
      }
      jsonWriter.endArray();
      // Storage type.
      jsonWriter.name(JSONNAMES[4]).value(config.storageType.name());
      // Hashing type.
      jsonWriter.name(JSONNAMES[5]).value(config.hashType.name());
      // Hash function.
      jsonWriter.name(JSONNAMES[6]).value(config.nodeHashFunction.toString());
      // Text compression.
      jsonWriter.name(JSONNAMES[7]).value(config.useTextCompression);
      // Path summary.
      jsonWriter.name(JSONNAMES[8]).value(config.withPathSummary);
      // ID.
      jsonWriter.name(JSONNAMES[9]).value(config.id);
      // Dewey IDs stored or not.
      jsonWriter.name(JSONNAMES[10]).value(config.areDeweyIDsStored);
      // Persistenter.
      jsonWriter.name(JSONNAMES[11]).value(config.recordPersister.getClass().getName());
      jsonWriter.endObject();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    // Database config.
    DatabaseConfiguration.serialize(config.databaseConfig);
  }

  /**
   * Deserializing a Resource configuration from a JSON-file from the persistent storage.
   *
   * @param file where the resource lies in.
   * @return a complete {@link ResourceConfiguration} instance
   * @throws SirixIOException if an I/O error occurs
   */
  public static ResourceConfiguration deserialize(final Path file) throws SirixIOException {
    try {
      final Path configFile = file.resolve(ResourcePaths.CONFIG_BINARY.getPath());
      final FileReader fileReader = new FileReader(configFile.toFile());
      final JsonReader jsonReader = new JsonReader(fileReader);
      jsonReader.beginObject();
      // Versioning.
      String name = jsonReader.nextName();
      assert name.equals(JSONNAMES[0]);
      jsonReader.beginObject();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[1]);
      final VersioningType revisioning = VersioningType.valueOf(jsonReader.nextString());
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
        jsonReader.beginObject();
        @SuppressWarnings("unchecked")
        final Class<ByteHandler> clazzName = (Class<ByteHandler>) Class.forName(jsonReader.nextName());
        handlerList.add(ByteHandlerKind.getKind(clazzName).deserialize(jsonReader));
        jsonReader.endObject();
      }
      jsonReader.endArray();
      final ByteHandlePipeline pipeline =
          new ByteHandlePipeline(handlerList.toArray(new ByteHandler[handlerList.size()]));
      // Storage type.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[4]);
      final StorageType storage = StorageType.valueOf(jsonReader.nextString());
      // Hashing type.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[5]);
      final HashType hashing = HashType.valueOf(jsonReader.nextString());
      // Hashing function.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[6]);

      final HashFunction hashFunction;
      switch (jsonReader.nextString()) {
        case "Hashing.sha256()":
          hashFunction = Hashing.sha256();
          break;
        default:
          throw new IllegalStateException("Hashing function not supported.");
      }
      // Text compression.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[7]);
      final boolean compression = jsonReader.nextBoolean();
      // Path summary.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[8]);
      final boolean pathSummary = jsonReader.nextBoolean();
      // Unique ID.
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[9]);
      final int ID = jsonReader.nextInt();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[10]);
      final boolean deweyIDsStored = jsonReader.nextBoolean();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[11]);
      final Class<?> persistenterClazz = Class.forName(jsonReader.nextString());
      final Constructor<?> persistenterConstr = persistenterClazz.getConstructors()[0];
      final RecordPersister persistenter = (RecordPersister) persistenterConstr.newInstance();
      jsonReader.endObject();
      jsonReader.close();
      fileReader.close();

      // Deserialize database config.
      final DatabaseConfiguration dbConfig = DatabaseConfiguration.deserialize(file.getParent().getParent());

      // Builder.
      final ResourceConfiguration.Builder builder = ResourceConfiguration.newBuilder(file.getFileName().toString());
      builder.byteHandlerPipeline(pipeline)
             .hashKind(hashing)
             .versioningApproach(revisioning)
             .revisionsToRestore(revisionToRestore)
             .storageType(storage)
             .persistenter(persistenter)
             .useTextCompression(compression)
             .buildPathSummary(pathSummary)
             .useDeweyIDs(deweyIDsStored);

      // Deserialized instance.
      final ResourceConfiguration config = new ResourceConfiguration(builder);
      config.setDatabaseConfiguration(dbConfig);
      return config.setID(ID);
    } catch (IOException | ClassNotFoundException | IllegalArgumentException | InstantiationException
        | IllegalAccessException | InvocationTargetException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Builder class for generating new {@link ResourceConfiguration} instance.
   */
  public static final class Builder {

    /** Hashing function for hashing nodes. */
    private HashFunction mHashFunction = Hashing.sha256();

    /** Type of Storage (File, Berkeley). */
    private StorageType mType = STORAGE;

    /** Kind of revisioning (Incremental, Differential). */
    private VersioningType mRevisionKind = VERSIONING;

    /** Kind of integrity hash (rolling, postorder). */
    private HashType mHashKind = HASHKIND;

    /** Number of revisions to restore a complete set of data. */
    private int mRevisionsToRestore = VERSIONSTORESTORE;

    /** Record/Node persistenter. */
    private RecordPersister mPersistenter = PERSISTENTER;

    /** Resource for this session. */
    private final String mResource;

    /** Determines if text-compression should be used or not (default is true). */
    private boolean mCompression;

    /** Byte handler pipeline. */
    private ByteHandlePipeline mByteHandler;

    /** Determines if DeweyIDs should be used or not. */
    private boolean mUseDeweyIDs;

    /** Determines if a path summary should be build or not. */
    private boolean mPathSummary;

    /**
     * Constructor, setting the mandatory fields.
     *
     * @param resource the name of the resource
     * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
     */
    public Builder(final String resource) {
      mResource = checkNotNull(resource);
      mPathSummary = true;

      // final Path path =
      // mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(mResource);

      mByteHandler = new ByteHandlePipeline(new SnappyCompressor());// new Encryptor(path));
    }

    /**
     * Set the storage type.
     *
     * @param type storage type to use
     * @return reference to the builder object
     */
    public Builder storageType(final StorageType type) {
      mType = checkNotNull(type);
      return this;
    }

    /**
     * Set the record persistenter.
     *
     * @param persistenter the record persistenter
     * @return reference to the builder object
     */
    public Builder persistenter(final RecordPersister persistenter) {
      mPersistenter = checkNotNull(persistenter);
      return this;
    }

//    /**
//     * Set the hash function.
//     *
//     * @param hashFunction the hash function
//     * @return reference to the builder object
//     */
//    public Builder hashFunction(final HashFunction hashFunction) {
//      mHashFunction = checkNotNull(hashFunction);
//      return this;
//    }

    /**
     * Set the versioning algorithm to use.
     *
     * @param versioning versioning algorithm to use
     * @return reference to the builder object
     */
    public Builder versioningApproach(final VersioningType versioning) {
      mRevisionKind = checkNotNull(versioning);
      return this;
    }

    /**
     * Set the hash kind to use for the nodes.
     *
     * @param hashKind hash kind to use
     * @return reference to the builder object
     */
    public Builder hashKind(final HashType hashKind) {
      mHashKind = checkNotNull(hashKind);
      return this;
    }

    /**
     * Set the byte handler pipeline.
     *
     * @param byteHandler byte handler pipeline
     * @return reference to the builder object
     */
    public Builder byteHandlerPipeline(final ByteHandlePipeline byteHandler) {
      mByteHandler = checkNotNull(byteHandler);
      return this;
    }

    /**
     * Set the number of revisions to restore after the last full dump.
     *
     * @param revisionsToRestore number of versions to restore
     * @return reference to the builder object
     */
    public Builder revisionsToRestore(final @Nonnegative int revisionsToRestore) {
      checkArgument(revisionsToRestore > 0, "revisionsToRestore must be > 0!");
      mRevisionsToRestore = revisionsToRestore;
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
     * @param useTextCompression use text compression or not (default: yes)
     * @return reference to the builder object
     */
    public Builder useTextCompression(final boolean useTextCompression) {
      mCompression = useTextCompression;
      return this;
    }

    /**
     * Determines if a path summary should be build.
     *
     * @return reference to the builder object
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      mPathSummary = buildPathSummary;
      return this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("Type", mType)
                        .add("RevisionKind", mRevisionKind)
                        .add("HashKind", mHashKind)
                        .add("HashFunction", mHashFunction)
                        .add("PathSummary", mPathSummary)
                        .add("TextCompression", mCompression)
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

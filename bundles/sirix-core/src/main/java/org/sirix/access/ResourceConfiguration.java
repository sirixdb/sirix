/*
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

import com.google.common.base.MoreObjects;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.sirix.access.trx.node.HashType;
import org.sirix.exception.SirixIOException;
import org.sirix.io.StorageType;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.ByteHandlerKind;
import org.sirix.io.bytepipe.SnappyCompressor;
import org.sirix.node.NodeSerializerImpl;
import org.sirix.node.interfaces.RecordSerializer;
import org.sirix.settings.VersioningType;
import org.sirix.utils.OS;

import org.checkerframework.checker.index.qual.NonNegative;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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

    /**
     * Folder for storage of data.
     */
    DATA(Paths.get("data"), true),

    /**
     * Folder for the transaction log.
     */
    TRANSACTION_INTENT_LOG(Paths.get("log"), true),

    /**
     * File to store the resource settings.
     */
    CONFIG_BINARY(Paths.get("ressetting.obj"), false),

    /**
     * File to store index definitions.
     */
    INDEXES(Paths.get("indexes"), true),

    /**
     * Folder to store the encryption key.
     */
    ENCRYPTION_KEY(Paths.get("encryption"), true),

    /**
     * Folder to store the update operations.
     */
    UPDATE_OPERATIONS(Paths.get("update-operations"), true);

    /**
     * Location of the file.
     */
    private final Path path;

    /**
     * Is the location a folder or no?
     */
    private final boolean isFolder;

    /**
     * Constructor.
     *
     * @param path     the path
     * @param isFolder determines if the path denotes a filer or not
     */
    ResourcePaths(final Path path, final boolean isFolder) {
      this.path = path;
      this.isFolder = isFolder;
    }

    /**
     * Getting the path.
     *
     * @return the path
     */
    public Path getPath() {
      return path;
    }

    /**
     * Check if file is denoted as folder or not.
     *
     * @return {@code true} if file is a folder, {@code false} otherwise
     */
    public boolean isFolder() {
      return isFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this enum.
     *
     * @param file to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to the one expected, 1 if the
     * structure has more folders
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
  /**
   * Standard storage.
   */
  private static final StorageType STORAGE = //StorageType.FILECHANNEL;
      OS.isWindows() ? StorageType.FILECHANNEL : OS.is64Bit() ? StorageType.MEMORY_MAPPED : StorageType.FILECHANNEL;

  /**
   * Standard versioning approach.
   */
  private static final VersioningType VERSIONING = VersioningType.SLIDING_SNAPSHOT;

  /**
   * Type of hashing.
   */
  private static final HashType HASHKIND = HashType.ROLLING;

  /**
   * Versions to restore.
   */
  private static final int VERSIONS_TO_RESTORE = 3;

  /**
   * Serializer for records.
   */
  private static final RecordSerializer NODE_SERIALIZER = new NodeSerializerImpl();

  // END FIXED STANDARD FIELDS

  // MEMBERS FOR FIXED FIELDS
  /**
   * Type of Storage (File, BerkeleyDB).
   */
  public final StorageType storageType;

  /**
   * Kind of revisioning (Full, Incremental, Differential).
   */
  public final VersioningType versioningType;

  /**
   * Kind of integrity hash (rolling, postorder).
   */
  public final HashType hashType;

  /**
   * Number of revisions to restore a complete set of data.
   */
  public final int maxNumberOfRevisionsToRestore;

  /**
   * Byte handler pipeline.
   */
  public final ByteHandlePipeline byteHandlePipeline;

  /**
   * Path for the resource to be associated.
   */
  public Path resourcePath;

  /**
   * DatabaseConfiguration for this {@link ResourceConfiguration}.
   */
  private DatabaseConfiguration databaseConfig;

  /**
   * Determines if text-compression should be used or not (default is true).
   */
  public final boolean useTextCompression;

  /**
   * Determines if a path summary should be build and kept up to date or not.
   */
  public final boolean withPathSummary;

  /**
   * Persistents records / commonly nodes.
   */
  public final RecordSerializer recordPersister;

  /**
   * Unique ID.
   */
  private long id;

  /**
   * Determines if dewey IDs are generated and stored or not.
   */
  public final boolean areDeweyIDsStored;

  /**
   * The hash function used for hashing nodes.
   */
  public final HashFunction nodeHashFunction;

  /**
   * The name of the resource.
   */
  private String resourceName;

  /**
   * Determines whether resource child count should be tracked
   */
  private boolean storeChildCount;

  /**
   * Determines if diffs are going to be stored or not.
   */
  public final boolean storeDiffs;

  /**
   * Determines if custom commit timestamps should be stored or not.
   */
  public final boolean customCommitTimestamps;

  /**
   * Store the full node history of each record.
   */
  public final boolean storeNodeHistory;

  // END MEMBERS FOR FIXED FIELDS

  /**
   * Get a new builder instance.
   *
   * @param resource the name of the resource
   * @return {@link Builder} instance
   * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
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
    storageType = builder.type;
    byteHandlePipeline = builder.byteHandler;
    versioningType = builder.revisionKind;
    hashType = builder.hashKind;
    maxNumberOfRevisionsToRestore = builder.maxNumberOfRevisionsToRestore;
    useTextCompression = builder.useTextCompression;
    withPathSummary = builder.pathSummary;
    areDeweyIDsStored = builder.useDeweyIDs;
    recordPersister = builder.persistenter;
    resourceName = builder.resource;
    nodeHashFunction = builder.hashFunction;
    storeChildCount = builder.storeChildCount;
    storeDiffs = builder.storeDiffs;
    customCommitTimestamps = builder.customCommitTimestamps;
    storeNodeHistory = builder.storeNodeHistory;
  }

  public boolean customCommitTimestamps() {
    return customCommitTimestamps;
  }

  ResourceConfiguration setDatabaseConfiguration(final DatabaseConfiguration config) {
    databaseConfig = checkNotNull(config);
    resourcePath = databaseConfig.getDatabaseFile()
                                 .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                 .resolve(resourceName);
    return this;
  }

  /**
   * Set a unique ID.
   *
   * @param id the ID to set
   * @return this instance
   */
  public ResourceConfiguration setID(final @NonNegative long id) {
    checkArgument(id >= 0, "The ID must be >= 0!");
    this.id = id;
    return this;
  }

  /**
   * Get the storage type.
   *
   * @return The storage type.
   */
  public StorageType getStorageType() {
    return storageType;
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
    return Objects.hash(storageType, versioningType, hashType, resourcePath, databaseConfig);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ResourceConfiguration))
      return false;

    final ResourceConfiguration other = (ResourceConfiguration) obj;
    return Objects.equals(storageType, other.storageType) && Objects.equals(versioningType, other.versioningType)
        && Objects.equals(hashType, other.hashType) && Objects.equals(resourcePath, other.resourcePath)
        && Objects.equals(databaseConfig, other.databaseConfig);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("Resource", resourcePath)
                      .add("Type", storageType)
                      .add("Revision", versioningType)
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

  public boolean storeDiffs() {
    return storeDiffs;
  }

  /**
   * Get the configuration file.
   *
   * @return configuration file
   */
  public Path getConfigFile() {
    return resourcePath.resolve(ResourcePaths.CONFIG_BINARY.getPath());
  }

  public boolean getStoreChildCount() {
    return storeChildCount;
  }

  public boolean storeNodeHistory() {
    return storeNodeHistory;
  }

  /**
   * JSON names.
   */
  private static final String[] JSONNAMES =
      { "revisioning", "revisioningClass", "numbersOfRevisiontoRestore", "byteHandlerClasses", "storageKind",
          "hashKind", "hashFunction", "compression", "pathSummary", "resourceID", "deweyIDsStored", "persistenter",
          "storeDiffs", "customCommitTimestamps", "storeNodeHistory" };

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
      jsonWriter.name(JSONNAMES[1]).value(config.versioningType.name());
      jsonWriter.name(JSONNAMES[2]).value(config.maxNumberOfRevisionsToRestore);
      jsonWriter.endObject();
      // ByteHandlers.
      jsonWriter.name(JSONNAMES[3]);
      jsonWriter.beginArray();
      for (final ByteHandler handler : config.byteHandlePipeline.getComponents()) {
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
      // Diffs.
      jsonWriter.name(JSONNAMES[12]).value(config.storeDiffs);
      // Custom commit timestamps.
      jsonWriter.name(JSONNAMES[13]).value(config.customCommitTimestamps);
      // Node history.
      jsonWriter.name(JSONNAMES[14]).value(config.storeNodeHistory);
      jsonWriter.endObject();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    // Database config.
    DatabaseConfiguration.serialize(config.databaseConfig);
  }

  /**
   * Deserializing a Resource configuration from a JSON-file from the persistent storage.
   * //todo add track child count parameter here
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
        @SuppressWarnings("unchecked") final Class<ByteHandler> clazzName =
            (Class<ByteHandler>) Class.forName(jsonReader.nextName());
        handlerList.add(ByteHandlerKind.getKind(clazzName).deserialize(jsonReader));
        jsonReader.endObject();
      }
      jsonReader.endArray();
      final ByteHandlePipeline pipeline = new ByteHandlePipeline(handlerList.toArray(new ByteHandler[0]));
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
      final RecordSerializer serializer = (RecordSerializer) persistenterConstr.newInstance();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[12]);
      final boolean storeDiffs = jsonReader.nextBoolean();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[13]);
      final boolean customCommitTimestamps = jsonReader.nextBoolean();
      name = jsonReader.nextName();
      assert name.equals(JSONNAMES[14]);
      final boolean storeNodeHistory = jsonReader.nextBoolean();

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
             .maxNumberOfRevisionsToRestore(revisionToRestore)
             .storageType(storage)
             .persistenter(serializer)
             .useTextCompression(compression)
             .buildPathSummary(pathSummary)
             .useDeweyIDs(deweyIDsStored)
             .storeDiffs(storeDiffs)
             .customCommitTimestamps(customCommitTimestamps)
             .storeNodeHistory(storeNodeHistory);

      // Deserialized instance.
      final ResourceConfiguration config = new ResourceConfiguration(builder);
      config.setDatabaseConfiguration(dbConfig);
      return config.setID(ID);
    } catch (IOException | ClassNotFoundException | IllegalArgumentException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Builder class for generating new {@link ResourceConfiguration} instance.
   */
  public static final class Builder {

    /**
     * Determines if diffs should be stored or not.
     */
    public boolean storeDiffs = true;

    /**
     * Hashing function for hashing nodes.
     */
    private HashFunction hashFunction = Hashing.sha256();

    /**
     * Type of Storage (File, Berkeley).
     */
    private StorageType type = STORAGE;

    /**
     * Kind of revisioning (Incremental, Differential).
     */
    private VersioningType revisionKind = VERSIONING;

    /**
     * Kind of integrity hash (rolling, postorder).
     */
    private HashType hashKind = HASHKIND;

    /**
     * Number of revisions to restore a complete set of data.
     */
    private int maxNumberOfRevisionsToRestore = VERSIONS_TO_RESTORE;

    /**
     * Record/Node persistenter.
     */
    private RecordSerializer persistenter = NODE_SERIALIZER;

    /**
     * Resource for this session.
     */
    private final String resource;

    /**
     * Determines if text-compression should be used or not (default is true).
     */
    private boolean useTextCompression;

    /**
     * Byte handler pipeline.
     */
    private ByteHandlePipeline byteHandler;

    /**
     * Determines if DeweyIDs should be used or not.
     */
    private boolean useDeweyIDs;

    /**
     * Determines if a path summary should be build or not.
     */
    private boolean pathSummary;

    /**
     * Determines whether child count should be tracked or not.
     */
    private boolean storeChildCount;

    /**
     * Determines if custom commit timestamps should be stored or not.
     */
    private boolean customCommitTimestamps;

    /**
     * Determines if node history should be stored or not.
     */
    private boolean storeNodeHistory;

    /**
     * Constructor, setting the mandatory fields.
     *
     * @param resource the name of the resource
     * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
     */
    public Builder(final String resource) {
      this.resource = checkNotNull(resource);
      pathSummary = true;
      storeChildCount = true;
      byteHandler = new ByteHandlePipeline(new SnappyCompressor());// new Encryptor(path));
    }

    /**
     * Set the storage type.
     *
     * @param type storage type to use
     * @return reference to the builder object
     */
    public Builder storageType(final StorageType type) {
      this.type = checkNotNull(type);
      return this;
    }

    /**
     * Set the record persistenter.
     *
     * @param persistenter the record persistenter
     * @return reference to the builder object
     */
    public Builder persistenter(final RecordSerializer persistenter) {
      this.persistenter = checkNotNull(persistenter);
      return this;
    }

    /**
     * Set to {@code false} if no diffs should be stored.
     *
     * @param storeDiffs {code true}, if diffs should be stored, {@code false} if not
     * @return reference to the builder object
     */
    public Builder storeDiffs(final boolean storeDiffs) {
      this.storeDiffs = storeDiffs;
      return this;
    }

    /**
     * Set the versioning algorithm to use.
     *
     * @param versioning versioning algorithm to use
     * @return reference to the builder object
     */
    public Builder versioningApproach(final VersioningType versioning) {
      revisionKind = checkNotNull(versioning);
      return this;
    }

    /**
     * Set the hash kind to use for the nodes.
     *
     * @param hashKind hash kind to use
     * @return reference to the builder object
     */
    public Builder hashKind(final HashType hashKind) {
      this.hashKind = checkNotNull(hashKind);
      return this;
    }

    /**
     * Set the byte handler pipeline.
     *
     * @param byteHandler byte handler pipeline
     * @return reference to the builder object
     */
    public Builder byteHandlerPipeline(final ByteHandlePipeline byteHandler) {
      this.byteHandler = checkNotNull(byteHandler);
      return this;
    }

    /**
     * Set the maximum number of revisions to restore.
     *
     * @param revisionsToRestore number of versions to restore
     * @return reference to the builder object
     */
    public Builder maxNumberOfRevisionsToRestore(final @NonNegative int revisionsToRestore) {
      checkArgument(revisionsToRestore > 0, "revisionsToRestore must be > 0!");
      this.maxNumberOfRevisionsToRestore = revisionsToRestore;
      return this;
    }

    /**
     * Determines if DeweyIDs should be stored or not.
     *
     * @param useDeweyIDs flag whihc represents to use the deweyIds
     * @return reference to the builder object
     */
    public Builder useDeweyIDs(final boolean useDeweyIDs) {
      this.useDeweyIDs = useDeweyIDs;
      return this;
    }

    /**
     * Determines if text-compression should be used or not.
     *
     * @param useTextCompression use text compression or not (default: yes)
     * @return reference to the builder object
     */
    public Builder useTextCompression(final boolean useTextCompression) {
      this.useTextCompression = useTextCompression;
      return this;
    }

    /**
     * Determines if a path summary should be build.
     *
     * @return reference to the builder object
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      pathSummary = buildPathSummary;
      return this;
    }

    /**
     * Determines if the child count of a node should be stored or not.
     *
     * @param storeChildCount store child count or not
     * @return reference to the builder object
     */
    public Builder storeChildCount(final boolean storeChildCount) {
      this.storeChildCount = storeChildCount;
      return this;
    }

    /**
     * Set to {@code true} if custom commit timestamps should be stored.
     *
     * @param customCommitTimestamps {code true}, if custom commit timestamps should be stored, {@code false} if not
     * @return reference to the builder object
     */
    public Builder customCommitTimestamps(final boolean customCommitTimestamps) {
      this.customCommitTimestamps = customCommitTimestamps;
      return this;
    }

    /**
     * Set to {@code true} if node history should be stored.
     *
     * @param storeNodeHistory {code true}, if node history should be stored, {@code false} if not
     * @return reference to the builder object
     */
    public Builder storeNodeHistory(boolean storeNodeHistory) {
      this.storeNodeHistory = storeNodeHistory;
      return this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("Type", type)
                        .add("RevisionKind", revisionKind)
                        .add("HashKind", hashKind)
                        .add("HashFunction", hashFunction)
                        .add("PathSummary", pathSummary)
                        .add("TextCompression", useTextCompression)
                        .add("Store diffs", storeDiffs)
                        .add("Store child count", storeChildCount)
                        .add("Store node history", storeNodeHistory)
                        .add("Custom commit timestamps", customCommitTimestamps)
                        .add("Max number of revisions to restore", maxNumberOfRevisionsToRestore)
                        .add("Use deweyIDs", useDeweyIDs)
                        .add("Byte handler pipeline", byteHandler)
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

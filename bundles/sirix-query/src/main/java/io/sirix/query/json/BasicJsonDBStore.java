package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import io.sirix.utils.OS;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Database storage.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDBStore implements JsonDBStore {

  /**
   * User home directory.
   */
  private static final String USER_HOME = System.getProperty("user.home");

  /**
   * Storage for databases: Sirix data in home directory.
   */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  /**
   * {@link Set} of databases.
   */
  private final Set<Database<JsonResourceSession>> databases;

  /**
   * Mapping sirix databases to collections.
   */
  private final ConcurrentMap<Database<JsonResourceSession>, JsonDBCollection> collections;

  /**
   * {@link StorageType} instance.
   */
  private final StorageType storageType;

  /**
   * The location to store created collections/databases.
   */
  private final Path location;

  /**
   * Determines if a path summary should be built for resources.
   */
  private final boolean buildPathSummary;

  /**
   * Determines if DeweyIDs should be generated for resources.
   */
  private final boolean useDeweyIDs;

  /**
   * Determines the hash type to use (default: rolling).
   */
  private final HashType hashType;

  /**
   * Determines the versioning type.
   */
  private final VersioningType versioningType;

  /**
   * Number of nodes before an auto-commit is issued during an import of an XML document.
   */
  private final int numberOfNodesBeforeAutoCommit;

  /**
   * Get a new builder instance.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder setting up the store.
   */
  @SuppressWarnings("unused")
  public static class Builder {
    /**
     * Storage type.
     */
    private StorageType storageType = System.getProperty("storageType") != null
        ? StorageType.fromString(System.getProperty("storageType"))
        : OS.isWindows()
            ? StorageType.FILE_CHANNEL
            : OS.is64Bit() ? StorageType.MEMORY_MAPPED : StorageType.FILE_CHANNEL;

    /**
     * The location to store created collections/databases.
     */
    private Path location =
        System.getProperty("dbLocation") != null ? Path.of(System.getProperty("dbLocation")) : LOCATION;

    /**
     * Determines if a path summary should be build for resources.
     */
    private boolean buildPathSummary =
        System.getProperty("buildPathSummary") == null || Boolean.parseBoolean(System.getProperty("buildPathSummary"));

    /**
     * Determines if DeweyIDs should be generated for resources.
     */
    private boolean useDeweyIDs =
        System.getProperty("useDeweyIDs") != null && Boolean.parseBoolean(System.getProperty("useDeweyIDs"));

    /**
     * Determines the hash type to use (default: rolling).
     */
    private HashType hashType =
        System.getProperty("hashType") != null ? HashType.fromString(System.getProperty("hashType")) : HashType.ROLLING;

    /**
     * Determines the versioning type.
     */
    private VersioningType versioningType = System.getProperty("versioningType") != null
        ? VersioningType.fromString(System.getProperty("versioningType"))
        : VersioningType.SLIDING_SNAPSHOT;

    /**
     * Number of nodes before an auto-commit is issued during an import of an XML document.
     */
    private int numberOfNodesBeforeAutoCommit =
        System.getProperty("numberOfNodesBeforeAutoCommit") != null ? Integer.parseInt(System.getProperty(
            "numberOfNodesBeforeAutoCommit")) : 262_144 << 2;

    /**
     * Set the storage type (default: file backend).
     *
     * @param storageType storage type
     * @return this builder instance
     */
    public Builder storageType(final StorageType storageType) {
      this.storageType = requireNonNull(storageType);
      return this;
    }

    /**
     * Set if path summaries should be build for resources.
     *
     * @param buildPathSummary {@code true} if path summaries should be build, {@code false} otherwise
     * @return this builder instance
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      this.buildPathSummary = buildPathSummary;
      return this;
    }

    /**
     * Determines Dewey-IDs should be generated for resources.
     *
     * @param storeDeweyIDs {@code true} if dewey-IDs should be generated, {@code false} otherwise
     * @return this builder instance
     */
    public Builder storeDeweyIds(final boolean storeDeweyIDs) {
      this.useDeweyIDs = storeDeweyIDs;
      return this;
    }

    /**
     * Set the location where to store the created databases/collections.
     *
     * @param location the location
     * @return this builder instance
     */
    public Builder location(final Path location) {
      this.location = requireNonNull(location);
      return this;
    }

    /**
     * Determines the hash type to use (default: rolling).
     *
     * @param hashType the hash type
     * @return this builder instance
     */
    public Builder hashType(final HashType hashType) {
      this.hashType = requireNonNull(hashType);
      return this;
    }

    /**
     * Sets the versioning type of the storage.
     *
     * @param versioningType the versioning type to set
     * @return this builder instance
     */
    public Builder versioningType(final VersioningType versioningType) {
      this.versioningType = versioningType;
      return this;
    }

    /**
     * Number of nodes, before the write trx is internally committed during an import of a file.
     *
     * @param numberOfNodesBeforeAutoCommit number of nodes to insert before an auto-commit is issued
     * @return this builder instance
     */
    public Builder numberOfNodesBeforeAutoCommit(final int numberOfNodesBeforeAutoCommit) {
      checkArgument(numberOfNodesBeforeAutoCommit > 0, "Must be > 0!");
      this.numberOfNodesBeforeAutoCommit = numberOfNodesBeforeAutoCommit;
      return this;
    }

    /**
     * Create a new {@link BasicJsonDBStore} instance
     *
     * @return new {@link BasicJsonDBStore} instance
     */
    public BasicJsonDBStore build() {
      return new BasicJsonDBStore(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder instance
   */
  private BasicJsonDBStore(final Builder builder) {
    databases = Collections.synchronizedSet(new HashSet<>());
    collections = new ConcurrentHashMap<>();
    storageType = builder.storageType;
    location = builder.location;
    buildPathSummary = builder.buildPathSummary;
    useDeweyIDs = builder.useDeweyIDs;
    hashType = builder.hashType;
    versioningType = builder.versioningType;
    numberOfNodesBeforeAutoCommit = builder.numberOfNodesBeforeAutoCommit;
  }

  /**
   * Get the location of the generated collections/databases.
   */
  public Path getLocation() {
    return location;
  }

  @Override
  public Options options() {
    return new Options(null,
                       null,
                       false,
                       buildPathSummary,
                       storageType,
                       useDeweyIDs,
                       hashType,
                       versioningType,
                       numberOfNodesBeforeAutoCommit);
  }

  @Override
  public JsonDBCollection lookup(final String name) {
    final Path dbPath = location.resolve(name);
    if (Databases.existsDatabase(dbPath)) {
      try {
        final var database = Databases.openJsonDatabase(dbPath);
        final Optional<Database<JsonResourceSession>> storedCollection =
            databases.stream().findFirst().filter(db -> db.equals(database));
        if (storedCollection.isPresent()) {
          final var db = storedCollection.get();

          if (db.isOpen()) {
            return collections.get(db);
          } else {
            databases.remove(db);
          }
        }
        databases.add(database);
        final JsonDBCollection collection = new JsonDBCollection(name, database, this);
        collections.put(database, collection);
        return collection;
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public JsonDBCollection create(final String name) {
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(location.resolve(name));
    try {
      if (Databases.createJsonDatabase(dbConf)) {
        throw new DocumentException("Document with name %s exists!", name);
      }

      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);

      final JsonDBCollection collection = new JsonDBCollection(name, database, this);
      collections.put(database, collection);
      return collection;
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(final String collName, final Path path) {
    return create(collName, null, path);
  }

  @Override
  public JsonDBCollection create(String collName, Path path, Object options) {
    return createCollection(collName, null, JsonShredder.createFileReader(path), options);
  }

  @Override
  public JsonDBCollection create(String collName, String json) {
    return create(collName, null, json);
  }

  @Override
  public JsonDBCollection create(String collName, String json, Object options) {
    return createCollection(collName, null, JsonShredder.createStringReader(json), options);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json) {
    final var options = new ArrayObject(new QNm[0], new Sequence[0]);
    if (json == null) {
      return createCollection(collName, optResName, null, options);
    }
    return createCollection(collName, optResName, JsonShredder.createStringReader(json), options);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json, Object options) {
    return createCollection(collName, optResName, JsonShredder.createStringReader(json), options);
  }

  @Override
  public JsonDBCollection create(final String collName, final String optResName, final Path path) {
    final var options = new ArrayObject(new QNm[0], new Sequence[0]);
    return createCollection(collName, optResName, JsonShredder.createFileReader(path), options);
  }

  @Override
  public JsonDBCollection create(final String collName, final String optResName, final Path path, Object options) {
    return createCollection(collName, optResName, JsonShredder.createFileReader(path), options);
  }

  @Override
  public JsonDBCollection create(String collName, String resourceName, JsonReader jsonReader) {
    final var options = new ArrayObject(new QNm[0], new Sequence[0]);
    return createCollection(collName, resourceName, jsonReader, options);
  }

  @Override
  public JsonDBCollection create(String collName, String resourceName, JsonReader reader, Object options) {
    return createCollection(collName, resourceName, reader, options);
  }

  private JsonDBCollection createCollection(final String collName, final String optionalResourceName,
      final JsonReader reader, final Object options) {
    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbPath);
      databases.add(database);

      final String resourceName;
      if (optionalResourceName != null) {
        final var resources = database.listResources();

        final Optional<Path> hasResourceWithName = resources.stream()
                                                            .filter(resource -> resource.getFileName()
                                                                                        .toString()
                                                                                        .equals(optionalResourceName))
                                                            .findFirst();

        if (hasResourceWithName.isPresent()) {
          int i = database.listResources().size() + 1;
          resourceName = "resource" + i;
        } else {
          resourceName = optionalResourceName;
        }
      } else {
        resourceName = "resource" + (database.listResources().size() + 1);
      }

      final var resourceOptions = createResource(options, database, resourceName);

      final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
      collections.put(database, collection);

      if (reader == null) {
        return collection;
      }

      try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
        wtx.commit(resourceOptions.commitMessage(), resourceOptions.commitTimestamp());
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @NotNull
  private Options createResource(Object options, Database<JsonResourceSession> database, String resourceName) {
    final var resourceOptions = OptionsFactory.createOptions(options,
                                                             new Options(null,
                                                                         null,
                                                                         false,
                                                                         buildPathSummary,
                                                                         storageType,
                                                                         useDeweyIDs,
                                                                         hashType,
                                                                         versioningType,
                                                                         numberOfNodesBeforeAutoCommit));

    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .useTextCompression(resourceOptions.useTextCompression())
                                                 .buildPathSummary(resourceOptions.buildPathSummary())
                                                 .customCommitTimestamps(resourceOptions.commitTimestamp() != null)
                                                 .storageType(resourceOptions.storageType())
                                                 .useDeweyIDs(resourceOptions.useDeweyIDs())
                                                 .hashKind(resourceOptions.hashType())
                                                 .versioningApproach(versioningType)
                                                 .build());
    return resourceOptions;
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders, Object options) {
    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      int numberOfResources = database.listResources().size();
      final var resourceFutures = new CompletableFuture[jsonReaders.size()];
      int i = 0;
      for (final var jsonReader : jsonReaders) {
        numberOfResources++;
        final String resourceName = "resource" + numberOfResources;
        resourceFutures[i++] =
            (CompletableFuture.runAsync(() -> createResource(collName, database, jsonReader, resourceName, options)));
      }
      CompletableFuture.allOf(resourceFutures).join();
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders) {
    return create(collName, jsonReaders, new ArrayObject(new QNm[0], new Sequence[0]));
  }

  @Override
  public JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceSession> database) {
    databases.add(database);
    collections.put(database, jsonDBCollection);
    return this;
  }

  @Override
  public JsonDBStore removeDatabase(final @NonNull Database<JsonResourceSession> database) {
    databases.remove(database);
    collections.remove(database);
    return this;
  }

  @Override
  public JsonDBCollection createFromJsonStrings(String collName, final @Nullable Stream<Str> jsonStrings) {
    if (jsonStrings == null) {
      return null;
    }

    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final var resourceFutures = new ArrayList<CompletableFuture<Void>>();
      int i = database.listResources().size() + 1;
      try (jsonStrings) {
        Str string;
        while ((string = jsonStrings.next()) != null) {
          final String currentString = string.stringValue();
          final String resourceName = "resource" + i;
          resourceFutures.add(CompletableFuture.runAsync(() -> createResource(collName,
                                                                              database,
                                                                              JsonShredder.createStringReader(
                                                                                  currentString),
                                                                              resourceName,
                                                                              new ArrayObject(new QNm[0],
                                                                                              new Sequence[0]))));
          i++;
        }
      }
      CompletableFuture.allOf(resourceFutures.toArray(new CompletableFuture[0])).join();
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private void createResource(String collName, final Database<JsonResourceSession> database, final JsonReader reader,
      final String resourceName, final Object options) {
    createResource(options, database, resourceName);
    try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
         final JsonNodeTrx wtx = manager.beginNodeTrx(numberOfNodesBeforeAutoCommit)) {
      final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
      collections.put(database, collection);
      wtx.insertSubtreeAsFirstChild(reader);
    }
  }

  @Override
  public JsonDBCollection createFromPaths(final String collName, final @Nullable Stream<Path> paths) {
    if (paths == null) {
      return null;
    }

    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final var resourceFutures = new ArrayList<CompletableFuture<Void>>();
      int i = database.listResources().size() + 1;
      try (paths) {
        Path path;
        while ((path = paths.next()) != null) {
          final Path currentPath = path;
          final String resourceName = "resource" + i;
          resourceFutures.add(CompletableFuture.runAsync(() -> {
            database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                         .storageType(storageType)
                                                         .useDeweyIDs(useDeweyIDs)
                                                         .useTextCompression(false)
                                                         .buildPathSummary(buildPathSummary)
                                                         .hashKind(hashType)
                                                         .versioningApproach(versioningType)
                                                         .build());
            try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
                 final JsonNodeTrx wtx = manager.beginNodeTrx(numberOfNodesBeforeAutoCommit)) {
              final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
              collections.put(database, collection);
              wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(currentPath));
            }
          }));
          i++;
        }
      }
      CompletableFuture.allOf(resourceFutures.toArray(new CompletableFuture[0])).join();
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void drop(final String name) {
    final Path dbPath = location.resolve(name);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
    if (!removeIfExisting(dbConfig)) {
      throw new DocumentException("No collection with the specified name found!");
    }
  }

  private boolean removeIfExisting(final DatabaseConfiguration dbConfig) {
    if (Databases.existsDatabase(dbConfig.getDatabaseFile())) {
      try {
        final Predicate<Database<JsonResourceSession>> databasePredicate =
            currDatabase -> currDatabase.getDatabaseConfig().getDatabaseFile().equals(dbConfig.getDatabaseFile());

        databases.removeIf(databasePredicate);
        collections.keySet().removeIf(databasePredicate);
        Databases.removeDatabase(dbConfig.getDatabaseFile());
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e);
      }
      return true;
    }

    return false;
  }

  @Override
  public void makeDir(final String path) {
    try {
      Files.createDirectory(java.nio.file.Paths.get(path));
    } catch (final IOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void close() {
    try {
      // First, commit and close any open write transactions on all resource sessions
      // This ensures transactions created by JsonDBObject.getReadWriteTrx() are properly closed
      for (final var database : databases) {
        for (final var resourcePath : database.listResources()) {
          final var resourceName = resourcePath.getFileName().toString();
          try {
            // beginResourceSession returns existing session if already open
            final var session = database.beginResourceSession(resourceName);
            session.getNodeTrx().ifPresent(wtx -> {
              try {
                // Commit any pending changes before closing
                wtx.commit();
              } catch (Exception e) {
                // If commit fails, rollback
                try {
                  wtx.rollback();
                } catch (Exception ignored) {
                }
              } finally {
                try {
                  wtx.close();
                } catch (Exception ignored) {
                }
              }
            });
          } catch (Exception e) {
            // Resource might not exist or session might already be closed
          }
        }
      }
      // Now close all databases (which also closes all remaining transactions)
      for (final var database : databases) {
        database.close();
      }
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }
}


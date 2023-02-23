package org.sirix.xquery.json;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.Database;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.io.StorageType;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.OS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

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
   * Get a new builder instance.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder setting up the store.
   */
  public static class Builder {
    /**
     * Storage type.
     */
    private StorageType storageType =
        OS.isWindows() ? StorageType.FILE_CHANNEL : OS.is64Bit() ? StorageType.MEMORY_MAPPED : StorageType.FILE_CHANNEL;

    /**
     * The location to store created collections/databases.
     */
    private Path location = LOCATION;

    /**
     * Determines if a path summary should be build for resources.
     */
    private boolean buildPathSummary = true;

    /**
     * Determines if DeweyIDs should be generated for resources.
     */
    private boolean useDeweyIDs = true;

    /**
     * Determines the hash type to use (default: rolling).
     */
    private HashType hashType = HashType.ROLLING;

    /**
     * Set the storage type (default: file backend).
     *
     * @param storageType storage type
     * @return this builder instance
     */
    public Builder storageType(final StorageType storageType) {
      this.storageType = checkNotNull(storageType);
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
      this.location = checkNotNull(location);
      return this;
    }

    /**
     * Determines the hash type to use (default: rolling).
     *
     * @param hashType the hash type
     * @return this builder instance
     */
    public Builder hashType(final HashType hashType) {
      this.hashType = checkNotNull(hashType);
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
  }

  /**
   * Get the location of the generated collections/databases.
   */
  public Path getLocation() {
    return location;
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
  public JsonDBCollection create(String collName, Path path, String commitMessage, Instant commitTimestamp) {
    return createCollection(collName, null, JsonShredder.createFileReader(path), commitMessage, commitTimestamp);
  }

  @Override
  public JsonDBCollection create(String collName, String json) {
    return create(collName, null, json);
  }

  @Override
  public JsonDBCollection create(String collName, String json, String commitMessage, Instant commitTimestamp) {
    return createCollection(collName, null, JsonShredder.createStringReader(json), commitMessage, commitTimestamp);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json) {
    if (json == null) {
      return createCollection(collName, optResName, null, null, null);
    }
    return createCollection(collName, optResName, JsonShredder.createStringReader(json), null, null);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json, String commitMessage,
      Instant commitTimestamp) {
    return createCollection(collName,
                            optResName,
                            JsonShredder.createStringReader(json),
                            commitMessage,
                            commitTimestamp);
  }

  @Override
  public JsonDBCollection create(final String collName, final String optResName, final Path path) {
    return createCollection(collName, optResName, JsonShredder.createFileReader(path), null, null);
  }

  @Override
  public JsonDBCollection create(final String collName, final String optResName, final Path path, String commitMessage,
      Instant commitTimestamp) {
    return createCollection(collName, optResName, JsonShredder.createFileReader(path), commitMessage, commitTimestamp);
  }

  @Override
  public JsonDBCollection create(String collName, String resourceName, JsonReader jsonReader) {
    return createCollection(collName, resourceName, jsonReader, null, null);
  }

  @Override
  public JsonDBCollection create(String collName, String resourceName, JsonReader reader, String commitMessage,
      Instant commitTimestamp) {
    return createCollection(collName, resourceName, reader, commitMessage, commitTimestamp);
  }

  private JsonDBCollection createCollection(final String collName, final String optionalResourceName,
      final JsonReader reader, final String commitMessage, final Instant commitTimestamp) {
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

      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useTextCompression(false)
                                                   .buildPathSummary(buildPathSummary)
                                                   .customCommitTimestamps(commitTimestamp != null)
                                                   .storageType(storageType)
                                                   .useDeweyIDs(useDeweyIDs)
                                                   .hashKind(hashType)
                                                   .build());
      final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
      collections.put(database, collection);

      if (reader == null) {
        return collection;
      }

      try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
        wtx.commit(commitMessage, commitTimestamp);
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders) {
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
            (CompletableFuture.runAsync(() -> createResource(collName, database, jsonReader, resourceName)));
      }
      CompletableFuture.allOf(resourceFutures).join();
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
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
                                                                              resourceName)));
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
      final String resourceName) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .storageType(storageType)
                                                 .useTextCompression(false)
                                                 .buildPathSummary(buildPathSummary)
                                                 .hashKind(hashType)
                                                 .useDeweyIDs(useDeweyIDs)
                                                 .build());
    try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
         final JsonNodeTrx wtx = manager.beginNodeTrx()) {
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
                                                         .buildPathSummary(true)
                                                         .hashKind(hashType)
                                                         .build());
            try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
                 final JsonNodeTrx wtx = manager.beginNodeTrx()) {
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
      for (final var database : databases) {
        database.close();
      }
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }
}


package org.sirix.xquery.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.io.StorageType;
import org.sirix.service.json.shredder.JsonShredder;

/**
 * Database storage.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class BasicJsonDBStore implements JsonDBStore {

  /** User home directory. */
  private static final String USER_HOME = System.getProperty("user.home");

  /** Storage for databases: Sirix data in home directory. */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  /** {@link Set} of databases. */
  private final Set<Database<JsonResourceManager>> mDatabases;

  /** Mapping sirix databases to collections. */
  private final ConcurrentMap<Database<JsonResourceManager>, JsonDBCollection> mCollections;

  /** {@link StorageType} instance. */
  private final StorageType mStorageType;

  /** The location to store created collections/databases. */
  private final Path mLocation;

  /** Determines if a path summary should be built. */
  private boolean mBuildPathSummary;

  /** Get a new builder instance. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder setting up the store.
   */
  public static class Builder {
    /** Storage type. */
    private StorageType mStorageType = StorageType.FILE;

    /** The location to store created collections/databases. */
    private Path mLocation = LOCATION;

    /** Determines if for resources a path summary should be build. */
    private boolean mBuildPathSummary = true;

    /**
     * Set the storage type (default: file backend).
     *
     * @param storageType storage type
     * @return this builder instance
     */
    public Builder storageType(final StorageType storageType) {
      mStorageType = checkNotNull(storageType);
      return this;
    }

    /**
     * Set if path summaries should be build for resources.
     *
     * @param buildPathSummary {@code true} if path summaries should be build, {@code false} otherwise
     * @return this builder instance
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      mBuildPathSummary = buildPathSummary;
      return this;
    }

    /**
     * Set the location where to store the created databases/collections.
     *
     * @param location the location
     * @return this builder instance
     */
    public Builder location(final Path location) {
      mLocation = checkNotNull(location);
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
    mDatabases = Collections.synchronizedSet(new HashSet<>());
    mCollections = new ConcurrentHashMap<>();
    mStorageType = builder.mStorageType;
    mLocation = builder.mLocation;
    mBuildPathSummary = builder.mBuildPathSummary;
  }

  /** Get the location of the generated collections/databases. */
  public Path getLocation() {
    return mLocation;
  }

  @Override
  public JsonDBCollection lookup(final String name) {
    final Path dbPath = mLocation.resolve(name);
    if (Databases.existsDatabase(dbPath)) {
      try {
        final var database = Databases.openJsonDatabase(dbPath);
        final Optional<Database<JsonResourceManager>> storedCollection =
            mDatabases.stream().findFirst().filter(db -> db.equals(database));
        if (storedCollection.isPresent()) {
          return mCollections.get(storedCollection.get());
        }
        mDatabases.add(database);
        final JsonDBCollection collection = new JsonDBCollection(name, database);
        mCollections.put(database, collection);
        return collection;
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public JsonDBCollection create(final String name) {
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(mLocation.resolve(name));
    try {
      if (Databases.createXmlDatabase(dbConf)) {
        throw new DocumentException("Document with name %s exists!", name);
      }

      final var database = Databases.openJsonDatabase(dbConf.getFile());
      mDatabases.add(database);

      final JsonDBCollection collection = new JsonDBCollection(name, database);
      mCollections.put(database, collection);
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
  public JsonDBCollection create(final String collName, final String optResName, final Path path) {
    final Path dbPath = mLocation.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      Databases.removeDatabase(dbPath);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbPath);
      mDatabases.add(database);
      final String resName = optResName != null
          ? optResName
          : new StringBuilder(3).append("resource").append(database.listResources().size() + 1).toString();
      database.createResource(ResourceConfiguration.newBuilder(resName)
                                                   .useDeweyIDs(true)
                                                   .useTextCompression(true)
                                                   .buildPathSummary(mBuildPathSummary)
                                                   .storageType(mStorageType)
                                                   .build());
      final JsonDBCollection collection = new JsonDBCollection(collName, database);
      mCollections.put(database, collection);

      try (final JsonResourceManager manager = database.openResourceManager(resName);
          final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(path));

        wtx.commit();
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(final String collName, final @Nullable Stream<Path> paths) {
    if (paths != null) {
      final Path dbPath = mLocation.resolve(collName);
      final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
      try {
        Databases.removeDatabase(dbPath);
        Databases.createJsonDatabase(dbConf);
        final var database = Databases.openJsonDatabase(dbConf.getFile());
        mDatabases.add(database);
        final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        int i = database.listResources().size() + 1;
        try {
          Path path = null;
          while ((path = paths.next()) != null) {
            final Path currentPath = path;
            final String resourceName = new StringBuilder("resource").append(String.valueOf(i)).toString();
            pool.submit(() -> {
              database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                           .storageType(mStorageType)
                                                           .useDeweyIDs(true)
                                                           .useTextCompression(true)
                                                           .buildPathSummary(true)
                                                           .build());
              try (final JsonResourceManager manager = database.openResourceManager(resourceName);
                  final JsonNodeTrx wtx = manager.beginNodeTrx()) {
                final JsonDBCollection collection = new JsonDBCollection(collName, database);
                mCollections.put(database, collection);
                wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(currentPath));
                wtx.commit();
              }
              return null;
            });
            i++;
          }
        } finally {
          paths.close();
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        return new JsonDBCollection(collName, database);
      } catch (final SirixRuntimeException | InterruptedException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public void drop(final String name) {
    final Path dbPath = mLocation.resolve(name);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
    if (Databases.existsDatabase(dbPath)) {
      try {
        Databases.removeDatabase(dbPath);
        try (final var database = Databases.openJsonDatabase(dbConfig.getFile())) {
          mDatabases.remove(database);
          mCollections.remove(database);
        }
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e);
      }
    }
    throw new DocumentException("No collection with the specified name found!");
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
      for (final var database : mDatabases) {
        database.close();
      }
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }
}


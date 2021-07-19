package org.sirix.xquery.node;

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
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.io.StorageType;
import org.sirix.service.InsertPosition;

/**
 * Database storage.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class BasicXmlDBStore implements XmlDBStore {

  /** User home directory. */
  private static final String USER_HOME = System.getProperty("user.home");

  /** Storage for databases: Sirix data in home directory. */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  /** {@link Set} of databases. */
  private final Set<Database<XmlResourceManager>> databases;

  /** Mapping sirix databases to collections. */
  private final ConcurrentMap<Database<XmlResourceManager>, XmlDBCollection> collections;

  /** {@link StorageType} instance. */
  private final StorageType storageType;

  /** The location to store created collections/databases. */
  private final Path location;

  /** Determines if a path summary should be built. */
  private final boolean buildPathSummary;

  /** Thread pool. */
  private final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  /** Get a new builder instance. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder setting up the store.
   */
  public static class Builder {
    /** Storage type. */
    private StorageType storageType = StorageType.FILE;

    /** The location to store created collections/databases. */
    private Path location = LOCATION;

    /** Determines if for resources a path summary should be build. */
    private boolean buildPathSummary = true;

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
     * Create a new {@link BasicXmlDBStore} instance
     *
     * @return new {@link BasicXmlDBStore} instance
     */
    public BasicXmlDBStore build() {
      return new BasicXmlDBStore(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder instance
   */
  private BasicXmlDBStore(final Builder builder) {
    databases = Collections.synchronizedSet(new HashSet<>());
    collections = new ConcurrentHashMap<>();
    storageType = builder.storageType;
    location = builder.location;
    buildPathSummary = builder.buildPathSummary;
  }

  /** Get the location of the generated collections/databases. */
  public Path getLocation() {
    return location;
  }

  @Override
  public XmlDBCollection lookup(final String name) {
    final Path dbPath = location.resolve(name);
    if (Databases.existsDatabase(dbPath)) {
      try {
        final var database = Databases.openXmlDatabase(dbPath);
        final Optional<Database<XmlResourceManager>> storedCollection =
            databases.stream().findFirst().filter(db -> db.equals(database));
        if (storedCollection.isPresent()) {
          return collections.get(storedCollection.get());
        }
        databases.add(database);
        final XmlDBCollection collection = new XmlDBCollection(name, database);
        collections.put(database, collection);
        return collection;
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public XmlDBCollection create(final String name) {
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(location.resolve(name));
    try {
      if (Databases.createXmlDatabase(dbConf)) {
        throw new DocumentException("Document with name %s exists!", name);
      }

      final var database = Databases.openXmlDatabase(dbConf.getDatabaseFile());
      databases.add(database);

      final XmlDBCollection collection = new XmlDBCollection(name, database);
      collections.put(database, collection);
      return collection;
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public XmlDBCollection create(final String collName, final SubtreeParser parser) {
    return create(collName, null, parser);
  }

  @Override
  public XmlDBCollection create(final String collName, final String optResName, final SubtreeParser parser) {
    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      Databases.removeDatabase(dbPath);
      Databases.createXmlDatabase(dbConf);
      final var database = Databases.openXmlDatabase(dbPath);
      databases.add(database);
      final String resName = optResName != null
          ? optResName
          : "resource" + (database.listResources().size() + 1);
      database.createResource(ResourceConfiguration.newBuilder(resName)
                                                   .useDeweyIDs(true)
                                                   .useTextCompression(true)
                                                   .buildPathSummary(buildPathSummary)
                                                   .storageType(storageType)
                                                   .build());
      final XmlDBCollection collection = new XmlDBCollection(collName, database);
      collections.put(database, collection);

      try (final XmlResourceManager manager = database.openResourceManager(resName);
          final XmlNodeTrx wtx = manager.beginNodeTrx()) {
        parser.parse(new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList()));

        wtx.commit();
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public XmlDBCollection create(final String collName, final @Nullable Stream<SubtreeParser> parsers) {
    if (parsers != null) {
      final Path dbPath = location.resolve(collName);
      final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
      try {
        Databases.removeDatabase(dbPath);
        Databases.createXmlDatabase(dbConf);
        final var database = Databases.openXmlDatabase(dbConf.getDatabaseFile());
        databases.add(database);
        int i = database.listResources().size() + 1;
        try (parsers) {
          SubtreeParser parser;
          while ((parser = parsers.next()) != null) {
            final SubtreeParser nextParser = parser;
            final String resourceName = "resource" + i;
            pool.submit(() -> {
              database.createResource(ResourceConfiguration.newBuilder(resourceName).storageType(storageType).useDeweyIDs(true).useTextCompression(true).buildPathSummary(true).build());
              try (final XmlResourceManager manager = database.openResourceManager(resourceName);
                   final XmlNodeTrx wtx = manager.beginNodeTrx()) {
                  final XmlDBCollection collection = new XmlDBCollection(collName, database);
                  collections.put(database, collection);
                  nextParser.parse(new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD,
                                                      Collections.emptyList()));
                  wtx.commit();
              }
              return null;
            });
            i++;
          }
        }
        return new XmlDBCollection(collName, database);
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public void drop(final String name) {
    final Path dbPath = location.resolve(name);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
    if (Databases.existsDatabase(dbPath)) {
      try {
        Databases.removeDatabase(dbPath);
        try (final var database = Databases.openXmlDatabase(dbConfig.getDatabaseFile())) {
          databases.remove(database);
          collections.remove(database);
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
      for (final var database : databases) {
        database.close();
      }
      pool.shutdown();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (final SirixException | InterruptedException e) {
      throw new DocumentException(e.getCause());
    }
  }
}

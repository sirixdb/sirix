package io.sirix.query.node;

import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.node.parser.NodeSubtreeParser;
import org.jspecify.annotations.Nullable;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.settings.VersioningType;
import io.sirix.utils.OS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static io.sirix.query.StoreDatabasePaths.resolveForCreate;

/**
 * Database storage.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicXmlDBStore implements XmlDBStore {

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
  private final Set<Database<XmlResourceSession>> databases;

  /**
   * Mapping sirix databases to collections.
   */
  private final ConcurrentMap<Database<XmlResourceSession>, XmlDBCollection> collections;

  /**
   * Store defaults for every resource created directly or through a collection.
   */
  private final XmlResourceOptions resourceOptions;

  /**
   * The location to store created collections/databases.
   */
  private final Path location;

  /**
   * Number of inserted nodes before an auto-commit during a direct import.
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
            : OS.is64Bit()
                ? StorageType.MEMORY_MAPPED
                : StorageType.FILE_CHANNEL;

    /**
     * The location to store created collections/databases.
     */
    private Path location = System.getProperty("dbLocation") != null
        ? Path.of(System.getProperty("dbLocation"))
        : LOCATION;

    /**
     * Determines if a path summary should be build for resources.
     */
    private boolean buildPathSummary =
        System.getProperty("buildPathSummary") == null || Boolean.parseBoolean(System.getProperty("buildPathSummary"));

    /**
     * Determines if per-path value statistics should be maintained. Opt-in (default {@code false});
     * requires {@link #buildPathSummary} to be {@code true}.
     */
    private boolean buildPathStatistics = System.getProperty("buildPathStatistics") != null
        && Boolean.parseBoolean(System.getProperty("buildPathStatistics"));

    /**
     * Determines if DeweyIDs should be generated for resources.
     */
    private boolean storeDeweyIds =
        System.getProperty("storeDeweyIds") == null || Boolean.parseBoolean(System.getProperty("storeDeweyIds"));

    /**
     * Determines the hash type to use (default: rolling).
     */
    private HashType hashType = System.getProperty("hashType") != null
        ? HashType.fromString(System.getProperty("hashType"))
        : HashType.ROLLING;

    /**
     * Determines the versioning type.
     */
    private VersioningType versioningType = System.getProperty("versioningType") != null
        ? VersioningType.fromString(System.getProperty("versioningType"))
        : VersioningType.SLIDING_SNAPSHOT;

    /**
     * Number of nodes before an auto-commit is issued during an import of an XML document.
     */
    private int numberOfNodesBeforeAutoCommit = System.getProperty("numberOfNodesBeforeAutoCommit") != null
        ? Integer.parseInt(System.getProperty("numberOfNodesBeforeAutoCommit"))
        : 262_144 << 2;

    /**
     * Whether to maintain the per-insert record-to-revisions index. Overridable via
     * {@code -DstoreNodeHistory=false}.
     */
    private boolean storeNodeHistory =
        System.getProperty("storeNodeHistory") == null || Boolean.parseBoolean(System.getProperty("storeNodeHistory"));

    /**
     * Toggle the record-to-revisions index.
     *
     * @param storeNodeHistory {@code true} to enable, {@code false} to skip the per-insert index entry
     * @return this builder instance
     */
    public Builder storeNodeHistory(final boolean storeNodeHistory) {
      this.storeNodeHistory = storeNodeHistory;
      return this;
    }

    /**
     * Determines if DeweyIDs should be stored or not.
     *
     * @param storeDeweyIds determines if DeweyIDs should be stored or not
     * @return this builder instance
     */
    public Builder storeDeweyIds(boolean storeDeweyIds) {
      this.storeDeweyIds = storeDeweyIds;
      return this;
    }

    /**
     * Number of nodes, before the write trx is internally committed during an import of a file.
     *
     * @param numberOfNodesBeforeAutoCommit number of nodes to insert before an auto-commit is issued
     * @return this builder instance
     */
    public Builder numberOfNodesBeforeAutoCommit(final int numberOfNodesBeforeAutoCommit) {
      this.numberOfNodesBeforeAutoCommit = numberOfNodesBeforeAutoCommit;
      return this;
    }

    /**
     * Set the hash type (default: file backend).
     *
     * @param hashType hash type
     * @return this builder instance
     */
    public Builder hashType(final HashType hashType) {
      this.hashType = requireNonNull(hashType);
      return this;
    }

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
     * Set whether per-path value statistics should be maintained on PathSummary nodes. Enables the
     * aggregate short-circuit for {@code sum / avg / min / max / count} queries at the cost of some
     * write-path overhead. Requires {@link #buildPathSummary(boolean)} to be {@code true}.
     *
     * @param buildPathStatistics {@code true} to enable per-path statistics
     * @return this builder instance
     */
    public Builder buildPathStatistics(final boolean buildPathStatistics) {
      this.buildPathStatistics = buildPathStatistics;
      return this;
    }

    /**
     * Sets the versioning type of the storage.
     *
     * @param versioningType the versioning type to set
     * @return this builder instance
     */
    public Builder versioningType(final VersioningType versioningType) {
      this.versioningType = requireNonNull(versioningType);
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
     * Create a new {@link BasicXmlDBStore} instance
     *
     * @return new {@link BasicXmlDBStore} instance
     */
    public BasicXmlDBStore build() {
      return new BasicXmlDBStore(this);
    }

    XmlResourceOptions resourceOptions() {
      return new XmlResourceOptions(storageType, buildPathSummary, buildPathStatistics, storeDeweyIds, hashType,
          versioningType, storeNodeHistory);
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
    resourceOptions = builder.resourceOptions();
    location = builder.location;
    numberOfNodesBeforeAutoCommit = builder.numberOfNodesBeforeAutoCommit;
  }

  /**
   * Get the location of the generated collections/databases.
   */
  public Path getLocation() {
    return location;
  }

  @Override
  public XmlDBCollection lookup(final String name) {
    final Path dbPath = databasePath(name);
    if (Databases.existsDatabase(dbPath)) {
      try {
        for (final var collection : collections.values()) {
          final var database = collection.getDatabase();
          if (collection.getName().equals(name) && database.isOpen()
              && database.getDatabaseConfig().getDatabaseFile().equals(dbPath)) {
            return collection;
          }
        }

        final var database = Databases.openXmlDatabase(dbPath);
        databases.add(database);
        final XmlDBCollection collection = new XmlDBCollectionImpl(name, database, resourceOptions);
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
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(resolveForCreate(location.resolve(name)));
    try {
      if (!Databases.createXmlDatabase(dbConf)) {
        if (Databases.existsDatabase(dbConf.getDatabaseFile())) {
          throw new DocumentException("Document with name %s exists!", name);
        }
        throw new DocumentException("Could not create document with name %s", name);
      }

      final var database = Databases.openXmlDatabase(dbConf.getDatabaseFile());
      databases.add(database);

      final XmlDBCollection collection = new XmlDBCollectionImpl(name, database, resourceOptions);
      collections.put(database, collection);
      return collection;
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public XmlDBCollection create(final String collName, final NodeSubtreeParser parser) {
    return createCollection(collName, null, parser, null, null);
  }

  @Override
  public XmlDBCollection create(final String collName, final NodeSubtreeParser parser, final String commitMessage,
      final Instant commitTimestamp) {
    return createCollection(collName, null, parser, commitMessage, commitTimestamp);
  }

  @Override
  public XmlDBCollection create(final String collName, final String optResName, final NodeSubtreeParser parser) {
    return createCollection(collName, null, parser, null, null);
  }

  @Override
  public XmlDBCollection create(final String collName, final String optResName, final NodeSubtreeParser parser,
      final String commitMessage, final Instant commitTimestamp) {
    return createCollection(collName, null, parser, commitMessage, commitTimestamp);
  }

  private XmlDBCollection createCollection(final String collName, final @Nullable String optResName,
      final NodeSubtreeParser parser, final @Nullable String commitMessage, final @Nullable Instant commitTimestamp) {
    final Path dbPath = resolveForCreate(location.resolve(collName));
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createXmlDatabase(dbConf);
      final var database = Databases.openXmlDatabase(dbPath);
      databases.add(database);
      final String resName = optResName != null
          ? optResName
          : "resource" + (database.listResources().size() + 1);
      database.createResource(resourceOptions.create(resName, commitTimestamp));
      final XmlDBCollection collection = new XmlDBCollectionImpl(collName, database, resourceOptions);
      collections.put(database, collection);

      try (final XmlResourceSession resourceSession = database.beginResourceSession(resName);
          final XmlNodeTrx wtx = resourceSession.beginNodeTrx(numberOfNodesBeforeAutoCommit)) {
        parser.parse(new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList()));
        wtx.commit(commitMessage, commitTimestamp);
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  /**
   * Keep the executor scoped to this call: it must drain submitted imports before the parser stream
   * closes, including when parsing, stream iteration or the caller's wait fails.
   */
  @Override
  public XmlDBCollection create(final String collName, final @Nullable Stream<NodeSubtreeParser> parsers) {
    requireNonNull(collName);
    if (parsers == null) {
      return null;
    }
    final Path dbPath = resolveForCreate(location.resolve(collName));
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createXmlDatabase(dbConf);
      final var database = Databases.openXmlDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final XmlDBCollection collection = new XmlDBCollectionImpl(collName, database, resourceOptions);
      final var imports = new ArrayList<Future<?>>();
      int i = database.listResources().size() + 1;
      // Resources close in reverse order: workers must finish before their source stream closes.
      try (parsers;
          final ExecutorService importPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
        NodeSubtreeParser parser;
        while ((parser = parsers.next()) != null) {
          final NodeSubtreeParser nextParser = parser;
          final String resourceName = "resource" + i++;
          imports.add(importPool.submit(() -> {
            database.createResource(resourceOptions.create(resourceName, null));
            try (final XmlResourceSession resourceSession = database.beginResourceSession(resourceName);
                final XmlNodeTrx wtx = resourceSession.beginNodeTrx(numberOfNodesBeforeAutoCommit)) {
              nextParser.parse(
                  new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList()));
              wtx.commit();
            }
            return null;
          }));
        }
        try {
          for (final Future<?> imported : imports) {
            imported.get();
          }
        } catch (final InterruptedException e) {
          // Executor close observes this flag and interrupts the workers before draining them.
          Thread.currentThread().interrupt();
          throw new DocumentException(e);
        }
      }
      collections.put(database, collection);
      return collection;
    } catch (final ExecutionException e) {
      throw new DocumentException(e.getCause());
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e);
    }
  }

  private Path databasePath(final String name) {
    final Path dbPath = location.resolve(name);
    try {
      return Files.exists(dbPath)
          ? dbPath.toRealPath()
          : dbPath;
    } catch (final IOException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public void drop(final String name) {
    final Path dbPath = databasePath(name);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
    if (!removeIfExisting(dbConfig)) {
      throw new DocumentException("No collection with the specified name found!");
    }
  }

  private boolean removeIfExisting(final DatabaseConfiguration dbConfig) {
    if (Databases.existsDatabase(dbConfig.getDatabaseFile())) {
      try {
        final Predicate<Database<XmlResourceSession>> databasePredicate = currDatabase -> !currDatabase.isOpen()
            || currDatabase.getDatabaseConfig().getDatabaseFile().equals(dbConfig.getDatabaseFile());

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
      Files.createDirectory(Paths.get(path));
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

package io.sirix.access;

import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.*;
import io.sirix.cache.BufferManager;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.Encryptor;
import io.sirix.utils.SirixFiles;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class LocalDatabase<T extends ResourceSession<? extends NodeReadOnlyTrx, W>, W extends NodeTrx & NodeCursor>
    implements Database<T> {

  /**
   * Logger for {@link LocalDatabase}.
   */
  private static final Logger logger = LoggerFactory.getLogger(LocalDatabase.class);

  /**
   * Unique ID of a resource.
   */
  private final AtomicLong resourceID = new AtomicLong();

  /**
   * The transaction manager.
   */
  private final TransactionManager transactionManager;

  /**
   * Determines if the database instance is in the closed state or not.
   */
  private volatile boolean isClosed;

  /**
   * Central repository of all resource-ID/resource-name tuples.
   */
  private final BiMap<Long, String> resourceIDsToResourceNames;

  /**
   * DatabaseConfiguration with fixed settings.
   */
  private final DatabaseConfiguration dbConfig;

  /**
   * The session management instance.
   *
   * <p>Instances of this class are responsible for registering themselves in the pool (in
   * {@link #LocalDatabase(TransactionManager, DatabaseConfiguration, PathBasedPool, ResourceStore, WriteLocksRegistry, PathBasedPool)}),
   * as well as de-registering themselves (in {@link #close()}).
   */
  private final PathBasedPool<Database<?>> sessions;

  /**
   * The resource store to open/close resource-managers.
   */
  private final ResourceStore<T> resourceStore;

  private final PathBasedPool<ResourceSession<?, ?>> resourceSessions;

  /**
   * This field should be use to fetch the locks for resource managers.
   */
  private final WriteLocksRegistry writeLocks;

  /**
   * The global buffer manager shared across all databases and resources.
   */
  private final BufferManager bufferManager;

  /**
   * Constructor.
   *
   * @param transactionManager A manager for database transactions.
   * @param dbConfig           {@link ResourceConfiguration} reference to configure the {@link Database}
   * @param sessions           The database sessions management instance.
   * @param resourceStore      The resource store used by this database.
   * @param writeLocks         Manages the locks for resource managers.
   * @param resourceSessions   The pool for resource managers.
   */
  public LocalDatabase(final TransactionManager transactionManager, final DatabaseConfiguration dbConfig,
      final PathBasedPool<Database<?>> sessions, final ResourceStore<T> resourceStore,
      final WriteLocksRegistry writeLocks, final PathBasedPool<ResourceSession<?, ?>> resourceSessions) {
    this.transactionManager = transactionManager;
    this.dbConfig = requireNonNull(dbConfig);
    this.sessions = sessions;
    this.resourceStore = resourceStore;
    this.resourceSessions = resourceSessions;
    this.writeLocks = writeLocks;
    this.resourceIDsToResourceNames = Maps.synchronizedBiMap(HashBiMap.create());
    this.sessions.putObject(dbConfig.getDatabaseFile(), this);
    this.bufferManager = Databases.getGlobalBufferManager();
  }

  @Override
  public @NonNull T beginResourceSession(final String resourceName) {
    assertNotClosed();

    final Path resourcePath =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resourceName);

    if (!Files.exists(resourcePath)) {
      throw new SirixUsageException("Resource could not be opened (since it was not created?) at location",
                                    resourcePath.toString());
    }

    if (resourceStore.hasOpenResourceSession(resourcePath)) {
      return resourceStore.getOpenResourceSession(resourcePath);
    }

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourcePath);

    // Resource must be associated with this database.
    assert resourceConfig.resourcePath.getParent().getParent().equals(dbConfig.getDatabaseFile());

    // Keep track of the resource-ID.
    resourceIDsToResourceNames.forcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    // Use the global BufferManager for this resource session.
    // Cache keys include (databaseId, resourceId) to prevent collisions.
    return resourceStore.beginResourceSession(resourceConfig, bufferManager, resourcePath);
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resourceConfig) {
    assertNotClosed();

    boolean returnVal = true;
    resourceConfig.setDatabaseConfiguration(dbConfig);
    final Path path = dbConfig.getDatabaseFile()
                              .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                              .resolve(resourceConfig.resourcePath);
    // If file is existing, skip.
    if (Files.exists(path)) {
      return false;
    } else {
      try {
        Files.createDirectory(path);
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }

      if (returnVal) {
        // Creation of the folder structure.
        for (final ResourceConfiguration.ResourcePaths resourcePath : ResourceConfiguration.ResourcePaths.values()) {
          final Path toCreate = path.resolve(resourcePath.getPath());

          try {
            if (resourcePath.isFolder()) {
              Files.createDirectory(toCreate);

              if (resourcePath == ResourceConfiguration.ResourcePaths.ENCRYPTION_KEY)
                createAndStoreKeysetIfNeeded(resourceConfig, toCreate);
            } else {
              Files.createFile(toCreate);
            }
          } catch (UnsupportedOperationException | IOException | SecurityException e) {
            returnVal = false;
          }

          if (!returnVal)
            break;
        }
      }
    }

    if (returnVal) {
      // If everything was correct so far, initialize storage.

      // Serialization of the config.
      resourceID.set(dbConfig.getMaxResourceID());
      ResourceConfiguration.serialize(resourceConfig.setID(resourceID.getAndIncrement()));
      dbConfig.setMaximumResourceID(resourceID.get());
      resourceIDsToResourceNames.forcePut(resourceID.get(), resourceConfig.getResource().getFileName().toString());

      returnVal = bootstrapResource(resourceConfig);
    }

    if (!returnVal) {
      // If something was not correct, delete the partly created substructure.
      SirixFiles.recursiveRemove(resourceConfig.resourcePath);
    }

    // Note: With global BufferManager, no per-resource setup needed.
    // Cache keys include (databaseId, resourceId) to prevent collisions.

    return returnVal;
  }

  void createAndStoreKeysetIfNeeded(final ResourceConfiguration resConfig, final Path createdPath) {
    final Path encryptionKeyPath = createdPath.resolve("encryptionKey.json");
    if (resConfig.byteHandlePipeline.getComponents().contains(new Encryptor(createdPath.getParent()))) {
      try {
        Files.createFile(encryptionKeyPath);
        final KeysetHandle handle = KeysetHandle.generateNew(StreamingAeadKeyTemplates.AES256_CTR_HMAC_SHA256_4KB);
        CleartextKeysetHandle.write(handle, JsonKeysetWriter.withPath(encryptionKeyPath));
      } catch (final GeneralSecurityException | IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private boolean bootstrapResource(ResourceConfiguration resConfig) {
    try (final T resourceTrxManager = beginResourceSession(resConfig.getResource().getFileName().toString());
         final W wtx = resourceTrxManager.beginNodeTrx(AfterCommitState.CLOSE)) {
      final var useCustomCommitTimestamps = resConfig.customCommitTimestamps();
      if (useCustomCommitTimestamps) {
        wtx.commit(null, Instant.ofEpochMilli(0));
      } else {
        wtx.commit();
      }
      return true;
    } catch (final SirixException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean isOpen() {
    return !isClosed;
  }

  @Override
  public synchronized Database<T> removeResource(final String name) {
    assertNotClosed();
    requireNonNull(name);

    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);

    // Check that no running resource managers / sessions are opened.
    if (this.resourceSessions.containsAnyEntry(resourceFile)) {
      throw new IllegalStateException("Open resource managers found, must be closed first: " + resourceSessions);
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0) {
      // CRITICAL FIX: Clear BufferManager caches for this resource BEFORE deletion
      // This prevents cache pollution when resource is removed and recreated with same IDs
      try {
        final var resourceConfig = ResourceConfiguration.deserialize(resourceFile);
        long databaseId = dbConfig.getDatabaseId();
        long resourceId = resourceConfig.getID();
        
        if (bufferManager != null) {
          bufferManager.clearCachesForResource(databaseId, resourceId);
        }
      } catch (Exception e) {
        // If deserialization fails, resource config might be corrupt - continue with deletion
        logger.warn("Could not deserialize resource config for cache clearing: {}", e.getMessage());
      }
      
      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      this.writeLocks.removeWriteLock(resourceFile);

      // Construct the path used as key in the cache repositories
      // This matches StorageType.getIntegerRevisionFileDataAsyncCache and getRevisionIndexHolder
      final var cacheKey = resourceFile.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                       .resolve(IOStorage.FILENAME);
      
      final var cache = StorageType.CACHE_REPOSITORY.remove(cacheKey);
      if (cache != null) {
        cache.synchronous().invalidateAll();
      }
      
      // Clear the optimized revision index for this resource
      StorageType.REVISION_INDEX_REPOSITORY.remove(cacheKey);
    }

    return this;
  }

  @Override
  public synchronized String getResourceName(final @NonNegative long id) {
    assertNotClosed();
    return resourceIDsToResourceNames.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    return resourceIDsToResourceNames.inverse().get(requireNonNull(name));
  }

  private void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Database is already closed.");
    }
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    assertNotClosed();
    return dbConfig;
  }

  @Override
  public synchronized boolean existsResource(final String resourceName) {
    assertNotClosed();
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resourceName);
    return Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    try (final Stream<Path> stream = Files.list(dbConfig.getDatabaseFile()
                                                        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.toList();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Transaction beginTransaction() {
    // FIXME
    return null;
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      return;
    }

    logger.trace("Close local database instance.");

    isClosed = true;
    resourceStore.close();
    transactionManager.close();

    // Remove from database mapping.
    this.sessions.removeObject(dbConfig.getDatabaseFile(), this);

    // Free all allocated memory if it's the last database which is closed.
    Databases.freeAllocatedMemory();

    // Remove lock file.
    SirixFiles.recursiveRemove(dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dbConfig", dbConfig).toString();
  }
}

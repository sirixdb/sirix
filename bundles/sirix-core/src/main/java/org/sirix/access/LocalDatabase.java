package org.sirix.access;

import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.Database;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;
import org.sirix.cache.BufferManager;
import org.sirix.cache.BufferManagerImpl;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.io.StorageType;
import org.sirix.io.bytepipe.Encryptor;
import org.sirix.utils.SirixFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LocalDatabase<T extends ResourceManager<? extends NodeReadOnlyTrx, W>, W extends NodeTrx & NodeCursor>
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
  protected final TransactionManager transactionManager;

  /**
   * Determines if the database instance is in the closed state or not.
   */
  protected volatile boolean isClosed;

  /**
   * Buffers / page cache for each resource.
   */
  protected final ConcurrentMap<Path, BufferManager> bufferManagers;

  /**
   * Central repository of all resource-ID/resource-name tuples.
   */
  protected final BiMap<Long, String> resourceIDsToResourceNames;

  /**
   * DatabaseConfiguration with fixed settings.
   */
  protected final DatabaseConfiguration dbConfig;

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
  protected final ResourceStore<T> resourceStore;

  private final PathBasedPool<ResourceManager<?, ?>> resourceManagers;

  /**
   * This field should be use to fetch the locks for resource managers.
   */
  private final WriteLocksRegistry writeLocks;

  /**
   * Constructor.
   *
   * @param transactionManager A manager for database transactions.
   * @param dbConfig         {@link ResourceConfiguration} reference to configure the {@link Database}
   * @param sessions         The database sessions management instance.
   * @param resourceStore    The resource store used by this database.
   * @param writeLocks       Manages the locks for resource managers.
   * @param resourceManagers The pool for resource managers.
   */
  public LocalDatabase(final TransactionManager transactionManager,
                       final DatabaseConfiguration dbConfig,
                       final PathBasedPool<Database<?>> sessions,
                       final ResourceStore<T> resourceStore,
                       final WriteLocksRegistry writeLocks,
                       final PathBasedPool<ResourceManager<?, ?>> resourceManagers) {

    this.transactionManager = transactionManager;
    this.dbConfig = checkNotNull(dbConfig);
    this.sessions = sessions;
    this.resourceStore = resourceStore;
    this.resourceManagers = resourceManagers;
    this.writeLocks = writeLocks;
    resourceIDsToResourceNames = Maps.synchronizedBiMap(HashBiMap.create());
    bufferManagers = new ConcurrentHashMap<>();

    this.sessions.putObject(dbConfig.getDatabaseFile(), this);
  }

  private void addResourceToBufferManagerMapping(Path resourceFile, ResourceConfiguration resourceConfig) {
    if (resourceConfig.getStorageType() == StorageType.MEMORY_MAPPED) {
      bufferManagers.put(resourceFile, new BufferManagerImpl(100, 50, 150, 50_000_000));
    } else {
      bufferManagers.put(resourceFile, new BufferManagerImpl(5_000, 1_000, 1_000, 50_000_000));
    }
  }

  @Override
  public T openResourceManager(final String resourceName) {
    assertNotClosed();

    final Path resourceFile = dbConfig.getDatabaseFile()
            .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
            .resolve(resourceName);

    if (!Files.exists(resourceFile)) {
      throw new SirixUsageException("Resource could not be opened (since it was not created?) at location",
              resourceFile.toString());
    }

    if (resourceStore.hasOpenResourceManager(resourceFile)) {
      return resourceStore.getOpenResourceManager(resourceFile);
    }

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourceFile);

    // Resource of must be associated to this database.
    assert resourceConfig.resourcePath.getParent().getParent().equals(dbConfig.getDatabaseFile());

    // Keep track of the resource-ID.
    resourceIDsToResourceNames.forcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    if (!bufferManagers.containsKey(resourceFile)) {
      addResourceToBufferManagerMapping(resourceFile, resourceConfig);
    }

    return resourceStore.openResource(resourceConfig, bufferManagers.get(resourceFile), resourceFile);
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resConfig) {
    assertNotClosed();

    boolean returnVal = true;
    resConfig.setDatabaseConfiguration(dbConfig);
    final Path path = dbConfig.getDatabaseFile()
                              .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                              .resolve(resConfig.resourcePath);
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
                createAndStoreKeysetIfNeeded(resConfig, toCreate);
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
      ResourceConfiguration.serialize(resConfig.setID(resourceID.getAndIncrement()));
      dbConfig.setMaximumResourceID(resourceID.get());
      resourceIDsToResourceNames.forcePut(resourceID.get(), resConfig.getResource().getFileName().toString());

      returnVal = bootstrapResource(resConfig);
    }

    if (!returnVal) {
      // If something was not correct, delete the partly created substructure.
      SirixFiles.recursiveRemove(resConfig.resourcePath);
    }

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
    try (
            final T resourceTrxManager =
                    openResourceManager(resConfig.getResource().getFileName().toString());
            final W wtx = resourceTrxManager.beginNodeTrx()) {
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
    checkNotNull(name);

    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);

    // Check that no running resource managers / sessions are opened.
    if (this.resourceManagers.containsAnyEntry(resourceFile)) {
      throw new IllegalStateException("Open resource managers found, must be closed first: " + resourceManagers);
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0) {
      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      this.writeLocks.removeWriteLock(resourceFile);

      bufferManagers.remove(resourceFile);
    }

    return this;
  }

  @Override
  public synchronized String getResourceName(final @NonNegative long id) {
    assertNotClosed();
    checkArgument(id >= 0, "The ID must be >= 0!");
    return resourceIDsToResourceNames.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    return resourceIDsToResourceNames.inverse().get(checkNotNull(name));
  }

  protected void assertNotClosed() {
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
      return stream.collect(Collectors.toList());
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

    // Remove lock file.
    SirixFiles.recursiveRemove(dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dbConfig", dbConfig).toString();
  }
}

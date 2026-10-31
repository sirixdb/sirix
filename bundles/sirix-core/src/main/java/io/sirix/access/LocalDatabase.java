package io.sirix.access;

import io.sirix.utils.ToStringHelper;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.settings.VersioningType;
import com.google.crypto.tink.InsecureSecretKeyAccess;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.TinkJsonProtoKeysetFormat;
import com.google.crypto.tink.streamingaead.PredefinedStreamingAeadParameters;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.Transaction;
import io.sirix.api.TransactionManager;
import io.sirix.cache.BufferManager;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionSortedRunSpill;
import io.sirix.io.SuperblockValidator;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.Encryptor;
import io.sirix.utils.SirixFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

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

  private @Nullable DerivedIndexMaintainer derivedMaintainer;

  /**
   * Central repository of all resource-ID/resource-name tuples. Uses two synchronized maps (forward +
   * inverse) as a replacement for Guava BiMap.
   */
  private final Map<Long, String> resourceIDsToResourceNames;
  private final Map<String, Long> resourceNamesToResourceIDs;

  /**
   * DatabaseConfiguration with fixed settings.
   */
  private final DatabaseConfiguration dbConfig;

  /**
   * The resource store to open/close resource sessions.
   */
  private final ResourceStore<T> resourceStore;

  private final PathBasedPool<ResourceSession<?, ?>> resourceSessions;

  /**
   * This field should be used to fetch the locks for resource sessions.
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
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   * @param resourceStore The resource store used by this database.
   * @param writeLocks Manages the locks for resource sessions.
   * @param resourceSessions The pool for resource sessions.
   */
  public LocalDatabase(final TransactionManager transactionManager, final DatabaseConfiguration dbConfig,
      final ResourceStore<T> resourceStore, final WriteLocksRegistry writeLocks,
      final PathBasedPool<ResourceSession<?, ?>> resourceSessions) {
    this.transactionManager = transactionManager;
    this.dbConfig = requireNonNull(dbConfig);
    this.resourceStore = resourceStore;
    this.resourceSessions = resourceSessions;
    this.writeLocks = writeLocks;
    this.resourceIDsToResourceNames = new HashMap<>();
    this.resourceNamesToResourceIDs = new HashMap<>();
    this.bufferManager = Databases.getGlobalBufferManager();
  }

  String canonicalResourceName(final String name) {
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), name, false);
    final String canonicalName = ResourceConfiguration.canonicalResourceName(name);
    final Path path =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName);
    if (!Files.exists(path)) {
      return canonicalName;
    }
    try {
      return path.toRealPath().getFileName().toString();
    } catch (final IOException exception) {
      throw new SirixIOException(exception);
    }
  }

  @Override
  public synchronized T beginResourceSession(final String resourceName) {
    assertNotClosed();
    final String canonicalName = canonicalResourceName(resourceName);
    final Path dataDir = dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path resourcePath = dataDir.resolve(canonicalName);

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
    biMapForcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    ProjectionSortedRunSpill.removeOrphanedRuns(resourceConfig);

    // Use the global BufferManager for this resource session.
    // Cache keys include (databaseId, resourceId) to prevent collisions.
    final T session = resourceStore.beginResourceSession(resourceConfig, bufferManager, resourcePath);
    if (DerivedIndexMaintainer.enabled(resourceConfig) && resourceConfig.getValidTimeConfig() != null
        && session instanceof JsonResourceSession json
        && !DerivedIndexMaintainer.isDerivedResourceName(canonicalName)) {
      final AbstractResourceSession<?, ?> shared = (AbstractResourceSession<?, ?>) session;
      shared.initializeDerivedIndexesWith(() -> enableDerivedIndexes(json));
      if (Files.exists(resourcePath.resolveSibling(resourcePath.getFileName() + DerivedIndexMaintainer.SUFFIX))
          || (session.getMostRecentRevisionNumber() > 0
              && json.getRtxIndexController(session.getMostRecentRevisionNumber())
                     .getIndexes()
                     .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE) != 0)) {
        shared.enableDerivedIndexes();
      }
    }
    return session;
  }

  private synchronized void enableDerivedIndexes(final JsonResourceSession source) {
    assertNotClosed();
    final AbstractResourceSession<?, ?> shared = (AbstractResourceSession<?, ?>) source;
    if (shared.getDerivedResource() != null) {
      return;
    }
    try {
      final JsonResourceSession derived = openDerivedResource(source);
      shared.retainForDerivedIndexes();
      ((AbstractResourceSession<?, ?>) derived).retainForDerivedIndexes();
      resourceSessions.removeObject(source.getResourcePath(), source);
      resourceSessions.removeObject(derived.getResourcePath(), derived);
      if (derivedMaintainer == null) {
        derivedMaintainer = new DerivedIndexMaintainer(getName());
      }
      shared.setDerivedResource(derivedMaintainer.register(source, derived));
    } catch (final RuntimeException unavailable) {
      // The source and its log remain authoritative even when derived storage cannot be opened.
      logger.error("Derived resource unavailable for {}; queries retain exact fallback", source.getResourcePath(),
          unavailable);
    }
  }

  private JsonResourceSession openDerivedResource(final JsonResourceSession source) {
    final String name = source.getResourcePath().getFileName() + DerivedIndexMaintainer.SUFFIX;
    final Path derivedPath = source.getResourcePath().resolveSibling(name);
    final List<ByteHandler> sourceHandlers = source.getResourceConfig().byteHandlePipeline.getComponents();
    final boolean encrypted = sourceHandlers.stream().anyMatch(Encryptor.class::isInstance);
    if (Files.exists(derivedPath)) {
      try {
        if (ResourceConfiguration.ResourcePaths.compareStructure(derivedPath) != 0) {
          throw new SirixIOException("Incomplete derived resource: " + name);
        }
        if (encrypted) {
          final List<ByteHandler> derivedHandlers =
              ResourceConfiguration.deserialize(derivedPath).byteHandlePipeline.getComponents();
          for (final ByteHandler handler : sourceHandlers) {
            if (handler instanceof Encryptor && !derivedHandlers.contains(handler)) {
              throw new SirixIOException("Derived resource does not preserve source encryption: " + name);
            }
          }
        }
        final JsonResourceSession derived = (JsonResourceSession) beginResourceSession(name);
        DerivedIndexMaintainer.validateSnapshot(derived);
        return derived;
      } catch (final RuntimeException unreadable) {
        logger.warn("Rebuilding unreadable derived resource {} from the source log", name, unreadable);
        final Path path = source.getResourcePath().resolveSibling(name);
        final T existing = resourceStore.getOpenResourceSession(path);
        if (existing != null) {
          resourceSessions.removeObject(path, existing);
        }
        removeResource(name);
      }
    }
    final ResourceConfiguration.Builder builder =
        ResourceConfiguration.newBuilder(name)
                             .storageType(StorageType.FILE_CHANNEL)
                             .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                             .storeDiffs(false)
                             .storeNodeHistory(false)
                             .buildPathSummary(false)
                             .customCommitTimestamps(true)
                             .aggregateKernel(true);
    if (encrypted) {
      builder.byteHandlerPipeline(new ByteHandlerPipeline(source.getResourceConfig().byteHandlePipeline));
    }
    if (!createResource(builder.build(), null)) {
      throw new SirixIOException("Could not create derived resource " + name);
    }
    return (JsonResourceSession) beginResourceSession(name);
  }

  /** A handle owns user sessions; the database owns their common storage and revision state. */
  <S extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> ResourceStore<S> newUserResourceStore(
      final User user) {
    requireNonNull(user);
    final AtomicReference<ResourceStore<S>> storeReference = new AtomicReference<>();
    final ResourceStore<S> store = new ResourceStoreImpl<>(resourceSessions, (config, buffers, path) -> {
      final T shared = resourceStore.getOpenResourceSession(path);
      // ResourceStoreImpl stores this factory without invoking it; storeReference is set before
      // this store is returned to a handle, so every later factory invocation sees that store.
      @SuppressWarnings({"unchecked", "NullAway"})
      final S session = (S) ((AbstractResourceSession<?, ?>) shared).openUserSession(storeReference.get(), user);
      return session;
    }, session -> ((AbstractResourceSession<?, ?>) session).releaseUserSession(), this);
    storeReference.set(store);
    return store;
  }

  synchronized <S extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> S beginUserResourceSession(
      final String resourceName, final ResourceStore<S> userStore) {
    final String canonicalName = canonicalResourceName(resourceName);
    if (DerivedIndexMaintainer.isDerivedResourceName(canonicalName)) {
      throw new SirixUsageException("Derived resources are internal: " + canonicalName);
    }
    final T shared = beginResourceSession(canonicalName);
    // The hidden database-owned session keeps storage warm, but is not a user of the resource.
    // The user session registration below is atomic with respect to removeResource's guard.
    resourceSessions.removeObject(shared.getResourcePath(), shared);
    return userStore.beginResourceSession(shared.getResourceConfig(), bufferManager, shared.getResourcePath());
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resourceConfig) {
    if (DerivedIndexMaintainer.isDerivedResourceName(canonicalResourceName(requireNonNull(resourceConfig).getName()))) {
      throw new SirixUsageException("Derived resource names are reserved");
    }
    return createResource(resourceConfig, null);
  }

  synchronized boolean createResource(final ResourceConfiguration resourceConfig,
      final @Nullable ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> userStore) {
    assertNotClosed();
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), requireNonNull(resourceConfig).getName(), true);

    boolean returnVal = true;
    resourceConfig.setDatabaseConfiguration(dbConfig);
    final Path path = resourceConfig.getResource();

    if (userStore != null
        && DerivedIndexMaintainer.isDerivedResourceName(canonicalResourceName(resourceConfig.getName()))) {
      throw new SirixUsageException("Derived resource names are reserved");
    }

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

      // Serialization of the config. Bump the persisted max resource ID BEFORE serializing:
      // ResourceConfiguration.serialize() re-writes the database config as its last step, and the
      // old ordering left dbConfig's max at the OLD value at that moment (setMaximumResourceID ran
      // afterwards, in memory only). The on-disk counter therefore stayed one behind, so after a
      // reopen the next createResource re-assigned the SAME id — colliding in the resource bimap
      // and, because the GLOBAL BufferManager keys caches by (databaseId, resourceId), serving one
      // resource's cached pages for another (cross-resource data leak).
      resourceID.set(dbConfig.getMaxResourceID());
      resourceConfig.setID(resourceID.getAndIncrement());
      dbConfig.setMaximumResourceID(resourceID.get());
      ResourceConfiguration.serialize(resourceConfig);
      biMapForcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

      returnVal = bootstrapResource(resourceConfig, userStore);
    }

    if (!returnVal) {
      // If something was not correct, delete the partly created substructure.
      biMapInverseRemove(resourceConfig.getResource().getFileName().toString());
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
        final KeysetHandle handle =
            KeysetHandle.generateNew(PredefinedStreamingAeadParameters.AES256_CTR_HMAC_SHA256_4KB);
        final String keysetJson = TinkJsonProtoKeysetFormat.serializeKeyset(handle, InsecureSecretKeyAccess.get());
        Files.writeString(encryptionKeyPath, keysetJson);
      } catch (final GeneralSecurityException | IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private boolean bootstrapResource(final ResourceConfiguration resConfig,
      final @Nullable ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> userStore) {
    final String resourceName = resConfig.getResource().getFileName().toString();
    try (final ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceTrxManager = userStore == null
        ? beginResourceSession(resourceName)
        : beginUserResourceSession(resourceName, userStore);
        final NodeTrx wtx = resourceTrxManager.beginNodeTrx(AfterCommitState.CLOSE)) {
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
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), canonicalResourceName(name), true);
    assertNotClosed();
    final String canonicalName = canonicalResourceName(name);
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName);

    // Check that no running resource sessions are opened.
    if (this.resourceSessions.containsAnyEntry(resourceFile)) {
      throw new IllegalStateException("Open resource sessions found, must be closed first: " + resourceSessions);
    }

    if (derivedMaintainer != null) {
      derivedMaintainer.remove(resourceFile);
    }
    final T shared = resourceStore.getOpenResourceSession(resourceFile);
    if (shared != null) {
      shared.close();
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile) && (DerivedIndexMaintainer.isDerivedResourceName(canonicalName)
        || ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0)) {
      // CRITICAL FIX: Clear BufferManager caches for this resource BEFORE deletion
      // This prevents cache pollution when resource is removed and recreated with same IDs
      try {
        final var resourceConfig = ResourceConfiguration.deserialize(resourceFile);
        long databaseId = dbConfig.getDatabaseId();
        long resourceId = resourceConfig.getID();

        if (bufferManager != null) {
          bufferManager.clearCachesForResource(databaseId, resourceId);
        }
        AbstractResourceSession.invalidateRevisionInfoCache(databaseId, resourceId);
      } catch (Exception e) {
        // If deserialization fails, resource config might be corrupt - continue with deletion
        logger.warn("Could not deserialize resource config for cache clearing: {}", e.getMessage());
      }

      // The resource's files are deleted below — a recreated resource at this path must get its
      // fresh superblock validated again.
      SuperblockValidator.invalidateUnder(resourceFile);

      // Projection decode cache is keyed by resource path — a recreated
      // resource must never be served the removed store's decoded columns.
      ProjectionIndexCatalog.invalidateUnder(resourceFile.toAbsolutePath().toString());

      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      this.writeLocks.removeWriteLock(resourceFile);

      // Construct the path used as key in the cache repositories
      // This matches StorageType.getIntegerRevisionFileDataAsyncCache and getRevisionIndexHolder
      final var cacheKey =
          resourceFile.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(IOStorage.FILENAME);

      final var cache = StorageType.CACHE_REPOSITORY.remove(cacheKey);
      if (cache != null) {
        cache.synchronous().invalidateAll();
      }

      // Clear the optimized revision index for this resource
      StorageType.REVISION_INDEX_REPOSITORY.remove(cacheKey);

      // Drop the name<->ID bimap entry — it used to survive removal, so getResourceID(name)
      // kept answering with the dead resource's ID.
      biMapInverseRemove(canonicalName);
    }

    final String derivedName = resourceFile.getFileName() + DerivedIndexMaintainer.SUFFIX;
    if (!DerivedIndexMaintainer.isDerivedResourceName(canonicalName)
        && Files.exists(resourceFile.resolveSibling(derivedName))) {
      removeResource(derivedName);
    }
    return this;
  }

  @Override
  public synchronized String getResourceName(final long id) {
    assertNotClosed();
    return resourceIDsToResourceNames.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    final Long id = resourceNamesToResourceIDs.get(canonicalResourceName(name));
    if (id == null) {
      throw new SirixUsageException("No resource found with name: " + name);
    }
    return id;
  }

  private void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Database is already closed.");
    }
  }

  /**
   * BiMap-style forcePut: removes any existing mapping for either the key or the value, then inserts
   * the new mapping into both forward and inverse maps.
   */
  private void biMapForcePut(final long id, final String name) {
    // Remove existing mapping for this value (name) if present
    final Long existingId = resourceNamesToResourceIDs.remove(name);
    if (existingId != null) {
      resourceIDsToResourceNames.remove(existingId);
    }
    // Remove existing mapping for this key (id) if present
    final String existingName = resourceIDsToResourceNames.remove(id);
    if (existingName != null) {
      resourceNamesToResourceIDs.remove(existingName);
    }
    resourceIDsToResourceNames.put(id, name);
    resourceNamesToResourceIDs.put(name, id);
  }

  /**
   * BiMap-style inverse remove: removes the mapping by name (value in forward map).
   */
  private void biMapInverseRemove(final String name) {
    final Long id = resourceNamesToResourceIDs.remove(name);
    if (id != null) {
      resourceIDsToResourceNames.remove(id);
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
    final String canonicalName = canonicalResourceName(resourceName);
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName);
    return Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    try (final Stream<Path> stream =
        Files.list(dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.filter(
          path -> !DerivedIndexMaintainer.isDerivedResourceName(canonicalResourceName(path.getFileName().toString())))
                   .toList();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Transaction beginTransaction() {
    throw new UnsupportedOperationException("Multi-resource transactions are not yet implemented. "
        + "Use beginNodeTrx() on individual ResourceSession instances instead.");
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      return;
    }

    logger.trace("Close local database instance.");

    if (derivedMaintainer != null) {
      derivedMaintainer.close();
    }
    resourceStore.close();
    transactionManager.close();
    isClosed = true;
    Databases.freeAllocatedMemory();
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("dbConfig", dbConfig).toString();
  }
}

package io.sirix.access;

import io.sirix.cache.ShardedPageCache;
import io.sirix.io.ResourceFormatCompatibility.Proof;
import io.sirix.exception.UnsupportedFormatVersionException;
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
import io.sirix.exception.SirixRuntimeException;
import io.sirix.exception.SirixUsageException;
import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.io.IOStorage;
import io.sirix.io.ResourceFormatCompatibility;
import io.sirix.page.StoreFeatures;
import io.sirix.io.StorageType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionSortedRunSpill;
import io.sirix.io.SuperblockValidator;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.Encryptor;
import io.sirix.io.ram.RAMStorage;
import io.sirix.utils.SirixFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
  private volatile boolean closing;

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
  private final DatabaseNotifications notifications;

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
   * Names of discarded IN_MEMORY resources whose directories could not be retired (TXN-615 clauses 6,
   * 7 and 8). They are treated as absent; creating one retries the retirement.
   */
  private final Set<String> hiddenInMemoryLeftovers = ConcurrentHashMap.newKeySet();

  private volatile boolean retirementScanPending;

  private final Map<Path, ResourceConfiguration> knownInMemoryResources = new HashMap<>();

  private final Map<Path, CleanupProof> pendingCleanup = new HashMap<>();

  private record CleanupProof(@Nullable Path source, @Nullable UUID incarnation) {
  }

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
    this.notifications = requireNonNull(resourceStore.getNotifications());
    this.notifications.bindDatabase(dbConfig.getDatabaseFile());
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

  private boolean hasKnownInMemorySource(final Path resourceFile) {
    final String name = resourceFile.getFileName().toString();
    return DerivedIndexMaintainer.isDerivedResourceName(name) && knownInMemoryResources.containsKey(
        resourceFile.resolveSibling(name.substring(0, name.length() - DerivedIndexMaintainer.SUFFIX.length())));
  }

  record OpenedSession<S extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>(S session,
      @Nullable JsonResourceSession derivedSource, boolean newlyAcquired) {
  }

  @Override
  public T beginResourceSession(final String resourceName) {
    final OpenedSession<T> opened = beginResourceSession(resourceName, null);
    finishResourceOpen(opened);
    return opened.session();
  }

  private synchronized OpenedSession<T> beginResourceSession(final String resourceName,
      final @Nullable Proof compatibility) {
    assertAcceptingResources();
    ensureInMemoryLeftoversRetired();
    final String canonicalName = canonicalResourceName(resourceName);
    final Path dataDir = dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path resourcePath = dataDir.resolve(canonicalName);

    if (!Files.exists(resourcePath) || hiddenInMemoryLeftovers.contains(canonicalName)
        || InMemoryResources.isRetiredResourceName(canonicalName)) {
      throw new SirixUsageException("Resource could not be opened (since it was not created?) at location",
          resourcePath.toString());
    }

    if (resourceStore.hasOpenResourceSession(resourcePath)) {
      final T session = resourceStore.getOpenResourceSession(resourcePath);
      return new OpenedSession<>(session, prepareDerivedIndexes(session, canonicalName), false);
    }

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourcePath);
    if (resourceConfig.storageType == StorageType.IN_MEMORY || hasKnownInMemorySource(resourcePath)) {
      knownInMemoryResources.put(resourcePath, resourceConfig);
    } else {
      knownInMemoryResources.remove(resourcePath);
    }

    // Resource must be associated with this database.
    assert resourceConfig.resourcePath.getParent().getParent().equals(dbConfig.getDatabaseFile());

    // Keep track of the resource-ID.
    biMapForcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    // Use the global BufferManager for this resource session.
    // Cache keys include (databaseId, resourceId) to prevent collisions.
    final T session = compatibility == null
        ? resourceStore.beginResourceSession(resourceConfig, bufferManager, resourcePath)
        : resourceStore.beginResourceSession(resourceConfig, bufferManager, resourcePath, compatibility);
    final StoreFeatures features = ((AbstractResourceSession<?, ?>) session).getStoreFeatures();
    if (features.readOnlyCompat() == 0) {
      ProjectionSortedRunSpill.removeOrphanedRuns(resourceConfig);
    }
    return new OpenedSession<>(session, prepareDerivedIndexes(session, canonicalName), true);
  }

  private @Nullable JsonResourceSession prepareDerivedIndexes(final T session, final String canonicalName) {
    final ResourceConfiguration resourceConfig = session.getResourceConfig();
    final StoreFeatures features = ((AbstractResourceSession<?, ?>) session).getStoreFeatures();
    if (features.readOnlyCompat() == 0 && DerivedIndexMaintainer.enabled(resourceConfig)
        && resourceConfig.getValidTimeConfig() != null && session instanceof JsonResourceSession json
        && !DerivedIndexMaintainer.isDerivedResourceName(canonicalName)) {
      final AbstractResourceSession<?, ?> shared = (AbstractResourceSession<?, ?>) session;
      shared.initializeDerivedIndexesWith(() -> enableDerivedIndexes(json, DerivedActivation.DECLARED));
      return json;
    }
    return null;
  }

  void finishResourceOpen(final OpenedSession<?> opened) {
    final @Nullable JsonResourceSession source = opened.derivedSource();
    if (source != null) {
      try {
        enableDerivedIndexes(source, DerivedActivation.OPPORTUNISTIC);
      } catch (final RuntimeException | Error failure) {
        if (opened.newlyAcquired()) {
          try {
            opened.session().close();
          } catch (final RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
          }
        }
        throw failure;
      }
    }
  }

  /**
   * Why derived indexes are being installed, which is what a failure to install them means.
   */
  private enum DerivedActivation {
    /**
     * A caller declared a temporal aggregate, so installing the hidden derived resource IS the caller's
     * request. A failure is the declaration's failure and must reach the caller: absorbing it would let
     * {@code createIndexes} report success while the index it was asked to create does not exist, and
     * the resource would serve the exact fallback for the rest of its session with nothing but a log
     * line to say so.
     */
    DECLARED,

    /**
     * A resource is being opened and may carry derived indexes from an earlier session. Installing them
     * is opportunistic: every query retains the exact fallback, so an unusable derived store must not
     * stop the resource from opening. Only the named failures of {@link #enableDerivedIndexes} are
     * absorbed here.
     */
    OPPORTUNISTIC
  }

  /**
   * Installs the hidden derived resource and registers it with the maintainer.
   *
   * <p>
   * {@link DerivedActivation#DECLARED} propagates every failure.
   * {@link DerivedActivation#OPPORTUNISTIC} absorbs only the named exceptions that mean "this derived
   * store is unusable" — a broken or forward-incompatible store ({@link SirixException},
   * {@link SirixRuntimeException}, {@link UnsupportedStoreFeatureException}), a snapshot whose
   * frontier has no durable mapping ({@link IllegalStateException}), or a derived subtree that cannot
   * be walked or removed ({@link UncheckedIOException}), or an authorization policy that does not
   * grant writes to the hidden derived resource ({@link SecurityException}) — and lets anything else
   * through. Foreign format versions always propagate. Runtime failures outside these named types are
   * not treated as an optional fallback.
   */
  private void enableDerivedIndexes(final JsonResourceSession source, final DerivedActivation activation) {
    final boolean onlyIfDeclared = activation == DerivedActivation.OPPORTUNISTIC;
    try {
      synchronized (this) {
        assertAcceptingResources();
        if (!needsDerivedInitialization(source, onlyIfDeclared)) {
          return;
        }
      }
      try (var retirement = notifications.retire()) {
        synchronized (this) {
          assertAcceptingResources();
          if (!needsDerivedInitialization(source, onlyIfDeclared)) {
            return;
          }
          final AbstractResourceSession<?, ?> shared = (AbstractResourceSession<?, ?>) source;
          final JsonResourceSession derived = openDerivedResource(source);
          shared.retainForDerivedIndexes();
          ((AbstractResourceSession<?, ?>) derived).retainForDerivedIndexes();
          resourceSessions.removeObject(source.getResourcePath(), source);
          resourceSessions.removeObject(derived.getResourcePath(), derived);
          if (derivedMaintainer == null) {
            derivedMaintainer = new DerivedIndexMaintainer(getName());
          }
          shared.setDerivedResource(derivedMaintainer.register(source, derived));
        }
      }
    } catch (final UnsupportedFormatVersionException unsupported) {
      throw unsupported;
    } catch (final SirixException | SirixRuntimeException | UnsupportedStoreFeatureException | IllegalStateException
        | UncheckedIOException | SecurityException unavailable) {
      if (activation == DerivedActivation.DECLARED) {
        throw unavailable;
      }
      logger.error("Derived resource unavailable for {}; queries retain exact fallback", source.getResourcePath(),
          unavailable);
    }
  }

  private boolean needsDerivedInitialization(final JsonResourceSession source, final boolean onlyIfDeclared) {
    if (((AbstractResourceSession<?, ?>) source).getDerivedResource() != null) {
      return false;
    }
    final Path path = source.getResourcePath();
    return !onlyIfDeclared || Files.exists(path.resolveSibling(path.getFileName() + DerivedIndexMaintainer.SUFFIX))
        || source.getMostRecentRevisionNumber() != 0
            && source.getRtxIndexController(source.getMostRecentRevisionNumber())
                     .getIndexes()
                     .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE) != 0;
  }

  private JsonResourceSession openDerivedResource(final JsonResourceSession source) {
    final String name = source.getResourcePath().getFileName() + DerivedIndexMaintainer.SUFFIX;
    final Path derivedPath = source.getResourcePath().resolveSibling(name);
    final List<ByteHandler> sourceHandlers = source.getResourceConfig().byteHandlePipeline.getComponents();
    final boolean encrypted = sourceHandlers.stream().anyMatch(Encryptor.class::isInstance);
    if (Files.exists(derivedPath)) {
      ResourceConfiguration derivedConfig = null;
      try {
        derivedConfig = ResourceConfiguration.deserialize(derivedPath);
        final Proof derivedCompatibility = ResourceFormatCompatibility.verify(derivedConfig);
        derivedCompatibility.uberPage().getFeatures().requireWritable();
        if (ResourceConfiguration.ResourcePaths.compareStructure(derivedPath) != 0) {
          throw new SirixIOException("Incomplete derived resource: " + name);
        }
        if (encrypted) {
          final List<ByteHandler> derivedHandlers = derivedConfig.byteHandlePipeline.getComponents();
          for (final ByteHandler handler : sourceHandlers) {
            if (handler instanceof Encryptor && !derivedHandlers.contains(handler)) {
              throw new SirixIOException("Derived resource does not preserve source encryption: " + name);
            }
          }
        }
        final JsonResourceSession derived =
            (JsonResourceSession) beginResourceSession(name, derivedCompatibility).session();
        DerivedIndexMaintainer.validateSnapshot(derived);
        return derived;
      } catch (final UnsupportedStoreFeatureException unsupported) {
        throw unsupported;
      } catch (final RuntimeException unreadable) {
        // Missing settings must not bypass the side-effect-free compatibility gate before deletion.
        // Derived stores use the default pipeline, or the source's pipeline when encrypted.
        final Proof compatibility;
        if (derivedConfig != null) {
          compatibility = ResourceFormatCompatibility.verify(derivedConfig);
        } else {
          final ResourceConfiguration.Builder probe = ResourceConfiguration.newBuilder(name);
          if (encrypted) {
            probe.byteHandlerPipeline(new ByteHandlerPipeline(source.getResourceConfig().byteHandlePipeline));
          }
          compatibility =
              ResourceFormatCompatibility.verifyWithoutSettings(probe.build().setDatabaseConfiguration(dbConfig));
        }
        compatibility.uberPage().getFeatures().requireWritable();
        compatibility.requireResource(derivedPath);
        logger.warn("Rebuilding unreadable derived resource {} from the source log", name, unreadable);
        final Path path = source.getResourcePath().resolveSibling(name);
        final T existing = resourceStore.getOpenResourceSession(path);
        if (existing != null) {
          resourceSessions.removeObject(path, existing);
        }
        removeResourceInternal(name);
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
  synchronized <S extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> ResourceStore<S> newUserResourceStore(
      final User user) {
    requireNonNull(user);
    assertAcceptingResources();
    final AtomicReference<ResourceStore<S>> storeReference = new AtomicReference<>();
    final ResourceStore<S> store = new ResourceStoreImpl<>(resourceSessions, (config, buffers, path) -> {
      final T shared = resourceStore.getOpenResourceSession(path);
      // ResourceStoreImpl stores this factory without invoking it; storeReference is set before
      // this store is returned to a handle, so every later factory invocation sees that store.
      @SuppressWarnings({"unchecked", "NullAway"})
      final S session = (S) ((AbstractResourceSession<?, ?>) shared).openUserSession(storeReference.get(), user);
      return session;
    }, session -> ((AbstractResourceSession<?, ?>) session).releaseUserSession(), this, notifications);
    storeReference.set(store);
    return store;
  }

  synchronized <S extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> OpenedSession<S> beginUserResourceSession(
      final String resourceName, final ResourceStore<S> userStore) {
    final String canonicalName = canonicalResourceName(resourceName);
    if (DerivedIndexMaintainer.isDerivedResourceName(canonicalName)) {
      throw new SirixUsageException("Derived resources are internal: " + canonicalName);
    }
    final OpenedSession<T> opened = beginResourceSession(canonicalName, null);
    final T shared = opened.session();
    // The hidden database-owned session keeps storage warm, but is not a user of the resource.
    // The user session registration below is atomic with respect to removeResource's guard.
    resourceSessions.removeObject(shared.getResourcePath(), shared);
    final boolean newlyAcquired = !userStore.hasOpenResourceSession(shared.getResourcePath());
    return new OpenedSession<>(
        userStore.beginResourceSession(shared.getResourceConfig(), bufferManager, shared.getResourcePath()),
        opened.derivedSource(), newlyAcquired);
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }

  @Override
  public boolean createResource(final ResourceConfiguration resourceConfig) {
    notifications.assertNotInCallback();
    if (DerivedIndexMaintainer.isDerivedResourceName(canonicalResourceName(requireNonNull(resourceConfig).getName()))) {
      throw new SirixUsageException("Derived resource names are reserved");
    }
    return createResource(resourceConfig, null);
  }

  boolean createResource(final ResourceConfiguration resourceConfig,
      final @Nullable ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> userStore) {
    try (var retirement = notifications.retire()) {
      return createResourceQuiescent(resourceConfig, userStore);
    }
  }

  private synchronized boolean createResourceQuiescent(final ResourceConfiguration resourceConfig,
      final @Nullable ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> userStore) {
    assertAcceptingResources();
    ensureInMemoryLeftoversRetired();
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), requireNonNull(resourceConfig).getName(), true);

    resourceConfig.setDatabaseConfiguration(dbConfig);
    final Path path = resourceConfig.getResource();
    requireCreatableResourceName(resourceConfig, userStore != null);

    // A leftover of a former IN_MEMORY resource holds nothing: retry its removal (TXN-615 clause 7).
    final String name = path.getFileName().toString();
    if (hiddenInMemoryLeftovers.contains(name) && path.equals(retireInMemoryResource(path, name))) {
      return false;
    }

    // If file is existing, skip.
    if (Files.exists(path)) {
      return false;
    }
    boolean returnVal = createResourceStructure(resourceConfig, path);

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

  /**
   * Refuse reserved resource names: derived-index names for a user store, and the retirement marker
   * of IN_MEMORY tombstones (TXN-615).
   */
  private void requireCreatableResourceName(final ResourceConfiguration resourceConfig, final boolean hasUserStore) {
    if (hasUserStore && DerivedIndexMaintainer.isDerivedResourceName(canonicalResourceName(resourceConfig.getName()))) {
      throw new SirixUsageException("Derived resource names are reserved");
    }
    if (InMemoryResources.isRetiredResourceName(canonicalResourceName(resourceConfig.getName()))) {
      throw new SirixUsageException("Resource names containing " + InMemoryResources.RETIRED_MARKER + " are reserved");
    }
  }

  /**
   * Create the resource directory and its folder structure.
   *
   * @return {@code false} as soon as any part cannot be created; the caller removes what was made
   */
  private boolean createResourceStructure(final ResourceConfiguration resourceConfig, final Path path) {
    try {
      Files.createDirectory(path);
    } catch (UnsupportedOperationException | IOException | SecurityException e) {
      return false;
    }
    for (final ResourceConfiguration.ResourcePaths resourcePath : ResourceConfiguration.ResourcePaths.values()) {
      final Path toCreate = path.resolve(resourcePath.getPath());
      try {
        if (resourcePath.isFolder()) {
          Files.createDirectory(toCreate);
          if (resourcePath == ResourceConfiguration.ResourcePaths.ENCRYPTION_KEY) {
            createAndStoreKeysetIfNeeded(resourceConfig, toCreate);
          }
        } else {
          Files.createFile(toCreate);
        }
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        return false;
      }
    }
    return true;
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
        : beginUserResourceSession(resourceName, userStore).session();
        final NodeTrx wtx = resourceTrxManager.beginNodeTrx(AfterCommitState.CLOSE)) {
      final var useCustomCommitTimestamps = resConfig.customCommitTimestamps();
      if (useCustomCommitTimestamps) {
        wtx.commit(null, Instant.EPOCH);
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
  public Database<T> removeResource(final String name) {
    notifications.assertNotInCallback();
    final String canonicalName = canonicalResourceName(name);
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), canonicalName, true);
    assertNotClosed();
    assertNoOpenResourceSessions(
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName));
    try (var gate = MutationGate.acquireResource(dbConfig.getDatabaseFile(), canonicalName);
        var retirement = notifications.retire()) {
      return removeResourceInternal(canonicalName);
    }
  }

  private void assertNoOpenResourceSessions(final Path resourceFile) {
    if (resourceSessions.containsAnyEntry(resourceFile)) {
      throw new IllegalStateException("Open resource sessions found, must be closed first: " + resourceSessions);
    }
  }

  private synchronized Database<T> removeResourceInternal(final String name) {
    ResourceAccessScope.checkResource(dbConfig.getDatabaseFile(), canonicalResourceName(name), true);
    assertAcceptingResources();
    final String canonicalName = canonicalResourceName(name);
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName);

    // Check that no running resource sessions are opened.
    assertNoOpenResourceSessions(resourceFile);

    if (derivedMaintainer != null) {
      derivedMaintainer.remove(resourceFile);
    }
    final T shared = resourceStore.getOpenResourceSession(resourceFile);
    if (shared != null) {
      shared.close();
    }

    if (hiddenInMemoryLeftovers.contains(canonicalName) || knownInMemoryResources.containsKey(resourceFile)
        || hasKnownInMemorySource(resourceFile)
        || (Files.isDirectory(resourceFile) && InMemoryResources.isInMemoryOwned(resourceFile))) {
      // TXN-615 clause 8: an IN_MEMORY resource's state and footprint go the retirement way.
      retireInMemoryResource(resourceFile, canonicalName);
      return this;
    } else if (Files.exists(resourceFile) && (DerivedIndexMaintainer.isDerivedResourceName(canonicalName)
        || ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0)) {
      // If file is existing and folder is a Sirix-dataplace, delete it.
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
      AbstractResourceSession.forgetCommitRecovery(resourceFile);

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
      removeResourceInternal(derivedName);
    }
    return this;
  }

  /** Drop what this process caches for a resource's content, before its files go. */
  private void discardResourceState(final Path resourceFile) {
    // CRITICAL FIX: Clear BufferManager caches for this resource BEFORE deletion
    // This prevents cache pollution when resource is removed and recreated with same IDs
    try {
      final ResourceConfiguration known = knownInMemoryResources.get(resourceFile);
      final ResourceConfiguration resourceConfig = known != null
          ? known
          : ResourceConfiguration.deserialize(resourceFile);
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
  }

  /** Drop process-wide resource caches and registrations. */
  private void forgetResource(final Path resourceFile, final String canonicalName) {
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

  /**
   * Discard the committed state and on-disk footprint of one resource proven to be IN_MEMORY
   * (TXN-615). The state is dropped even when the directory cannot be removed; such a directory stays
   * hidden from this instance. Retirement renames it to {@code $retired-<uuid>}, independently of the
   * original name length.
   *
   * @return the original directory or tombstone that remains, or {@code null} if neither remains
   */
  private @Nullable Path retireInMemoryResource(final Path resourceFile, final String name) {
    rememberCleanup(resourceFile);
    if (!DerivedIndexMaintainer.isDerivedResourceName(name)) {
      final String derivedName = name + DerivedIndexMaintainer.SUFFIX;
      final Path derived = resourceFile.resolveSibling(derivedName);
      if (!resourceDoesNotExist(derived)) {
        removeResourceInternal(derivedName);
        if (!resourceDoesNotExist(derived)) {
          discardInMemoryResourceState(resourceFile);
          hiddenInMemoryLeftovers.add(name);
          return resourceFile;
        }
      }
    }
    discardInMemoryResourceState(resourceFile);
    // Rename first: either the whole directory leaves the name at once, or nothing changed and the
    // settings that prove it in-memory state are still there for the next attempt. A plain
    // recursive delete could empty the directory and then fail to remove it, leaving an unprovable
    // leftover that TXN-615 clause 6 would have to treat as persistent.
    final Path remaining = retireDirectory(resourceFile);
    if (!resourceFile.equals(remaining)) {
      hiddenInMemoryLeftovers.remove(name);
    } else {
      hiddenInMemoryLeftovers.add(name);
    }
    return remaining;
  }

  /** Confirm absence even when an overlong derived name makes an attribute lookup inconclusive. */
  private static boolean resourceDoesNotExist(final Path resourceFile) {
    if (Files.notExists(resourceFile)) {
      return true;
    }
    try (final Stream<Path> siblings = Files.list(resourceFile.getParent())) {
      return siblings.noneMatch(resourceFile::equals);
    } catch (final IOException | UncheckedIOException | SecurityException failure) {
      // An inaccessible parent supplies no proof of absence: retain the source for a retry.
      return false;
    }
  }

  private @Nullable Path retireDirectory(final Path resourceFile) {
    if (Files.notExists(resourceFile)) {
      pendingCleanup.remove(resourceFile);
      return null;
    }
    final Path tombstone = resourceFile.resolveSibling(InMemoryResources.RETIRED_MARKER + UUID.randomUUID());
    try {
      Files.move(resourceFile, tombstone, StandardCopyOption.ATOMIC_MOVE);
    } catch (final IOException | UnsupportedOperationException | SecurityException renameFailure) {
      logger.warn("Could not remove the leftovers of in-memory resource {}", resourceFile, renameFailure);
      return resourceFile;
    }
    pendingCleanup.remove(resourceFile);
    return removeTombstone(tombstone)
        ? null
        : tombstone;
  }

  private void discardInMemoryResourceState(final Path resourceFile) {
    try {
      discardResourceState(resourceFile);
    } finally {
      RAMStorage.discardUnder(resourceFile);
      forgetResource(resourceFile, resourceFile.getFileName().toString());
      knownInMemoryResources.remove(resourceFile);
    }
  }

  private void rememberCleanup(final Path path) {
    if (pendingCleanup.containsKey(path)) {
      return;
    }
    final String name = path.getFileName().toString();
    if (InMemoryResources.isRetiredResourceName(name)) {
      pendingCleanup.put(path, new CleanupProof(null, null));
      return;
    }
    final Path source = DerivedIndexMaintainer.isDerivedResourceName(name)
        ? path.resolveSibling(name.substring(0, name.length() - DerivedIndexMaintainer.SUFFIX.length()))
        : path;
    final ResourceConfiguration known = knownInMemoryResources.get(source);
    final ResourceConfiguration config = known != null
        ? known
        : ResourceConfiguration.deserialize(source);
    pendingCleanup.put(path, new CleanupProof(source, config.resourceUuid));
  }

  synchronized boolean hasPendingCleanup() {
    return !pendingCleanup.isEmpty();
  }

  synchronized void retryPendingCleanup(final boolean retireOriginals) {
    final List<Path> paths = List.copyOf(pendingCleanup.keySet());
    for (int pass = 0; pass < 2; pass++) {
      for (final Path path : paths) {
        final CleanupProof proof = pendingCleanup.get(path);
        if (proof == null) {
          continue;
        }
        final String name = path.getFileName().toString();
        if (proof.source() == null) {
          if (pass == 0) {
            removeTombstone(path);
          }
          continue;
        }
        if (DerivedIndexMaintainer.isDerivedResourceName(name) != (pass == 0)) {
          continue;
        }
        if (Files.notExists(path)) {
          pendingCleanup.remove(path);
          continue;
        }
        try {
          if (!Objects.equals(proof.incarnation(), ResourceConfiguration.deserialize(proof.source()).resourceUuid)) {
            pendingCleanup.remove(path);
          } else if (retireOriginals && (pass == 0
              || !pendingCleanup.containsKey(path.resolveSibling(name + DerivedIndexMaintainer.SUFFIX)))) {
            retireDirectory(path);
          }
        } catch (final RuntimeException failure) {
          logger.warn("Could not retry retired resource directory {}", path, failure);
        }
      }
    }
    if (!pendingCleanup.isEmpty()) {
      throw new SirixIOException("Could not remove the leftovers of in-memory resources " + pendingCleanup.keySet());
    }
  }

  /**
   * Delete a retired resource directory. Its reserved name keeps it out of every listing, so a
   * failure only leaves garbage for the next cleanup.
   *
   * @return whether the tombstone is gone
   */
  private boolean removeTombstone(final Path tombstone) {
    rememberCleanup(tombstone);
    try {
      SirixFiles.recursiveRemove(tombstone);
    } catch (final RuntimeException removalFailure) {
      logger.warn("Could not delete retired resource directory {}", tombstone, removalFailure);
    }
    if (Files.notExists(tombstone)) {
      pendingCleanup.remove(tombstone);
      return true;
    }
    return false;
  }


  /**
   * Discard every IN_MEMORY resource of this database, with its derived state, and remove their
   * on-disk footprint (TXN-615 clauses 3, 6 and 8). Derived resources go first: their source's
   * settings are the proof that they are in-memory state, so a source stays on disk (hidden) while
   * its derived resource could not be removed.
   *
   * @return a failure naming every directory that could not be removed, or {@code null}
   */
  private synchronized @Nullable SirixIOException retireInMemoryResources() {
    final Path resources = dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    if (Files.notExists(resources)) {
      retirementScanPending = false;
      return null;
    }
    final List<Path> derived = new ArrayList<>();
    final List<Path> sources = new ArrayList<>();
    final List<Path> remaining = new ArrayList<>();
    try {
      collectInMemoryResources(resources, derived, sources, remaining);
    } catch (final IOException | UncheckedIOException | SecurityException e) {
      retirementScanPending = true;
      return new SirixIOException("Could not list the resources of " + dbConfig.getDatabaseFile(), e);
    }
    final Set<String> keptSources = retireDerivedResources(derived, remaining);
    retireSourceResources(sources, keptSources, remaining);
    retirementScanPending = false;
    return remaining.isEmpty()
        ? null
        : new SirixIOException("Could not remove the leftovers of in-memory resources " + remaining);
  }

  /**
   * Sort the entries of the resources folder: retirement tombstones are deleted at once (adding any
   * that remain to {@code remaining}); provably IN_MEMORY resources are remembered for cleanup and
   * collected as derived resources or sources.
   */
  private void collectInMemoryResources(final Path resources, final List<Path> derived, final List<Path> sources,
      final List<Path> remaining) throws IOException {
    try (final Stream<Path> stream = Files.list(resources)) {
      for (final Path resource : stream.toList()) {
        if (InMemoryResources.isRetiredResourceName(resource.getFileName().toString())) {
          if (!removeTombstone(resource)) {
            remaining.add(resource);
          }
        } else if (Files.isDirectory(resource) && InMemoryResources.isInMemoryOwned(resource)) {
          rememberCleanup(resource);
          if (DerivedIndexMaintainer.isDerivedResourceName(resource.getFileName().toString())) {
            derived.add(resource);
          } else {
            sources.add(resource);
          }
        }
      }
    }
  }

  /**
   * Retire the derived resources first, adding what remains to {@code remaining}.
   *
   * @return the names of the sources whose settings must stay on disk as proof for a derived resource
   *         that could not leave its name
   */
  private Set<String> retireDerivedResources(final List<Path> derived, final List<Path> remaining) {
    final Set<String> keptSources = new HashSet<>();
    for (final Path resource : derived) {
      final String name = resource.getFileName().toString();
      final Path leftover = retireInMemoryResource(resource, name);
      if (leftover != null) {
        remaining.add(leftover);
        if (leftover.equals(resource)) {
          keptSources.add(name.substring(0, name.length() - DerivedIndexMaintainer.SUFFIX.length()));
        }
      }
    }
    return keptSources;
  }

  /**
   * Retire the sources, keeping a kept source's settings as proof while still discarding its state.
   */
  private void retireSourceResources(final List<Path> sources, final Set<String> keptSources,
      final List<Path> remaining) {
    for (final Path resource : sources) {
      final String name = resource.getFileName().toString();
      if (keptSources.contains(name)) {
        // Keep the proof on disk for the derived leftover; the state itself goes now.
        discardInMemoryResourceState(resource);
        hiddenInMemoryLeftovers.add(name);
        remaining.add(resource);
      } else {
        final Path leftover = retireInMemoryResource(resource, name);
        if (leftover != null) {
          remaining.add(leftover);
        }
      }
    }
  }

  /**
   * Remove what IN_MEMORY resources left behind when an earlier instance or process ended without
   * removing it (TXN-615 clause 6). Opening the database is not an error: a directory that cannot be
   * removed is treated as absent, and creating its name retries the removal.
   */
  void retireInMemoryLeftovers() {
    final SirixIOException failure = retireInMemoryResources();
    if (failure != null) {
      logger.warn("In-memory resource leftovers stay hidden in {}", dbConfig.getDatabaseFile(), failure);
    }
  }

  private void ensureInMemoryLeftoversRetired() {
    if (retirementScanPending) {
      synchronized (this) {
        if (retirementScanPending) {
          final SirixIOException failure = retireInMemoryResources();
          if (retirementScanPending) {
            throw requireNonNull(failure);
          }
        }
      }
    }
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

  private void assertAcceptingResources() {
    assertNotClosed();
    if (closing) {
      throw new IllegalStateException("Database resource admission is closed for teardown");
    }
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
    ensureInMemoryLeftoversRetired();
    final String canonicalName = canonicalResourceName(resourceName);
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(canonicalName);
    return !hiddenInMemoryLeftovers.contains(canonicalName) && !InMemoryResources.isRetiredResourceName(canonicalName)
        && Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    ensureInMemoryLeftoversRetired();
    try (final Stream<Path> stream =
        Files.list(dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.filter(path -> {
        final String name = canonicalResourceName(path.getFileName().toString());
        return !DerivedIndexMaintainer.isDerivedResourceName(name) && !InMemoryResources.isRetiredResourceName(name)
            && !hiddenInMemoryLeftovers.contains(name);
      }).toList();
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
  public void close() {
    assertCanClose();
    try (DatabaseNotifications.Retirement ignored = notifications.retire()) {
      synchronized (this) {
        closing = true;
      }
      closeQuiescent();
    }
  }

  void assertCanClose() {
    notifications.assertNotInCallback();
    resourceStore.assertCanClose();
  }

  DatabaseNotifications getNotifications() {
    return notifications;
  }

  private void closeQuiescent() {
    if (isClosed) {
      retryPendingCleanup(true);
      return;
    }

    logger.trace("Close local database instance.");
    if (derivedMaintainer != null) {
      derivedMaintainer.close();
    }
    // A failing teardown throws here and discards nothing: the instance stays open, owned and
    // complete, and a retry finishes the work (TXN-101, TXN-503, TXN-615 clause 7).
    final List<ResourceSession<?, ?>> users = new ArrayList<>();
    final Path data = dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    synchronized (this) {
      for (final var entry : resourceSessions.asMap().entrySet()) {
        if (data.equals(entry.getKey().getParent())) {
          users.addAll(entry.getValue());
        }
      }
    }
    Throwable failure = null;
    for (final ResourceSession<?, ?> session : users) {
      try {
        session.close();
      } catch (final RuntimeException | Error next) {
        failure = ShardedPageCache.retainCleanupFailure(failure, next);
      }
    }
    ShardedPageCache.rethrowCleanupFailure(failure);
    try {
      resourceStore.close();
      transactionManager.close();
    } catch (final RuntimeException | Error teardownFailure) {
      // User sessions have drained; a backend failure leaves the committed instance usable.
      // Earlier user-session refusal keeps admission closed until those live owners finish.
      synchronized (this) {
        closing = false;
      }
      throw teardownFailure;
    }
    notifications.close();

    // TXN-615 clauses 3 and 7: no session is live any more. Discard every IN_MEMORY resource and its
    // on-disk footprint; the instance ends even if removing leftovers fails.
    SirixIOException retirementFailure = null;
    try {
      for (final Path path : knownInMemoryResources.keySet()) {
        if (!Files.notExists(path)) {
          rememberCleanup(path);
        }
      }
      retirementFailure = retireInMemoryResources();
    } finally {
      try {
        for (final Path path : List.copyOf(knownInMemoryResources.keySet())) {
          discardInMemoryResourceState(path);
        }
        RAMStorage.discardUnder(dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()));
      } finally {
        isClosed = true;
        Databases.freeAllocatedMemory();
      }
    }
    if (retirementFailure != null) {
      throw retirementFailure;
    }
    pendingCleanup.keySet().removeIf(Files::notExists);
    if (!pendingCleanup.isEmpty()) {
      throw new SirixIOException("Could not remove the leftovers of in-memory resources " + pendingCleanup.keySet());
    }
  }



  @Override
  public String toString() {
    return ToStringHelper.of(this).add("dbConfig", dbConfig).toString();
  }
}

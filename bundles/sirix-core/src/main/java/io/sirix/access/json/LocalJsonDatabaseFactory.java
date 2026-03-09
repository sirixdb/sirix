package io.sirix.access.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.LocalDatabase;
import io.sirix.access.LocalDatabaseFactory;
import io.sirix.access.PathBasedPool;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceSessionFactory;
import io.sirix.access.ResourceStore;
import io.sirix.access.ResourceStoreImpl;
import io.sirix.access.User;
import io.sirix.access.WriteLocksRegistry;
import io.sirix.access.trx.TransactionManagerImpl;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A database session factory for JSON databases. Replaces the Dagger-managed
 * {@code JsonLocalDatabaseComponent} and {@code JsonLocalDatabaseModule} with direct
 * constructor-based wiring.
 *
 * @author Joao Sousa
 */
public final class LocalJsonDatabaseFactory implements LocalDatabaseFactory<JsonResourceSession> {

  private static final Logger logger = LoggerFactory.getLogger(LocalJsonDatabaseFactory.class);

  private final WriteLocksRegistry writeLocksRegistry;
  private final PathBasedPool<Database<?>> databaseSessions;
  private final PathBasedPool<ResourceSession<?, ?>> resourceSessions;

  public LocalJsonDatabaseFactory(final WriteLocksRegistry writeLocksRegistry,
      final PathBasedPool<Database<?>> databaseSessions,
      final PathBasedPool<ResourceSession<?, ?>> resourceSessions) {
    this.writeLocksRegistry = writeLocksRegistry;
    this.databaseSessions = databaseSessions;
    this.resourceSessions = resourceSessions;
  }

  @Override
  public Database<JsonResourceSession> createDatabase(final DatabaseConfiguration configuration, final User user) {
    logger.trace("Creating new local JSON database instance (open)");

    // Database-scoped dependencies (previously in LocalDatabaseModule + JsonLocalDatabaseModule)
    final String databaseName = configuration.getDatabaseName();
    final DatabaseType databaseType = configuration.getDatabaseType();
    final TransactionManager transactionManager = new TransactionManagerImpl();

    // Use AtomicReference to break the circular dependency:
    // ResourceStoreImpl needs ResourceSessionFactory, and the factory lambda needs ResourceStoreImpl.
    final AtomicReference<ResourceStore<JsonResourceSession>> resourceStoreRef = new AtomicReference<>();

    final ResourceSessionFactory<JsonResourceSession> resourceSessionFactory =
        (final ResourceConfiguration resourceConfig, final BufferManager bufferManager,
            final Path resourceFile) -> {

          // Resource-session-scoped dependencies (previously in ResourceSessionModule)
          final IOStorage storage = StorageType.getStorage(resourceConfig);
          final Semaphore writeLock = writeLocksRegistry.getWriteLock(resourceConfig.getResource());
          final UberPage uberPage = loadUberPage(storage);
          final StorageEngineWriterFactory storageEngineWriterFactory = new StorageEngineWriterFactory(databaseType);

          return new JsonResourceSessionImpl(resourceStoreRef.get(), resourceConfig, bufferManager,
              storage, uberPage, writeLock, user, databaseName, storageEngineWriterFactory);
        };

    final ResourceStore<JsonResourceSession> resourceStore =
        new ResourceStoreImpl<>(resourceSessions, resourceSessionFactory);
    resourceStoreRef.set(resourceStore);

    return new LocalDatabase<>(transactionManager, configuration, databaseSessions, resourceStore,
        writeLocksRegistry, resourceSessions);
  }

  /**
   * Loads the {@link UberPage} from storage, or bootstraps a new one if the storage does not exist
   * yet. This replaces {@code ResourceSessionModule.rootPage()}.
   */
  private static UberPage loadUberPage(final IOStorage storage) {
    if (storage.exists()) {
      try (final Reader reader = storage.createReader()) {
        final PageReference firstRef = reader.readUberPageReference();
        if (firstRef.getPage() == null) {
          return (UberPage) reader.read(firstRef, null);
        } else {
          return (UberPage) firstRef.getPage();
        }
      }
    } else {
      return new UberPage();
    }
  }
}

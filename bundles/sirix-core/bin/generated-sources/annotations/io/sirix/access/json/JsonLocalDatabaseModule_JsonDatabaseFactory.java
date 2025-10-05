package io.sirix.access.json;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.PathBasedPool;
import io.sirix.access.ResourceStore;
import io.sirix.access.WriteLocksRegistry;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class JsonLocalDatabaseModule_JsonDatabaseFactory implements Factory<Database<JsonResourceSession>> {
  private final Provider<TransactionManager> transactionManagerProvider;

  private final Provider<DatabaseConfiguration> dbConfigProvider;

  private final Provider<PathBasedPool<Database<?>>> sessionsProvider;

  private final Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider;

  private final Provider<WriteLocksRegistry> writeLocksProvider;

  private final Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider;

  public JsonLocalDatabaseModule_JsonDatabaseFactory(
      Provider<TransactionManager> transactionManagerProvider,
      Provider<DatabaseConfiguration> dbConfigProvider,
      Provider<PathBasedPool<Database<?>>> sessionsProvider,
      Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider,
      Provider<WriteLocksRegistry> writeLocksProvider,
      Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider) {
    this.transactionManagerProvider = transactionManagerProvider;
    this.dbConfigProvider = dbConfigProvider;
    this.sessionsProvider = sessionsProvider;
    this.resourceStoreProvider = resourceStoreProvider;
    this.writeLocksProvider = writeLocksProvider;
    this.resourceManagersProvider = resourceManagersProvider;
  }

  @Override
  public Database<JsonResourceSession> get() {
    return jsonDatabase(transactionManagerProvider.get(), dbConfigProvider.get(), sessionsProvider.get(), resourceStoreProvider.get(), writeLocksProvider.get(), resourceManagersProvider.get());
  }

  public static JsonLocalDatabaseModule_JsonDatabaseFactory create(
      Provider<TransactionManager> transactionManagerProvider,
      Provider<DatabaseConfiguration> dbConfigProvider,
      Provider<PathBasedPool<Database<?>>> sessionsProvider,
      Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider,
      Provider<WriteLocksRegistry> writeLocksProvider,
      Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider) {
    return new JsonLocalDatabaseModule_JsonDatabaseFactory(transactionManagerProvider, dbConfigProvider, sessionsProvider, resourceStoreProvider, writeLocksProvider, resourceManagersProvider);
  }

  public static Database<JsonResourceSession> jsonDatabase(TransactionManager transactionManager,
      DatabaseConfiguration dbConfig, PathBasedPool<Database<?>> sessions,
      ResourceStore<JsonResourceSession> resourceStore, WriteLocksRegistry writeLocks,
      PathBasedPool<ResourceSession<?, ?>> resourceManagers) {
    return Preconditions.checkNotNullFromProvides(JsonLocalDatabaseModule.jsonDatabase(transactionManager, dbConfig, sessions, resourceStore, writeLocks, resourceManagers));
  }
}

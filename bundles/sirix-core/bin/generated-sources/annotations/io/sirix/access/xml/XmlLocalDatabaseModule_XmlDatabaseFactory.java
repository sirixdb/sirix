package io.sirix.access.xml;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.PathBasedPool;
import io.sirix.access.ResourceStore;
import io.sirix.access.WriteLocksRegistry;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.xml.XmlResourceSession;
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
public final class XmlLocalDatabaseModule_XmlDatabaseFactory implements Factory<Database<XmlResourceSession>> {
  private final Provider<TransactionManager> transactionManagerProvider;

  private final Provider<DatabaseConfiguration> dbConfigProvider;

  private final Provider<PathBasedPool<Database<?>>> sessionsProvider;

  private final Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider;

  private final Provider<WriteLocksRegistry> writeLocksProvider;

  private final Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider;

  public XmlLocalDatabaseModule_XmlDatabaseFactory(
      Provider<TransactionManager> transactionManagerProvider,
      Provider<DatabaseConfiguration> dbConfigProvider,
      Provider<PathBasedPool<Database<?>>> sessionsProvider,
      Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider,
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
  public Database<XmlResourceSession> get() {
    return xmlDatabase(transactionManagerProvider.get(), dbConfigProvider.get(), sessionsProvider.get(), resourceStoreProvider.get(), writeLocksProvider.get(), resourceManagersProvider.get());
  }

  public static XmlLocalDatabaseModule_XmlDatabaseFactory create(
      Provider<TransactionManager> transactionManagerProvider,
      Provider<DatabaseConfiguration> dbConfigProvider,
      Provider<PathBasedPool<Database<?>>> sessionsProvider,
      Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider,
      Provider<WriteLocksRegistry> writeLocksProvider,
      Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider) {
    return new XmlLocalDatabaseModule_XmlDatabaseFactory(transactionManagerProvider, dbConfigProvider, sessionsProvider, resourceStoreProvider, writeLocksProvider, resourceManagersProvider);
  }

  public static Database<XmlResourceSession> xmlDatabase(TransactionManager transactionManager,
      DatabaseConfiguration dbConfig, PathBasedPool<Database<?>> sessions,
      ResourceStore<XmlResourceSession> resourceStore, WriteLocksRegistry writeLocks,
      PathBasedPool<ResourceSession<?, ?>> resourceManagers) {
    return Preconditions.checkNotNullFromProvides(XmlLocalDatabaseModule.xmlDatabase(transactionManager, dbConfig, sessions, resourceStore, writeLocks, resourceManagers));
  }
}

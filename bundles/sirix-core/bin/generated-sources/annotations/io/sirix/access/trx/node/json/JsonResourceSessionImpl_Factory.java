package io.sirix.access.trx.node.json;

import dagger.internal.Factory;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.User;
import io.sirix.access.trx.page.PageTrxFactory;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.io.IOStorage;
import io.sirix.page.UberPage;
import java.util.concurrent.Semaphore;
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
public final class JsonResourceSessionImpl_Factory implements Factory<JsonResourceSessionImpl> {
  private final Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider;

  private final Provider<ResourceConfiguration> resourceConfProvider;

  private final Provider<BufferManager> bufferManagerProvider;

  private final Provider<IOStorage> storageProvider;

  private final Provider<UberPage> uberPageProvider;

  private final Provider<Semaphore> writeLockProvider;

  private final Provider<User> userProvider;

  private final Provider<String> databaseNameProvider;

  private final Provider<PageTrxFactory> pageTrxFactoryProvider;

  public JsonResourceSessionImpl_Factory(
      Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider,
      Provider<ResourceConfiguration> resourceConfProvider,
      Provider<BufferManager> bufferManagerProvider, Provider<IOStorage> storageProvider,
      Provider<UberPage> uberPageProvider, Provider<Semaphore> writeLockProvider,
      Provider<User> userProvider, Provider<String> databaseNameProvider,
      Provider<PageTrxFactory> pageTrxFactoryProvider) {
    this.resourceStoreProvider = resourceStoreProvider;
    this.resourceConfProvider = resourceConfProvider;
    this.bufferManagerProvider = bufferManagerProvider;
    this.storageProvider = storageProvider;
    this.uberPageProvider = uberPageProvider;
    this.writeLockProvider = writeLockProvider;
    this.userProvider = userProvider;
    this.databaseNameProvider = databaseNameProvider;
    this.pageTrxFactoryProvider = pageTrxFactoryProvider;
  }

  @Override
  public JsonResourceSessionImpl get() {
    return newInstance(resourceStoreProvider.get(), resourceConfProvider.get(), bufferManagerProvider.get(), storageProvider.get(), uberPageProvider.get(), writeLockProvider.get(), userProvider.get(), databaseNameProvider.get(), pageTrxFactoryProvider.get());
  }

  public static JsonResourceSessionImpl_Factory create(
      Provider<ResourceStore<JsonResourceSession>> resourceStoreProvider,
      Provider<ResourceConfiguration> resourceConfProvider,
      Provider<BufferManager> bufferManagerProvider, Provider<IOStorage> storageProvider,
      Provider<UberPage> uberPageProvider, Provider<Semaphore> writeLockProvider,
      Provider<User> userProvider, Provider<String> databaseNameProvider,
      Provider<PageTrxFactory> pageTrxFactoryProvider) {
    return new JsonResourceSessionImpl_Factory(resourceStoreProvider, resourceConfProvider, bufferManagerProvider, storageProvider, uberPageProvider, writeLockProvider, userProvider, databaseNameProvider, pageTrxFactoryProvider);
  }

  public static JsonResourceSessionImpl newInstance(
      ResourceStore<JsonResourceSession> resourceStore, ResourceConfiguration resourceConf,
      BufferManager bufferManager, IOStorage storage, UberPage uberPage, Semaphore writeLock,
      User user, String databaseName, PageTrxFactory pageTrxFactory) {
    return new JsonResourceSessionImpl(resourceStore, resourceConf, bufferManager, storage, uberPage, writeLock, user, databaseName, pageTrxFactory);
  }
}

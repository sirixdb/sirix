package io.sirix.access.trx.node.xml;

import dagger.internal.Factory;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.User;
import io.sirix.access.trx.page.PageTrxFactory;
import io.sirix.api.xml.XmlResourceSession;
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
public final class XmlResourceSessionImpl_Factory implements Factory<XmlResourceSessionImpl> {
  private final Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider;

  private final Provider<ResourceConfiguration> resourceConfProvider;

  private final Provider<BufferManager> bufferManagerProvider;

  private final Provider<IOStorage> storageProvider;

  private final Provider<UberPage> uberPageProvider;

  private final Provider<Semaphore> writeLockProvider;

  private final Provider<User> userProvider;

  private final Provider<PageTrxFactory> pageTrxFactoryProvider;

  public XmlResourceSessionImpl_Factory(
      Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider,
      Provider<ResourceConfiguration> resourceConfProvider,
      Provider<BufferManager> bufferManagerProvider, Provider<IOStorage> storageProvider,
      Provider<UberPage> uberPageProvider, Provider<Semaphore> writeLockProvider,
      Provider<User> userProvider, Provider<PageTrxFactory> pageTrxFactoryProvider) {
    this.resourceStoreProvider = resourceStoreProvider;
    this.resourceConfProvider = resourceConfProvider;
    this.bufferManagerProvider = bufferManagerProvider;
    this.storageProvider = storageProvider;
    this.uberPageProvider = uberPageProvider;
    this.writeLockProvider = writeLockProvider;
    this.userProvider = userProvider;
    this.pageTrxFactoryProvider = pageTrxFactoryProvider;
  }

  @Override
  public XmlResourceSessionImpl get() {
    return newInstance(resourceStoreProvider.get(), resourceConfProvider.get(), bufferManagerProvider.get(), storageProvider.get(), uberPageProvider.get(), writeLockProvider.get(), userProvider.get(), pageTrxFactoryProvider.get());
  }

  public static XmlResourceSessionImpl_Factory create(
      Provider<ResourceStore<XmlResourceSession>> resourceStoreProvider,
      Provider<ResourceConfiguration> resourceConfProvider,
      Provider<BufferManager> bufferManagerProvider, Provider<IOStorage> storageProvider,
      Provider<UberPage> uberPageProvider, Provider<Semaphore> writeLockProvider,
      Provider<User> userProvider, Provider<PageTrxFactory> pageTrxFactoryProvider) {
    return new XmlResourceSessionImpl_Factory(resourceStoreProvider, resourceConfProvider, bufferManagerProvider, storageProvider, uberPageProvider, writeLockProvider, userProvider, pageTrxFactoryProvider);
  }

  public static XmlResourceSessionImpl newInstance(ResourceStore<XmlResourceSession> resourceStore,
      ResourceConfiguration resourceConf, BufferManager bufferManager, IOStorage storage,
      UberPage uberPage, Semaphore writeLock, User user, PageTrxFactory pageTrxFactory) {
    return new XmlResourceSessionImpl(resourceStore, resourceConf, bufferManager, storage, uberPage, writeLock, user, pageTrxFactory);
  }
}

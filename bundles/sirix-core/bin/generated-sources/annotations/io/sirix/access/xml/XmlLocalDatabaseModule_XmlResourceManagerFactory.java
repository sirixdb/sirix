package io.sirix.access.xml;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.PathBasedPool;
import io.sirix.access.ResourceSessionFactory;
import io.sirix.access.ResourceStore;
import io.sirix.api.ResourceSession;
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
public final class XmlLocalDatabaseModule_XmlResourceManagerFactory implements Factory<ResourceStore<XmlResourceSession>> {
  private final Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider;

  private final Provider<ResourceSessionFactory<XmlResourceSession>> resourceSessionFactoryProvider;

  public XmlLocalDatabaseModule_XmlResourceManagerFactory(
      Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider,
      Provider<ResourceSessionFactory<XmlResourceSession>> resourceSessionFactoryProvider) {
    this.allResourceManagersProvider = allResourceManagersProvider;
    this.resourceSessionFactoryProvider = resourceSessionFactoryProvider;
  }

  @Override
  public ResourceStore<XmlResourceSession> get() {
    return xmlResourceManager(allResourceManagersProvider.get(), resourceSessionFactoryProvider.get());
  }

  public static XmlLocalDatabaseModule_XmlResourceManagerFactory create(
      Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider,
      Provider<ResourceSessionFactory<XmlResourceSession>> resourceSessionFactoryProvider) {
    return new XmlLocalDatabaseModule_XmlResourceManagerFactory(allResourceManagersProvider, resourceSessionFactoryProvider);
  }

  public static ResourceStore<XmlResourceSession> xmlResourceManager(
      PathBasedPool<ResourceSession<?, ?>> allResourceManagers,
      ResourceSessionFactory<XmlResourceSession> resourceSessionFactory) {
    return Preconditions.checkNotNullFromProvides(XmlLocalDatabaseModule.xmlResourceManager(allResourceManagers, resourceSessionFactory));
  }
}

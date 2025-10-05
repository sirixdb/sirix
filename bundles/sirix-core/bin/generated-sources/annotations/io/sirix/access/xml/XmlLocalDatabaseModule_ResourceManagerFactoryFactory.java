package io.sirix.access.xml;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.ResourceSessionFactory;
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
public final class XmlLocalDatabaseModule_ResourceManagerFactoryFactory implements Factory<ResourceSessionFactory<XmlResourceSession>> {
  private final Provider<XmlResourceManagerComponent.Builder> subComponentBuilderProvider;

  public XmlLocalDatabaseModule_ResourceManagerFactoryFactory(
      Provider<XmlResourceManagerComponent.Builder> subComponentBuilderProvider) {
    this.subComponentBuilderProvider = subComponentBuilderProvider;
  }

  @Override
  public ResourceSessionFactory<XmlResourceSession> get() {
    return resourceManagerFactory(subComponentBuilderProvider);
  }

  public static XmlLocalDatabaseModule_ResourceManagerFactoryFactory create(
      Provider<XmlResourceManagerComponent.Builder> subComponentBuilderProvider) {
    return new XmlLocalDatabaseModule_ResourceManagerFactoryFactory(subComponentBuilderProvider);
  }

  public static ResourceSessionFactory<XmlResourceSession> resourceManagerFactory(
      Provider<XmlResourceManagerComponent.Builder> subComponentBuilder) {
    return Preconditions.checkNotNullFromProvides(XmlLocalDatabaseModule.resourceManagerFactory(subComponentBuilder));
  }
}

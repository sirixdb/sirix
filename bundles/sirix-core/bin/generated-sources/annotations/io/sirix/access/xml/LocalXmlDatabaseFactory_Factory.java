package io.sirix.access.xml;

import dagger.internal.Factory;
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
public final class LocalXmlDatabaseFactory_Factory implements Factory<LocalXmlDatabaseFactory> {
  private final Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilderProvider;

  public LocalXmlDatabaseFactory_Factory(
      Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilderProvider) {
    this.subcomponentBuilderProvider = subcomponentBuilderProvider;
  }

  @Override
  public LocalXmlDatabaseFactory get() {
    return newInstance(subcomponentBuilderProvider);
  }

  public static LocalXmlDatabaseFactory_Factory create(
      Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilderProvider) {
    return new LocalXmlDatabaseFactory_Factory(subcomponentBuilderProvider);
  }

  public static LocalXmlDatabaseFactory newInstance(
      Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilder) {
    return new LocalXmlDatabaseFactory(subcomponentBuilder);
  }
}

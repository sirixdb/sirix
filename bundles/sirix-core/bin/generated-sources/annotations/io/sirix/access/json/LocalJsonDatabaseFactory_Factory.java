package io.sirix.access.json;

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
public final class LocalJsonDatabaseFactory_Factory implements Factory<LocalJsonDatabaseFactory> {
  private final Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilderProvider;

  public LocalJsonDatabaseFactory_Factory(
      Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilderProvider) {
    this.subComponentBuilderProvider = subComponentBuilderProvider;
  }

  @Override
  public LocalJsonDatabaseFactory get() {
    return newInstance(subComponentBuilderProvider);
  }

  public static LocalJsonDatabaseFactory_Factory create(
      Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilderProvider) {
    return new LocalJsonDatabaseFactory_Factory(subComponentBuilderProvider);
  }

  public static LocalJsonDatabaseFactory newInstance(
      Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilder) {
    return new LocalJsonDatabaseFactory(subComponentBuilder);
  }
}

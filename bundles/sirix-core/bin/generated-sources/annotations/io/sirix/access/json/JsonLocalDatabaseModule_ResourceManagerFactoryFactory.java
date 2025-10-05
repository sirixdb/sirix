package io.sirix.access.json;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.ResourceSessionFactory;
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
public final class JsonLocalDatabaseModule_ResourceManagerFactoryFactory implements Factory<ResourceSessionFactory<JsonResourceSession>> {
  private final Provider<JsonResourceSessionComponent.Builder> subComponentBuilderProvider;

  public JsonLocalDatabaseModule_ResourceManagerFactoryFactory(
      Provider<JsonResourceSessionComponent.Builder> subComponentBuilderProvider) {
    this.subComponentBuilderProvider = subComponentBuilderProvider;
  }

  @Override
  public ResourceSessionFactory<JsonResourceSession> get() {
    return resourceManagerFactory(subComponentBuilderProvider);
  }

  public static JsonLocalDatabaseModule_ResourceManagerFactoryFactory create(
      Provider<JsonResourceSessionComponent.Builder> subComponentBuilderProvider) {
    return new JsonLocalDatabaseModule_ResourceManagerFactoryFactory(subComponentBuilderProvider);
  }

  public static ResourceSessionFactory<JsonResourceSession> resourceManagerFactory(
      Provider<JsonResourceSessionComponent.Builder> subComponentBuilder) {
    return Preconditions.checkNotNullFromProvides(JsonLocalDatabaseModule.resourceManagerFactory(subComponentBuilder));
  }
}

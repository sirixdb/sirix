package io.sirix.access.json;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.access.PathBasedPool;
import io.sirix.access.ResourceSessionFactory;
import io.sirix.access.ResourceStore;
import io.sirix.api.ResourceSession;
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
public final class JsonLocalDatabaseModule_JsonResourceManagerFactory implements Factory<ResourceStore<JsonResourceSession>> {
  private final Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider;

  private final Provider<ResourceSessionFactory<JsonResourceSession>> resourceSessionFactoryProvider;

  public JsonLocalDatabaseModule_JsonResourceManagerFactory(
      Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider,
      Provider<ResourceSessionFactory<JsonResourceSession>> resourceSessionFactoryProvider) {
    this.allResourceManagersProvider = allResourceManagersProvider;
    this.resourceSessionFactoryProvider = resourceSessionFactoryProvider;
  }

  @Override
  public ResourceStore<JsonResourceSession> get() {
    return jsonResourceManager(allResourceManagersProvider.get(), resourceSessionFactoryProvider.get());
  }

  public static JsonLocalDatabaseModule_JsonResourceManagerFactory create(
      Provider<PathBasedPool<ResourceSession<?, ?>>> allResourceManagersProvider,
      Provider<ResourceSessionFactory<JsonResourceSession>> resourceSessionFactoryProvider) {
    return new JsonLocalDatabaseModule_JsonResourceManagerFactory(allResourceManagersProvider, resourceSessionFactoryProvider);
  }

  public static ResourceStore<JsonResourceSession> jsonResourceManager(
      PathBasedPool<ResourceSession<?, ?>> allResourceManagers,
      ResourceSessionFactory<JsonResourceSession> resourceSessionFactory) {
    return Preconditions.checkNotNullFromProvides(JsonLocalDatabaseModule.jsonResourceManager(allResourceManagers, resourceSessionFactory));
  }
}

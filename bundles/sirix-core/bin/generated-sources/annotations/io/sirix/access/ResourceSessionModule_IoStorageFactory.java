package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.io.IOStorage;
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
public final class ResourceSessionModule_IoStorageFactory implements Factory<IOStorage> {
  private final Provider<ResourceConfiguration> resourceConfigurationProvider;

  public ResourceSessionModule_IoStorageFactory(
      Provider<ResourceConfiguration> resourceConfigurationProvider) {
    this.resourceConfigurationProvider = resourceConfigurationProvider;
  }

  @Override
  public IOStorage get() {
    return ioStorage(resourceConfigurationProvider.get());
  }

  public static ResourceSessionModule_IoStorageFactory create(
      Provider<ResourceConfiguration> resourceConfigurationProvider) {
    return new ResourceSessionModule_IoStorageFactory(resourceConfigurationProvider);
  }

  public static IOStorage ioStorage(ResourceConfiguration resourceConfiguration) {
    return Preconditions.checkNotNullFromProvides(ResourceSessionModule.ioStorage(resourceConfiguration));
  }
}

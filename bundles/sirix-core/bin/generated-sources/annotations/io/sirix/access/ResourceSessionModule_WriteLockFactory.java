package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
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
public final class ResourceSessionModule_WriteLockFactory implements Factory<Semaphore> {
  private final Provider<WriteLocksRegistry> registryProvider;

  private final Provider<ResourceConfiguration> resourceConfigurationProvider;

  public ResourceSessionModule_WriteLockFactory(Provider<WriteLocksRegistry> registryProvider,
      Provider<ResourceConfiguration> resourceConfigurationProvider) {
    this.registryProvider = registryProvider;
    this.resourceConfigurationProvider = resourceConfigurationProvider;
  }

  @Override
  public Semaphore get() {
    return writeLock(registryProvider.get(), resourceConfigurationProvider.get());
  }

  public static ResourceSessionModule_WriteLockFactory create(
      Provider<WriteLocksRegistry> registryProvider,
      Provider<ResourceConfiguration> resourceConfigurationProvider) {
    return new ResourceSessionModule_WriteLockFactory(registryProvider, resourceConfigurationProvider);
  }

  public static Semaphore writeLock(WriteLocksRegistry registry,
      ResourceConfiguration resourceConfiguration) {
    return Preconditions.checkNotNullFromProvides(ResourceSessionModule.writeLock(registry, resourceConfiguration));
  }
}

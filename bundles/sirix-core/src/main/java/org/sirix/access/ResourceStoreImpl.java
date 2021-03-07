package org.sirix.access;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResourceStoreImpl<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource managers.
   */
  private final Map<Path, R> resourceManagers;

  private final PathBasedPool<ResourceManager<?, ?>> allResourceManagers;

  private final ResourceManagerFactory<R> resourceManagerFactory;

  public ResourceStoreImpl(final PathBasedPool<ResourceManager<?, ?>> allResourceManagers,
                           final ResourceManagerFactory<R> resourceManagerFactory) {

    this.resourceManagers = new ConcurrentHashMap<>();
    this.allResourceManagers = allResourceManagers;
    this.resourceManagerFactory = resourceManagerFactory;
  }

  @Override
  public R openResource(final @Nonnull ResourceConfiguration resourceConfig,
                        final @Nonnull BufferManager bufferManager,
                        final @Nonnull Path resourceFile) {
    return this.resourceManagers.computeIfAbsent(resourceFile, k -> {

      final var resourceManager = this.resourceManagerFactory.create(resourceConfig, bufferManager, resourceFile);
      this.allResourceManagers.putObject(resourceFile, resourceManager);
      return resourceManager;
    });

  }

  @Override
  public boolean hasOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceManagers.containsKey(resourceFile);
  }

  @Override
  public R getOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceManagers.get(resourceFile);
  }

  @Override
  public void close() {
    resourceManagers.forEach((resourceName, resourceMgr) -> resourceMgr.close());
    resourceManagers.clear();
  }

  @Override
  public boolean closeResourceManager(final Path resourceFile) {
    final R manager = resourceManagers.remove(resourceFile);
    this.allResourceManagers.removeObject(resourceFile, manager);
    return manager != null;
  }
}

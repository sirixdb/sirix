package org.sirix.access;

import org.sirix.access.trx.node.AbstractResourceSession;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.cache.BufferManager;

import org.checkerframework.checker.nullness.qual.NonNull;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResourceStoreImpl<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource managers.
   */
  private final Map<Path, R> resourceSessions;

  private final PathBasedPool<ResourceSession<?, ?>> allResourceSessions;

  private final ResourceSessionFactory<R> resourceSessionFactory;

  public ResourceStoreImpl(final PathBasedPool<ResourceSession<?, ?>> allResourceSessions,
                           final ResourceSessionFactory<R> resourceSessionFactory) {

    this.resourceSessions = new ConcurrentHashMap<>();
    this.allResourceSessions = allResourceSessions;
    this.resourceSessionFactory = resourceSessionFactory;
  }

  @Override
  public R beginResourceSession(final @NonNull ResourceConfiguration resourceConfig,
                        final @NonNull BufferManager bufferManager,
                        final @NonNull Path resourceFile) {
    return this.resourceSessions.computeIfAbsent(resourceFile, k -> {
      final var resourceSession = this.resourceSessionFactory.create(resourceConfig, bufferManager, resourceFile);
      this.allResourceSessions.putObject(resourceFile, resourceSession);
      if (resourceSession.getMostRecentRevisionNumber() > 0) {
        ((AbstractResourceSession<?, ?>) resourceSession).createPageTrxPool();
      }
      return resourceSession;
    });
  }

  @Override
  public boolean hasOpenResourceSession(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceSessions.containsKey(resourceFile);
  }

  @Override
  public R getOpenResourceSession(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceSessions.get(resourceFile);
  }

  @Override
  public void close() {
    resourceSessions.forEach((resourceName, resourceMgr) -> resourceMgr.close());
    resourceSessions.clear();
  }

  @Override
  public boolean closeResourceSession(final Path resourceFile) {
    final R session = resourceSessions.remove(resourceFile);
    this.allResourceSessions.removeObject(resourceFile, session);
    return session != null;
  }
}

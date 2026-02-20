package io.sirix.access;

import static java.util.Objects.requireNonNull;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import org.checkerframework.checker.nullness.qual.NonNull;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceStoreImpl<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource sessions.
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
      final @NonNull BufferManager bufferManager, final @NonNull Path resourceFile) {
    return this.resourceSessions.computeIfAbsent(resourceFile, k -> {
      final var resourceSession = this.resourceSessionFactory.create(resourceConfig, bufferManager, resourceFile);
      this.allResourceSessions.putObject(resourceFile, resourceSession);
      if (resourceSession.getMostRecentRevisionNumber() > 0) {
        ((AbstractResourceSession<?, ?>) resourceSession).createStorageEnginePool();
      }
      return resourceSession;
    });
  }

  @Override
  public boolean hasOpenResourceSession(final Path resourceFile) {
    requireNonNull(resourceFile);
    return resourceSessions.containsKey(resourceFile);
  }

  @Override
  public R getOpenResourceSession(final Path resourceFile) {
    requireNonNull(resourceFile);
    return resourceSessions.get(resourceFile);
  }

  @Override
  public void close() {
    resourceSessions.forEach((_, resourceSession) -> resourceSession.close());
    resourceSessions.clear();
  }

  @Override
  public boolean closeResourceSession(final Path resourceFile) {
    final R session = resourceSessions.remove(resourceFile);
    this.allResourceSessions.removeObject(resourceFile, session);
    return session != null;
  }
}

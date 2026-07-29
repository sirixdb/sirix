package io.sirix.access;

import static java.util.Objects.requireNonNull;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

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
  public R beginResourceSession(final ResourceConfiguration resourceConfig,
      final BufferManager bufferManager, final Path resourceFile) {
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

  /**
   * Close every open resource session.
   *
   * <p>A session that fails to close must not take the others down with it. The straightforward
   * loop propagated the first exception, so the sessions after it stayed open and stayed
   * registered in {@code allResourceSessions} — and since the owning database marks itself closed
   * before calling this, nothing ever came back to finish the job. The first failure is still
   * reported once every session has been given its turn.
   */
  @Override
  public void close() {
    RuntimeException failure = null;
    for (final Map.Entry<Path, R> entry : resourceSessions.entrySet()) {
      try {
        entry.getValue().close();
      } catch (final RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      } finally {
        allResourceSessions.removeObject(entry.getKey(), entry.getValue());
      }
    }
    resourceSessions.clear();
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public boolean closeResourceSession(final Path resourceFile) {
    final R session = resourceSessions.remove(resourceFile);
    this.allResourceSessions.removeObject(resourceFile, session);
    return session != null;
  }
}

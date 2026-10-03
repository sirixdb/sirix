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
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

public class ResourceStoreImpl<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource sessions.
   */
  private final Map<Path, R> resourceSessions;

  private final PathBasedPool<ResourceSession<?, ?>> allResourceSessions;

  private final ResourceSessionFactory<R> resourceSessionFactory;

  private final @Nullable Consumer<R> sessionClosed;

  private final Object lifecycleMonitor;

  public ResourceStoreImpl(final PathBasedPool<ResourceSession<?, ?>> allResourceSessions,
      final ResourceSessionFactory<R> resourceSessionFactory) {
    this(allResourceSessions, resourceSessionFactory, null, null);
  }

  ResourceStoreImpl(final PathBasedPool<ResourceSession<?, ?>> allResourceSessions,
      final ResourceSessionFactory<R> resourceSessionFactory, final @Nullable Consumer<R> sessionClosed,
      final @Nullable Object lifecycleMonitor) {

    this.resourceSessions = new ConcurrentHashMap<>();
    this.allResourceSessions = allResourceSessions;
    this.resourceSessionFactory = resourceSessionFactory;
    this.sessionClosed = sessionClosed;
    this.lifecycleMonitor = lifecycleMonitor == null
        ? this
        : lifecycleMonitor;
  }

  @Override
  public R beginResourceSession(final ResourceConfiguration resourceConfig, final BufferManager bufferManager,
      final Path resourceFile) {
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
   */
  @Override
  // Throwable identity, not value equality, determines whether addSuppressed would suppress itself.
  @SuppressWarnings("ReferenceEquality")
  public void close() {
    Throwable failure = null;
    for (final Map.Entry<Path, R> entry : resourceSessions.entrySet()) {
      try {
        entry.getValue().close();
      } catch (final RuntimeException | Error e) {
        if (failure == null) {
          failure = e;
        } else if (failure != e) {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure instanceof RuntimeException exception) {
      throw exception;
    }
    if (failure instanceof Error error) {
      throw error;
    }
  }

  @Override
  public boolean closeResourceSession(final Path resourceFile) {
    synchronized (lifecycleMonitor) {
      final R session = resourceSessions.get(resourceFile);
      if (session == null) {
        return false;
      }
      if (sessionClosed != null) {
        sessionClosed.accept(session);
      }
      resourceSessions.remove(resourceFile, session);
      allResourceSessions.removeObject(resourceFile, session);
      return true;
    }
  }
}

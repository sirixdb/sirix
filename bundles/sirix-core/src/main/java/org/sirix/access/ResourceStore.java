package org.sirix.access;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.cache.BufferManager;

import java.nio.file.Path;

public interface ResourceStore<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    extends AutoCloseable {

  /**
   * Open a resource, that is get an instance of a {@link ResourceSession} in order to read/write from
   * the resource.
   *
   * @param resourceConfig The resource configuration.
   * @param bufferManager  The buffer manager.
   * @param resourcePath   The resource to open.
   * @return A resource manager.
   * @throws NullPointerException if one if the arguments is {@code null}
   */
  R beginResourceSession(@NonNull ResourceConfiguration resourceConfig, @NonNull BufferManager bufferManager,
      @NonNull Path resourcePath);

  boolean hasOpenResourceSession(Path resourcePath);

  R getOpenResourceSession(Path resourcePath);

  @Override
  void close();

  boolean closeResourceSession(Path resourceFile);

}

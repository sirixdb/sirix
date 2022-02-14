package org.sirix.access;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;

import java.nio.file.Path;


public interface ResourceStore<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    extends AutoCloseable {

  /**
   * Open a resource, that is get an instance of a {@link ResourceManager} in order to read/write from
   * the resource.
   *
   * @param resourceConfig The resource configuration.
   * @param bufferManager The buffer manager.
   * @param resourceFile The resource to open.
   * @return A resource manager.
   * @throws NullPointerException if one if the arguments is {@code null}
   */
  R openResource(@NonNull ResourceConfiguration resourceConfig,
                 @NonNull BufferManager bufferManager, @NonNull Path resourceFile);

  boolean hasOpenResourceManager(Path resourceFile);

  R getOpenResourceManager(Path resourceFile);

  @Override
  void close();

  boolean closeResourceManager(Path resourceFile);

}

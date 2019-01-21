package org.sirix.access;

import java.nio.file.Path;
import javax.annotation.Nonnull;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;


public interface ResourceStore<R extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>>
    extends AutoCloseable {

  /**
   * Open a resource, that is get an instance of a {@link ResourceManager} in order to read/write from
   * the resource.
   *
   * @param database The database.
   * @param resourceConfig The resource configuration.
   * @param bufferManager The buffer manager.
   * @param resourceFile The resource to open.
   * @return A resource manager.
   * @throws NullPointerException if one if the arguments is {@code null}
   */
  public R openResource(@Nonnull Database<R> database, @Nonnull ResourceConfiguration resourceConfig,
      @Nonnull BufferManager bufferManager, @Nonnull Path resourceFile);

  boolean hasOpenResourceManager(Path resourceFile);

  R getOpenResourceManager(Path resourceFile);

  @Override
  void close();

  boolean closeResource(Path resourceFile);

}

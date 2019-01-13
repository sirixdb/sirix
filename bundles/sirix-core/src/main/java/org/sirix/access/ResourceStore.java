package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.trx.node.json.JsonResourceManager;
import org.sirix.access.trx.node.xdm.XdmResourceManagerImpl;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class ResourceStore implements AutoCloseable {
  /** Central repository of all open XDM resource managers. */
  private final ConcurrentMap<Path, XdmResourceManager> mXdmResourceManagers;

  /** Central repository of all open JSON resource managers. */
  private final ConcurrentMap<Path, JsonResourceManager> mJsonResourceManagers;

  /**
   * Constructor.
   *
   * @throws NullPointerException if one if the arguments is {@code null}
   */
  public ResourceStore() {
    mXdmResourceManagers = new ConcurrentHashMap<>();
    mJsonResourceManagers = new ConcurrentHashMap<>();
  }

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
  public XdmResourceManager openXdmResource(final @Nonnull LocalDatabase database,
      final @Nonnull ResourceConfiguration resourceConfig, final @Nonnull BufferManager bufferManager,
      final @Nonnull Path resourceFile) {
    checkNotNull(database);
    checkNotNull(resourceConfig);
    checkNotNull(bufferManager);
    checkNotNull(resourceFile);

    return mXdmResourceManagers.computeIfAbsent(resourceFile, k -> {
      final Storage storage = StorageType.getStorage(resourceConfig);
      final UberPage uberPage;

      if (storage.exists()) {
        try (final Reader reader = storage.createReader()) {
          final PageReference firstRef = reader.readUberPageReference();
          if (firstRef.getPage() == null) {
            uberPage = (UberPage) reader.read(firstRef, null);
          } else {
            uberPage = (UberPage) firstRef.getPage();
          }
        }
      } else {
        // Bootstrap uber page and make sure there already is a root node.
        uberPage = new UberPage();
      }

      // Get sempahores.
      final Semaphore readSem = Databases.computeReadSempahoreIfAbsent(resourceConfig.getResource(),
          database.getDatabaseConfig().getMaxResourceReadTrx());
      final Lock writeLock = Databases.computeWriteLockIfAbsent(resourceConfig.getResource());

      // Create the resource manager instance.
      final XdmResourceManager resourceManager = new XdmResourceManagerImpl(database, this, resourceConfig,
          bufferManager, StorageType.getStorage(resourceConfig), uberPage, readSem, writeLock);

      // Put it in the databases cache.
      Databases.putResourceManager(resourceFile, resourceManager);

      // And return it.
      return resourceManager;
    });
  }

  public boolean hasOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return mXdmResourceManagers.containsKey(resourceFile);
  }

  public XdmResourceManager getOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return mXdmResourceManagers.get(resourceFile);
  }

  @Override
  public void close() {
    mXdmResourceManagers.forEach((resourceName, resourceMgr) -> resourceMgr.close());
  }

  public boolean closeResource(final Path resourceFile) {
    final XdmResourceManager manager = mXdmResourceManagers.remove(resourceFile);
    Databases.removeResourceManager(resourceFile, manager);
    return manager != null;
  }
}

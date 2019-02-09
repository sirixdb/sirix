package org.sirix.access.xdm;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import org.sirix.access.AbstractResourceStore;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.xdm.XdmResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.UberPage;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class XdmResourceStore extends AbstractResourceStore<XdmResourceManager> {

  /**
   * Default constructor.
   */
  public XdmResourceStore() {
    super(new ConcurrentHashMap<>());
  }

  @Override
  public XdmResourceManager openResource(final @Nonnull Database<XdmResourceManager> database,
      final @Nonnull ResourceConfiguration resourceConfig, final @Nonnull BufferManager bufferManager,
      final @Nonnull Path resourceFile) {
    checkNotNull(database);
    checkNotNull(resourceConfig);
    checkNotNull(bufferManager);
    checkNotNull(resourceFile);

    return mResourceManagers.computeIfAbsent(resourceFile, k -> {
      final Storage storage = StorageType.getStorage(resourceConfig);
      final UberPage uberPage = getUberPage(storage);

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
}

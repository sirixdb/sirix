package org.sirix.access.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import org.sirix.access.AbstractResourceStore;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.trx.node.xml.XmlResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.UberPage;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlResourceStore extends AbstractResourceStore<XmlResourceManager> {

  /**
   * Default constructor.
   */
  public XmlResourceStore() {
    super(new ConcurrentHashMap<>(), null);
  }

  /**
   * Constructor.
   */
  public XmlResourceStore(final User user) {
    super(new ConcurrentHashMap<>(), user);
  }

  @Override
  public XmlResourceManager openResource(final @Nonnull Database<XmlResourceManager> database,
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
      final XmlResourceManager resourceManager = new XmlResourceManagerImpl(database, this, resourceConfig,
          bufferManager, StorageType.getStorage(resourceConfig), uberPage, readSem, writeLock, mUser);

      // Put it in the databases cache.
      Databases.putResourceManager(resourceFile, resourceManager);

      // And return it.
      return resourceManager;
    });
  }
}

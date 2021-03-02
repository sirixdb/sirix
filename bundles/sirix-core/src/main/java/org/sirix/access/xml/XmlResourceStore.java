package org.sirix.access.xml;

import org.sirix.access.AbstractResourceStore;
import org.sirix.access.DatabasesInternals;
import org.sirix.access.PathBasedPool;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.WriteLocksRegistry;
import org.sirix.access.trx.node.xml.XmlResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.IOStorage;
import org.sirix.io.StorageType;
import org.sirix.page.UberPage;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlResourceStore extends AbstractResourceStore<XmlResourceManager> {

  /**
   * This field should be use to fetch the locks for resource managers.
   */
  private final WriteLocksRegistry writeLocksRegistry;

  /**
   * Constructor.
   */
  public XmlResourceStore(final User user,
                          final WriteLocksRegistry writeLocksRegistry,
                          final PathBasedPool<ResourceManager<?, ?>> allResourceManagers) {
    super(new ConcurrentHashMap<>(), allResourceManagers, user);

    this.writeLocksRegistry = writeLocksRegistry;
  }

  @Override
  public XmlResourceManager openResource(final @Nonnull Database<XmlResourceManager> database,
      final @Nonnull ResourceConfiguration resourceConfig, final @Nonnull BufferManager bufferManager,
      final @Nonnull Path resourceFile) {
    checkNotNull(database);
    checkNotNull(resourceConfig);
    checkNotNull(bufferManager);
    checkNotNull(resourceFile);

    return resourceManagers.computeIfAbsent(resourceFile, k -> {
      final IOStorage storage = StorageType.getStorage(resourceConfig);
      final UberPage uberPage = getUberPage(storage);

      final Lock writeLock = this.writeLocksRegistry.getWriteLock(resourceConfig.getResource());

      // Create the resource manager instance.
      final XmlResourceManager resourceManager = new XmlResourceManagerImpl(database, this, resourceConfig,
          bufferManager, StorageType.getStorage(resourceConfig), uberPage, writeLock, user);

      // Put it in the databases cache.
      this.allResourceManagers.putObject(resourceFile, resourceManager);

      // And return it.
      return resourceManager;
    });
  }
}

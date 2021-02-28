package org.sirix.access.json;

import org.sirix.access.AbstractResourceStore;
import org.sirix.access.DatabasesInternals;
import org.sirix.access.PathBasedPool;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.WriteLocksRegistry;
import org.sirix.access.trx.node.json.JsonResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonResourceManager;
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
public final class JsonResourceStore extends AbstractResourceStore<JsonResourceManager> {

  private final WriteLocksRegistry writeLocksRegistry;

  /**
   * Constructor.
   */
  public JsonResourceStore(final User user,
                           final WriteLocksRegistry writeLocksRegistry,
                           final PathBasedPool<ResourceManager<?, ?>> allResourceManagers) {
    super(new ConcurrentHashMap<>(), allResourceManagers, user);

    this.writeLocksRegistry = writeLocksRegistry;
  }

  @Override
  public JsonResourceManager openResource(final @Nonnull Database<JsonResourceManager> database,
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
      final JsonResourceManager resourceManager = new JsonResourceManagerImpl(database, this, resourceConfig,
          bufferManager, StorageType.getStorage(resourceConfig), uberPage, writeLock, user);

      // Put it in the databases cache.
      this.allResourceManagers.putObject(resourceFile, resourceManager);

      // And return it.
      return resourceManager;
    });
  }
}

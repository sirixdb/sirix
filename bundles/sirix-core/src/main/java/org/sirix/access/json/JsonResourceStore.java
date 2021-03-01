package org.sirix.access.json;

import org.sirix.access.AbstractResourceStore;
import org.sirix.access.DatabasesInternals;
import org.sirix.access.PathBasedPool;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.trx.node.json.JsonResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.IOStorage;
import org.sirix.io.StorageType;
import org.sirix.page.UberPage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

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

  /**
   * {@link LogWrapper} reference.
   */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(JsonResourceStore.class));

  /**
   * Constructor.
   */
  public JsonResourceStore(final User user,
                           final PathBasedPool<ResourceManager<?, ?>> allResourceManagers) {
    super(new ConcurrentHashMap<>(), allResourceManagers, user);
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

      final Lock writeLock = DatabasesInternals.computeWriteLockIfAbsent(resourceConfig.getResource());

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

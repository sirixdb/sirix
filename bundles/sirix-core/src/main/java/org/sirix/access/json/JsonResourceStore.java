package org.sirix.access.json;

import org.sirix.access.AbstractResourceStore;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.trx.node.json.JsonResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.UberPage;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class JsonResourceStore extends AbstractResourceStore<JsonResourceManager> {

  /**
   * Default constructor.
   */
  public JsonResourceStore() {
    super(new ConcurrentHashMap<>(), null);
  }

  /**
   * Constructor.
   */
  public JsonResourceStore(final User user) {
    super(new ConcurrentHashMap<>(), user);
  }

  @Override
  public JsonResourceManager openResource(final @Nonnull Database<JsonResourceManager> database,
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
      final JsonResourceManager resourceManager = new JsonResourceManagerImpl(database, this, resourceConfig,
          bufferManager, StorageType.getStorage(resourceConfig), uberPage, readSem, writeLock, mUser);

      // Put it in the databases cache.
      Databases.putResourceManager(resourceFile, resourceManager);

      // And return it.
      return resourceManager;
    });
  }
}

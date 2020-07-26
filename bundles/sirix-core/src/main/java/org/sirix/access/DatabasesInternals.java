package org.sirix.access;

import org.sirix.api.Database;
import org.sirix.api.ResourceManager;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DatabasesInternals {
  private DatabasesInternals() {
    throw new AssertionError();
  }

  public static Lock computeWriteLockIfAbsent(Path resourcePath) {
    return Databases.RESOURCE_WRITE_LOCKS.computeIfAbsent(resourcePath, res -> new ReentrantLock());
  }

  public static void removeWriteLock(Path resourcePath) {
    Databases.RESOURCE_WRITE_LOCKS.remove(resourcePath);
  }

  public static ConcurrentMap<Path, Set<Database<?>>> getOpenDatabases() {
    return Databases.DATABASE_SESSIONS;
  }

  /**
   * Put a resource manager into the internal map.
   *
   * @param file resource file to put into the map
   * @param resourceManager resourceManager handle to put into the map
   */
  public static synchronized void putResourceManager(final Path file, final ResourceManager<?, ?> resourceManager) {
    Databases.RESOURCE_MANAGERS.computeIfAbsent(file, path -> new HashSet<>()).add(resourceManager);
  }

  /**
   * Remove a resource manager.
   *
   * @param file the resource file
   * @param resourceManager manager to remove
   */
  public static synchronized void removeResourceManager(final Path file, final ResourceManager<?, ?> resourceManager) {
    final Set<ResourceManager<?, ?>> resourceManagers = Databases.RESOURCE_MANAGERS.get(file);

    if (resourceManagers == null) {
      return;
    }

    resourceManagers.remove(resourceManager);

    if (resourceManagers.isEmpty()) {
      Databases.RESOURCE_MANAGERS.remove(file);
    }
  }
}

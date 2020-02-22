package org.sirix.access;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.sirix.api.ResourceManager;

public final class DatabasesInternals {
  private DatabasesInternals() {
    throw new AssertionError();
  }

  public static Semaphore computeReadSempahoreIfAbsent(Path resourcePath, int numberOfPermits) {
    return Databases.RESOURCE_READ_SEMAPHORES.computeIfAbsent(resourcePath, res -> new Semaphore(numberOfPermits));
  }

  public static Lock computeWriteLockIfAbsent(Path resourcePath) {
    return Databases.RESOURCE_WRITE_SEMAPHORES.computeIfAbsent(resourcePath, res -> new ReentrantLock());
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

    if (resourceManagers.isEmpty())
      Databases.RESOURCE_MANAGERS.remove(file);
  }
}

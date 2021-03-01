package org.sirix.access;

import org.sirix.api.Database;

import java.nio.file.Path;
import java.util.Map;
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

  public static Map<Path, Set<Database<?>>> getOpenDatabases() {
    return Databases.MANAGER.sessions().asMap();
  }

}

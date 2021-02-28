package org.sirix.access;

import org.sirix.api.Database;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public final class DatabasesInternals {

  private DatabasesInternals() {
    throw new AssertionError();
  }

  public static Map<Path, Set<Database<?>>> getOpenDatabases() {
    return Databases.MANAGER.sessions().asMap();
  }

}

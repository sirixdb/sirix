package io.sirix.access;

import io.sirix.api.Database;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public final class DatabasesInternals {

  private DatabasesInternals() {
    throw new AssertionError();
  }

  /**
   * Return an immutable snapshot of user database handles grouped by canonical path. Temporary
   * backends used only for deletion are excluded.
   *
   * @return immutable registry membership; the handles retain their live lifecycle state
   */
  public static Map<Path, Set<Database<?>>> getOpenDatabases() {
    return Databases.snapshotOpenDatabases();
  }

}

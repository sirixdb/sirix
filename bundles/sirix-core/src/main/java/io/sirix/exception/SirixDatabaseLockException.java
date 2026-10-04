package io.sirix.exception;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/** Raised when another process owns the database's operating-system file lock. */
public final class SirixDatabaseLockException extends SirixUsageException {
  private static final long serialVersionUID = 1L;

  private final Path databasePath;

  public SirixDatabaseLockException(final Path databasePath) {
    super("Database is already owned by another process: " + requireNonNull(databasePath));
    this.databasePath = databasePath;
  }

  public Path getDatabasePath() {
    return databasePath;
  }
}

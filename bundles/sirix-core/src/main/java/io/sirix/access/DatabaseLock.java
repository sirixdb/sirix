package io.sirix.access;

import io.sirix.exception.SirixDatabaseLockException;
import io.sirix.exception.SirixIOException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Process ownership of a database. The persistent lock file is never unlinked on close. */
final class DatabaseLock implements AutoCloseable {
  private final FileChannel channel;

  // Retain the lock for the lifetime of the channel. Closing the channel releases it, even when
  // cleanup of the database fails. A crashed process also releases the OS lock automatically.
  private final FileLock lock;

  private DatabaseLock(final FileChannel channel, final FileLock lock) {
    this.channel = channel;
    this.lock = lock;
  }

  static DatabaseLock acquire(final Path databasePath) {
    final Path lockPath = databasePath.resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile());
    final FileChannel channel;
    try {
      channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    } catch (final IOException e) {
      throw new SirixIOException("Could not open database ownership lock at " + lockPath, e);
    }

    try {
      final FileLock lock = channel.tryLock();
      if (lock == null) {
        throw new SirixDatabaseLockException(databasePath);
      }
      return new DatabaseLock(channel, lock);
    } catch (final IOException | RuntimeException | Error e) {
      try {
        channel.close();
      } catch (final IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      if (e instanceof OverlappingFileLockException) {
        throw new SirixDatabaseLockException(databasePath);
      }
      if (e instanceof RuntimeException failure) {
        throw failure;
      }
      if (e instanceof Error failure) {
        throw failure;
      }
      throw new SirixIOException("Could not acquire database ownership lock at " + lockPath, e);
    }
  }

  @Override
  public void close() {
    try {
      channel.close();
    } catch (final IOException e) {
      throw new SirixIOException("Could not release database ownership lock " + lock, e);
    }
  }
}

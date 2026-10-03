package io.sirix.access;

import io.sirix.exception.SirixDatabaseLockException;
import io.sirix.exception.SirixIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** Process ownership of a database. The persistent lock file is never unlinked on close. */
final class DatabaseLock implements AutoCloseable {
  private final FileChannel channel;
  private final FileChannel verificationChannel;

  // The database owner closes these channels only after its writers and backend are quiesced.
  // A crashed process also releases the OS lock automatically.
  private final FileLock lock;

  private DatabaseLock(final FileChannel channel, final FileLock lock, final FileChannel verificationChannel) {
    this.channel = channel;
    this.lock = lock;
    this.verificationChannel = verificationChannel;
  }

  static DatabaseLock acquire(final Path databasePath) {
    final Path lockPath = databasePath.resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile());
    final FileChannel channel;
    try {
      channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    } catch (final IOException e) {
      throw new SirixIOException("Could not open database ownership lock at " + lockPath, e);
    }

    FileChannel verificationChannel = null;
    try {
      final FileLock lock = channel.tryLock(0, 1, false);
      if (lock == null) {
        throw new SirixDatabaseLockException(databasePath);
      }
      // Validate that this descriptor still names the current .lock generation after acquisition;
      // removal/recreation may have unlinked the file between open and tryLock. Keep the token
      // outside the locked byte so the verification read also works with mandatory file locking.
      final UUID generation = UUID.randomUUID();
      final ByteBuffer token = ByteBuffer.allocate(2 * Long.BYTES);
      token.putLong(generation.getMostSignificantBits()).putLong(generation.getLeastSignificantBits()).flip();
      channel.position(1);
      while (token.hasRemaining()) {
        channel.write(token);
      }
      // Retain this channel until ownership ends: closing any descriptor for the same file can
      // release POSIX process locks, including the one held through the ownership channel.
      verificationChannel = FileChannel.open(lockPath, StandardOpenOption.READ);
      verificationChannel.position(1);
      token.clear();
      while (token.hasRemaining()) {
        if (verificationChannel.read(token) < 0) {
          throw new SirixDatabaseLockException(databasePath);
        }
      }
      token.flip();
      if (token.getLong() != generation.getMostSignificantBits()
          || token.getLong() != generation.getLeastSignificantBits()) {
        throw new SirixDatabaseLockException(databasePath);
      }
      return new DatabaseLock(channel, lock, verificationChannel);
    } catch (final IOException | RuntimeException | Error e) {
      if (verificationChannel != null) {
        try {
          verificationChannel.close();
        } catch (final IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
      }
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
      try {
        verificationChannel.close();
      } finally {
        channel.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException("Could not release database ownership lock " + lock, e);
    }
  }
}

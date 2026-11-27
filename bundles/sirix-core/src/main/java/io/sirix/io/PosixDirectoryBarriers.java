/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import static java.util.Objects.requireNonNull;

/**
 * Linux, macOS and other POSIX systems: the barrier is a force of a directory channel. On macOS the
 * JDK implements every {@link FileChannel#force} as {@code fcntl(F_FULLFSYNC)} (JDK-8080589), the
 * full device flush TXN-616 asks for there; elsewhere it is {@code fsync} of the directory
 * descriptor.
 */
final class PosixDirectoryBarriers implements DirectoryBarriers {

  static final PosixDirectoryBarriers INSTANCE = new PosixDirectoryBarriers(ChannelOpener.FILE_SYSTEM);

  private final ChannelOpener opener;

  PosixDirectoryBarriers(final ChannelOpener opener) {
    this.opener = requireNonNull(opener);
  }

  @Override
  public void syncDirectory(final Path directory) {
    try (final FileChannel channel = opener.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (final IOException | UnsupportedOperationException e) {
      throw DurableFiles.barrierFailure(directory, e);
    }
  }

  @Override
  public void move(final Path source, final Path target, final boolean replaceExisting) throws IOException {
    final Path directory = DurableFiles.siblingDirectory(source, target);
    if (replaceExisting) {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } else {
      // rename(2) silently replaces an existing file; callers pass fresh names, so refuse instead.
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
        throw new FileAlreadyExistsException(target.toString());
      }
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }
    syncDirectory(directory);
  }
}

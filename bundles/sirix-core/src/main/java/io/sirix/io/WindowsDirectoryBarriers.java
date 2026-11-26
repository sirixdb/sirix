/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * Windows: a directory cannot be opened as a {@link FileChannel}, so TXN-616's barriers are
 * kernel32 calls through {@link Kernel32Calls}.
 *
 * <ul>
 * <li>A create or delete is made durable by {@code FlushFileBuffers} on a handle to the directory,
 * opened with {@code FILE_FLAG_BACKUP_SEMANTICS} (required to open a directory) and
 * {@code GENERIC_WRITE} (required by {@code FlushFileBuffers}).</li>
 * <li>A rename is issued as {@code MoveFileExW} with {@code MOVEFILE_WRITE_THROUGH}, which returns
 * only once the rename is on disk; {@code Files.move} passes no such flag.</li>
 * </ul>
 */
final class WindowsDirectoryBarriers implements DirectoryBarriers {

  static final int GENERIC_WRITE = 0x4000_0000;
  static final int FILE_SHARE_READ_WRITE_DELETE = 0x1 | 0x2 | 0x4;
  static final int OPEN_EXISTING = 3;
  static final int FILE_FLAG_BACKUP_SEMANTICS = 0x0200_0000;
  static final int MOVEFILE_REPLACE_EXISTING = 0x1;
  static final int MOVEFILE_WRITE_THROUGH = 0x8;
  static final long INVALID_HANDLE_VALUE = -1L;

  static final int ERROR_FILE_NOT_FOUND = 2;
  static final int ERROR_PATH_NOT_FOUND = 3;
  static final int ERROR_ACCESS_DENIED = 5;
  static final int ERROR_FILE_EXISTS = 80;
  static final int ERROR_ALREADY_EXISTS = 183;

  /** Longer Win32 paths need the extended-length prefix, as in the JDK's {@code WindowsPath}. */
  static final int MAX_PLAIN_PATH = 247;

  private final Kernel32Calls kernel32;

  WindowsDirectoryBarriers(final Kernel32Calls kernel32) {
    this.kernel32 = requireNonNull(kernel32);
  }

  @Override
  public void syncDirectory(final Path directory) {
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment state = arena.allocate(kernel32.stateLayout);
      final MemorySegment handle = (MemorySegment) kernel32.createFileW.invokeExact(state, wide(arena, directory),
          GENERIC_WRITE, FILE_SHARE_READ_WRITE_DELETE, MemorySegment.NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS,
          MemorySegment.NULL);
      if (handle.address() == INVALID_HANDLE_VALUE) {
        throw DurableFiles.barrierFailure(directory, failure("CreateFileW", lastError(state), directory, null));
      }
      try {
        final int flushed = (int) kernel32.flushFileBuffers.invokeExact(state, handle);
        if (flushed == 0) {
          throw DurableFiles.barrierFailure(directory, failure("FlushFileBuffers", lastError(state), directory, null));
        }
      } finally {
        // A failed close leaks nothing the flush depended on: its result is already known.
        final int ignored = (int) kernel32.closeHandle.invokeExact(handle);
      }
    } catch (final SirixIOException | Error e) {
      throw e;
    } catch (final Throwable e) {
      throw DurableFiles.barrierFailure(directory, e);
    }
  }

  @Override
  public void move(final Path source, final Path target, final boolean replaceExisting) throws IOException {
    final int flags = replaceExisting
        ? MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING
        : MOVEFILE_WRITE_THROUGH;
    final int moved;
    final int error;
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment state = arena.allocate(kernel32.stateLayout);
      moved = (int) kernel32.moveFileExW.invokeExact(state, wide(arena, source), wide(arena, target), flags);
      error = lastError(state);
    } catch (final Error e) {
      throw e;
    } catch (final Throwable e) {
      throw new IOException("MoveFileExW failed for " + source + " -> " + target, e);
    }
    if (moved == 0) {
      throw failure("MoveFileExW", error, source, target);
    }
  }

  private int lastError(final MemorySegment state) {
    return (int) kernel32.lastError.get(state, 0L);
  }

  /** Maps a Win32 error the way {@code Files.move} does, so callers handle both alike. */
  static FileSystemException failure(final String call, final int error, final Path file, final @Nullable Path other) {
    final String reason = call + " failed with Windows error " + error;
    final String otherName = other == null
        ? null
        : other.toString();
    return switch (error) {
      case ERROR_FILE_NOT_FOUND, ERROR_PATH_NOT_FOUND -> new NoSuchFileException(file.toString(), otherName, reason);
      case ERROR_ACCESS_DENIED -> new AccessDeniedException(file.toString(), otherName, reason);
      case ERROR_FILE_EXISTS, ERROR_ALREADY_EXISTS ->
        new FileAlreadyExistsException(file.toString(), otherName, reason);
      default -> new FileSystemException(file.toString(), otherName, reason);
    };
  }

  /** The Win32 spelling of an absolute path: extended-length once the plain limit is exceeded. */
  static String win32Path(final String absolute) {
    if (absolute.length() <= MAX_PLAIN_PATH || absolute.startsWith("\\\\?\\")) {
      return absolute;
    }
    return absolute.startsWith("\\\\")
        ? "\\\\?\\UNC\\" + absolute.substring(2)
        : "\\\\?\\" + absolute;
  }

  /** A NUL-terminated UTF-16LE copy of {@code path}'s Win32 spelling. */
  private static MemorySegment wide(final Arena arena, final Path path) {
    return arena.allocateFrom(win32Path(path.toAbsolutePath().normalize().toString()), StandardCharsets.UTF_16LE);
  }
}

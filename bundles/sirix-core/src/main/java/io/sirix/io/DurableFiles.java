/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import io.sirix.utils.OS;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Directory-entry durability (TXN-616): the one place the engine makes a file's <em>name</em>
 * durable, as opposed to its contents.
 *
 * <p>
 * Forcing a file's contents does not make a new, renamed or removed name in its directory durable.
 * Every site whose guarantee relies on a name calls this class after changing it:
 *
 * <ul>
 * <li>{@link #syncDirectory(Path)} is the directory barrier. It covers every earlier create, rename
 * and delete in that directory, so an operation that changes several names in one directory issues
 * it once, after the last change: a directory force on Linux and other POSIX systems,
 * {@code F_FULLFSYNC} on macOS, {@code FlushFileBuffers} on a backup-semantics directory handle on
 * Windows.</li>
 * <li>{@link #replace(Path, Path)} and {@link #rename(Path, Path)} are atomic renames that are
 * durable on return: {@code MoveFileExW} with {@code MOVEFILE_WRITE_THROUGH} on Windows, an atomic
 * move followed by the barrier on their shared directory elsewhere.</li>
 * </ul>
 *
 * <p>
 * A barrier is never skipped: a platform or filesystem that rejects it fails the operation with a
 * {@link SirixIOException} naming the directory (TXN-616). A caller on the commit path must treat
 * that failure as an unknown durability outcome and never retry it (TXN-705).
 *
 * <p>
 * None of this runs per record or per page. Barrier placement and commit-path cost are owned by
 * TXN-616 in {@code spec/TXN-transactions-durability.md}.
 */
public final class DurableFiles {

  /** Observes completed directory barriers and file forces; installed by crash-model tests only. */
  @FunctionalInterface
  public interface BarrierObserver {
    /**
     * Called after a barrier on {@code directory} completed: every entry change made in it so far is
     * durable.
     *
     * @param directory the directory whose entries are now durable
     */
    void afterBarrier(Path directory);

    /**
     * Called after {@link #forceFile(Path)} completed: the file's current contents are durable.
     *
     * @param file the file whose contents are now durable
     */
    default void afterForce(final Path file) {}
  }

  /** Removes an observer installed with {@link #observeBarriers(BarrierObserver)}. */
  public interface Observation extends AutoCloseable {
    @Override
    void close();
  }

  /** Chosen once: callers never branch on the operating system. */
  private static final DirectoryBarriers PLATFORM = OS.isWindows()
      ? new WindowsDirectoryBarriers(Kernel32Calls.Native.KERNEL32)
      : PosixDirectoryBarriers.INSTANCE;

  private static volatile @Nullable BarrierObserver observer;

  private DurableFiles() {
    throw new AssertionError("static utility");
  }

  /**
   * Installs {@code barrierObserver} until the returned observation is closed. Only one observer is
   * active at a time; a test that installs one must run isolated from other users of this class.
   *
   * @param barrierObserver the observer to call after each completed barrier
   * @return the observation, whose {@code close} restores the previous observer
   */
  public static Observation observeBarriers(final BarrierObserver barrierObserver) {
    requireNonNull(barrierObserver, "barrierObserver");
    final BarrierObserver previous = observer;
    observer = barrierObserver;
    return () -> observer = previous;
  }

  /**
   * Makes every earlier create, rename and delete of an entry in {@code directory} durable.
   *
   * @param directory an existing directory
   * @throws SirixIOException naming {@code directory} if the barrier is rejected or fails
   */
  public static void syncDirectory(final Path directory) {
    PLATFORM.syncDirectory(requireNonNull(directory, "directory"));
    notifyBarrier(directory);
  }

  /**
   * Forces the contents and file-size metadata of an existing regular file, for files written through
   * streams that offer no force of their own.
   *
   * @param file an existing regular file
   * @throws SirixIOException naming {@code file} if the force fails
   */
  public static void forceFile(final Path file) {
    forceFile(file, ChannelOpener.FILE_SYSTEM);
  }

  static void forceFile(final Path file, final ChannelOpener opener) {
    requireNonNull(file, "file");
    // Write access on every platform: FlushFileBuffers, which the JDK's force calls on Windows, needs
    // it, and every file forced here is one the engine has just written.
    try (final FileChannel channel = opener.open(file, StandardOpenOption.WRITE)) {
      channel.force(true);
    } catch (final IOException e) {
      throw new SirixIOException("Cannot make the contents of " + file + " durable", e);
    }
    final BarrierObserver current = observer;
    if (current != null) {
      current.afterForce(file);
    }
  }

  /**
   * Atomically replaces {@code target} with {@code source} and makes the replacement durable before
   * returning: after a crash, {@code target} names either its previous contents or the contents of
   * {@code source}, and once this returns, the latter. The caller forces the contents of
   * {@code source} first.
   *
   * @param source the forced file to move, a sibling of {@code target}
   * @param target the name to replace or create
   * @throws IOException if the move itself fails; nothing was replaced
   * @throws SirixIOException naming the directory if the move happened but cannot be made durable
   */
  public static void replace(final Path source, final Path target) throws IOException {
    move(source, target, true);
  }

  /**
   * Atomically renames {@code source} to the absent name {@code target} and makes the rename durable
   * before returning.
   *
   * @param source the existing file or directory
   * @param target an absent sibling name
   * @throws IOException if the rename itself fails, {@link FileAlreadyExistsException} if
   *         {@code target} exists; nothing was renamed
   * @throws SirixIOException naming the directory if the rename happened but cannot be made durable
   */
  public static void rename(final Path source, final Path target) throws IOException {
    move(source, target, false);
  }

  /**
   * Creates {@code directory} and every missing ancestor, and makes their names durable before
   * returning, including the name of an existing target directory. Entries later created inside
   * {@code directory} need their own barrier.
   *
   * @param directory the directory to create or accept
   * @throws IOException if a directory cannot be created
   * @throws SirixIOException naming the directory whose barrier fails
   */
  public static void createDirectories(final Path directory) throws IOException {
    final Path absolute = requireNonNull(directory, "directory").toAbsolutePath().normalize();
    final List<Path> missing = new ArrayList<>();
    for (Path ancestor = absolute; ancestor != null && Files.notExists(ancestor); ancestor = ancestor.getParent()) {
      missing.add(ancestor);
    }
    if (missing.isEmpty()) {
      Files.createDirectories(absolute);
      if (absolute.getParent() != null) {
        syncDirectory(absolute.getParent());
      }
    }
    for (int i = missing.size() - 1; i >= 0; i--) {
      final Path created = missing.get(i);
      Files.createDirectories(created);
      syncCreatedEntry(created);
    }
  }

  public static void createFile(final Path file) throws IOException {
    Files.createFile(requireNonNull(file, "file"));
    syncCreatedEntry(file);
  }

  private static void syncCreatedEntry(final Path entry) {
    try {
      syncDirectory(parentOf(entry));
    } catch (final SirixIOException failure) {
      try {
        Files.delete(entry);
      } catch (final IOException cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }
  }

  private static void move(final Path source, final Path target, final boolean replaceExisting) throws IOException {
    requireNonNull(source, "source");
    requireNonNull(target, "target");
    final Path targetDirectory = parentOf(target);
    PLATFORM.move(source, target, replaceExisting);
    notifyBarrier(targetDirectory);
  }

  static Path parentOf(final Path path) {
    final Path parent = path.toAbsolutePath().getParent();
    if (parent == null) {
      throw new IllegalArgumentException("A filesystem root has no parent directory: " + path);
    }
    return parent;
  }

  static Path siblingDirectory(final Path source, final Path target) {
    final Path directory = parentOf(source);
    if (!directory.equals(parentOf(target))) {
      throw new IllegalArgumentException("Durable moves require sibling names: " + source + " -> " + target);
    }
    return directory;
  }

  static SirixIOException barrierFailure(final Path directory, final Throwable cause) {
    return new SirixIOException("Cannot make the entries of directory " + directory + " durable", cause);
  }

  private static void notifyBarrier(final Path directory) {
    final BarrierObserver current = observer;
    if (current != null) {
      current.afterBarrier(directory);
    }
  }
}

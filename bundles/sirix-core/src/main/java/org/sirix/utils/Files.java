package org.sirix.utils;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Set;
import org.sirix.exception.SirixIOException;

/**
 * Static methods for file operations.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class Files {

  /** Utility methods. */
  private Files() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Recursively remove a directory.
   *
   * @param path {@link Path} to the directory
   * @param options {@link Set} of {@link FileVisitOption}s (currently only supports following
   *        {@code symlinks} or not).
   * @throws SirixIOException if any I/O operation fails
   * @throws NullPointerException if any of the arguments are {@code null}
   */
  public static void recursiveRemove(final Path path, final Set<FileVisitOption> options) {
    try {
      if (java.nio.file.Files.exists(path)) {
        java.nio.file.Files.walkFileTree(checkNotNull(path), checkNotNull(options),
            Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                  throws IOException {
                java.nio.file.Files.delete(file);
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
                  throws IOException {
                if (exc == null) {
                  java.nio.file.Files.delete(dir);
                  return FileVisitResult.CONTINUE;
                } else {
                  // Directory iteration failed.
                  throw exc;
                }
              }
            });
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Recursively remove a directory. Doesn't follow symlinks.
   *
   * @param path {@link Path} to the directory
   * @throws SirixIOException if any I/O operation fails
   * @throws NullPointerException if any of the arguments are {@code null}
   */
  public static void recursiveRemove(final Path path) {
    recursiveRemove(path, EnumSet.noneOf(FileVisitOption.class));
  }
}

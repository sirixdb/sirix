package org.treetank.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Set;

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
   * @param pPath
   *          {@link Path} to the directory
   * @param pOptions
   *          {@link Set} of {@link FileVisitOption}s (currently only supports following {@code symlinks} or
   *          not).
   * @throws IOException
   *           if any I/O operation fails
   * @throws NullPointerException
   *           if any of the arguments are {@code null}
   */
  public static void recursiveRemove(final Path pPath, final Set<FileVisitOption> pOptions)
    throws IOException {
    if (java.nio.file.Files.exists(pPath)) {
      java.nio.file.Files.walkFileTree(checkNotNull(pPath), checkNotNull(pOptions), Integer.MAX_VALUE,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(final Path pFile, final BasicFileAttributes pAttrs)
            throws IOException {
            java.nio.file.Files.delete(pFile);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(final Path pDir, final IOException pExc)
            throws IOException {
            if (pExc == null) {
              java.nio.file.Files.delete(pDir);
              return FileVisitResult.CONTINUE;
            } else {
              // Directory iteration failed.
              throw pExc;
            }
          }
        });
    }
  }

  /**
   * Recursively remove a directory. Doesn't follow symlinks.
   * 
   * @param pPath
   *          {@link Path} to the directory
   * @throws IOException
   *           if any I/O operation fails
   * @throws NullPointerException
   *           if any of the arguments are {@code null}
   */
  public static void recursiveRemove(final Path pPath) throws IOException {
    recursiveRemove(pPath, EnumSet.noneOf(FileVisitOption.class));
  }
}

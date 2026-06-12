package io.sirix.io;

import io.sirix.exception.SirixIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Open-time superblock validation, performed once per JVM per canonical file path.
 * <p>
 * Storage instances are request-scoped (a REST request opens and closes its own storage), so a
 * per-instance "validated" flag re-paid two file opens + header reads on every request. The
 * superblock is immutable after the first commit; once a path's header validated, re-checking it
 * for every new storage instance buys nothing — corruption that happens AFTER the first
 * validation is caught by the per-read beacon checksums, which is the existing protection model.
 * <p>
 * Invalidation: resource removal drops the resource's paths ({@link #invalidateUnder(Path)});
 * tests that mutate files out-of-band simulate a cold process via
 * {@code Databases.clearGlobalCaches()}, which calls {@link #clear()}.
 *
 * @author Johannes Lichtenberger
 */
public final class SuperblockValidator {

  /** Canonical paths whose superblock has been validated in this JVM. */
  private static final Set<Path> VALIDATED_PATHS = ConcurrentHashMap.newKeySet();

  private SuperblockValidator() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Validates the superblock at offset 0 of {@code path} unless this JVM already validated it.
   * Fresh/empty files are skipped WITHOUT marking the path validated — the first commit writes the
   * superblock, and the next open after that must actually check it.
   *
   * @param path the data or revisions file
   * @param role expected file role ({@link Superblock#ROLE_DATA} or {@link Superblock#ROLE_REVISIONS})
   * @throws SirixIOException if the superblock is invalid or an I/O error occurs
   */
  public static void validateOnce(final Path path, final byte role) {
    final Path canonical = canonical(path);
    if (VALIDATED_PATHS.contains(canonical)) {
      return;
    }
    if (validate(canonical, role)) {
      VALIDATED_PATHS.add(canonical);
    }
  }

  /**
   * Drops every validated path below {@code directory} — call when a resource's files are deleted
   * so a recreated resource at the same path gets its new superblock validated again.
   */
  public static void invalidateUnder(final Path directory) {
    final Path canonical = canonical(directory);
    VALIDATED_PATHS.removeIf(path -> path.startsWith(canonical));
  }

  /** Drops all validated paths — cold-process simulation for out-of-band file mutation tests. */
  public static void clear() {
    VALIDATED_PATHS.clear();
  }

  private static Path canonical(final Path path) {
    return path.toAbsolutePath().normalize();
  }

  /**
   * @return {@code true} if a superblock was actually validated, {@code false} for a fresh/empty
   *         file (nothing to validate yet)
   */
  private static boolean validate(final Path path, final byte role) {
    try {
      if (!Files.exists(path) || Files.size(path) == 0) {
        return false; // fresh file — the first commit writes the superblock
      }
      try (final FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
        final ByteBuffer buf = ByteBuffer.allocate(Superblock.BYTES);
        int position = 0;
        while (buf.hasRemaining()) {
          final int read = ch.read(buf, position);
          if (read < 0) {
            break;
          }
          position += read;
        }
        buf.flip();
        Superblock.validate(buf, role, path.getFileName().toString());
      }
      return true;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}

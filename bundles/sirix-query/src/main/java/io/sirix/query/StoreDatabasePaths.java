package io.sirix.query;

import io.brackit.query.jdm.DocumentException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public final class StoreDatabasePaths {
  private StoreDatabasePaths() {}

  public static Path resolveForCreate(final Path databasePath) {
    requireNonNull(databasePath);
    try {
      final Path target = Files.isSymbolicLink(databasePath)
          ? databasePath.resolveSibling(Files.readSymbolicLink(databasePath))
          : databasePath;
      return Files.exists(target)
          ? target.toRealPath()
          : target;
    } catch (final IOException e) {
      throw new DocumentException(e);
    }
  }
}

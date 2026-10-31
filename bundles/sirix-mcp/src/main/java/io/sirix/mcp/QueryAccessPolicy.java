package io.sirix.mcp;

import io.sirix.access.ResourceAccessScope;
import io.sirix.api.ResourceSession;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import io.sirix.access.DatabaseConfiguration;

/** Enforces the actual query targets, including direct transaction functions and temporal axes. */
final class QueryAccessPolicy implements ResourceAccessScope.Policy {
  private final Path root;
  private final AccessControl access;
  private final String database;
  private final String resource;
  private final int revision;

  QueryAccessPolicy(final Path root, final AccessControl access, final String database, final String resource,
      final Integer revision) {
    this.root = canonicalPath(root);
    this.access = access;
    this.database = database;
    this.resource = resource;
    this.revision = revision == null
        ? -1
        : revision;
  }

  static Path canonicalPath(final Path path) {
    final Path absolute = path.toAbsolutePath();
    try {
      if (Files.isSymbolicLink(absolute)) {
        try {
          return absolute.toRealPath();
        } catch (final NoSuchFileException missing) {
          return canonicalPath(absolute.resolveSibling(Files.readSymbolicLink(absolute)));
        }
      }
      if (Files.exists(absolute)) {
        return absolute.toRealPath();
      }
      final Path parent = absolute.getParent();
      return parent == null
          ? absolute
          : canonicalPath(parent).resolve(absolute.getFileName());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void checkDatabase(final Path databasePath, final boolean write) {
    databaseTarget(databasePath, write);
  }

  private Path databaseTarget(final Path databasePath, final boolean write) {
    final Path parent = canonicalPath(databasePath.toAbsolutePath().getParent());
    if (!root.equals(parent))
      throw new AccessControl.AccessDeniedException("Query database is outside the configured data directory");
    final Path target = access.databasePath(databasePath.getFileName().toString());
    if (revision > 0 && !database.equals(target.getFileName().toString()))
      throw new AccessControl.AccessDeniedException("Historical queries are scoped to the selected database/resource");
    if (write)
      checkWrite();
    return target;
  }

  @Override
  public void checkResource(final Path databasePath, final String resourceName, final boolean write) {
    final Path target = databaseTarget(databasePath, write);
    final String logicalResource = DerivedIndexMaintainer.isDerivedResourceName(resourceName)
        ? resourceName.substring(0, resourceName.length() - DerivedIndexMaintainer.SUFFIX.length())
        : resourceName;
    AccessControl.namedPath(target.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()), resourceName,
        "resource");
    access.checkAccess(target.getFileName().toString(), logicalResource);
    if (revision > 0 && !resource.equals(logicalResource)) {
      throw new AccessControl.AccessDeniedException("Historical queries are scoped to the selected database/resource");
    }
  }

  @Override
  public void checkWrite() {
    throw new AccessControl.AccessDeniedException("sirix_query is read-only; use the explicit write tools");
  }

  @Override
  public void checkRead(final ResourceSession<?, ?> session, final int requestedRevision) {
    final var config = session.getResourceConfig();
    checkResource(config.getResource().getParent().getParent(), config.getName(), false);
    if (revision > 0 && !DerivedIndexMaintainer.isDerivedResourceName(config.getName())
        && requestedRevision != revision) {
      throw new AccessControl.AccessDeniedException("Query requested a revision outside its historical context");
    }
  }

  @Override
  public int headRevision(final ResourceSession<?, ?> session, final int actualHead) {
    // Sessions are authorized on open and transaction acquisition. Keep this hot getter
    // allocation-free.
    return revision < 0 || DerivedIndexMaintainer.isDerivedResourceName(session.getResourceConfig().getName())
        ? actualHead
        : revision;
  }
}

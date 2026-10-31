package io.sirix.mcp;

import io.sirix.access.ResourceAccessScope;
import io.sirix.api.ResourceSession;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Enforces the actual query targets, including direct transaction functions and temporal axes. */
final class QueryAccessPolicy implements ResourceAccessScope.Policy {
  private final Path root;
  private final AccessControl access;
  private final String database;
  private final String resource;
  private final int revision;
  private final boolean readOnly;

  QueryAccessPolicy(final Path root, final AccessControl access, final String database, final String resource,
      final Integer revision, final boolean readOnly) {
    this.root = canonicalPath(root);
    this.access = access;
    this.database = database;
    this.resource = resource;
    this.revision = revision == null
        ? -1
        : revision;
    this.readOnly = readOnly;
  }

  private static Path canonicalPath(final Path path) {
    final Path absolute = path.toAbsolutePath();
    try {
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
    checkDatabaseTarget(canonicalPath(databasePath), write);
  }

  private void checkDatabaseTarget(final Path target, final boolean write) {
    if (!root.equals(target.getParent())) {
      throw new AccessControl.AccessDeniedException("Query database is outside the configured data directory");
    }
    final String databaseName = target.getFileName().toString();
    access.checkDatabaseAccess(databaseName);
    if (revision > 0 && !database.equals(databaseName)) {
      throw new AccessControl.AccessDeniedException("Historical queries are scoped to the selected database/resource");
    }
    if (write)
      checkWrite();
  }

  @Override
  public void checkResource(final Path databasePath, final String resourceName, final boolean write) {
    final Path target = canonicalPath(databasePath);
    checkDatabaseTarget(target, write);
    final String logicalResource = DerivedIndexMaintainer.isDerivedResourceName(resourceName)
        ? resourceName.substring(0, resourceName.length() - DerivedIndexMaintainer.SUFFIX.length())
        : resourceName;
    access.checkAccess(target.getFileName().toString(), logicalResource);
    if (revision > 0 && !resource.equals(logicalResource)) {
      throw new AccessControl.AccessDeniedException("Historical queries are scoped to the selected database/resource");
    }
  }

  @Override
  public void checkWrite() {
    if (readOnly) {
      throw new AccessControl.AccessDeniedException(
          "Write operations are disabled. Server is running in read-only mode.");
    }
    if (revision > 0) {
      throw new AccessControl.AccessDeniedException("Historical queries are read-only");
    }
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

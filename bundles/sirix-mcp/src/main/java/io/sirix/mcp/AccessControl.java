package io.sirix.mcp;

import io.sirix.access.ResourceAccessScope;
import io.sirix.access.DatabaseConfiguration;
import java.nio.file.Path;

/**
 * Enforces database/resource access control based on {@link McpServerConfig}.
 *
 * <p>
 * Every tool handler must call {@link #checkAccess} before opening a transaction. This is the
 * primary defense against prompt injection-driven data exfiltration: even if the agent is tricked
 * into reading a restricted resource, the access control layer rejects the request before any data
 * is touched.
 */
public final class AccessControl {

  private final McpServerConfig config;

  public AccessControl(McpServerConfig config) {
    this.config = config;
  }

  /**
   * Checks that the given database is accessible. Throws if denied.
   *
   * @throws AccessDeniedException if the database is in the deny list or not in the allow list
   */
  public void checkDatabaseAccess(String database) {
    if (!validDatabaseName(database)) {
      throw new IllegalArgumentException("Invalid database name");
    }
    if (!config.isDatabaseAllowed(database)) {
      throw new AccessDeniedException("Access denied to database: " + database);
    }
  }

  /**
   * Checks that the given database/resource pair is accessible. Throws if denied.
   *
   * @throws AccessDeniedException if access is denied at the database or resource level
   */
  public void checkAccess(String database, String resource) {
    checkDatabaseAccess(database);
    if (!config.isResourceAllowed(database, resource)) {
      throw new AccessDeniedException("Access denied to resource: " + database + "/" + resource);
    }
  }

  Path databasePath(final String database) {
    checkDatabaseAccess(database);
    final Path root = QueryAccessPolicy.canonicalPath(Path.of(config.databasePath()));
    final Path target = QueryAccessPolicy.canonicalPath(root.resolve(database));
    if (!root.equals(target.getParent()))
      throw new AccessDeniedException("Database is outside the configured data directory");
    checkDatabaseAccess(target.getFileName().toString());
    return target;
  }

  Path resourcePath(final String database, final String resource, final Path databasePath) {
    checkAccess(database, resource);
    if (!validDatabaseName(resource))
      throw new IllegalArgumentException("Invalid resource name");
    final Path resources = databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path target = QueryAccessPolicy.canonicalPath(resources.resolve(resource));
    if (!resources.equals(target.getParent()))
      throw new AccessDeniedException("Resource is outside the database resource directory");
    final String canonicalDatabase = databasePath.getFileName().toString();
    final String canonicalResource = target.getFileName().toString();
    checkAccess(database, canonicalResource);
    checkAccess(canonicalDatabase, resource);
    checkAccess(canonicalDatabase, canonicalResource);
    return target;
  }

  /** Check whether write operations are permitted. */
  public void checkWriteAccess() {
    if (config.readOnly()) {
      throw new AccessDeniedException("Write operations are disabled. Server is running in read-only mode.");
    }
    ResourceAccessScope.checkWrite();
  }

  private static boolean validDatabaseName(final String name) {
    if (name == null || name.isEmpty() || name.length() > 128)
      return false;
    for (int index = 0; index < name.length(); index++) {
      final char character = name.charAt(index);
      final boolean alphanumeric = (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
          || (character >= '0' && character <= '9');
      if (!alphanumeric && (index == 0 || (character != '.' && character != '_' && character != '-'))) {
        return false;
      }
    }
    return true;
  }

  public static final class AccessDeniedException extends RuntimeException {
    public AccessDeniedException(String message) {
      super(message);
    }
  }
}

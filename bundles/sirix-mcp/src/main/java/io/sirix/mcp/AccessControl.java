package io.sirix.mcp;

/**
 * Enforces database/resource access control based on {@link McpServerConfig}.
 *
 * <p>Every tool handler must call {@link #checkAccess} before opening a transaction.
 * This is the primary defense against prompt injection-driven data exfiltration:
 * even if the agent is tricked into reading a restricted resource, the access
 * control layer rejects the request before any data is touched.
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
    if (!config.isDatabaseAllowed(database)) {
      throw new AccessDeniedException("Access denied to database: " + database);
    }
    if (!config.isResourceAllowed(database, resource)) {
      throw new AccessDeniedException(
          "Access denied to resource: " + database + "/" + resource);
    }
  }

  /** Check whether write operations are permitted. */
  public void checkWriteAccess() {
    if (config.readOnly()) {
      throw new AccessDeniedException(
          "Write operations are disabled. Server is running in read-only mode.");
    }
  }

  public static final class AccessDeniedException extends RuntimeException {
    public AccessDeniedException(String message) {
      super(message);
    }
  }
}

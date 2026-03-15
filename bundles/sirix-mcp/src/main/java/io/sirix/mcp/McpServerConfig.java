package io.sirix.mcp;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the SirixDB MCP server.
 *
 * <p>Security defaults follow the principle of least privilege:
 * read-only by default. All databases at the configured path are accessible
 * unless restricted via {@code allowDatabases} or {@code denyDatabases}.
 */
public record McpServerConfig(
    String name,
    String version,
    String transport,
    String databasePath,
    boolean readOnly,
    List<String> allowDatabases,
    List<String> denyDatabases,
    Map<String, List<String>> allowResources,
    int maxResultSize,
    int maxStringValueLength,
    boolean sanitizeOutput,
    boolean confirmDestructive,
    boolean auditLog,
    String auditLogPath
) {

  private static final JsonMapper MAPPER = JsonMapper.builder()
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .build();

  /** Secure defaults. */
  public static McpServerConfig defaults(Path databasePath) {
    return new McpServerConfig(
        "sirixdb-mcp",
        "1.0.0",
        "stdio",
        databasePath.toAbsolutePath().toString(),
        true,   // read-only by default
        List.of(), // no allow list = all databases accessible (use denyDatabases to restrict)
        List.of(), // no deny list
        Map.of(),  // no resource-level restrictions
        100,       // max results
        4096,      // max string value length
        true,      // sanitize output
        true,      // confirm destructive operations
        true,      // audit logging on
        null       // audit log to stderr by default
    );
  }

  /** Returns a copy with the readOnly flag changed. */
  public McpServerConfig withReadOnly(boolean readOnly) {
    return new McpServerConfig(name, version, transport, databasePath, readOnly,
        allowDatabases, denyDatabases, allowResources, maxResultSize,
        maxStringValueLength, sanitizeOutput, confirmDestructive, auditLog, auditLogPath);
  }

  public static McpServerConfig load(Path configFile) throws IOException {
    return MAPPER.readValue(Files.readString(configFile), McpServerConfig.class);
  }

  /** Check if a database is accessible under this configuration. */
  public boolean isDatabaseAllowed(String database) {
    if (!denyDatabases.isEmpty() && denyDatabases.contains(database)) {
      return false;
    }
    if (!allowDatabases.isEmpty()) {
      return allowDatabases.contains(database);
    }
    return true;
  }

  /** Check if a resource within a database is accessible. */
  public boolean isResourceAllowed(String database, String resource) {
    if (!isDatabaseAllowed(database)) {
      return false;
    }
    if (allowResources.isEmpty()) {
      return true;
    }
    var allowed = allowResources.get(database);
    if (allowed == null) {
      return true;
    }
    return allowed.contains("*") || allowed.contains(resource);
  }
}

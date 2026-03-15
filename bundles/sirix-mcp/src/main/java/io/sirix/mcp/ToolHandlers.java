package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.server.McpSyncServerExchange;

import tools.jackson.databind.json.JsonMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * MCP tool handler implementations for SirixDB operations.
 *
 * <p>Every handler follows the same pattern:
 * <ol>
 *   <li>Extract parameters from the request</li>
 *   <li>Check access control (throws {@link AccessControl.AccessDeniedException} if denied)</li>
 *   <li>Execute the SirixDB operation</li>
 *   <li>Sanitize output</li>
 *   <li>Log to audit trail</li>
 *   <li>Return result</li>
 * </ol>
 *
 * <p>This class contains stubs that will be filled in as SirixDB integration is wired up.
 * The security infrastructure (access control, sanitization, audit) is fully functional.
 */
public final class ToolHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ToolHandlers.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  private final McpServerConfig config;
  private final AccessControl accessControl;
  private final OutputSanitizer sanitizer;
  private final SnapshotRegistry snapshots;
  private final AuditLog auditLog;

  public ToolHandlers(McpServerConfig config, AccessControl accessControl,
      OutputSanitizer sanitizer, SnapshotRegistry snapshots, AuditLog auditLog) {
    this.config = config;
    this.accessControl = accessControl;
    this.sanitizer = sanitizer;
    this.snapshots = snapshots;
    this.auditLog = auditLog;
  }

  // --- Read tools ---

  public CallToolResult listDatabases(McpSyncServerExchange exchange, CallToolRequest request) {
    return withAudit("sirix_list_databases", request.arguments(), () -> {
      // Stub: wire up Databases.openDatabase() to list available databases at config.databasePath()
      return textResult("[]");
    });
  }

  public CallToolResult listResources(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");

    return withAudit("sirix_list_resources", args, () -> {
      accessControl.checkDatabaseAccess(database);
      // Stub: wire up Database.listResources()
      return textResult("[]");
    });
  }

  public CallToolResult resourceInfo(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_resource_info", args, () -> {
      accessControl.checkAccess(database, resource);
      // Stub: wire up ResourceSession.getMostRecentRevisionNumber(), getHistory(), etc.
      return textResult("{}");
    });
  }

  public CallToolResult query(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var queryStr = requireString(args, "query");
    var database = requireString(args, "database");
    var resource = optionalString(args, "resource");

    return withAudit("sirix_query", args, () -> {
      if (resource != null) {
        accessControl.checkAccess(database, resource);
      } else {
        accessControl.checkDatabaseAccess(database);
      }

      // Stub: wire up SirixQueryContext with read-only CommitStrategy.EXPLICIT
      // Resolve revision from: args.revision, args.snapshot (via snapshotRegistry), or args.timestamp
      var result = "[]"; // placeholder

      // Check for prompt injection in results
      var warning = sanitizer.detectInjection(result);
      var sanitized = sanitizer.sanitize(result);

      if (warning != null) {
        return textResult(warning + "\n\n" + sanitized);
      }
      return textResult(sanitized);
    });
  }

  public CallToolResult history(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_history", args, () -> {
      accessControl.checkAccess(database, resource);
      // Stub: wire up ResourceSession.getHistory(count)
      return textResult("[]");
    });
  }

  public CallToolResult diff(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_diff", args, () -> {
      accessControl.checkAccess(database, resource);

      // Resolve from/to revisions (support both revision numbers and snapshot names)
      // Stub: wire up diff infrastructure
      return textResult("{}");
    });
  }

  public CallToolResult listSnapshots(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_list_snapshots", args, () -> {
      accessControl.checkAccess(database, resource);
      var snapshotMap = snapshots.list(database, resource);
      return textResult(MAPPER.writeValueAsString(snapshotMap));
    });
  }

  public CallToolResult createSnapshot(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");
    var name = requireString(args, "name");
    var revision = optionalInt(args, "revision");

    return withAudit("sirix_create_snapshot", args, () -> {
      accessControl.checkAccess(database, resource);

      int rev;
      if (revision != null) {
        rev = revision;
      } else {
        // Stub: get latest revision from ResourceSession.getMostRecentRevisionNumber()
        rev = 0; // placeholder
      }

      snapshots.create(database, resource, name, rev);
      return textResult("Snapshot '" + name + "' created at revision " + rev);
    });
  }

  // --- Write tools ---

  public CallToolResult insert(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_insert", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      // Stub: wire up JsonNodeTrx.insertSubtreeAsFirstChild()
      return textResult("Insert completed");
    });
  }

  public CallToolResult update(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_update", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      // Stub: wire up JsonNodeTrx.setStringValue() / setNumberValue() etc.
      return textResult("Update completed");
    });
  }

  public CallToolResult delete(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_delete", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      // Stub: wire up JsonNodeTrx.remove()
      return textResult("Delete completed");
    });
  }

  public CallToolResult revert(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_revert", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      // Stub: wire up JsonNodeTrx.revertTo(revision)
      return textResult("Revert completed");
    });
  }

  public CallToolResult deleteSnapshot(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");
    var name = requireString(args, "name");

    return withAudit("sirix_delete_snapshot", args, () -> {
      accessControl.checkAccess(database, resource);
      boolean existed = snapshots.delete(database, resource, name);
      return textResult(existed
          ? "Snapshot '" + name + "' deleted"
          : "Snapshot '" + name + "' not found");
    });
  }

  // --- Helpers ---

  @FunctionalInterface
  private interface ToolAction {
    CallToolResult execute() throws Exception;
  }

  private CallToolResult withAudit(String toolName, Map<String, Object> params, ToolAction action) {
    try {
      var result = action.execute();
      auditLog.log(toolName, params, "success", null);
      return result;
    } catch (AccessControl.AccessDeniedException e) {
      auditLog.log(toolName, params, "denied", e.getMessage());
      return errorResult(e.getMessage());
    } catch (Exception e) {
      auditLog.log(toolName, params, "error", e.getMessage());
      LOG.error("Tool {} failed", toolName, e);
      return errorResult("Internal error: " + e.getMessage());
    }
  }

  private static CallToolResult textResult(String text) {
    return new CallToolResult(List.of(new TextContent(text)), false, null, null);
  }

  private static CallToolResult errorResult(String message) {
    return new CallToolResult(List.of(new TextContent("ERROR: " + message)), true, null, null);
  }

  private static String requireString(Map<String, Object> args, String key) {
    var value = args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + key);
    }
    return value.toString();
  }

  private static String optionalString(Map<String, Object> args, String key) {
    var value = args.get(key);
    return value != null ? value.toString() : null;
  }

  private static Integer optionalInt(Map<String, Object> args, String key) {
    var value = args.get(key);
    if (value == null) return null;
    if (value instanceof Number n) return n.intValue();
    return Integer.parseInt(value.toString());
  }
}

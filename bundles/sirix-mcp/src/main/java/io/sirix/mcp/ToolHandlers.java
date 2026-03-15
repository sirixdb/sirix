package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.server.McpSyncServerExchange;

import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.shredder.JsonShredder;

import io.brackit.query.Query;

import tools.jackson.databind.json.JsonMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 */
public final class ToolHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ToolHandlers.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  private static final int DEFAULT_HISTORY_COUNT = 20;

  private final McpServerConfig config;
  private final AccessControl accessControl;
  private final OutputSanitizer sanitizer;
  private final SnapshotRegistry snapshots;
  private final AuditLog auditLog;
  private final Path databasePath;

  public ToolHandlers(McpServerConfig config, AccessControl accessControl,
      OutputSanitizer sanitizer, SnapshotRegistry snapshots, AuditLog auditLog) {
    this.config = config;
    this.accessControl = accessControl;
    this.sanitizer = sanitizer;
    this.snapshots = snapshots;
    this.auditLog = auditLog;
    this.databasePath = Path.of(config.databasePath());
  }

  // --- Read tools ---

  public CallToolResult listDatabases(McpSyncServerExchange exchange, CallToolRequest request) {
    return withAudit("sirix_list_databases", request.arguments(), () -> {
      final var dbNames = new ArrayList<String>();

      if (Files.isDirectory(databasePath)) {
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(databasePath)) {
          for (final Path entry : stream) {
            if (Databases.existsDatabase(entry)) {
              final String name = entry.getFileName().toString();
              if (config.isDatabaseAllowed(name)) {
                dbNames.add(name);
              }
            }
          }
        }
      }

      return textResult(MAPPER.writeValueAsString(dbNames));
    });
  }

  public CallToolResult listResources(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");

    return withAudit("sirix_list_resources", args, () -> {
      accessControl.checkDatabaseAccess(database);

      final var resourceNames = new ArrayList<String>();
      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        final List<Path> resources = db.listResources();
        for (final Path resourcePath : resources) {
          resourceNames.add(resourcePath.getFileName().toString());
        }
      }

      return textResult(MAPPER.writeValueAsString(resourceNames));
    });
  }

  public CallToolResult resourceInfo(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_resource_info", args, () -> {
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource)) {

        final int latestRevision = session.getMostRecentRevisionNumber();
        final var info = new LinkedHashMap<String, Object>();
        info.put("database", database);
        info.put("resource", resource);
        info.put("latestRevision", latestRevision);

        final List<RevisionInfo> history = session.getHistory(latestRevision, 1);
        if (!history.isEmpty()) {
          info.put("lastModified", history.getFirst().getRevisionTimestamp().toString());
          info.put("created", history.getLast().getRevisionTimestamp().toString());
        }

        return textResult(MAPPER.writeValueAsString(info));
      }
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

      final String result;

      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
              .location(databasePath).build();
           final SirixQueryContext ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(
              store, SirixQueryContext.CommitStrategy.EXPLICIT);
           final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
           final ByteArrayOutputStream out = new ByteArrayOutputStream();
           final PrintWriter writer = new PrintWriter(out)) {
        new Query(chain, queryStr).serialize(ctx, writer);
        writer.flush();
        result = out.toString();
      }

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

      final Integer count = optionalInt(args, "count");
      final int limit = count != null ? count : DEFAULT_HISTORY_COUNT;
      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource)) {

        final List<RevisionInfo> revisions = session.getHistory(limit);
        final var entries = new ArrayList<Map<String, Object>>(revisions.size());

        for (final RevisionInfo rev : revisions) {
          final var entry = new LinkedHashMap<String, Object>();
          entry.put("revision", rev.getRevision());
          entry.put("timestamp", rev.getRevisionTimestamp().toString());
          rev.getCommitMessage().ifPresent(msg -> entry.put("message", msg));
          entry.put("user", rev.getUser().getName());
          entries.add(entry);
        }

        return textResult(MAPPER.writeValueAsString(entries));
      }
    });
  }

  public CallToolResult diff(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");

    return withAudit("sirix_diff", args, () -> {
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource)) {

        final int fromRev = resolveRevision(args, "from_revision", "from_snapshot",
            database, resource, session.getMostRecentRevisionNumber() - 1);
        final int toRev = resolveRevision(args, "to_revision", "to_snapshot",
            database, resource, session.getMostRecentRevisionNumber());

        if (fromRev < 1 || toRev < 1 || fromRev > session.getMostRecentRevisionNumber()
            || toRev > session.getMostRecentRevisionNumber()) {
          throw new IllegalArgumentException(
              "Invalid revision range: " + fromRev + ".." + toRev
                  + " (latest: " + session.getMostRecentRevisionNumber() + ")");
        }

        final var jsonDiff = new BasicJsonDiff(database);
        final String diffResult = jsonDiff.generateDiff(session, fromRev, toRev);

        var sanitized = sanitizer.sanitize(diffResult);
        return textResult(sanitized);
      }
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

      final int rev;
      if (revision != null) {
        rev = revision;
      } else {
        final Path dbPath = databasePath.resolve(database);
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
             final JsonResourceSession session = db.beginResourceSession(resource)) {
          rev = session.getMostRecentRevisionNumber();
        }
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
    var data = requireString(args, "data");
    var message = optionalString(args, "message");

    return withAudit("sirix_insert", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        wtx.moveToDocumentRoot();
        if (wtx.moveToFirstChild()) {
          wtx.insertSubtreeAsRightSibling(
              JsonShredder.createStringReader(data), JsonNodeTrx.Commit.NO);
        } else {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(data), JsonNodeTrx.Commit.NO);
        }

        wtx.commit(message);

        return textResult("Insert completed (revision " + session.getMostRecentRevisionNumber() + ")");
      }
    });
  }

  public CallToolResult update(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");
    var nodeKey = requireLong(args, "nodeKey");
    var value = requireString(args, "value");
    var message = optionalString(args, "message");

    return withAudit("sirix_update", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        if (!wtx.moveTo(nodeKey)) {
          throw new IllegalArgumentException("Node not found: " + nodeKey);
        }

        if (wtx.isStringValue()) {
          wtx.setStringValue(value);
        } else if (wtx.isNumberValue()) {
          wtx.setNumberValue(Double.parseDouble(value));
        } else if (wtx.isBooleanValue()) {
          wtx.setBooleanValue(Boolean.parseBoolean(value));
        } else if (wtx.isObjectKey()) {
          wtx.setObjectKeyName(value);
        } else {
          throw new IllegalArgumentException(
              "Node " + nodeKey + " is not a value or object key node");
        }

        wtx.commit(message);

        return textResult("Update completed (revision " + session.getMostRecentRevisionNumber() + ")");
      }
    });
  }

  public CallToolResult delete(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");
    var nodeKey = requireLong(args, "nodeKey");
    var message = optionalString(args, "message");

    return withAudit("sirix_delete", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        if (!wtx.moveTo(nodeKey)) {
          throw new IllegalArgumentException("Node not found: " + nodeKey);
        }

        wtx.remove();
        wtx.commit(message);

        return textResult("Delete completed (revision " + session.getMostRecentRevisionNumber() + ")");
      }
    });
  }

  public CallToolResult revert(McpSyncServerExchange exchange, CallToolRequest request) {
    var args = request.arguments();
    var database = requireString(args, "database");
    var resource = requireString(args, "resource");
    var message = optionalString(args, "message");

    return withAudit("sirix_revert", args, () -> {
      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);

      final Path dbPath = databasePath.resolve(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        final int targetRevision = resolveRevision(args, "revision", "snapshot",
            database, resource, -1);

        if (targetRevision < 1 || targetRevision > session.getMostRecentRevisionNumber()) {
          throw new IllegalArgumentException(
              "Invalid revision: " + targetRevision
                  + " (latest: " + session.getMostRecentRevisionNumber() + ")");
        }

        wtx.revertTo(targetRevision);

        if (message != null) {
          wtx.commit(message);
        } else {
          wtx.commit("Reverted to revision " + targetRevision);
        }

        return textResult("Reverted to revision " + targetRevision
            + " (new revision " + session.getMostRecentRevisionNumber() + ")");
      }
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

  /**
   * Resolves a revision number from either a direct revision parameter or a snapshot name.
   *
   * @param args the request arguments
   * @param revisionKey the key for the revision number parameter
   * @param snapshotKey the key for the snapshot name parameter
   * @param database the database name (for snapshot lookup)
   * @param resource the resource name (for snapshot lookup)
   * @param defaultRevision fallback if neither is specified (-1 means required)
   * @return the resolved revision number
   */
  private int resolveRevision(Map<String, Object> args, String revisionKey, String snapshotKey,
      String database, String resource, int defaultRevision) {
    final var revisionArg = optionalInt(args, revisionKey);
    if (revisionArg != null) {
      return revisionArg;
    }

    final var snapshotName = optionalString(args, snapshotKey);
    if (snapshotName != null) {
      final var rev = snapshots.resolve(database, resource, snapshotName);
      if (rev == null) {
        throw new IllegalArgumentException("Snapshot not found: " + snapshotName);
      }
      return rev;
    }

    if (defaultRevision < 0) {
      throw new IllegalArgumentException(
          "Either '" + revisionKey + "' or '" + snapshotKey + "' must be specified");
    }
    return defaultRevision;
  }

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
      final String detail = e.getMessage() != null
          ? e.getMessage()
          : e.getClass().getSimpleName();
      auditLog.log(toolName, params, "error", detail);
      LOG.error("Tool {} failed", toolName, e);
      return errorResult("Internal error: " + detail);
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

  private static long requireLong(Map<String, Object> args, String key) {
    var value = args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + key);
    }
    if (value instanceof Number n) return n.longValue();
    return Long.parseLong(value.toString());
  }
}

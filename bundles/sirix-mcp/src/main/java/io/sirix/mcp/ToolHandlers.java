package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.server.McpSyncServerExchange;

import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceAccessScope;
import io.sirix.exception.SirixRevisionPrunedException;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;

import io.brackit.query.Query;

import tools.jackson.databind.json.JsonMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * MCP tool handler implementations for SirixDB operations.
 *
 * <p>
 * Every handler follows the same pattern:
 * <ol>
 * <li>Extract parameters from the request</li>
 * <li>Check access control (throws {@link AccessControl.AccessDeniedException} if denied)</li>
 * <li>Execute the SirixDB operation</li>
 * <li>Sanitize output</li>
 * <li>Log to audit trail</li>
 * <li>Return result</li>
 * </ol>
 */
public final class ToolHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ToolHandlers.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  private static final int DEFAULT_HISTORY_COUNT = 20;
  private static final Pattern VALID_NAME = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$");

  private final McpServerConfig config;
  private final AccessControl accessControl;
  private final OutputSanitizer sanitizer;
  private final SnapshotRegistry snapshots;
  private final AuditLog auditLog;
  private final Path databasePath;

  public ToolHandlers(McpServerConfig config, AccessControl accessControl, OutputSanitizer sanitizer,
      SnapshotRegistry snapshots, AuditLog auditLog) {
    this.config = config;
    this.accessControl = accessControl;
    this.sanitizer = sanitizer;
    this.snapshots = snapshots;
    this.auditLog = auditLog;
    this.databasePath = Path.of(config.databasePath()).toAbsolutePath().normalize();
  }

  // --- Read tools ---

  public CallToolResult listDatabases(McpSyncServerExchange exchange, CallToolRequest request) {
    return withAudit("sirix_list_databases", request.arguments(), () -> {
      final var dbNames = new ArrayList<String>();

      if (Files.isDirectory(databasePath)) {
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(databasePath)) {
          for (final Path entry : stream) {
            if (Databases.existsDatabase(entry) && Databases.getDatabaseType(entry) == DatabaseType.JSON) {
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
    final var args = request.arguments();

    return withAudit("sirix_list_resources", args, () -> {
      final var database = requireString(args, "database");

      accessControl.checkDatabaseAccess(database);

      final var resourceNames = new ArrayList<String>();
      final Path dbPath = safeDatabasePath(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        final List<Path> resources = db.listResources();
        for (final Path resourcePath : resources) {
          final String name = resourcePath.getFileName().toString();
          if (config.isResourceAllowed(database, name))
            resourceNames.add(name);
        }
      }

      return textResult(MAPPER.writeValueAsString(resourceNames));
    });
  }

  public CallToolResult resourceInfo(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_resource_info", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");

      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(resource)) {

        final int latestRevision = session.getMostRecentRevisionNumber();
        final var info = new LinkedHashMap<String, Object>();
        info.put("database", database);
        info.put("resource", resource);
        info.put("latestRevision", latestRevision);

        if (latestRevision > 1) {
          final List<RevisionInfo> history = session.getHistory(latestRevision, 1);
          if (!history.isEmpty()) {
            info.put("lastModified", history.getFirst().getRevisionTimestamp().toString());
            info.put("created", history.getLast().getRevisionTimestamp().toString());
          }
        } else {
          final List<RevisionInfo> history = session.getHistory(1);
          if (!history.isEmpty()) {
            info.put("created", history.getFirst().getRevisionTimestamp().toString());
            info.put("lastModified", history.getFirst().getRevisionTimestamp().toString());
          }
        }

        return textResult(MAPPER.writeValueAsString(info));
      }
    });
  }

  public CallToolResult query(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_query", args, () -> {
      final var queryStr = requireString(args, "query");
      final var database = requireString(args, "database");
      final var resource = optionalString(args, "resource");

      if (resource != null) {
        accessControl.checkAccess(database, resource);
      } else {
        accessControl.checkDatabaseAccess(database);
      }

      safeDatabasePath(database);
      if (resource != null)
        validateResourceName(resource);
      final Integer revision = queryRevision(args, database, resource);
      final int offset = nonNegativeOffset(args);
      final boolean completeEvaluation = !config.readOnly() && revision == null;
      if (completeEvaluation && offset != 0) {
        throw new IllegalArgumentException("Continuation requires a historical selector or a read-only server");
      }
      final var policy =
          new QueryAccessPolicy(databasePath, accessControl, database, resource, revision, config.readOnly());
      return ResourceAccessScope.where(policy).call(() -> {
        final BasicJsonDBStore baseStore = BasicJsonDBStore.newBuilder().location(databasePath).build();
        final var store = new GuardedJsonDBStore(baseStore, accessControl);
        try (
            final SirixQueryContext ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(store,
                SirixQueryContext.CommitStrategy.EXPLICIT);
            final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
          if (resource != null) {
            ctx.setContextItem(store.lookup(database)
                                    .getDocument(resource, revision == null
                                        ? -1
                                        : revision));
          }
          final var sequence = new Query(chain, queryStr).execute(ctx);
          final var result = BoundedQueryResult.serialize(sequence, offset, config, sanitizer, completeEvaluation);
          if (revision != null)
            result.put("revision", revision);
          return structuredResult(result, false);
        }
      });
    });
  }

  public CallToolResult history(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_history", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");

      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Integer count = optionalInt(args, "count");
      final int limit = count != null
          ? count
          : DEFAULT_HISTORY_COUNT;
      final Path dbPath = safeDatabasePath(database);

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
    final var args = request.arguments();

    return withAudit("sirix_diff", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");

      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(resource)) {

        final int latest = session.getMostRecentRevisionNumber();
        if (latest < 2 && !args.containsKey("from_revision") && !args.containsKey("to_revision")
            && !args.containsKey("from_snapshot") && !args.containsKey("to_snapshot")) {
          return textResult("No diff available: only " + latest + (latest == 1
              ? " revision exists"
              : " revisions exist"));
        }

        final int fromRev = resolveRevision(args, "from_revision", "from_snapshot", database, resource, latest - 1);
        final int toRev = resolveRevision(args, "to_revision", "to_snapshot", database, resource, latest);

        if (fromRev >= toRev) {
          throw new IllegalArgumentException("from_revision must be less than to_revision");
        }
        validateRevision(session, fromRev);
        validateRevision(session, toRev);
        final var result = BoundedDiff.generate(database, session, fromRev, toRev, nonNegativeOffset(args), config);
        final String warning = sanitizer.detectInjection(MAPPER.writeValueAsString(result));
        if (warning != null)
          result.put("warning", warning);
        return new CallToolResult(List.of(new TextContent(sanitizer.sanitize(MAPPER.writeValueAsString(result)))),
            false, result, null);
      }
    });
  }

  public CallToolResult listSnapshots(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_list_snapshots", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");

      accessControl.checkAccess(database, resource);
      final var snapshotMap = snapshots.list(database, resource);
      return textResult(MAPPER.writeValueAsString(snapshotMap));
    });
  }

  public CallToolResult createSnapshot(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_create_snapshot", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var name = requireString(args, "name");
      final var revision = optionalInt(args, "revision");

      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final int rev;
      final Path dbPath = safeDatabasePath(database);
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(resource)) {
        rev = revision != null
            ? revision
            : session.getMostRecentRevisionNumber();
        validateRevision(session, rev);
      }

      snapshots.create(database, resource, name, rev);
      return textResult("Snapshot '" + name + "' created at revision " + rev);
    });
  }

  // --- Write tools ---

  public CallToolResult insert(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_insert", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var data = requireString(args, "data");
      final var message = optionalString(args, "message");

      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(resource);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {

        wtx.moveToDocumentRoot();
        if (wtx.moveToFirstChild()) {
          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(data), JsonNodeTrx.Commit.NO);
        } else {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(data), JsonNodeTrx.Commit.NO);
        }

        wtx.commit(message);

        return textResult("Insert completed (revision " + session.getMostRecentRevisionNumber() + ")");
      }
    });
  }

  public CallToolResult update(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_update", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var nodeKey = requireLong(args, "nodeKey");
      final var value = requireString(args, "value");
      final var message = optionalString(args, "message");

      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

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
          throw new IllegalArgumentException("Node " + nodeKey + " is not a value or object key node");
        }

        wtx.commit(message);

        return textResult("Update completed (revision " + session.getMostRecentRevisionNumber() + ")");
      }
    });
  }

  public CallToolResult delete(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_delete", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var nodeKey = requireLong(args, "nodeKey");
      final var message = optionalString(args, "message");

      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

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
    final var args = request.arguments();

    return withAudit("sirix_revert", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var message = optionalString(args, "message");

      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      validateResourceName(resource);

      final Path dbPath = safeDatabasePath(database);

      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(resource);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {

        final int latest = session.getMostRecentRevisionNumber();
        final int targetRevision = resolveRevision(args, "revision", "snapshot", database, resource, -1);

        final Integer expectedHead = optionalInt(args, "expected_head");
        if (expectedHead != null && expectedHead != latest) {
          return structuredResult(Map.of("code", "stale_head", "expectedHead", expectedHead, "previousHead", latest,
              "message", "Resource head changed; inspect it before retrying"), true);
        }
        validateRevision(session, targetRevision);
        if (targetRevision == latest) {
          return structuredResult(Map.of("revision", latest, "previousHead", latest, "message",
              "Already at revision " + latest + ", nothing to revert"), false);
        }

        wtx.revertTo(targetRevision);

        if (message != null) {
          wtx.commit(message);
        } else {
          wtx.commit("Reverted to revision " + targetRevision);
        }

        return structuredResult(Map.of("revision", session.getMostRecentRevisionNumber(), "previousHead", latest,
            "targetRevision", targetRevision, "message", "Reverted to revision " + targetRevision), false);
      }
    });
  }

  public CallToolResult deleteSnapshot(McpSyncServerExchange exchange, CallToolRequest request) {
    final var args = request.arguments();

    return withAudit("sirix_delete_snapshot", args, () -> {
      final var database = requireString(args, "database");
      final var resource = requireString(args, "resource");
      final var name = requireString(args, "name");

      accessControl.checkWriteAccess();
      accessControl.checkAccess(database, resource);
      boolean existed = snapshots.delete(database, resource, name);
      return textResult(existed
          ? "Snapshot '" + name + "' deleted"
          : "Snapshot '" + name + "' not found");
    });
  }

  private Path safeDatabasePath(String database) {
    validateDatabaseName(database);
    final Path resolved = databasePath.resolve(database).normalize();
    if (!resolved.startsWith(databasePath)) {
      throw new IllegalArgumentException("Invalid database name");
    }
    return resolved;
  }

  private static void validateDatabaseName(String name) {
    if (!VALID_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid database name: must be 1-128 alphanumeric characters, hyphens, underscores, or dots");
    }
  }

  private static void validateResourceName(String name) {
    if (!VALID_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid resource name: must be 1-128 alphanumeric characters, hyphens, underscores, or dots");
    }
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
  private int resolveRevision(Map<String, Object> args, String revisionKey, String snapshotKey, String database,
      String resource, int defaultRevision) {
    final var revisionArg = optionalInt(args, revisionKey);
    final var snapshotName = optionalString(args, snapshotKey);
    if (revisionArg != null && snapshotName != null) {
      throw new IllegalArgumentException("Specify exactly one of '" + revisionKey + "' and '" + snapshotKey + "'");
    }
    if (revisionArg != null)
      return revisionArg;

    if (snapshotName != null) {
      final var rev = snapshots.resolve(database, resource, snapshotName);
      if (rev == null) {
        throw new IllegalArgumentException("Snapshot not found: " + snapshotName);
      }
      return rev;
    }

    if (defaultRevision < 0) {
      throw new IllegalArgumentException("Either '" + revisionKey + "' or '" + snapshotKey + "' must be specified");
    }
    return defaultRevision;
  }

  private Integer queryRevision(final Map<String, Object> args, final String database, final String resource) {
    final int selectors = (args.containsKey("revision")
        ? 1
        : 0)
        + (args.containsKey("snapshot")
            ? 1
            : 0)
        + (args.containsKey("timestamp")
            ? 1
            : 0);
    if (selectors > 1)
      throw new IllegalArgumentException("Specify exactly one of revision, timestamp, snapshot");
    if (selectors == 0)
      return null;
    if (resource == null)
      throw new IllegalArgumentException("A resource is required for a historical selector");
    try (final var db = Databases.openJsonDatabase(safeDatabasePath(database));
        final var session = db.beginResourceSession(resource)) {
      final int revision;
      if (args.containsKey("timestamp")) {
        final Instant timestamp = Instant.parse(requireString(args, "timestamp"));
        revision = session.getRevisionNumber(timestamp);
      } else {
        revision = resolveRevision(args, "revision", "snapshot", database, resource, -1);
      }
      validateRevision(session, revision);
      return revision;
    }
  }

  private static void validateRevision(final JsonResourceSession session, final int revision) {
    if (revision < 1 || revision > session.getMostRecentRevisionNumber()) {
      throw new IllegalArgumentException("Invalid revision: " + revision);
    }
    // The public reader API validates retention and the selected revision root.
    try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
      reader.getRevisionNumber();
    }
  }

  private static int nonNegativeOffset(final Map<String, Object> args) {
    final Integer offset = optionalInt(args, "offset");
    if (offset != null && offset < 0)
      throw new IllegalArgumentException("offset must not be negative");
    return offset == null
        ? 0
        : offset;
  }

  private static CallToolResult structuredResult(final Map<String, Object> result, final boolean error) {
    return new CallToolResult(List.of(new TextContent(MAPPER.writeValueAsString(result))), error, result, null);
  }

  @FunctionalInterface
  private interface ToolAction {
    CallToolResult execute() throws Exception;
  }

  private CallToolResult withAudit(String toolName, Map<String, Object> params, ToolAction action) {
    try {
      final var result = action.execute();
      auditLog.log(toolName, params, Boolean.TRUE.equals(result.isError())
          ? "error"
          : "success", null);
      return result;
    } catch (SirixRevisionPrunedException e) {
      auditLog.log(toolName, params, "error", e.getMessage());
      return structuredResult(Map.of("code", "revision_pruned", "revision", e.getRevision(), "timestamp",
          e.getTimestamp().toString(), "message", e.getMessage()), true);
    } catch (AccessControl.AccessDeniedException e) {
      auditLog.log(toolName, params, "denied", e.getMessage());
      return errorResult(e.getMessage());
    } catch (IllegalArgumentException e) {
      auditLog.log(toolName, params, "error", e.getMessage());
      return errorResult(e.getMessage());
    } catch (Exception e) {
      final String detail = e.getMessage() != null
          ? e.getMessage()
          : e.getClass().getSimpleName();
      auditLog.log(toolName, params, "error", detail);
      LOG.error("Tool {} failed", toolName, e);
      return errorResult("Operation failed. Check server logs for details.");
    }
  }

  private static CallToolResult textResult(String text) {
    return new CallToolResult(List.of(new TextContent(text)), false, null, null);
  }

  private static CallToolResult errorResult(String message) {
    return new CallToolResult(List.of(new TextContent("ERROR: " + message)), true, null, null);
  }

  private static String requireString(Map<String, Object> args, String key) {
    final var value = args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + key);
    }
    if (!(value instanceof String text))
      throw new IllegalArgumentException("Parameter '" + key + "' must be a string");
    return text;
  }

  private static String optionalString(Map<String, Object> args, String key) {
    final var value = args.get(key);
    if (value == null)
      return null;
    if (!(value instanceof String text))
      throw new IllegalArgumentException("Parameter '" + key + "' must be a string");
    return text;
  }

  private static Integer optionalInt(Map<String, Object> args, String key) {
    final var value = args.get(key);
    if (value == null)
      return null;
    try {
      return Integer.valueOf(value.toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Parameter '" + key + "' must be a 32-bit integer", e);
    }
  }

  private static long requireLong(Map<String, Object> args, String key) {
    final var value = args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + key);
    }
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Parameter '" + key + "' must be a 64-bit integer", e);
    }
  }
}

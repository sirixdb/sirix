package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ToolHandlersIntegrationTest {

  private static final String DB_NAME = "testdb";
  private static final String RESOURCE_NAME = "testres";
  private static final String SAMPLE_JSON = "{\"name\":\"Alice\",\"age\":30,\"active\":true}";

  @TempDir Path tempDir;

  private ToolHandlers handlers;
  private ToolHandlers readWriteHandlers;

  @BeforeEach
  void setUp() throws IOException {
    final Path dbPath = tempDir.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final var db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
      try (final var session = db.beginResourceSession(RESOURCE_NAME);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(SAMPLE_JSON));
      }
    }
    final var roConfig = McpServerConfig.defaults(tempDir);
    final var roAc = new AccessControl(roConfig);
    final var sanitizer = new OutputSanitizer(roConfig);
    final var snapshots = new SnapshotRegistry(tempDir);
    final var audit = AuditLog.create(roConfig);
    handlers = new ToolHandlers(roConfig, roAc, sanitizer, snapshots, audit);

    final var rwConfig = McpServerConfig.defaults(tempDir).withReadOnly(false);
    final var rwAc = new AccessControl(rwConfig);
    readWriteHandlers = new ToolHandlers(rwConfig, rwAc, sanitizer, snapshots, audit);
  }

  private static ToolHandlers createHandlers(Path basePath, McpServerConfig config) {
    try {
      return new ToolHandlers(config, new AccessControl(config),
          new OutputSanitizer(config), new SnapshotRegistry(basePath), AuditLog.create(config));
    } catch (IOException e) { throw new RuntimeException(e); }
  }

  private static CallToolRequest request(Map<String, Object> args) {
    return new CallToolRequest("test", args);
  }

  private static String resultText(CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }

  @Nested class ListDatabases {
    @Test void returnsExistingDatabases() {
      final var result = handlers.listDatabases(null, request(Map.of()));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains(DB_NAME);
    }
    @Test void returnsEmptyForEmptyPath(@TempDir Path emptyDir) {
      final var h = createHandlers(emptyDir, McpServerConfig.defaults(emptyDir));
      final var result = h.listDatabases(null, request(Map.of()));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).isEqualTo("[]");
    }
    @Test void respectsDenyList() {
      final var config = new McpServerConfig("test", "1.0.0", "stdio", tempDir.toString(),
          true, List.of(), List.of(DB_NAME), Map.of(), 100, 4096, false, false, false, null);
      final var h = createHandlers(tempDir, config);
      final var result = h.listDatabases(null, request(Map.of()));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).isEqualTo("[]");
    }
  }

  @Nested class ListResources {
    @Test void returnsResourceNames() {
      final var result = handlers.listResources(null, request(Map.of("database", DB_NAME)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains(RESOURCE_NAME);
    }
    @Test void rejectsInvalidDatabaseName() {
      final var result = handlers.listResources(null, request(Map.of("database", "../../etc")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Invalid database name");
    }
    @Test void rejectsAccessDeniedDatabase() {
      final var config = new McpServerConfig("test", "1.0.0", "stdio", tempDir.toString(),
          true, List.of(), List.of(DB_NAME), Map.of(), 100, 4096, false, false, false, null);
      final var h = createHandlers(tempDir, config);
      final var result = h.listResources(null, request(Map.of("database", DB_NAME)));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Access denied");
    }
  }

  @Nested class ResourceInfo {
    @Test void returnsMetadata() {
      final var result = handlers.resourceInfo(null,
          request(Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
      assertThat(result.isError()).isFalse();
      final String text = resultText(result);
      assertThat(text).contains("\"latestRevision\"");
      assertThat(text).contains("\"created\"");
      assertThat(text).contains("\"lastModified\"");
    }
    @Test void handlesNonexistentResource() {
      final var result = handlers.resourceInfo(null,
          request(Map.of("database", DB_NAME, "resource", "nonexistent")));
      assertThat(result.isError()).isTrue();
    }
  }

  @Nested class QueryTool {
    @Test void executesJsoniqQuery() {
      final var result = handlers.query(null, request(Map.of(
          "query", "jn:doc('" + DB_NAME + "','" + RESOURCE_NAME + "')(\"name\")",
          "database", DB_NAME)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Alice");
    }
    @Test void queryRespectsDatabaseAccessControl() {
      final var config = new McpServerConfig("test", "1.0.0", "stdio", tempDir.toString(),
          true, List.of(), List.of(DB_NAME), Map.of(), 100, 4096, false, false, false, null);
      final var h = createHandlers(tempDir, config);
      final var result = h.query(null, request(Map.of("query", "1+1", "database", DB_NAME)));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Access denied");
    }
  }

  @Nested class History {
    @Test void returnsRevisionHistory() {
      final var result = handlers.history(null,
          request(Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("\"revision\"").contains("\"timestamp\"");
    }
    @Test void respectsCountParameter() {
      final Path dbPath = tempDir.resolve(DB_NAME);
      try (final var db = Databases.openJsonDatabase(dbPath);
           final var session = db.beginResourceSession(RESOURCE_NAME);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveTo(3);
        wtx.setStringValue("Bob");
        wtx.commit();
      }
      final var result = handlers.history(null,
          request(Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "count", 1)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result).split("\"revision\"").length - 1).isEqualTo(1);
    }
  }

  @Nested class Diff {
    @Test void returnsDiffBetweenRevisions() {
      final Path dbPath = tempDir.resolve(DB_NAME);
      try (final var db = Databases.openJsonDatabase(dbPath);
           final var session = db.beginResourceSession(RESOURCE_NAME);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveTo(3);
        wtx.setStringValue("Bob");
        wtx.commit();
      }
      final var result = handlers.diff(null, request(Map.of("database", DB_NAME,
          "resource", RESOURCE_NAME, "from_revision", 1, "to_revision", 2)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).isNotEmpty();
    }
    @Test void returnsMessageForSingleRevision(@TempDir Path singleRevDir) {
      final Path dbPath = singleRevDir.resolve("singledb");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
      try (final var db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder("res").build());
        try (final var session = db.beginResourceSession("res");
             final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"));
        }
      }
      final var h = createHandlers(singleRevDir, McpServerConfig.defaults(singleRevDir));
      final var result = h.diff(null, request(Map.of("database", "singledb", "resource", "res")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("only 1 revision");
    }
  }

  @Nested class Snapshots {
    @Test void createAndListSnapshots() {
      final var createResult = handlers.createSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "v1")));
      assertThat(createResult.isError()).isFalse();
      final var listResult = handlers.listSnapshots(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME)));
      assertThat(resultText(listResult)).contains("v1");
    }
    @Test void createSnapshotUsesLatestRevision() {
      final var result = handlers.createSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "latest")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("revision 1");
    }
  }

  @Nested class Insert {
    @Test void insertsIntoEmptyResource(@TempDir Path insertDir) {
      final Path dbPath = insertDir.resolve("insertdb");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
      try (final var db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder("emptyres").build());
      }
      final var h = createHandlers(insertDir, McpServerConfig.defaults(insertDir).withReadOnly(false));
      final var result = h.insert(null, request(Map.of(
          "database", "insertdb", "resource", "emptyres", "data", "{\"inserted\":true}")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Insert completed");
    }
    @Test void readOnlyModeBlocksInsert() {
      final var result = handlers.insert(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "data", "{}")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("read-only");
    }
  }

  @Nested class Update {
    @Test void updatesStringValue() {
      final var result = readWriteHandlers.update(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 3, "value", "Bob")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Update completed");
      final var queryResult = readWriteHandlers.query(null, request(Map.of(
          "query", "jn:doc('" + DB_NAME + "','" + RESOURCE_NAME + "')(\"name\")",
          "database", DB_NAME)));
      assertThat(queryResult.isError()).isFalse();
      assertThat(resultText(queryResult)).contains("Bob");
    }
    @Test void rejectsNonexistentNode() {
      final var result = readWriteHandlers.update(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 99999, "value", "test")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Node not found");
    }
  }

  @Nested class Delete {
    @Test void deletesNode() {
      final var result = readWriteHandlers.delete(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 6)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Delete completed");
    }
    @Test void rejectsNonexistentNode() {
      final var result = readWriteHandlers.delete(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 99999)));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Node not found");
    }
  }

  @Nested class Revert {
    @Test void revertsToEarlierRevision() {
      readWriteHandlers.update(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 3, "value", "Modified")));
      final var result = readWriteHandlers.revert(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "revision", 1)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Reverted to revision 1");
    }
    @Test void revertToCurrentRevisionIsNoop() {
      final var result = readWriteHandlers.revert(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "revision", 1)));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Already at revision");
    }
    @Test void revertsViaSnapshotName() {
      readWriteHandlers.createSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "before-change", "revision", 1)));
      readWriteHandlers.update(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 3, "value", "Changed")));
      final var result = readWriteHandlers.revert(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "snapshot", "before-change")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("Reverted to revision 1");
    }
    @Test void rejectsMissingRevisionAndSnapshot() {
      final var result = readWriteHandlers.revert(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME)));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("must be specified");
    }
  }

  @Nested class DeleteSnapshot {
    @Test void deletesExistingSnapshot() {
      readWriteHandlers.createSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "temp")));
      final var result = readWriteHandlers.deleteSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "temp")));
      assertThat(result.isError()).isFalse();
      assertThat(resultText(result)).contains("deleted");
    }
    @Test void readOnlyModeBlocksDeleteSnapshot() {
      final var result = handlers.deleteSnapshot(null, request(Map.of(
          "database", DB_NAME, "resource", RESOURCE_NAME, "name", "x")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("read-only");
    }
  }

  @Nested class PathTraversal {
    @Test void rejectsDotDotInDatabaseName() {
      final var result = handlers.listResources(null, request(Map.of("database", "../escape")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Invalid database name");
    }
    @Test void rejectsSlashInDatabaseName() {
      final var result = handlers.resourceInfo(null, request(Map.of("database", "foo/bar", "resource", "res")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Invalid database name");
    }
    @Test void rejectsSlashInResourceName() {
      final var result = handlers.resourceInfo(null, request(Map.of("database", DB_NAME, "resource", "../etc/passwd")));
      assertThat(result.isError()).isTrue();
      assertThat(resultText(result)).contains("Invalid resource name");
    }
  }

  @Nested class GuardedStoreTest {
    @Test void queryCannotCreateDatabaseViaJnStore() {
      final var result = handlers.query(null, request(Map.of(
          "query", "jn:store('attack-db','res','{}')", "database", DB_NAME)));
      assertThat(result.isError()).isTrue();
    }
    @Test void queryCannotAccessDeniedDatabaseViaJnDoc() {
      final var config = new McpServerConfig("test", "1.0.0", "stdio", tempDir.toString(),
          true, List.of(), List.of("secret-db"), Map.of(), 100, 4096, false, false, false, null);
      final var h = createHandlers(tempDir, config);
      final var result = h.query(null, request(Map.of(
          "query", "jn:doc('secret-db','res')", "database", DB_NAME)));
      assertThat(result.isError()).isTrue();
    }
  }
}

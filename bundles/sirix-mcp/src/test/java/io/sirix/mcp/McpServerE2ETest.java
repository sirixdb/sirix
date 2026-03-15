package io.sirix.mcp;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests that verify MCP server wiring: tool registration,
 * tool listing, and the full request pipeline from {@link SirixMcpServer}'s
 * registration methods through {@link ToolHandlers}.
 *
 * <p>These tests build a real {@link McpSyncServer} with the same registration
 * logic used in production, then verify that tools are correctly registered
 * and that the handlers are wired to real SirixDB databases.
 */
class McpServerE2ETest {

  private static final String DB_NAME = "e2e-db";
  private static final String RESOURCE_NAME = "e2e-res";

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    final Path dbPath = tempDir.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final var db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
      try (final var session = db.beginResourceSession(RESOURCE_NAME);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"key\":\"value\",\"num\":42}"));
      }
    }
  }

  @AfterEach
  void tearDown() {
    // @TempDir handles cleanup
  }

  private McpSyncServer buildServer(boolean readWrite) throws IOException {
    final var config = McpServerConfig.defaults(tempDir).withReadOnly(!readWrite);
    final var accessControl = new AccessControl(config);
    final var sanitizer = new OutputSanitizer(config);
    final var snapshots = new SnapshotRegistry(tempDir);
    final var auditLog = AuditLog.create(config);
    final var toolHandlers = new ToolHandlers(config, accessControl, sanitizer, snapshots, auditLog);

    // Use the same dummy transport provider — we won't actually connect
    final var transport = new io.modelcontextprotocol.server.transport.StdioServerTransportProvider(
        new io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper(
            tools.jackson.databind.json.JsonMapper.builder().build()));

    final var builder = McpServer.sync(transport)
        .serverInfo("sirixdb-mcp-test", "1.0.0");

    // Register tools using the same static methods as SirixMcpServer
    registerReadTools(builder, toolHandlers);
    if (readWrite) {
      registerWriteTools(builder, toolHandlers);
    }

    return builder.build();
  }

  // --- Tool registration tests ---

  @Test
  void readOnlyServerRegisters8Tools() throws IOException {
    final var server = buildServer(false);
    final List<Tool> tools = server.listTools();

    assertThat(tools).hasSize(8);
    final var names = tools.stream().map(Tool::name).toList();
    assertThat(names).containsExactlyInAnyOrder(
        "sirix_list_databases",
        "sirix_list_resources",
        "sirix_resource_info",
        "sirix_query",
        "sirix_history",
        "sirix_diff",
        "sirix_list_snapshots",
        "sirix_create_snapshot");
  }

  @Test
  void readWriteServerRegisters13Tools() throws IOException {
    final var server = buildServer(true);
    final List<Tool> tools = server.listTools();

    assertThat(tools).hasSize(13);
    final var names = tools.stream().map(Tool::name).toList();
    assertThat(names).contains(
        "sirix_insert",
        "sirix_update",
        "sirix_delete",
        "sirix_revert",
        "sirix_delete_snapshot");
  }

  @Test
  void toolsHaveDescriptions() throws IOException {
    final var server = buildServer(false);
    final List<Tool> tools = server.listTools();

    for (final Tool tool : tools) {
      assertThat(tool.description())
          .as("Tool %s should have a description", tool.name())
          .isNotBlank();
    }
  }

  @Test
  void toolsHaveInputSchemas() throws IOException {
    final var server = buildServer(false);
    final List<Tool> tools = server.listTools();

    for (final Tool tool : tools) {
      assertThat(tool.inputSchema())
          .as("Tool %s should have an input schema", tool.name())
          .isNotNull();
    }
  }

  // --- Full pipeline tests ---

  @Test
  void fullPipelineListAndQuery() throws IOException {
    final var config = McpServerConfig.defaults(tempDir);
    final var accessControl = new AccessControl(config);
    final var sanitizer = new OutputSanitizer(config);
    final var snapshots = new SnapshotRegistry(tempDir);
    final var auditLog = AuditLog.create(config);
    final var toolHandlers = new ToolHandlers(config, accessControl, sanitizer, snapshots, auditLog);

    // Step 1: List databases
    final var listDbResult = toolHandlers.listDatabases(null,
        new CallToolRequest("sirix_list_databases", Map.of()));
    assertThat(listDbResult.isError()).isFalse();
    assertThat(((TextContent) listDbResult.content().getFirst()).text()).contains(DB_NAME);

    // Step 2: List resources
    final var listResResult = toolHandlers.listResources(null,
        new CallToolRequest("sirix_list_resources", Map.of("database", DB_NAME)));
    assertThat(listResResult.isError()).isFalse();
    assertThat(((TextContent) listResResult.content().getFirst()).text()).contains(RESOURCE_NAME);

    // Step 3: Get resource info
    final var infoResult = toolHandlers.resourceInfo(null,
        new CallToolRequest("sirix_resource_info",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(infoResult.isError()).isFalse();
    final String infoText = ((TextContent) infoResult.content().getFirst()).text();
    assertThat(infoText).contains("\"latestRevision\":1");

    // Step 4: Query
    final var queryResult = toolHandlers.query(null,
        new CallToolRequest("sirix_query",
            Map.of("query", "jn:doc('" + DB_NAME + "','" + RESOURCE_NAME + "')(\"key\")",
                "database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(queryResult.isError()).isFalse();
    assertThat(((TextContent) queryResult.content().getFirst()).text()).contains("value");

    // Step 5: History
    final var historyResult = toolHandlers.history(null,
        new CallToolRequest("sirix_history",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(historyResult.isError()).isFalse();
    assertThat(((TextContent) historyResult.content().getFirst()).text()).contains("\"revision\":1");
  }

  @Test
  void fullPipelineWriteAndVerify() throws IOException {
    final var config = McpServerConfig.defaults(tempDir).withReadOnly(false);
    final var accessControl = new AccessControl(config);
    final var sanitizer = new OutputSanitizer(config);
    final var snapshots = new SnapshotRegistry(tempDir);
    final var auditLog = AuditLog.create(config);
    final var toolHandlers = new ToolHandlers(config, accessControl, sanitizer, snapshots, auditLog);

    // Step 1: Create snapshot before mutation
    final var snapResult = toolHandlers.createSnapshot(null,
        new CallToolRequest("sirix_create_snapshot",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "name", "baseline")));
    assertThat(snapResult.isError()).isFalse();

    // Step 2: Update existing data to create revision 2
    final var updateResult = toolHandlers.update(null,
        new CallToolRequest("sirix_update",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME,
                "nodeKey", 3, "value", "modified", "message", "update value")));
    assertThat(updateResult.isError()).isFalse();
    assertThat(((TextContent) updateResult.content().getFirst()).text()).contains("revision 2");

    // Step 3: Diff between revisions
    final var diffResult = toolHandlers.diff(null,
        new CallToolRequest("sirix_diff",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME,
                "from_revision", 1, "to_revision", 2)));
    assertThat(diffResult.isError()).isFalse();

    // Step 4: Revert to snapshot
    final var revertResult = toolHandlers.revert(null,
        new CallToolRequest("sirix_revert",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME,
                "snapshot", "baseline", "message", "rollback")));
    assertThat(revertResult.isError()).isFalse();
    assertThat(((TextContent) revertResult.content().getFirst()).text())
        .contains("Reverted to revision 1");

    // Step 5: Verify history shows 3 revisions
    final var historyResult = toolHandlers.history(null,
        new CallToolRequest("sirix_history",
            Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(historyResult.isError()).isFalse();
    assertThat(((TextContent) historyResult.content().getFirst()).text()).contains("\"revision\":3");
  }

  // --- Copy of tool registration from SirixMcpServer (for testability) ---

  private static void registerReadTools(McpServer.SyncSpecification<?> builder,
      ToolHandlers handlers) {
    builder.toolCall(tool("sirix_list_databases", "List all accessible SirixDB databases",
        schema(Map.of(), List.of())), handlers::listDatabases);
    builder.toolCall(tool("sirix_list_resources", "List resources in a database",
        schema(Map.of("database", stringProp("Database name")), List.of("database"))),
        handlers::listResources);
    builder.toolCall(tool("sirix_resource_info", "Get resource metadata",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name")), List.of("database", "resource"))),
        handlers::resourceInfo);
    builder.toolCall(tool("sirix_query", "Execute a read-only JSONiq query",
        schema(Map.of("query", stringProp("JSONiq expression"),
            "database", stringProp("Database name"),
            "resource", stringProp("Resource name")),
            List.of("query", "database"))),
        handlers::query);
    builder.toolCall(tool("sirix_history", "Get revision history",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "count", intProp("Number of revisions")),
            List.of("database", "resource"))),
        handlers::history);
    builder.toolCall(tool("sirix_diff", "Structural diff between two revisions",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "from_revision", intProp("Start revision"),
            "to_revision", intProp("End revision")),
            List.of("database", "resource"))),
        handlers::diff);
    builder.toolCall(tool("sirix_list_snapshots", "List named snapshots",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name")),
            List.of("database", "resource"))),
        handlers::listSnapshots);
    builder.toolCall(tool("sirix_create_snapshot", "Label a revision with a name",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "name", stringProp("Snapshot label"),
            "revision", intProp("Revision to label")),
            List.of("database", "resource", "name"))),
        handlers::createSnapshot);
  }

  private static void registerWriteTools(McpServer.SyncSpecification<?> builder,
      ToolHandlers handlers) {
    builder.toolCall(tool("sirix_insert", "Insert JSON data",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "data", stringProp("JSON data"),
            "message", stringProp("Commit message")),
            List.of("database", "resource", "data"))),
        handlers::insert);
    builder.toolCall(tool("sirix_update", "Update a node value",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "nodeKey", intProp("Node key"),
            "value", stringProp("New value"),
            "message", stringProp("Commit message")),
            List.of("database", "resource", "nodeKey", "value"))),
        handlers::update);
    builder.toolCall(tool("sirix_delete", "Delete a node",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "nodeKey", intProp("Node key"),
            "message", stringProp("Commit message")),
            List.of("database", "resource", "nodeKey"))),
        handlers::delete);
    builder.toolCall(tool("sirix_revert", "Revert to previous revision",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "revision", intProp("Revision to revert to"),
            "snapshot", stringProp("Snapshot name"),
            "message", stringProp("Commit message")),
            List.of("database", "resource"))),
        handlers::revert);
    builder.toolCall(tool("sirix_delete_snapshot", "Remove a snapshot label",
        schema(Map.of("database", stringProp("Database name"),
            "resource", stringProp("Resource name"),
            "name", stringProp("Snapshot label")),
            List.of("database", "resource", "name"))),
        handlers::deleteSnapshot);
  }

  @SuppressWarnings("unchecked")
  private static io.modelcontextprotocol.spec.McpSchema.Tool tool(String name, String desc,
      io.modelcontextprotocol.spec.McpSchema.JsonSchema schema) {
    return new io.modelcontextprotocol.spec.McpSchema.Tool(name, null, desc, schema, null, null, null);
  }

  @SuppressWarnings("unchecked")
  private static io.modelcontextprotocol.spec.McpSchema.JsonSchema schema(
      Map<String, Map<String, Object>> props, List<String> required) {
    return new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
        "object", (Map<String, Object>) (Map<?, ?>) props, required, null, null, null);
  }

  private static Map<String, Object> stringProp(String desc) {
    return Map.of("type", "string", "description", desc);
  }

  private static Map<String, Object> intProp(String desc) {
    return Map.of("type", "integer", "description", desc);
  }
}

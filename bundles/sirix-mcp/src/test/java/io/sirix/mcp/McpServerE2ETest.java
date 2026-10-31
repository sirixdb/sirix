package io.sirix.mcp;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import tools.jackson.databind.json.JsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpServerE2ETest {

  private static final String DB_NAME = "e2e-db";
  private static final String RESOURCE_NAME = "e2e-res";

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    final Path dbPath = tempDir.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final var db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
      try (final var session = db.beginResourceSession(RESOURCE_NAME);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\":\"value\",\"num\":42}"));
      }
    }
  }

  private ToolHandlers buildHandlers(boolean readWrite) throws IOException {
    final var config = McpServerConfig.defaults(tempDir).withReadOnly(!readWrite);
    return new ToolHandlers(config, new AccessControl(config), new OutputSanitizer(config),
        new SnapshotRegistry(tempDir), AuditLog.create(config));
  }

  private McpSyncServer buildServer(boolean readWrite) throws IOException {
    final var config = McpServerConfig.defaults(tempDir).withReadOnly(!readWrite);
    final var toolHandlers = buildHandlers(readWrite);
    final var transport = new StdioServerTransportProvider(
        new JacksonMcpJsonMapper(
            JsonMapper.builder().build()));
    final var builder = McpServer.sync(transport).serverInfo("sirixdb-mcp-test", "1.0.0");
    SirixMcpServer.registerReadTools(builder, toolHandlers);
    if (readWrite) SirixMcpServer.registerWriteTools(builder, toolHandlers);
    return builder.build();
  }

  @Test void readOnlyServerRegisters8Tools() throws IOException {
    assertThat(buildServer(false).listTools()).hasSize(8);
  }

  @Test void readWriteServerRegisters14Tools() throws IOException {
    final var names = buildServer(true).listTools().stream().map(Tool::name).toList();
    assertThat(names).hasSize(14).contains("sirix_create_resource", "sirix_insert", "sirix_update", "sirix_delete",
        "sirix_revert", "sirix_delete_snapshot");
  }

  @Test void toolsHaveDescriptionsAndSchemas() throws IOException {
    for (final boolean readWrite : new boolean[] {false, true}) {
      for (final Tool tool : buildServer(readWrite).listTools()) {
        assertThat(tool.description()).as("Tool %s", tool.name()).isNotBlank();
        assertThat(tool.inputSchema()).as("Tool %s", tool.name()).isNotNull();
        if (tool.name().equals("sirix_query"))
          assertThat(tool.annotations().readOnlyHint()).isTrue();
      }
    }
  }

  @Test void fullPipelineListAndQuery() throws IOException {
    final var h = buildHandlers(false);
    final var listDb = h.listDatabases(null, new CallToolRequest("sirix_list_databases", Map.of()));
    assertThat(listDb.isError()).isFalse();
    assertThat(((TextContent) listDb.content().getFirst()).text()).contains(DB_NAME);

    final var listRes = h.listResources(null, new CallToolRequest("sirix_list_resources", Map.of("database", DB_NAME)));
    assertThat(((TextContent) listRes.content().getFirst()).text()).contains(RESOURCE_NAME);

    final var info = h.resourceInfo(null, new CallToolRequest("sirix_resource_info",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(((TextContent) info.content().getFirst()).text()).contains("\"latestRevision\":1");

    final var query = h.query(null, new CallToolRequest("sirix_query",
        Map.of("query", "jn:doc('" + DB_NAME + "','" + RESOURCE_NAME + "')(\"key\")", "database", DB_NAME)));
    assertThat(query.isError()).isFalse();
    assertThat(((TextContent) query.content().getFirst()).text()).contains("value");
  }

  @Test void fullPipelineWriteAndVerify() throws IOException {
    final var h = buildHandlers(true);
    h.createSnapshot(null, new CallToolRequest("sirix_create_snapshot",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "name", "baseline")));
    // Cat-1: post-fusion (iter#32) the OBJECT_KEY+STRING_VALUE pair "key":"value" collapses
    // into one OBJECT_NAMED_STRING at nodeKey=2 (was nodeKey=3 pre-fusion).
    final var update = h.update(null, new CallToolRequest("sirix_update",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "nodeKey", 2, "value", "modified", "message", "change")));
    assertThat(((TextContent) update.content().getFirst()).text()).contains("revision 2");

    final var diff = h.diff(null, new CallToolRequest("sirix_diff",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "from_revision", 1, "to_revision", 2)));
    assertThat(diff.isError()).isFalse();

    final var revert = h.revert(null, new CallToolRequest("sirix_revert",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME, "snapshot", "baseline", "message", "rollback")));
    assertThat(((TextContent) revert.content().getFirst()).text()).contains("Reverted to revision 1");

    final var history = h.history(null, new CallToolRequest("sirix_history",
        Map.of("database", DB_NAME, "resource", RESOURCE_NAME)));
    assertThat(((TextContent) history.content().getFirst()).text()).contains("\"revision\":3");
  }

}

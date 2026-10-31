package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class HistoricalDiffContextTest {
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  @TempDir
  Path directory;

  @Test
  void authorizedCrossRevisionDiffsAgreeWithAndWithoutSidecars() throws Exception {
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final Path sidecar;
    try (final var db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("data")
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .useDeweyIDs(true)
                                             .storeDiffs(true)
                                             .build());
      try (final var session = db.beginResourceSession("data"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"keep\":\"before\",\"gone\":\"old-value\"}"), JsonNodeTrx.Commit.NO);
        writer.commit();
        assertThat(writer.moveTo(2)).isTrue();
        writer.setStringValue("after");
        assertThat(writer.moveTo(3)).isTrue();
        writer.remove();
        writer.commit();
        sidecar = session.getResourceConfig()
                         .getResource()
                         .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                         .resolve("diffFromRev1toRev2.json");
      }
    }
    assertThat(sidecar).exists();
    JsonNode expected = null;
    for (final boolean cached : new boolean[] {true, false}) {
      if (!cached)
        Files.delete(sidecar);
      for (final boolean readOnly : new boolean[] {true, false}) {
        final var config = new McpServerConfig("test", "1", "stdio", directory.toString(), readOnly, List.of(),
            List.of(), Map.of("db", List.of("data")), 100, 4096, false, false, false, null);
        final var access = new AccessControl(config);
        final var h = new ToolHandlers(config, access, new OutputSanitizer(config), new SnapshotRegistry(access),
            AuditLog.create(config));
        for (final int revision : new int[] {1, 2}) {
          final var response = h.query(null, request("jn:diff('db','data',1,2)", revision));
          assertThat(response.isError()).as(text(response)).isFalse();
          final var result = (Map<?, ?>) response.structuredContent();
          assertThat(result.get("revision")).isEqualTo(revision);
          final var items = (List<?>) result.get("items");
          assertThat(items).hasSize(1);
          final var diff = MAPPER.readTree(items.getFirst().toString());
          assertThat(diff.get("old-revision").asInt()).isEqualTo(1);
          assertThat(diff.get("new-revision").asInt()).isEqualTo(2);
          assertThat(diff.get("diffs").size()).isEqualTo(2);
          boolean deletion = false;
          for (final JsonNode operation : diff.get("diffs"))
            deletion |= operation.has("delete");
          assertThat(deletion).isTrue();
          if (expected == null)
            expected = diff.get("diffs");
          else
            assertThat(diff.get("diffs")).isEqualTo(expected);
          final var context = h.query(null, request("$$.keep", revision));
          assertThat(context.isError()).as(text(context)).isFalse();
          assertThat(((Map<?, ?>) context.structuredContent()).get("items")).isEqualTo(List.of(revision == 1
              ? "\"before\""
              : "\"after\""));
        }
      }
    }
  }

  private static CallToolRequest request(final String query, final int revision) {
    return new CallToolRequest("test",
        Map.of("database", "db", "resource", "data", "revision", revision, "query", query));
  }

  private static String text(final CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }
}

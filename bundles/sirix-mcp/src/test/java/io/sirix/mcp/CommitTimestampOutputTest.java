package io.sirix.mcp;

import tools.jackson.databind.json.JsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * VER-201: public commit timestamp text preserves every stored microsecond with ISO_INSTANT
 * formatting.
 */
final class CommitTimestampOutputTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"2021-03-04T05:06:07Z,2021-03-04T05:06:08Z", "2021-03-04T05:06:07.123Z,2021-03-04T05:06:08.123Z",
      "2021-03-04T05:06:07.123456Z,2021-03-04T05:06:08.123456Z"})
  void metadataAndHistoryPreserveIsoTimestampPrecision(final String first, final String second) throws Exception {
    final var config = McpServerConfig.defaults(directory);
    final var access = new AccessControl(config);
    final var handlers = new ToolHandlers(config, access, new OutputSanitizer(config), new SnapshotRegistry(access),
        AuditLog.create(config));
    final var request = new CallToolRequest("test", Map.of("database", "db", "resource", "res"));
    final var mapper = JsonMapper.builder().build();
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res"); final var writer = session.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild("value");
        writer.commit(null, Instant.parse(first));
        final var single = mapper.readTree(text(handlers.resourceInfo(null, request)));
        assertEquals(first, single.get("created").asText());
        assertEquals(first, single.get("lastModified").asText());
        writer.commit(null, Instant.parse(second));
      }
    }
    final var info = mapper.readTree(text(handlers.resourceInfo(null, request)));
    assertEquals(first, info.get("created").asText());
    assertEquals(second, info.get("lastModified").asText());
    final var history = mapper.readTree(text(handlers.history(null, request)));
    assertEquals(2, history.size());
    assertEquals(second, history.get(0).get("timestamp").asText());
    assertEquals(first, history.get(1).get("timestamp").asText());
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("res")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(reader.getRevisionTimestamp(),
              Instant.parse(history.get(2 - revision).get("timestamp").asText()));
        }
      }
    }
  }

  private static String text(final CallToolResult result) {
    assertFalse(result.isError());
    return ((TextContent) result.content().getFirst()).text();
  }
}

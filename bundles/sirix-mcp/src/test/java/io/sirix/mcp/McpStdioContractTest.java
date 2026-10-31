package io.sirix.mcp;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the actual launcher entry point and MCP JSON-RPC contract across two server JVMs. */
class McpStdioContractTest {
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  @TempDir
  Path directory;

  @Test
  @Timeout(90)
  void queryMutationDiffRevertAndColdServerRestart() throws Exception {
    final Path databasePath = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var db = Databases.openJsonDatabase(databasePath)) {
      db.createResource(ResourceConfiguration.newBuilder("res").build());
      try (final var session = db.beginResourceSession("res"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"Alice\"}"));
      }
    }
    try (final var server = new Server(directory)) {
      final JsonNode tools = server.rpc("tools/list", Map.of());
      assertThat(tools.get("tools").size()).isEqualTo(13);
      assertThat(server.call("sirix_query", Map.of("query", "jn:doc('db','res').name"))
                       .get("structuredContent")
                       .get("items")
                       .toString()).contains("Alice");
      assertThat(
          server.call("sirix_create_snapshot", Map.of("name", "baseline")).path("isError").asBoolean()).isFalse();
      assertThat(
          server.call("sirix_update", Map.of("nodeKey", 2, "value", "Bob")).path("isError").asBoolean()).isFalse();
      final JsonNode historical =
          server.call("sirix_query", Map.of("query", "jn:doc('db','res').name", "snapshot", "baseline"));
      assertThat(historical.get("structuredContent").get("items").toString()).contains("Alice").doesNotContain("Bob");
      final JsonNode diff = server.call("sirix_diff", Map.of("from_revision", 1, "to_revision", 2));
      assertThat(diff.get("structuredContent").get("diffs").size()).isEqualTo(1);
      assertThat(diff.get("structuredContent").get("truncated").asBoolean()).isFalse();
      final JsonNode stale = server.call("sirix_revert", Map.of("snapshot", "baseline", "expected_head", 1));
      assertThat(stale.path("isError").asBoolean()).isTrue();
      assertThat(stale.get("structuredContent").get("code").asText()).isEqualTo("stale_head");
      final JsonNode revert = server.call("sirix_revert", Map.of("snapshot", "baseline", "expected_head", 2));
      assertThat(revert.get("structuredContent").get("revision").asInt()).isEqualTo(3);
      assertThat(revert.get("structuredContent").get("previousHead").asInt()).isEqualTo(2);
    }
    try (final var cold = new Server(directory)) {
      assertThat(cold.call("sirix_list_snapshots", Map.of()).toString()).contains("baseline");
      assertThat(cold.call("sirix_query", Map.of("query", "jn:doc('db','res').name"))
                     .get("structuredContent")
                     .get("items")
                     .toString()).contains("Alice");
      assertThat(cold.call("sirix_query", Map.of("query", "jn:doc('db','res').name", "revision", 2))
                     .get("structuredContent")
                     .get("items")
                     .toString()).contains("Bob");
    }
  }

  private static final class Server implements AutoCloseable {
    private final Process process;
    private final BufferedReader output;
    private final BufferedWriter input;
    private int nextId;

    private Server(final Path directory) throws IOException {
      final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
      process = new ProcessBuilder(java, "-Xmx512m", "--enable-preview", "--add-modules", "jdk.incubator.vector",
          "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/java.io=ALL-UNNAMED", "--add-opens",
          "java.base/java.nio=ALL-UNNAMED", "-cp", System.getProperty("sirix.mcp.runtimeClasspath"),
          SirixMcpServer.class.getName(), "--database-path", directory.toString(), "--read-write").redirectError(
              ProcessBuilder.Redirect.INHERIT).start();
      output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
      input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
      try {
        rpc("initialize", Map.of("protocolVersion", "2025-06-18", "capabilities", Map.of(), "clientInfo",
            Map.of("name", "sirix-contract-test", "version", "1")));
        send(Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));
      } catch (final IOException | RuntimeException exception) {
        process.destroyForcibly();
        throw exception;
      }
    }

    private JsonNode call(final String name, final Map<String, Object> parameters) throws IOException {
      final var args = new LinkedHashMap<String, Object>();
      args.put("database", "db");
      args.put("resource", "res");
      args.putAll(parameters);
      return rpc("tools/call", Map.of("name", name, "arguments", args));
    }

    private JsonNode rpc(final String method, final Map<String, Object> parameters) throws IOException {
      final int id = ++nextId;
      send(Map.of("jsonrpc", "2.0", "id", id, "method", method, "params", parameters));
      String line;
      while ((line = output.readLine()) != null) {
        final JsonNode message = MAPPER.readTree(line);
        if (message.has("id") && message.get("id").asInt() == id) {
          assertThat(message.has("error")).as(line).isFalse();
          return message.get("result");
        }
      }
      throw new IOException("MCP server exited before responding to " + method);
    }

    private void send(final Map<String, Object> message) throws IOException {
      input.write(MAPPER.writeValueAsString(message));
      input.newLine();
      input.flush();
    }

    @Override
    public void close() throws Exception {
      input.close();
      // ProcessHandle preserves the readable pipe so shutdown output is also checked as JSON-RPC.
      process.toHandle().destroy();
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.toHandle().destroyForcibly();
        process.waitFor();
      }
      String line;
      while ((line = output.readLine()) != null) {
        assertThat(MAPPER.readTree(line).path("jsonrpc").asText()).as(line).isEqualTo("2.0");
      }
      output.close();
    }
  }
}

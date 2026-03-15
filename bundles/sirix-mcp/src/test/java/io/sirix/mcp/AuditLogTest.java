package io.sirix.mcp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLogTest {

  @TempDir
  Path tempDir;

  private McpServerConfig configWithAudit(String path) {
    return new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        true, List.of(), List.of(), Map.of(),
        100, 4096, true, true, true, path);
  }

  private McpServerConfig configWithoutAudit() {
    return new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        true, List.of(), List.of(), Map.of(),
        100, 4096, true, true, false, null);
  }

  @Test
  void writesJsonlToFile() throws Exception {
    var logPath = tempDir.resolve("audit.jsonl").toString();
    try (var audit = AuditLog.create(configWithAudit(logPath))) {
      audit.log("sirix_query", Map.of("database", "mydb"), "success", null);
    }

    var lines = Files.readAllLines(Path.of(logPath));
    assertThat(lines).hasSize(1);
    assertThat(lines.get(0)).contains("sirix_query");
    assertThat(lines.get(0)).contains("\"status\":\"success\"");
  }

  @Test
  void redactsSensitiveParams() throws Exception {
    var logPath = tempDir.resolve("audit.jsonl").toString();
    try (var audit = AuditLog.create(configWithAudit(logPath))) {
      audit.log("sirix_query",
          Map.of("database", "mydb", "query", "SELECT * FROM secrets"),
          "success", null);
    }

    var content = Files.readString(Path.of(logPath));
    assertThat(content).doesNotContain("SELECT * FROM secrets");
    assertThat(content).contains("[REDACTED]");
    assertThat(content).contains("mydb");
  }

  @Test
  void redactsDataValueAndMessage() throws Exception {
    var logPath = tempDir.resolve("audit.jsonl").toString();
    try (var audit = AuditLog.create(configWithAudit(logPath))) {
      audit.log("sirix_insert",
          Map.of("database", "db", "data", "{\"secret\":true}", "message", "adding stuff"),
          "success", null);
    }

    var content = Files.readString(Path.of(logPath));
    assertThat(content).doesNotContain("{\"secret\":true}");
    assertThat(content).doesNotContain("adding stuff");
  }

  @Test
  void disabledAuditLogsNothing() throws Exception {
    try (var audit = AuditLog.create(configWithoutAudit())) {
      audit.log("sirix_query", Map.of("database", "mydb"), "success", null);
    }
    // No exception, no file written — just verifying it doesn't fail
  }

  @Test
  void includesTimestampAndDetail() throws Exception {
    var logPath = tempDir.resolve("audit.jsonl").toString();
    try (var audit = AuditLog.create(configWithAudit(logPath))) {
      audit.log("sirix_delete", Map.of("database", "db"), "error", "node not found");
    }

    var content = Files.readString(Path.of(logPath));
    assertThat(content).contains("timestamp");
    assertThat(content).contains("node not found");
    assertThat(content).contains("\"status\":\"error\"");
  }
}

package io.sirix.mcp;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.io.OutputStreamWriter;

import static java.util.Objects.requireNonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Append-only audit log for MCP tool invocations.
 *
 * <p>
 * Every tool call is recorded with timestamp, tool name, parameters (with values redacted for write
 * operations), and result status. This enables post-incident investigation when prompt injection is
 * suspected.
 *
 * <p>
 * Format: one JSON object per line (JSONL).
 */
public final class AuditLog implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLog.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  private final @Nullable Writer writer;

  private AuditLog(@Nullable Writer writer) {
    this.writer = writer;
  }

  public static AuditLog create(McpServerConfig config) throws IOException {
    if (!config.auditLog()) {
      return new AuditLog(null);
    }

    Writer writer;
    final String auditLogPath = config.auditLogPath();
    if (auditLogPath != null) {
      var path = Path.of(auditLogPath);
      Files.createDirectories(requireNonNull(path.getParent()));
      writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } else {
      // Default: log to stderr so it doesn't interfere with stdio MCP transport on stdout
      writer = new OutputStreamWriter(System.err);
    }

    return new AuditLog(writer);
  }

  /**
   * Log a tool invocation.
   *
   * @param toolName the MCP tool name
   * @param params tool parameters (values may be redacted)
   * @param status "success", "error", or "denied"
   * @param detail additional detail (error message, result count, etc.)
   */
  public void log(String toolName, @Nullable Map<String, ?> params, String status, @Nullable String detail) {
    if (writer == null) {
      return;
    }

    try {
      ObjectNode entry = MAPPER.createObjectNode();
      entry.put("timestamp", Instant.now().toString());
      entry.put("tool", toolName);
      entry.set("params", MAPPER.valueToTree(redactSensitiveValues(params)));
      entry.put("status", status);
      if (detail != null) {
        entry.put("detail", detail);
      }

      synchronized (writer) {
        writer.write(MAPPER.writeValueAsString(entry));
        writer.write('\n');
        writer.flush();
      }
    } catch (IOException e) {
      LOG.warn("Failed to write audit log entry", e);
    }
  }

  /** Redact actual data values from write operations to avoid logging sensitive content. */
  private Map<String, Object> redactSensitiveValues(@Nullable Map<String, ?> params) {
    if (params == null) {
      return Map.of();
    }
    return params.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> switch (e.getKey()) {
      case "data", "value", "query", "message" -> "[REDACTED]";
      default -> e.getValue() != null
          ? e.getValue()
          : "null";
    }));
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }
}

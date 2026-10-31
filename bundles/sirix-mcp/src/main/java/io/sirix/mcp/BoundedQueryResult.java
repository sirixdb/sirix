package io.sirix.mcp;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.query.SirixStringSerializer;
import io.sirix.mcp.LimitedStringWriter.LimitExceededException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/** Serializes at most one page, discarding an oversized value atomically. */
final class BoundedQueryResult {
  private BoundedQueryResult() {}

  static Map<String, Object> serialize(final Sequence sequence, final int offset, final McpServerConfig config,
      final OutputSanitizer sanitizer) {
    final var values = new ArrayList<String>(Math.min(config.maxResultSize(), 128));
    boolean truncated = false;
    boolean oversized = false;
    int position = 0;
    String warning = null;
    if (sequence != null) {
      final var buffer = new LimitedStringWriter(config.maxStringValueLength());
      try (final var writer = new PrintWriter(buffer);
          final var iterator = sequence instanceof Item single
              ? new ItemSequence(single).iterate()
              : sequence.iterate()) {
        final var serializer = new SirixStringSerializer(writer);
        Item item;
        while ((item = iterator.next()) != null) {
          if (position++ < offset)
            continue;
          if (values.size() == config.maxResultSize()) {
            truncated = true;
            break;
          }
          buffer.reset(config.maxStringValueLength());
          try {
            serializer.serialize(item);
            writer.flush();
            final String value = buffer.toString();
            final String detected = sanitizer.detectInjection(value);
            if (detected != null)
              warning = detected;
            values.add(sanitizer.sanitize(value));
          } catch (final LimitExceededException exception) {
            oversized = true;
            truncated = true;
            break;
          }
        }
      }
    }
    final var result = new LinkedHashMap<String, Object>();
    result.put("items", values);
    result.put("truncated", truncated);
    if (warning != null)
      result.put("warning", warning);
    if (truncated) {
      result.put("continuation", oversized
          ? Map.of("offset", offset + values.size(), "hint",
              "Project a smaller value or split the document into items; this value exceeds maxStringValueLength")
          : Map.of("offset", offset + values.size(), "hint", "Repeat the same query and selector with this offset"));
    }
    return result;
  }

}

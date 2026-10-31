package io.sirix.mcp;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedQueryResultTest {
  private static McpServerConfig config() {
    return new McpServerConfig("test", "1", "stdio", Path.of(".").toAbsolutePath().toString(), true, List.of(),
        List.of(), Map.of(), 2, 16, false, false, false, null);
  }

  @Test
  void consumesOnlyOnePageAndOneLookaheadItem() {
    final var consumed = new AtomicInteger();
    final var closed = new AtomicInteger();
    final var config = config();
    final var result =
        BoundedQueryResult.serialize(sequence("small", consumed, closed), 0, config, new OutputSanitizer(config));
    assertThat(result).containsEntry("truncated", true);
    assertThat((List<?>) result.get("items")).hasSize(2);
    assertThat(consumed).hasValue(3);
    assertThat(closed).hasValue(1);
  }

  @Test
  void stopsAtOversizedValueWithoutConsumingTheRest() {
    final var consumed = new AtomicInteger();
    final var closed = new AtomicInteger();
    final var config = config();
    final var result = BoundedQueryResult.serialize(sequence("large".repeat(10000), consumed, closed), 0, config,
        new OutputSanitizer(config));
    assertThat(result).containsEntry("items", List.of()).containsEntry("truncated", true);
    assertThat(consumed).hasValue(1);
    assertThat(closed).hasValue(1);
  }

  @Test
  void readWriteServersAlsoStopAfterOnePage() {
    final var consumed = new AtomicInteger();
    final var closed = new AtomicInteger();
    final var config = config().withReadOnly(false);
    final var result = BoundedQueryResult.serialize(sequence("small", consumed, closed, 5), 0, config,
        new OutputSanitizer(config));
    assertThat(result).containsEntry("truncated", true);
    assertThat((List<?>) result.get("items")).hasSize(2);
    assertThat(consumed).hasValue(3);
    assertThat(closed).hasValue(1);
    assertThat(((Map<?, ?>) result.get("continuation")).containsKey("offset")).isTrue();
  }

  private static AbstractSequence sequence(final String text, final AtomicInteger consumed,
      final AtomicInteger closed) {
    return sequence(text, consumed, closed, Integer.MAX_VALUE);
  }

  private static AbstractSequence sequence(final String text, final AtomicInteger consumed, final AtomicInteger closed,
      final int maximum) {
    final var value = new Str(text);
    return new AbstractSequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          @Override
          public Item next() {
            if (consumed.get() == maximum)
              return null;
            consumed.incrementAndGet();
            return value;
          }

          @Override
          public void close() {
            closed.incrementAndGet();
          }
        };
      }
    };
  }
}

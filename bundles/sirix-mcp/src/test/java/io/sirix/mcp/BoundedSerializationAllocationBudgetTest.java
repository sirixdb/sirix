package io.sirix.mcp;

import com.sun.management.ThreadMXBean;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.query.SirixStringSerializer;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BoundedSerializationAllocationBudgetTest {
  private static final int LIMIT = 4096;
  private static volatile Object sink;

  @Test
  void queryPagesAllocateReturnedStringsWithoutReallocatingScratchPerItem() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final var bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
    for (final int rows : new int[] {256, 4096}) {
      final Item[] items = new Item[rows];
      Arrays.fill(items, new Str("row-value"));
      final var sequence = new ItemSequence(items);
      final var config = new McpServerConfig("test", "1", "stdio", Path.of(".").toAbsolutePath().toString(), true,
          List.of(), List.of(), Map.of(), rows, LIMIT, false, false, false, null);
      final var sanitizer = new OutputSanitizer(config);
      for (int warmup = 0; warmup < 32; warmup++) {
        sink = BoundedQueryResult.serialize(sequence, 0, config, sanitizer);
        sink = withRowScratch(items);
      }
      final long pageStart = bean.getCurrentThreadAllocatedBytes();
      final var page = BoundedQueryResult.serialize(sequence, 0, config, sanitizer);
      sink = page;
      final long pageBytes = bean.getCurrentThreadAllocatedBytes() - pageStart;
      final long controlStart = bean.getCurrentThreadAllocatedBytes();
      final var control = withRowScratch(items);
      sink = control;
      final long controlBytes = bean.getCurrentThreadAllocatedBytes() - controlStart;
      System.out.printf("BOUNDED-QUERY-ALLOCATION rows=%d pageBytes=%d controlBytes=%d%n", rows, pageBytes,
          controlBytes);
      assertThat(page).containsEntry("items", control).containsEntry("truncated", false);
      assertThat(control).hasSize(rows).allMatch("row-value"::equals);
      assertThat(pageBytes).as("page allocation for %s returned strings", rows)
                           .isLessThanOrEqualTo(65536L + rows * 1024L);
      assertThat(controlBytes).as("per-row scratch allocation witness for %s rows", rows)
                              .isGreaterThanOrEqualTo(rows * (long) LIMIT);
    }
  }

  private static List<String> withRowScratch(final Item[] items) {
    final var values = new ArrayList<String>(items.length);
    for (final Item item : items) {
      final var buffer = new LimitedStringWriter(LIMIT);
      try (final var writer = new PrintWriter(buffer)) {
        new SirixStringSerializer(writer).serialize(item);
        writer.flush();
        values.add(buffer.toString());
      }
    }
    return values;
  }
}

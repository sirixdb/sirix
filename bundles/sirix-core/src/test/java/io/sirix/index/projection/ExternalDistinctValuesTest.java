package io.sirix.index.projection;

import io.sirix.node.ValueDictionaryEntryNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The corpus half of the dictionary pre-pass: distinct values, ascending in the engine's collation,
 * under a byte budget the caller sets.
 *
 * <p>
 * The property under test is the same at every budget — the output is the distinct input, sorted —
 * so every case asserts it against a {@link TreeSet} ordered by the very comparator the dictionary
 * uses, and the budget only decides whether the answer came out of the arena or out of a merge of
 * spilled runs.
 * </p>
 */
final class ExternalDistinctValuesTest {

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  /** The expected answer: distinct, ascending under the dictionary's comparator. */
  private static List<String> expected(final List<String> input) {
    final TreeSet<String> sorted = new TreeSet<>((left, right) -> {
      final byte[] a = utf8(left);
      final byte[] b = utf8(right);
      return ValueDictionaryEntryNode.compareUtf16Range(a, 0, a.length, b, 0, b.length);
    });
    sorted.addAll(input);
    return new ArrayList<>(sorted);
  }

  private static List<String> drain(final ExternalDistinctValues values) {
    final List<String> out = new ArrayList<>();
    final Iterator<byte[]> ascending = values.ascending();
    while (ascending.hasNext()) {
      out.add(new String(ascending.next(), StandardCharsets.UTF_8));
    }
    return out;
  }

  private static void addAll(final ExternalDistinctValues values, final List<String> input) {
    for (final String value : input) {
      final byte[] bytes = utf8(value);
      values.add(bytes, 0, bytes.length);
    }
  }

  @Test
  @DisplayName("everything within the budget: distinct and ascending, out of the arena, no run spilled")
  void withinTheBudgetNothingSpills(@TempDir final Path dir) {
    final List<String> input = List.of("delta", "alpha", "charlie", "alpha", "bravo", "delta", "alpha");
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, 1 << 20)) {
      addAll(values, input);
      assertEquals(7L, values.appended(), "duplicates are appended; they are removed by the sort");
      final List<String> out = drain(values);
      assertEquals(0, values.spilledRuns(), "a megabyte budget holds seven short values");
      assertEquals(expected(input), out);
      assertEquals(4L, values.distinct());
      assertFalse(Files.exists(dir.resolve("nothing")), "no run file is named");
    }
  }

  @Test
  @DisplayName("a budget smaller than the data spills runs and the merge still yields the distinct set once")
  void aTinyBudgetSpillsAndMerges(@TempDir final Path dir) {
    final List<String> input = new ArrayList<>();
    final Random random = new Random(20260903L);
    for (int i = 0; i < 4_000; i++) {
      // Deliberately few distinct values relative to the appends, so the merge must drop copies of
      // one value arriving from many different runs — the case a per-run dedup alone gets wrong.
      input.add("value-" + random.nextInt(300));
    }
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, ExternalDistinctValues.MIN_BUDGET_BYTES)) {
      addAll(values, input);
      final List<String> out = drain(values);
      assertTrue(values.spilledRuns() > 1, "a 4 KiB budget over 4,000 values must spill several runs: "
          + values.spilledRuns());
      assertEquals(expected(input), out, "the merged output is the distinct input, ascending");
      assertEquals(300L, values.distinct(), "each of the 300 values appears exactly once");
      assertEquals(4_000L, values.appended());
    }
  }

  @Test
  @DisplayName("the spilled runs are deleted when the collector closes")
  void closeDeletesTheRuns(@TempDir final Path dir) throws Exception {
    final ExternalDistinctValues values = new ExternalDistinctValues(dir, ExternalDistinctValues.MIN_BUDGET_BYTES);
    final List<String> input = new ArrayList<>();
    for (int i = 0; i < 2_000; i++) {
      input.add("v" + i);
    }
    addAll(values, input);
    assertEquals(expected(input), drain(values));
    assertTrue(values.spilledRuns() > 0);
    try (var stream = Files.list(dir)) {
      assertTrue(stream.findAny().isPresent(), "runs are on disk while the iterator reads them");
    }
    values.close();
    try (var stream = Files.list(dir)) {
      assertFalse(stream.findAny().isPresent(), "close deletes every run it wrote");
    }
  }

  @Test
  @DisplayName("ordering is the dictionary's own comparator, not byte order: values above the BMP sort by code point")
  void orderingIsTheDictionaryComparator(@TempDir final Path dir) {
    // U+FF21 (fullwidth A) is ABOVE U+10000 (a supplementary character) in UTF-8 byte order and
    // BELOW it under UTF-16 code-point order, which is what the dictionary compares in. A collector
    // that sorted bytes would put them the other way round and the appender would reject the run.
    final String supplementary = new String(Character.toChars(0x10000));
    final String fullwidth = "Ａ";
    final List<String> input = List.of(supplementary, fullwidth, "a");
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, 1 << 20)) {
      addAll(values, input);
      final List<String> out = drain(values);
      assertEquals(expected(input), out);
      assertEquals(List.of("a", supplementary, fullwidth), out,
          "the supplementary character sorts BELOW the fullwidth one, as UTF-16 code points");
    }
  }

  @Test
  @DisplayName("the same holds when the collation-ordered values are split across spilled runs")
  void orderingSurvivesTheMerge(@TempDir final Path dir) {
    final String supplementary = new String(Character.toChars(0x10000));
    final String fullwidth = "Ａ";
    final List<String> input = new ArrayList<>();
    for (int i = 0; i < 400; i++) {
      input.add(supplementary);
      input.add(fullwidth);
      input.add("a" + i);
    }
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, ExternalDistinctValues.MIN_BUDGET_BYTES)) {
      addAll(values, input);
      final List<String> out = drain(values);
      assertTrue(values.spilledRuns() > 1, "the input must not fit the budget");
      assertEquals(expected(input), out);
    }
  }

  @Test
  @DisplayName("empty values, an empty collector, and the contract that a drained collector is done")
  void edgesAndContract(@TempDir final Path dir) {
    try (ExternalDistinctValues empty = new ExternalDistinctValues(dir, 1 << 20)) {
      assertEquals(List.of(), drain(empty));
      assertEquals(0L, empty.distinct());
      assertThrows(IllegalStateException.class, empty::ascending, "ascending() drains the collector");
      final byte[] value = utf8("x");
      assertThrows(IllegalStateException.class, () -> empty.add(value, 0, 1), "a drained collector takes no value");
    }
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, 1 << 20)) {
      // The empty string is a real dictionary entry (the per-leaf dictionaries carry it).
      addAll(values, List.of("", "b", "", "a"));
      assertEquals(List.of("", "a", "b"), drain(values));
    }
    assertThrows(IllegalArgumentException.class, () -> new ExternalDistinctValues(dir, 8L), "budget floor");
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, 1 << 20)) {
      final byte[] value = utf8("abc");
      assertThrows(IndexOutOfBoundsException.class, () -> values.add(value, 1, 5));
    }
  }

  @Test
  @DisplayName("a value larger than the whole budget is still collected, in its own run")
  void aValueLargerThanTheBudget(@TempDir final Path dir) {
    final StringBuilder huge = new StringBuilder();
    for (int i = 0; i < 20_000; i++) {
      huge.append('x');
    }
    final List<String> input = List.of("a", huge.toString(), "b", huge.toString());
    try (ExternalDistinctValues values = new ExternalDistinctValues(dir, ExternalDistinctValues.MIN_BUDGET_BYTES)) {
      addAll(values, input);
      assertEquals(expected(input), drain(values), "the oversized value is not dropped or truncated");
      assertEquals(3L, values.distinct());
    }
  }
}

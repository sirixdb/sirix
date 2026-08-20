package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dictionary writer's runtime bound: the protection that needs no row-count hint.
 *
 * <p>
 * Defect (11) was not a crash. A 100M-row load elected three long, near-unique string columns, and
 * their arenas doubled until the collector took 3.4 cores and the load wrote one megabyte a minute
 * while still reporting itself alive. Nothing threw, so nothing could react. This pins the bound
 * that turns that into a refusal a caller can act on — and pins that it is TYPED, because the
 * caller has to tell "this dictionary got too big", which is expected at scale and recoverable,
 * from a genuine encoding fault, which is not.
 * </p>
 */
final class GlobalValueDictionaryWriterBudgetTest {

  private static byte[] value(final int i) {
    return ("http://example.com/a/rather/long/path/segment?id=" + i).getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("A bounded writer refuses once its retained bytes reach the budget")
  void boundedWriterRefusesAtTheBudget() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter(3, GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (64L << 10));
    final GlobalDictionaryBudgetExceededException thrown =
        assertThrows(GlobalDictionaryBudgetExceededException.class, () -> {
          for (int i = 0; i < 1_000_000; i++) {
            final byte[] v = value(i);
            writer.intern(v, 0, v.length);
          }
        });

    assertEquals(3, thrown.column(), "the refusal must name the column, or the log cannot say what to fix");
    assertTrue(thrown.entryCount() > 0, "it stopped before interning anything, so the bound is mis-scaled");
    assertTrue(thrown.retainedBytes() <= thrown.budgetBytes() + (64L << 10),
        "retained " + thrown.retainedBytes() + " overshot the " + thrown.budgetBytes()
            + " budget by more than one growth step — the check runs too late");
    assertTrue(thrown.getMessage().contains("budget"), thrown.getMessage());
  }

  @Test
  @DisplayName("An unbounded writer is unaffected — the bound is opt-in, not a behaviour change")
  void unboundedWriterInternsFreely() {
    // NON-VACUITY, and the regression guard for every existing caller: the SAME loop that refuses
    // above must run to completion when no budget is imposed. Without this, a mis-scaled bound that
    // refused everything would still pass the test above.
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    for (int i = 0; i < 20_000; i++) {
      final byte[] v = value(i);
      writer.intern(v, 0, v.length);
    }
    assertEquals(20_000, writer.entryCount());
    assertTrue(writer.retainedBytes() > GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (64L << 10),
        "the unbounded run must have exceeded the bounded run's budget, or the two arms are not comparable");
  }

  @Test
  @DisplayName("Interning a repeated value is a hit and costs no budget")
  void repeatedValuesDoNotConsumeBudget() {
    // The bound is on DISTINCT values; a column that repeats heavily is exactly the case a
    // resource-wide dictionary is for, and it must not be penalised for row count.
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter(0, GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (64L << 10));
    final byte[] v = value(1);
    final int first = writer.intern(v, 0, v.length);
    for (int i = 0; i < 100_000; i++) {
      assertEquals(first, writer.intern(v, 0, v.length));
    }
    assertEquals(1, writer.entryCount());
  }
}

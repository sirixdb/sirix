package io.sirix.cache;

import io.sirix.cache.HeapCacheBudget.Limit;
import io.sirix.cache.HeapCacheBudget.Pool;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HeapCacheBudgetTest {
  @Test
  void defaultsPartitionOneEnvelopeAndReserveHalfTheHeapForExecution() {
    final long heap = 6L << 30;
    final HeapCacheBudget budget = new HeapCacheBudget(heap, ignored -> null);
    assertEquals(heap / 2, budget.metrics().envelopeBytes());
    assertEquals(heap / 2, budget.metrics().workspaceReserveBytes());
    assertEquals(heap / 4, budget.bytes(Pool.PROJECTION));
    assertEquals(heap / 8, budget.bytes(Pool.DERIVED));
    assertEquals(heap / 8, budget.projectionFillBytes());
    assertEquals(heap / 8, budget.queryWorkspaceBytes());
    assertBounded(budget);
  }

  @Test
  void everyOverrideIsClampedEvenWhenAllPoolsRequestTheWholeHeap() {
    final Map<String, String> properties = new HashMap<>();
    properties.put(HeapCacheBudget.MAX_BYTES_PROPERTY, Long.toString(Long.MAX_VALUE));
    properties.put(HeapCacheBudget.PROJECTION_FILL_BYTES_PROPERTY, Long.toString(Long.MAX_VALUE));
    for (final Pool pool : Pool.values()) {
      properties.put(pool.property(), Long.toString(Long.MAX_VALUE));
    }
    for (final long heap : new long[] {0, 1, 79, 1024, 6L << 30, Long.MAX_VALUE}) {
      final HeapCacheBudget budget = new HeapCacheBudget(heap, properties::get);
      assertBounded(budget);
      assertEquals(heap / 2, budget.metrics().envelopeBytes());
      assertTrue(budget.metrics().workspaceReserveBytes() >= heap / 2);
      final long fills = budget.projectionFillBytes();
      assertTrue(fills + fills / 4 <= budget.bytes(Pool.PROJECTION));
    }
  }

  @Test
  void zeroDisablesAndUnusedSharesAreNotGrantedAgainToSiblings() {
    final Map<String, String> properties = new HashMap<>();
    properties.put(HeapCacheBudget.MAX_BYTES_PROPERTY, "800");
    properties.put(Pool.PROJECTION.property(), "0");
    properties.put(Pool.DERIVED.property(), "99999");
    final HeapCacheBudget budget = new HeapCacheBudget(8000, properties::get);
    assertEquals(0, budget.bytes(Pool.PROJECTION));
    assertEquals(0, budget.projectionFillBytes());
    assertEquals(200, budget.bytes(Pool.DERIVED));
    assertBounded(budget);
    properties.put(HeapCacheBudget.MAX_BYTES_PROPERTY, "0");
    final HeapCacheBudget disabled = new HeapCacheBudget(8000, properties::get);
    for (final Pool pool : Pool.values()) {
      assertEquals(0, disabled.bytes(pool));
    }
    assertEquals(8000, disabled.metrics().workspaceReserveBytes());
  }

  @Test
  void invalidConfigurationCannotTurnALimitIntoUnboundedRetention() {
    for (final String invalid : new String[] {"-1", "wrong", "9223372036854775808"}) {
      assertThrows(IllegalArgumentException.class, () -> new HeapCacheBudget(1024, ignored -> invalid));
    }
    assertThrows(IllegalArgumentException.class, () -> new HeapCacheBudget(-1, ignored -> null));
  }

  @Test
  void integerWeightsConservativelyPriceLongCapacitiesAndOversizedEntries() {
    for (final long maximum : new long[] {0, 1, Integer.MAX_VALUE, 8L << 30, Long.MAX_VALUE}) {
      final long unit = HeapCacheBudget.weightUnit(maximum);
      final long capacity = maximum / unit;
      assertTrue(capacity < Integer.MAX_VALUE);
      if (maximum < Long.MAX_VALUE) {
        assertTrue(HeapCacheBudget.weight(maximum + 1, unit) > capacity);
      }
      final long entry = maximum / 3;
      final int weight = HeapCacheBudget.weight(entry, unit);
      assertTrue(weight >= entry / unit);
      assertTrue(weight * unit >= entry);
    }
    assertEquals(Integer.MAX_VALUE, HeapCacheBudget.weight(Long.MAX_VALUE, 1));
    assertThrows(IllegalArgumentException.class, () -> HeapCacheBudget.weight(-1, 1));
    assertThrows(IllegalArgumentException.class, () -> HeapCacheBudget.weight(1, 0));
  }

  @Test
  void dictionaryWeightsIncludeOffsetsAndLargeSpilledValuesAndZeroDisables() {
    final GlobalDictionaryRecordCacheKey key = new GlobalDictionaryRecordCacheKey(1, 2, 3, 4);
    final ValueDictionaryValueBlockNode block =
        new ValueDictionaryValueBlockNode(4, 1, new int[257], new byte[0]);
    final GlobalDictionaryRecordCache small = new GlobalDictionaryRecordCache(1024);
    small.put(key, block);
    assertNull(small.get(key), "the offset array alone exceeds this allowance");
    small.put(key, new ValueDictionaryEntryNode(4, new byte[2048]));
    assertNull(small.get(key), "a spilled string must be charged for its retained bytes");
    final GlobalDictionaryRecordCache disabled = new GlobalDictionaryRecordCache(0);
    disabled.put(key, new ValueDictionaryValueBlockNode(4, 1, new int[] {0, 0}, new byte[0]));
    assertNull(disabled.get(key));
    assertTrue(disabled.asMap().isEmpty());
  }

  @Test
  void disabledGlobalVerdictsDoNotRetainEvenEmptyArrays() {
    final GlobalVerdictCache cache = new GlobalVerdictCache(0);
    final GlobalVerdictCacheKey key = new GlobalVerdictCacheKey(1, 2, 3, 4, 0, "EQ", "0");
    cache.put(key, new long[0]);
    assertNull(cache.get(key));
    assertTrue(cache.asMap().isEmpty());
  }

  private static void assertBounded(final HeapCacheBudget budget) {
    long sum = 0;
    for (final Limit limit : budget.metrics().pools().values()) {
      assertTrue(limit.effectiveBytes() >= 0);
      assertTrue(limit.effectiveBytes() <= limit.ceilingBytes());
      sum = Math.addExact(sum, limit.effectiveBytes());
    }
    assertTrue(sum <= budget.metrics().envelopeBytes());
  }
}

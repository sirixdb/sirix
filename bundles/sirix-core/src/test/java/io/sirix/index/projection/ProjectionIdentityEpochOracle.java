package io.sirix.index.projection;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reads the sorted covering view independently of the replay writer and columnar row groups. */
public final class ProjectionIdentityEpochOracle {
  private ProjectionIdentityEpochOracle() {}

  public static byte[] assertOrder(final ProjectionIndexRowGroupPage page, final int row,
      final byte @Nullable [] previous) {
    final byte[] label = page.copyOrderLabelAt(row);
    assertTrue(label.length > 0, "non-empty document order label");
    if (previous != null) {
      assertTrue(Arrays.compareUnsigned(previous, label) < 0, "strict document order");
    }
    return label;
  }

  public static void assertSortedRows(final JsonNodeReadOnlyTrx reader, final int index, final LongArrayList keys,
      final LongArrayList values) {
    assertEquals(keys.size(), values.size());
    final var directory = ProjectionSortedDirectory.open(reader.getStorageEngineReader(), index);
    assertNotNull(directory);
    assertEquals(0, directory.unencodableRows());
    final long[] ordinals = new long[keys.size()];
    for (int row = 0; row < ordinals.length; row++) {
      ordinals[row] = row;
    }
    LongArrays.quickSort(ordinals, (left, right) -> {
      final int valueOrder = Long.compare(values.getLong((int) left), values.getLong((int) right));
      return valueOrder == 0
          ? Long.compare(keys.getLong((int) left), keys.getLong((int) right))
          : valueOrder;
    });
    final var cursor = directory.first();
    for (final long ordinal : ordinals) {
      assertTrue(cursor.isValid(), "missing sorted record");
      final byte[] key = cursor.copyKey();
      assertEquals(1 + 2 * Long.BYTES, key.length);
      assertEquals(values.getLong((int) ordinal), ProjectionSortedGroupScan.readOrderedLong(key, 1));
      assertEquals(keys.getLong((int) ordinal),
          ProjectionSortedGroupScan.readOrderedLong(key, key.length - Long.BYTES));
      cursor.advance();
    }
    assertFalse(cursor.isValid(), "extra sorted record");
  }
}

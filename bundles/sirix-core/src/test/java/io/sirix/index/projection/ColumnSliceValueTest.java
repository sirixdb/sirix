package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Column slice metadata and heap payloads compare by value, including nullable lanes. */
final class ColumnSliceValueTest {
  @Test
  void comparesAllMetadataAndArrayContents() {
    final ColumnSlice expected = slice(-1);
    final ColumnSlice copy = slice(-1);
    assertEquals(expected, copy);
    assertEquals(expected.hashCode(), copy.hashCode());
    for (int component = 0; component < 12; component++) {
      assertNotEquals(expected, slice(component), "different slice component " + component);
    }
    // equals(null) is the call under test: with null first, JUnit would never ask the slice.
    final Object absent = null;
    assertNotEquals(expected, absent);
    final Object unrelated = "slice";
    assertNotEquals(expected, unrelated);
    assertEquals("ColumnSlice[rowCount=1, flags=1, min=1, max=1, presenceWords=[1], numericValues=[1], "
        + "boolWords=[1], stringDictIds=[1], dictBytes=[1], dictOffsets=[1], setCounts=[1], dictHashes=[1], "
        + "packedStringIds=null, lane=null]", expected.toString());
  }

  @Test
  void handlesAbsentArrays() {
    final ColumnSlice left = new ColumnSlice(0, (byte) 0, 0, 0, new long[0], null, null, null, null, null);
    final ColumnSlice right = new ColumnSlice(0, (byte) 0, 0, 0, new long[0], null, null, null, null, null);
    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());
  }

  private static ColumnSlice slice(final int changed) {
    final int[] values = new int[12];
    for (int i = 0; i < values.length; i++) {
      values[i] = changed == i
          ? 2
          : 1;
    }
    return new ColumnSlice(values[0], (byte) values[1], values[2], values[3], new long[] {values[4]},
        new long[] {values[5]}, new long[] {values[6]}, new int[] {values[7]}, new byte[] {(byte) values[8]},
        new int[] {values[9]}, new int[] {values[10]}, new long[] {values[11]}, null, null);
  }
}

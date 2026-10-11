package io.sirix.index.changelog;

import io.sirix.index.changelog.ChangeLogSlab.Entry;
import io.sirix.index.changelog.ChangeLogSlab.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ChangeLogSlabTest {
  @Test
  void formatOneGoldenSlabPreservesBaseAndBothCells() {
    final Row before = new Row(true, 2);
    before.flags[0] = ChangeLogSlab.PRESENT;
    before.flags[1] = ChangeLogSlab.PRESENT;
    before.numbers[0] = -7;
    before.text[1] = "x";
    final Row after = before.copy();
    after.numbers[0] = 42;
    final ChangeLogSlab slab = new ChangeLogSlab(3, 9, 2, new byte[] {ChangeLogSlab.LONG, ChangeLogSlab.STRING},
        List.of(new Entry(11, new long[] {1}, before, after), new Entry(15, new long[] {3}, after, null)));
    final byte[] golden = HexFormat.of()
                                   .parseHex("53434c47000000010000000300000009000000020000000200020102000000010178"
                                       + "0b000000000000000101010d01000101540100" + "040000000000000003010154010000");
    assertArrayEquals(golden, slab.encode());
    final ChangeLogSlab decoded = ChangeLogSlab.decode(golden);
    assertEquals(2, decoded.baseRevision());
    assertEquals(9, decoded.revision());
    assertEquals(-7, Objects.requireNonNull(decoded.entries().getFirst().before()).numbers[0]);
    assertEquals("x", Objects.requireNonNull(decoded.entries().getFirst().after()).text[1]);
    assertNull(decoded.entries().getLast().after());
    assertArrayEquals(golden, decoded.encode());
  }

  @Test
  void allCellKindsAndExtremeLongsRoundTrip() {
    final byte[] kinds = {1, 1, 2, 3, 4, 5, 6, 7};
    final Row row = new Row(false, kinds.length);
    Arrays.fill(row.flags, ChangeLogSlab.PRESENT);
    row.numbers[0] = Long.MIN_VALUE;
    row.numbers[1] = Long.MAX_VALUE;
    row.text[2] = "\u00e4\uD83D\uDE00";
    row.numbers[3] = 1;
    row.numbers[4] = Double.doubleToRawLongBits(0.25);
    row.text[5] = new String[] {"", "x", "x"};
    row.numbers[6] = -1000;
    row.flags[7] = ChangeLogSlab.PRESENT | ChangeLogSlab.UNREPRESENTABLE;
    final ChangeLogSlab slab =
        new ChangeLogSlab(0, 1, 0, kinds, List.of(new Entry(Long.MAX_VALUE, new long[] {-1}, null, row)));
    final ChangeLogSlab decoded = ChangeLogSlab.decode(slab.encode());
    final Row actual = Objects.requireNonNull(decoded.entries().getFirst().after());
    assertFalse(actual.object);
    assertArrayEquals(row.numbers, actual.numbers);
    assertArrayEquals(row.flags, actual.flags);
    assertEquals(row.text[2], actual.text[2]);
    assertArrayEquals((String[]) row.text[5], (String[]) actual.text[5]);
    assertArrayEquals(slab.encode(), decoded.encode());
  }

  @Test
  void lexicalTimestampFlagsPreserveEpochsAndProvenance() {
    final byte[] kinds = {ChangeLogSlab.TIMESTAMP, ChangeLogSlab.TIMESTAMP, ChangeLogSlab.STRING};
    final Row before = new Row(true, kinds.length);
    Arrays.fill(before.flags, (byte) (ChangeLogSlab.PRESENT | ChangeLogSlab.UNREPRESENTABLE));
    before.numbers[0] = 1_704_067_200L;
    before.numbers[1] = -62_167_219_200L;
    before.flags[1] |= ChangeLogSlab.NON_DOUBLE;
    final Row after = before.copy();
    after.numbers[0] += 86_400;
    after.flags[1] = 0;
    after.numbers[1] = 0;
    final ChangeLogSlab slab = new ChangeLogSlab(0, 2, 1, kinds,
        List.of(new Entry(1, new long[] {3}, before, after)));
    final ChangeLogSlab decoded = ChangeLogSlab.decode(slab.encode());
    final Row old = Objects.requireNonNull(decoded.entries().getFirst().before());
    final Row current = Objects.requireNonNull(decoded.entries().getFirst().after());
    assertArrayEquals(before.numbers, old.numbers);
    assertArrayEquals(before.flags, old.flags);
    assertArrayEquals(after.numbers, current.numbers);
    assertArrayEquals(after.flags, current.flags);
    assertNull(old.text[2]);
    assertArrayEquals(slab.encode(), decoded.encode());
  }

  @Test
  void rejectsTruncatedUnsupportedAndTrailingFormats() {
    final byte[] bytes = new ChangeLogSlab(0, 1, 0, new byte[] {1}, List.of()).encode();
    for (int end = 0; end < bytes.length; end++) {
      final byte[] truncated = Arrays.copyOf(bytes, end);
      assertThrows(IllegalStateException.class, () -> ChangeLogSlab.decode(truncated));
    }
    final byte[] extended = Arrays.copyOf(bytes, bytes.length + 1);
    assertThrows(IllegalStateException.class, () -> ChangeLogSlab.decode(extended));
    bytes[7] = 2;
    assertThrows(IllegalStateException.class, () -> ChangeLogSlab.decode(bytes));
  }
}

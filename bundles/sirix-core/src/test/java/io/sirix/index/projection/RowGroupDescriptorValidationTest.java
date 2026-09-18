package io.sirix.index.projection;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RowGroupDescriptorValidationTest {
  private static volatile byte[] validationInput;

  @Test
  void validDescriptorsDoNotAllocateDiagnosticLabels() {
    final ThreadMXBean accounting = assertInstanceOf(ThreadMXBean.class, ManagementFactory.getThreadMXBean());
    assertTrue(accounting.isThreadAllocatedMemorySupported());
    if (!accounting.isThreadAllocatedMemoryEnabled())
      accounting.setThreadAllocatedMemoryEnabled(true);
    final byte[] kinds = new byte[16];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG);
    final byte[][] descriptors = {descriptor(kinds, false), descriptor(kinds, true)};
    for (final byte[] descriptor : descriptors) {
      validationInput = descriptor;
      for (int iteration = 0; iteration < 20_000; iteration++)
        RowGroupDescriptor.validate(validationInput);
    }
    final long threadId = Thread.currentThread().threadId();
    final long before = accounting.getThreadAllocatedBytes(threadId);
    for (int iteration = 0; iteration < 20_000; iteration++) {
      validationInput = descriptors[iteration & 1];
      RowGroupDescriptor.validate(validationInput);
    }
    final long allocated = accounting.getThreadAllocatedBytes(threadId) - before;
    assertTrue(allocated <= 1024, "successful validation allocated " + allocated + " bytes for 20000 descriptors");
  }

  @Test
  void corruptionRetainsColumnQualifiedAndUnqualifiedDiagnosticNames() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final byte[] valid = descriptor(kinds, true);
    final int header = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES;
    final byte[] shortKeys = valid.clone();
    putInt(shortKeys, entryOffset(shortKeys, 0) + 2, header);
    assertMessage("Corrupt leaf descriptor: KEYS has " + header + " bytes, expected at least " + (header + 25),
        shortKeys);
    final byte[] shortBody = valid.clone();
    putInt(shortBody,
        entryOffset(shortBody,
            RowGroupDescriptor.entryIndexOf(shortBody, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0))) + 2,
        header);
    assertMessage("Corrupt leaf descriptor: BODY(0) has " + header + " bytes, expected at least " + (header + 18),
        shortBody);
    final byte[] dictMirror = valid.clone();
    dictMirror[entryOffset(dictMirror,
        RowGroupDescriptor.entryIndexOf(dictMirror, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(1))) + 14] =
            1;
    assertMessage("Corrupt leaf descriptor: DICT(1) carries BODY-only mirror fields", dictMirror);

    final byte[] empty = descriptor(kinds, false);
    final byte[] badBodyFence = empty.clone();
    final int body =
        RowGroupDescriptor.entryIndexOf(badBodyFence, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1));
    putLong(badBodyFence, entryOffset(badBodyFence, body) + 15, 0);
    assertMessage("Corrupt leaf descriptor: empty BODY(1) zone map is [0, " + Long.MIN_VALUE
        + "] instead of the canonical sentinel pair", badBodyFence);
    final byte[] badKeyFence = empty.clone();
    putLong(badKeyFence, 11, 0);
    assertMessage("Corrupt leaf descriptor: empty record-key fence is [0, " + Long.MIN_VALUE
        + "] instead of the canonical sentinel pair", badKeyFence);
  }

  private static byte[] descriptor(final byte[] kinds, final boolean nonempty) {
    final ProjectionIndexRowGroupPage group = new ProjectionIndexRowGroupPage(kinds);
    if (nonempty) {
      final long[] numbers = new long[kinds.length];
      final String[] strings = new String[kinds.length];
      final boolean[] present = new boolean[kinds.length];
      Arrays.fill(numbers, 42);
      Arrays.fill(strings, "value");
      Arrays.fill(present, true);
      assertTrue(group.appendRow(1, numbers, new boolean[kinds.length], strings, present, new boolean[kinds.length],
          new boolean[kinds.length]));
    }
    return ProjectionIndexColumnSegmentCodec.encode(group.serialize()).descriptor();
  }

  private static void assertMessage(final String expected, final byte[] descriptor) {
    assertEquals(expected,
        assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(descriptor)).getMessage());
  }

  private static int entryOffset(final byte[] descriptor, final int entry) {
    assertTrue(entry >= 0);
    return RowGroupDescriptor.MIN_BYTES + RowGroupDescriptor.columnCount(descriptor)
        + entry * RowGroupDescriptor.ENTRY_BYTES;
  }

  private static void putInt(final byte[] target, final int offset, final int value) {
    for (int i = 0; i < Integer.BYTES; i++)
      target[offset + i] = (byte) (value >>> (8 * i));
  }

  private static void putLong(final byte[] target, final int offset, final long value) {
    for (int i = 0; i < Long.BYTES; i++)
      target[offset + i] = (byte) (value >>> (8 * i));
  }
}

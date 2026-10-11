package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GcOwnedNativeMemoryTest {

  @ParameterizedTest
  @ValueSource(longs = {0L, 1L, 1024L, 131073L})
  void allocationsHaveIndependentLifetimes(final long bytes) {
    final MemorySegment first = GcOwnedNativeMemory.allocate(bytes);
    final MemorySegment second = GcOwnedNativeMemory.allocate(bytes);
    assertTrue(first.isNative());
    assertTrue(second.isNative());
    assertEquals(bytes, first.byteSize());
    assertEquals(bytes, second.byteSize());
    assertNotSame(first.scope(), second.scope());
    if (bytes > 0L) {
      assertEquals((byte) 0, first.get(ValueLayout.JAVA_BYTE, bytes - 1L));
      first.set(ValueLayout.JAVA_BYTE, bytes - 1L, (byte) 42);
      assertEquals((byte) 0, second.get(ValueLayout.JAVA_BYTE, bytes - 1L));
    }
  }

  @Test
  void rejectsNegativeSizes() {
    assertThrows(IllegalArgumentException.class, () -> GcOwnedNativeMemory.allocate(-1L));
  }
}

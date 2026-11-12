package io.sirix.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class WindowsMemorySegmentAllocatorTest {
  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;

  @ParameterizedTest
  @CsvSource({"1, 4096", "4097, 8192", "8193, 16384", "16385, 32768", "32769, 65536", "65537, 131072",
      "131073, 262144"})
  void reusePreservesTheWholeSizeClass(final long requested, final long capacity) throws Exception {
    final WindowsMemorySegmentAllocator allocator = pooledAllocator();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment backing = arena.allocate(capacity);
      allocator.release(backing);
      final MemorySegment first = allocator.allocate(requested);
      assertSame(backing, first);
      assertEquals(capacity, first.byteSize());
      allocator.release(first);
      assertSame(backing, allocator.borrowSegment(capacity));
      allocator.release(backing);
      try (ScratchLease scratch = new ScratchLease(allocator)) {
        for (int round = 0; round < 512; round++) {
          scratch.acquire(Math.min(requested + round, capacity), ScratchUse.READ);
          assertSame(backing, scratch.segment());
          assertEquals(capacity, scratch.capacity());
          scratch.segment().set(ValueLayout.JAVA_BYTE, capacity - 1L, (byte) round);
          assertEquals((byte) round, scratch.segment().get(ValueLayout.JAVA_BYTE, capacity - 1L));
          scratch.close();
        }
      }
      assertSame(backing, allocator.allocate(capacity));
    }
  }

  @Test
  void invalidSizesAreRejectedBeforeLookingInPools() throws Exception {
    final WindowsMemorySegmentAllocator allocator = pooledAllocator();
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(0L));
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(-1L));
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(Long.MAX_VALUE));
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void progressivelyLargerReadsReuseOneMapping() throws Exception {
    final WindowsMemorySegmentAllocator allocator = newAllocator();
    allocator.init(64 * MIB);
    try {
      final long address;
      final MemorySegment initial = allocator.allocate(128 * KIB + 1L);
      address = initial.address();
      assertEquals(256 * KIB, initial.byteSize());
      allocator.release(initial);
      try (ScratchLease scratch = new ScratchLease(allocator)) {
        for (int round = 0; round < 512; round++) {
          scratch.acquire(128 * KIB + 1L + round, ScratchUse.READ);
          assertEquals(address, scratch.segment().address());
          assertEquals(256 * KIB, allocator.getPhysicalMemoryBytes());
          scratch.close();
        }
      }
    } finally {
      allocator.free(allocator.allocate(256 * KIB));
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void idleClassesFundALargerRequestWhileLentMemorySurvives() throws Exception {
    final WindowsMemorySegmentAllocator allocator = newAllocator();
    allocator.init(MIB);
    final MemorySegment lent = allocator.allocate(256 * KIB);
    MemorySegment larger = null;
    try {
      final MemorySegment first = allocator.allocate(256 * KIB);
      final MemorySegment second = allocator.allocate(256 * KIB);
      allocator.release(first);
      allocator.release(second);
      lent.set(ValueLayout.JAVA_LONG, 0L, 59L);
      larger = allocator.allocate(512 * KIB);
      assertEquals(768 * KIB, allocator.getPhysicalMemoryBytes());
      assertEquals(59L, lent.get(ValueLayout.JAVA_LONG, 0L));
    } finally {
      if (larger != null) {
        allocator.free(larger);
      }
      allocator.free(lent);
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  private static WindowsMemorySegmentAllocator newAllocator() throws Exception {
    final Constructor<WindowsMemorySegmentAllocator> constructor =
        WindowsMemorySegmentAllocator.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  @SuppressWarnings("unchecked")
  private static WindowsMemorySegmentAllocator pooledAllocator() throws Exception {
    final WindowsMemorySegmentAllocator allocator = newAllocator();
    final Field poolsField = WindowsMemorySegmentAllocator.class.getDeclaredField("segmentPools");
    poolsField.setAccessible(true);
    final Deque<MemorySegment>[] pools = (Deque<MemorySegment>[]) poolsField.get(allocator);
    for (int index = 0; index < pools.length; index++) {
      pools[index] = new ConcurrentLinkedDeque<>();
    }
    final Field initializedField = WindowsMemorySegmentAllocator.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    initializedField.setBoolean(allocator, true);
    return allocator;
  }
}

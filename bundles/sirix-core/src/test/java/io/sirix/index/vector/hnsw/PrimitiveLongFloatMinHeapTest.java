package io.sirix.index.vector.hnsw;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveLongFloatMinHeapTest {

  @Test
  void insertAndPollOrdering() {
    final var heap = new PrimitiveLongFloatMinHeap(4);
    heap.insert(10L, 5.0f);
    heap.insert(20L, 1.0f);
    heap.insert(30L, 3.0f);
    heap.insert(40L, 7.0f);

    assertEquals(4, heap.size());

    // Min distance should come out first
    assertEquals(1.0f, heap.peekDistance());
    assertEquals(20L, heap.peekKey());
    assertEquals(20L, heap.poll());

    assertEquals(3.0f, heap.peekDistance());
    assertEquals(30L, heap.poll());

    assertEquals(5.0f, heap.peekDistance());
    assertEquals(10L, heap.poll());

    assertEquals(7.0f, heap.peekDistance());
    assertEquals(40L, heap.poll());

    assertTrue(heap.isEmpty());
  }

  @Test
  void emptyHeapOperations() {
    final var heap = new PrimitiveLongFloatMinHeap(4);
    assertTrue(heap.isEmpty());
    assertEquals(0, heap.size());

    assertThrows(NoSuchElementException.class, heap::peekKey);
    assertThrows(NoSuchElementException.class, heap::peekDistance);
    assertThrows(NoSuchElementException.class, heap::poll);
    assertThrows(NoSuchElementException.class, heap::pollDistance);
  }

  @Test
  void capacityGrowth() {
    final var heap = new PrimitiveLongFloatMinHeap(2);

    // Insert more than initial capacity
    for (int i = 0; i < 100; i++) {
      heap.insert(i, 100.0f - i);
    }

    assertEquals(100, heap.size());

    // Verify min ordering is maintained after growth
    float prevDist = -1.0f;
    for (int i = 0; i < 100; i++) {
      final float dist = heap.peekDistance();
      assertTrue(dist >= prevDist, "Heap order violated at index " + i);
      prevDist = dist;
      heap.poll();
    }
  }

  @Test
  void clearAndReuse() {
    final var heap = new PrimitiveLongFloatMinHeap(8);
    heap.insert(1L, 1.0f);
    heap.insert(2L, 2.0f);
    heap.insert(3L, 3.0f);

    assertEquals(3, heap.size());
    heap.clear();
    assertTrue(heap.isEmpty());
    assertEquals(0, heap.size());

    // Reuse after clear
    heap.insert(100L, 0.5f);
    heap.insert(200L, 0.1f);

    assertEquals(2, heap.size());
    assertEquals(200L, heap.peekKey());
    assertEquals(0.1f, heap.peekDistance());
  }

  @Test
  void toSortedKeys() {
    final var heap = new PrimitiveLongFloatMinHeap(8);
    heap.insert(5L, 50.0f);
    heap.insert(1L, 10.0f);
    heap.insert(3L, 30.0f);
    heap.insert(2L, 20.0f);
    heap.insert(4L, 40.0f);

    final long[] sorted = heap.toSortedKeys();
    assertArrayEquals(new long[]{1L, 2L, 3L, 4L, 5L}, sorted);
    assertTrue(heap.isEmpty());
  }

  @Test
  void pollDistanceReturnsAndRemovesMin() {
    final var heap = new PrimitiveLongFloatMinHeap(4);
    heap.insert(10L, 3.0f);
    heap.insert(20L, 1.0f);
    heap.insert(30L, 2.0f);

    assertEquals(1.0f, heap.pollDistance());
    assertEquals(2, heap.size());
    assertEquals(2.0f, heap.pollDistance());
    assertEquals(1, heap.size());
    assertEquals(3.0f, heap.pollDistance());
    assertTrue(heap.isEmpty());
  }

  @Test
  void singleElement() {
    final var heap = new PrimitiveLongFloatMinHeap(1);
    heap.insert(42L, 3.14f);

    assertEquals(1, heap.size());
    assertFalse(heap.isEmpty());
    assertEquals(42L, heap.peekKey());
    assertEquals(3.14f, heap.peekDistance());
    assertEquals(42L, heap.poll());
    assertTrue(heap.isEmpty());
  }

  @Test
  void duplicateDistances() {
    final var heap = new PrimitiveLongFloatMinHeap(4);
    heap.insert(1L, 5.0f);
    heap.insert(2L, 5.0f);
    heap.insert(3L, 5.0f);

    assertEquals(3, heap.size());
    // All should come out (order among equal distances is implementation-defined)
    heap.poll();
    heap.poll();
    heap.poll();
    assertTrue(heap.isEmpty());
  }

  @Test
  void invalidInitialCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new PrimitiveLongFloatMinHeap(0));
    assertThrows(IllegalArgumentException.class, () -> new PrimitiveLongFloatMinHeap(-1));
  }
}

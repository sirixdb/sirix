package io.sirix.test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Lightweight replacement for Guava's IteratorTester.
 * Verifies iterator contract compliance across multiple runs.
 */
public final class IteratorTester<E> {

  private final int iterations;
  private final List<E> expectedElements;
  private final Supplier<Iterator<E>> iteratorSupplier;

  public IteratorTester(int iterations, List<E> expectedElements, Supplier<Iterator<E>> iteratorSupplier) {
    this.iterations = iterations;
    this.expectedElements = expectedElements;
    this.iteratorSupplier = iteratorSupplier;
  }

  public void test() {
    for (int i = 0; i < iterations; i++) {
      final Iterator<E> iterator = iteratorSupplier.get();
      int index = 0;
      while (iterator.hasNext()) {
        assertTrue("Iterator produced more elements than expected", index < expectedElements.size());
        assertEquals(expectedElements.get(index), iterator.next());
        index++;
      }
      assertEquals("Iterator produced fewer elements than expected", expectedElements.size(), index);
      assertFalse(iterator.hasNext());
      assertThrows(NoSuchElementException.class, iterator::next);
      assertThrows(UnsupportedOperationException.class, iterator::remove);
    }
  }
}

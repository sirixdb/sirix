package io.sirix.io;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for {@link RevisionIndex} covering all corner cases and edge cases defined in
 * the optimization plan.
 * 
 * <p>
 * Corner Cases (C1-C6):
 * <ul>
 * <li>C1: Empty database</li>
 * <li>C2: Timestamp before first revision</li>
 * <li>C3: Timestamp after last revision</li>
 * <li>C4: Exact timestamp match</li>
 * <li>C5: Timestamp between revisions</li>
 * <li>C6: Single revision</li>
 * </ul>
 * 
 * <p>
 * Edge Cases (E1-E6):
 * <ul>
 * <li>E1: Duplicate timestamps</li>
 * <li>E2: Non-power-of-2 array size</li>
 * <li>E3: SIMD array not aligned</li>
 * <li>E4: Concurrent access</li>
 * <li>E5: Large revision counts</li>
 * <li>E6: Long.MIN_VALUE/MAX_VALUE timestamps</li>
 * </ul>
 */
class RevisionIndexTest {

  @Nested
  @DisplayName("Corner Cases (C1-C6)")
  class CornerCases {

    @Test
    @DisplayName("C1: Empty database returns -1")
    void testEmptyDatabase() {
      RevisionIndex index = RevisionIndex.EMPTY;

      assertEquals(-1, index.findRevision(0L));
      assertEquals(-1, index.findRevision(100L));
      assertEquals(-1, index.findRevision(Long.MAX_VALUE));
      assertEquals(0, index.size());
      assertTrue(index.isEmpty());
    }

    @Test
    @DisplayName("C2: Timestamp before first revision returns -1")
    void testTimestampBeforeFirst() {
      long[] timestamps = {100L, 200L, 300L, 400L, 500L};
      long[] offsets = {1000L, 2000L, 3000L, 4000L, 5000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Timestamps before first should return -(0 + 1) = -1
      assertEquals(-1, index.findRevision(50L));
      assertEquals(-1, index.findRevision(0L));
      assertEquals(-1, index.findRevision(99L));
    }

    @Test
    @DisplayName("C3: Timestamp after last revision returns -(size + 1)")
    void testTimestampAfterLast() {
      long[] timestamps = {100L, 200L, 300L, 400L, 500L};
      long[] offsets = {1000L, 2000L, 3000L, 4000L, 5000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Timestamps after last should return -(size + 1) = -(5 + 1) = -6
      assertEquals(-6, index.findRevision(501L));
      assertEquals(-6, index.findRevision(600L));
      assertEquals(-6, index.findRevision(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @CsvSource({"100, 0", // First element
        "200, 1", "300, 2", // Middle element
        "400, 3", "500, 4" // Last element
    })
    @DisplayName("C4: Exact timestamp match returns positive index")
    void testExactMatch(long timestamp, int expectedIndex) {
      long[] timestamps = {100L, 200L, 300L, 400L, 500L};
      long[] offsets = {1000L, 2000L, 3000L, 4000L, 5000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertEquals(expectedIndex, index.findRevision(timestamp));
    }

    @ParameterizedTest
    @CsvSource({"150, -2", // Between index 0 and 1, insertion point = 1, return -(1+1) = -2
        "250, -3", // Between index 1 and 2, insertion point = 2, return -(2+1) = -3
        "350, -4", // Between index 2 and 3, insertion point = 3, return -(3+1) = -4
        "450, -5" // Between index 3 and 4, insertion point = 4, return -(4+1) = -5
    })
    @DisplayName("C5: Timestamp between revisions returns negative insertion point")
    void testBetweenRevisions(long timestamp, int expectedResult) {
      long[] timestamps = {100L, 200L, 300L, 400L, 500L};
      long[] offsets = {1000L, 2000L, 3000L, 4000L, 5000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertEquals(expectedResult, index.findRevision(timestamp));
    }

    @Test
    @DisplayName("C6: Single revision database")
    void testSingleRevision() {
      long[] timestamps = {100L};
      long[] offsets = {1000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Before single revision
      assertEquals(-1, index.findRevision(50L));

      // Exact match
      assertEquals(0, index.findRevision(100L));

      // After single revision
      assertEquals(-2, index.findRevision(150L));

      assertEquals(1, index.size());
      assertFalse(index.isEmpty());
    }
  }

  @Nested
  @DisplayName("Edge Cases (E1-E6)")
  class EdgeCases {

    @Test
    @DisplayName("E1: Duplicate timestamps - returns first match")
    void testDuplicateTimestamps() {
      long[] timestamps = {100L, 100L, 200L}; // Duplicate at 100
      long[] offsets = {1000L, 1001L, 2000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Should return one of the matching indices (0 or 1)
      int result = index.findRevision(100L);
      assertTrue(result == 0 || result == 1, "Expected 0 or 1 for duplicate timestamp, got " + result);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 7, 9, 10, 15, 17, 31, 33, 63, 65, 100, 127, 129})
    @DisplayName("E2: Non-power-of-2 array sizes")
    void testNonPowerOfTwoSizes(int size) {
      long[] timestamps = new long[size];
      long[] offsets = new long[size];
      for (int i = 0; i < size; i++) {
        timestamps[i] = (i + 1) * 100L;
        offsets[i] = (i + 1) * 1000L;
      }

      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Test first, middle, last
      assertEquals(0, index.findRevision(100L));
      assertEquals(size - 1, index.findRevision(size * 100L));

      // Test before and after
      assertEquals(-1, index.findRevision(50L));
      assertEquals(-(size + 1), index.findRevision((size + 1) * 100L));

      // Test a value in between
      if (size > 1) {
        int mid = size / 2;
        assertEquals(mid, index.findRevision((mid + 1) * 100L));
      }
    }

    @Test
    @DisplayName("E3: Non-power-of-2 sizes - Eytzinger handles irregular tree shapes")
    void testNonPowerOfTwoSizes() {
      // Create arrays of various sizes to test irregular Eytzinger tree handling
      int threshold = RevisionIndex.getEytzingerThreshold();

      // Test sizes around the threshold
      int[] testSizes = {threshold - 1, threshold, threshold + 1, threshold + 7, threshold + 13};

      for (int size : testSizes) {
        long[] timestamps = new long[size];
        long[] offsets = new long[size];

        for (int i = 0; i < size; i++) {
          timestamps[i] = (i + 1) * 100L;
          offsets[i] = (i + 1) * 1000L;
        }

        RevisionIndex index = RevisionIndex.create(timestamps, offsets);

        // Test last element
        assertEquals(size - 1, index.findRevision(size * 100L));

        // Test searching for value just before last
        assertEquals(-(size), index.findRevision(size * 100L - 50));

        // Test first element
        assertEquals(0, index.findRevision(100L));
      }
    }

    @Test
    @DisplayName("E4: Concurrent reads are thread-safe")
    void testConcurrentReads() throws InterruptedException {
      long[] timestamps = new long[1000];
      long[] offsets = new long[1000];
      for (int i = 0; i < 1000; i++) {
        timestamps[i] = (i + 1) * 100L;
        offsets[i] = (i + 1) * 1000L;
      }

      RevisionIndex index = RevisionIndex.create(timestamps, offsets);
      RevisionIndexHolder holder = new RevisionIndexHolder(index);

      int numThreads = 10;
      int numIterations = 1000;
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger errors = new AtomicInteger(0);

      ExecutorService executor = Executors.newFixedThreadPool(numThreads);

      for (int t = 0; t < numThreads; t++) {
        executor.submit(() -> {
          try {
            Random random = new Random();
            for (int i = 0; i < numIterations; i++) {
              RevisionIndex idx = holder.get();
              int rev = random.nextInt(1000);
              long ts = (rev + 1) * 100L;

              int result = idx.findRevision(ts);
              if (result != rev) {
                errors.incrementAndGet();
              }
            }
          } finally {
            latch.countDown();
          }
        });
      }

      latch.await(30, TimeUnit.SECONDS);
      executor.shutdown();

      assertEquals(0, errors.get(), "Concurrent reads should not cause errors");
    }

    @Test
    @DisplayName("E5: Large revision counts")
    void testLargeRevisionCount() {
      int size = 100_000;
      long[] timestamps = new long[size];
      long[] offsets = new long[size];

      for (int i = 0; i < size; i++) {
        timestamps[i] = i * 1000L; // 1 second apart
        offsets[i] = i * 4096L; // 4KB pages
      }

      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Test various lookups
      assertEquals(0, index.findRevision(0L));
      assertEquals(size - 1, index.findRevision((size - 1) * 1000L));
      assertEquals(50000, index.findRevision(50000 * 1000L));

      // Test not found
      assertEquals(-1, index.findRevision(-1L));
      assertEquals(-(size + 1), index.findRevision(size * 1000L));
    }

    @Test
    @DisplayName("E6: Long.MIN_VALUE and Long.MAX_VALUE timestamps")
    void testExtremeLongValues() {
      // Note: We can't use MIN_VALUE as first element because it would make
      // "before first" impossible to test. Use a range that includes extreme values.
      long[] timestamps = {Long.MIN_VALUE + 1, 0L, Long.MAX_VALUE - 1};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Exact matches
      assertEquals(0, index.findRevision(Long.MIN_VALUE + 1));
      assertEquals(1, index.findRevision(0L));
      assertEquals(2, index.findRevision(Long.MAX_VALUE - 1));

      // Before first
      assertEquals(-1, index.findRevision(Long.MIN_VALUE));

      // After last
      assertEquals(-4, index.findRevision(Long.MAX_VALUE));
    }
  }

  @Nested
  @DisplayName("Equivalence with Arrays.binarySearch")
  class EquivalenceTests {

    @Test
    @DisplayName("Random searches match Arrays.binarySearch semantics")
    void testRandomSearchEquivalence() {
      Random random = new Random(42); // Fixed seed for reproducibility

      // Create random sorted timestamps
      int size = 100;
      long[] timestamps = new long[size];
      long[] offsets = new long[size];

      long current = random.nextInt(1000);
      for (int i = 0; i < size; i++) {
        timestamps[i] = current;
        offsets[i] = i * 1000L;
        current += random.nextInt(100) + 1; // Ensure strictly increasing
      }

      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Test 10000 random searches
      for (int i = 0; i < 10000; i++) {
        long searchKey = random.nextLong();

        int expected = Arrays.binarySearch(timestamps, searchKey);
        int actual = index.findRevision(searchKey);

        assertEquals(expected, actual,
            "Mismatch for searchKey=" + searchKey + " expected=" + expected + " actual=" + actual);
      }
    }
  }

  @Nested
  @DisplayName("withNewRevision - Copy on Write")
  class CopyOnWriteTests {

    @Test
    @DisplayName("Adding new revision creates new index")
    void testWithNewRevision() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      RevisionIndex newIndex = index.withNewRevision(4000L, 400L);

      // Original unchanged
      assertEquals(3, index.size());
      assertEquals(-4, index.findRevision(400L));

      // New index has the addition
      assertEquals(4, newIndex.size());
      assertEquals(3, newIndex.findRevision(400L));
      assertEquals(4000L, newIndex.getOffset(3));
    }

    @Test
    @DisplayName("Cannot add revision with timestamp before last")
    void testNonMonotonicTimestamp() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertThrows(IllegalArgumentException.class, () -> index.withNewRevision(4000L, 250L));
    }

    @Test
    @DisplayName("Can add revision with same timestamp as last")
    void testSameTimestamp() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      // Same timestamp should be allowed
      RevisionIndex newIndex = index.withNewRevision(4000L, 300L);
      assertEquals(4, newIndex.size());
    }
  }

  @Nested
  @DisplayName("Accessor Methods")
  class AccessorTests {

    @Test
    @DisplayName("getOffset returns correct file offset")
    void testGetOffset() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertEquals(1000L, index.getOffset(0));
      assertEquals(2000L, index.getOffset(1));
      assertEquals(3000L, index.getOffset(2));
    }

    @Test
    @DisplayName("getOffset throws for invalid revision")
    void testGetOffsetOutOfBounds() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertThrows(IndexOutOfBoundsException.class, () -> index.getOffset(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> index.getOffset(3));
    }

    @Test
    @DisplayName("getTimestamp returns correct Instant")
    void testGetTimestamp() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      assertEquals(Instant.ofEpochMilli(100L), index.getTimestamp(0));
      assertEquals(Instant.ofEpochMilli(200L), index.getTimestamp(1));
      assertEquals(Instant.ofEpochMilli(300L), index.getTimestamp(2));
    }

    @Test
    @DisplayName("getRevisionFileData returns correct data")
    void testGetRevisionFileData() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L, 3000L};
      RevisionIndex index = RevisionIndex.create(timestamps, offsets);

      RevisionFileData data = index.getRevisionFileData(1);
      assertNotNull(data);
      assertEquals(2000L, data.offset());
      assertEquals(Instant.ofEpochMilli(200L), data.timestamp());
    }
  }

  @Nested
  @DisplayName("Validation")
  class ValidationTests {

    @Test
    @DisplayName("create throws for mismatched array lengths")
    void testMismatchedArrayLengths() {
      long[] timestamps = {100L, 200L, 300L};
      long[] offsets = {1000L, 2000L};

      assertThrows(IllegalArgumentException.class, () -> RevisionIndex.create(timestamps, offsets));
    }

    @Test
    @DisplayName("create throws for unsorted timestamps")
    void testUnsortedTimestamps() {
      long[] timestamps = {100L, 300L, 200L}; // Not sorted
      long[] offsets = {1000L, 3000L, 2000L};

      assertThrows(IllegalArgumentException.class, () -> RevisionIndex.create(timestamps, offsets));
    }

    @Test
    @DisplayName("create returns EMPTY singleton for empty arrays")
    void testEmptyArrays() {
      RevisionIndex index = RevisionIndex.create(new long[0], new long[0]);
      assertEquals(RevisionIndex.EMPTY, index);
    }
  }
}


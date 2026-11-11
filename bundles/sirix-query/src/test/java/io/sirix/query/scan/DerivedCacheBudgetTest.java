package io.sirix.query.scan;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.query.scan.DerivedCacheBudget.Entry;
import io.sirix.query.scan.DerivedCacheBudget.Partition;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("native-image")
final class DerivedCacheBudgetTest {
  private static final long GIB = 1L << 30;

  /** A slab-owning entry with a controllable staleness and dependency. */
  private static final class Stub extends Entry {
    final Object key;
    final Object dependency;
    boolean stale;
    int evictions;

    Stub(final Object key, final Object dependency) {
      this.key = key;
      this.dependency = dependency;
    }

    @Override
    protected boolean isStale() {
      return stale;
    }

    @Override
    protected boolean dependsOn(final Object identity) {
      return dependency == identity;
    }

    @Override
    protected void evicted() {
      evictions++;
    }
  }

  private static Stub publish(final Partition partition, final long bytes, final Object key, final Object dependency) {
    final Reservation reservation = partition.reserve(bytes);
    assertNotNull(reservation);
    try (reservation) {
      final Stub stub = new Stub(key, dependency);
      reservation.publish(stub);
      stub.release(); // the publisher's lease
      return stub;
    }
  }

  private static Stub find(final Partition partition, final Object key) {
    return (Stub) partition.acquire(entry -> entry instanceof Stub stub && stub.key == key);
  }

  @Test
  void exceptionalAllocationRefundsBothPartitionsAndAllowsAnotherReservation() {
    final FrameSlotAllocator delegate = new FrameSlotAllocator(4L << 20);
    try {
      final FailingAllocator allocator = new FailingAllocator(delegate);
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 8000, 800);
      for (final Partition partition : new Partition[] {budget.numeric(), budget.packed()}) {
        final long bytes = partition.metrics().maximumBytes();
        for (final boolean error : new boolean[] {false, true}) {
          allocator.fail = true;
          allocator.error = error;
          if (error) {
            assertThrows(OutOfMemoryError.class, () -> partition.reserve(bytes));
          } else {
            assertThrows(IllegalStateException.class, () -> partition.reserve(bytes));
          }
          assertEquals(0L, partition.metrics().inFlightBytes());
          partition.clear();
          assertEquals(0L, partition.metrics().inFlightBytes());
          try (Reservation reservation = partition.reserve(bytes)) {
            assertNotNull(reservation);
            assertEquals(bytes, partition.metrics().inFlightBytes());
          }
          assertEquals(0L, partition.metrics().inFlightBytes());
          assertEquals(0L, delegate.getSlabBytes());
        }
      }
    } finally {
      delegate.shutdown();
    }
  }

  private static final class FailingAllocator implements MemorySegmentAllocator {
    private final FrameSlotAllocator delegate;
    private boolean fail;
    private boolean error;

    private FailingAllocator(final FrameSlotAllocator delegate) {
      this.delegate = delegate;
    }

    @Override
    public MemorySegment allocateSlab(final long bytes) {
      if (fail) {
        fail = false;
        if (error) {
          throw new OutOfMemoryError("injected allocation failure");
        }
        throw new IllegalStateException("injected allocation failure");
      }
      return delegate.allocateSlab(bytes);
    }

    @Override
    public boolean releaseSlab(final MemorySegment slab) {
      return delegate.releaseSlab(slab);
    }

    @Override
    public void init(final long maxBufferSize) {
      delegate.init(maxBufferSize);
    }

    @Override
    public boolean isInitialized() {
      return delegate.isInitialized();
    }

    @Override
    public void free() {
      delegate.free();
    }

    @Override
    public MemorySegment allocate(final long size) {
      return delegate.allocate(size);
    }

    @Override
    public void release(final MemorySegment segment) {
      delegate.release(segment);
    }

    @Override
    public long getMaxBufferSize() {
      return delegate.getMaxBufferSize();
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      delegate.resetSegment(segment);
    }
  }

  @Test
  void defaultsDeriveFromTheAllocatorAllowanceAndOverridesStayWithinIt() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    try {
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 8 * GIB, -1);
      assertEquals(GIB, budget.metrics().effectiveBytes());
      assertEquals(768L << 20, budget.metrics().numeric().maximumBytes());
      assertEquals(256L << 20, budget.metrics().packed().maximumBytes());
      assertEquals(2 * GIB, new DerivedCacheBudget(allocator, 16 * GIB, -1).metrics().effectiveBytes());
      assertEquals(123, new DerivedCacheBudget(allocator, 8 * GIB, 123).metrics().effectiveBytes());
      assertEquals(8 * GIB, new DerivedCacheBudget(allocator, 8 * GIB, Long.MAX_VALUE).metrics().effectiveBytes(),
          "an override never exceeds the allocator allowance");
      assertEquals(0, new DerivedCacheBudget(allocator, 8 * GIB, 0).metrics().effectiveBytes());
      assertEquals(0, new DerivedCacheBudget(allocator, 0, -1).metrics().effectiveBytes());
      assertThrows(IllegalArgumentException.class, () -> new DerivedCacheBudget(allocator, -1, -1));
      assertThrows(IllegalArgumentException.class, () -> new DerivedCacheBudget(allocator, GIB, -2));
      assertSame(allocator, budget.allocator());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void simultaneousConstructionAndRetainedEntriesNeverExceedTheCombinedBudget() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    try {
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 800 * 8, 800);
      final Object numericKey = new Object();
      final Object packedKey = new Object();
      final CountDownLatch building = new CountDownLatch(2);
      final CountDownLatch publish = new CountDownLatch(1);
      try (var executor = Executors.newFixedThreadPool(2)) {
        final var numeric = executor.submit(() -> construct(budget.numeric(), numericKey, 600, building, publish));
        final var packed = executor.submit(() -> construct(budget.packed(), packedKey, 200, building, publish));
        building.await();
        try {
          assertEquals(800, budget.metrics().inFlightBytes());
          assertEquals(0, budget.metrics().retainedBytes());
          assertEquals(2 * 4096, allocator.getSlabBytes(), "both slabs are mapped and charged while building");
          assertNull(budget.numeric().reserve(1));
          assertNull(budget.packed().reserve(1));
        } finally {
          publish.countDown();
        }
        numeric.get();
        packed.get();
      }
      assertEquals(800, budget.metrics().retainedBytes());
      assertEquals(0, budget.metrics().inFlightBytes());
      assertEquals(2, budget.metrics().rejectedBytes());
      assertEquals(2, budget.metrics().numeric().entries() + budget.metrics().packed().entries());
      final Stub retained = find(budget.numeric(), numericKey);
      assertNotNull(retained);
      retained.release();
      budget.numeric().clear();
      budget.packed().clear();
      assertEquals(0, budget.metrics().retainedBytes());
      assertEquals(0, budget.metrics().retiredLeasedBytes());
      assertEquals(0, allocator.getSlabBytes(), "every slab is refunded once nothing holds it");
    } finally {
      allocator.shutdown();
    }
  }

  private static void construct(final Partition partition, final Object key, final long bytes,
      final CountDownLatch building, final CountDownLatch publish) {
    try (Reservation reservation = partition.reserve(bytes)) {
      building.countDown();
      assertNotNull(reservation);
      publish.await();
      final Stub stub = new Stub(key, null);
      reservation.publish(stub);
      stub.release();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void pressureEvictsOnlyUnleasedEntriesAndExplicitEvictionDefersFreeingToTheLastReader() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    try {
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 1000 * 8, 1000);
      final Partition numeric = budget.numeric(); // 750 bytes
      final Object first = new Object();
      final Object second = new Object();
      final Stub a = publish(numeric, 400, first, null);
      final Stub reader = find(numeric, first);
      assertSame(a, reader, "a reader holds a lease on the oldest entry");
      final Stub b = publish(numeric, 300, second, null);
      assertEquals(700, numeric.metrics().retainedBytes());
      final Reservation third = numeric.reserve(200);
      assertNotNull(third, "the unleased entry (b) is evicted although a is colder");
      assertEquals(1, b.evictions);
      assertEquals(0, a.evictions, "an entry under a lease is in use and stays resident");
      assertEquals(400, numeric.metrics().retainedBytes());
      assertEquals(0, numeric.metrics().retiredLeasedBytes(), "b had no reader, so its slab is gone");
      assertEquals(200, numeric.metrics().inFlightBytes());
      assertEquals(2 * 4096, allocator.getSlabBytes());
      assertNull(numeric.reserve(200), "a is leased, so nothing else can make room");
      third.close();
      assertEquals(4096, allocator.getSlabBytes(), "an aborted reservation releases its slab");
      numeric.clear();
      assertEquals(1, a.evictions);
      assertNull(find(numeric, first), "an evicted entry is no longer served");
      assertEquals(0, numeric.metrics().retainedBytes());
      assertEquals(400, numeric.metrics().retiredLeasedBytes(), "a stays mapped for its reader");
      assertEquals(4096, allocator.getSlabBytes());
      assertNull(numeric.reserve(400), "retired-but-leased bytes still count against the share");
      assertNotNull(numeric.reserve(350).slab(), "what the reader does not pin is available");
      a.release();
      assertEquals(0, numeric.metrics().retiredLeasedBytes());
      assertEquals(4096, allocator.getSlabBytes(), "the last lease freed a's slab; the reservation remains");
      assertFalse(a.tryAcquire(), "a freed entry cannot be acquired again");
      numeric.clear();
    } finally {
      budget(allocator);
    }
  }

  @Test
  void staleAndDependentEntriesAreReapedAndTheAllocatorCanRefuseWithoutAnError() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try {
      // D is 8 MiB, far beyond what this allocator can commit: the allocator refuses, nothing breaks.
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 64L << 20, -1);
      assertNull(budget.numeric().reserve(2L << 20));
      assertEquals(2L << 20, budget.metrics().rejectedBytes());
      assertEquals(0, budget.metrics().inFlightBytes());
      final Object identity = new Object();
      final Stub snapshot = publish(budget.numeric(), 100, "snapshot", null);
      final Stub packed = publish(budget.packed(), 50, "packed", identity);
      final Stub unrelated = publish(budget.packed(), 50, "other", null);
      budget.packed().evictDependents(identity);
      assertEquals(1, packed.evictions);
      assertEquals(0, unrelated.evictions);
      assertEquals(50, budget.metrics().packed().retainedBytes());
      snapshot.stale = true;
      final Reservation admitted = budget.numeric().reserve(1);
      assertNotNull(admitted, "reaping a stale entry happens before admission");
      assertEquals(1, snapshot.evictions);
      admitted.close();
      budget.numeric().clear();
      budget.packed().clear();
      assertEquals(0, allocator.getSlabBytes());
    } finally {
      budget(allocator);
    }
  }

  private static void budget(final FrameSlotAllocator allocator) {
    // Release whatever a failed assertion left behind so the allocator can shut down.
    try {
      allocator.shutdown();
    } catch (final IllegalStateException leaked) {
      // Reported through the failing assertion; the virtual reservation ends with the process.
    }
  }

  @Test
  void failureReleasesReservationsAndSaturatingArithmeticRejectsUnrepresentableSizes() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    try {
      assertEquals(Long.MAX_VALUE, DerivedCacheBudget.saturatedMultiply(Long.MAX_VALUE, 2));
      assertEquals(0, DerivedCacheBudget.saturatedMultiply(Long.MAX_VALUE, 0));
      assertEquals(Long.MAX_VALUE, DerivedCacheBudget.saturatedAdd(Long.MAX_VALUE, 1));
      assertEquals(64, DerivedCacheBudget.alignUp(1));
      assertEquals(Long.MAX_VALUE, DerivedCacheBudget.alignUp(Long.MAX_VALUE - 3));
      final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, Long.MAX_VALUE, -1);
      assertNull(budget.numeric().reserve(Long.MAX_VALUE / 8), "the allocator cannot commit it");
      assertEquals(0, budget.metrics().inFlightBytes());
      assertNull(budget.numeric().reserve(Long.MAX_VALUE));
      assertEquals(Long.MAX_VALUE, budget.metrics().rejectedBytes(), "rejected bytes saturate");
      final Reservation reservation = budget.numeric().reserve(4096);
      assertNotNull(reservation);
      assertEquals(4096, budget.metrics().inFlightBytes());
      reservation.close();
      reservation.close();
      assertEquals(0, budget.metrics().inFlightBytes(), "closing twice releases once");
      assertEquals(0, allocator.getSlabBytes());
      assertThrows(IllegalArgumentException.class, () -> budget.numeric().reserve(0));
      final DerivedCacheBudget disabled = new DerivedCacheBudget(allocator, 800, 0);
      assertNull(disabled.numeric().reserve(1));
      assertNull(disabled.packed().reserve(1));
    } finally {
      allocator.shutdown();
    }
  }
}

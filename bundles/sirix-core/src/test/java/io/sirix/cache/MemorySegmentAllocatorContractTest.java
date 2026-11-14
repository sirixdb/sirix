/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a caller of {@link MemorySegmentAllocator} relies on, asserted on the one allocator there
 * is: which allocator a process gets, what a request is lent, what is refused, and that concurrent
 * callers never share a segment.
 *
 * <p>
 * These are the assertions of the tests of the two removed pool allocators that still describe the
 * configured allocator. Where the frame allocator answers differently, the difference is stated: it
 * serves a size beyond its largest class instead of rejecting it, and it ignores the release of
 * memory it never lent instead of throwing. Every assertion is on a value the allocator maintains;
 * nothing here measures time. Each test owns its allocator, so no figure depends on what ran
 * before.
 */
final class MemorySegmentAllocatorContractTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;

  /** At the 64 MiB from which the allocator reserves headroom, as a database's allocator does. */
  private static final long BUDGET = 64L * MIB;

  private static final long MARK = 0x0123_4567_89AB_CDEFL;

  private final List<FrameSlotAllocator> allocators = new ArrayList<>();

  /** An allocator of this test's own, shut down after it. */
  private FrameSlotAllocator allocator() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    allocators.add(allocator);
    return allocator;
  }

  /**
   * Shut down here and not in a {@code finally} of the test: a shutdown is refused while a segment is
   * lent, and from a {@code finally} that refusal would replace the assertion that left it lent.
   */
  @AfterEach
  void shutDownAllocators() {
    for (final FrameSlotAllocator allocator : allocators) {
      allocator.shutdown();
    }
  }

  @Test
  void theConfiguredAllocatorIsTheFrameAllocator() {
    final MemorySegmentAllocator configured = Allocators.getInstance();
    assertSame(FrameSlotAllocator.getInstance(), configured);
    assertSame(configured, Allocators.resolve(null), "the property left unset");
    assertSame(configured, Allocators.resolve(Allocators.FRAME));
    assertSame(configured, Allocators.resolve("FRAME"), "the value is not case sensitive");
  }

  @ParameterizedTest
  @ValueSource(strings = {"pool", "windowspool", "POOL", "linux", ""})
  void aChoiceOtherThanTheFrameAllocatorIsRefused(final String choice) {
    final IllegalArgumentException refusal =
        assertThrows(IllegalArgumentException.class, () -> Allocators.resolve(choice));
    assertTrue(refusal.getMessage().contains(Allocators.ALLOCATOR_PROPERTY + "=" + choice),
        "the refusal names the setting to remove: " + refusal.getMessage());
  }

  /** The size table the pool allocators shared, asserted on the classes of the frame allocator. */
  @ParameterizedTest
  @CsvSource({"1, 0, 4096", "3000, 0, 4096", "4096, 0, 4096", "5000, 1, 8192", "8192, 1, 8192", "12000, 2, 16384",
      "50000, 4, 65536", "262143, 6, 262144", "262144, 6, 262144"})
  void aSizeMapsToTheSmallestClassThatHoldsIt(final long size, final int classIndex, final long capacity) {
    assertEquals(classIndex, FrameSlotAllocator.indexForSize(size));
    assertEquals(capacity, FrameSlotAllocator.SIZE_CLASSES[classIndex]);
    assertEquals(capacity, MemorySegmentAllocator.SEGMENT_SIZES[classIndex],
        "the sizes the interface names are the allocator's classes");
  }

  @Test
  void aSizeBeyondTheLargestClassHasNoClass() {
    assertEquals(MemorySegmentAllocator.SEGMENT_SIZES.length, FrameSlotAllocator.SIZE_CLASSES.length);
    assertEquals(-1, FrameSlotAllocator.indexForSize(MemorySegmentAllocator.TWO_FIFTYSIX_KB + 1L));
    assertEquals(-1, FrameSlotAllocator.indexForSize(Long.MAX_VALUE));
  }

  @ParameterizedTest
  @CsvSource({"1, 4096", "4096, 4096", "4097, 8192", "5000, 8192", "8193, 16384", "10000, 16384", "16384, 16384",
      "16385, 32768", "32768, 32768", "32769, 65536", "50000, 65536", "65536, 65536", "65537, 131072", "100000, 131072",
      "131072, 131072", "131073, 262144", "200000, 262144", "262144, 262144"})
  void aRequestIsLentTheWholeSegmentOfItsSizeClass(final long requested, final long capacity) {
    final FrameSlotAllocator allocator = allocator();
    // Many rounds: an allocation charged with the request and refunded with the segment would drift.
    for (int round = 0; round < 100; round++) {
      final MemorySegment segment = allocator.allocate(requested);
      assertEquals(capacity, segment.byteSize());
      assertEquals(capacity, allocator.getActiveMemoryBytes(), "charged with the segment, not with the request");
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0L, MARK + round);
      segment.set(ValueLayout.JAVA_BYTE, capacity - 1L, (byte) round);
      assertEquals(MARK + round, segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0L));
      assertEquals((byte) round, segment.get(ValueLayout.JAVA_BYTE, capacity - 1L), "usable up to its last byte");
      allocator.release(segment);
      assertEquals(0L, allocator.getActiveMemoryBytes());
    }
    assertEquals(capacity, allocator.getPhysicalMemoryBytes(), "one segment, lent again in every round");
  }

  @Test
  void aSizeThatIsNotPositiveIsRefused() {
    final FrameSlotAllocator allocator = allocator();
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(0L));
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(-1L));
    assertThrows(IllegalArgumentException.class, () -> allocator.allocate(Long.MIN_VALUE));
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  /**
   * The pool allocators rejected every size beyond their largest segment. The frame allocator serves
   * such a size from the same budget and refuses only what the budget cannot hold.
   */
  @Test
  void aSizeBeyondTheLargestClassIsServedUnlessTheBudgetCannotHoldIt() {
    final FrameSlotAllocator allocator = allocator();
    final MemorySegment large = allocator.allocate(550_000L);
    assertEquals(550_000L, large.byteSize());
    assertEquals(550_000L, allocator.getActiveMemoryBytes());
    large.set(ValueLayout.JAVA_BYTE, 549_999L, (byte) 99);
    assertEquals((byte) 99, large.get(ValueLayout.JAVA_BYTE, 549_999L));
    allocator.release(large);
    assertEquals(0L, allocator.getPhysicalMemoryBytes(), "its memory goes back with it");

    assertThrows(OutOfMemoryError.class, () -> allocator.allocate(Long.MAX_VALUE));
    assertEquals(0L, allocator.getPhysicalMemoryBytes(), "a refusal commits nothing");
    assertEquals(0L, allocator.getActiveMemoryBytes());
  }

  @Test
  void aLaterInitKeepsTheFirstAllowance() {
    final FrameSlotAllocator allocator = allocator();
    final MemorySegment lent = allocator.allocate(4L * KIB);
    lent.set(ValueLayout.JAVA_LONG, 0L, MARK);

    allocator.init(2L * BUDGET);
    allocator.init(BUDGET / 2L);

    assertTrue(allocator.isInitialized());
    assertEquals(BUDGET, allocator.getMaxBufferSize(), "what is lent was admitted against the first allowance");
    assertEquals(MARK, lent.get(ValueLayout.JAVA_LONG, 0L), "and it survives the call");
    allocator.release(lent);
  }

  /** The pool allocators threw here. The frame allocator leaves memory it never lent to its owner. */
  @Test
  void releasingMemoryTheAllocatorNeverLentChangesNothing() {
    final FrameSlotAllocator allocator = allocator();
    final int classIndex = FrameSlotAllocator.indexForSize(4L * KIB);
    final MemorySegment lent = allocator.allocate(4L * KIB);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment foreign = arena.allocate(550_000L);
      foreign.set(ValueLayout.JAVA_LONG, 0L, MARK);
      allocator.release(foreign);
      allocator.release(arena.allocate(4L * KIB));
      allocator.release(null);
      assertTrue(foreign.scope().isAlive(), "its owner alone ends its life");
      assertEquals(MARK, foreign.get(ValueLayout.JAVA_LONG, 0L));
    }
    assertEquals(0L, allocator.releaseCount(classIndex));
    assertEquals(1, allocator.liveSlotCount(classIndex));
    assertEquals(4L * KIB, allocator.getActiveMemoryBytes());
    allocator.release(lent);
    assertEquals(1L, allocator.releaseCount(classIndex));
    assertEquals(0L, allocator.getActiveMemoryBytes());
  }

  /**
   * Eight workers allocate through the interface in every size class and each holds several segments
   * at a time. A segment lent to two of them at once would carry the other's mark when it is
   * returned.
   */
  @Test
  void concurrentCallersHoldDisjointSegmentsAndReturnAllOfThem() throws Exception {
    final FrameSlotAllocator allocator = allocator();
    final int workers = 8;
    final int rounds = 1_000;
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicLong returned = new AtomicLong();
    final List<Future<Long>> lentByWorker = new ArrayList<>(workers);
    for (int worker = 0; worker < workers; worker++) {
      final int workerId = worker;
      lentByWorker.add(pool.submit(() -> {
        start.await();
        return churn(allocator, workerId, rounds, returned);
      }));
    }

    start.countDown();
    pool.shutdown();
    long lent = 0L;
    for (final Future<Long> worker : lentByWorker) {
      // The bound is a hang guard, as in the other concurrency tests of this package.
      lent += worker.get(60, TimeUnit.SECONDS);
    }
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals((long) workers * rounds, lent);
    assertEquals(lent, returned.get(), "every segment went back");
    assertEquals(0L, allocator.getActiveMemoryBytes());
    for (int classIndex = 0; classIndex < FrameSlotAllocator.SIZE_CLASSES.length; classIndex++) {
      assertEquals(0, allocator.liveSlotCount(classIndex));
      assertEquals(allocator.allocateCount(classIndex), allocator.releaseCount(classIndex));
    }
    assertTrue(allocator.getPhysicalMemoryBytes() <= allocator.getMaxBufferSize(),
        "what stays committed for reuse fits the allowance");
  }

  /**
   * One worker's rounds: lend, mark, hold up to six, return the oldest. Returns how many it was lent.
   */
  private static long churn(final FrameSlotAllocator allocator, final int workerId, final int rounds,
      final AtomicLong returned) {
    final int held = 6;
    final ArrayDeque<MemorySegment> mine = new ArrayDeque<>(held);
    long lent = 0L;
    try {
      for (int round = 0; round < rounds; round++) {
        final long size = FrameSlotAllocator.SIZE_CLASSES[(workerId + round) % FrameSlotAllocator.SIZE_CLASSES.length];
        final MemorySegment segment = allocator.allocate(size);
        lent++;
        final long mark = ((long) workerId << 32) | round;
        segment.set(ValueLayout.JAVA_LONG, 0L, mark);
        segment.set(ValueLayout.JAVA_LONG, size - Long.BYTES, mark);
        mine.addLast(segment);
        if (mine.size() == held) {
          giveBack(allocator, mine.removeFirst(), workerId, returned);
        }
      }
    } finally {
      while (!mine.isEmpty()) {
        giveBack(allocator, mine.removeFirst(), workerId, returned);
      }
    }
    return lent;
  }

  private static void giveBack(final FrameSlotAllocator allocator, final MemorySegment segment, final int workerId,
      final AtomicLong returned) {
    final long first = segment.get(ValueLayout.JAVA_LONG, 0L);
    final long last = segment.get(ValueLayout.JAVA_LONG, segment.byteSize() - Long.BYTES);
    allocator.release(segment);
    returned.incrementAndGet();
    assertEquals(workerId, (int) (first >>> 32), "another caller wrote into a segment this one held");
    assertEquals(first, last, "another caller wrote into a segment this one held");
  }

  /**
   * With the pool allocators went the scratch source that served any other implementation. Scratch
   * buffers and execution capacity now come from the frame allocator alone, and asking another
   * implementation for them fails instead of mapping memory outside the budget.
   */
  @Test
  void anotherImplementationLendsNoScratchAndGrantsNoExecutionCapacity() {
    final MemorySegmentAllocator other = new ArenaBacked();
    assertThrows(IllegalArgumentException.class, () -> ScratchSource.of(other));
    final ScratchLease lease = new ScratchLease(other);
    assertThrows(IllegalArgumentException.class, () -> lease.acquire(4L * KIB, ScratchUse.READ));
    assertThrows(UnsupportedOperationException.class, other::aggregationBudget);
    assertSame(ScratchStats.EMPTY, other.scratchStats(), "it keeps no scratch counters either");
  }

  /** An implementation that is not the frame allocator, as a test double of the interface is. */
  private static final class ArenaBacked implements MemorySegmentAllocator {
    private final Arena arena = Arena.ofAuto();

    @Override
    public void init(final long maxBufferSize) {
      // nothing to reserve
    }

    @Override
    public boolean isInitialized() {
      return true;
    }

    @Override
    public void free() {
      // the collector frees the arena
    }

    @Override
    public MemorySegment allocate(final long size) {
      return arena.allocate(size);
    }

    @Override
    public void release(final MemorySegment segment) {
      // the collector frees the arena
    }

    @Override
    public long getMaxBufferSize() {
      return MIB;
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      segment.fill((byte) 0);
    }
  }
}

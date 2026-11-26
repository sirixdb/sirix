/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.Databases;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MEM-202 — priority eviction up to the shortfall. The pressure listener receives the shortfall in
 * bytes and evicts unguarded entries in this order: combined record pages, then HOT leaves, then
 * fragments, the record-fragment and native HOT fragment caches in proportion to their current
 * weights; it never evicts from a later tier while an earlier tier holds an unguarded entry, stops
 * once the frames it freed cover the shortfall rounded up to one batch (2 MiB), and never evicts a
 * guarded entry. A cache under its cap is not shed by a quarter.
 *
 * <p>
 * The caches hold pages over real allocator frames of 64 KiB, so "freed" is measured twice: as
 * cache weight and as the bytes the allocator holds recycled.
 */
@MemPlanned(step = 2, value = {"MEM-202"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class PriorityEvictionTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;
  private static final long FRAME = 64 * KIB;
  private static final int FRAME_CLASS = FrameSlotAllocator.indexForSize(FRAME);
  /** The frame class of the request whose byte shortfall the byte cases hand to the listener. */
  private static final int REQUESTING_CLASS = 0;
  private static final long SMALL_FRAME = 32 * KIB;
  private static final int SMALL_CLASS = FrameSlotAllocator.indexForSize(SMALL_FRAME);
  private static final long BUDGET = 32 * MIB;
  private static final int RECORD_PAGES = 128;
  private static final int HOT_LEAVES = 64;
  private static final int RECORD_FRAGMENTS = 96;
  private static final int NATIVE_FRAGMENTS = 32;

  /** One cached page and the key it is cached under. */
  private record Entry(PageReference key, MemCachePageStub page) {
  }

  private final HeapCacheBudget heap = new HeapCacheBudget(Runtime.getRuntime().maxMemory(), ignored -> null);
  private final List<MemCachePageStub> guarded = new ArrayList<>();
  private FrameSlotAllocator.PressureListener previousListener;
  private FrameSlotAllocator allocator;
  private BufferManagerImpl manager;
  private long nextKey;

  @BeforeEach
  void setUp() {
    previousListener = FrameSlotAllocator.pressureListener();
    allocator = new FrameSlotAllocator(BUDGET);
  }

  @AfterEach
  void tearDown() {
    try {
      for (final MemCachePageStub page : guarded) {
        page.releaseGuard();
      }
      if (manager != null) {
        manager.close();
      }
    } finally {
      FrameSlotAllocator.setPressureListener(previousListener);
      allocator.shutdown();
    }
  }

  private void openManager(final Map<String, String> properties) {
    manager = MemCacheSeam.newBufferManager(FrameBudgetOracle.frameCommitLimit(allocator), properties, heap);
  }

  private List<Entry> fill(final ShardedPageCache<CacheablePage> cache, final int count) {
    return fill(cache, count, FRAME);
  }

  private List<Entry> fill(final ShardedPageCache<CacheablePage> cache, final int count, final long frameBytes) {
    final List<Entry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final long index = nextKey++;
      final MemCachePageStub page = MemCachePageStub.overFrame(index, allocator, frameBytes);
      final PageReference key = MemCachePageStub.keyOf(index);
      cache.put(key, page);
      entries.add(new Entry(key, page));
    }
    return entries;
  }

  /** Guards every entry from {@code firstGuarded} on; the entries before it stay evictable. */
  private void guardFrom(final List<Entry> entries, final int firstGuarded) {
    for (int i = firstGuarded; i < entries.size(); i++) {
      assertTrue(entries.get(i).page().acquireGuard());
      guarded.add(entries.get(i).page());
    }
  }

  private static boolean cached(final ShardedPageCache<CacheablePage> cache, final Entry entry) {
    return cache.asMap().get(entry.key()) == entry.page();
  }

  private static long evictedBytes(final List<Entry> entries, final ShardedPageCache<CacheablePage> cache) {
    long evicted = 0L;
    for (final Entry entry : entries) {
      if (!cached(cache, entry)) {
        evicted += FRAME;
      }
    }
    return evicted;
  }

  private static void assertWithinShortfallAndOneBatch(final long freed, final long shortfall) {
    assertTrue(freed >= shortfall, "MEM-202: freed " + freed + " bytes must cover the shortfall " + shortfall);
    assertTrue(freed <= shortfall + MemCacheSeam.BATCH_BYTES,
        "MEM-202: freed " + freed + " bytes must not exceed shortfall " + shortfall + " by more than one batch");
  }

  @Test
  @DisplayName("MEM-202: a shortfall of one frame evicts only combined record pages")
  void aShortfallOfOneFrameEvictsOnlyCombinedRecordPages() {
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final ShardedPageCache<CacheablePage> leaves = MemCacheSeam.hotLeaves(manager);
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final ShardedPageCache<CacheablePage> nativeFragments = MemCacheSeam.nativeHotFragments(manager);
    final List<Entry> pageEntries = fill(pages, RECORD_PAGES);
    final List<Entry> leafEntries = fill(leaves, HOT_LEAVES);
    final List<Entry> fragmentEntries = fill(fragments, RECORD_FRAGMENTS);
    final List<Entry> nativeEntries = fill(nativeFragments, NATIVE_FRAGMENTS);
    final long recycledBefore = FrameBudgetOracle.recycledBytes(allocator, FRAME_CLASS);

    MemCacheSeam.firePressureBytes(FRAME, REQUESTING_CLASS);

    final long freed = evictedBytes(pageEntries, pages);
    assertWithinShortfallAndOneBatch(freed, FRAME);
    assertEquals(freed, FrameBudgetOracle.recycledBytes(allocator, FRAME_CLASS) - recycledBefore,
        "MEM-202: freed frames enter their class's recycled set");
    assertEquals(0L, evictedBytes(leafEntries, leaves), "MEM-202: HOT leaves hold while record pages can be evicted");
    assertEquals(0L, evictedBytes(fragmentEntries, fragments),
        "MEM-202: fragments hold while record pages can be evicted");
    assertEquals(0L, evictedBytes(nativeEntries, nativeFragments),
        "MEM-202: native HOT fragments hold while record pages can be evicted");
  }

  @Test
  @DisplayName("MEM-202: a shortfall beyond the unguarded record pages continues with HOT leaves, not fragments")
  void aLargerShortfallContinuesWithHotLeavesBeforeFragments() {
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final ShardedPageCache<CacheablePage> leaves = MemCacheSeam.hotLeaves(manager);
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final ShardedPageCache<CacheablePage> nativeFragments = MemCacheSeam.nativeHotFragments(manager);
    final List<Entry> pageEntries = fill(pages, RECORD_PAGES);
    final List<Entry> leafEntries = fill(leaves, HOT_LEAVES);
    final List<Entry> fragmentEntries = fill(fragments, RECORD_FRAGMENTS);
    final List<Entry> nativeEntries = fill(nativeFragments, NATIVE_FRAGMENTS);
    final int unguardedPages = 16;
    guardFrom(pageEntries, unguardedPages);
    final long shortfall = 3 * MIB;

    MemCacheSeam.firePressureBytes(shortfall, REQUESTING_CLASS);

    final long fromPages = evictedBytes(pageEntries, pages);
    final long fromLeaves = evictedBytes(leafEntries, leaves);
    assertEquals(unguardedPages * FRAME, fromPages, "MEM-202: every unguarded record page goes before any HOT leaf");
    assertTrue(fromLeaves > 0, "MEM-202: HOT leaves are evicted next");
    assertWithinShortfallAndOneBatch(fromPages + fromLeaves, shortfall);
    assertEquals(0L, evictedBytes(fragmentEntries, fragments),
        "MEM-202: fragments hold while HOT leaves can be evicted");
    assertEquals(0L, evictedBytes(nativeEntries, nativeFragments),
        "MEM-202: native HOT fragments hold while HOT leaves can be evicted");
    for (int i = unguardedPages; i < pageEntries.size(); i++) {
      assertTrue(cached(pages, pageEntries.get(i)), "MEM-202: a guarded record page is never evicted");
    }
  }

  @Test
  @DisplayName("MEM-202: the two fragment caches shed in proportion to their weights, within one entry")
  void theFragmentTierShedsInProportionToTheWeightsOfItsTwoCaches() {
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final ShardedPageCache<CacheablePage> leaves = MemCacheSeam.hotLeaves(manager);
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final ShardedPageCache<CacheablePage> nativeFragments = MemCacheSeam.nativeHotFragments(manager);
    final List<Entry> pageEntries = fill(pages, RECORD_PAGES);
    final List<Entry> leafEntries = fill(leaves, HOT_LEAVES);
    final List<Entry> fragmentEntries = fill(fragments, RECORD_FRAGMENTS);
    final List<Entry> nativeEntries = fill(nativeFragments, NATIVE_FRAGMENTS);
    final int unguarded = 8;
    guardFrom(pageEntries, unguarded);
    guardFrom(leafEntries, unguarded);
    final long fragmentWeight = fragments.getCurrentWeightBytes();
    final long nativeWeight = nativeFragments.getCurrentWeightBytes();
    final long shortfall = 2L * unguarded * FRAME + 4 * MIB;

    MemCacheSeam.firePressureBytes(shortfall, REQUESTING_CLASS);

    assertEquals(unguarded * FRAME, evictedBytes(pageEntries, pages), "MEM-202: record pages first");
    assertEquals(unguarded * FRAME, evictedBytes(leafEntries, leaves), "MEM-202: HOT leaves second");
    final long fromFragments = evictedBytes(fragmentEntries, fragments);
    final long fromNative = evictedBytes(nativeEntries, nativeFragments);
    final long tier = fromFragments + fromNative;
    assertWithinShortfallAndOneBatch(2L * unguarded * FRAME + tier, shortfall);
    final long expectedFragments = tier * fragmentWeight / (fragmentWeight + nativeWeight);
    final long expectedNative = tier * nativeWeight / (fragmentWeight + nativeWeight);
    assertTrue(Math.abs(fromFragments - expectedFragments) <= FRAME, "MEM-202: record fragments shed " + fromFragments
        + " of " + tier + ", proportional share " + expectedFragments);
    assertTrue(Math.abs(fromNative - expectedNative) <= FRAME,
        "MEM-202: native HOT fragments shed " + fromNative + " of " + tier + ", proportional share " + expectedNative);
  }

  @Test
  @DisplayName("MEM-202: a guarded entry is never evicted, however large the shortfall")
  void aGuardedEntryIsNeverEvicted() {
    openManager(Map.of());
    final List<ShardedPageCache<CacheablePage>> caches =
        List.of(MemCacheSeam.recordPages(manager), MemCacheSeam.hotLeaves(manager),
            MemCacheSeam.recordFragments(manager), MemCacheSeam.nativeHotFragments(manager));
    final List<List<Entry>> entries = new ArrayList<>();
    for (final ShardedPageCache<CacheablePage> cache : caches) {
      final List<Entry> filled = fill(cache, 32);
      guardFrom(filled, 24);
      entries.add(filled);
    }

    MemCacheSeam.firePressureBytes(BUDGET, REQUESTING_CLASS);

    for (int tier = 0; tier < caches.size(); tier++) {
      for (int i = 0; i < 32; i++) {
        final Entry entry = entries.get(tier).get(i);
        if (i >= 24) {
          assertTrue(cached(caches.get(tier), entry), "MEM-202: guarded entry " + i + " of tier " + tier + " evicted");
          assertFalse(entry.page().isTornDown(), "MEM-202: a guarded page must stay open");
        } else {
          assertFalse(cached(caches.get(tier), entry),
              "MEM-202: a shortfall beyond all unguarded frames must evict unguarded entry " + i + " of tier " + tier);
        }
      }
    }
  }

  @Test
  @DisplayName("MEM-202: a cache under its cap is not shed by a quarter of what it holds")
  void aCacheUnderItsCapIsNotShedByAQuarter() {
    openManager(Map.of(Databases.PROP_RECORD_PAGE_CACHE, Long.toString(FrameBudgetOracle.frameCommitLimit(allocator))));
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final List<Entry> entries = fill(pages, 320);
    final long held = pages.getCurrentWeightBytes();

    MemCacheSeam.firePressureBytes(FRAME, REQUESTING_CLASS);

    final long freed = evictedBytes(entries, pages);
    assertWithinShortfallAndOneBatch(freed, FRAME);
    assertTrue(freed < held / 4, "MEM-202: shedding " + freed + " of " + held + " held is the blanket quarter");
  }

  @Test
  @Timeout(60)
  @MemPlanned(step = 2, value = {"MEM-202", "MEM-103"})
  @DisplayName("MEM-202: the allocator hands the listener the kind, amount and class of the refused request")
  void theAllocatorHandsTheListenerTheKindAmountAndClassOfTheRefusedRequest() {
    final long limit = FrameBudgetOracle.frameCommitLimit(allocator);
    final List<MemorySegment> held = new ArrayList<>();
    for (long charged = 0; charged < limit; charged += FRAME) {
      held.add(allocator.allocate(FRAME));
    }
    MemorySegment small = null;
    try (MemPoolSeam.PressureProbe probe = MemPoolSeam.installPressureProbe()) {
      probe.onPressure(() -> allocator.release(held.remove(held.size() - 1)));

      try {
        small = allocator.allocate(FrameSlotAllocator.SIZE_CLASSES[0]);
      } catch (final OutOfMemoryError starved) {
        // JUnit aborts the whole run on an OutOfMemoryError; report it as this test's failure.
        fail("MEM-202: a 4 KiB frame must be allocatable once the listener released a frame: " + starved.getMessage(),
            starved);
      }

      assertFalse(probe.invocations().isEmpty(), "MEM-202: the refusal must reach the listener");
      final MemPoolSeam.PressureRequest request = MemPoolSeam.PressureRequest.decode(probe.invocations().get(0));
      assertFalse(request.slotShortfall(), "MEM-103: a full budget is a byte shortfall");
      assertEquals(FrameSlotAllocator.SIZE_CLASSES[0], request.amount(),
          "MEM-202: the shortfall is charged - (L - s) = one 4 KiB frame at a full budget");
      assertEquals(0, request.requestingClass(), "MEM-103: the listener learns the requesting frame class");
    } finally {
      if (small != null) {
        allocator.release(small);
      }
      held.forEach(allocator::release);
    }
  }

  /** Class {@link #FRAME_CLASS} has {@code slots} slots of address space; budget is left. */
  private void useAllocatorWithFrameClassCappedAt(final long slots) {
    allocator.shutdown();
    allocator =
        MemCacheSeam.newBoundedAllocator(BUDGET, RecordingVirtualMemory.withOsPageBytes(4096L), FRAME_CLASS, slots);
  }

  private void assertFrameClassExhaustedWithBudgetLeft() {
    assertEquals(allocator.slotCount(FRAME_CLASS), allocator.liveSlotCount(FRAME_CLASS),
        "the class must have no slot left");
    assertTrue(allocator.getPhysicalMemoryBytes() + FRAME <= FrameBudgetOracle.frameCommitLimit(allocator),
        "the budget must have room left, so this is slot pressure and not byte pressure");
  }

  @Test
  @DisplayName("MEM-202: a slot shortfall evicts one frame of the requesting class and no other class")
  void aSlotShortfallEvictsOneFrameOfTheRequestingClassOnly() {
    useAllocatorWithFrameClassCappedAt(64);
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final ShardedPageCache<CacheablePage> leaves = MemCacheSeam.hotLeaves(manager);
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final ShardedPageCache<CacheablePage> nativeFragments = MemCacheSeam.nativeHotFragments(manager);
    // The other class goes first, so the clock order meets its frames before the class-c frames.
    final List<Entry> smallEntries = fill(pages, 8, SMALL_FRAME);
    final List<Entry> pageEntries = fill(pages, 24);
    final List<Entry> leafEntries = fill(leaves, 16);
    final List<Entry> fragmentEntries = fill(fragments, 16);
    final List<Entry> nativeEntries = fill(nativeFragments, 8);
    assertFrameClassExhaustedWithBudgetLeft();
    final long recycledSmall = FrameBudgetOracle.recycledBytes(allocator, SMALL_CLASS);
    final long committed = allocator.getPhysicalMemoryBytes();

    MemCacheSeam.fireSlotPressure(FRAME_CLASS);

    assertEquals(1, allocator.recycledSlotCount(FRAME_CLASS), "MEM-202: eviction stops once one slot is recycled");
    assertEquals(FRAME, evictedBytes(pageEntries, pages), "MEM-202: the record-page tier gives that slot first");
    assertEquals(0L, evictedBytes(smallEntries, pages), "MEM-202: frames of other classes are retained");
    assertEquals(recycledSmall, FrameBudgetOracle.recycledBytes(allocator, SMALL_CLASS));
    assertEquals(0L, evictedBytes(leafEntries, leaves), "MEM-202: later tiers hold while the earlier one has a frame");
    assertEquals(0L, evictedBytes(fragmentEntries, fragments));
    assertEquals(0L, evictedBytes(nativeEntries, nativeFragments));
    assertEquals(committed, allocator.getPhysicalMemoryBytes(), "MEM-103: slot relief transfers no bytes");
  }

  @Test
  @DisplayName("MEM-202: a slot shortfall keeps the tier order, restricted to the requesting class")
  void aSlotShortfallKeepsTheTierOrderRestrictedToTheClass() {
    useAllocatorWithFrameClassCappedAt(64);
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);
    final ShardedPageCache<CacheablePage> leaves = MemCacheSeam.hotLeaves(manager);
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final List<Entry> smallEntries = fill(pages, 8, SMALL_FRAME);
    final List<Entry> pageEntries = fill(pages, 24);
    final List<Entry> leafEntries = fill(leaves, 24);
    final List<Entry> fragmentEntries = fill(fragments, 16);
    guardFrom(pageEntries, 0);
    assertFrameClassExhaustedWithBudgetLeft();

    MemCacheSeam.fireSlotPressure(FRAME_CLASS);

    assertEquals(1, allocator.recycledSlotCount(FRAME_CLASS), "MEM-202: exactly one slot of the class is recycled");
    assertEquals(0L, evictedBytes(pageEntries, pages), "MEM-202: guarded record pages are never evicted");
    assertEquals(0L, evictedBytes(smallEntries, pages),
        "MEM-202: unguarded frames of another class are not eligible, however early the tier");
    assertEquals(FRAME, evictedBytes(leafEntries, leaves), "MEM-202: the next tier gives the slot");
    assertEquals(0L, evictedBytes(fragmentEntries, fragments), "MEM-202: fragments hold while HOT leaves can give one");
  }
}

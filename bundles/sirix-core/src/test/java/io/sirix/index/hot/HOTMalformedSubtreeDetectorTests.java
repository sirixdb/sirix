/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.MalformedSubtree;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.PageResolver;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The detector must hold a leaf for as long as it reads that leaf's keys.
 *
 * <p>
 * A leaf the page cache hands out is a pointer to off-heap memory. Eviction releases that memory as
 * soon as nobody guards the leaf, the frame is handed out again, and a reader that kept going then
 * reads another page's bytes as keys. The detector did exactly that: it checked {@code isClosed()}
 * once and read without a guard, so a concurrent eviction made it report I5, I8 and I12 violations
 * in a sound trie and end the writing transaction.
 *
 * <p>
 * These tests evict at the one point where that used to matter, between the detector settling on a
 * leaf and reading it, through {@link HOTMalformedSubtreeDetector#setRetainedLeafTestHook}.
 * {@link FrameSlotAllocator#setPoisonOnReleaseForTesting} zero-fills a released frame, so a read
 * after eviction cannot return the old bytes by luck. Every leaf shares a one-byte prefix with its
 * sibling and differs from it only behind that prefix, which is what makes a zeroed leaf read as a
 * bare prefix and trip the invariants deterministically.
 *
 * <p>
 * These cases are {@link Isolated} and therefore have their own class next to
 * {@link HOTMalformedSubtreeDetectorTest}. It carries that name with an {@code s} because the
 * pull-request mutation lane runs exactly {@code <Class>Test} and {@code <Class>Tests} against a
 * changed class ({@code docs/VERIFICATION.md}).
 */
@Isolated
@DisplayName("HOTMalformedSubtreeDetector holds a leaf while it reads it")
final class HOTMalformedSubtreeDetectorTests {

  private static final byte[] VALUE = {1, 2, 3};

  /** Child 0: byte 1's most significant bit is clear in every key. */
  private static final byte[][] ZERO_BIT_KEYS = {{0x10, 0x00, 0x01}, {0x10, 0x01, 0x01}};

  /** Child 1: byte 1's most significant bit is set in every key. */
  private static final byte[][] ONE_BIT_KEYS = {{0x10, (byte) 0x80, 0x01}, {0x10, (byte) 0x81, 0x01}};

  /** Child 1 with a last key that has the bit clear and still sorts last: an I5 defect only. */
  private static final byte[][] ONE_BIT_KEYS_WITH_I5_DEFECT =
      {{0x10, (byte) 0x80, 0x01}, {0x10, (byte) 0x81, 0x01}, {0x11, 0x00, 0x01}};

  /** Child 0 with a last key that sorts after child 1's first key: an I12 defect only. */
  private static final byte[][] ZERO_BIT_KEYS_WITH_I12_DEFECT = {{0x10, 0x00, 0x01}, {0x10, (byte) 0x90, 0x01}};

  /**
   * Leaf reads of the clean two-leaf trie, in order: first and last key of child 0, first and last
   * key of child 1, then the I5 walk of child 1 (child 0's stored partial is zero).
   */
  private static final int LEAF_READS = 5;

  @BeforeEach
  void poisonReleasedFrames() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
  }

  @AfterEach
  void restore() {
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(null);
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
  }

  @ParameterizedTest(name = "eviction during leaf read {0} (0 = every read)")
  @ValueSource(ints = {0, 1, 2, 3, 4, 5})
  @DisplayName("a leaf evicted while its keys are read cannot fabricate a defect")
  void evictionDuringALeafReadFabricatesNoDefect(final int evictedRead) {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS, ONE_BIT_KEYS);
    final List<HOTLeafPage> evicted = new ArrayList<>();
    final AtomicInteger reads = new AtomicInteger();
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(leaf -> {
      final int read = reads.incrementAndGet();
      if (evictedRead == 0 || evictedRead == read) {
        // What the page cache does to a cold leaf under pressure, at the worst possible moment.
        leaf.close();
        evicted.add(leaf);
      }
    });
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);

      assertTrue(malformed.isEmpty(), "a sound trie was reported malformed: " + malformed);
      assertEquals(LEAF_READS, reads.get(), "the detector reads the leaves of this trie five times");
      assertEquals(evictedRead == 0
          ? LEAF_READS
          : 1, evicted.size(), "the eviction must have happened during a leaf read");
      for (final HOTLeafPage leaf : evicted) {
        assertTrue(leaf.isClosed(), "the deferred eviction completes when the detector lets go");
      }
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("a real defect is still reported, with its real key, when every read is evicted")
  void aRealDefectSurvivesEviction() {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS, ONE_BIT_KEYS_WITH_I5_DEFECT);
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(HOTLeafPage::close);
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);

      assertEquals(1, malformed.size(), "exactly the root owns the defect: " + malformed);
      assertEquals("I5-leaf-constancy", malformed.get(0).invariant(), malformed.get(0).detail());
      assertTrue(malformed.get(0).detail().endsWith("subtree key 110001"), malformed.get(0).detail());
      assertTrue(trie.reloads > 0, "an evicted leaf must have been resolved again");
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("interleaved child ranges are still reported when every read is evicted")
  void anInterleavedRangeSurvivesEviction() {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS_WITH_I12_DEFECT, ONE_BIT_KEYS);
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(HOTLeafPage::close);
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);

      assertEquals(1, malformed.size(), "exactly the root owns the defect: " + malformed);
      assertEquals("I12-subtree-ranges-disjoint", malformed.get(0).invariant(), malformed.get(0).detail());
      assertTrue(malformed.get(0).detail().contains("spans up to 109001"), malformed.get(0).detail());
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("a defect below an indirect child is found through that child when every read is evicted")
  void aDefectBelowAnIndirectChildSurvivesEviction() {
    // Root: byte 1's top bit. Its child 1 is an indirect page on byte 2's top bit over two leaves.
    // The stray key 110080 sits in the deepest leaf, keeps the inner node clean (byte 2's top bit is
    // set) and breaks the root's I5 (byte 1's top bit is clear), so only the nested walk can see it.
    final PageReference zeroBit = reference(leaf(1, ZERO_BIT_KEYS));
    final PageReference innerLow =
        reference(leaf(2, new byte[][] {{0x10, (byte) 0x80, 0x01}, {0x10, (byte) 0x80, 0x02}}));
    final PageReference innerHigh = reference(leaf(3,
        new byte[][] {{0x10, (byte) 0x80, (byte) 0x80}, {0x10, (byte) 0x80, (byte) 0x81}, {0x11, 0x00, (byte) 0x80}}));
    final PageReference inner = reference(HOTIndirectPage.createSpanNode(4, 1, 0, 1L << 47, new int[] {0, 1},
        new PageReference[] {innerLow, innerHigh}, 1));
    final PageReference root = reference(
        HOTIndirectPage.createSpanNode(5, 1, 0, 1L << 55, new int[] {0, 1}, new PageReference[] {zeroBit, inner}, 2));
    final ReloadingTrie trie = new ReloadingTrie(root);
    final List<HOTLeafPage> evicted = new ArrayList<>();
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(leaf -> {
      leaf.close();
      evicted.add(leaf);
    });
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(root, trie);

      assertEquals(1, malformed.size(), "exactly the root owns the defect: " + malformed);
      assertEquals(5, malformed.get(0).indirectPageKey());
      assertEquals("I5-leaf-constancy", malformed.get(0).invariant(), malformed.get(0).detail());
      assertTrue(malformed.get(0).detail().endsWith("subtree key 110080"), malformed.get(0).detail());
      assertFalse(evicted.isEmpty(), "the eviction must have happened during a leaf read");
      for (final HOTLeafPage leaf : evicted) {
        assertTrue(leaf.isClosed(), "the deferred eviction completes when the detector lets go");
      }
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("a read that throws releases its guard")
  void aFailingReadReleasesItsGuard() {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS, ONE_BIT_KEYS);
    final AtomicInteger reads = new AtomicInteger();
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(leaf -> {
      if (reads.incrementAndGet() == LEAF_READS) {
        throw new IllegalStateException("read failed");
      }
    });
    try {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> HOTMalformedSubtreeDetector.detect(trie.root, trie));

      assertEquals("read failed", failure.getMessage());
      assertEquals(LEAF_READS, reads.get());
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("a leaf evicted before the guard is taken is resolved again and then read")
  void aLeafEvictedBeforeTheGuardIsResolvedAgain() {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS, ONE_BIT_KEYS_WITH_I5_DEFECT);
    // Every read of child 1 loses the race as often as the detector tolerates, then wins.
    trie.unretainableResolutionsPerRead = HOTMalformedSubtreeDetector.MAX_LEAF_RETAIN_ATTEMPTS - 1;
    HOTMalformedSubtreeDetector.setRetainedLeafTestHook(leaf -> trie.unretainableSinceLastRead = 0);
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);

      assertEquals(1, malformed.size(), "the leaf was read after the retries: " + malformed);
      assertEquals("I5-leaf-constancy", malformed.get(0).invariant(), malformed.get(0).detail());
      assertTrue(trie.unretainableHandedOut > 0, "the detector must have met an unretainable leaf");
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @DisplayName("a leaf that cannot be retained is skipped after a bounded number of resolutions")
  void aLeafThatCannotBeRetainedIsSkipped() {
    final TwoLeafTrie trie = new TwoLeafTrie(ZERO_BIT_KEYS, ONE_BIT_KEYS_WITH_I5_DEFECT);
    trie.unretainableResolutionsPerRead = Integer.MAX_VALUE;
    try {
      final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);

      assertTrue(malformed.isEmpty(), "bytes the detector could not hold must not be judged: " + malformed);
      // Child 1 is resolved for the trie condition and for the descent, which only look at the page
      // type, and for two reads that each give up after the bound: its first key, after which the
      // range check skips the child, and its I5 walk.
      assertEquals(2 + 2 * HOTMalformedSubtreeDetector.MAX_LEAF_RETAIN_ATTEMPTS, trie.oneBitResolutions);
      trie.assertNoGuardIsHeld();
    } finally {
      trie.close();
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  @DisplayName("a concurrent evictor never makes a canonical trie look malformed")
  void concurrentEvictionFabricatesNoDefect() throws InterruptedException {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(6_000);
    for (int i = 0; i < 6_000; i++) {
      // A shared four-byte prefix, as index keys of one path or one relation have.
      entries.add(new HOTBulkBuilder.Entry(
          new byte[] {0x12, 0x34, 0x56, 0x78, (byte) (i >>> 16), (byte) (i >>> 8), (byte) i, 0x01}, VALUE));
    }
    final HOTBulkBuilder.BuildResult built =
        HOTBulkBuilder.build(entries, 1, IndexType.CAS, new AtomicLong(1)::getAndIncrement);
    final ReloadingTrie trie = new ReloadingTrie(built.rootReference());
    assertTrue(trie.references.size() > 2, "the trie must have several leaves");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong evictions = new AtomicLong();
    final Thread evictor = new Thread(() -> {
      while (!stop.get()) {
        for (final PageReference reference : trie.references) {
          if (reference.getPage() instanceof HOTLeafPage leaf && !leaf.isOrphaned()) {
            leaf.close();
            evictions.incrementAndGet();
          }
        }
      }
    }, "detector-leaf-evictor");
    evictor.setDaemon(true);
    evictor.start();
    try {
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      int walks = 0;
      while (walks < 20 || System.nanoTime() < deadline) {
        final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(trie.root, trie);
        assertTrue(malformed.isEmpty(), "walk " + walks + " reported a sound trie malformed: " + malformed);
        walks++;
      }
    } finally {
      stop.set(true);
      evictor.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(evictor.isAlive(), "the evictor must stop");
      trie.close();
    }
    assertTrue(evictions.get() > 0, "no leaf was evicted, so nothing was exercised");
  }

  /**
   * A clean BiNode over two leaves that discriminates on byte 1's most significant bit. Its resolver
   * reloads an evicted leaf, as the writer's does from the store, and can hand out a leaf that is
   * already being torn down in place of child 1.
   */
  private static final class TwoLeafTrie implements PageResolver {
    private final byte[][] zeroBitKeys;
    private final byte[][] oneBitKeys;
    private final PageReference zeroBit = new PageReference();
    private final PageReference oneBit = new PageReference();
    private final PageReference root = new PageReference();
    private final List<HOTLeafPage> handedOut = new ArrayList<>();
    private final HOTLeafPage unretainable;
    private int reloads;
    private int oneBitResolutions;
    private int unretainableResolutionsPerRead;
    private int unretainableSinceLastRead;
    private int unretainableHandedOut;

    TwoLeafTrie(final byte[][] zeroBitKeys, final byte[][] oneBitKeys) {
      this.zeroBitKeys = zeroBitKeys;
      this.oneBitKeys = oneBitKeys;
      zeroBit.setPage(leaf(1, zeroBitKeys));
      oneBit.setPage(leaf(2, oneBitKeys));
      // bitMask 1L << 55 with initialBytePos 0 captures absolute key bit 8: byte 1's top bit.
      root.setPage(HOTIndirectPage.createSpanNode(3, 1, 0, 1L << 55, new int[] {0, 1},
          new PageReference[] {zeroBit, oneBit}, 1));
      // Orphaned but still guarded by someone else: open, and it refuses every further guard. This
      // is what the detector sees when eviction wins between its resolution and its guard.
      unretainable = leaf(2, oneBitKeys);
      assertTrue(unretainable.acquireGuard());
      unretainable.close();
      assertFalse(unretainable.isClosed());
      assertFalse(unretainable.acquireGuard());
    }

    @Override
    public Page resolve(final PageReference reference) {
      if (reference == root) {
        return root.getPage();
      }
      if (reference == oneBit) {
        oneBitResolutions++;
        if (unretainableSinceLastRead < unretainableResolutionsPerRead) {
          unretainableSinceLastRead++;
          unretainableHandedOut++;
          return unretainable;
        }
      }
      // PageReference.getPage() drops a closed leaf, which is the point where the store is read.
      Page page = reference.getPage();
      if (page == null) {
        assertTrue(reference == zeroBit || reference == oneBit, "unknown reference");
        page = reference == zeroBit
            ? leaf(1, zeroBitKeys)
            : leaf(2, oneBitKeys);
        reference.setPage(page);
        reloads++;
      }
      handedOut.add((HOTLeafPage) page);
      return page;
    }

    void assertNoGuardIsHeld() {
      for (final HOTLeafPage leaf : handedOut) {
        assertEquals(0, leaf.getGuardCount(), "the detector must release every guard it takes");
      }
      assertEquals(1, unretainable.getGuardCount(), "only this test guards the unretainable leaf");
    }

    void close() {
      unretainable.releaseGuard();
      assertTrue(unretainable.isClosed());
      for (final PageReference reference : new PageReference[] {zeroBit, oneBit}) {
        if (reference.getPage() instanceof HOTLeafPage leaf) {
          leaf.close();
        }
      }
    }
  }

  /** A bulk-built trie whose resolver reloads every leaf an evictor thread has closed. */
  private static final class ReloadingTrie implements PageResolver {
    private final PageReference root;
    private final List<PageReference> references = new ArrayList<>();
    private final Map<PageReference, byte[][]> keysByReference = new IdentityHashMap<>();
    private final Map<PageReference, Long> pageKeyByReference = new IdentityHashMap<>();

    ReloadingTrie(final PageReference root) {
      this.root = root;
      collect(root);
    }

    private void collect(final PageReference reference) {
      final Page page = reference.getPage();
      if (page instanceof HOTLeafPage leaf) {
        final byte[][] keys = new byte[leaf.getEntryCount()][];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = leaf.getKey(i);
        }
        references.add(reference);
        keysByReference.put(reference, keys);
        pageKeyByReference.put(reference, leaf.getPageKey());
      } else if (page instanceof HOTIndirectPage indirect) {
        for (int i = 0; i < indirect.getNumChildren(); i++) {
          collect(indirect.getChildReference(i));
        }
      }
    }

    @Override
    public Page resolve(final PageReference reference) {
      final Page page = reference.getPage();
      final byte[][] keys = keysByReference.get(reference);
      if (keys == null || (page instanceof HOTLeafPage leaf && !leaf.isOrphaned())) {
        return page;
      }
      final HOTLeafPage reloaded = leaf(pageKeyByReference.get(reference), keys);
      reference.setPage(reloaded);
      return reloaded;
    }

    void close() {
      for (final PageReference reference : references) {
        if (reference.getPage() instanceof HOTLeafPage leaf) {
          leaf.close();
        }
      }
    }
  }

  private static PageReference reference(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[][] keys) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.CAS);
    for (final byte[] key : keys) {
      assertTrue(leaf.put(key, VALUE), "the fixture leaf must take every key");
    }
    return leaf;
  }
}

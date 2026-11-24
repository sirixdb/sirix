/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.Databases;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.hot.HOTConstructedShapeIntegrationTest.Fixture;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * The structural validator that runs after a published splice reads leaf keys — the first and the
 * last key of a child's subtree for I8/I12, and every key of a subtree for I5. It must hold each of
 * those leaves while it reads it.
 *
 * <p>
 * A leaf the page cache hands out is a pointer to off-heap memory. Eviction releases that memory as
 * soon as nobody holds the leaf, the frame is handed out again, and a validator that kept reading
 * judges another page's bytes: it reports a sound trie as malformed, which ends the transaction and
 * poisons derived maintenance. That is issue #1458, seen on the {@code macos-latest} and
 * {@code windows-latest} lanes, where a source resource and its {@code $derived} resource share one
 * page cache and each writer thread evicts the other's leaves.
 * </p>
 *
 * <p>
 * The two moments this test puts together are the ones a concurrent evictor wins. At the splice's
 * publication boundary — the last point before validation — every leaf nobody holds is retired, so
 * the validator that follows has to resolve its leaves again. And at every engine call the writer
 * makes, including each of those resolutions, the same eviction runs again; after an unguarded leaf
 * handoff it runs <em>after</em> the engine returned the leaf, which is the race itself.
 * {@link FrameSlotAllocator#setPoisonOnReleaseForTesting} zero-fills a released frame, so a read of
 * one is not right by luck: every leaf of this shape holds keys that differ only in their last byte,
 * and a zeroed leaf reads them all as the bare common prefix, which no invariant accepts.
 * </p>
 *
 * <p>
 * {@link HOTIndexWriterLeafRetentionTest} covers the writer's own sibling reads. This test is about
 * the validation that follows them: it resolves through the same mutation-wide retention
 * ({@code AbstractHOTIndexWriter.enterMutation}), and with that retention removed the detector reads
 * a frame it does not hold.
 * </p>
 */
@Isolated
@DisplayName("The HOT structural validator holds every leaf whose keys it reads")
final class HOTStructuralValidatorLeafRetentionTest {

  @TempDir
  Path temporaryDirectory;

  @BeforeEach
  void poisonReleasedFrames() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
  }

  @AfterEach
  void restore() {
    AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(0);
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
    Databases.clearGlobalCaches();
  }

  /**
   * Bits 48 and 50 with the partials of groups 0x00, 0x20, 0x80 and 0xA0/0xE0. A put of group 0xC0
   * routes to the 0x80 leaf and branches on bit 49; the new child's partial also captures group 0xE0
   * of the last leaf, so that leaf is split and the splice publishes a block whose other children are
   * the committed leaves of groups 0x00 and 0x20 — the leaves the validator then reads from the page
   * cache.
   */
  private static void branchingShape(final Fixture fixture) {
    fixture.install(fixture.node(new int[] {48, 50}, new int[] {0, 1, 2, 3}, fixture.leaf(slot(0x00, 1), slot(0x00, 2)),
        fixture.leaf(slot(0x20, 1), slot(0x20, 2)), fixture.leaf(slot(0x80, 1), slot(0x80, 2)),
        fixture.leaf(slot(0xA0, 1), slot(0xA0, 2), slot(0xE0, 1), slot(0xE0, 2))));
  }

  @Test
  @DisplayName("validation after a published splice survives eviction between a leaf's resolution and its read")
  void validationSurvivesEvictionBetweenResolutionAndRead() {
    final HOTLeafEvictionAdversary[] adversary = new HOTLeafEvictionAdversary[1];
    final AtomicInteger publications = new AtomicInteger();
    final AtomicInteger heldAtPublication = new AtomicInteger();
    final AtomicInteger residentAtPublication = new AtomicInteger();
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("validator-eviction"), VersioningType.FULL,
        engine -> {
          adversary[0] = new HOTLeafEvictionAdversary(engine);
          return adversary[0].engine();
        })) {
      branchingShape(fixture);
      // Cold: every leaf of the shape now belongs to the page cache, which is the case the rule is
      // about. A leaf the transaction log owns cannot be evicted and needs no hold.
      fixture.commitBaselineAndReopen();

      AbstractHOTIndexWriter.setStructuralPublicationTestHook(() -> {
        publications.incrementAndGet();
        final ShardedPageCache<HOTLeafPage> cache = leafCache(fixture);
        int held = 0;
        int resident = 0;
        for (final HOTLeafPage leaf : cache.asMap().values()) {
          resident++;
          if (leaf.getGuardCount() > 0) {
            held++;
          }
        }
        residentAtPublication.addAndGet(resident);
        heldAtPublication.addAndGet(held);
        // Retire everything the mutation does not hold, so the validation that follows must resolve
        // its leaves again — and prove that a hold is what keeps a leaf alive across this point.
        HOTLeafEvictionAdversary.evictEveryUnguardedLeaf(fixture.engine);
      });

      adversary[0].arm(HOTLeafEvictionAdversary.EVERY_POINT);
      // The put is followed by the fixture's own comparison of the whole index against its model.
      fixture.put(slot(0xC0, 1), HOTConstructedShapeIntegrationTest.payload(3));
      adversary[0].disarm();

      assertTrue(publications.get() > 0,
          "the put must have published a structural splice, which is what the validator then inspects");
      assertTrue(adversary[0].strikes() > 0, "the adversary must have evicted at the writer's engine calls");
      assertTrue(residentAtPublication.get() > 0,
          "the validator's leaves must be the page cache's, or this test cannot observe the rule");
      assertTrue(heldAtPublication.get() > 0,
          "the mutation must hold the cache-backed leaves its validation reads: resident="
              + residentAtPublication.get() + ", held=" + heldAtPublication.get());

      assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
          ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, slot(0xC0, 1)),
          "the key the put placed must be readable afterwards");
      fixture.commitAndVerifyHistory("structural validation under eviction at every engine call");
    }
  }

  @Test
  @DisplayName("validation after a consolidation survives the same eviction")
  void consolidationValidationSurvivesEvictionBetweenResolutionAndRead() {
    // The consolidation cadence makes the writer rewrite a block out of leaves it did not route to,
    // so the validation that follows reads a scope built from several committed leaves at once.
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(1);
    final HOTLeafEvictionAdversary[] adversary = new HOTLeafEvictionAdversary[1];
    final AtomicInteger publications = new AtomicInteger();
    final AtomicInteger heldAtPublication = new AtomicInteger();
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("validator-consolidation"), VersioningType.FULL,
        engine -> {
          adversary[0] = new HOTLeafEvictionAdversary(engine);
          return adversary[0].engine();
        })) {
      consolidationShape(fixture);
      fixture.commitBaselineAndReopen();

      AbstractHOTIndexWriter.setStructuralPublicationTestHook(() -> {
        publications.incrementAndGet();
        for (final HOTLeafPage leaf : leafCache(fixture).asMap().values()) {
          if (leaf.getGuardCount() > 0) {
            heldAtPublication.incrementAndGet();
          }
        }
        HOTLeafEvictionAdversary.evictEveryUnguardedLeaf(fixture.engine);
      });

      adversary[0].arm(HOTLeafEvictionAdversary.EVERY_POINT);
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));
      adversary[0].disarm();

      assertTrue(publications.get() > 0, "the consolidating put must have published a structural change");
      assertTrue(heldAtPublication.get() > 0, "the mutation must hold the cache-backed leaves its validation reads");

      // What a consolidation owes its caller is the data, not a shape: every key of every group it
      // merged must still read back, whichever pairs the writer chose to merge.
      for (final long key : new long[] {slot(0x00, 1), slot(0x00, 2), slot(0x00, 3), slot(0x20, 1), slot(0x20, 2),
          slot(0x40, 1), slot(0x40, 2), slot(0x80, 1), slot(0x80, 2)}) {
        assertArrayEquals(new byte[0], ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, key),
            "key " + Long.toHexString(key) + " must still be found with its value");
      }
      assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
          ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, slot(0x80, 3)),
          "the key the consolidating put placed must read back with its value");
      fixture.commitAndVerifyHistory("structural validation after a consolidation under eviction");
    }
  }

  /**
   * Bits 58 and 62 with the partials of 0, 2 and 3, where the two upper children are nodes: the one
   * of partial 2 holds the leaves of 0x20, 0x21 and of 0x28, 0x29 below bit 60, the one of partial 3
   * those of 0x2a, 0x2b and of 0x3a, 0x3b below bit 59. A validation of this root descends into both
   * subtrees — their first and last keys for I8/I12, and every key below a non-zero partial for I5 —
   * so it reads leaves the mutation itself never routed to.
   */
  private static void deepScopeShape(final Fixture fixture) {
    final PageReference second =
        fixture.node(new int[] {60}, new int[] {0, 1}, fixture.leaf(0x20, 0x21), fixture.leaf(0x28, 0x29));
    final PageReference third =
        fixture.node(new int[] {59}, new int[] {0, 1}, fixture.leaf(0x2a, 0x2b), fixture.leaf(0x3a, 0x3b));
    fixture.install(fixture.node(new int[] {58, 62}, new int[] {0, 2, 3}, fixture.leaf(0x10, 0x11), second, third));
  }

  @Test
  @DisplayName("validation reading a subtree child's leaves survives their eviction")
  void validationOfASubtreeChildSurvivesEviction() {
    // 0x02 needs the root's partial 1, which no child carries, so the put adds a child at the root:
    // the published scope is the root block, and validating it walks both subtrees below it.
    final HOTLeafEvictionAdversary[] adversary = new HOTLeafEvictionAdversary[1];
    final AtomicInteger publications = new AtomicInteger();
    try (Fixture fixture =
        new Fixture(temporaryDirectory.resolve("validator-subtree"), VersioningType.FULL, engine -> {
          adversary[0] = new HOTLeafEvictionAdversary(engine);
          return adversary[0].engine();
        })) {
      deepScopeShape(fixture);
      fixture.commitBaselineAndReopen();

      AbstractHOTIndexWriter.setStructuralPublicationTestHook(() -> {
        publications.incrementAndGet();
        HOTLeafEvictionAdversary.evictEveryUnguardedLeaf(fixture.engine);
      });

      adversary[0].arm(HOTLeafEvictionAdversary.EVERY_POINT);
      fixture.put(0x02L, HOTConstructedShapeIntegrationTest.payload(3));
      adversary[0].disarm();

      assertTrue(publications.get() > 0, "the put must have published a structural splice at the root");
      assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
          ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, 0x02L),
          "the key the put placed must be readable afterwards");
      fixture.commitAndVerifyHistory("structural validation of a subtree child under eviction");
    }
  }

  /**
   * Bits 48 to 50 with the partials of groups 0x00, 0x20, 0x40 and 0x80. Groups 0x00 and 0x20 are
   * BiNode-paired and small, so a consolidation of the root merges them and then group 0x40 into
   * their leaf.
   */
  private static void consolidationShape(final Fixture fixture) {
    fixture.install(fixture.node(new int[] {48, 49, 50}, new int[] {0, 1, 2, 4},
        fixture.leaf(slot(0x00, 1), slot(0x00, 2), slot(0x00, 3)), fixture.leaf(slot(0x20, 1), slot(0x20, 2)),
        fixture.leaf(slot(0x40, 1), slot(0x40, 2)), fixture.leaf(slot(0x80, 1), slot(0x80, 2))));
  }

  @SuppressWarnings("unchecked")
  private static ShardedPageCache<HOTLeafPage> leafCache(final Fixture fixture) {
    return (ShardedPageCache<HOTLeafPage>) fixture.engine.getBufferManager().getHOTLeafPageCache();
  }

  /** A projection slot key whose seventh byte is {@code group} and whose last byte is {@code low}. */
  private static long slot(final int group, final int low) {
    return ((long) group << 8) | low;
  }
}

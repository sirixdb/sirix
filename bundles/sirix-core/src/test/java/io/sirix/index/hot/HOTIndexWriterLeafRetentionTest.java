/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTConstructedShapeIntegrationTest.Fixture;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A HOT writer must hold a leaf for as long as it reads that leaf's contents, or prove after the
 * read that the leaf was still there.
 *
 * <p>
 * A leaf the page cache hands out is a pointer to off-heap memory. Eviction releases that memory as
 * soon as nobody guards the leaf, the frame is handed out again, and a writer that kept going reads
 * another page's bytes: as the keys it compares to prove a structural change safe, or as the keys
 * and values it copies into a new leaf. Only the key's own leaf is safe by construction, because
 * the transaction log owns it. Every sibling leaf is the cache's.
 * </p>
 *
 * <p>
 * These tests evict at the points where the writer hands control to its storage engine, through
 * {@link HOTLeafEvictionAdversary}. A released frame keeps its bytes until it is reused, so a stale
 * read is usually right by luck. {@link FrameSlotAllocator#setPoisonOnReleaseForTesting} removes
 * the luck: it zero-fills a released frame, and every leaf of every shape holds at least two keys
 * that differ only in their last byte, so a zeroed leaf reads every key as the bare common prefix,
 * which no model contains. {@link HOTIncrementalInsertLeafRetentionTest} covers the consolidation
 * primitive on its own, including released frames that a foreign leaf has taken.
 * </p>
 */
@Isolated
@DisplayName("The HOT writer holds or validates every leaf it reads")
final class HOTIndexWriterLeafRetentionTest {

  @TempDir
  Path temporaryDirectory;

  private int databases;

  @BeforeEach
  void poisonReleasedFrames() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
  }

  @AfterEach
  void restore() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(0);
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("periodic consolidation keeps every sibling entry under eviction at every engine call")
  void writerConsolidationSurvivesEvictionAtEveryPoint() {
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(1);
    sweepStrikePoints("consolidation", HOTIndexWriterLeafRetentionTest::consolidationShape, fixture -> {
    }, slot(0x80, 3), HOTIndexWriterLeafRetentionTest::assertConsolidated);
  }

  @Test
  @DisplayName("consolidation holds the leaves an earlier put left swizzled")
  void writerConsolidationHoldsLeavesAnEarlierPutResolved() {
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(2);
    sweepStrikePoints("consolidation-swizzled", HOTIndexWriterLeafRetentionTest::consolidationShape, fixture -> {
      // The first put adds a child for group 0xC0. Its structural check resolves every child of the
      // root, and those leaves stay swizzled on the root's references when the put lets go of them.
      // The cadence fires on the second put, which finds them resolved and held by nobody.
      fixture.put(slot(0xC0, 1), HOTConstructedShapeIntegrationTest.payload(3));
      final HOTIndirectPage root =
          assertInstanceOf(HOTIndirectPage.class, fixture.engine.loadHOTPage(fixture.storage.getRootReference()));
      assertEquals(5, root.getNumChildren(), "the first put must have added a child and consolidated nothing");
      int swizzledCommittedLeaves = 0;
      for (int i = 0; i < root.getNumChildren(); i++) {
        final PageReference child = root.getChildReference(i);
        if (child.getKey() >= 0 && child.getPage() instanceof HOTLeafPage leaf) {
          assertEquals(0, leaf.getGuardCount(), "the first put must have let go of every leaf");
          swizzledCommittedLeaves++;
        }
      }
      assertEquals(3, swizzledCommittedLeaves, "groups 0x00, 0x20 and 0x40 must be resolved before the second put");
    }, slot(0x80, 3), HOTIndexWriterLeafRetentionTest::assertConsolidated);
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

  /** Every mergeable pair merged: what is left of the root is two leaves. */
  private static void assertConsolidated(final Fixture fixture) {
    final HOTIndirectPage root =
        assertInstanceOf(HOTIndirectPage.class, fixture.engine.loadHOTPage(fixture.storage.getRootReference()));
    assertEquals(2, root.getNumChildren(), "the put must have consolidated the sibling leaves");
  }

  @Test
  @DisplayName("a merge that would cross a neighbour is declined whenever that neighbour is evicted")
  void writerDeclinesACrossingMergeUnderEvictionAtEveryPoint() {
    final long[] delegatedBefore = new long[1];
    // Group 0x33 routes to the leaf of 0x36 and 0x37 and would become its smallest key, below the
    // 0x34 that ends the leaf before it: merged in place the two ranges interleave. The writer learns
    // that from the last key of that neighbour, which it does not hold; it validates the read after it.
    sweepStrikePoints("merge-crossing", HOTIndexWriterLeafRetentionTest::spineShape,
        fixture -> delegatedBefore[0] = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(), 0x33L,
        fixture -> assertEquals(delegatedBefore[0] + 1, AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(),
            "the merge crosses its neighbour and must be handed to the frontier"));
  }

  /**
   * Bits 58 and 62 with the partials of 0, 2 and 3, where the two upper groups are nodes: group 2 on
   * bit 60 holds the leaves of 0x20, 0x21 and of 0x28, 0x29, group 3 on bit 59 those of 0x2a, 0x2b
   * and of 0x3a, 0x3b. The groups' ranges are disjoint and adjacent -- 0x29, then 0x2a -- and group 3
   * holds keys on both sides of the 0x2c below: the bits 59 to 61 that no ancestor discriminates let
   * a key routed to group 2 sort into group 3's range.
   */
  private static void straddlingSubtreeShape(final Fixture fixture) {
    final PageReference second =
        fixture.node(new int[] {60}, new int[] {0, 1}, fixture.leaf(0x20, 0x21), fixture.leaf(0x28, 0x29));
    final PageReference third =
        fixture.node(new int[] {59}, new int[] {0, 1}, fixture.leaf(0x2a, 0x2b), fixture.leaf(0x3a, 0x3b));
    fixture.install(fixture.node(new int[] {58, 62}, new int[] {0, 2, 3}, fixture.leaf(0x10, 0x11), second, third));
  }

  @Test
  @DisplayName("a merge that would cross a neighbouring subtree is declined whenever its leaf is evicted")
  void writerDeclinesAMergeCrossingASubtreeUnderEvictionAtEveryPoint() {
    final long[] delegatedBefore = new long[1];
    // 0x2c routes to the leaf of 0x28 and 0x29 and would become its greatest key -- its mismatch bit
    // 61 is less significant than that leaf's parent's bit 60, so this is a merge -- but it sorts
    // past the 0x2a that begins the subtree beside it: merged in place the two ranges interleave.
    // The neighbour is a node, so the writer descends to the leaf holding the subtree's SMALLEST
    // key, unheld. Reading the greatest key instead (0x3b, above 0x2c) would call the merge ordered.
    sweepStrikePoints("merge-crossing-subtree", HOTIndexWriterLeafRetentionTest::straddlingSubtreeShape,
        fixture -> delegatedBefore[0] = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(), 0x2cL,
        fixture -> assertEquals(delegatedBefore[0] + 1, AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(),
            "the merge crosses the subtree beside it and must be handed to the frontier"));
  }

  @Test
  @DisplayName("a merge beside a neighbour stays in place whenever that neighbour is evicted")
  void writerKeepsAnOrderedMergeInPlaceUnderEvictionAtEveryPoint() {
    final long[] delegatedBefore = new long[1];
    // 0x35 becomes the greatest key of the leaf that ends at 0x34 and stays below the 0x36 that starts
    // the next leaf, so it merges in place. A zeroed neighbour reads as a key below 0x35.
    sweepStrikePoints("merge-ordered", HOTIndexWriterLeafRetentionTest::spineShape,
        fixture -> delegatedBefore[0] = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(), 0x35L,
        fixture -> assertEquals(delegatedBefore[0], AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(),
            "the merge keeps the order of the spine and must stay in place"));
  }

  /**
   * Bits 58 and 62 of keys that differ in their last byte only: the leaf of 0x10 and 0x11, the leaf
   * of 0x25, 0x2d and 0x34, and below a node on bit 60 the leaves of 0x36, 0x37 and of 0x3a, 0x3b.
   */
  private static void spineShape(final Fixture fixture) {
    final PageReference below =
        fixture.node(new int[] {60}, new int[] {0, 1}, fixture.leaf(0x36, 0x37), fixture.leaf(0x3a, 0x3b));
    fixture.install(fixture.node(new int[] {58, 62}, new int[] {0, 2, 3}, fixture.leaf(0x10, 0x11),
        fixture.leaf(0x25, 0x2d, 0x34), below));
  }

  @ParameterizedTest(name = "the put extends its leaf upwards: {0}")
  @ValueSource(booleans = {true, false})
  @DisplayName("a merge beside a committed neighbour reads that neighbour without taking a guard")
  void aMergeReadsItsNeighbourWithoutAGuard(final boolean upwards) {
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("unguarded-" + upwards), VersioningType.FULL)) {
      // Two leaves under one node on bit 56. A key just above the first leaf's last key stays below
      // the 0x85 that starts the second; a key just below 0x85 stays above the first leaf's 0x02. Each
      // such put compares itself with that key of the other leaf, which the page cache owns.
      fixture.install(
          fixture.node(new int[] {56}, new int[] {0, 1}, fixture.leaf(0x01, 0x02), fixture.leaf(0x85, 0x86)));
      fixture.commitBaselineAndReopen();
      final long delegatedBefore = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get();
      // The first put loads the neighbour; the handoff from the page cache marks it as recently used.
      fixture.put(upwards
          ? 0x03
          : 0x84, HOTConstructedShapeIntegrationTest.payload(3));
      final ShardedPageCache<HOTLeafPage> cache = leafCache(fixture);
      assertEquals(1, cache.asMap().size(), "the committed neighbour is the one leaf left to the page cache");
      // Taking a guard sets that mark as well, so with it cleared a second put must leave it clear.
      for (final HOTLeafPage leaf : cache.asMap().values()) {
        leaf.clearHot();
      }
      fixture.storage.putBlob(upwards
          ? 0x04
          : 0x83, HOTConstructedShapeIntegrationTest.payload(3));
      for (final HOTLeafPage leaf : cache.asMap().values()) {
        assertFalse(leaf.isHot(), "a put that only merges must not take a guard on its neighbour");
        assertEquals(0, leaf.getGuardCount());
      }
      assertEquals(delegatedBefore, AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(),
          "both puts merge in place: the neighbour was read and found on the far side of them");
    }
  }

  /**
   * The neighbour on the merging side is a node, not a leaf. The comparison must then descend to the
   * leaf that holds the extreme nearest the merging leaf -- leftmost below a neighbour above it,
   * rightmost below one beneath it -- and must get there without guarding a leaf. A neighbour that is
   * itself a leaf returns at the first hop, so only this shape walks the descent down.
   *
   * <p>
   * Every leaf of the shape is the page cache's after the reopen, and the cache marks a leaf it hands
   * out, and the writer marks a leaf it guards, as recently used. With that mark cleared, a leaf that
   * is hot after the second put was either guarded or resolved for the first time: the assertion
   * below therefore pins both the retention rule and which of the subtree's two leaves is read.
   * </p>
   */
  @ParameterizedTest(name = "the put extends its leaf upwards: {0}")
  @ValueSource(booleans = {true, false})
  @DisplayName("a merge beside a committed subtree descends unheld to that subtree's nearest extreme")
  void aMergeReadsTheNearestExtremeOfANeighbouringSubtree(final boolean upwards) {
    try (Fixture fixture =
        new Fixture(temporaryDirectory.resolve("unguarded-subtree-" + upwards), VersioningType.FULL)) {
      // Above the puts: the leaves of 0x85, 0x86 and of 0xa5, 0xa6 below a node on bit 58. Below
      // them: those of 0x01, 0x02 and of 0x21, 0x22. Bit 56 separates that subtree from the leaf the
      // puts extend, so each put compares itself with the subtree key nearest to it.
      final PageReference subtree = upwards
          ? fixture.node(new int[] {58}, new int[] {0, 1}, fixture.leaf(0x85, 0x86), fixture.leaf(0xa5, 0xa6))
          : fixture.node(new int[] {58}, new int[] {0, 1}, fixture.leaf(0x01, 0x02), fixture.leaf(0x21, 0x22));
      fixture.install(upwards
          ? fixture.node(new int[] {56}, new int[] {0, 1}, fixture.leaf(0x01, 0x02), subtree)
          : fixture.node(new int[] {56}, new int[] {0, 1}, subtree, fixture.leaf(0x85, 0x86)));
      fixture.commitBaselineAndReopen();
      final long delegatedBefore = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get();
      // The first put resolves the nearest extreme through the page cache and swizzles it onto its
      // reference; the second finds it there, and that is the read that must not guard it.
      fixture.put(upwards
          ? 0x03
          : 0x84, HOTConstructedShapeIntegrationTest.payload(3));
      final ShardedPageCache<HOTLeafPage> cache = leafCache(fixture);
      final byte[] nearest = HOTConstructedShapeIntegrationTest.key(upwards
          ? 0x85
          : 0x21);
      final byte[] farther = HOTConstructedShapeIntegrationTest.key(upwards
          ? 0xa5
          : 0x01);
      int leavesOfTheSubtree = 0;
      for (final HOTLeafPage leaf : cache.asMap().values()) {
        final byte[] first = leaf.getFirstKey();
        if (Arrays.equals(first, nearest) || Arrays.equals(first, farther)) {
          leavesOfTheSubtree++;
        }
        leaf.clearHot();
      }
      assertEquals(2, leavesOfTheSubtree,
          "both leaves of the neighbouring subtree are the page cache's, and are cold from here on");
      fixture.storage.putBlob(upwards
          ? 0x04
          : 0x83, HOTConstructedShapeIntegrationTest.payload(3));
      for (final HOTLeafPage leaf : cache.asMap().values()) {
        assertFalse(leaf.isHot(),
            "the descent to the subtree's nearest extreme must neither guard a leaf nor resolve another one");
        assertEquals(0, leaf.getGuardCount());
      }
      assertEquals(delegatedBefore, AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get(),
          "both puts merge in place: the subtree's nearest key was read and found on the far side of them");
    }
  }

  @Test
  @DisplayName("a delta fold and a posting removal run inside the scope and keep their results")
  void aFoldAndARemovalKeepTheirResults() {
    final Path path = temporaryDirectory.resolve("postings");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("postings").build()));
      try (JsonResourceSession session = database.beginResourceSession("postings");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        // A writer with the production thresholds removes from a chunk this small directly, through
        // doRemovePostingBit.
        final CASValue removed = new CASValue(new Int64(3L), Type.INR, 1L);
        final HOTIndexWriter<CASValue> direct =
            HOTIndexWriter.create(transaction.getStorageEngineWriter(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
        direct.indexNodeKey(removed, 20L);
        direct.indexNodeKey(removed, 21L);
        assertTrue(direct.remove(removed, 21L), "removing a posting that exists reports it");
        assertFalse(direct.remove(removed, 21L), "removing it again reports that nothing was removed");
        final NodeReferences left = direct.get(removed, SearchMode.EQUAL);
        assertNotNull(left);
        assertEquals(1, left.getNodeKeys().getLongCardinality());
        assertTrue(left.contains(20L));

        // For this writer every chunk counts as hot and the second delta reaches the bound, so the
        // third node key folds: the base chunk is replaced through doReplacePostingChunk.
        final CASValue key = new CASValue(new Int64(7L), Type.INR, 1L);
        final HOTIndexWriter<CASValue> folding = HOTIndexWriter.create(transaction.getStorageEngineWriter(),
            CASKeySerializer.INSTANCE, IndexType.CAS, 0, 1, 2);
        final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
        folding.indexNodeKey(key, 10L);
        folding.indexNodeKey(key, 11L);
        folding.indexNodeKey(key, 12L);
        assertTrue(HOTIndexWriter.postingDeltaFolds() > foldsBefore, "the third node key must have folded the deltas");
        final NodeReferences folded = folding.get(key, SearchMode.EQUAL);
        assertNotNull(folded, "the folded chunk must be readable");
        assertEquals(3, folded.getNodeKeys().getLongCardinality(), "the fold must have replaced the base chunk");
        assertTrue(folded.contains(10L) && folded.contains(11L) && folded.contains(12L));
        transaction.rollback();
      }
    }
  }

  @Test
  @DisplayName("the two-leaf strand migration keeps every source entry under eviction at every engine call")
  void writerTwoLeafMigrationSurvivesEvictionAtEveryPoint() {
    final long[] migrationsBefore = new long[1];
    sweepStrikePoints("two-leaf-migration", HOTIndexWriterLeafRetentionTest::migrationShape,
        fixture -> migrationsBefore[0] = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get(), slot(0xC0, 1),
        fixture -> assertEquals(migrationsBefore[0] + 1, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get(),
            "the put must have taken the two-leaf migration"));
  }

  @Test
  @DisplayName("a mutation holds the committed leaves it resolved, and not the leaves it built")
  void aMutationHoldsCommittedSiblingsAndNotItsOwnLeaves() {
    final HOTIndirectPage[] candidate = new HOTIndirectPage[1];
    final List<Integer> committedGuards = new ArrayList<>();
    final List<Integer> builtGuards = new ArrayList<>();
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("held"), VersioningType.FULL)) {
      migrationShape(fixture);
      fixture.commitBaselineAndReopen();
      // An earlier put hands the route's own leaf to the transaction log, so the first leaf the
      // migrating put holds is a sibling, which it resolves again after it has held the others.
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterReattachTestHook(node -> candidate[0] = node);
      // By now the migration has validated its candidate, which resolves every child of it: the
      // committed siblings through their published references, the two replacement leaves through
      // references nobody published yet.
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
        final HOTIndirectPage node = candidate[0];
        for (int i = 0; i < node.getNumChildren(); i++) {
          final PageReference child = node.getChildReference(i);
          if (child.getKey() < 0 && child.getLogKey() < 0 && child.getPage() instanceof HOTLeafPage leaf) {
            builtGuards.add(leaf.getGuardCount());
          }
        }
        for (final HOTLeafPage leaf : leafCache(fixture).asMap().values()) {
          committedGuards.add(leaf.getGuardCount());
        }
      });
      fixture.put(slot(0xC0, 1), HOTConstructedShapeIntegrationTest.payload(3));

      assertEquals(List.of(0, 0), builtGuards, "the migrated child and the replaced source are the writer's own");
      // Groups 0x00 and 0x20 and the source leaf; the 0x80 leaf moved to the transaction log.
      assertEquals(List.of(1, 1, 1), committedGuards,
          "every committed leaf the put resolved is held, once, until the put ends");
      assertNoGuardIsHeld(fixture);
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterReattachTestHook(null);
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
    }
  }

  @Test
  @DisplayName("a put that fails while it holds leaves releases them")
  void aFailingMutationReleasesWhatItHolds() {
    final List<Integer> committedGuards = new ArrayList<>();
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("failing"), VersioningType.FULL)) {
      migrationShape(fixture);
      fixture.commitBaselineAndReopen();
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
        for (final HOTLeafPage leaf : leafCache(fixture).asMap().values()) {
          committedGuards.add(leaf.getGuardCount());
        }
        throw new IllegalStateException("the migration fails after it has published");
      });
      final byte[] key = HOTConstructedShapeIntegrationTest.key(slot(0xC0, 1));
      final byte[] value = fixture.encoded(HOTConstructedShapeIntegrationTest.payload(3));
      final AbstractHOTIndexWriter<Long> writer = fixture.storage;
      assertThrows(IllegalStateException.class, () -> writer.doIndex(key, key.length, value, value.length));

      assertEquals(List.of(1, 1, 1), committedGuards, "the put must have failed while it held its siblings");
      assertNoGuardIsHeld(fixture);
      fixture.rollbackAndVerifyBaseline("migration failed after publication; all three sibling guards released");
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
    }
  }

  @ParameterizedTest(name = "after {0}")
  @ValueSource(strings = {"a put that fails", "a removal that fails", "a replacement that fails",
      "a removal that finds nothing", "a replacement"})
  @DisplayName("however a mutation ends, the writer is outside its scope afterwards")
  void aMutationEndsItsScope(final String mutation) {
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("ended"), VersioningType.FULL)) {
      migrationShape(fixture);
      fixture.commitBaselineAndReopen();
      final AbstractHOTIndexWriter<Long> writer = fixture.storage;
      final byte[] present = HOTConstructedShapeIntegrationTest.key(slot(0x00, 1));
      final byte[] absent = HOTConstructedShapeIntegrationTest.key(slot(0x00, 9));
      final byte[] value = fixture.encoded(HOTConstructedShapeIntegrationTest.payload(3));
      switch (mutation) {
        case "a put that fails" -> assertThrows(IllegalArgumentException.class,
            () -> writer.doIndex(present, present.length + 1, value, value.length));
        case "a removal that fails" -> assertThrows(IllegalArgumentException.class,
            () -> writer.doRemovePostingBit(present, present.length + 1, 0L));
        case "a replacement that fails" -> assertThrows(IllegalArgumentException.class,
            () -> writer.doReplacePostingChunk(present, 0, value, value.length, false));
        case "a removal that finds nothing" -> assertFalse(writer.doRemovePostingBit(absent, absent.length, 0L));
        case "a replacement" -> writer.doReplacePostingChunk(present, present.length, value, value.length, false);
        default -> throw new IllegalArgumentException(mutation);
      }

      // Between mutations the writer holds nothing. A scan of the whole index resolves every leaf,
      // the committed ones from the page cache; had the mutation left its scope open, each of them
      // would now carry a guard that nothing releases.
      writer.subtreeKeyWithBitVerdict(writer.getRootReference(), 0, 1, present);
      assertFalse(leafCache(fixture).asMap().isEmpty(), "the scan must have resolved committed leaves");
      assertNoGuardIsHeld(fixture);
    }
  }

  /**
   * Bits 48 and 50. A put of group 0xC0 routes to the 0x80 leaf and branches on bit 49, and the new
   * child's partial also captures group 0xE0 of the last leaf: that leaf is split, its 0xE0 entries
   * are copied into the new child and its 0xA0 entries into the replacement.
   */
  private static void migrationShape(final Fixture fixture) {
    fixture.install(fixture.node(new int[] {48, 50}, new int[] {0, 1, 2, 3}, fixture.leaf(slot(0x00, 1), slot(0x00, 2)),
        fixture.leaf(slot(0x20, 1), slot(0x20, 2)), fixture.leaf(slot(0x80, 1), slot(0x80, 2)),
        fixture.leaf(slot(0xA0, 1), slot(0xA0, 2), slot(0xE0, 1), slot(0xE0, 2))));
  }

  /**
   * Build {@code shape}, commit it and reopen it cold, run {@code prelude} undisturbed, then put
   * {@code slot} through the adversary once striking at every point and once per single strike point.
   * Each run starts from its own store, and after the put the whole index must still equal the model
   * and pass the structural validator. Every failing run is reported, not only the first.
   */
  private void sweepStrikePoints(final String name, final Consumer<Fixture> shape, final Consumer<Fixture> prelude,
      final long slot, final Consumer<Fixture> reached) {
    final List<String> failures = new ArrayList<>();
    assertTrue(
        putUnderEviction(name, shape, prelude, slot, reached, HOTLeafEvictionAdversary.EVERY_POINT, failures) > 0,
        "the put must call its storage engine");
    // An eviction makes the put reload what it lost, so the number of points depends on where the
    // strike lands: keep going until a run ends before its strike point.
    int runs = 1;
    for (int point = 1; point <= putUnderEviction(name, shape, prelude, slot, reached, point, failures); point++) {
      runs++;
    }
    assertTrue(failures.isEmpty(), failures.size() + " of " + runs + " runs failed:\n" + String.join("\n", failures));
  }

  /** @return the engine calls the put made, each of which is one strike point */
  private int putUnderEviction(final String name, final Consumer<Fixture> shape, final Consumer<Fixture> prelude,
      final long slot, final Consumer<Fixture> reached, final int strikePoint, final List<String> failures) {
    final HOTLeafEvictionAdversary[] adversary = new HOTLeafEvictionAdversary[1];
    final Path path = temporaryDirectory.resolve(name + '-' + databases++);
    final String run = "strike at " + (strikePoint == HOTLeafEvictionAdversary.EVERY_POINT
        ? "every point"
        : "point " + strikePoint);
    try (Fixture fixture = new Fixture(path, VersioningType.FULL, engine -> {
      adversary[0] = new HOTLeafEvictionAdversary(engine);
      return adversary[0].engine();
    })) {
      shape.accept(fixture);
      fixture.commitBaselineAndReopen();
      prelude.accept(fixture);
      adversary[0].arm(strikePoint);
      try {
        // The put is followed by the fixture's own check of the whole index against its model.
        fixture.put(slot, HOTConstructedShapeIntegrationTest.payload(3));
        adversary[0].disarm();
        reached.accept(fixture);
        assertTrue(adversary[0].strikes() > 0 || strikePoint > adversary[0].points(), "the adversary must have struck");
        assertNoGuardIsHeld(fixture);
        final int points = adversary[0].points();
        final int evicted = adversary[0].evictedLeaves();
        fixture.commitAndVerifyHistory(name + "; " + run + "; engine calls=" + points + "; evicted leaves=" + evicted
            + "; no retained guards after put");
      } catch (final RuntimeException | AssertionError failure) {
        failures.add(run + ": " + firstLine(failure));
      } finally {
        adversary[0].disarm();
      }
      return adversary[0].points();
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  private static String firstLine(final Throwable failure) {
    final String text = failure.toString();
    final int end = text.indexOf('\n');
    final String line = end < 0
        ? text
        : text.substring(0, end);
    return line.length() > 240
        ? line.substring(0, 240)
        : line;
  }

  private static void assertNoGuardIsHeld(final Fixture fixture) {
    for (final HOTLeafPage leaf : leafCache(fixture).asMap().values()) {
      assertEquals(0, leaf.getGuardCount(), "the writer must release every guard when its put ends");
    }
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

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Type;
import io.sirix.exception.SirixLeafRetiredException;
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
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import java.util.concurrent.TimeUnit;
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
    AbstractHOTIndexWriter.setHOTLeafGuardRetryDeadlineForTesting(0L);
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
        if (child.getKey() >= 0 && child.getPage() instanceof HOTLeafPage) {
          swizzledCommittedLeaves++;
        }
      }
      assertEquals(3, swizzledCommittedLeaves, "groups 0x00, 0x20 and 0x40 must be resolved before the second put");
    }, slot(0x80, 3), HOTIndexWriterLeafRetentionTest::assertConsolidated);
  }

  /**
   * {@link HOTIncrementalInsert#consolidateNodeLeaves} holds the pair it merges and requires its
   * caller to hold the node's other direct leaf children; it states that precondition as an
   * assertion. This test is the evidence that the production caller keeps it: it drives a real
   * consolidation through the writer, so the assertion runs inside the merge on the production path,
   * and it fails if the writer ever stops resolving those children through the holding resolver.
   *
   * <p>
   * The check would be worthless if assertions were off, so that is asserted first rather than
   * assumed from the build configuration.
   * </p>
   */
  @Test
  @DisplayName("the production consolidation path holds every child the primitive requires of it")
  void writerConsolidationSatisfiesThePrimitivesPrecondition() {
    assertTrue(HOTIncrementalInsert.class.desiredAssertionStatus(),
        "this test IS the production path's precondition check, so it needs assertions enabled");
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(1);
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("precondition"), VersioningType.FULL)) {
      consolidationShape(fixture);
      fixture.commitBaselineAndReopen();
      // Every leaf of this shape is the page cache's after the reopen, which is the case the
      // precondition is about: the writer must hold them for the consolidation it then performs.
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));
      assertConsolidated(fixture);
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

  /**
   * What a consolidation owes its caller is the data, not a shape: every key of every group it merged
   * must still read back with its own value, whichever pairs the writer chose to merge.
   */
  private static void assertConsolidated(final Fixture fixture) {
    // consolidationShape installs these, and both callers put slot(0x80, 3) through the fixture,
    // whose own model comparison has already checked every value: what is left to state here is
    // that merging the groups together lost none of them.
    for (final long key : new long[] {slot(0x00, 1), slot(0x00, 2), slot(0x00, 3), slot(0x20, 1), slot(0x20, 2),
        slot(0x40, 1), slot(0x40, 2), slot(0x80, 1), slot(0x80, 2)}) {
      assertNotNull(ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, key),
          "key " + Long.toHexString(key) + " must still be found after the consolidation");
    }
    assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
        ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, slot(0x80, 3)),
        "the key the consolidating put placed must read back with its value");
  }

  @Test
  @DisplayName("a merge that would cross a neighbour is declined whenever that neighbour is evicted")
  void writerDeclinesACrossingMergeUnderEvictionAtEveryPoint() {
    // Group 0x33 routes to the leaf of 0x36 and 0x37 and would become its smallest key, below the
    // 0x34 that ends the leaf before it: merged in place the two ranges interleave. The writer learns
    // that from the last key of that neighbour, which it does not hold; it validates the read after it.
    sweepStrikePoints("merge-crossing", HOTIndexWriterLeafRetentionTest::spineShape, fixture -> {
    }, 0x33L,
        fixture -> assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
            ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, 0x33L),
            "the key the put placed must be readable afterwards, however the writer placed it"));
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
    // 0x2c routes to the leaf of 0x28 and 0x29 and would become its greatest key -- its mismatch bit
    // 61 is less significant than that leaf's parent's bit 60, so this is a merge -- but it sorts
    // past the 0x2a that begins the subtree beside it: merged in place the two ranges interleave.
    // The neighbour is a node, so the writer descends to the leaf holding the subtree's SMALLEST
    // key, unheld. Reading the greatest key instead (0x3b, above 0x2c) would call the merge ordered.
    sweepStrikePoints("merge-crossing-subtree", HOTIndexWriterLeafRetentionTest::straddlingSubtreeShape, fixture -> {
    }, 0x2cL,
        fixture -> assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
            ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, 0x2cL),
            "the key the put placed must be readable afterwards, however the writer placed it"));
  }

  @Test
  @DisplayName("a merge beside a neighbour stays in place whenever that neighbour is evicted")
  void writerKeepsAnOrderedMergeInPlaceUnderEvictionAtEveryPoint() {
    // 0x35 becomes the greatest key of the leaf that ends at 0x34 and stays below the 0x36 that starts
    // the next leaf, so it merges in place. A zeroed neighbour reads as a key below 0x35.
    sweepStrikePoints("merge-ordered", HOTIndexWriterLeafRetentionTest::spineShape, fixture -> {
    }, 0x35L,
        fixture -> assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
            ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, 0x35L),
            "the key the put placed must be readable afterwards, however the writer placed it"));
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
    sweepStrikePoints("two-leaf-migration", HOTIndexWriterLeafRetentionTest::migrationShape, fixture -> {
    }, slot(0xC0, 1),
        fixture -> assertArrayEquals(HOTConstructedShapeIntegrationTest.payload(3),
            ProjectionIndexHOTStorage.readBlob(fixture.engine, 0, slot(0xC0, 1)),
            "the migrated key must be readable afterwards"));
  }

  /**
   * FMT-304 clause 5 requires every hold to be released by the time the mutation ends. What that
   * means to the rest of the engine is that memory stays reclaimable: a leaf the mutation held
   * belongs to the page cache again once the put returns, so pressure eviction can take it back. A
   * hold a mutation forgets to release keeps that leaf resident for the life of the session, where it
   * answers every read perfectly and quietly starves eviction -- which is why no read-path test can
   * see it.
   *
   * <p>
   * The observation is the page cache's own, because pressure eviction takes every leaf nobody holds
   * and leaves a held one resident, exactly as in production. The allocator's live-slot count is
   * deliberately not asserted: measured on this fixture, evicting all three resident leaves left
   * {@code FrameSlotAllocator.liveSlotCount} unchanged at 2, and residency accounted for only one
   * slot above the empty-cache baseline, so cache-resident leaves here are not backed by that size
   * class and the count cannot see a leaked hold either.
   * </p>
   */
  @Test
  @DisplayName("when a mutation ends, every leaf it held is the page cache's again")
  void whenAMutationEndsTheLeavesItHeldBecomeReclaimable() {
    // Without the cadence the put consolidates nothing, resolves no sibling and holds nothing at
    // all, so a mutation that never released would look exactly like one that did. This is the
    // sequence that makes the writer hold committed leaves: the first put leaves the root's children
    // swizzled and unheld, and the second finds them resolved and consolidates under them.
    AbstractHOTIndexWriter.setConsolidationIntervalForTesting(2);
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("reclaimable"), VersioningType.FULL)) {
      consolidationShape(fixture);
      fixture.commitBaselineAndReopen();
      fixture.put(slot(0xC0, 1), HOTConstructedShapeIntegrationTest.payload(3));
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));

      final ShardedPageCache<HOTLeafPage> cache = leafCache(fixture);
      final int residentAfterPut = cache.asMap().size();
      assertTrue(residentAfterPut > 0, "the put must have left committed leaves with the page cache");

      final int evicted = HOTLeafEvictionAdversary.evictEveryUnguardedLeaf(fixture.engine);

      assertEquals(residentAfterPut, evicted, "every leaf the ended mutation held must be evictable");
      assertEquals(0, cache.asMap().size(),
          "the mutation has ended, so eviction must be able to take back every leaf it held");
    }
  }

  /**
   * FMT-304's interim behaviour at the bound on required re-resolution, in the case where the
   * mutation never starts: a put reads the slot it is about to overwrite before it enters its
   * mutation, and the adversary keeps retiring the leaf that read needs, so no attempt can guard it.
   *
   * <p>
   * The failure must name leaf retirement and be its own type, and the published revision must be
   * exactly what it was. Nothing of the transaction's private index state changed here -- the
   * mutation had not begun -- so the transaction is not poisoned, and rollback still works.
   * </p>
   */
  @Test
  @DisplayName("a required read that eviction keeps retiring fails by name and leaves published data alone")
  void aRequiredReadThatKeepsLosingItsLeafFailsByName() {
    AbstractHOTIndexWriter.setHOTLeafGuardRetryDeadlineForTesting(TimeUnit.MILLISECONDS.toNanos(20L));
    final HOTLeafEvictionAdversary[] adversary = new HOTLeafEvictionAdversary[1];
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("retired-read"), VersioningType.FULL, engine -> {
      adversary[0] = new HOTLeafEvictionAdversary(engine);
      return adversary[0].engine();
    })) {
      consolidationShape(fixture);
      fixture.commitBaselineAndReopen();
      adversary[0].starveGuardAcquisition(true);
      adversary[0].arm(HOTLeafEvictionAdversary.EVERY_POINT);

      final byte[] payload = HOTConstructedShapeIntegrationTest.payload(3);
      final SirixLeafRetiredException failure =
          assertThrows(SirixLeafRetiredException.class, () -> fixture.storage.putBlob(slot(0x80, 3), payload),
              "a required read that eviction keeps retiring must fail by its own name");
      adversary[0].disarm();
      assertTrue(failure.getMessage().contains("retired"), "the failure must say what happened: " + failure);
      assertTrue(failure.getWaitedNanos() > 0L, "the failure must report the bound it waited for");

      fixture.verifyBaselineThroughFreshReader("a required read failed at the re-resolution bound");
      fixture.rollbackAndVerifyBaseline("rollback after a failed required read");
    }
  }

  /** @return the deepest cause of {@code failure}, which is the reason a refusal rests on */
  private static Throwable rootCauseOf(final Throwable failure) {
    Throwable cause = failure;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  @Test
  @DisplayName("a put that fails while it holds leaves releases them")
  void aFailingMutationReleasesWhatItHolds() {
    try (Fixture fixture = new Fixture(temporaryDirectory.resolve("failing"), VersioningType.FULL)) {
      migrationShape(fixture);
      fixture.commitBaselineAndReopen();
      fixture.put(slot(0x80, 3), HOTConstructedShapeIntegrationTest.payload(3));
      // The hook fires inside the migration, after it has published its replacement leaves and while
      // it is still reading the siblings it resolved, which is the point a release has to survive.
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
        throw new IllegalStateException("the migration fails after it has published");
      });
      final byte[] key = HOTConstructedShapeIntegrationTest.key(slot(0xC0, 1));
      final byte[] value = fixture.encoded(HOTConstructedShapeIntegrationTest.payload(3));
      final AbstractHOTIndexWriter<Long> writer = fixture.storage;
      assertThrows(IllegalStateException.class, () -> writer.doIndex(key, key.length, value, value.length));

      // The mutation ended by exception, and clause 5 binds that path too: eviction must be able to
      // take back every leaf it held. This replaces the guard counts this test used to read.
      final ShardedPageCache<HOTLeafPage> cache = leafCache(fixture);
      final int residentAfterFailure = cache.asMap().size();
      assertTrue(residentAfterFailure > 0, "the failed put must have left committed leaves with the page cache");
      assertEquals(residentAfterFailure, HOTLeafEvictionAdversary.evictEveryUnguardedLeaf(fixture.engine),
          "a put that fails must still let eviction take back every leaf it held");
      assertEquals(0, cache.asMap().size(), "a failed mutation must leave no leaf pinned");

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

  @SuppressWarnings("unchecked")
  private static ShardedPageCache<HOTLeafPage> leafCache(final Fixture fixture) {
    return (ShardedPageCache<HOTLeafPage>) fixture.engine.getBufferManager().getHOTLeafPageCache();
  }

  /** A projection slot key whose seventh byte is {@code group} and whose last byte is {@code low}. */
  private static long slot(final int group, final int low) {
    return ((long) group << 8) | low;
  }
}

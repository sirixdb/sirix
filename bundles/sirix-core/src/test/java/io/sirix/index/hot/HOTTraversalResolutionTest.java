/*
 * Copyright (c) 2026, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.BufferManager;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which page the HOT write-path descent may be handed for a reference.
 *
 * <p>
 * A HOT leaf that an incremental merge folded into a sibling is released — its off-heap frame is
 * freed — while references naming it survive: every indirect-page copy-on-write deep-copies its
 * child references, so copies of the reference outlive the merge. The transaction-intent log
 * already refuses to resolve such an entry, but that alone only closes one of three doors. The
 * reference itself still swizzles the freed instance, and behind that sits the page's durable image
 * from an earlier async-flush epoch: the leaf's PRE-MERGE bytes, whose keys have since been copied
 * into the merge target.
 * </p>
 *
 * <p>
 * Descending into either one is not a crash but something worse — a structurally plausible leaf
 * holding keys that also live elsewhere. {@code analyzeDescent} then reports a mismatch bit at or
 * above an ancestor's discriminative bit, the insert dispatch takes its branch-escape arm, and the
 * branch cannot be placed incrementally. On PROJECTION, which refuses subtree rebuilds outright,
 * that ends the transaction with "requires a subtree rebuild; refusing the transaction before
 * publication" — the {@code windows-latest / query} lane's failure, reachable exactly when the
 * earlier epoch had already given the leaf a durable offset.
 * </p>
 */
final class HOTTraversalResolutionTest {

  /** A durable offset the reference carries from an earlier async-flush epoch. */
  private static final long DURABLE_OFFSET = 4096L;

  /** The page key of the leaf an incremental merge folded into its sibling. */
  private static final long MERGED_AWAY_PAGE_KEY = 17L;

  /** The durable offset a pinned-trie spill publishes for the merge target mid-transaction. */
  private static final long REPLACEMENT_OFFSET = 8192L;

  /**
   * The page key the writer's allocator issues in
   * {@link #aLeafCreatedForAnEmptySlotTakesAnAllocatedPageKey()}.
   */
  private static final long ALLOCATED_PAGE_KEY = 23L;

  /** The page-key namespace {@link TestIndexWriter} owns. */
  private static final long INDEX_SCOPE = TransactionIntentLog.indexScope(IndexType.PATH, 0);

  /**
   * The regression: a released entry must not fall back to storage. Its durable image is the leaf's
   * pre-merge content, so serving it duplicates every merged key.
   */
  @Test
  void aReleasedLeafEntryDoesNotResurrectItsPreMergeDurableImage() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      // The earlier epoch flushed this leaf, so the reference can still address it on disk.
      reference.setKey(DURABLE_OFFSET);

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(reference), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final HOTLeafPage preMergeImage = mock(HOTLeafPage.class);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(preMergeImage);

      assertNull(resolveForTraversal(storageEngineWriter, reference),
          "a released entry must resolve to nothing, not to the leaf's pre-merge durable image");
      verify(storageEngineWriter, never()).loadHOTPage(any(PageReference.class));
    } finally {
      log.close();
    }
  }

  /**
   * The second door: the freed instance is still swizzled on the reference. {@code close()} orphans a
   * HOT leaf at once but defers the teardown to the last {@code releaseGuard()}, so through that
   * whole window {@code isClosed()} reads false while the page already refuses every guard — which is
   * precisely why {@code isClosed()} alone is the wrong test here.
   */
  @Test
  void anOrphanedLeafSwizzledOnTheReferenceIsNotHandedToTheDescent() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage orphanedLeaf = mock(HOTLeafPage.class);
      when(orphanedLeaf.isClosed()).thenReturn(false);
      when(orphanedLeaf.isOrphaned()).thenReturn(true);

      final PageReference reference = new PageReference();
      reference.setPage(orphanedLeaf);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);

      assertNull(resolveForTraversal(storageEngineWriter, reference),
          "an orphaned-but-not-yet-torn-down leaf must not be handed to the descent");
    } finally {
      log.close();
    }
  }

  /** The live direction: a usable swizzled leaf is still served without touching storage. */
  @Test
  void aLiveSwizzledLeafStillResolvesWithoutReachingStorage() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage liveLeaf = mock(HOTLeafPage.class);
      when(liveLeaf.isClosed()).thenReturn(false);
      when(liveLeaf.isOrphaned()).thenReturn(false);

      final PageReference reference = new PageReference();
      reference.setPage(liveLeaf);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);

      assertSame(liveLeaf, resolveForTraversal(storageEngineWriter, reference));
      verify(storageEngineWriter, never()).loadHOTPage(any(PageReference.class));
    } finally {
      log.close();
    }
  }

  /**
   * The door behind the two above, and the one the {@code windows-latest / query} lane actually walks
   * through. A reference does not keep its log identity for the whole transaction: the pinned trie
   * spill publishes a durable offset through the handle every copy of a reference shares, and
   * {@code refreshTransactionLogReference()} then drops the log key. A copy taken before that point
   * also holds no swizzle — the copy constructor refuses to duplicate one for a reference that has a
   * resolution path — so by the time the descent asks about it, NOTHING about the reference says the
   * leaf it names was merged away. Only the reloaded page does: it carries the page key
   * {@code releaseOrphanedHOTLeaves} recorded.
   */
  @Test
  void aReleasedLeafIsStillRefusedAfterItsReferenceLostBothIdentityAndSwizzle() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      // Every indirect-page copy-on-write deep-copies its child references; this is one of those
      // copies, sharing the original's resolution handle and carrying no swizzle of its own.
      final PageReference copy = new PageReference(reference);
      assertNull(copy.getPage(), "a copy of a logged reference must not duplicate the swizzle");

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(reference), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      // The earlier epoch's spill completes: the shared handle publishes a durable offset, so both
      // the original and the copy stop naming a log entry at all.
      PageReference.completeTransactionLogReference(copy.transactionLogReference(), DURABLE_OFFSET);
      copy.refreshTransactionLogReference();
      assertEquals(DURABLE_OFFSET, copy.getKey());

      // What that offset addresses is the leaf's PRE-MERGE image: same logical page, same page key,
      // holding the entries the merge has since copied into the sibling.
      final HOTLeafPage preMergeImage = mock(HOTLeafPage.class);
      when(preMergeImage.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(preMergeImage);

      assertNull(resolveForTraversal(storageEngineWriter, copy),
          "a reference that lost its log identity must still not seed the descent with a merged-away leaf");
    } finally {
      log.close();
    }
  }

  /**
   * The same erasure one step earlier: {@code get()} itself performs it. The reference still names
   * its released entry on the way in, and the resolution rewrites it into a pure durable reference
   * before returning — so the classification has to be made against the identity the reference
   * carried BEFORE the lookup.
   */
  @Test
  void aReleasedEntryIsRecognisedFromTheIdentityHeldBeforeTheLookupErasedIt() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(reference), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      // Published but NOT yet refreshed: the reference still reads as a log entry until the very
      // resolution that is about to answer for it.
      PageReference.completeTransactionLogReference(reference.transactionLogReference(), DURABLE_OFFSET);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(mock(HOTLeafPage.class));

      assertNull(resolveForTraversal(storageEngineWriter, reference),
          "the released entry must be recognised from the pre-lookup identity");
      verify(storageEngineWriter, never()).loadHOTPage(any(PageReference.class));
    } finally {
      log.close();
    }
  }

  /**
   * A leaf the descent creates for an unoccupied slot must take its page key from the writer's
   * allocator. It used to take {@code reference.getKey()} — a durable BYTE OFFSET, the only creation
   * site in the whole HOT machinery that bypassed the allocator. Offsets and allocated page keys
   * share no numbering, so that stamped a second page with an already-issued key, and page keys are
   * identity here: the released-leaf test above, and {@code strandConfinedToLeaf}, both decide by
   * page key.
   */
  @Test
  void aLeafCreatedForAnEmptySlotTakesAnAllocatedPageKey() {
    final TransactionIntentLog log = newLog();
    try {
      final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
      when(storageEngineWriter.getLog()).thenReturn(log);
      when(storageEngineWriter.getRevisionNumber()).thenReturn(1);
      when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(mock(RevisionRootPage.class));
      stubPathPageKeyAllocator(storageEngineWriter);

      final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
      // An empty slot that nevertheless carries a durable offset: exactly the state the offset-as-
      // page-key code turned into a page key.
      final PageReference emptySlot = new PageReference().setKey(DURABLE_OFFSET);

      final AbstractHOTIndexWriter.LeafNavigationResult navigation =
          writer.prepareLeafOfTreeForTest(emptySlot, new byte[Long.BYTES], Long.BYTES);

      assertEquals(ALLOCATED_PAGE_KEY, navigation.leaf().getPageKey(),
          "a fresh leaf must carry an allocator-issued page key");
      assertNotEquals(DURABLE_OFFSET, navigation.leaf().getPageKey(), "a durable byte offset is not a page key");
    } finally {
      log.close();
    }
  }

  @Test
  void repeatedTopLevelDescentsReuseTheWriterOwnedNavigationCarrierAndPathBuffers() {
    final TransactionIntentLog log = newLog();
    try {
      final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
      when(storageEngineWriter.getLog()).thenReturn(log);
      when(storageEngineWriter.getRevisionNumber()).thenReturn(1);
      when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(mock(RevisionRootPage.class));
      stubPathPageKeyAllocator(storageEngineWriter);

      final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
      final PageReference emptySlot = new PageReference();
      final byte[] oversizedKey = new byte[64];
      oversizedKey[Long.BYTES] = (byte) 0xFF; // poison outside the valid key prefix

      final AbstractHOTIndexWriter.LeafNavigationResult first =
          writer.prepareLeafOfTreeForTest(emptySlot, oversizedKey, Long.BYTES);
      final HOTIndirectPage[] pathNodes = first.pathNodes();
      final PageReference[] pathRefs = first.pathRefs();
      final int[] pathChildIndices = first.pathChildIndices();

      final AbstractHOTIndexWriter.LeafNavigationResult second =
          writer.prepareLeafOfTreeForTest(emptySlot, oversizedKey, Long.BYTES);

      assertSame(first, second, "a transaction writer must not allocate a result carrier per descent");
      assertSame(pathNodes, second.pathNodes(), "path-node scratch must remain writer-owned");
      assertSame(pathRefs, second.pathRefs(), "path-reference scratch must remain writer-owned");
      assertSame(pathChildIndices, second.pathChildIndices(), "child-slot scratch must remain writer-owned");
    } finally {
      log.close();
    }
  }

  @Test
  void emptySlotRegistrationFailurePoisonsAndReleasesTheFreshLeaf() {
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    final IllegalStateException sentinel = new IllegalStateException("injected empty-slot registration failure");
    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(1);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(mock(RevisionRootPage.class));
    stubPathPageKeyAllocator(storageEngineWriter);
    doThrow(sentinel).when(log).put(any(PageReference.class), any(PageContainer.class));
    final HOTLeafPage initializationProbe = new HOTLeafPage(0, 1, IndexType.PATH);
    initializationProbe.close();
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> writer.prepareLeafOfTreeForTest(new PageReference(), new byte[Long.BYTES], Long.BYTES));

    assertSame(sentinel, failure);
    verify(storageEngineWriter).markTransactionRollbackOnly(same(sentinel));
    assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
        "the unregistered empty-tree leaf must return its off-heap frame");
  }

  /**
   * The filter must not over-reach: a reference this transaction never logged is simply not resident,
   * and reloading it is the whole point of the durable fallback.
   */
  @Test
  void aReferenceWithNoLogEntryStillReloadsFromStorage() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference reference = new PageReference();
      reference.setKey(DURABLE_OFFSET);

      final HOTLeafPage durableLeaf = mock(HOTLeafPage.class);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(durableLeaf);

      assertSame(durableLeaf, resolveForTraversal(storageEngineWriter, reference),
          "a merely non-resident reference must still reload");
    } finally {
      log.close();
    }
  }

  /**
   * Page keys are unique per index, not per transaction: each {@code (indexType, indexNumber)} pair
   * has its own {@code maxHotPageKey} counter while one log serves all of a transaction's indexes. A
   * merge in one index must therefore never refuse a live leaf in another that happens to carry the
   * same number — refusing a live leaf is a worse failure than the resurrection this refusal exists
   * to stop.
   */
  @Test
  void aMergeInOneIndexDoesNotRefuseALiveLeafOfAnotherWithTheSamePageKey() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference casReference = new PageReference();
      log.put(casReference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      log.releaseOrphanedHOTLeaves(TransactionIntentLog.indexScope(IndexType.CAS, 0), null, List.of(casReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      // A PATH leaf that is perfectly alive and shares the number, because its index counts its own.
      final HOTLeafPage livePathLeaf = mock(HOTLeafPage.class);
      when(livePathLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference pathReference = new PageReference().setKey(DURABLE_OFFSET);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(livePathLeaf);

      assertSame(livePathLeaf, resolveForTraversal(storageEngineWriter, pathReference),
          "a release in another index's page-key namespace must not refuse this leaf");
    } finally {
      log.close();
    }
  }

  /**
   * Refusing the pre-merge image is only half of the answer. The descent that reached the released
   * reference still has to continue somewhere, and the write path reads "nowhere" as "unoccupied
   * slot": it fabricates a fresh empty leaf over the reference, which keeps the slot's routing
   * partial while losing every key of it — the entries are in the merge target, and the stale slot
   * now answers them absent. The release therefore forwards the orphan's identity to the page that
   * absorbed it, and the descent continues THERE.
   */
  @Test
  void aReleasedLeafResolvesToThePageThatAbsorbedItsEntries() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference orphan = new PageReference();
      log.put(orphan, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      // A copy taken by an indirect-page copy-on-write before the merge: it keeps naming the orphan.
      final PageReference staleCopy = new PageReference(orphan);

      final HOTLeafPage mergeTarget = mock(HOTLeafPage.class);
      final PageReference replacement = new PageReference();
      log.put(replacement, PageContainer.getInstance(mergeTarget, mergeTarget));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, replacement, List.of(orphan),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      assertSame(mergeTarget, resolveForTraversal(storageEngineWriter, staleCopy),
          "a stale reference to a merged-away leaf must resolve to the leaf that absorbed its entries");
      verify(storageEngineWriter, never()).loadHOTPage(any(PageReference.class));
    } finally {
      log.close();
    }
  }

  /**
   * The same forwarding, one identity erasure later. Once the shared handle has published a durable
   * offset the reference names no entry at all, so only the reloaded page's own key still says the
   * leaf was merged away — and that key is what names the replacement.
   */
  @Test
  void aReleasedLeafResolvesToItsReplacementAfterItsReferenceLostItsLogIdentity() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference orphan = new PageReference();
      log.put(orphan, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      final PageReference staleCopy = new PageReference(orphan);

      final HOTLeafPage mergeTarget = mock(HOTLeafPage.class);
      final PageReference replacement = new PageReference();
      log.put(replacement, PageContainer.getInstance(mergeTarget, mergeTarget));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, replacement, List.of(orphan),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      PageReference.completeTransactionLogReference(staleCopy.transactionLogReference(), DURABLE_OFFSET);
      staleCopy.refreshTransactionLogReference();
      assertEquals(DURABLE_OFFSET, staleCopy.getKey());

      final HOTLeafPage preMergeImage = mock(HOTLeafPage.class);
      when(preMergeImage.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(preMergeImage);

      assertSame(mergeTarget, resolveForTraversal(storageEngineWriter, staleCopy),
          "the durable pre-merge image must be replaced by the page that absorbed its entries");
    } finally {
      log.close();
    }
  }

  /**
   * The forwarding has to outlive the replacement's own log identity, which is as perishable as the
   * orphan's. The pinned trie spills HOT leaves mid-transaction: the merge target's shared handle
   * publishes a durable offset and {@code refreshTransactionLogReference()} drops its log key on the
   * spot, so a merge running afterwards can name no {@code (generation, logKey)} for it at all. A
   * forwarding recorded as that identity is then no forwarding — the descent dead-ends exactly as if
   * the merge had named nothing, and the empty-slot arm refuses. Whether the spill has happened yet
   * is pure IO timing, which is why this only ever surfaced on the {@code windows-latest / query}
   * lane, as {@code "descent reached a released leaf with no replacement to forward to"}.
   */
  @Test
  void aReleasedLeafResolvesToAReplacementThatAlreadySpilledToADurableOffset() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference orphan = new PageReference();
      log.put(orphan, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      final PageReference staleCopy = new PageReference(orphan);

      final HOTLeafPage mergeTarget = mock(HOTLeafPage.class);
      final PageReference replacement = new PageReference();
      log.put(replacement, PageContainer.getInstance(mergeTarget, mergeTarget));
      // The spill, before the merge: from here the merge target names no log entry, only a disk offset.
      PageReference.completeTransactionLogReference(replacement.transactionLogReference(), REPLACEMENT_OFFSET);
      assertTrue(replacement.refreshTransactionLogReference(), "the merge target must now be a disk reference");
      assertEquals(Constants.NULL_ID_INT, replacement.getLogKey());

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, replacement, List.of(orphan),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(
          argThat(loaded -> loaded != null && loaded.getKey() == REPLACEMENT_OFFSET))).thenReturn(mergeTarget);

      assertSame(mergeTarget, resolveForTraversal(storageEngineWriter, staleCopy),
          "a replacement that spilled to a durable offset before the merge must still be forwarded to");
    } finally {
      log.close();
    }
  }

  /**
   * And when the descent still ends on a released leaf — a merge that named no replacement — the
   * empty-slot arm must NOT answer with a fresh leaf. Doing so is what turns a recoverable stale
   * reference into a silently truncated trie: the slot keeps routing its partial and answers every
   * key of it absent, and the first later insert whose mismatch bit reaches an ancestor's
   * discriminative bit fails as an unplaceable branch.
   */
  @Test
  void aDescentEndingOnAReleasedLeafRefusesToFabricateAnEmptyLeaf() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference orphan = new PageReference();
      log.put(orphan, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(orphan), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
      when(storageEngineWriter.getLog()).thenReturn(log);
      when(storageEngineWriter.getRevisionNumber()).thenReturn(1);
      when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(mock(RevisionRootPage.class));
      stubPathPageKeyAllocator(storageEngineWriter);

      final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> writer.prepareLeafOfTreeForTest(orphan, new byte[Long.BYTES], Long.BYTES),
          "a released leaf must not be answered with a fabricated empty leaf");
      assertTrue(failure.getMessage().contains("released leaf"), failure.getMessage());
    } finally {
      log.close();
    }
  }

  /**
   * The regression this test exists for: an unresolvable log entry is NOT proof that a merge released
   * its leaf, and treating it as one dead-ends a descent that had somewhere perfectly good to go.
   *
   * <p>
   * {@code publishPinnedSpillCandidate} closes the exact container it has just written out to a
   * durable offset — that is how the pinned-trie spill hands a HOT leaf to storage mid-transaction —
   * and it merges nothing away, so it records no forwarding link and blacklists no page key. An entry
   * still holding that closed leaf therefore looks byte-identical to a merged-away one from the
   * reference's side, while its durable image is the leaf's CURRENT content, not a pre-merge one.
   * Refusing it ended the {@code windows-latest / query} lane's transaction with
   * {@code "descent reached a released leaf with no replacement to forward to"} — the same message a
   * genuine merge dead end produces, which is why successive fixes to the FORWARDING never moved it.
   * </p>
   */
  @Test
  void aLeafClosedWithoutAMergeStillReloadsItsDurableImage() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage spilledLeaf = newReleasableLeaf();
      when(spilledLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(spilledLeaf, spilledLeaf));
      // The spill: the bytes are on disk under this offset, and the container it wrote out is closed.
      reference.setKey(DURABLE_OFFSET);
      spilledLeaf.close();
      assertTrue(log.namesReleasedHOTLeafEntry(reference), "the entry must now be unresolvable");

      // What the offset addresses is the leaf's LIVE content — the spill is what put it there.
      final HOTLeafPage durableImage = mock(HOTLeafPage.class);
      when(durableImage.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(durableImage);

      assertSame(durableImage, resolveForTraversal(storageEngineWriter, reference),
          "a leaf closed without an incremental merge must still reload its durable image");
    } finally {
      log.close();
    }
  }

  /**
   * The same page key, once a merge really has released it: the blacklist — not the entry's
   * resolvability — is what decides, so the durable image is still refused here.
   */
  @Test
  void theSamePageKeyIsStillRefusedOnceAMergeHasReleasedIt() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      when(mergedAwayLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      reference.setKey(DURABLE_OFFSET);

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(reference), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(mock(HOTLeafPage.class));

      assertNull(resolveForTraversal(storageEngineWriter, reference),
          "a merge-released page key must still refuse its pre-merge durable image");
      verify(storageEngineWriter, never()).loadHOTPage(any(PageReference.class));
    } finally {
      log.close();
    }
  }

  /**
   * A copy of a spilled entry's reference must reload through the reference the LOG owns.
   *
   * <p>
   * The {@code windows-latest / query} lane's remaining dead end, and the one no fix to the merge
   * FORWARDING could move — which is why the failure message stayed byte-identical across three
   * rounds that each closed a different forwarding door. A pinned-trie spill writes a HOT leaf out
   * mid-transaction, applies the durable offset to the reference the log holds for that entry, and
   * closes the exact container it wrote. Every indirect-page copy-on-write deep-copies its child
   * references, so the reference the trie routes through can be a copy taken before that publication:
   * it names the now-unresolvable entry, carries no offset of its own, and
   * {@code refreshTransactionLogReference()} has no completed handle to follow. Nothing MOVED — the
   * page is exactly where the spill put it — so the descent has to reach it through the owning
   * reference rather than dead-end on the copy.
   * </p>
   */
  @Test
  void aCopyOfASpilledEntrysReferenceReloadsThroughTheReferenceTheLogOwns() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage spilledLeaf = newReleasableLeaf();
      when(spilledLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference owningReference = new PageReference();
      log.put(owningReference, PageContainer.getInstance(spilledLeaf, spilledLeaf));

      // What the trie routes through: an independent copy taken before the spill published anything.
      final PageReference copyHeldByTheTrie = new PageReference(owningReference);

      // The spill: the bytes are on disk under an offset applied to the OWNING reference, and the
      // container it wrote out is closed. The copy learns neither.
      owningReference.setKey(DURABLE_OFFSET);
      spilledLeaf.close();
      assertTrue(log.namesReleasedHOTLeafEntry(copyHeldByTheTrie), "the copy still names the closed entry");
      assertEquals(Constants.NULL_ID_LONG, copyHeldByTheTrie.getKey(),
          "the copy is what this test is about: it never received the spill's durable offset");

      // What the offset addresses is the leaf's LIVE content — the spill is what put it there.
      final HOTLeafPage durableImage = mock(HOTLeafPage.class);
      when(durableImage.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final StorageEngineWriter storageEngineWriter = storageEngineWriter(log);
      when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenReturn(durableImage);

      assertSame(durableImage, resolveForTraversal(storageEngineWriter, copyHeldByTheTrie),
          "a copy that never received the spill's offset must resolve through the reference the log owns");
    } finally {
      log.close();
    }
  }

  /**
   * The refusal itself is unchanged where it belongs: a MERGE that named no replacement really did
   * move this slot's keys into a sibling, so answering {@code null} would hand the empty-slot arm the
   * one value it reads as permission to fabricate a leaf over a slot that still routes for them.
   */
  @Test
  void aMergedAwayEntryWithNoReplacementIsStillRefusedRatherThanFabricated() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage closedLeaf = newReleasableLeaf();
      when(closedLeaf.getPageKey()).thenReturn(MERGED_AWAY_PAGE_KEY);
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(closedLeaf, closedLeaf));
      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(reference), TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
      when(storageEngineWriter.getLog()).thenReturn(log);
      when(storageEngineWriter.getRevisionNumber()).thenReturn(1);
      when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(mock(RevisionRootPage.class));
      stubPathPageKeyAllocator(storageEngineWriter);

      final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> writer.prepareLeafOfTreeForTest(reference, new byte[Long.BYTES], Long.BYTES),
          "a merged-away entry with no replacement must not be answered with a fabricated leaf");
      assertTrue(failure.getMessage().contains("released leaf"), failure.getMessage());
    } finally {
      log.close();
    }
  }

  private static @Nullable Page resolveForTraversal(final StorageEngineWriter storageEngineWriter,
      final PageReference reference) {
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      final Method method =
          AbstractHOTIndexWriter.class.getDeclaredMethod("resolveHOTPageForTraversal", PageReference.class);
      method.setAccessible(true);
      return (Page) method.invoke(writer, reference);
    } catch (final InvocationTargetException invocationFailure) {
      final Throwable cause = invocationFailure.getCause();
      if (cause instanceof RuntimeException runtimeFailure) {
        throw runtimeFailure;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new AssertionError(cause);
    } catch (final ReflectiveOperationException reflectionFailure) {
      throw new AssertionError(reflectionFailure);
    }
  }

  /** A mock whose {@code close()} reports through {@code isClosed()}/{@code isOrphaned()}. */
  private static HOTLeafPage newReleasableLeaf() {
    final HOTLeafPage leaf = mock(HOTLeafPage.class);
    final AtomicBoolean released = new AtomicBoolean();
    doAnswer(invocation -> {
      released.set(true);
      return null;
    }).when(leaf).close();
    when(leaf.isClosed()).thenAnswer(invocation -> released.get());
    when(leaf.isOrphaned()).thenAnswer(invocation -> released.get());
    return leaf;
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }

  private static void stubPathPageKeyAllocator(final StorageEngineWriter storageEngineWriter) {
    final PathPage pathPage = mock(PathPage.class);
    // doReturn/when, not when(...).thenReturn: the writer mocks here use RETURNS_DEEP_STUBS, and
    // prepareSecondaryIndexPage's return type erases to Page — so inside when(...) the deep-stub
    // answer hands back a Page mock that javac's inserted checkcast to PathPage rejects before the
    // stubbing exists. The doReturn form invokes the method in statement position, where no cast is
    // inserted, and installs the answer without consulting the deep-stub default.
    doReturn(pathPage).when(storageEngineWriter).prepareSecondaryIndexPage(IndexType.PATH);
    when(pathPage.incrementAndGetMaxHotPageKey(0)).thenReturn(ALLOCATED_PAGE_KEY);
  }

  private static StorageEngineWriter storageEngineWriter(final TransactionIntentLog log) {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(log);
    return storageEngineWriter;
  }

  private static final class TestIndexWriter extends AbstractHOTIndexWriter<Long> {
    private byte[] keyBuffer = new byte[Long.BYTES];

    private TestIndexWriter(final StorageEngineWriter storageEngineWriter) {
      super(storageEngineWriter, IndexType.PATH, 0);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] newBuffer) {
      keyBuffer = newBuffer;
    }

    @Override
    protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
      return 0;
    }

    /** The index page has no bearing on which page key a fresh leaf gets; skip its copy-on-write. */
    @Override
    protected void prepareIndexPage() {
      // no-op
    }

    LeafNavigationResult prepareLeafOfTreeForTest(final PageReference rootRef, final byte[] keyBuf, final int keyLen) {
      return prepareLeafOfTree(rootRef, keyBuf, keyLen);
    }
  }
}

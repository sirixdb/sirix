/*
 * Copyright (c) 2026, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.BufferManager;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, List.of(reference));

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

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, List.of(reference));

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

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, List.of(reference));

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
      when(storageEngineWriter.getPathPage(any()).incrementAndGetMaxHotPageKey(0)).thenReturn(ALLOCATED_PAGE_KEY);

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
      log.releaseOrphanedHOTLeaves(TransactionIntentLog.indexScope(IndexType.CAS, 0), List.of(casReference));

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

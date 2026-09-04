/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Identity, ownership and metadata regressions for the generic branch-leaf pairing primitive. */
final class HOTBranchLeafPairIdentityTest {

  private static final long PAIR_MASK = 0xA000_0000_0000_0000L; // absolute bits 0 and 2
  private static final int FRAME_CLASS = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
  private static FrameSlotAllocator frameAllocator;

  @BeforeAll
  static void initializeAllocator() {
    frameAllocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    frameAllocator.init(8L * 1024 * 1024 * 1024);
  }

  @Test
  void heightOneParentKeepsOneLogicalSourceIdentityAcrossSnapshotAndSpill() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final TransactionIntentLog log = newLog();
    final HeightOneShape shape = installHeightOneShape(log);
    final PageReference staleSource = new PageReference(shape.sourceReference());
    final WriterFixture fixture = writerFixture(log, shape.rootReference());

    try {
      fixture.writer().insert(key(0x60), value(0x60));

      final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, pageOf(log, shape.rootReference()));
      final PageReference pairedSource = findLeafReference(root, shape.source(), log);
      assertNotNull(pairedSource, "the pair must retain the exact source leaf");
      assertNotSameReference(shape.sourceReference(), pairedSource,
          "integrate needs an independent wrapper, not an alias of its spine slot");
      assertSame(shape.sourceReference().transactionLogReference(), pairedSource.transactionLogReference(),
          "the independent wrapper must preserve the source's one logical TIL identity");
      assertSame(shape.source(), pageOf(log, pairedSource));
      assertFalse(shape.source().isClosed());
      assertFalse(
          log.namesReleasedHOTLeafPage(TransactionIntentLog.indexScope(IndexType.PATH, 0), shape.source().getPageKey()),
          "a live shared identity must not be retired");
      assertTrue(HOTMalformedSubtreeDetector.detect(shape.rootReference(), ref -> pageOf(log, ref)).isEmpty());

      assertTrue(log.snapshot() > 0,
          "snapshot returns the prior sparse array extent even after HOT pages move to pinned slots");
      log.cleanupSnapshot();
      assertSame(shape.source(), pageOf(log, staleSource));
      assertSame(shape.source(), pageOf(log, pairedSource));

      spillPinnedReference(log, shape.sourceReference(), 91_000L);
      assertNull(log.get(staleSource));
      assertNull(log.get(pairedSource));
      assertEquals(91_000L, staleSource.getKey());
      assertEquals(91_000L, pairedSource.getKey());
      assertNotEquals(shape.rootReference().getKey(), pairedSource.getKey(),
          "the child must never resolve to its containing parent");
    } finally {
      log.close();
    }

    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS),
        "the shared-handle path must retain no extra HOT frame after transaction close");
  }

  @Test
  void rootLeafReplacementUsesAFreshPhysicalCopyAndCannotCycleToItself() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final TransactionIntentLog log = newLog();
    final HOTLeafPage source = projectionSource(20, 1);
    final PageReference sourceReference = reference(source);
    final PageReference staleSource = new PageReference();
    PageReference pendingSideReference = null;

    try {
      pendingSideReference = source.getPageReference(HOTLeafPage.overflowPageRefKey(0x10, 2));
      log.put(sourceReference, PageContainer.getInstance(source, source));
      staleSource.shareTransactionLogReference(sourceReference);
      staleSource.setLogKey(sourceReference.getLogKey());
      staleSource.setActiveTilGeneration(sourceReference.getActiveTilGeneration());

      final HOTLeafPage copiedSource = source.copyAsFreshPage(2_000L, 2);
      final PageReference copiedSourceReference = reference(copiedSource);
      assertProjectionCopy(source, copiedSource, pendingSideReference, 2_000L, 2);
      final HOTLeafPage right = new HOTLeafPage(2_001L, 2, IndexType.PROJECTION);
      assertTrue(right.put(key(0x80), new byte[] {9, 8, 7}));
      final PageReference rightReference = reference(right);
      final HOTIncrementalInsert.BiNode pair =
          new HOTIncrementalInsert.BiNode(0, 1, copiedSourceReference, rightReference);
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(new HOTIndirectPage[0],
          new PageReference[] {sourceReference}, new int[0], 0, pair, 2, new AtomicLong(2_100L)::getAndIncrement);

      log.put(copiedSourceReference, PageContainer.getInstance(copiedSource, copiedSource));
      log.put(rightReference, PageContainer.getInstance(right, right));
      final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, result.touchedRef().getPage());
      log.put(result.touchedRef(), PageContainer.getInstance(root, root));

      assertTrue(source.isClosed(), "same-reference publication must retire the displaced source frame");
      assertSame(root, pageOf(log, staleSource), "a stale source handle must resolve to the replacement root");
      assertSame(copiedSource, pageOf(log, copiedSourceReference),
          "the legitimate child must resolve to its fresh physical page, not back to the root");
      assertNotEquals(source.getPageKey(), copiedSource.getPageKey());

      assertTrue(log.snapshot() > 0);
      log.cleanupSnapshot();
      spillPinnedReference(log, sourceReference, 92_000L);
      assertNull(log.get(staleSource));
      assertEquals(92_000L, staleSource.getKey());
      assertSame(copiedSource, pageOf(log, copiedSourceReference));
    } finally {
      log.close();
      if (pendingSideReference != null && pendingSideReference.hasPendingPageWrite()) {
        pendingSideReference.cancelPendingPageWrite();
      }
    }

    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS),
        "root replacement and its independent child must release every frame exactly once");
  }

  @Test
  void mixedHeightParentCopiesTheSourceBeforePublishingAtItsOwnReference() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final TransactionIntentLog log = newLog();
    final MixedHeightShape shape = installMixedHeightShape(log);
    final PageReference staleSource = new PageReference(shape.sourceReference());
    final WriterFixture fixture = writerFixture(log, shape.rootReference());

    try {
      fixture.writer().insert(key(0x60), value(0x60));

      final HOTIndirectPage pair = assertInstanceOf(HOTIndirectPage.class, pageOf(log, shape.sourceReference()));
      final HOTLeafPage copiedSource = findLeafByKey(pair, key(0x00), log);
      assertNotNull(copiedSource);
      assertNotSame(shape.source(), copiedSource);
      assertNotEquals(shape.source().getPageKey(), copiedSource.getPageKey());
      assertTrue(shape.source().isClosed(), "publishing at the source identity must retire only the old frame");
      assertSame(pair, pageOf(log, staleSource), "the stale identity follows the replacement pair root");

      final PageReference copiedSourceReference = findLeafReference(pair, copiedSource, log);
      assertNotNull(copiedSourceReference);
      assertSame(copiedSource, pageOf(log, copiedSourceReference));
      assertNotSame(pair, pageOf(log, copiedSourceReference));
      assertTrue(HOTMalformedSubtreeDetector.detect(shape.rootReference(), ref -> pageOf(log, ref)).isEmpty());

      assertTrue(log.snapshot() > 0);
      log.cleanupSnapshot();
      spillPinnedReference(log, shape.sourceReference(), 93_000L);
      assertNull(log.get(staleSource));
      assertEquals(93_000L, staleSource.getKey());
      assertSame(copiedSource, pageOf(log, copiedSourceReference),
          "spilling the containing pair must not redirect its independent child back to the pair");
    } finally {
      log.close();
    }

    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS),
        "the mixed-height copy path must retain no old or copied source frame after close");
  }

  @Test
  void parentRegistrationFailureRetainsBothAlreadyLoggedPairLeaves() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final TransactionIntentLog delegate = newLog();
    final TransactionIntentLog log = spy(delegate);
    final MixedHeightShape shape = installMixedHeightShape(log);
    final WriterFixture fixture = writerFixture(log, shape.rootReference());
    final IllegalStateException sentinel = new IllegalStateException("injected pair-root registration failure");

    doAnswer(invocation -> {
      final PageReference reference = invocation.getArgument(0);
      final PageContainer container = invocation.getArgument(1);
      if (reference == shape.sourceReference() && container.getModified() instanceof HOTIndirectPage) {
        throw sentinel;
      }
      return invocation.callRealMethod();
    }).when(log).put(any(PageReference.class), any(PageContainer.class));

    try {
      assertSame(sentinel,
          assertThrows(IllegalStateException.class, () -> fixture.writer().insert(key(0x60), value(0x60))));
      verify(fixture.storageEngineWriter(), atLeastOnce()).markTransactionRollbackOnly(sentinel);

      final List<HOTLeafPage> registeredFreshLeaves = new ArrayList<>();
      for (final PageContainer container : log.getList()) {
        if (container.getModified() instanceof HOTLeafPage leaf && leaf.getPageKey() >= 1_000L) {
          registeredFreshLeaves.add(leaf);
        }
      }
      assertEquals(2, registeredFreshLeaves.size(),
          "post-order registration must have transferred both source-copy and right-leaf ownership");
      for (final HOTLeafPage leaf : registeredFreshLeaves) {
        assertFalse(leaf.isClosed(), "a publication catch must not free a TIL-owned leaf frame");
      }
      assertFalse(shape.source().isClosed(), "the failed parent replacement still owns the original source");
    } finally {
      log.close();
    }

    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS),
        "rollback teardown must release original and already-transferred fresh frames exactly once");
  }

  @Test
  void freshProjectionCopyPreservesOpaqueStateAndReleasesFramesOnSuccessAndFailure() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final HOTLeafPage source = projectionSource(30, 4);
    final PageReference pending = source.getPageReference(HOTLeafPage.overflowPageRefKey(0x10, 2));
    final HOTLeafPage copy;
    try {
      copy = source.copyAsFreshPage(31, 9);
      try {
        assertProjectionCopy(source, copy, pending, 31L, 9);
        assertEquals(liveBefore + 2, frameAllocator.liveSlotCount(FRAME_CLASS));
      } finally {
        copy.close();
      }
      assertEquals(liveBefore + 1, frameAllocator.liveSlotCount(FRAME_CLASS));
    } finally {
      source.close();
      if (pending != null && pending.hasPendingPageWrite()) {
        pending.cancelPendingPageWrite();
      }
    }
    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS));

    final HOTLeafPage malformed = new HOTLeafPage(32, 4, IndexType.PROJECTION);
    final OverflowPage pendingPage = new OverflowPage(new byte[] {5, 4, 3});
    final PageReference malformedPending = reference(pendingPage);
    malformedPending.bindPendingPageWrite(pendingPage);
    malformedPending.setKey(777L); // impossible pending shape; copy must fail closed after allocating its frame
    malformed.setPageReference(HOTLeafPage.overflowPageRefKey(0x10, 0), malformedPending);
    final int beforeFailure = frameAllocator.liveSlotCount(FRAME_CLASS);
    try {
      assertThrows(IllegalStateException.class, () -> malformed.copyAsFreshPage(33, 5));
      assertEquals(beforeFailure, frameAllocator.liveSlotCount(FRAME_CLASS),
          "a side-reference validation failure must return the newly allocated copy frame");
    } finally {
      malformed.close();
      malformedPending.cancelPendingPageWrite();
    }
    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS));
  }

  @Test
  void consolidationFailureBeforeItsOwnPublicationStillPoisonsTheCompletedPrimaryPut() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final PathPage pathPage = mock(PathPage.class);
    final IllegalStateException sentinel = new IllegalStateException("injected consolidation allocation failure");
    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(2);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getPathPage(revisionRootPage)).thenReturn(pathPage);
    doReturn(pathPage).when(storageEngineWriter).prepareSecondaryIndexPage(IndexType.PATH);
    when(pathPage.incrementAndGetMaxHotPageKey(0)).thenThrow(sentinel);

    final HOTLeafPage left = leaf(300, 0x00);
    final HOTLeafPage right = leaf(301, 0x80);
    final HOTLeafPage high = leaf(302, 0xA0);
    final PageReference leftReference = reference(left);
    final PageReference rightReference = reference(right);
    final PageReference highReference = reference(high);
    final HOTIndirectPage parent = HOTIndirectPage.createSpanNode(303, 1, 0, PAIR_MASK, new int[] {0, 2, 3},
        new PageReference[] {leftReference, rightReference, highReference}, 1);
    final PageReference parentReference = reference(parent);
    final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(left,
        leftReference, new HOTIndirectPage[] {parent}, new PageReference[] {parentReference}, new int[] {0}, 1);
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);

    try {
      assertSame(sentinel, assertThrows(IllegalStateException.class, () -> invokePrivate(writer,
          "consolidateLeafParent", new Class<?>[] {AbstractHOTIndexWriter.LeafNavigationResult.class}, route)));
      verify(storageEngineWriter).markTransactionRollbackOnly(sentinel);
      assertSame(parent, parentReference.getPage(),
          "the maintenance parent must remain untouched when construction fails before publication");
      assertFalse(left.isClosed());
      assertFalse(right.isClosed());
      assertFalse(high.isClosed());
    } finally {
      left.close();
      right.close();
      high.close();
    }
    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS));
  }

  @Test
  @ResourceLock("HOT_STRUCTURAL_PUBLICATION_TEST_HOOK")
  void consolidationPublicationFailureClosesEveryUnregisteredReplacementLeaf() {
    final int liveBefore = frameAllocator.liveSlotCount(FRAME_CLASS);
    final TransactionIntentLog log = newLog();
    final HeightOneShape shape = installHeightOneShape(log);
    final WriterFixture fixture = writerFixture(log, shape.rootReference());
    final HOTIndirectPage parent = assertInstanceOf(HOTIndirectPage.class, pageOf(log, shape.rootReference()));
    final List<HOTLeafPage> originalLeaves = new ArrayList<>(parent.getNumChildren());
    for (int childIndex = 0; childIndex < parent.getNumChildren(); childIndex++) {
      originalLeaves.add(assertInstanceOf(HOTLeafPage.class, pageOf(log, parent.getChildReference(childIndex))));
    }
    final AbstractHOTIndexWriter.LeafNavigationResult route =
        new AbstractHOTIndexWriter.LeafNavigationResult(shape.source(), shape.sourceReference(),
            new HOTIndirectPage[] {parent}, new PageReference[] {shape.rootReference()}, new int[] {0}, 1);
    final IllegalStateException sentinel = new IllegalStateException("injected after consolidation publication");
    final List<HOTLeafPage> locallyOwnedReplacements = new ArrayList<>();

    AbstractHOTIndexWriter.setStructuralPublicationTestHook(() -> {
      final Page published = shape.rootReference().getPage();
      if (published instanceof HOTIndirectPage consolidated) {
        for (int childIndex = 0; childIndex < consolidated.getNumChildren(); childIndex++) {
          final PageReference childReference = consolidated.getChildReference(childIndex);
          if (childReference != null && childReference.getKey() < 0 && childReference.getLogKey() < 0
              && childReference.getPage() instanceof HOTLeafPage freshLeaf) {
            locallyOwnedReplacements.add(freshLeaf);
          }
        }
      }
      throw sentinel;
    });

    try {
      assertSame(sentinel, assertThrows(IllegalStateException.class, () -> invokePrivate(fixture.writer(),
          "consolidateLeafParent", new Class<?>[] {AbstractHOTIndexWriter.LeafNavigationResult.class}, route)));
      verify(fixture.storageEngineWriter()).markTransactionRollbackOnly(sentinel);
      assertFalse(locallyOwnedReplacements.isEmpty(),
          "the fixture must publish at least one locally owned merged leaf before registration");
      for (final HOTLeafPage replacement : locallyOwnedReplacements) {
        assertTrue(replacement.isClosed(), "an unregistered consolidation replacement must be closed");
      }
      for (final HOTLeafPage original : originalLeaves) {
        assertFalse(original.isClosed(), "cleanup must not retire a TIL-owned source leaf");
      }
    } finally {
      AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
      log.close();
    }

    assertEquals(liveBefore, frameAllocator.liveSlotCount(FRAME_CLASS),
        "the failed consolidation publication must retain no HOT frame");
  }

  private static void assertProjectionCopy(final HOTLeafPage source, final HOTLeafPage copy,
      final PageReference pendingSideReference, final long expectedPageKey, final int expectedRevision) {
    assertEquals(IndexType.PROJECTION, copy.getIndexType());
    assertNotEquals(source.getPageKey(), copy.getPageKey());
    assertEquals(expectedPageKey, copy.getPageKey());
    assertEquals(expectedRevision, copy.getRevision());
    assertTrue(copy.isCompleteDump());
    assertNull(copy.getCompletePageRef());
    assertArrayEquals(source.getAncestorOwnedBits(), copy.getAncestorOwnedBits());
    assertArrayEquals(source.getAncestorOwnedValues(), copy.getAncestorOwnedValues());
    assertEquals(source.getEntryCount(), copy.getEntryCount());
    for (int i = 0; i < source.getEntryCount(); i++) {
      assertArrayEquals(source.getKey(i), copy.getKey(i));
      assertArrayEquals(source.getValue(i), copy.getValue(i), "opaque projection bytes must be byte-exact");
    }
    assertEquals(source.segmentRefCount(), copy.segmentRefCount());
    for (final long refKey : source.overflowPageRefKeysSorted()) {
      final PageReference sourceRef = source.getPageReference(refKey);
      final PageReference copyRef = copy.getPageReference(refKey);
      assertNotNull(copyRef);
      if (sourceRef == pendingSideReference) {
        assertSame(sourceRef, copyRef, "a bounded pending immutable write must keep its exact coordination identity");
      } else {
        assertNotSameReference(sourceRef, copyRef, "ordinary side references require independent wrappers");
        assertEquals(sourceRef.getKey(), copyRef.getKey());
        assertSame(sourceRef.getPage(), copyRef.getPage(),
            "a resident immutable side page may be shared while reference wrappers remain independent");
      }
    }
  }

  private static void invokePrivate(final Object target, final String methodName, final Class<?>[] parameterTypes,
      final Object... arguments) {
    try {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod(methodName, parameterTypes);
      method.setAccessible(true);
      method.invoke(target, arguments);
    } catch (final InvocationTargetException invocationFailure) {
      final Throwable cause = invocationFailure.getCause();
      if (cause instanceof RuntimeException runtimeFailure) {
        throw runtimeFailure;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new AssertionError("private HOT test invocation failed", cause);
    } catch (final ReflectiveOperationException reflectionFailure) {
      throw new AssertionError("cannot invoke private HOT test target " + methodName, reflectionFailure);
    }
  }

  private static HOTLeafPage projectionSource(final long pageKey, final int revision) {
    final HOTLeafPage source = new HOTLeafPage(pageKey, revision, IndexType.PROJECTION);
    assertTrue(source.put(key(0x10), new byte[0]));
    assertTrue(source.put(key(0x18), new byte[] {0, (byte) 0xFE, 7, 6}));
    source.setAncestorOwnedBits(new int[] {0, 3}, new byte[] {0, 1});
    source.setCompletePageRef(mock(HOTLeafPage.class));
    source.setCompleteDump(false);

    final PageReference durable = new PageReference().setKey(71_000L);
    source.setPageReference(HOTLeafPage.overflowPageRefKey(0x10, 0), durable);
    source.setPageReference(HOTLeafPage.overflowPageRefKey(0x18, 1), reference(mock(Page.class)));
    final OverflowPage pendingPage = new OverflowPage(new byte[] {9, 8, 7});
    final PageReference pending = reference(pendingPage);
    pending.bindPendingPageWrite(pendingPage);
    source.setPageReference(HOTLeafPage.overflowPageRefKey(0x10, 2), pending);
    return source;
  }

  private static HeightOneShape installHeightOneShape(final TransactionIntentLog log) {
    final HOTLeafPage source = leaf(100, 0x00);
    final PageReference sourceReference = reference(source);
    final HOTLeafPage middle = leaf(101, 0x80);
    final PageReference middleReference = reference(middle);
    final HOTLeafPage high = leaf(102, 0xA0);
    final PageReference highReference = reference(high);
    final HOTIndirectPage root = HOTIndirectPage.createSpanNode(103, 1, 0, PAIR_MASK, new int[] {0, 2, 3},
        new PageReference[] {sourceReference, middleReference, highReference}, 1);
    final PageReference rootReference = reference(root);

    put(log, sourceReference, source);
    put(log, middleReference, middle);
    put(log, highReference, high);
    put(log, rootReference, root);
    return new HeightOneShape(rootReference, sourceReference, source);
  }

  private static MixedHeightShape installMixedHeightShape(final TransactionIntentLog log) {
    final HOTLeafPage source = leaf(200, 0x00);
    final PageReference sourceReference = reference(source);
    final HOTLeafPage middleLow = leaf(201, 0x80);
    final PageReference middleLowReference = reference(middleLow);
    final HOTLeafPage middleHigh = leaf(202, 0x90);
    final PageReference middleHighReference = reference(middleHigh);
    final HOTIndirectPage middle = HOTIndirectPage.createSpanNode(203, 1, 0, 0x1000_0000_0000_0000L, new int[] {0, 1},
        new PageReference[] {middleLowReference, middleHighReference}, 1);
    final PageReference middleReference = reference(middle);
    final HOTLeafPage high = leaf(204, 0xA0);
    final PageReference highReference = reference(high);
    final HOTIndirectPage root = HOTIndirectPage.createSpanNode(205, 1, 0, PAIR_MASK, new int[] {0, 2, 3},
        new PageReference[] {sourceReference, middleReference, highReference}, 2);
    final PageReference rootReference = reference(root);

    put(log, sourceReference, source);
    put(log, middleLowReference, middleLow);
    put(log, middleHighReference, middleHigh);
    put(log, middleReference, middle);
    put(log, highReference, high);
    put(log, rootReference, root);
    return new MixedHeightShape(rootReference, sourceReference, source);
  }

  private static WriterFixture writerFixture(final TransactionIntentLog log, final PageReference rootReference) {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final PathPage pathPage = mock(PathPage.class);
    final AtomicLong pageKeys = new AtomicLong(1_000L);
    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(2);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getPathPage(revisionRootPage)).thenReturn(pathPage);
    doReturn(pathPage).when(storageEngineWriter).prepareSecondaryIndexPage(IndexType.PATH);
    when(pathPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(ignored -> pageKeys.getAndIncrement());
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    writer.installRoot(rootReference);
    return new WriterFixture(storageEngineWriter, writer);
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }

  private static void put(final TransactionIntentLog log, final PageReference reference, final Page page) {
    log.put(reference, PageContainer.getInstance(page, page));
  }

  private static Page pageOf(final TransactionIntentLog log, final PageReference reference) {
    final PageContainer container = log.get(reference);
    if (container != null) {
      return container.getModified() != null
          ? container.getModified()
          : container.getComplete();
    }
    return reference.getPage();
  }

  private static PageReference findLeafReference(final HOTIndirectPage root, final HOTLeafPage target,
      final TransactionIntentLog log) {
    final Set<Page> visited = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    return findLeafReference(root, target, log, visited);
  }

  private static PageReference findLeafReference(final HOTIndirectPage node, final HOTLeafPage target,
      final TransactionIntentLog log, final Set<Page> visited) {
    if (!visited.add(node)) {
      return null;
    }
    for (int i = 0; i < node.getNumChildren(); i++) {
      final PageReference childReference = node.getChildReference(i);
      final Page child = pageOf(log, childReference);
      if (child == target) {
        return childReference;
      }
      if (child instanceof HOTIndirectPage indirect) {
        final PageReference found = findLeafReference(indirect, target, log, visited);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static HOTLeafPage findLeafByKey(final HOTIndirectPage root, final byte[] key,
      final TransactionIntentLog log) {
    Page current = root;
    for (int depth = 0; depth < 8 && current instanceof HOTIndirectPage indirect; depth++) {
      final int childIndex = indirect.findChildIndex(key);
      if (childIndex < 0) {
        return null;
      }
      current = pageOf(log, indirect.getChildReference(childIndex));
    }
    return current instanceof HOTLeafPage leaf && leaf.findEntry(key) >= 0
        ? leaf
        : null;
  }

  private static void spillPinnedReference(final TransactionIntentLog log, final PageReference target,
      final long diskOffset) {
    final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
    final int attempts = Math.max(1, log.pinnedHighWater() * 2);
    for (int i = 0; i < attempts; i++) {
      batch.clear();
      if (log.capturePinnedSpillCandidates(1, batch) == 0) {
        continue;
      }
      if (batch.referenceAt(0) == target) {
        batch.setWriteResult(0, diskOffset, 0L, false);
        log.publishPinnedSpillCandidate(batch, 0);
        batch.clear();
        return;
      }
    }
    throw new AssertionError("target reference was not offered as a bounded pinned-spill candidate");
  }

  private static HOTLeafPage leaf(final long pageKey, final int unsignedKey) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PATH);
    assertTrue(leaf.put(key(unsignedKey), value(unsignedKey)));
    return leaf;
  }

  private static PageReference reference(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static byte[] key(final int unsignedByte) {
    return new byte[] {(byte) unsignedByte};
  }

  private static byte[] value(final int unsignedByte) {
    return new byte[] {(byte) unsignedByte};
  }

  private static void assertNotSameReference(final Object unexpected, final Object actual, final String message) {
    assertFalse(unexpected == actual, message);
  }

  private record HeightOneShape(PageReference rootReference, PageReference sourceReference, HOTLeafPage source) {
  }

  private record MixedHeightShape(PageReference rootReference, PageReference sourceReference, HOTLeafPage source) {
  }

  private record WriterFixture(StorageEngineWriter storageEngineWriter, TestIndexWriter writer) {
  }

  private static final class TestIndexWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestIndexWriter(final StorageEngineWriter storageEngineWriter) {
      super(storageEngineWriter, IndexType.PATH, 0);
    }

    private void installRoot(final PageReference root) {
      rootReference = root;
    }

    private void insert(final byte[] key, final byte[] value) {
      doIndex(key, key.length, value, value.length);
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
    protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
      System.arraycopy(key, 0, buffer, offset, key.length);
      return key.length;
    }

    @Override
    protected void prepareIndexPage() {
      // This focused fixture installs an already materialized, TIL-owned root directly.
    }
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Atomicity and exact-height coverage for full-node Direction 1 inside a split half. */
final class HOTDirectionOneSplitHalfAtomicityTest {

  @Test
  void tilClearedUniqueTallestChildKeepsExactHeightAcrossRefreshAndNextInsert() {
    final FullNodeShape shape = fullNodeShape();
    final WriterFixture fixture = writerFixture(shape.rootRef, -1, null);
    stageTallChildAsTilOnly(shape, fixture);
    final HOTIncrementalInsert.BiNode split = initialSplitWithResolvedChildren(shape);
    final HOTIndirectPage leftHalf = (HOTIndirectPage) split.left().getPage();

    try {
      assertTrue(fixture.writer.directionOneForTest(shape.route, shape.originalNode, split, leftHalf, shape.insertedKey,
          value(0x51)));

      Page root = fixture.resolve(shape.rootRef);
      assertEquals(34, countEntries(root, fixture));
      assertExactStoredHeights(root, fixture);
      assertTrue(HOTMalformedSubtreeDetector.detect(shape.rootRef, fixture::resolve).isEmpty());

      // A second insert through the rebuilt unique-tallest child exercises the height-sensitive
      // integrate decision made from the refreshed split.
      fixture.writer.insert(key(0x52), value(0x52));
      root = fixture.resolve(shape.rootRef);
      assertEquals(35, countEntries(root, fixture));
      assertExactStoredHeights(root, fixture);
      assertTrue(HOTMalformedSubtreeDetector.detect(shape.rootRef, fixture::resolve).isEmpty());
      assertTrue(HOTInvariantValidator.validate(shape.rootRef, fixture.storageEngineWriter).violations().isEmpty());
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeReachable(shape.rootRef, fixture, new IdentityHashMap<>());
    }
  }

  @Test
  void refreshedSplitAllocatorFailureAfterSubInsertPoisonsAndCannotFallback() {
    final FullNodeShape shape = fullNodeShape();
    final IllegalArgumentException sentinel = new IllegalArgumentException("injected refreshed-split allocation");
    final WriterFixture fixture = writerFixture(shape.rootRef, 0, sentinel);
    stageTallChildAsTilOnly(shape, fixture);
    final HOTIncrementalInsert.BiNode split = initialSplitWithResolvedChildren(shape);
    final HOTIndirectPage leftHalf = (HOTIndirectPage) split.left().getPage();

    try {
      final IllegalArgumentException failure =
          assertThrows(IllegalArgumentException.class, () -> fixture.writer.directionOneForTest(shape.route,
              shape.originalNode, split, leftHalf, shape.insertedKey, value(0x51)));

      assertSame(sentinel, failure);
      assertSame(shape.originalNode, shape.rootRef.getPage(),
          "the refreshed split failed before the parent/root publication boundary");
      assertTrue(shape.tallLowLeaf.findEntry(shape.insertedKey) >= 0,
          "the sub-insert must have logically published before the injected allocator failure");
      verify(fixture.storageEngineWriter, atLeastOnce()).markTransactionRollbackOnly(same(sentinel));
    } finally {
      closeReachable(shape.rootRef, fixture, new IdentityHashMap<>());
    }
  }

  private static void stageTallChildAsTilOnly(final FullNodeShape shape, final WriterFixture fixture) {
    // Reproduce real TransactionIntentLog.put: the modified page is authoritative in the TIL and
    // the shared reference is unswizzled. The first speculative split retains this exact reference.
    fixture.logged.put(shape.tallChildRef, PageContainer.getInstance(shape.tallChild, shape.tallChild));
    fixture.logged.put(shape.tallLowRef, PageContainer.getInstance(shape.tallLowLeaf, shape.tallLowLeaf));
    shape.tallChildRef.setPage(null);
  }

  private static HOTIncrementalInsert.BiNode initialSplitWithResolvedChildren(final FullNodeShape shape) {
    // The production full-node path resolves every direct child before its first height-sensitive
    // split. Clear the swizzle again afterwards to model the later TIL.put performed by subInsertAt;
    // directionOneIntoSplitHalf must re-resolve that child before producing the refreshed split.
    shape.tallChildRef.setPage(shape.tallChild);
    final HOTIncrementalInsert.BiNode split =
        HOTIncrementalInsert.splitIndirect(shape.originalNode, 2, new AtomicLong(500)::getAndIncrement);
    shape.tallChildRef.setPage(null);
    return split;
  }

  private static FullNodeShape fullNodeShape() {
    final PageReference[] children = new PageReference[HOTIndirectPage.MAX_NODE_ENTRIES];
    final int[] partials = new int[HOTIndirectPage.MAX_NODE_ENTRIES];
    final AtomicLong pageKeys = new AtomicLong(10);
    PageReference tallChildRef = null;
    PageReference tallLowRef = null;
    HOTIndirectPage tallChild = null;
    HOTLeafPage tallLowLeaf = null;
    for (int slot = 0; slot < children.length; slot++) {
      partials[slot] = slot;
      if (slot == 10) {
        tallLowLeaf = leaf(pageKeys.getAndIncrement(), key(0x50), value(0x50));
        final HOTLeafPage tallHighLeaf = leaf(pageKeys.getAndIncrement(), key(0x54), value(0x54));
        tallLowRef = durableReference(tallLowLeaf, 200 + slot * 2L);
        tallChild = HOTIndirectPage.createSpanNode(pageKeys.getAndIncrement(), 1, 0, 1L << (63 - 5), new int[] {0, 1},
            new PageReference[] {tallLowRef, durableReference(tallHighLeaf, 201 + slot * 2L)}, 1);
        tallChildRef = durableReference(tallChild, 300 + slot);
        children[slot] = tallChildRef;
      } else {
        children[slot] = durableReference(leaf(pageKeys.getAndIncrement(), key(slot << 3), value(slot)), 300 + slot);
      }
    }
    final HOTIndirectPage originalNode = HOTIndirectPage.createMultiNode(pageKeys.getAndIncrement(), 1, 0,
        0xF800_0000_0000_0000L, partials, children, 2);
    final PageReference rootRef = durableReference(originalNode, 400);
    final byte[] insertedKey = key(0x51);
    final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(
        tallLowLeaf, tallLowRef, new HOTIndirectPage[] {originalNode, tallChild},
        new PageReference[] {rootRef, tallChildRef}, new int[] {10, 0}, 2);
    return new FullNodeShape(rootRef, originalNode, tallChildRef, tallChild, tallLowRef, tallLowLeaf, insertedKey,
        route);
  }

  private static WriterFixture writerFixture(final PageReference rootReference, final int failAllocationCall,
      final RuntimeException allocationFailure) {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final PathPage pathPage = mock(PathPage.class);
    final AtomicLong pageKeys = new AtomicLong(1_000);
    final AtomicInteger allocationCalls = new AtomicInteger();
    final Map<PageReference, PageContainer> logged = new IdentityHashMap<>();

    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(2);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getPathPage(revisionRootPage)).thenReturn(pathPage);
    doReturn(pathPage).when(storageEngineWriter).prepareSecondaryIndexPage(IndexType.PATH);
    when(pathPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(invocation -> {
      if (allocationCalls.getAndIncrement() == failAllocationCall) {
        throw allocationFailure;
      }
      return pageKeys.getAndIncrement();
    });
    when(log.get(any(PageReference.class))).thenAnswer(invocation -> logged.get(invocation.getArgument(0)));
    when(storageEngineWriter.loadHOTPage(any(PageReference.class))).thenAnswer(invocation -> {
      final PageReference reference = invocation.getArgument(0);
      final PageContainer container = logged.get(reference);
      if (container != null) {
        return container.getModified() != null
            ? container.getModified()
            : container.getComplete();
      }
      return reference.getPage();
    });
    org.mockito.Mockito.doAnswer(invocation -> {
      final PageReference reference = invocation.getArgument(0);
      logged.put(reference, invocation.getArgument(1));
      reference.setPage(null);
      return null;
    }).when(log).put(any(PageReference.class), any(PageContainer.class));

    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    writer.installRoot(rootReference);
    return new WriterFixture(storageEngineWriter, writer, logged);
  }

  private static int assertExactStoredHeights(final Page page, final WriterFixture fixture) {
    if (page instanceof HOTLeafPage) {
      return 0;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int maxChildHeight = 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      maxChildHeight =
          Math.max(maxChildHeight, assertExactStoredHeights(fixture.resolve(indirect.getChildReference(i)), fixture));
    }
    final int exactHeight = maxChildHeight + 1;
    assertEquals(exactHeight, indirect.getHeight(), "stale height at page " + indirect.getPageKey());
    return exactHeight;
  }

  private static int countEntries(final Page page, final WriterFixture fixture) {
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getEntryCount();
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int entries = 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      entries += countEntries(fixture.resolve(indirect.getChildReference(i)), fixture);
    }
    return entries;
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[] key, final byte[] value) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PATH);
    assertTrue(leaf.put(key, value));
    return leaf;
  }

  private static PageReference durableReference(final Page page, final long durableKey) {
    final PageReference reference = new PageReference();
    reference.setKey(durableKey);
    reference.setPage(page);
    return reference;
  }

  private static byte[] key(final int value) {
    return new byte[] {(byte) value};
  }

  private static byte[] value(final int value) {
    return new byte[] {(byte) value};
  }

  private static void closeReachable(final PageReference reference, final WriterFixture fixture,
      final Map<Page, Boolean> visited) {
    final Page page = fixture.resolve(reference);
    if (page == null || visited.put(page, Boolean.TRUE) != null) {
      return;
    }
    if (page instanceof HOTLeafPage leaf) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      closeReachable(indirect.getChildReference(i), fixture, visited);
    }
  }

  private record FullNodeShape(PageReference rootRef, HOTIndirectPage originalNode, PageReference tallChildRef,
      HOTIndirectPage tallChild, PageReference tallLowRef, HOTLeafPage tallLowLeaf, byte[] insertedKey,
      AbstractHOTIndexWriter.LeafNavigationResult route) {
  }

  private record WriterFixture(StorageEngineWriter storageEngineWriter, TestIndexWriter writer,
      Map<PageReference, PageContainer> logged) {
    private Page resolve(final PageReference reference) {
      final PageContainer container = logged.get(reference);
      if (container != null) {
        return container.getModified() != null
            ? container.getModified()
            : container.getComplete();
      }
      return reference.getPage();
    }
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

    private boolean directionOneForTest(final AbstractHOTIndexWriter.LeafNavigationResult route,
        final HOTIndirectPage originalNode, final HOTIncrementalInsert.BiNode split, final HOTIndirectPage half,
        final byte[] key, final byte[] value) {
      try {
        final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("directionOneIntoSplitHalf",
            AbstractHOTIndexWriter.LeafNavigationResult.class, HOTIndirectPage.class, int.class,
            HOTIncrementalInsert.BiNode.class, HOTIndirectPage.class, boolean.class, int.class, byte[].class,
            byte[].class, int.class);
        method.setAccessible(true);
        return (boolean) method.invoke(this, route, originalNode, 0, split, half, false, 10, key, value, 2);
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
      // The focused fixture installs its materialized root directly.
    }
  }
}

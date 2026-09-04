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
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A missed propagation preflight must fail closed without rewriting raw projection slots. */
final class HOTProjectionPropagationFallbackTest {

  @Test
  void postPublicationOrderViolationPoisonsWithoutASecondMutationAlgorithm() {
    final byte[] tombstone = new byte[0];
    final byte[] rawDescriptor = new byte[] {0, (byte) 0xFF, 0x11, 0x22};
    final HOTLeafPage physicallyFirst = leaf(10, key(0x20), rawDescriptor);
    final HOTLeafPage rebuiltSlot = leaf(11, key(0x10), tombstone);
    final PageReference rebuiltSlotRef = durableReference(rebuiltSlot, 111);
    final PageReference rootRef = durableReference(HOTIndirectPage.createSpanNode(12, 1, 0, 1L << (63 - 2),
        new int[] {0, 1}, new PageReference[] {durableReference(physicallyFirst, 110), rebuiltSlotRef}, 1), 112);
    final WriterFixture fixture = writerFixture(rootRef);
    final long failuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();

    try {
      final HOTIndirectPage root = (HOTIndirectPage) fixture.resolve(rootRef);
      final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(
          rebuiltSlot, rebuiltSlotRef, new HOTIndirectPage[] {root}, new PageReference[] {rootRef}, new int[] {1}, 1);

      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.propagateForTest(route, key(0x10)));

      assertTrue(failure.getMessage().contains("crossed a sibling boundary"), failure.getMessage());
      assertEquals(failuresBefore + 1, AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get());
      final Map<Integer, byte[]> values = new LinkedHashMap<>();
      collect(fixture.resolve(rootRef), fixture, values);
      assertEquals(2, values.size(), "the already-published trigger key must not be appended a second time");
      assertArrayEquals(tombstone, values.get(0x10), "the zero-length projection tombstone must remain exact");
      assertArrayEquals(rawDescriptor, values.get(0x20), "raw projection bytes must never enter bitmap OR merging");
      assertTrue(!HOTMalformedSubtreeDetector.detect(rootRef, fixture::resolve).isEmpty(),
          "the validator must not hide the deliberately malformed published fixture by rewriting it");
      verify(fixture.storageEngineWriter, atLeastOnce()).markTransactionRollbackOnly(same(failure));
    } finally {
      closeReachable(rootRef, fixture, new IdentityHashMap<>());
      closeIfOpen(physicallyFirst);
      closeIfOpen(rebuiltSlot);
    }
  }

  @Test
  void unresolvedSiblingDuringPostPublicationPropagationPoisonsInsteadOfAssumingLeafHeight() {
    final HOTLeafPage rebuiltSlot = leaf(20, key(0x10), new byte[] {1});
    final PageReference rebuiltSlotRef = durableReference(rebuiltSlot, 120);
    final PageReference unresolvedTallSibling = new PageReference();
    unresolvedTallSibling.setKey(121);
    final PageReference rootRef = durableReference(HOTIndirectPage.createSpanNode(22, 1, 0, 1L << (63 - 2),
        new int[] {0, 1}, new PageReference[] {rebuiltSlotRef, unresolvedTallSibling}, 2), 122);
    final WriterFixture fixture = writerFixture(rootRef);
    final HOTIndirectPage root = (HOTIndirectPage) fixture.resolve(rootRef);
    final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(
        rebuiltSlot, rebuiltSlotRef, new HOTIndirectPage[] {root}, new PageReference[] {rootRef}, new int[] {0}, 1);

    try {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.propagateForTest(route, key(0x10)));

      assertTrue(failure.getMessage().contains("cannot resolve child 1"), failure.getMessage());
      assertSame(root, rootRef.getPage(), "no stale-low ancestor may be published on an unresolved height");
      verify(fixture.storageEngineWriter, atLeastOnce()).markTransactionRollbackOnly(same(failure));
    } finally {
      closeIfOpen(rebuiltSlot);
    }
  }

  private static void collect(final Page page, final WriterFixture fixture, final Map<Integer, byte[]> values) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        final byte[] key = leaf.getKey(i);
        assertEquals(1, key.length);
        final long valueRef = leaf.valueRef(i);
        final int valueLength = HOTLeafPage.refLength(valueRef);
        assertTrue(valueLength >= 0);
        final byte[] value = new byte[valueLength];
        if (valueLength > 0) {
          leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
        }
        final byte[] previous = values.put(Byte.toUnsignedInt(key[0]), value);
        assertTrue(previous == null, "projection slot key must have exactly one physical owner");
      }
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      collect(fixture.resolve(indirect.getChildReference(i)), fixture, values);
    }
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[] key, final byte[] value) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PROJECTION);
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

  private static WriterFixture writerFixture(final PageReference rootReference) {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final ProjectionIndexPage projectionIndexPage = mock(ProjectionIndexPage.class);
    final AtomicLong pageKeys = new AtomicLong(1_000);
    final Map<PageReference, PageContainer> logged = new IdentityHashMap<>();

    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(2);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getProjectionIndexPage(revisionRootPage)).thenReturn(projectionIndexPage);
    when(storageEngineWriter.<ProjectionIndexPage>prepareSecondaryIndexPage(IndexType.PROJECTION)).thenReturn(
        projectionIndexPage);
    when(projectionIndexPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(invocation -> pageKeys.getAndIncrement());
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
      logged.put(invocation.getArgument(0), invocation.getArgument(1));
      return null;
    }).when(log).put(any(PageReference.class), any(PageContainer.class));

    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    writer.installRoot(rootReference);
    return new WriterFixture(storageEngineWriter, writer, logged);
  }

  private static void closeReachable(final PageReference reference, final WriterFixture fixture,
      final Map<Page, Boolean> visited) {
    final Page page = fixture.resolve(reference);
    if (page == null || visited.put(page, Boolean.TRUE) != null) {
      return;
    }
    if (page instanceof HOTLeafPage leaf) {
      closeIfOpen(leaf);
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      closeReachable(indirect.getChildReference(i), fixture, visited);
    }
  }

  private static void closeIfOpen(final HOTLeafPage leaf) {
    if (!leaf.isClosed()) {
      leaf.close();
    }
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
      super(storageEngineWriter, IndexType.PROJECTION, 0);
    }

    private void installRoot(final PageReference root) {
      rootReference = root;
    }

    private void propagateForTest(final AbstractHOTIndexWriter.LeafNavigationResult route, final byte[] triggerKey) {
      try {
        final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("propagateStructuralSpliceUpSpine",
            AbstractHOTIndexWriter.LeafNavigationResult.class, int.class, byte[].class);
        method.setAccessible(true);
        method.invoke(this, route, 1, triggerKey);
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

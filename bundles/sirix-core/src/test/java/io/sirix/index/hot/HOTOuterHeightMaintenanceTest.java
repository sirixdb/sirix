package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exact maxima after a local replacement: growth, shrinkage, ties and failure boundaries. */
final class HOTOuterHeightMaintenanceTest {
  @ParameterizedTest
  @CsvSource({"0,2,0", "2,0,0", "2,1,2", "1,2,3", "3,1,2", "1,1,3", "1,0,3"})
  void propagatesOnlyTheChangedMaximum(final int oldHeight, final int newHeight, final int siblingHeight) {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.CAS)) {
      final Fixture fixture = new Fixture();
      final PageReference replacement = ref(chain(newHeight, leaf));
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(20, 1, 1, replacement,
          ref(chain(siblingHeight, leaf)), Math.max(oldHeight, siblingHeight) + 1);
      final HOTIndirectPage root =
          HOTIndirectPage.createBiNode(21, 1, 0, ref(parent), ref(leaf), parent.getHeight() + 1);
      final AbstractHOTIndexWriter.LeafNavigationResult route =
          new AbstractHOTIndexWriter.LeafNavigationResult(leaf, replacement, new HOTIndirectPage[] {root, parent},
              new PageReference[] {ref(root), ref(parent)}, new int[] {0, 0}, 2);

      fixture.writer.refresh(route, replacement, oldHeight);

      assertEquals(structuralHeight(parent), parent.getHeight());
      assertEquals(structuralHeight(root), root.getHeight());
    }
  }

  @Test
  void unresolvedReplacementPoisonsThePublishedMutation() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.CAS)) {
      final Fixture fixture = new Fixture();
      final PageReference missing = new PageReference();
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(2, 1, 0, missing, ref(leaf), 2);
      final AbstractHOTIndexWriter.LeafNavigationResult route = route(parent, missing, leaf);
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.refresh(route, missing, 1));
      verify(fixture.storage).markTransactionRollbackOnly(failure);
      assertEquals(2, parent.getHeight());
    }
  }

  @Test
  void unresolvedSiblingCannotBeAssumedToHaveLeafHeightOnShrink() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.CAS)) {
      final Fixture fixture = new Fixture();
      final PageReference replacement = ref(leaf);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(2, 1, 0, replacement, new PageReference(), 3);
      final AbstractHOTIndexWriter.LeafNavigationResult route = route(parent, replacement, leaf);
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.refresh(route, replacement, 2));
      verify(fixture.storage).markTransactionRollbackOnly(failure);
      assertEquals(3, parent.getHeight());
    }
  }

  private static AbstractHOTIndexWriter.LeafNavigationResult route(final HOTIndirectPage parent,
      final PageReference replacement, final HOTLeafPage leaf) {
    return new AbstractHOTIndexWriter.LeafNavigationResult(leaf, replacement, new HOTIndirectPage[] {parent},
        new PageReference[] {ref(parent)}, new int[] {0}, 1);
  }

  private static Page chain(final int height, final HOTLeafPage leaf) {
    Page result = leaf;
    for (int level = 1; level <= height; level++) {
      result = HOTIndirectPage.createBiNode(level + 1, 1, 8 - level, ref(result), ref(leaf), level);
    }
    return result;
  }

  private static int structuralHeight(final Page page) {
    if (page instanceof HOTLeafPage) {
      return 0;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int height = 0;
    for (int slot = 0; slot < indirect.getNumChildren(); slot++) {
      height = Math.max(height, structuralHeight(indirect.getChildReference(slot).getPage()) + 1);
    }
    return height;
  }

  private static PageReference ref(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static final class Fixture {
    private final StorageEngineWriter storage = mock(StorageEngineWriter.class);
    private final TestWriter writer;

    private Fixture() {
      when(storage.getLog()).thenReturn(mock(TransactionIntentLog.class));
      when(storage.loadHOTPage(any(PageReference.class))).thenAnswer(call -> {
        final PageReference reference = call.getArgument(0);
        return reference.getPage();
      });
      writer = new TestWriter(storage);
    }
  }

  private static final class TestWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestWriter(final StorageEngineWriter storage) {
      super(storage, IndexType.CAS, 0);
    }

    private void refresh(final LeafNavigationResult route, final PageReference replacement, final int oldHeight) {
      try {
        final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("refreshOuterHeight",
            LeafNavigationResult.class, int.class, PageReference.class, int.class);
        method.setAccessible(true);
        method.invoke(this, route, route.pathDepth(), replacement, oldHeight);
      } catch (final InvocationTargetException failure) {
        if (failure.getCause() instanceof RuntimeException runtime) {
          throw runtime;
        }
        throw new AssertionError(failure.getCause());
      } catch (final ReflectiveOperationException failure) {
        throw new AssertionError(failure);
      }
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] buffer) {
      keyBuffer = buffer;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
      System.arraycopy(key, 0, buffer, offset, key.length);
      return key.length;
    }

    @Override
    protected void prepareIndexPage() {
      // The focused fixture owns the complete published spine directly.
    }
  }
}

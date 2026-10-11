package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class HOTInvariantValidatorWorkTest {
  private final List<Page> pages = new ArrayList<>();

  @AfterEach
  void closePages() {
    for (final Page page : pages) {
      page.close();
    }
  }

  @Test
  void resolvesEachPageOnceWhileCheckingEveryStoredKey() {
    final List<PageReference> references = new ArrayList<>();
    final PageReference left = branch(references, 0);
    final PageReference right = branch(references, 0x80);
    final PageReference root = reference(references, HOTIndirectPage.createBiNode(100, 0, 0, left, right, 2));
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    for (final Page page : pages) {
      if (page instanceof HOTLeafPage leaf) {
        clearInvocations(leaf);
      }
    }

    final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, reader);
    result.assertOk();
    assertEquals(128, result.storedKeyCount());
    assertEquals(2, result.observedHeight());
    for (final PageReference reference : references) {
      verify(reader, times(1)).loadHOTPage(same(reference));
    }
    for (final Page page : pages) {
      if (page instanceof HOTLeafPage leaf) {
        for (int i = 0; i < leaf.getEntryCount(); i++) {
          verify(leaf, times(1)).getKey(i);
        }
      }
    }
  }

  @Test
  void nextPassResolvesTheCurrentTilPagesAndStillDetectsMisrouting() {
    final List<PageReference> references = new ArrayList<>();
    final PageReference left = leaf(references, 0, 0);
    final PageReference right = leaf(references, 0x80, 0);
    final PageReference root = reference(references, HOTIndirectPage.createBiNode(100, 0, 0, left, right, 1));
    final PageReference replacement = leaf(references, 0, 0x80);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final AtomicReference<Page> resolved = new AtomicReference<>(right.getPage());
    when(reader.loadHOTPage(same(right))).thenAnswer(invocation -> resolved.get());

    HOTInvariantValidator.validate(root, reader).assertOk();
    resolved.set(replacement.getPage());
    final HOTInvariantValidator.Result changed = HOTInvariantValidator.validate(root, reader);
    assertTrue(changed.violations().stream().anyMatch(v -> v.invariant().equals("I6-pext-routes-to-leaf")));
    assertEquals(64, changed.storedKeyCount());
    verify(reader, times(2)).loadHOTPage(same(right));
    verify((HOTLeafPage) left.getPage(), times(2)).getAllKeys();
  }

  @Test
  void duplicateStoredKeysStillReportCrossLeafUniqueness() {
    final List<PageReference> references = new ArrayList<>();
    final PageReference left = leaf(references, 0, 0);
    final PageReference right = leaf(references, 0, 0);
    final PageReference root = reference(references, HOTIndirectPage.createBiNode(100, 0, 0, left, right, 1));

    final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, mock(StorageEngineReader.class));
    assertEquals(64, result.storedKeyCount());
    assertEquals(32,
        result.violations().stream().filter(v -> v.invariant().equals("I1-cross-leaf-uniqueness")).count());
  }

  @Test
  void storedBytesDoNotHideABrokenLeafLookup() {
    final List<PageReference> references = new ArrayList<>();
    final PageReference left = leaf(references, 0, 0);
    final PageReference right = leaf(references, 0x80, 0);
    final PageReference root = reference(references, HOTIndirectPage.createBiNode(100, 0, 0, left, right, 1));
    final HOTLeafPage brokenLookup = (HOTLeafPage) left.getPage();
    doReturn(-1).when(brokenLookup).findEntry(any(byte[].class));

    final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, mock(StorageEngineReader.class));
    assertEquals(64, result.storedKeyCount());
    assertEquals(32, result.violations().size());
    assertTrue(result.violations().stream().allMatch(v -> v.invariant().equals("I6-pext-routes-to-leaf")));
  }

  @Test
  void unreadableSlotStillReturnsAStructuredViolation() {
    final HOTLeafPage leaf = new HOTLeafPage(1, 0, IndexType.NAME);
    assertTrue(leaf.put(new byte[] {1}, new byte[] {2}));
    // Corrupt the persisted suffix length so both key and value bounds are unreadable.
    leaf.getSlot(0).set(ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), 0, (short) 0xFFFF);
    final PageReference root = reference(new ArrayList<>(), leaf);

    final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, mock(StorageEngineReader.class));
    assertEquals(0, result.storedKeyCount());
    assertEquals(1, result.violations().size());
    assertEquals("I1-leaf-key-uniqueness", result.violations().getFirst().invariant());
    assertTrue(result.violations().getFirst().message().contains("unreadable value"));
  }

  private PageReference branch(final List<PageReference> references, final int firstByte) {
    final PageReference low = leaf(references, firstByte, 0);
    final PageReference high = leaf(references, firstByte, 0x80);
    return reference(references, HOTIndirectPage.createBiNode(50 + firstByte, 0, 8, low, high, 1));
  }

  private PageReference leaf(final List<PageReference> references, final int firstByte, final int secondByte) {
    final HOTLeafPage leaf = spy(new HOTLeafPage(references.size(), 0, IndexType.NAME));
    for (int i = 0; i < 32; i++) {
      assertTrue(leaf.put(new byte[] {(byte) firstByte, (byte) (secondByte + i)}, new byte[] {(byte) 0xAA}));
    }
    return reference(references, leaf);
  }

  private PageReference reference(final List<PageReference> references, final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    references.add(reference);
    pages.add(page);
    return reference;
  }
}

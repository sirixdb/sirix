package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Ownership conservation and fail-closed decoding of folded posting side pages. */
final class ReferencedPostingTest {
  @Test
  void markerRoundTripAndMissingOrMismatchedPayloadsFailClosed() {
    final long key = HOTLeafPage.overflowPageRefKey(123, PostingDeltas.REFERENCE_SUB_ID);
    final byte[] payload = payload(31);
    final byte[] marker = NodeReferencesSerializer.encodeReferenced(key, payload.length);
    assertEquals(13, marker.length);
    assertEquals(key, NodeReferencesSerializer.referencedKey(marker, 0));
    assertEquals(payload.length, NodeReferencesSerializer.referencedPayloadLength(marker, 0));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.encodeReferenced(key, 0));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.encodeReferenced(key, 65536));
    assertThrows(IllegalStateException.class, () -> NodeReferencesSerializer.deserializeChunk(marker));
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.CAS)) {
      assertTrue(leaf.put(key(4), marker));
      assertThrows(IllegalStateException.class, () -> NodeReferencesSerializer.resolveReferencedPayload(leaf, key,
          payload.length, ReferencedPostingTest::read));
      final PageReference reference = reference(payload);
      leaf.setPageReference(key, reference);
      assertArrayEquals(payload,
          NodeReferencesSerializer.resolveReferencedPayload(leaf, key, payload.length, ReferencedPostingTest::read));
      assertThrows(IllegalStateException.class, () -> NodeReferencesSerializer.resolveReferencedPayload(leaf, key,
          payload.length + 1, ReferencedPostingTest::read));
      assertThrows(IllegalStateException.class,
          () -> NodeReferencesSerializer.resolveReferencedPayload(leaf, key, payload.length, ignored -> null));
      assertThrows(IllegalStateException.class,
          () -> leaf.mergeWithNodeRefs(key(4), key(4).length, payload, payload.length));
    }
  }

  @Test
  void projectionBytesAreNeverTreatedAsPostingOwnership() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      assertTrue(leaf.put(key(4), NodeReferencesSerializer.encodeReferenced(17, 10)));
      assertEquals(-1, leaf.findReferencedPostingOwner(17));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void leafSplitVariantsMoveEachReferenceExactlyOnce(final boolean insert) {
    try (HOTLeafPage left = new HOTLeafPage(1, 1, IndexType.CAS);
        HOTLeafPage right = new HOTLeafPage(2, 1, IndexType.CAS)) {
      final List<PageReference> references = seed(left, 24);
      if (insert) {
        final byte[] incoming = key(25);
        final byte[] value = payload(99);
        assertTrue(left.splitToWithInsert(right, incoming, incoming.length, value, value.length));
      } else {
        assertNotNull(left.splitTo(right));
      }
      assertTrue(left.segmentRefCount() > 0);
      assertTrue(right.segmentRefCount() > 0);
      assertOwners(List.of(left, right), references);
    }
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"CAS", "VALIDTIME"})
  void incrementalSplitCarriesPostingReferences(final IndexType type) {
    final List<HOTLeafPage> halves = new ArrayList<>();
    try (HOTLeafPage source = new HOTLeafPage(1, 1, type)) {
      final List<PageReference> references = seed(source, 24);
      final AtomicLong allocator = new AtomicLong(2);
      final HOTIncrementalInsert.BiNode split =
          HOTIncrementalInsert.splitLeafPage(source, key(99), payload(99), 2, type, allocator::getAndIncrement);
      collect(split.left().getPage(), halves);
      collect(split.right().getPage(), halves);
      assertOwners(halves, references);
      assertEquals(references.size(), source.segmentRefCount(), "unpublished source ownership is unchanged");
    } finally {
      halves.forEach(HOTLeafPage::close);
    }
  }

  @Test
  void frontierReattachmentRenamesCollidingHashesWithoutDroppingPayloads() {
    final TestWriter writer = new TestWriter();
    final long collision = HOTLeafPage.overflowPageRefKey(8, PostingDeltas.REFERENCE_SUB_ID);
    try (HOTLeafPage source1 = new HOTLeafPage(1, 1, IndexType.CAS);
        HOTLeafPage source2 = new HOTLeafPage(2, 1, IndexType.CAS);
        HOTLeafPage rebuilt = new HOTLeafPage(3, 2, IndexType.CAS)) {
      final byte[] first = payload(11);
      final byte[] second = payload(22);
      assertTrue(source1.put(key(1), NodeReferencesSerializer.encodeReferenced(collision, first.length)));
      assertTrue(source2.put(key(2), NodeReferencesSerializer.encodeReferenced(collision, second.length)));
      final PageReference firstRef = reference(first);
      final PageReference secondRef = reference(second);
      source1.setPageReference(collision, firstRef);
      source2.setPageReference(collision, secondRef);
      final List<HOTBulkBuilder.Entry> entries = new ArrayList<>();
      final List<Object> references = new ArrayList<>();
      invoke(writer, "collectLeafEntries", new Class<?>[] {HOTLeafPage.class, List.class, List.class}, source1, entries,
          references);
      invoke(writer, "collectLeafEntries", new Class<?>[] {HOTLeafPage.class, List.class, List.class}, source2, entries,
          references);
      for (final HOTBulkBuilder.Entry entry : entries) {
        assertTrue(rebuilt.put(entry.key(), entry.value()));
      }
      invoke(writer, "reattachSegmentRefs", new Class<?>[] {Page.class, List.class}, rebuilt, references);
      assertEquals(2, rebuilt.segmentRefCount());
      for (int slot = 0; slot < 2; slot++) {
        final long value = rebuilt.valueRef(slot);
        final long refKey = NodeReferencesSerializer.referencedKey(rebuilt, value);
        assertSame(slot == 0
            ? firstRef
            : secondRef, rebuilt.getPageReference(refKey));
        assertArrayEquals(slot == 0
            ? first
            : second,
            NodeReferencesSerializer.resolveReferencedPayload(rebuilt, refKey,
                NodeReferencesSerializer.referencedPayloadLength(rebuilt, value), ReferencedPostingTest::read));
      }
      assertSame(firstRef, source1.getPageReference(collision));
      assertSame(secondRef, source2.getPageReference(collision));
    }
  }

  @Test
  void boundarySplitCarriesMarkersAndRejectsLostOwnersBeforePublication() {
    final TestWriter writer = new TestWriter();
    try (HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.CAS);
        HOTLeafPage left = new HOTLeafPage(2, 2, IndexType.CAS);
        HOTLeafPage right = new HOTLeafPage(3, 2, IndexType.CAS)) {
      final List<PageReference> refs = seed(source, 2);
      assertTrue(left.put(source.getKey(0), source.copyStoredValue(0)));
      assertTrue(right.put(source.getKey(1), source.copyStoredValue(1)));
      final Class<?>[] types = {HOTLeafPage.class, HOTLeafPage.class, HOTLeafPage.class};
      invoke(writer, "rehomeSplitLeafSideReferences", types, source, left, right);
      assertOwners(List.of(left, right), refs);
      assertThrows(IllegalStateException.class,
          () -> invoke(writer, "rehomeSplitLeafSideReferences", types, source, left, null));
    }
  }

  private static List<PageReference> seed(final HOTLeafPage leaf, final int size) {
    final List<PageReference> references = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      final byte[] key = key(i);
      final byte[] payload = payload(i);
      final long refKey = PostingDeltas.referenceKey(key, key.length);
      assertTrue(leaf.put(key, NodeReferencesSerializer.encodeReferenced(refKey, payload.length)));
      final PageReference reference = reference(payload);
      leaf.setPageReference(refKey, reference);
      references.add(reference);
    }
    return references;
  }

  private static void assertOwners(final List<HOTLeafPage> leaves, final List<PageReference> references) {
    assertEquals(references.size(), leaves.stream().mapToInt(HOTLeafPage::segmentRefCount).sum());
    for (int i = 0; i < references.size(); i++) {
      final byte[] key = key(i);
      final long refKey = PostingDeltas.referenceKey(key, key.length);
      int owners = 0;
      for (final HOTLeafPage leaf : leaves) {
        final int slot = leaf.findEntry(key);
        if (slot >= 0) {
          owners++;
          assertEquals(slot, leaf.findReferencedPostingOwner(refKey));
          assertSame(references.get(i), leaf.getPageReference(refKey));
        } else {
          assertNull(leaf.getPageReference(refKey));
        }
      }
      assertEquals(1, owners);
    }
  }

  private static byte[] payload(final long bit) {
    final NodeReferences references = new NodeReferences();
    references.addNodeKey(bit);
    return NodeReferencesSerializer.serialize(references);
  }

  private static byte[] key(final int ordinal) {
    final byte[] key = new byte[21];
    key[0] = 1;
    key[8] = (byte) ordinal;
    return key;
  }

  private static PageReference reference(final byte[] payload) {
    final PageReference reference = new PageReference();
    reference.setPage(new OverflowPage(payload));
    return reference;
  }

  private static OverflowPage read(final PageReference reference) {
    return (OverflowPage) reference.getPage();
  }

  private static void collect(final Page page, final List<HOTLeafPage> leaves) {
    if (page instanceof HOTLeafPage leaf) {
      leaves.add(leaf);
    } else {
      final HOTIndirectPage indirect = (HOTIndirectPage) page;
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collect(indirect.getChildReference(i).getPage(), leaves);
      }
    }
  }

  private static void invoke(final TestWriter writer, final String methodName, final Class<?>[] types,
      final Object... args) {
    try {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod(methodName, types);
      method.setAccessible(true);
      method.invoke(writer, args);
    } catch (final InvocationTargetException failure) {
      if (failure.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw new AssertionError(failure.getCause());
    } catch (final ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }

  private static final class TestWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] buffer = new byte[32];

    private TestWriter() {
      super(mock(StorageEngineWriter.class), IndexType.CAS, 0);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return buffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] replacement) {
      buffer = replacement;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] target, final int offset) {
      System.arraycopy(key, 0, target, offset, key.length);
      return key.length;
    }
  }
}

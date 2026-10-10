/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.BufferManager;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A replacing frontier split must carry the side-map references of the entry it drops onto the
 * key's fresh leaf.
 *
 * <p>
 * A projection slot whose blob was spilled to a segment page owns a side reference on its leaf.
 * Replacing that blob with an inline value writes the new value first and releases the page
 * afterwards, so when the larger value overflows the leaf the slot still owns the page. The
 * overflow is discharged through the complete frontier only where the integrate cascade is refused,
 * which this constructed trie reaches at a full parent under a full grandparent whose split breaks
 * the trie condition: here the grandparent's upper half is a node that straddles bit 44 next to a
 * leaf that differs from it below that bit, so the recompressed half would discriminate on the
 * node's own most significant bit. The frontier then splits the overflowing leaf immediately before
 * the key, drops the key's stale entry from it, and used to throw {@code lost side-reference owner}
 * because the owner was in neither half; the transaction was poisoned and the load stopped.
 * </p>
 */
final class HOTFrontierSideReferenceCarryTest {

  /** Slot keys are {@code (rowGroup << 16) | kind}, as the projection store builds them. */
  private static long slot(final int rowGroup, final int kind) {
    return ((long) rowGroup << 16) | kind;
  }

  @Test
  void droppedOwnersSideReferenceFollowsTheKeyToItsFreshLeaf() {
    try (final Fixture fixture = new Fixture()) {
      // The overflowing leaf, the parent's last child: the key with a side reference plus seven
      // fillers that leave no room for a larger value. All eight share the parent's partial 31 (bit
      // 44 set, low kind bits set) and differ in the kind's upper bits only, so they sort above every
      // sibling and their split bit is fresh to every ancestor.
      final long key = slot(40, 15);
      final PageReference segmentPage = new PageReference();
      segmentPage.setPage(new OverflowPage(new byte[] {1, 2, 3}));
      final long refKey = HOTLeafPage.overflowPageRefKey(key, 0);
      final HOTLeafPage overflowing = fixture.emptyLeaf();
      assertTrue(overflowing.put(fixture.keyBytes(key), new byte[17]));
      for (int j = 1; j < 8; j++) {
        assertTrue(overflowing.put(fixture.keyBytes(slot(40, 15 | (j << 4))), new byte[8_400]));
      }
      overflowing.setPageReference(refKey, segmentPage);
      final PageReference overflowingRef = fixture.register(overflowing);

      // The full parent: 32 leaves over bit 44 and the four low kind bits, row groups 32 and 40.
      final PageReference[] parentChildren = new PageReference[32];
      final int[] parentPartials = new int[32];
      for (int i = 0; i < 32; i++) {
        final int rowGroup = i < 16
            ? 32
            : 40;
        final int kind = i & 15;
        parentChildren[i] = i == 31
            ? overflowingRef
            : fixture.leaf(slot(rowGroup, kind));
        parentPartials[i] = i;
      }
      final PageReference parent = fixture.node(new int[] {44, 60, 61, 62, 63}, parentPartials, parentChildren);

      // The full grandparent: thirty single-key leaves for row groups 0..29, the parent at partial
      // 32, and a leaf for row group 44 whose partial differs from the parent's at bits 44 and 45
      // only. Splitting it at bit 42 leaves an upper half discriminating on bit 44, the parent's own
      // most significant bit, which the integrate pre-check refuses.
      final PageReference[] grandChildren = new PageReference[32];
      final int[] grandPartials = new int[32];
      for (int g = 0; g < 30; g++) {
        grandChildren[g] = fixture.leaf(slot(g, 0));
        grandPartials[g] = g;
      }
      grandChildren[30] = parent;
      grandPartials[30] = 32;
      grandChildren[31] = fixture.leaf(slot(44, 0));
      grandPartials[31] = 44;
      fixture.install(fixture.node(new int[] {42, 43, 44, 45, 46, 47}, grandPartials, grandChildren));
      HOTInvariantValidator.validate(fixture.root(), fixture.storage).assertOk();

      final long routed = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get();
      final long carried = AbstractHOTIndexWriter.FRONTIER_SPLIT_CARRIED_OWNER_SIDE_REFERENCES.get();
      final byte[] replacement = new byte[7_000];
      Arrays.fill(replacement, (byte) 0x5a);
      assertDoesNotThrow(() -> fixture.writer.put(key, replacement),
          () -> "handler=" + fixture.writer.lastDispatchHandler);

      HOTInvariantValidator.validate(fixture.root(), fixture.storage).assertOk();
      final HOTLeafPage home = fixture.leafOf(key);
      assertArrayEquals(replacement, home.copyStoredValue(home.findEntry(fixture.keyBytes(key))),
          "the key must read its replacement value from the leaf it routes to");
      assertSame(segmentPage, home.getPageReference(refKey),
          "the side reference must follow its owning slot onto the key's fresh leaf");
      assertEquals(1, home.segmentRefCount());
      assertEquals(routed + 1, AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get(),
          "the overflow must have been refused by the integrate pre-check and routed to the frontier");
      assertEquals(carried + 1, AbstractHOTIndexWriter.FRONTIER_SPLIT_CARRIED_OWNER_SIDE_REFERENCES.get(),
          "the frontier split must have carried the dropped owner's reference");
    }
  }

  /** A projection-typed writer over an intent log, with the trie installed directly as its root. */
  private static final class Fixture implements AutoCloseable {
    private final TransactionIntentLog log =
        new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
    private final AtomicLong pageKeys = new AtomicLong(100);
    private final StorageEngineWriter storage = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    private final TestWriter writer;
    private @Nullable PageReference root;

    private Fixture() {
      final RevisionRootPage revisionRoot = mock(RevisionRootPage.class);
      final ProjectionIndexPage projectionPage = mock(ProjectionIndexPage.class);
      when(storage.getLog()).thenReturn(log);
      when(storage.getRevisionNumber()).thenReturn(2);
      when(storage.getActualRevisionRootPage()).thenReturn(revisionRoot);
      when(storage.getProjectionIndexPage(revisionRoot)).thenReturn(projectionPage);
      doReturn(projectionPage).when(storage).prepareSecondaryIndexPage(IndexType.PROJECTION);
      when(projectionPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(ignored -> pageKeys.getAndIncrement());
      when(storage.loadHOTPage(any(PageReference.class))).thenAnswer(invocation -> page(invocation.getArgument(0)));
      writer = new TestWriter(storage);
    }

    private byte[] keyBytes(final long slotKey) {
      final byte[] bytes = new byte[HOTLongKeySerializer.SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serialize(slotKey, bytes, 0);
      return bytes;
    }

    private HOTLeafPage emptyLeaf() {
      return new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PROJECTION);
    }

    private PageReference leaf(final long... slotKeys) {
      final HOTLeafPage leaf = emptyLeaf();
      for (final long slotKey : slotKeys) {
        assertTrue(leaf.put(keyBytes(slotKey), new byte[17]));
      }
      return register(leaf);
    }

    private PageReference node(final int[] bits, final int[] partials, final PageReference... children) {
      int height = 1;
      for (final PageReference child : children) {
        if (page(child) instanceof HOTIndirectPage indirect) {
          height = Math.max(height, indirect.getHeight() + 1);
        }
      }
      return register(HOTBulkBuilder.assembleIndirect(bits, partials, children, height, 1, IndexType.PROJECTION,
          pageKeys::getAndIncrement));
    }

    private PageReference register(final Page page) {
      final PageReference reference = new PageReference();
      reference.setPage(page);
      log.put(reference, PageContainer.getInstance(page, page));
      return reference;
    }

    private Page page(final PageReference reference) {
      final PageContainer container = log.get(reference);
      return container == null
          ? reference.getPage()
          : container.getModified();
    }

    private void install(final PageReference reference) {
      root = reference;
      writer.rootReference = reference;
    }

    private PageReference root() {
      return Objects.requireNonNull(root, "fixture root must be installed before use");
    }

    /** The leaf the key routes to, through the installed root. */
    private HOTLeafPage leafOf(final long slotKey) {
      final byte[] key = keyBytes(slotKey);
      Page current = page(root());
      for (int depth = 0; current instanceof HOTIndirectPage node && depth < 32; depth++) {
        current = page(node.getChildReference(node.findChildIndex(key)));
      }
      final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, current);
      assertNotNull(leaf);
      assertTrue(leaf.findEntry(key) >= 0, "key must route to its owning leaf: " + slotKey);
      return leaf;
    }

    @Override
    public void close() {
      log.close();
    }
  }

  private static final class TestWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestWriter(final StorageEngineWriter storage) {
      super(storage, IndexType.PROJECTION, 0);
    }

    private void put(final long slotKey, final byte[] value) {
      final byte[] key = new byte[HOTLongKeySerializer.SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serialize(slotKey, key, 0);
      doIndex(key, key.length, value, value.length);
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
      // The fixture installs a TIL-owned root directly.
    }
  }
}

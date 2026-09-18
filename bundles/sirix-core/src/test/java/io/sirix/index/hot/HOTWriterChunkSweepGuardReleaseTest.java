package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A writer caches its chunk reader across lookups, so a sweep that fell back to guarded reads must
 * hand its last leaf back to eviction, and leave the cached reader optimistic, once the sweep ends.
 */
final class HOTWriterChunkSweepGuardReleaseTest {

  private static final long LOGICAL_KEY = 17L;
  private static final long NODE_KEY = 42L;

  @Test
  void guardedSweepReleasesItsLastLeafWhenTheSweepEnds() {
    try (final Fixture fixture = new Fixture()) {
      final NodeReferences result = fixture.writer.get(LOGICAL_KEY, SearchMode.EQUAL);

      assertNotNull(result);
      assertArrayEquals(new long[] {NODE_KEY}, result.toSortedArray());
      assertTrue(fixture.evictions > 0, "the sweep must exercise the eviction window");
      verify(fixture.storage, times(1)).loadHOTPageAndGuard(fixture.root);
      final HOTLeafPage guarded = fixture.leaves.getLast();
      assertEquals(0, guarded.getGuardCount(), "a finished sweep must not keep its leaf guarded");
      assertTrue(guarded.isClosed(), "the evicted leaf's deferred teardown must complete once the sweep ends");
    }
  }

  @Test
  void nextSweepThroughTheCachedReaderIsOptimisticAgain() {
    try (final Fixture fixture = new Fixture()) {
      assertNotNull(fixture.writer.get(LOGICAL_KEY, SearchMode.EQUAL));
      fixture.evictDuringRead = false;
      final int leavesAfterFirstSweep = fixture.leaves.size();

      final NodeReferences result = fixture.writer.get(LOGICAL_KEY, SearchMode.EQUAL);

      assertNotNull(result);
      assertArrayEquals(new long[] {NODE_KEY}, result.toSortedArray());
      assertEquals(leavesAfterFirstSweep + 1, fixture.leaves.size());
      verify(fixture.storage, times(1)).loadHOTPageAndGuard(fixture.root);
      verify(fixture.leaves.getLast(), never()).acquireGuard();
      assertEquals(0, fixture.leaves.getLast().getGuardCount());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Arena arena = Arena.ofShared();
    private final List<HOTLeafPage> leaves = new ArrayList<>();
    private final StorageEngineWriter storage = mock(StorageEngineWriter.class);
    private final PageReference root = new PageReference().setKey(123L);
    private final HOTLongIndexWriter writer;
    private boolean evictDuringRead = true;
    private int evictions;

    private Fixture() {
      final PathPage paths = new PathPage();
      paths.setOrCreateReference(0, root);
      when(storage.<PathPage>prepareSecondaryIndexPage(IndexType.PATH)).thenReturn(paths);
      when(storage.loadHOTPage(root)).thenAnswer(_ -> newLeaf());
      when(storage.loadHOTPageAndGuard(root)).thenAnswer(_ -> {
        final HOTLeafPage leaf = newLeaf();
        assertTrue(leaf.acquireGuard());
        return leaf;
      });
      writer = HOTLongIndexWriter.create(storage, IndexType.PATH, 0);
    }

    private HOTLeafPage newLeaf() {
      final HOTLeafPage leaf = spy(new HOTLeafPage(123L, 1, IndexType.PATH,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0));
      final byte[] composite = new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serialize(LOGICAL_KEY, composite, 0);
      HOTKeySerializer.writeChunkIdxBE(composite, HOTLongKeySerializer.SERIALIZED_SIZE, 0);
      final NodeReferences refs = new NodeReferences();
      refs.getNodeKeys().add(NODE_KEY);
      assertTrue(leaf.put(composite, NodeReferencesSerializer.serialize(refs)));
      doAnswer(invocation -> {
        final Object value = invocation.callRealMethod();
        if (evictDuringRead) {
          evictions++;
          // close() is guard-aware: retirement must defer freeing an actively guarded leaf.
          leaf.close();
        }
        return value;
      }).when(leaf).copyStoredValue(anyInt());
      leaves.add(leaf);
      return leaf;
    }

    @Override
    public void close() {
      for (final HOTLeafPage leaf : leaves) {
        leaf.close();
      }
      arena.close();
    }
  }
}

package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Deterministic eviction at the boundary between seeking a chunk and reading its payload. */
final class HOTReaderEvictionProgressTest {

  private static final long LOGICAL_KEY = 17L;
  private static final long NODE_KEY = 42L;

  @Test
  void pointReadCompletesWhenEveryChunkReadRacesEviction() {
    try (final Fixture fixture = new Fixture(true)) {
      final NodeReferences result = fixture.reader.get(LOGICAL_KEY, SearchMode.EQUAL);

      assertNotNull(result);
      assertArrayEquals(new long[] {NODE_KEY}, result.toSortedArray());
      assertTrue(fixture.evictions > 0, "the lookup must exercise the eviction window");
      for (final HOTLeafPage leaf : fixture.leaves) {
        assertEquals(0, leaf.getGuardCount(), "a completed lookup must release every guard");
      }
    }
  }

  @Test
  void uncontendedPointReadRemainsOptimistic() {
    try (final Fixture fixture = new Fixture(false)) {
      final NodeReferences result = fixture.reader.get(LOGICAL_KEY, SearchMode.EQUAL);

      assertNotNull(result);
      assertArrayEquals(new long[] {NODE_KEY}, result.toSortedArray());
      assertEquals(1, fixture.leaves.size());
      verify(fixture.leaves.getFirst(), never()).acquireGuard();
    }
  }

  @Test
  void pooledReaderResetsGuardedRecoveryAfterTheWalk() {
    try (final Fixture fixture = new Fixture(true)) {
      assertNotNull(fixture.reader.get(LOGICAL_KEY, SearchMode.EQUAL));
      fixture.evictDuringRead = false;

      final NodeReferences result = fixture.reader.get(LOGICAL_KEY, SearchMode.EQUAL);

      assertNotNull(result);
      assertArrayEquals(new long[] {NODE_KEY}, result.toSortedArray());
      verify(fixture.leaves.getLast(), never()).acquireGuard();
    }
  }

  @Test
  void genuineReadFailureReleasesTheRecoveryGuard() {
    try (final Fixture fixture = new Fixture(true)) {
      final IllegalStateException failure = new IllegalStateException("stable payload failure");
      fixture.guardedReadFailure = failure;

      assertSame(failure,
          assertThrows(IllegalStateException.class, () -> fixture.reader.get(LOGICAL_KEY, SearchMode.EQUAL)));

      assertTrue(fixture.evictions > 0);
      for (final HOTLeafPage leaf : fixture.leaves) {
        assertEquals(0, leaf.getGuardCount(), "exceptional completion must release every guard");
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Arena arena = Arena.ofShared();
    private final List<HOTLeafPage> leaves = new ArrayList<>();
    private boolean evictDuringRead;
    private IllegalStateException guardedReadFailure;
    private final HOTLongIndexReader reader;
    private int evictions;

    private Fixture(final boolean evictDuringRead) {
      this.evictDuringRead = evictDuringRead;
      final StorageEngineReader storage = mock(StorageEngineReader.class);
      final RevisionRootPage revisionRoot = new RevisionRootPage();
      final PathPage paths = new PathPage();
      final PageReference root = new PageReference().setKey(123L);
      paths.setOrCreateReference(0, root);
      when(storage.getActualRevisionRootPage()).thenReturn(revisionRoot);
      when(storage.getPathPage(revisionRoot)).thenReturn(paths);
      when(storage.loadHOTPage(root)).thenAnswer(_ -> newLeaf());
      when(storage.loadHOTPageAndGuard(root)).thenAnswer(_ -> {
        final HOTLeafPage leaf = newLeaf();
        assertTrue(leaf.acquireGuard());
        return leaf;
      });
      reader = HOTLongIndexReader.create(storage, IndexType.PATH, 0);
    }

    private HOTLeafPage newLeaf() {
      final HOTLeafPage leaf = spy(new HOTLeafPage(123L, 1, IndexType.PATH, arena.allocate(HOTLeafPage.DEFAULT_SIZE),
          null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0));
      final byte[] composite = new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE];
      PathKeySerializer.INSTANCE.serialize(LOGICAL_KEY, composite, 0);
      HOTKeySerializer.writeChunkIdxBE(composite, HOTLongKeySerializer.SERIALIZED_SIZE, 0);
      final NodeReferences refs = new NodeReferences();
      refs.getNodeKeys().add(NODE_KEY);
      assertTrue(leaf.put(composite, NodeReferencesSerializer.serialize(refs)));
      doAnswer(invocation -> {
        if (guardedReadFailure != null && leaf.getGuardCount() > 0) {
          throw guardedReadFailure;
        }
        final int comparison = (int) invocation.callRealMethod();
        if (evictDuringRead) {
          evictions++;
          // close() is guard-aware: retirement must defer freeing an actively pinned leaf.
          leaf.close();
        }
        return comparison;
      }).when(leaf).compareKeyPrefix(anyInt(), any(byte[].class), anyInt());
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

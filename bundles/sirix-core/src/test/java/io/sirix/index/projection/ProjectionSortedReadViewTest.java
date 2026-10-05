/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.HOTReadIntent;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.cache.ProjectionBlobCache.Key;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

final class ProjectionSortedReadViewTest {
  private static ProjectionSortedReadView view(final boolean columnar) {
    final byte[][] keys = new byte[128][];
    final byte[][] values = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = columnar
          ? SortedScanFixtures.key(new SortedScanFixtures.Row("group", i, i + 1))
          : new byte[] {1, (byte) i};
      values[i] = columnar
          ? new byte[8]
          : new byte[] {1, 0, 0, 0};
    }
    final ProjectionSortedLeaf encoded = ProjectionSortedLeaf.encode(keys, values, keys.length);
    assertNotNull(encoded);
    final byte[] source = encoded.encodedBytes();
    final ProjectionSortedReadView view = ProjectionSortedReadView.open(source, !columnar,
        mock(StorageEngineReader.class), 0, HOTReadIntent.SELECTIVE, null);
    Arrays.fill(source, (byte) 0);
    assertArrayEquals(keys[0], view.leaf().copyKey(0));
    assertEquals(columnar, view.leaf().dataBlock() != null, "exercise both encoded schemes");
    return view;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retainedDescriptorsOwnEveryArrayBoundaryAndWritersReceivePrivateCopies(final boolean columnar) {
    final ProjectionSortedReadView view = view(columnar);
    final byte[] original = view.copyBytes();
    Arrays.fill(view.copyBytes(), (byte) 0);
    Arrays.fill(view.leaf().encodedBytes(), (byte) 0);
    Arrays.fill(view.leaf().copyKey(0), (byte) 0);
    Arrays.fill(view.leaf().copyPayload(0), (byte) 0);
    if (columnar) {
      Arrays.fill(view.leaf().dataBlock().bytes(), (byte) 0);
    } else {
      Arrays.fill(view.directory().encodedBytes(), (byte) 0);
      Arrays.fill(view.directory().copyKey(0), (byte) 0);
      assertEquals(1, view.directory().intPayloadAt(0));
    }
    assertArrayEquals(original, view.copyBytes());
    final ProjectionSortedLeaf writer = ProjectionSortedLeaf.open(view.copyBytes());
    Arrays.fill(writer.encodedBytes(), (byte) 0);
    assertArrayEquals(original, view.copyBytes());
  }

  @Test
  void cacheAccountsViewsAndFencesDeferredPublicationAcrossReuse() {
    final ProjectionSortedReadView view = view(false);
    final ProjectionBlobCache cache = new ProjectionBlobCache(64 * 1024);
    final Key key = new Key(1, 2, 3, 4, 5, 6);
    final long empty = cache.getCurrentWeightBytes();
    assertTrue(cache.put(key, view.copyBytes(), cache.generation()));
    assertTrue(cache.putView(key, view, cache.generation()));
    assertSame(view, cache.getView(key), "a hit reuses the already validated descriptor");
    assertTrue(cache.getCurrentWeightBytes() >= empty + view.retainedSizeBytes());
    Arrays.fill(cache.get(key), (byte) 0);
    assertArrayEquals(view.copyBytes(), cache.get(key));
    final ReadScope stale = new ReadScope();
    stale.retainView(cache, key, view, cache.generation(), false);
    final long old = cache.generation();
    cache.invalidate(reference -> reference.getResourceId() == 2);
    stale.finish(true);
    assertNull(cache.getView(key));
    assertFalse(cache.putView(key, view, old));
    for (int i = 0; i < 1000; i++) {
      assertTrue(cache.putView(new Key(1, 2, 3, 4, 5, i), view, cache.generation()));
      assertTrue(cache.getCurrentWeightBytes() <= cache.getMaxWeightBytes());
    }
    cache.clear();
    assertEquals(empty, cache.getCurrentWeightBytes());
  }

  @Test
  void indirectDirectoryOwnsAndAccountsReferencedSeparatorArrays() {
    final byte[][] payloads = new byte[2][8];
    for (int i = 0; i < 2; i++) {
      ProjectionIndexRowGroupCodec.putIntLEAt(payloads[i], 0, i + 1);
      ProjectionIndexRowGroupCodec.putIntLEAt(payloads[i], 4, i + 11);
    }
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(new byte[][] {{0}, {1}}, payloads, 2);
    assertNotNull(leaf);
    final byte[] first = new byte[1024];
    final byte[] second = new byte[1024];
    Arrays.fill(first, (byte) 1);
    Arrays.fill(second, (byte) 2);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final ProjectionSortedReadView view;
    try (MockedStatic<ProjectionIndexHOTStorage> reads = mockStatic(ProjectionIndexHOTStorage.class)) {
      reads.when(() -> ProjectionIndexHOTStorage.readBlob(reader, 0,
          ProjectionSortedDirectoryNode.SEPARATOR_SLOT_BASE + 11, HOTReadIntent.SELECTIVE, null)).thenReturn(first);
      reads.when(() -> ProjectionIndexHOTStorage.readBlob(reader, 0,
          ProjectionSortedDirectoryNode.SEPARATOR_SLOT_BASE + 12, HOTReadIntent.SELECTIVE, null)).thenReturn(second);
      view = ProjectionSortedReadView.open(leaf.encodedBytes(), true, reader, 0, HOTReadIntent.SELECTIVE, null);
    }
    Arrays.fill(first, (byte) 7);
    Arrays.fill(second, (byte) 8);
    assertEquals(1, view.directory().copyKey(0)[0]);
    assertEquals(2, view.directory().copyKey(1)[0]);
    Arrays.fill(view.directory().copyKey(0), (byte) 9);
    assertEquals(1, view.directory().copyKey(0)[0]);
    assertTrue(view.retainedSizeBytes() >= view.encodedSize() + first.length + second.length);
    final ProjectionBlobCache small = new ProjectionBlobCache(1024);
    assertTrue(small.canCache(view.encodedSize()), "encoded child ids alone fit");
    assertFalse(small.canCache(view), "referenced separator ownership counts against the same byte ceiling");
  }

  @Test
  void firstUseValidatesLeafAndDirectoryBeforePublication() {
    final byte[] corrupt = view(false).copyBytes();
    corrupt[0] = 0;
    assertThrows(IllegalArgumentException.class, () -> ProjectionSortedReadView.open(corrupt, false,
        mock(StorageEngineReader.class), 0, HOTReadIntent.POINT, null));
    final ProjectionSortedLeaf invalidChildren =
        ProjectionSortedLeaf.encode(new byte[][] {{1}}, new byte[][] {{0, 0, 0, 0}}, 1);
    assertNotNull(invalidChildren);
    assertThrows(IllegalStateException.class, () -> ProjectionSortedReadView.open(invalidChildren.encodedBytes(), true,
        mock(StorageEngineReader.class), 0, HOTReadIntent.SELECTIVE, null));
  }
}

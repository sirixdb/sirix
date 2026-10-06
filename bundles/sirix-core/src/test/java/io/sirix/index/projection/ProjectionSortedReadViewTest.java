/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.HOTReadIntent;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.cache.ProjectionBlobCache.Key;
import io.sirix.index.IndexType;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.HOTProjectionImage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.io.IOException;
import java.nio.ByteBuffer;

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
import static org.mockito.Mockito.when;

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
  void directoryRoutingIsChargedAndSharesTheExistingAdmissionAndGenerationFence() throws IOException {
    final ResourceConfiguration configuration = ResourceConfiguration.newBuilder("routing-view").build();
    final HOTProjectionImage image;
    final PageReference reference = new PageReference().setKey(7);
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut out = new MemorySegmentBytesOut(16 * 1024)) {
      assertTrue(leaf.put(new byte[] {1}, new byte[8192]));
      leaf.setCompleteDump(true);
      new PagePersister().serializePage(configuration, out, leaf, SerializationType.DATA);
      image = HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(out.toByteArray()), reference, configuration);
    }
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    when(reader.getDatabaseId()).thenReturn(1L);
    when(reader.getResourceId()).thenReturn(2L);
    when(reader.getRevisionNumber()).thenReturn(3);
    final ProjectionBlobCache cache = new ProjectionBlobCache(64 * 1024);
    final ReadScope source = new ReadScope();
    source.rememberImage(reader, reference, image, cache.generation());
    final ProjectionSortedReadView view =
        ProjectionSortedReadView.open(view(false).copyBytes(), true, reader, 0, HOTReadIntent.SELECTIVE, source);
    final Key key = new Key(1, 2, 7, 0, 3, 10);
    final long empty = cache.getCurrentWeightBytes();
    source.retainView(cache, key, view, cache.generation(), false);
    assertEquals(empty, cache.getCurrentWeightBytes(), "ambiguous metadata cannot admit its attached image");
    source.finish(true);
    assertSame(view, cache.getView(key));
    assertTrue(view.retainedSizeBytes() >= view.encodedSize() + image.retainedSizeBytes());
    assertTrue(cache.getCurrentWeightBytes() >= empty + view.retainedSizeBytes());
    final ReadScope reuse = new ReadScope();
    cache.getView(key).restoreRouting(reader, reuse);
    assertSame(image, reuse.image(reader, reference, cache.generation()));
    cache.clear();
    assertNull(reuse.image(reader, reference, cache.generation()), "evicted routing cannot survive offset reuse");
    reuse.finish(false);
    assertEquals(0, reuse.retainedImageBytes());
    final ProjectionBlobCache small = new ProjectionBlobCache(8192);
    assertTrue(small.canCache(view.encodedSize()));
    assertFalse(small.canCache(view), "the whole routing image counts, not only directory bytes");
    assertFalse(small.putView(key, view, small.generation()));
    assertTrue(small.getCurrentWeightBytes() <= small.getMaxWeightBytes());

    final ReadScope rawHeader = new ReadScope();
    rawHeader.rememberImage(reader, reference, image, cache.generation());
    final byte[] header = {1, 2, 3};
    rawHeader.retainBlob(cache, key, header, cache.generation(), false);
    assertNull(cache.get(key));
    rawHeader.finish(true);
    assertTrue(cache.getCurrentWeightBytes() >= empty + image.retainedSizeBytes() + header.length);
    final ReadScope rawHit = new ReadScope();
    assertArrayEquals(header, cache.get(key, reader, rawHit));
    assertSame(image, rawHit.image(reader, reference, cache.generation()));
    Arrays.fill(cache.get(key, reader, rawHit), (byte) 0);
    assertArrayEquals(header, cache.get(key));
    cache.clear();
    assertNull(rawHit.image(reader, reference, cache.generation()));
    final ReadScope abandoned = new ReadScope();
    abandoned.rememberImage(reader, reference, image, cache.generation());
    abandoned.retainBlob(cache, key, header, cache.generation(), false);
    abandoned.finish(false);
    assertEquals(empty, cache.getCurrentWeightBytes());
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

package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.ShardedPageCache;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.HOTMiniPageCache.ReadScope.ImageSnapshot;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.HOTProjectionRead;
import io.sirix.page.HOTProjectionImage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Raw images retain all committed slots, but borrow neither buffers nor mutable search metadata.
 */
final class HOTCompactFragmentReadTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-fragment")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .build();

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 17, 32, 33, 256, 512})
  void packedImageOutlivesPooledInputAndCopiesToOrdinaryWritableCapacity(final int count) throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(count)) {
      wire = serialize(original);
    }
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(wire));
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    try (FileChannelReader reader = reader(contents, pipeline);
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      assertTrue(compact.isCompactImage(), "the raw image must not acquire a writable allocator frame");
      assertTrue(compact.slots().isNative(), "a compact image owns GC-managed native memory, never a heap segment");
      assertEquals(compact.getUsedSlotsSize(), compact.slots().byteSize());
      assertEquals(count, compact.size());
      assertTrue(compact.isCompleteDump());
      assertEquals(9, compact.getRevision());
      assertEquals(777, compact.getPageReference(42).getKey());
      final byte[] replacement;
      try (HOTLeafPage other = leaf(1)) {
        replacement = frame(serialize(other));
      }
      contents.set(replacement);
      for (int reuse = 0; reuse < 12; reuse++) {
        reader.readHOTLeafFragment(reference(), CONFIG).close();
      }
      Arrays.fill(wire, (byte) 0xA5);
      for (int row = 0; row < count; row++) {
        assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
      }
      assertTrue(compact.findEntry(new byte[] {0}) < 0);
      assertTrue(compact.findEntry(key(999)) < 0);
      try (HOTLeafPage copy = compact.copyForRevision(10)) {
        assertEquals(HOTLeafPage.DEFAULT_SIZE, copy.slots().byteSize());
        assertNotSame(compact.getPageReference(42), copy.getPageReference(42));
        if (count < HOTLeafPage.MAX_ENTRIES) {
          assertTrue(copy.put(key(999), new byte[] {88}));
          assertTrue(compact.findEntry(key(999)) < 0);
        }
      }
      verify(pipeline, never()).decompressScoped(any(MemorySegment.class));
    }
  }

  @Test
  void aCachedCompactImageIsChargedItsPackedBytesPlusItsFixedPerPageHeap() throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(64)) {
      wire = serialize(original);
    }
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(wire)), new ByteHandlerPipeline());
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      assertTrue(compact.isCompactImage(), "this charge is only interesting for a compact image");
      final long packed = compact.slots().byteSize();
      final long perPageHeap = compact.estimatedCanonicalCacheRetainedHeapBytes();
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1L << 20);
      final PageReference key = new PageReference().setKey(123).setDatabaseId(0).setResourceId(0);
      cache.put(key, compact);
      assertEquals(packed + perPageHeap, cache.getCurrentWeightBytes(),
          "a heap-resident image must be charged its packed bytes and its fixed per-page heap");
      assertTrue(cache.getCurrentWeightBytes() - packed >= 4L * 1024L,
          "the page object, slot directory, dirty bitmap and side map must be charged too, not only the "
              + "packed bytes: " + (cache.getCurrentWeightBytes() - packed));
      cache.remove(key);
      assertEquals(0L, cache.getCurrentWeightBytes());
    }
  }

  @Test
  void outerTransformationReleasesItsOwnerAndReturnedSlotsStayIndependent() throws IOException {
    final MemorySegment decoded;
    try (HOTLeafPage original = leaf(17)) {
      decoded = MemorySegment.ofArray(serialize(original));
    }
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(decoded, decoded, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(new byte[] {1, 2, 3})), handler);
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      verify(release).run();
      decoded.fill((byte) 0);
      for (int row = 0; row < 17; row++) {
        assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
      }
    }
  }

  @Test
  void checksumIsVerifiedBeforeRawFragmentDecode() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(2)) {
      body = serialize(original);
    }
    final ResourceConfiguration checked =
        ResourceConfiguration.newBuilder("checked-compact").verifyChecksumsOnRead(true).build();
    final PageReference reference = reference();
    reference.setHash(PageHasher.computeLong(body, checked.hashAlgorithm));
    body[body.length - 1] ^= 1;
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(body)), new ByteHandlerPipeline())) {
      assertThrows(SirixCorruptionException.class, () -> reader.readHOTLeafFragment(reference, checked));
    }
  }

  @Test
  void corruptTrailerFailsAndReturnsDecompressionOwner() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(2)) {
      body = serialize(original);
    }
    final MemorySegment decoded = MemorySegment.ofArray(Arrays.copyOf(body, body.length - 1));
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(decoded, decoded, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(new byte[] {1})), handler)) {
      assertThrows(IndexOutOfBoundsException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG));
      verify(release).run();
    }
  }

  @Test
  void chainExtentReadsMatchPerCallReadsAndKeepEveryTruncationFailure() throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(33)) {
      wire = serialize(original);
    }
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(wire));
    final FileChannel channel = channel(contents);
    try (FileChannelReader reader = reader(channel, new ByteHandlerPipeline())) {
      final long extent = reader.committedDataExtent();
      assertEquals(contents.get().length, extent);
      verify(channel, times(1)).size();
      try (HOTLeafPage compact =
          assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG, extent))) {
        assertEquals(33, compact.size());
        assertTrue(compact.isCompleteDump());
        for (int row = 0; row < 33; row++) {
          assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
        }
      }
      verify(channel, times(1)).size(); // the bounded read never asked the file for its size
      reader.readHOTLeafFragment(reference(), CONFIG).close();
      verify(channel, times(2)).size(); // the two-argument form still asks once per call
      reader.readHOTLeafFragment(reference(), CONFIG, -1L).close();
      verify(channel, times(3)).size(); // a negative bound reads exactly like the two-argument form
      // A bound below the declared length fails closed before any body allocation.
      final SirixIOException bounded =
          assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG, 8L));
      assertTrue(bounded.getMessage().contains("out of bounds"), bounded.getMessage());
      // Truncation after the bound was taken is still detected by the real short read.
      contents.set(Arrays.copyOf(contents.get(), contents.get().length / 2));
      final SirixIOException truncated =
          assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG, extent));
      assertTrue(truncated.getMessage().contains("Truncated"), truncated.getMessage());
      assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG));
    }
  }

  @Test
  void compactImageRejectsMutationAndCopiesRestoreWritableCapacity() throws Exception {
    final byte[] wire;
    try (HOTLeafPage original = leaf(17)) {
      wire = serialize(original);
    }
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(wire)), new ByteHandlerPipeline());
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      assertEquals(17, offsetDirectoryLength(compact));
      try (HOTLeafPage copy = compact.copy()) {
        assertEquals(HOTLeafPage.MAX_ENTRIES, offsetDirectoryLength(copy));
        for (int row = 17; row < HOTLeafPage.MAX_ENTRIES; row++) {
          assertTrue(copy.put(key(row), value(row)));
        }
        assertEquals(HOTLeafPage.MAX_ENTRIES, copy.size());
        assertFalse(copy.put(key(HOTLeafPage.MAX_ENTRIES), value(1)));
      }
      assertEquals(17, offsetDirectoryLength(compact));
      final Class<? extends Throwable> rejection = HOTLeafPage.class.desiredAssertionStatus()
          ? AssertionError.class
          : IllegalStateException.class;
      assertTrue(compact.acquireGuard());
      try {
        assertThrows(rejection, () -> compact.put(key(18), value(18)));
        assertThrows(rejection, () -> compact.updateValue(1, new byte[] {99}));
        assertThrows(rejection, () -> compact.updateValueRange(1, new byte[] {99}, 0, 1));
        assertThrows(rejection, () -> compact.deleteAt(1));
        assertThrows(rejection, compact::compact);
        try (HOTLeafPage target = compact.copyForRead()) {
          assertThrows(rejection, () -> compact.splitTo(target));
          assertThrows(rejection, () -> target.splitTo(compact));
          assertThrows(rejection,
              () -> compact.splitToWithInsert(target, key(18), key(18).length, value(18), value(18).length));
          assertThrows(rejection,
              () -> target.splitToWithInsert(compact, key(18), key(18).length, value(18), value(18).length));
          assertArrayEquals(wire, serialize(target));
        }
        assertEquals(1, compact.getGuardCount());
        assertEquals(17, compact.size());
        assertEquals(17, offsetDirectoryLength(compact));
        assertTrue(compact.isCompactImage());
        assertArrayEquals(wire, serialize(compact));
        try (HOTLeafPage copy = compact.copyForRead()) {
          for (int row = 60; row >= 17; row--) {
            assertTrue(copy.put(key(row), value(row)));
          }
          assertTrue(copy.updateValue(1, new byte[] {99}));
          assertTrue(copy.slots().isNative());
          assertEquals(61, copy.size());
          for (int row = 0; row <= 60; row++) {
            assertArrayEquals(row == 1
                ? new byte[] {99}
                : value(row), copy.copyStoredValue(copy.findEntry(key(row))));
          }
        }
      } finally {
        compact.releaseGuard();
      }
      assertArrayEquals(wire, serialize(compact));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void selectiveCompleteImageOwnsOnlyBoundedHeapAndPreservesAllValues(final VersioningType versioning)
      throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(512)) {
      wire = serialize(original);
    }
    final ResourceConfiguration configuration = ResourceConfiguration.newBuilder("selective-complete")
                                                                     .versioningApproach(versioning)
                                                                     .verifyChecksumsOnRead(true)
                                                                     .byteHandlerPipeline(new ByteHandlerPipeline())
                                                                     .build();
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(wire));
    final PageReference reference = reference();
    reference.setHash(PageHasher.computeLong(wire, configuration.hashAlgorithm));
    try (FileChannelReader reader = reader(contents, new ByteHandlerPipeline())) {
      final HOTProjectionRead result = reader.readHOTProjectionEntry(reference, configuration, key(250), 42, true);
      assertNotNull(result);
      assertNull(result.indirect());
      assertNotNull(result.image());
      assertArrayEquals(value(250), Objects.requireNonNull(result.entry()).value());
      assertEquals(777, Objects.requireNonNull(result.entry().sideReference()).getKey());
      assertEquals(configuration.getID(), result.entry().sideReference().getResourceId());
      assertTrue(result.image().retainedSizeBytes() <= HOTProjectionImage.MAX_BYTES + 48);
      final ReadScope scope = new ReadScope();
      scope.rememberImage(reader, reference, result.image(), 1);
      assertSame(result.image(), scope.image(reader, reference, 1));
      assertNull(scope.image(new Object(), reference, 1));
      assertNull(scope.image(reader, reference, 2), "truncation generation fences the entire image");
      final PageReference changedHash = reference();
      changedHash.setHash(99);
      assertNull(scope.image(reader, changedHash, 1));
      assertTrue(scope.retainedImageBytes() > 0);
      contents.set(frame(new byte[] {0}));
      Arrays.fill(wire, (byte) 0);
      for (int row = 0; row < 512; row++) {
        assertArrayEquals(value(row), Objects.requireNonNull(result.image().select(key(row), 42)).value());
      }
      assertNull(result.image().select(new byte[] {0}, 42));
      assertNull(result.image().select(key(999), 42));
      assertNull(Objects.requireNonNull(result.image().select(key(10), 43)).sideReference());
      scope.finish(false);
      assertEquals(0, scope.retainedImageBytes());
      assertNull(scope.image(reader, reference, 1));
    }
  }

  @Test
  void retainedRoutingUsesImmutableIdentityAcrossReadersAndRejectsEveryStaleFence() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(64)) {
      body = serialize(original);
    }
    final PageReference reference = reference();
    reference.setHash(PageHasher.computeLong(body, CONFIG.hashAlgorithm));
    final HOTProjectionImage image = HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(body), reference, CONFIG);
    final StorageEngineReader first = mock(StorageEngineReader.class);
    when(first.getDatabaseId()).thenReturn(7L);
    when(first.getResourceId()).thenReturn(11L);
    when(first.getRevisionNumber()).thenReturn(13);
    final ReadScope initial = new ReadScope();
    initial.rememberImage(first, reference, image, 17);
    final ImageSnapshot snapshot = initial.snapshotImage();
    assertNotNull(snapshot);
    assertTrue(snapshot.retainedSizeBytes() >= image.retainedSizeBytes() + 64);
    initial.finish(false);
    final StorageEngineReader next = mock(StorageEngineReader.class);
    when(next.getDatabaseId()).thenReturn(7L);
    when(next.getResourceId()).thenReturn(11L);
    when(next.getRevisionNumber()).thenReturn(13);
    final ReadScope reused = new ReadScope();
    reused.restoreImage(next, snapshot);
    assertSame(image, reused.image(next, reference, 17));
    assertSame(image, reused.image(first, reference, 17), "a forwarding reader with the same identity can use it");
    assertNull(reused.image(next, reference, 18), "truncation fences a still reachable snapshot");
    final PageReference moved = new PageReference().setKey(1);
    moved.setHash(reference.getHashAsLong());
    assertNull(reused.image(next, moved, 17));
    assertNull(reused.image(next, reference(), 17));
    final PageReference changed = new PageReference().setKey(0);
    changed.setHash(99);
    assertNull(reused.image(next, changed, 17));
    when(next.getDatabaseId()).thenReturn(8L);
    final ReadScope wrongDatabase = new ReadScope();
    wrongDatabase.restoreImage(next, snapshot);
    assertEquals(0, wrongDatabase.retainedImageBytes());
    when(next.getDatabaseId()).thenReturn(7L);
    when(next.getResourceId()).thenReturn(12L);
    final ReadScope wrongResource = new ReadScope();
    wrongResource.restoreImage(next, snapshot);
    assertEquals(0, wrongResource.retainedImageBytes());
    when(next.getResourceId()).thenReturn(11L);
    when(next.getRevisionNumber()).thenReturn(14);
    final ReadScope wrongRevision = new ReadScope();
    wrongRevision.restoreImage(next, snapshot);
    assertEquals(0, wrongRevision.retainedImageBytes());
    reused.finish(false);
    reused.restoreImage(first, snapshot);
    assertEquals(0, reused.retainedImageBytes(), "finishing a scope releases all physical ownership");
  }

  @Test
  void oversizedCompleteImageSelectsWithoutRetainingIt() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(512)) {
      for (int i = 1000; i < 5000; i++) {
        original.setPageReference(i, new PageReference().setKey(i + 100));
      }
      body = serialize(original);
    }
    assertTrue(body.length > HOTProjectionImage.MAX_BYTES);
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(body)), new ByteHandlerPipeline())) {
      final HOTProjectionRead result = reader.readHOTProjectionEntry(reference(), CONFIG, key(250), 42, true);
      assertNotNull(result);
      assertNull(result.image(), "a large side trailer must not escape the scoped heap ceiling");
      assertArrayEquals(value(250), Objects.requireNonNull(result.entry()).value());
      assertEquals(777, Objects.requireNonNull(result.entry().sideReference()).getKey());
      final HOTProjectionRead unretained = reader.readHOTProjectionEntry(reference(), CONFIG, key(250), 42, false);
      assertNotNull(unretained);
      assertNull(unretained.image());
      assertArrayEquals(result.entry().value(), Objects.requireNonNull(unretained.entry()).value());
    }
  }

  @Test
  void unsupportedByteHandlerDeclinesBeforeReadingTheFrame() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final ByteHandler handler = mock(ByteHandler.class);
    when(handler.supportsMemorySegments()).thenReturn(false);
    try (FileChannelReader reader = reader(channel, handler)) {
      assertNull(reader.readHOTProjectionEntry(reference(), CONFIG, key(0), 42, true));
      verify(channel, never()).size();
      verify(channel, never()).read(any(ByteBuffer.class), anyLong());
    }
  }

  @Test
  void selectiveReadVerifiesTheWholeFrameAndRejectsTruncation() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(64)) {
      body = serialize(original);
    }
    final ResourceConfiguration checked =
        ResourceConfiguration.newBuilder("checked-selective").verifyChecksumsOnRead(true).build();
    final PageReference reference = reference();
    reference.setHash(PageHasher.computeLong(body, checked.hashAlgorithm));
    body[body.length - 1] ^= 1;
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(body));
    try (FileChannelReader reader = reader(contents, new ByteHandlerPipeline())) {
      assertThrows(SirixCorruptionException.class,
          () -> reader.readHOTProjectionEntry(reference, checked, key(0), 42, true));
      contents.set(Arrays.copyOf(contents.get(), Objects.requireNonNull(contents.get()).length / 2));
      assertThrows(SirixIOException.class, () -> reader.readHOTProjectionEntry(reference, checked, key(0), 42, true));
    }
  }

  @Test
  void selectiveTrailerFailureReleasesTheDecompressionOwner() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(2)) {
      body = serialize(original);
    }
    final MemorySegment decoded = MemorySegment.ofArray(Arrays.copyOf(body, body.length - 1));
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(decoded, decoded, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(new byte[] {1})), handler)) {
      assertThrows(IllegalStateException.class,
          () -> reader.readHOTProjectionEntry(reference(), CONFIG, key(1), 42, true));
      verify(release).run();
    }
  }

  private static int offsetDirectoryLength(final HOTLeafPage page) throws Exception {
    final Field directory = HOTLeafPage.class.getDeclaredField("slotOffsets");
    directory.setAccessible(true);
    return ((int[]) directory.get(page)).length;
  }

  private static HOTLeafPage leaf(final int count) {
    final HOTLeafPage page = new HOTLeafPage(123, 9, IndexType.PROJECTION);
    for (int row = 0; row < count; row++) {
      assertTrue(page.put(key(row), value(row)));
    }
    page.setPageReference(42, new PageReference().setKey(777));
    page.setCompleteDump(true);
    return page;
  }

  private static byte[] key(final int row) {
    return new byte[] {12, 13, 14, 15, (byte) (row >>> 8), (byte) row};
  }

  private static byte[] value(final int row) {
    final byte[] value = new byte[row % 31];
    Arrays.fill(value, (byte) row);
    return value;
  }

  private static byte[] serialize(final HOTLeafPage page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      return out.toByteArray();
    }
  }

  private static byte[] frame(final byte[] body) {
    return ByteBuffer.allocate(body.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(body.length).put(body).array();
  }

  private static PageReference reference() {
    return new PageReference().setKey(0);
  }

  private static FileChannelReader reader(final AtomicReference<byte[]> contents, final ByteHandler handler)
      throws IOException {
    return reader(channel(contents), handler);
  }

  private static FileChannelReader reader(final FileChannel channel, final ByteHandler handler) {
    return new FileChannelReader(channel, mock(FileChannel.class), handler, SerializationType.DATA, new PagePersister(),
        Caffeine.newBuilder().build());
  }

  private static FileChannel channel(final AtomicReference<byte[]> contents) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenAnswer(_ -> (long) contents.get().length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final int from = Math.toIntExact(invocation.<Long>getArgument(1));
      final byte[] bytes = contents.get();
      if (from >= bytes.length) {
        return -1;
      }
      final int length = Math.min(target.remaining(), bytes.length - from);
      target.put(bytes, from, length);
      return length;
    });
    return channel;
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.HOTMiniPageCache.ReadScope.ImageSnapshot;
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.io.PageHasher;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

final class HOTProjectionImageTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("projection-image").verifyChecksumsOnRead(true).build();

  @Test
  void nativeSideTrailerUsesOneLifecycleHoldPerBatchAndNoFixupList() {
    final int count = 512;
    final byte[] trailer = new byte[count * 2 * Long.BYTES];
    final ByteBuffer encoded = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      encoded.putLong(i - 256).putLong(i + 1000);
    }
    try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      final HOTLeafPage leaf = spy(original);
      leaf.readSideReferences(pairs(trailer, count), count);
      PageUtils.fixupPageReferenceIds(leaf, 17, 19);
      verify(leaf, times(2)).beginSideReferenceRead();
      verify(leaf, times(2)).endSideReferenceRead();
      verify(leaf, never()).getReferences();
      for (int i = 0; i < count; i++) {
        // The decoder installs a map sized for the trailer, so read back through the decoding leaf
        // rather than the spied original, which only aliased the pre-decode map.
        final PageReference reference = leaf.getPageReference(i - 256);
        assertNotNull(reference);
        assertEquals(i + 1000, reference.getKey());
        assertEquals(17, reference.getDatabaseId());
        assertEquals(19, reference.getResourceId());
      }
    }
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readSideReferences(pairs(trailer, count), count + 1));
      assertNull(leaf.getPageReference(0), "reject dimensions before populating the map");
      encoded.putLong(Long.BYTES, -1);
      assertThrows(IllegalStateException.class, () -> leaf.readSideReferences(pairs(trailer, count), count));
      assertNull(leaf.getPageReference(-256));
    }
  }

  /** Decode a serialized trailer into the interleaved pair array the leaf installs from. */
  private static long[] pairs(final byte[] trailer, final int count) {
    final long[] pairs = new long[2 * count];
    new ByteArrayBytesIn(trailer).readLongs(pairs, 0, pairs.length);
    return pairs;
  }

  @Test
  void ownedImageSelectsEveryValueAndSignedSideReferenceAfterSourceMutation() throws IOException {
    final byte[] wire = wire(512);
    final ByteBuffer source = ByteBuffer.allocateDirect(wire.length + 32);
    source.position(13).put(wire).limit(13 + wire.length).position(13);
    final HOTProjectionImage image = HOTProjectionImage.copyVerifiedFrame(source, reference(wire), CONFIG);
    assertEquals(13, source.position());
    assertEquals(13 + wire.length, source.limit());
    assertTrue(image.retainedSizeBytes() >= wire.length + 64);
    source.clear();
    while (source.hasRemaining()) {
      source.put((byte) 0);
    }
    Arrays.fill(wire, (byte) 0);
    for (int row = 0; row < 512; row++) {
      final HOTLeafEntry entry = image.select(key(row), row - 256);
      assertNotNull(entry);
      assertArrayEquals(value(row), entry.value());
      assertEquals(row + 1000, Objects.requireNonNull(entry.sideReference()).getKey());
      Arrays.fill(entry.value(), (byte) 0);
      entry.sideReference().setKey(0);
      assertArrayEquals(value(row), Objects.requireNonNull(image.select(key(row), row - 256)).value());
      assertEquals(row + 1000,
          Objects.requireNonNull(Objects.requireNonNull(image.select(key(row), row - 256)).sideReference()).getKey());
    }
    assertNull(image.select(new byte[] {0}, 0));
    assertNull(image.select(key(512), 0));
    assertNull(Objects.requireNonNull(image.select(key(10), Long.MIN_VALUE)).sideReference());
    assertNull(Objects.requireNonNull(image.select(key(10), Long.MAX_VALUE)).sideReference());
  }

  @Test
  void everyFrameByteIsCheckedBeforeAnySelectedResult() throws IOException {
    final byte[] wire = wire(32);
    final PageReference reference = reference(wire);
    for (int i = 0; i < wire.length; i++) {
      wire[i] ^= 1;
      assertThrows(SirixCorruptionException.class,
          () -> HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(wire), reference, CONFIG));
      wire[i] ^= 1;
    }
  }

  @Test
  void completeImageRejectsMalformedDimensionsSlotsAndUnvisitedSideReferences() throws IOException {
    final byte[] wire = wire(2);
    final ByteArrayBytesIn input = new ByteArrayBytesIn(wire);
    input.position(3); // kind and envelope
    Utils.getVarLong(input);
    input.skip(Integer.BYTES + Byte.BYTES);
    final int prefixLengthAt = (int) input.position();
    final int prefix = Short.toUnsignedInt(input.readShort());
    input.skip(prefix);
    final int countAt = (int) input.position();
    final int count = input.readInt() & Integer.MAX_VALUE;
    final int usedAt = (int) input.position();
    final int used = input.readInt();
    final int offsets = (int) input.position();
    final int slots = offsets + count * Integer.BYTES;
    input.position(slots + used);
    final int sideCountAt = (int) input.position();
    assertEquals(2, Utils.getVarLong(input));
    final int sideStart = (int) input.position();

    assertBadHeader(wire, prefixLengthAt, 0xffff, Short.BYTES);
    assertBadHeader(wire, countAt, HOTLeafPage.MAX_ENTRIES + 1, Integer.BYTES);
    assertBadHeader(wire, usedAt, -1, Integer.BYTES);
    assertBadHeader(wire, sideCountAt, 127, Byte.BYTES);
    assertBadHeader(wire, sideStart + 3 * Long.BYTES, -1, Long.BYTES);
    // The second (unrequested) reference must still be strictly greater than the first.
    assertBadHeader(wire, sideStart + 2 * Long.BYTES, -256, Long.BYTES);
    assertBadHeader(wire, sideStart + 2 * Long.BYTES, -257, Long.BYTES);
    assertThrows(IllegalStateException.class, () -> open(Arrays.copyOf(wire, wire.length - 1)));
    assertThrows(IllegalStateException.class, () -> open(Arrays.copyOf(wire, wire.length + 1)));

    final byte[] badOffset = wire.clone();
    ByteBuffer.wrap(badOffset).order(ByteOrder.LITTLE_ENDIAN).putInt(offsets, -1);
    final HOTProjectionImage offsetImage = open(badOffset);
    assertThrows(IllegalStateException.class, () -> offsetImage.select(key(0), -256));
    final byte[] badKeyLength = wire.clone();
    ByteBuffer.wrap(badKeyLength).order(ByteOrder.LITTLE_ENDIAN).putShort(slots, (short) 0xffff);
    final HOTProjectionImage keyImage = open(badKeyLength);
    assertThrows(IllegalStateException.class, () -> keyImage.select(key(0), -256));
    final byte[] badValueLength = wire.clone();
    final int suffixLength = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN).getShort(slots) & 0xffff;
    ByteBuffer.wrap(badValueLength)
              .order(ByteOrder.LITTLE_ENDIAN)
              .putShort(slots + Short.BYTES + suffixLength, (short) 0xffff);
    final HOTProjectionImage valueImage = open(badValueLength);
    assertThrows(IllegalStateException.class, () -> valueImage.select(key(0), -256));
  }

  @Test
  void fixedWordDirectoryOwnsAndAccountsEverySignedKeyAndRelocatedValueRange() throws IOException {
    final long[] keys =
        {Long.MIN_VALUE, Long.MIN_VALUE + 1, -129, -128, -1, 0, 1, 127, 128, 65_535, 1L << 46, Long.MAX_VALUE};
    final byte[] dense = wordWire(keys);
    final ByteArrayBytesIn input = new ByteArrayBytesIn(dense);
    input.position(3);
    Utils.getVarLong(input);
    input.skip(Integer.BYTES + Byte.BYTES);
    final int prefix = Short.toUnsignedInt(input.readShort());
    input.skip(prefix);
    final int count = input.readInt() & Integer.MAX_VALUE;
    final int usedAt = (int) input.position();
    final int used = input.readInt();
    final int offsets = (int) input.position();
    final int slots = offsets + count * Integer.BYTES;
    // Reverse the physical records and leave retired bytes before each one. Logical slot order
    // remains the independent signed-long key order; serialized offsets need not be monotonic.
    final byte[] relocated = new byte[dense.length + count * 3];
    System.arraycopy(dense, 0, relocated, 0, slots);
    final ByteBuffer original = ByteBuffer.wrap(dense).order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffer changed = ByteBuffer.wrap(relocated).order(ByteOrder.LITTLE_ENDIAN);
    changed.putInt(usedAt, used + count * 3);
    int at = 0;
    for (int row = count - 1; row >= 0; row--) {
      final int offset = original.getInt(offsets + row * Integer.BYTES);
      final int suffix = original.getShort(slots + offset) & 0xffff;
      final int length = original.getShort(slots + offset + Short.BYTES + suffix) & 0xffff;
      final int recordSize = 2 * Short.BYTES + suffix + length;
      at += 3;
      changed.putInt(offsets + row * Integer.BYTES, at);
      System.arraycopy(dense, slots + offset, relocated, slots + at, recordSize);
      at += recordSize;
    }
    System.arraycopy(dense, slots + used, relocated, slots + at, dense.length - slots - used);
    final HOTProjectionImage image = open(relocated);
    final long coldBytes = 72 + ((16L + relocated.length + 7) & ~7L);
    assertEquals(coldBytes, image.retainedSizeBytes(), "cold images must not expand every word slot");
    for (int row = 0; row < keys.length; row++) {
      assertArrayEquals(value(row), Objects.requireNonNull(image.select(wordKey(keys[row]), row - 6)).value());
    }
    assertEquals(coldBytes, image.retainedSizeBytes(), "uncached selection must retain no word directory");
    image.prepareForCache();
    final long preparedBytes = image.retainedSizeBytes();
    image.prepareForCache();
    assertEquals(preparedBytes, image.retainedSizeBytes(), "repeated preparation publishes no extra retained array");
    assertEquals(72 + ((16L + relocated.length + 7) & ~7L) + 16L + keys.length * 2L * Long.BYTES,
        image.retainedSizeBytes(), "charge the entire primitive directory in addition to the owned image");
    Arrays.fill(relocated, (byte) 0);
    for (int row = 0; row < keys.length; row++) {
      final HOTLeafEntry entry = image.select(wordKey(keys[row]), row - 6);
      assertNotNull(entry);
      assertArrayEquals(value(row), entry.value());
      assertEquals(row + 1000, Objects.requireNonNull(entry.sideReference()).getKey());
      Arrays.fill(entry.value(), (byte) 0);
      assertArrayEquals(value(row), Objects.requireNonNull(image.select(wordKey(keys[row]), row - 6)).value());
    }
    assertNull(image.select(wordKey(2), 0));
    assertNull(image.select(Arrays.copyOf(wordKey(0), Long.BYTES - 1), 0));
    assertNull(image.select(Arrays.copyOf(wordKey(0), Long.BYTES + 1), 0));
  }

  @Test
  void malformedWordSlotsCannotPublishUncheckedRanges() throws IOException {
    final byte[] wire = wordWire(new long[] {1, 2});
    final ByteArrayBytesIn input = new ByteArrayBytesIn(wire);
    input.position(3);
    Utils.getVarLong(input);
    input.skip(Integer.BYTES + Byte.BYTES);
    input.skip(Short.toUnsignedInt(input.readShort()));
    final int count = input.readInt() & Integer.MAX_VALUE;
    final int used = input.readInt();
    final int offsets = (int) input.position();
    final int slots = offsets + count * Integer.BYTES;
    final ByteBuffer changed = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN);
    final int offset = changed.getInt(offsets);
    final int suffix = changed.getShort(slots + offset) & 0xffff;
    changed.putShort(slots + offset + Short.BYTES + suffix, (short) (used + 1));
    final HOTProjectionImage image = open(wire);
    image.prepareForCache();
    assertThrows(IllegalStateException.class, () -> image.select(wordKey(1), -6));
    assertEquals(72 + ((16L + wire.length + 7) & ~7L), image.retainedSizeBytes(),
        "a malformed word slot must decline the prevalidated directory");
  }

  @Test
  void routingPublicationChargesPreparedDirectoryAndRejectsAnOversizedCombinedEntry() throws IOException {
    final byte[] wire = wordWire(new long[] {1, 2, 3, 4, 5, 6});
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    when(reader.getRevisionNumber()).thenReturn(1);
    for (final int budget : new int[] {4096, 1024}) {
      final ProjectionBlobCache cache = new ProjectionBlobCache(budget);
      final long empty = cache.getCurrentWeightBytes();
      final HOTProjectionImage image = open(wire);
      final long cold = image.retainedSizeBytes();
      final ReadScope scope = new ReadScope();
      final PageReference reference = reference(wire);
      scope.rememberImage(reader, reference, image, cache.generation());
      final ImageSnapshot routing = scope.snapshotImage();
      assertNotNull(routing);
      final ProjectionBlobCache.Key key =
          new ProjectionBlobCache.Key(0, 0, reference.getKey(), reference.getHashAsLong(), 1, 0);
      final boolean admitted = cache.put(key, new byte[] {7}, routing, cache.generation());
      assertEquals(cold + 16 + 6 * 2 * Long.BYTES, image.retainedSizeBytes(),
          "preparation must finish before the combined admission weight is computed");
      if (budget == 4096) {
        assertTrue(admitted);
        assertEquals(empty + 192 + 8 + routing.retainedSizeBytes(), cache.getCurrentWeightBytes());
        assertArrayEquals(new byte[] {7}, cache.get(key));
      } else {
        assertFalse(admitted, "the root fits before preparation but exceeds this set's byte budget afterward");
        assertEquals(empty, cache.getCurrentWeightBytes());
        assertNull(cache.get(key));
      }
      scope.finish(false);
    }
  }

  private static byte[] wordWire(final long[] keys) throws IOException {
    try (HOTLeafPage leaf = new HOTLeafPage(123, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      for (int row = 0; row < keys.length; row++) {
        assertTrue(leaf.put(wordKey(keys[row]), value(row)));
        leaf.setPageReference(row - 6, new PageReference().setKey(row + 1000));
      }
      leaf.setCompleteDump(true);
      new PagePersister().serializePage(CONFIG, out, leaf, SerializationType.DATA);
      return out.toByteArray();
    }
  }

  private static byte[] wordKey(final long value) {
    final byte[] key = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(value, key, 0);
    return key;
  }

  private static void assertBadHeader(final byte[] wire, final int at, final long value, final int width) {
    final byte[] corrupt = wire.clone();
    final ByteBuffer bytes = ByteBuffer.wrap(corrupt).order(ByteOrder.LITTLE_ENDIAN);
    switch (width) {
      case Byte.BYTES -> bytes.put(at, (byte) value);
      case Short.BYTES -> bytes.putShort(at, (short) value);
      case Integer.BYTES -> bytes.putInt(at, (int) value);
      case Long.BYTES -> bytes.putLong(at, value);
      default -> throw new AssertionError("unsupported width");
    }
    assertThrows(IllegalStateException.class, () -> open(corrupt));
  }

  private static HOTProjectionImage open(final byte[] wire) {
    return HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(wire), reference(wire), CONFIG);
  }

  private static PageReference reference(final byte[] wire) {
    final PageReference reference = new PageReference().setKey(0);
    reference.setHash(PageHasher.computeLong(wire, CONFIG.hashAlgorithm));
    return reference;
  }

  private static byte[] wire(final int rows) throws IOException {
    try (HOTLeafPage leaf = new HOTLeafPage(123, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      for (int row = 0; row < rows; row++) {
        assertTrue(leaf.put(key(row), value(row)));
        leaf.setPageReference(row - 256, new PageReference().setKey(row + 1000));
      }
      leaf.setCompleteDump(true);
      new PagePersister().serializePage(CONFIG, out, leaf, SerializationType.DATA);
      return out.toByteArray();
    }
  }

  private static byte[] key(final int row) {
    return new byte[] {11, 12, (byte) (row >>> 8), (byte) row};
  }

  private static byte[] value(final int row) {
    return new byte[] {(byte) row, (byte) (row >>> 8)};
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.IndexType;
import io.sirix.io.PageHasher;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
      leaf.readSideReferences(new ByteArrayBytesIn(trailer), count);
      PageUtils.fixupPageReferenceIds(leaf, 17, 19);
      verify(leaf, times(2)).beginSideReferenceRead();
      verify(leaf, times(2)).endSideReferenceRead();
      verify(leaf, never()).getReferences();
      for (int i = 0; i < count; i++) {
        final PageReference reference = original.getPageReference(i - 256);
        assertNotNull(reference);
        assertEquals(i + 1000, reference.getKey());
        assertEquals(17, reference.getDatabaseId());
        assertEquals(19, reference.getResourceId());
      }
    }
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> leaf.readSideReferences(new ByteArrayBytesIn(trailer), count + 1));
      assertNull(leaf.getPageReference(0), "reject dimensions before populating the map");
      encoded.putLong(Long.BYTES, -1);
      assertThrows(IllegalStateException.class, () -> leaf.readSideReferences(new ByteArrayBytesIn(trailer), count));
      assertNull(leaf.getPageReference(-256));
    }
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
      assertEquals(row + 1000, entry.sideReference().getKey());
      Arrays.fill(entry.value(), (byte) 0);
      entry.sideReference().setKey(0);
      assertArrayEquals(value(row), image.select(key(row), row - 256).value());
      assertEquals(row + 1000, image.select(key(row), row - 256).sideReference().getKey());
    }
    assertNull(image.select(new byte[] {0}, 0));
    assertNull(image.select(key(512), 0));
    assertNull(image.select(key(10), Long.MIN_VALUE).sideReference());
    assertNull(image.select(key(10), Long.MAX_VALUE).sideReference());
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

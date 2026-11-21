package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTPostingFrame;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.hot.page.HOTLeafBody;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import io.sirix.settings.VersioningType;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class HOTLeafBodyEncodingTest {
  private static final ResourceConfiguration FULL =
      new ResourceConfiguration.Builder("hot-body").versioningApproach(VersioningType.FULL).build();
  private static final ResourceConfiguration SLIDING =
      new ResourceConfiguration.Builder("hot-fragments").versioningApproach(VersioningType.SLIDING_SNAPSHOT).build();

  private static final byte[] NARROW_LENGTHS =
      HexFormat.of().parseHex("0c000c0307030000000a00000200000008000000000004000107012a01090133");
  private static final byte[] SHARED =
      HexFormat.of().parseHex("0c000c0707030000000a0000020000001000000000000b000201070801020304050607080109080000");

  @BeforeAll
  static void allocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @Test
  void goldenWidthsAndAliasDecodeOnAllProjectionReadPaths() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      assertTrue(leaf.put(new byte[] {7}, new byte[] {42}));
      assertTrue(leaf.put(new byte[] {9}, new byte[] {51}));
      assertArrayEquals(NARROW_LENGTHS, write(FULL, leaf, false));
      assertProjection(NARROW_LENGTHS, new byte[] {51});
    }
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      final byte[] shared = {1, 2, 3, 4, 5, 6, 7, 8};
      assertTrue(leaf.put(new byte[] {7}, shared));
      assertTrue(leaf.put(new byte[] {9}, shared));
      assertArrayEquals(SHARED, write(FULL, leaf, false));
      assertProjection(SHARED, shared);
    }
  }

  @Test
  void borrowedSelectionCanSwitchBetweenHeapAndNativeFrames() {
    final byte[] key = {9};
    final ByteArrayBytesIn heap = new ByteArrayBytesIn(NARROW_LENGTHS);
    final byte[] nativeBytes = NARROW_LENGTHS.clone();
    nativeBytes[nativeBytes.length - 1] = 99;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegmentBytesIn input =
          new MemorySegmentBytesIn(arena.allocateFrom(ValueLayout.JAVA_BYTE, nativeBytes));
      for (int repetition = 0; repetition < 3; repetition++) {
        heap.position(1);
        assertArrayEquals(new byte[] {51}, PageKind.deserializeHOTProjectionEntry(heap, key, 0).value());
        input.position(1);
        assertArrayEquals(new byte[] {99}, PageKind.deserializeHOTProjectionEntry(input, key, 0).value());
      }
    }
  }

  @Test
  void oneByteBorrowedProbePreservesUnsignedAndPrefixOrder() {
    final byte[] frame =
        {45, 46, 0, 0, 3, 0, 7, 0, 11, 0, 0, 1, 20, 1, 127, 1, 21, 1, (byte) 128, 1, 22, 2, (byte) 129, 1, 1, 23, 47};
    try (Arena arena = Arena.ofConfined()) {
      final BytesIn<?>[] inputs =
          {new ByteArrayBytesIn(frame), new MemorySegmentBytesIn(arena.allocateFrom(ValueLayout.JAVA_BYTE, frame))};
      for (final BytesIn<?> input : inputs) {
        input.position(2);
        assertArrayEquals(new byte[] {21}, HOTLeafBody.selectBorrowed(input, 3, 4, 16, new byte[] {127}, 0, true));
        assertEquals(47, input.readByte());
        input.position(2);
        assertArrayEquals(new byte[] {22},
            HOTLeafBody.selectBorrowed(input, 3, 4, 16, new byte[] {(byte) 128}, 0, true));
        input.position(2);
        assertNull(HOTLeafBody.selectBorrowed(input, 3, 4, 16, new byte[] {(byte) 129}, 0, true));
        input.position(2);
        assertNull(HOTLeafBody.selectBorrowed(input, 3, 4, 16, new byte[] {0}, 0, true));
      }
    }
    frame[13] = 3;
    final ByteArrayBytesIn malformed = new ByteArrayBytesIn(frame);
    malformed.position(2);
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(malformed, 3, 4, 16, new byte[] {127}, 0, true));
  }

  @Test
  void borrowedSelectionRejectsTruncationAfterTheSelectedSlot() {
    final byte[] ungrouped = HexFormat.of().parseHex("0000070002010704010203040109040000");
    final byte[] grouped = HexFormat.of().parseHex("00000400010000070101012a0102012b");
    final byte[] shared = {1, 2, 3, 4};
    assertArrayEquals(shared,
        HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(ungrouped), 7, 2, 12, new byte[] {7}, 0, true));
    assertArrayEquals(new byte[] {42},
        HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(grouped), 11, 2, 8, new byte[] {7, 1}, 0, true));
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(Arrays.copyOf(ungrouped, ungrouped.length - 1)), 7, 2, 12,
            new byte[] {7}, 0, true));
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(Arrays.copyOf(grouped, grouped.length - 1)), 11, 2, 8,
            new byte[] {7, 1}, 0, true));
  }

  @Test
  void borrowedOneByteSearchKeepsUnsignedAdjacentOffsetsAndFinalBounds() {
    final int count = 130;
    final int slotBytes = 258;
    final int used = count * slotBytes;
    final byte[] value = new byte[255];
    final ByteBuffer frame = ByteBuffer.allocate(count * Short.BYTES + used).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      frame.putShort((short) (i * slotBytes));
    }
    for (int i = 0; i < count; i++) {
      value[0] = (byte) i;
      frame.put((byte) 1).put((byte) i).put((byte) value.length).put(value);
    }
    for (final int index : new int[] {127, 128, 129}) {
      value[0] = (byte) index;
      assertArrayEquals(value, HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(frame.array()), 3, count, used,
          new byte[] {(byte) index}, 0, true));
    }
    frame.putShort(129 * Short.BYTES, (short) (128 * slotBytes));
    assertThrows(IllegalStateException.class, () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(frame.array()), 3,
        count, used, new byte[] {(byte) 128}, 0, true));
  }

  @Test
  void borrowedAdjacentOffsetCannotExposeBytesAfterTheSlotRegion() {
    final byte[] frame = {0, 0, 10, 0, 1, 7, 7, 31, 32, 33, 34, 35, 36, 37};
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(frame), 3, 2, 8, new byte[] {7}, 0, true));
  }

  @Test
  void keyAndValueWidthDecisionsAreIndependentAt255And256() throws Exception {
    for (final int keyBytes : new int[] {255, 256}) {
      for (final int valueBytes : new int[] {255, 256}) {
        try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
          final byte[] key0 = new byte[keyBytes];
          final byte[] key1 = key0.clone();
          key1[0] = 1;
          final byte[] value0 = new byte[valueBytes];
          final byte[] value1 = value0.clone();
          value1[0] = 2;
          assertTrue(leaf.put(key0, value0));
          assertTrue(leaf.put(key1, value1));
          final byte[] bytes = write(FULL, leaf, false);
          final int mode = (keyBytes == 255
              ? 1
              : 0)
              | (valueBytes == 255
                  ? 2
                  : 0);
          assertEquals(mode == 0
              ? 4
              : 12, bytes[2]);
          if (mode != 0) {
            assertEquals(mode, bytes[3]);
          }
          try (HOTLeafPage decoded = read(bytes)) {
            assertArrayEquals(value0, decoded.getValue(decoded.findEntry(key0)));
            assertArrayEquals(value1, decoded.getValue(decoded.findEntry(key1)));
          }
        }
      }
    }
  }

  @Test
  void metadataMustEarnItsBytes() throws Exception {
    for (final int length : new int[] {255, 256}) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
        assertTrue(leaf.put(new byte[] {7}, new byte[length]));
        final byte[] frame = write(FULL, leaf, false);
        if (length == 255) {
          assertEquals(12, frame[2]);
          assertEquals(3, frame[3]);
        } else {
          assertEquals(4, frame[2], "a mode byte cancels the one narrow length");
        }
      }
    }
    for (final int length : new int[] {3, 4}) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
        assertTrue(leaf.put(new byte[] {7}, new byte[length]));
        assertTrue(leaf.put(new byte[] {9}, new byte[length]));
        assertEquals(length == 3
            ? 3
            : 7, write(FULL, leaf, false)[3],
            "the sharing bitmap must cost less than replacing inline values with pointers");
      }
    }
    for (final int length : new int[] {4, 5}) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
        final byte[] first = new byte[256];
        final byte[] second = first.clone();
        final byte[] third = first.clone();
        second[0] = 1;
        third[0] = 2;
        assertTrue(leaf.put(first, new byte[length]));
        assertTrue(leaf.put(second, new byte[length]));
        assertTrue(leaf.put(third, new byte[256]));
        final byte[] frame = write(FULL, leaf, false);
        if (length == 4) {
          assertEquals(4, frame[2], "sharing saves exactly the combined mode and bitmap cost");
        } else {
          assertEquals(12, frame[2]);
          assertEquals(4, frame[3]);
        }
      }
    }
    for (final int groupSize : new int[] {7, 8}) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
        for (int i = 0; i < 16; i++) {
          assertTrue(leaf.put(new byte[] {(byte) i}, new byte[] {42}));
        }
        for (int i = 0; i < groupSize; i++) {
          assertTrue(leaf.put(new byte[] {16, (byte) i}, new byte[] {42}));
        }
        assertEquals(groupSize == 7
            ? 3
            : 11, write(FULL, leaf, false)[3], "the two group records cost six bytes before prefix savings");
      }
    }
  }

  @Test
  void aHashCollisionDoesNotShareUnequalValues() throws Exception {
    // These unequal eight-byte values collide in the writer's 32-bit slot hash.
    final byte[] first = HexFormat.of().parseHex("0000000400000000");
    final byte[] collision = HexFormat.of().parseHex("3d73379d1ef7158b");
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      assertTrue(leaf.put(new byte[] {7}, first));
      assertTrue(leaf.put(new byte[] {8}, collision));
      assertTrue(leaf.put(new byte[] {9}, first));
      final byte[] frame = write(FULL, leaf, false);
      assertEquals(7, frame[3]);
      assertEquals(4, frame[26], "only the third slot may alias");
      try (HOTLeafPage decoded = read(frame)) {
        assertArrayEquals(first, decoded.getValue(0));
        assertArrayEquals(collision, decoded.getValue(1));
        assertArrayEquals(first, decoded.getValue(2));
      }
    }
  }

  @Test
  void sharingCrossesAProbeClusterWithoutAliasingTwoByteValues() throws Exception {
    // The first three values occupy the same bucket in the writer's sharing table.
    final byte[] first = HexFormat.of().parseHex("0000000070726f6265636f6c6c6973696f6e");
    final byte[] second = HexFormat.of().parseHex("0002000070726f6265636f6c6c6973696f6e");
    final byte[] third = HexFormat.of().parseHex("0004000070726f6265636f6c6c6973696f6e");
    final byte[][] values = {first, second, third, first, first, {7, 7}, {7, 7}};
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (int i = 0; i < values.length; i++) {
        assertTrue(leaf.put(new byte[] {(byte) (7 + i)}, values[i]));
      }
      final byte[] frame = write(FULL, leaf, false);
      assertEquals(7, frame[3]);
      assertEquals(0x18, frame[20 + values.length * Short.BYTES]);
      try (HOTLeafPage decoded = read(frame)) {
        for (int i = 0; i < values.length; i++) {
          assertArrayEquals(values[i], decoded.getValue(i));
        }
      }
    }
  }

  @Test
  void rejectsUnknownModesAndMalformedPackedRegions() {
    for (final byte mode : new byte[] {0, 0x20, 0x41, (byte) 0x80}) {
      final byte[] frame = NARROW_LENGTHS.clone();
      frame[3] = mode;
      final ByteArrayBytesIn input = new ByteArrayBytesIn(frame);
      assertThrows(IllegalStateException.class,
          () -> new PagePersister().deserializePage(FULL, input, SerializationType.DATA));
      assertEquals(4, input.position());
    }
    for (int length = 4; length < SHARED.length; length++) {
      final byte[] truncated = Arrays.copyOf(SHARED, length);
      assertThrows(RuntimeException.class, () -> read(truncated).close());
    }
    for (final int[] change : new int[][] {{20, 1}, {22, 2}, {24, 6}, {24, 3}, {38, 7}, {39, 1}, {39, 11}}) {
      final byte[] bad = SHARED.clone();
      bad[change[0]] = (byte) change[1];
      assertThrows(IllegalStateException.class, () -> read(bad).close(), Arrays.toString(change));
    }
  }

  @Test
  void packedDimensionsIncludeTheMaximumAndExcludeEmptyBodies() {
    final int count = HOTLeafPage.MAX_ENTRIES;
    final ByteBuffer many = ByteBuffer.allocate(count * 6).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      many.putShort((short) (i * 4));
    }
    for (int i = 0; i < count; i++) {
      many.put((byte) 2).put((byte) (i >>> Byte.SIZE)).put((byte) i).put((byte) 0);
    }
    HOTLeafBody.read(new ByteArrayBytesIn(many.array()), 3, count, count * 4).validate(0, IndexType.PROJECTION);

    final int used = HOTLeafPage.DEFAULT_SIZE;
    final ByteBuffer wide = ByteBuffer.allocate(3 + used).order(ByteOrder.LITTLE_ENDIAN);
    wide.position(5);
    wide.putShort((short) (used - 4));
    HOTLeafBody.read(new ByteArrayBytesIn(wide.array()), HOTLeafBody.SHARED_VALUES, 1, used)
               .validate(0, IndexType.PROJECTION);
    assertThrows(IllegalStateException.class, () -> HOTLeafBody.read(new ByteArrayBytesIn(new byte[2]), 3, 0, 2));
    assertThrows(IllegalStateException.class, () -> HOTLeafBody.read(new ByteArrayBytesIn(new byte[2]), 3, 1, 0));
  }

  @Test
  void checksSuffixBoundsBeforeUsingASelectedSlot() {
    final byte[] exact = {0, 0, 2, 0, 0, 0, 0, 0};
    // An empty key tail and an empty value: the value ref has length zero.
    assertEquals(0, (int) HOTLeafBody.read(new ByteArrayBytesIn(exact), 3, 2, 4).checkedValueRef(1));
    final byte[] truncatedHeader = Arrays.copyOf(exact, exact.length - 1);
    final HOTLeafBody shortHeader = HOTLeafBody.read(new ByteArrayBytesIn(truncatedHeader), 3, 2, 3);
    assertThrows(IllegalStateException.class, () -> shortHeader.checkedValueRef(1));
    final byte[] truncatedKey = exact.clone();
    truncatedKey[6] = 1;
    final HOTLeafBody shortKey = HOTLeafBody.read(new ByteArrayBytesIn(truncatedKey), 3, 2, 4);
    assertThrows(IllegalStateException.class, () -> shortKey.checkedValueRef(1));
    final HOTLeafBody good = HOTLeafBody.read(new ByteArrayBytesIn(new byte[] {0, 0, 1, 7, 0}), 3, 1, 3);
    good.validate(65_534, IndexType.PROJECTION);
    assertThrows(IllegalStateException.class, () -> good.validate(65_535, IndexType.PROJECTION));
  }

  @Test
  void validatesPostingContentsBeforePublishingTheLeaf() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.CAS)) {
      assertTrue(leaf.put(new byte[] {7}, posting(101, 450)));
      assertTrue(leaf.put(new byte[] {9}, posting(15, 20)));
      final byte[] encoded = write(FULL, leaf, false);
      assertEquals(0x13, encoded[3]);
      final byte[] undeclared = encoded.clone();
      undeclared[3] &= ~0x10;
      assertThrows(IllegalStateException.class, () -> read(undeclared).close());
      final byte[] invalid = encoded.clone();
      invalid[invalid.length - 1] = 0;
      assertThrows(IllegalStateException.class, () -> read(invalid).close());
    }
    final byte[] differentKeyLength = {0, 0, 2, 1, 2, 5, 1, 0, 0, 0, 0};
    final HOTLeafBody malformed = HOTLeafBody.read(new ByteArrayBytesIn(differentKeyLength), 0x13, 1, 9);
    assertThrows(IllegalStateException.class, () -> malformed.validate(0, IndexType.CAS));
  }

  @Test
  void projectionReadersRejectAPostingFrameModeEvenForInlineValues() {
    final byte[] illegal = NARROW_LENGTHS.clone();
    illegal[3] |= HOTLeafBody.POSTING_FRAMES;
    assertThrows(IllegalStateException.class, () -> read(illegal).close());
    final ByteArrayBytesIn input = new ByteArrayBytesIn(illegal);
    input.position(1);
    assertThrows(IllegalStateException.class, () -> PageKind.deserializeHOTProjectionEntry(input, new byte[] {9}, 0));
    input.position(1);
    assertThrows(IllegalStateException.class, () -> HOTProjectionImage.copyIfFits(input));
  }

  @Test
  void packedLeafBindsAndReleasesItsAllocationOnRejectionAndClose() throws Exception {
    final FrameSlotAllocator allocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(NARROW_LENGTHS.length);
    final long priorAllocations = allocator.allocateCount(frameClass);
    final long priorReleases = allocator.releaseCount(frameClass);
    try (HOTLeafPage leaf = read(NARROW_LENGTHS)) {
      final long stamp = leaf.readStamp();
      assertTrue(stamp > 0 && (stamp & 1) == 0, "the packed body must use its allocator slot generation");
      assertTrue(leaf.validateStamp(leaf.readStampBinding(), stamp));
    }
    final byte[] trailing = Arrays.copyOf(NARROW_LENGTHS, NARROW_LENGTHS.length + 1);
    assertThrows(IllegalStateException.class, () -> read(trailing).close());
    assertEquals(2, allocator.allocateCount(frameClass) - priorAllocations);
    assertEquals(2, allocator.releaseCount(frameClass) - priorReleases);

    final AtomicInteger releases = new AtomicInteger();
    final MemorySegment segment = MemorySegment.ofArray(trailing);
    final ByteHandler.DecompressionResult frame =
        new ByteHandler.DecompressionResult(segment, segment, releases::incrementAndGet, new AtomicBoolean());
    assertThrows(IllegalStateException.class, () -> new PagePersister().deserializePage(FULL,
        new MemorySegmentBytesIn(segment), SerializationType.DATA, frame));
    assertTrue(frame.ownershipTransferred().get());
    assertEquals(1, releases.get());
    frame.close();
    assertEquals(1, releases.get());
  }

  @Test
  void nonzeroSharedPointersSelectTheEarlierSlotExactly() {
    final byte[] frame = HexFormat.of().parseHex("000006000c0011000c01100301020301200304050601300302000140030000");
    final HOTLeafBody body = HOTLeafBody.read(new ByteArrayBytesIn(frame), 7, 4, 22);
    body.validate(0, IndexType.PROJECTION);
    assertArrayEquals(new byte[] {1, 2, 3}, body.copyValue(body.checkedValueRef(3)));
    assertArrayEquals(new byte[] {4, 5, 6}, body.copyValue(body.checkedValueRef(2)));
    assertEquals(body.checkedValueRef(2), body.valueRef(2));
    for (final byte invalid : new byte[] {1, 4, 6, 8}) {
      final byte[] corrupt = frame.clone();
      corrupt[corrupt.length - 2] = invalid;
      final HOTLeafBody malformed = HOTLeafBody.read(new ByteArrayBytesIn(corrupt), 7, 4, 22);
      assertThrows(IllegalStateException.class, () -> malformed.validate(0, IndexType.PROJECTION));
    }
    final byte[] repeatedOwner = frame.clone();
    repeatedOwner[repeatedOwner.length - 2] = 2;
    HOTLeafBody.read(new ByteArrayBytesIn(repeatedOwner), 7, 4, 22).validate(0, IndexType.PROJECTION);
    repeatedOwner[repeatedOwner.length - 3] = 4;
    final HOTLeafBody wrongLength = HOTLeafBody.read(new ByteArrayBytesIn(repeatedOwner), 7, 4, 22);
    assertThrows(IllegalStateException.class, () -> wrongLength.validate(0, IndexType.PROJECTION));

    final byte[] noSaving = HexFormat.of().parseHex("000005000201070202040109020000");
    final HOTLeafBody tooShort = HOTLeafBody.read(new ByteArrayBytesIn(noSaving), 7, 2, 10);
    assertThrows(IllegalStateException.class, () -> tooShort.validate(0, IndexType.PROJECTION));
  }

  @Test
  void nativeRepeatedAliasesMatchBothTargetAndLengthInsideExactSlotBounds() {
    final byte[] frame =
        HexFormat.of().parseHex("727374000006000c0011000c01100301020301200304050601300302000140030200");
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment valid = arena.allocateFrom(ValueLayout.JAVA_BYTE, frame);
      final MemorySegmentBytesIn source = new MemorySegmentBytesIn(valid.asReadOnly());
      source.position(3);
      final HOTLeafBody body = HOTLeafBody.read(source, 7, 4, 22);
      body.validate(0, IndexType.PROJECTION);
      assertEquals(frame.length, source.position());
      assertArrayEquals(new byte[] {4, 5, 6}, body.copyValue(body.valueRef(3)));

      for (final int[] change : new int[][] {{2, 4}, {1, 4}, {0, 1}}) {
        final byte[] malformed = frame.clone();
        malformed[malformed.length - 3 + change[0]] = (byte) change[1];
        final MemorySegment invalid = arena.allocateFrom(ValueLayout.JAVA_BYTE, malformed);
        final MemorySegmentBytesIn input = new MemorySegmentBytesIn(invalid.asReadOnly());
        input.position(3);
        final HOTLeafBody bad = HOTLeafBody.read(input, 7, 4, 22);
        assertThrows(IllegalStateException.class, () -> bad.validate(0, IndexType.PROJECTION));
      }
    }
  }

  @Test
  void sharedWriterEmitsAnOffsetIntoTheDirectory() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      final byte[] unique = {1, 2, 3, 4};
      final byte[] shared = {5, 6, 7, 8};
      assertTrue(leaf.put(new byte[] {7}, unique));
      assertTrue(leaf.put(new byte[] {8}, shared));
      assertTrue(leaf.put(new byte[] {9}, shared));
      final byte[] frame = write(FULL, leaf, false);
      assertEquals(7, frame[3]);
      assertEquals(2, Byte.toUnsignedInt(frame[frame.length - 2]));
      assertEquals(0, frame[frame.length - 1]);
      try (HOTLeafPage decoded = read(frame)) {
        assertArrayEquals(shared, decoded.getValue(decoded.findEntry(new byte[] {9})));
      }
    }
  }

  @Test
  void fixedWordLeafResolvesNonzeroOwnersWithEitherValueWidth() throws Exception {
    for (final int length : new int[] {8, 257}) {
      final int width = length <= 255
          ? Byte.BYTES
          : Short.BYTES;
      final int headerBytes = Byte.BYTES + Long.BYTES + width;
      final int inlineBytes = headerBytes + length;
      final int used = 2 * inlineBytes + headerBytes + Short.BYTES;
      final byte[] shared = new byte[length];
      shared[0] = 2;
      final ByteBuffer frame = ByteBuffer.allocate(27 + used).order(ByteOrder.LITTLE_ENDIAN);
      frame.put(new byte[] {12, 0, 12, (byte) (width == 1
          ? 7
          : 5), 7});
      frame.putInt(3).put(IndexType.PROJECTION.getID()).putShort((short) 0).putInt(3).putInt(used);
      frame.putShort((short) 0).putShort((short) inlineBytes).putShort((short) (inlineBytes * 2)).put((byte) 4);
      for (int i = 1; i <= 3; i++) {
        frame.put((byte) Long.BYTES).put(key(i));
        if (width == 1) {
          frame.put((byte) length);
        } else {
          frame.putShort((short) length);
        }
        if (i == 3) {
          frame.putShort((short) 2);
        } else {
          frame.put(i == 2
              ? shared
              : new byte[length]);
        }
      }
      assertEquals(frame.capacity(), frame.position());
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, frame.array());
        final MemorySegmentBytesIn source = new MemorySegmentBytesIn(segment.asReadOnly());
        try (HOTLeafPage decoded =
            (HOTLeafPage) new PagePersister().deserializePage(FULL, source, SerializationType.DATA)) {
          final long owner = decoded.valueRef(decoded.findEntry(key(2)));
          final long alias = decoded.valueRef(decoded.findEntry(key(3)));
          assertEquals(owner, alias);
          assertEquals(length, HOTLeafPage.refLength(alias));
          assertArrayEquals(shared, decoded.getValue(decoded.findEntry(key(3))));
        }
      }
    }
  }

  @Test
  void fixedWordProjectionWritesOrdinarySlotsAndNarrowDirectory() throws Exception {
    final byte[] repeated = {1, 2, 3, 4, 5, 6, 7, 8};
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (int i = 0; i < 17; i++) {
        assertTrue(leaf.put(key(i), repeated));
      }
      final byte[] frame = write(FULL, leaf, false);
      assertEquals(HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS, frame[2]);
      try (HOTLeafPage decoded = read(frame)) {
        assertFalse(decoded.hasPackedBody());
        assertTrue(decoded.valueRef(0) != decoded.valueRef(1));
        assertArrayEquals(repeated, decoded.getValue(decoded.findEntry(key(16))));
      }
    }
  }

  @Test
  void preparedVariableKeysKeepTheBorrowedSelectionOrderAndBounds() throws Exception {
    final byte[][] keys = {{6, 7}, {6, 9, 11}, {6, 9, 11, 13}, {6, 10, 0, 0, 0, 0, 0, 0, 0, 0}};
    final byte[] value = {1, 2, 3, 4};
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (final byte[] key : keys) {
        assertTrue(leaf.put(key, value));
      }
      assertEquals(1, leaf.getCommonPrefixLen());
      final byte[] frame = write(FULL, leaf, false);
      final ByteArrayBytesIn input = new ByteArrayBytesIn(frame);
      input.position(1);
      final HOTProjectionImage image = HOTProjectionImage.copyIfFits(input);
      for (final byte[] key : keys) {
        assertArrayEquals(value, image.select(key, 0).value());
      }
      final long unpreparedBytes = image.retainedSizeBytes();
      image.prepareForCache();
      assertEquals(unpreparedBytes, image.retainedSizeBytes());
      for (final byte[] key : keys) {
        assertArrayEquals(value, image.select(key, 0).value());
      }
      assertNull(image.select(new byte[] {6, 9, 12}, 0));
      frame[frame.length - 2] = 1;
      input.position(1);
      final HOTProjectionImage bad = HOTProjectionImage.copyIfFits(input);
      assertThrows(IllegalStateException.class, bad::prepareForCache);
    }
  }

  @Test
  void preparedUniformKeysIncludeTheirCommonPrefix() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      final byte[] first = {7, 13, 2};
      final byte[] last = {7, 13, (byte) 255};
      assertTrue(leaf.put(first, new byte[] {31}));
      assertTrue(leaf.put(last, new byte[] {32}));
      assertEquals(2, leaf.getCommonPrefixLen());
      final ByteArrayBytesIn input = new ByteArrayBytesIn(write(FULL, leaf, false));
      input.position(1);
      final HOTProjectionImage image = HOTProjectionImage.copyIfFits(input);
      final long unpreparedBytes = image.retainedSizeBytes();
      image.prepareForCache();
      assertEquals(unpreparedBytes + 16 + 4 * Long.BYTES, image.retainedSizeBytes());
      assertArrayEquals(new byte[] {31}, image.select(first, 0).value());
      assertArrayEquals(new byte[] {32}, image.select(last, 0).value());
      assertNull(image.select(new byte[] {7, 13, 3}, 0));
    }
  }

  @Test
  void packedImagesRejectUnorderedPreparedKeysAndMalformedBorrowedValues() {
    for (final byte secondKey : new byte[] {6, 7}) {
      final byte[] unordered = NARROW_LENGTHS.clone();
      unordered[unordered.length - 3] = secondKey;
      final ByteArrayBytesIn input = new ByteArrayBytesIn(unordered);
      input.position(1);
      final HOTProjectionImage image = HOTProjectionImage.copyIfFits(input);
      assertThrows(IllegalStateException.class, image::prepareForCache);
    }
    final byte[] truncatedValue = NARROW_LENGTHS.clone();
    truncatedValue[truncatedValue.length - 2] = 2;
    final ByteArrayBytesIn input = new ByteArrayBytesIn(truncatedValue);
    input.position(1);
    final HOTProjectionImage image = HOTProjectionImage.copyIfFits(input);
    assertThrows(IllegalStateException.class, () -> image.select(new byte[] {9}, 0));
  }

  @Test
  void packedAliasesCannotExpandPastTheOrdinarySlotCapacity() {
    final int inlineLength = 255;
    final int extraKeyBytes = 16;
    for (final int count : new int[] {8, 252, 253}) {
      final int directory = count * Short.BYTES;
      final int bitmap = (count + 7) / 8;
      final int slots = directory + bitmap;
      final ByteBuffer frame = ByteBuffer.allocate(slots + 3 + extraKeyBytes + inlineLength + (count - 1) * 5)
                                         .order(ByteOrder.LITTLE_ENDIAN);
      frame.position(slots);
      frame.put((byte) (1 + extraKeyBytes)).put(new byte[1 + extraKeyBytes]);
      frame.put((byte) inlineLength).put(new byte[inlineLength]);
      for (int i = 1; i < count; i++) {
        frame.putShort(i * Short.BYTES, (short) (frame.position() - slots));
        final int at = directory + (i >>> 3);
        frame.put(at, (byte) (frame.get(at) | (1 << (i & 7))));
        frame.put((byte) 1).put((byte) i).put((byte) inlineLength).putShort((short) 0);
      }
      final HOTLeafBody body =
          HOTLeafBody.read(new ByteArrayBytesIn(frame.array()), 7, count, frame.position() - slots);
      if (count <= 252) {
        body.validate(0, IndexType.PROJECTION);
      } else {
        assertThrows(IllegalStateException.class, () -> body.validate(0, IndexType.PROJECTION));
      }
    }
  }

  @Test
  void groupedKeysReadLikeContiguousBytesFromEveryOffset() {
    final byte[] first = {1, 2, 3};
    final byte[] second = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    final byte[][] keys = new byte[32][];
    final ByteBuffer encoded = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
    final int slots = 5 + keys.length * Short.BYTES + 6 + first.length + second.length;
    encoded.position(5 + keys.length * Short.BYTES);
    encoded.put((byte) first.length).putShort((short) 0);
    encoded.put((byte) second.length).putShort((short) first.length);
    encoded.put(first).put(second);
    for (int i = 0; i < keys.length; i++) {
      final byte[] prefix = i < 16
          ? first
          : second;
      final int length = i % 16;
      keys[i] = Arrays.copyOf(prefix, prefix.length + length);
      Arrays.fill(keys[i], prefix.length, keys[i].length, (byte) (128 + i));
      encoded.putShort(5 + i * Short.BYTES, (short) (encoded.position() - slots));
      encoded.put((byte) length).put(keys[i], prefix.length, length).put((byte) 1).put((byte) i);
    }
    final ByteArrayBytesIn in = new ByteArrayBytesIn(Arrays.copyOf(encoded.array(), encoded.position()));
    in.position(5);
    final HOTLeafBody body = HOTLeafBody.read(in, 0x0b, keys.length, encoded.position() - slots);
    body.validate(0, IndexType.PROJECTION);
    for (int index = 0; index < keys.length; index++) {
      final byte[] key = keys[index];
      final byte[] copy = new byte[key.length + 1];
      body.copyKeyTail(index, MemorySegment.ofArray(copy), 1);
      assertArrayEquals(key, Arrays.copyOfRange(copy, 1, copy.length));
      assertEquals(-1, body.differingBit(index, index));
      if (index != 0) {
        final byte[] previous = keys[index - 1];
        final int at = Arrays.mismatch(previous, key);
        final int bit = at == previous.length
            ? at * Byte.SIZE
            : at * Byte.SIZE + Integer.numberOfLeadingZeros(Byte.toUnsignedInt((byte) (previous[at] ^ key[at]))) - 24;
        assertEquals(bit, body.differingBit(index - 1, index));
        assertTrue(body.compareKeyTail(index, previous, 0, Math.min(key.length, previous.length)) >= 0);
      }
      assertEquals(0, body.compareKeyTail(index, key, 0, key.length));
      for (int pos = 0; pos < key.length; pos++) {
        assertEquals(Byte.toUnsignedInt(key[pos]), body.keyTailByte(index, pos));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(key, pos, pos + Long.BYTES)).getLong(),
            body.keyTailWord(index, pos));
        if (pos <= key.length - Integer.BYTES) {
          assertEquals(ByteBuffer.wrap(key).getInt(pos), body.keyTailInt(index, pos));
        }
      }
    }
  }

  @Test
  void groupedPrefixAndPostingReadsMatchOrdinarySlotsAcrossGroupBoundaries() throws Exception {
    for (final IndexType index : new IndexType[] {IndexType.CAS, IndexType.VALIDTIME}) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, index)) {
        for (int i = 0; i < 35; i++) {
          assertTrue(leaf.put(key(i), posting(i, i + 256)));
        }
        final byte[] encoded = write(FULL, leaf, false);
        assertEquals(0x1b, encoded[3]);
        final ByteArrayBytesIn header = new ByteArrayBytesIn(encoded);
        header.position(4);
        Utils.getVarLong(header);
        header.skip(Integer.BYTES + 1);
        header.skip(Short.toUnsignedInt(header.readShort()));
        final int count = header.readInt() & Integer.MAX_VALUE;
        header.skip(Integer.BYTES + (long) count * Short.BYTES);
        final byte[] groupMetadata = new byte[27];
        header.read(groupMetadata);
        assertArrayEquals(HexFormat.of().parseHex("060000060600060c00000507090b0d010507090b0d020507090b0d"),
            groupMetadata);
        final byte[] badGroup = encoded.clone();
        badGroup[(int) header.position() - groupMetadata.length + 4] = 7;
        assertThrows(IllegalStateException.class, () -> read(badGroup).close());
        final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
        try (HOTLeafPage packed = read(encoded); HOTLeafPage raw = read(write(FULL, leaf, true))) {
          for (int i = 0; i < 35; i++) {
            final byte[] key = key(i);
            assertEquals(raw.findEntry(key), packed.findEntry(key));
            assertArrayEquals(key, packed.getKey(i));
            assertEquals(key.length, packed.getKeyLength(i));
            assertEquals(0, packed.compareKeyWithBound(i, key));
            assertEquals(raw.compareKeyPrefix(i, key, 5), packed.compareKeyPrefix(i, key, 5));
            assertEquals(raw.compareKeyPrefixPart(i, 2, key, 6), packed.compareKeyPrefixPart(i, 2, key, 6));
            assertEquals(raw.readKeyIntBE(i, 2), packed.readKeyIntBE(i, 2));
            assertEquals(raw.decodeKey8BE(i), packed.decodeKey8BE(i));
            assertArrayEquals(raw.getValue(i), packed.getValue(i));
            // A posting frame is exported in its ordinary representation.
            assertArrayEquals(raw.getValue(i), packed.getValueSlice(i).toArray(ValueLayout.JAVA_BYTE));
            final long ref = packed.valueRef(i);
            assertEquals(HOTPostingFrame.TAG, packed.refByteAt(ref, 0));
            assertTrue(accumulator.addChunk(packed, ref, 3L << 16, null));
            assertArrayEquals(new long[] {(3L << 16) | i, (3L << 16) | (i + 256)},
                accumulator.toNodeReferencesAndReset().toSortedArray());
            assertEquals(raw.findEntry(Arrays.copyOf(key, 7)), packed.findEntry(Arrays.copyOf(key, 7)));
          }
          assertTrue(encoded.length < write(FULL, leaf, true).length);
        }
      }
    }
  }

  @Test
  void fixedWordProjectionKeepsContiguousSuffixesDespiteRepeatedPrefixes() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (int i = 0; i < 35; i++) {
        assertTrue(leaf.put(key(i), new byte[] {(byte) i, (byte) (i + 1)}));
      }
      final byte[] frame = write(FULL, leaf, false);
      assertEquals(HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS, frame[2]);
      try (HOTLeafPage decoded = read(frame)) {
        for (int i = 0; i < 35; i++) {
          assertEquals(i, decoded.findEntry(key(i)));
        }
        assertEquals(-36, decoded.findEntry(new byte[] {3, 0, 0, 0, 0, 0, 0, 0}));
      }
    }
  }

  @Test
  void fixedWordSearchKeepsTheLastProbeInsideAnEmptyValueSlotRegion() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (int i = 0; i < 35; i++) {
        assertTrue(leaf.put(ByteBuffer.allocate(Long.BYTES).putLong(i).array(), new byte[0]));
      }
      try (HOTLeafPage decoded = read(write(FULL, leaf, false))) {
        assertEquals(34, decoded.findEntry(ByteBuffer.allocate(Long.BYTES).putLong(34).array()));
        assertEquals(-36, decoded.findEntry(ByteBuffer.allocate(Long.BYTES).putLong(35).array()));
      }
    }
  }

  @Test
  void completeCacheCopyKeepsThePackedFrameAndWritableCopyKeepsSeparateValues() throws Exception {
    final byte[] frame = SHARED.clone();
    frame[28] = 77; // First byte of the inline value in the first slot.
    final HOTLeafPage source = read(frame);
    try (source; HOTLeafPage cached = source.copyForCache(); HOTLeafPage writable = source.copyForRead()) {
      assertTrue(cached.hasPackedBody());
      assertFalse(writable.hasPackedBody());
      source.close();
      final byte[] shared = {77, 2, 3, 4, 5, 6, 7, 8};
      assertArrayEquals(shared, cached.getValue(cached.findEntry(new byte[] {9})));
      assertTrue(writable.putOrReplace(new byte[] {7}, new byte[] {19}));
      assertArrayEquals(shared, writable.getValue(writable.findEntry(new byte[] {9})));
      assertArrayEquals(shared, cached.getValue(cached.findEntry(new byte[] {7})));
    }
  }

  @Test
  void reconstructedPostingFragmentsCopyEncodedOrdinalsIntoTheirDestinationSlots() throws Exception {
    final byte[] tombstone = {(byte) 0xfe};
    for (final IndexType index : new IndexType[] {IndexType.CAS, IndexType.VALIDTIME}) {
      try (HOTLeafPage oldest = new HOTLeafPage(7, 3, index); HOTLeafPage newest = new HOTLeafPage(7, 4, index)) {
        assertTrue(oldest.put(key(0), posting(1, 2)));
        assertTrue(oldest.put(key(1), posting(255, 256)));
        assertTrue(oldest.put(key(2), posting(0, 65535)));
        oldest.setCompleteDump(true);
        assertTrue(newest.put(key(1), tombstone));
        try (HOTLeafPage packed = read(write(FULL, oldest, false)); HOTLeafPage raw = read(write(FULL, newest, true))) {
          assertTrue(packed.isPostingFrame(packed.valueRef(0)));
          for (final VersioningType versioning : List.of(VersioningType.DIFFERENTIAL, VersioningType.INCREMENTAL,
              VersioningType.SLIDING_SNAPSHOT)) {
            try (HOTLeafPage result = versioning.combineHOTLeafPages(List.of(raw, packed), 3, null)) {
              assertEquals(3, result.getEntryCount());
              assertArrayEquals(posting(1, 2), result.getValue(result.findEntry(key(0))));
              assertArrayEquals(tombstone, result.getValue(result.findEntry(key(1))));
              assertArrayEquals(posting(0, 65535), result.getValue(result.findEntry(key(2))));
            }
          }
        }
      }
    }
  }

  @Test
  void sparseAliasesReferOnlyToTheirOwnFramesAndCopiesDoNotAlias() throws Exception {
    final byte[] initial = {11, 22, 33, 44, 55, 66, 77};
    final byte[] change = {12, 23, 34, 45, 56, 67, 78};
    final byte[][] keys = new byte[4][];
    try (HOTLeafPage source = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      for (int i = 0; i < keys.length; i++) {
        keys[i] = Arrays.copyOf(key(i), Long.BYTES - 1);
        assertTrue(source.put(keys[i], initial));
      }
      try (HOTLeafPage old = read(write(FULL, source, true)); HOTLeafPage writer = old.copyForRevision(4)) {
        assertTrue(writer.putOrReplace(keys[1], change));
        assertTrue(writer.putOrReplace(keys[3], change));
        final byte[] encoded = write(SLIDING, writer, false);
        assertTrue((encoded[3] & 4) != 0);
        try (HOTLeafPage latest = read(encoded); HOTLeafPage reconstructed = latest.copy()) {
          assertFalse(latest.isCompleteDump());
          assertEquals(2, latest.getEntryCount());
          assertTrue(reconstructed.fillProjectionEntry(old, 0));
          assertTrue(reconstructed.fillProjectionEntry(old, 2));
          assertTrue(reconstructed.putOrReplace(keys[1], initial));
          assertArrayEquals(initial, old.getValue(old.findEntry(keys[1])));
          assertArrayEquals(change, latest.getValue(latest.findEntry(keys[1])));
          assertArrayEquals(change, reconstructed.getValue(reconstructed.findEntry(keys[3])));
          assertArrayEquals(initial, reconstructed.getValue(reconstructed.findEntry(keys[2])));
        }
      }
    }
  }

  /** The body-mode flag needs the narrow directory: every reader refuses it alone, with or without entries. */
  @Test
  void bodyModeWithoutTheNarrowDirectoryIsRefusedByEveryReader() {
    final byte[] entries = NARROW_LENGTHS.clone();
    entries[2] = HOTLeafBody.FLAG_BODY_MODE;
    final byte[] empty = HexFormat.of().parseHex("0c00080307030000000a00000000000000000000");
    for (final byte[] frame : new byte[][] {entries, empty}) {
      assertThrows(IllegalStateException.class, () -> read(frame).close());
      final ByteArrayBytesIn selection = new ByteArrayBytesIn(frame);
      selection.position(1);
      assertThrows(IllegalStateException.class,
          () -> PageKind.deserializeHOTProjectionEntry(selection, new byte[] {9}, 0));
      final ByteArrayBytesIn image = new ByteArrayBytesIn(frame);
      image.position(1);
      assertThrows(IllegalStateException.class, () -> HOTProjectionImage.copyIfFits(image));
    }
  }

  /** Readers accept any {@link BytesIn}, not only the two built-in sources. */
  @Test
  void packedLeafReadsFromAnyBytesInImplementation() throws Exception {
    for (final byte[] frame : new byte[][] {NARROW_LENGTHS, SHARED}) {
      final byte[] expected = frame == SHARED
          ? new byte[] {1, 2, 3, 4, 5, 6, 7, 8}
          : new byte[] {51};
      try (HOTLeafPage leaf =
          (HOTLeafPage) new PagePersister().deserializePage(FULL, new ForeignBytesIn(frame), SerializationType.DATA)) {
        assertArrayEquals(expected, leaf.getValue(leaf.findEntry(new byte[] {9})));
      }
      final ForeignBytesIn selection = new ForeignBytesIn(frame);
      selection.position(1);
      assertArrayEquals(expected, PageKind.deserializeHOTProjectionEntry(selection, new byte[] {9}, 0).value());
    }
  }

  /** Selection from an unvalidated region refuses malformed shared values and slot bounds. */
  @Test
  void borrowedSelectionRefusesMalformedSharingAndSlotBounds() {
    // A shared value must be longer than its two-byte pointer.
    final byte[] twoByteShare = HexFormat.of().parseHex("000005000201070201020109020000");
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(twoByteShare), 7, 2, 10, new byte[] {9}, 0, true));

    // Entry 2 shares entry 4's value, a later entry, whose directory offset aliases slot 0.
    final byte[] laterOwner =
        HexFormat.of().parseHex("000007000e00130000000700" + "04" + "01050401020304" + "0107040a0b0c0d" + "0109040800"
            + "010b0133");
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(laterOwner), 7, 6, 23, new byte[] {9}, 0, true));

    // Slot 0 ends at the next directory offset, past the slot region, inside trailing bytes.
    final byte[] pastUsed = HexFormat.of().parseHex("00000900010706010203040506");
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegmentBytesIn input = new MemorySegmentBytesIn(arena.allocateFrom(ValueLayout.JAVA_BYTE, pastUsed));
      assertThrows(IllegalStateException.class,
          () -> HOTLeafBody.selectBorrowed(input, 3, 2, 8, new byte[] {7}, 0, true));
    }

    // Two-byte key headers: the last slot holds one byte, at the very end of the input.
    final byte[] oneByteSlot = HexFormat.of().parseHex("00000500010007012a01");
    assertThrows(IllegalStateException.class,
        () -> HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(oneByteSlot), 2, 2, 6, new byte[] {9}, 0, true));
  }

  /** A cached image with group prefixes and variable key lengths selects every key. */
  @Test
  void preparedGroupedImageSelectsEveryKey() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION)) {
      final byte[][] keys = new byte[36][];
      for (int i = 0; i < 35; i++) {
        keys[i] = key(i);
      }
      keys[35] = new byte[] {3, 5, 7, 9, 11, 13, 0, 19, 1};
      for (int i = 0; i < keys.length; i++) {
        assertTrue(leaf.put(keys[i], new byte[] {(byte) i, 1}));
      }
      final byte[] encoded = write(FULL, leaf, false);
      assertTrue((encoded[3] & HOTLeafBody.GROUP_PREFIXES) != 0, "the body groups key prefixes");
      final ByteArrayBytesIn source = new ByteArrayBytesIn(encoded);
      source.position(1);
      final HOTProjectionImage image = HOTProjectionImage.copyIfFits(source);
      image.prepareForCache();
      for (int i = 0; i < keys.length; i++) {
        assertArrayEquals(new byte[] {(byte) i, 1}, image.select(keys[i], 0).value(), "key " + i);
      }
      assertNull(image.select(new byte[] {3, 5, 7, 9, 11, 13, 0, 19}, 0));
    }
  }

  /** A third-party {@link BytesIn}: delegates to a heap source without being one. */
  private static final class ForeignBytesIn implements BytesIn<byte[]> {
    private final ByteArrayBytesIn delegate;

    ForeignBytesIn(final byte[] bytes) {
      delegate = new ByteArrayBytesIn(bytes);
    }

    @Override
    public int readInt() {
      return delegate.readInt();
    }

    @Override
    public long readLong() {
      return delegate.readLong();
    }

    @Override
    public byte readByte() {
      return delegate.readByte();
    }

    @Override
    public boolean readBoolean() {
      return delegate.readBoolean();
    }

    @Override
    public double readDouble() {
      return delegate.readDouble();
    }

    @Override
    public float readFloat() {
      return delegate.readFloat();
    }

    @Override
    public short readShort() {
      return delegate.readShort();
    }

    @Override
    public BigInteger readBigInteger() {
      return delegate.readBigInteger();
    }

    @Override
    public String readUtf8() {
      return delegate.readUtf8();
    }

    @Override
    public long readStopBit() {
      return delegate.readStopBit();
    }

    @Override
    public void read(final byte[] bytes) {
      delegate.read(bytes);
    }

    @Override
    public void read(final byte[] bytes, final int offset, final int length) {
      delegate.read(bytes, offset, length);
    }

    @Override
    public long position() {
      return delegate.position();
    }

    @Override
    public void position(final long newPosition) {
      delegate.position(newPosition);
    }

    @Override
    public void skip(final long bytes) {
      delegate.skip(bytes);
    }

    @Override
    public long remaining() {
      return delegate.remaining();
    }

    @Override
    public byte[] getSource() {
      return delegate.getSource();
    }

    @Override
    public byte[] getUnderlying() {
      return delegate.getUnderlying();
    }

    @Override
    public InputStream inputStream() {
      return delegate.inputStream();
    }

    @Override
    public byte[] toByteArray() {
      return delegate.toByteArray();
    }
  }

  private static byte[] key(final int i) {
    return new byte[] {(byte) (i >>> 4), 5, 7, 9, 11, 13, (byte) i, 19};
  }

  private static byte[] posting(final int first, final int second) {
    return ByteBuffer.allocate(18).put((byte) 0).put((byte) 2).putLong(first).putLong(second).array();
  }

  private static byte[] write(final ResourceConfiguration config, final HOTLeafPage leaf, final boolean raw)
      throws Exception {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      if (raw) {
        PageKind.serializeHOTLeaf(config, sink, leaf, false);
      } else {
        PageKind.HOT_LEAF_PAGE.serializePage(config, sink, leaf, SerializationType.DATA);
      }
      return sink.toByteArray();
    }
  }

  private static HOTLeafPage read(final byte[] frame) throws Exception {
    return (HOTLeafPage) new PagePersister().deserializePage(FULL, new ByteArrayBytesIn(frame), SerializationType.DATA);
  }

  private static void assertProjection(final byte[] frame, final byte[] expected) throws Exception {
    final byte[] key = {9};
    try (HOTLeafPage leaf = read(frame)) {
      assertArrayEquals(expected, leaf.getValue(leaf.findEntry(key)));
      assertArrayEquals(expected, leaf.getValueSlice(leaf.findEntry(key)).toArray(ValueLayout.JAVA_BYTE));
      // An encoded image is immutable: mutate a copy.
      assertImmutable(leaf::compact);
    }
    final ByteArrayBytesIn source = new ByteArrayBytesIn(frame);
    source.position(1);
    assertArrayEquals(expected, PageKind.deserializeHOTProjectionEntry(source, key, 0).value());
    source.position(1);
    final HOTProjectionImage image = HOTProjectionImage.copyIfFits(source);
    assertArrayEquals(expected, image.select(key, 0).value());
    image.prepareForCache();
    assertArrayEquals(expected, image.select(key, 0).value());
    assertNull(image.select(new byte[] {8}, 0));
    source.position(1);
    try (HOTLeafPage leaf = PageKind.deserializeHOTLeafFragment(source)) {
      assertArrayEquals(expected, leaf.getValue(leaf.findEntry(key)));
      assertImmutable(leaf::compact);
    }
  }

  /** Immutable images refuse mutation: main's assertion under -ea, IllegalStateException without. */
  private static void assertImmutable(final Executable mutation) {
    final Throwable refusal = assertThrows(Throwable.class, mutation);
    assertTrue(refusal instanceof IllegalStateException || refusal instanceof AssertionError, refusal.toString());
  }
}

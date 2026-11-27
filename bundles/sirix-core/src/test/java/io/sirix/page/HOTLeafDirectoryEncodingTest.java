package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafBody;
import io.sirix.index.hot.page.HOTLeafEntry;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class HOTLeafDirectoryEncodingTest {
  private static final ResourceConfiguration CONFIG =
      new ResourceConfiguration.Builder("hot-directory").versioningApproach(VersioningType.FULL).build();
  private static final byte[] EMPTY = HexFormat.of().parseHex("0c000007030000000a00000000000000000000");
  private static final byte[] BETA_WIDE =
      HexFormat.of().parseHex("0c000007030000000a0000020000000c000000000000000600000001000701002a010009010033");
  private static final byte[] NARROW =
      HexFormat.of().parseHex("0c000407030000000a0000020000000c0000000000060001000701002a010009010033");

  @BeforeAll
  static void allocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @Test
  void modeZeroGoldenFrameUsesUnsignedShortDirectory() throws Exception {
    assertLookupContract(NARROW, -1L);

    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      assertTrue(leaf.put(new byte[] {7}, new byte[] {42}));
      assertTrue(leaf.put(new byte[] {9}, new byte[] {51}));
      PageKind.serializeHOTLeaf(CONFIG, sink, leaf, false);
      assertArrayEquals(NARROW, sink.toByteArray());
    }
  }

  @Test
  void ordinaryNarrowBodiesPackFullAndSparseDirectories() throws Exception {
    final ResourceConfiguration incremental =
        new ResourceConfiguration.Builder("hot-directory-incremental").versioningApproach(VersioningType.INCREMENTAL)
                                                                      .build();
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
        HOTLeafPage complete = new HOTLeafPage(7, 2, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      assertTrue(leaf.put(new byte[] {7}, new byte[16]));
      assertTrue(leaf.put(new byte[] {9}, new byte[] {51}));
      assertTrue(leaf.updateValue(0, new byte[] {42}));
      final int usedBefore = leaf.getUsedSlotsSize();
      final int secondOffsetBefore = leaf.getSlotOffset(1);
      assertTrue(secondOffsetBefore > 6, "fixture must leave a retired range between the two slots");
      PageKind.serializeHOTLeaf(CONFIG, sink, leaf, false);
      assertArrayEquals(NARROW, sink.toByteArray());
      assertEquals(usedBefore, leaf.getUsedSlotsSize());
      assertEquals(secondOffsetBefore, leaf.getSlotOffset(1));

      leaf.setCompletePageRef(complete);
      leaf.clearDirtyBitmap();
      assertTrue(leaf.updateValue(1, new byte[] {51}));
      leaf.setCompleteDump(true);
      sink.clear();
      PageKind.serializeHOTLeaf(incremental, sink, leaf, false);
      final byte[] sparse = HexFormat.of().parseHex("0c000407030000000a000001000080060000000000010009010033");
      assertArrayEquals(sparse, sink.toByteArray());
      try (HOTLeafPage decoded = read(sparse)) {
        assertTrue(decoded.isCompleteDump());
        assertEquals(1, decoded.getEntryCount());
        assertArrayEquals(new byte[] {51}, decoded.getValue(decoded.findEntry(new byte[] {9})));
      }
    }
  }

  @Test
  void emptyLeafOmitsDirectoryAndNonemptyLeafRetainsSideReferences() throws Exception {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, sink, leaf, SerializationType.DATA);
      assertArrayEquals(EMPTY, sink.toByteArray());
      try (HOTLeafPage empty = read(EMPTY)) {
        assertEquals(0, empty.getEntryCount());
      }
      assertNull(select(EMPTY, (byte) 7));
      final ByteArrayBytesIn source = new ByteArrayBytesIn(EMPTY);
      source.position(1);
      assertNull(HOTProjectionImage.copyIfFits(source).select(new byte[] {7}, 123L));
      assertTrue(leaf.put(new byte[] {7}, new byte[] {42}));
      assertTrue(leaf.put(new byte[] {9}, new byte[] {51}));
      leaf.setPageReference(123L, new PageReference().setKey(888L));
      sink.clear();
      PageKind.serializeHOTLeaf(CONFIG, sink, leaf, false);
      final byte[] narrow = sink.toByteArray();
      assertEquals(5, narrow[2]);
      assertLookupContract(narrow, 888L);
    }
  }

  @Test
  void upperHalfOfNarrowOffsetDomainIsUnsigned() throws Exception {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      sink.write(NARROW, 0, 15);
      sink.writeInt(65_536);
      sink.writeShort((short) 0);
      sink.writeShort((short) 65_530);
      sink.writeShort((short) 1);
      sink.writeByte((byte) 7);
      sink.writeShort((short) 65_525);
      sink.write(new byte[65_525]);
      sink.write(new byte[] {1, 0, 9, 1, 0, 51});
      final byte[] frame = sink.toByteArray();
      final ByteArrayBytesIn source = new ByteArrayBytesIn(frame);
      try (HOTLeafPage leaf =
          (HOTLeafPage) new PagePersister().deserializePage(CONFIG, source, SerializationType.DATA)) {
        assertArrayEquals(new byte[] {51}, leaf.getValue(leaf.findEntry(new byte[] {9})));
        sink.clear();
        PageKind.serializeHOTLeaf(CONFIG, sink, leaf, false);
        assertArrayEquals(frame, sink.toByteArray());
      }
      assertArrayEquals(new byte[] {51}, select(frame, (byte) 9).value());
    }
  }

  @Test
  void legacyWideAndMismatchedDirectoryFlagsAreRefused() {
    final byte[] nonemptyWithoutDirectoryFlag = NARROW.clone();
    nonemptyWithoutDirectoryFlag[2] = 0;
    final byte[] emptyWithDirectoryFlag = EMPTY.clone();
    emptyWithDirectoryFlag[2] = 4;
    for (final byte[] frame : new byte[][] {BETA_WIDE, nonemptyWithoutDirectoryFlag, emptyWithDirectoryFlag}) {
      assertMalformedFrame(frame);
    }
  }

  @Test
  void modeZeroAcceptsPhysicalGapsAndReorderedSlotStarts() throws Exception {
    final byte[] frame =
        HexFormat.of().parseHex("0c000407030000000a0000020000000e00000008000000010009010033000001000701002a");
    assertLookupContract(frame, -1L);
  }

  @Test
  void modeZeroRejectsTruncationAndTrailingBytes() {
    for (int length = 8; length < NARROW.length; length++) {
      assertMalformedFrame(Arrays.copyOf(NARROW, length));
    }
    assertMalformedFrame(Arrays.copyOf(NARROW, NARROW.length + 1));
    assertMalformedFrame(Arrays.copyOf(EMPTY, EMPTY.length + 1));
  }

  @Test
  void modeZeroRejectsInvalidOffsetsAndSelectedSlotLengths() {
    final byte[] offset = NARROW.clone();
    offset[19] = 13;
    assertThrows(IllegalStateException.class, () -> read(offset));
    assertThrows(IllegalStateException.class, () -> select(offset, (byte) 7));

    final byte[] slot = NARROW.clone();
    slot[23] = 13;
    assertThrows(IllegalStateException.class, () -> select(slot, (byte) 7));
  }

  @Test
  void sideMapRequiresStrictlyAscendingKeysAndDurableOffsets() throws Exception {
    assertLookupContract(withSideMap(new byte[] {2}, Long.MIN_VALUE, 123L, 123L, 888L), 888L);
    assertMalformedFrame(withSideMap(new byte[] {2}, 123L, 888L, 123L, 999L));
    assertMalformedFrame(withSideMap(new byte[] {2}, 123L, 888L, -1L, 999L));
    assertMalformedFrame(withSideMap(new byte[] {1}, 123L, -1L));
  }

  @Test
  void stopBitFieldsRefuseEncodingsWiderThanTheirWireDomains() {
    final byte continuation = (byte) 0x80;
    final byte[] overwidePageKey = new byte[NARROW.length + 10];
    System.arraycopy(NARROW, 0, overwidePageKey, 0, 3);
    overwidePageKey[3] = (byte) 0x87;
    Arrays.fill(overwidePageKey, 4, 13, continuation);
    System.arraycopy(NARROW, 4, overwidePageKey, 14, NARROW.length - 4);
    assertMalformedFrame(overwidePageKey);

    final byte[] overwideCount = {(byte) 0x81, continuation, continuation, continuation, continuation, 0};
    assertMalformedFrame(withSideMap(overwideCount, 123L, 888L));
  }

  /** FMT-211: the page key is a u64 stop-bit field of up to ten groups, least significant first. */
  @Test
  void pageKeyDecodesAtEveryGroupCountAndMatchesTheWriter() throws Exception {
    final long[] keys = new long[19];
    for (int groups = 1; groups <= 9; groups++) {
      keys[2 * groups - 2] = (1L << (7 * groups - 7)) - 1;
      keys[2 * groups - 1] = groups == 9
          ? Long.MAX_VALUE
          : 1L << (7 * groups);
    }
    keys[18] = (1L << 56) | 1L;
    for (final long key : keys) {
      final byte[] frame = withPageKey(stopBit(key));
      assertLookupContract(frame, -1L);
      try (HOTLeafPage decoded = read(frame);
          HOTLeafPage leaf = new HOTLeafPage(key, 3, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
        assertEquals(key, decoded.getPageKey());
        assertTrue(leaf.put(new byte[] {7}, new byte[] {42}));
        assertTrue(leaf.put(new byte[] {9}, new byte[] {51}));
        PageKind.serializeHOTLeaf(CONFIG, sink, leaf, false);
        assertArrayEquals(frame, sink.toByteArray(), "writer encoding of page key " + key);
      }
    }
  }

  /** FMT-211: a field is valid when it terminates within its width, including in the tenth group. */
  @Test
  void pageKeyAcceptsGroupsPaddedWithinTenBytes() throws Exception {
    for (final String padded : new String[] {"8700", "87808080808080808000"}) {
      final byte[] frame = withPageKey(HexFormat.of().parseHex(padded));
      assertLookupContract(frame, -1L);
      try (HOTLeafPage decoded = read(frame)) {
        assertEquals(7L, decoded.getPageKey());
      }
    }
    try (HOTLeafPage decoded = read(withPageKey(HexFormat.of().parseHex("ffffffffffffffffff01")))) {
      assertEquals(-1L, decoded.getPageKey(), "the tenth group carries bit 63 of the u64 domain");
    }
    assertMalformedFrame(withPageKey(HexFormat.of().parseHex("ffffffffffffffffff02")));
  }

  /**
   * FMT-211 caps the side-reference count at MAX_ENTRIES owners times 65,536 sub-ids. A trailer of
   * exactly that many pairs is accepted, and one more pair is refused even with every byte present.
   * The 512 MiB trailers live in sparse mapped files, so neither memory nor disk holds their span.
   */
  @Test
  void sideReferenceCountAcceptsTheFormatMaximumAndRefusesOneMore(@TempDir final Path directory) throws IOException {
    final int maximum = HOTLeafBody.MAX_SIDE_REFERENCES;
    assertEquals(maximum, sideReferenceCount(directory.resolve("maximum"), maximum));
    assertThrows(IllegalStateException.class, () -> sideReferenceCount(directory.resolve("over"), maximum + 1L));
  }

  /** Read the count of a trailer that declares {@code count} pairs and holds exactly their bytes. */
  private static int sideReferenceCount(final Path file, final long count) throws IOException {
    final byte[] header = stopBit(count);
    final long size = header.length + count * 2 * Long.BYTES;
    try (FileChannel channel = FileChannel.open(file, CREATE_NEW, SPARSE, READ, WRITE);
        Arena arena = Arena.ofConfined()) {
      channel.write(ByteBuffer.wrap(header), 0);
      channel.write(ByteBuffer.wrap(new byte[1]), size - 1);
      final MemorySegment trailer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
      return HOTLeafBody.readSideReferenceCount(new MemorySegmentBytesIn(trailer));
    }
  }

  /**
   * An ordinary decoded leaf is a writable leaf: it accepts inserts up to MAX_ENTRIES. A compact
   * fragment is an immutable cached image and refuses compaction (writers mutate a copy).
   */
  @Test
  void decodedLeafAcceptsInsertsToCapacityAndCompactFragmentStaysImmutable() throws Exception {
    try (HOTLeafPage leaf = read(NARROW)) {
      for (int i = leaf.getEntryCount(); i < HOTLeafPage.MAX_ENTRIES; i++) {
        assertTrue(leaf.put(new byte[] {10, (byte) (i >>> 8), (byte) i}, new byte[] {1}), "insert " + i);
      }
      assertEquals(HOTLeafPage.MAX_ENTRIES, leaf.getEntryCount());
      assertArrayEquals(new byte[] {42}, leaf.getValue(leaf.findEntry(new byte[] {7})));
    }
    final ByteArrayBytesIn source = new ByteArrayBytesIn(NARROW);
    source.position(1);
    try (HOTLeafPage fragment = PageKind.deserializeHOTLeafFragment(source)) {
      assertImmutable(fragment::compact);
      assertArrayEquals(new byte[] {51}, fragment.getValue(fragment.findEntry(new byte[] {9})));
    }
  }

  /** FMT-211 rejects only negative side-map page offsets; offset zero is in the domain. */
  @Test
  void sideMapAcceptsPageOffsetZero() throws Exception {
    assertLookupContract(withSideMap(new byte[] {1}, 123L, 0L), 0L);
  }

  /** FMT-212 admits an empty suffix with an empty value; FMT-211 lets it end the slot region. */
  @Test
  void emptySuffixAndValueSlotMayEndTheSlotRegion() throws Exception {
    final byte[] frame =
        HexFormat.of().parseHex("0c000407030000000a0000020000000a00000006000000010009010033" + "00000000");
    try (HOTLeafPage leaf = read(frame)) {
      assertEquals(2, leaf.getEntryCount());
      assertArrayEquals(new byte[0], leaf.getKey(0));
      assertArrayEquals(new byte[] {9}, leaf.getKey(1));
      assertEquals(0, leaf.findEntry(new byte[0]));
      assertNoValueBytes(leaf.getValue(0));
      assertArrayEquals(new byte[] {51}, leaf.getValue(1));
    }
    assertNoValueBytes(selectKey(frame, new byte[0]).value());
    assertArrayEquals(new byte[] {51}, select(frame, (byte) 9).value());
    final ByteArrayBytesIn image = new ByteArrayBytesIn(frame);
    image.position(1);
    assertNoValueBytes(HOTProjectionImage.copyIfFits(image).select(new byte[0], 0).value());
  }

  /** The in-memory API may represent a present empty value as null or as an empty array. */
  private static void assertNoValueBytes(final byte[] value) {
    assertTrue(value == null || value.length == 0, "an empty stored value must yield no bytes");
  }

  @Test
  void experimentalAndUnknownEnvelopeFlagsAreRejected() {
    for (final byte flags : new byte[] {2, 6, 8}) {
      final byte[] frame = NARROW.clone();
      frame[2] = flags;
      assertMalformedFrame(frame);
    }
  }

  /** Spec oracle for FMT-211 stop-bit groups, independent of the engine's writer. */
  private static byte[] stopBit(final long value) {
    final byte[] groups = new byte[10];
    long rest = value;
    int length = 0;
    while ((rest & ~0x7fL) != 0) {
      groups[length++] = (byte) (rest & 0x7f | 0x80);
      rest >>>= 7;
    }
    groups[length++] = (byte) rest;
    return Arrays.copyOf(groups, length);
  }

  /** NARROW with its one-byte page key {@code 07} at offset 3 replaced. */
  private static byte[] withPageKey(final byte[] pageKey) {
    final byte[] frame = new byte[NARROW.length - 1 + pageKey.length];
    System.arraycopy(NARROW, 0, frame, 0, 3);
    System.arraycopy(pageKey, 0, frame, 3, pageKey.length);
    System.arraycopy(NARROW, 4, frame, 3 + pageKey.length, NARROW.length - 4);
    return frame;
  }

  private static byte[] withSideMap(final byte[] count, final long... pairs) {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      final byte[] frame = NARROW.clone();
      frame[2] = 5;
      sink.write(frame);
      sink.write(count);
      for (final long word : pairs) {
        sink.writeLong(word);
      }
      return sink.toByteArray();
    }
  }

  private static void assertMalformedFrame(final byte[] frame) {
    for (final BytesIn<?> input : new BytesIn<?>[] {new ByteArrayBytesIn(frame),
        new MemorySegmentBytesIn(MemorySegment.ofArray(frame))}) {
      assertThrows(RuntimeException.class, () -> {
        try (HOTLeafPage decoded =
            (HOTLeafPage) new PagePersister().deserializePage(CONFIG, input, SerializationType.DATA)) {
          decoded.getEntryCount();
        }
      });
      input.position(1);
      assertThrows(RuntimeException.class, () -> PageKind.deserializeHOTProjectionEntry(input, new byte[] {7}, 123L));
      input.position(1);
      assertThrows(RuntimeException.class, () -> HOTProjectionImage.copyIfFits(input));
    }
  }

  private static void assertLookupContract(final byte[] frame, final long sideKey) throws Exception {
    try (HOTLeafPage leaf = read(frame)) {
      assertEquals(2, leaf.getEntryCount());
      assertArrayEquals(new byte[] {7}, leaf.getKey(0));
      assertArrayEquals(new byte[] {9}, leaf.getKey(1));
      assertArrayEquals(new byte[] {42}, leaf.getValue(leaf.findEntry(new byte[] {7})));
      assertArrayEquals(new byte[] {51}, leaf.getValue(leaf.findEntry(new byte[] {9})));
    }
    final HOTLeafEntry selected = select(frame, (byte) 7);
    assertArrayEquals(new byte[] {42}, selected.value());
    if (sideKey < 0) {
      assertNull(selected.sideReference());
    } else {
      assertEquals(sideKey, selected.sideReference().getKey());
    }
    assertNull(select(frame, (byte) 8));
    final ByteArrayBytesIn heapFrame = new ByteArrayBytesIn(frame);
    heapFrame.position(1);
    assertArrayEquals(new byte[] {51}, HOTProjectionImage.copyIfFits(heapFrame).select(new byte[] {9}, 0).value());
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment bytes = arena.allocateFrom(ValueLayout.JAVA_BYTE, frame);
      final MemorySegmentBytesIn input = new MemorySegmentBytesIn(bytes);
      input.position(1);
      try (HOTLeafPage fragment = PageKind.deserializeHOTLeafFragment(input)) {
        assertArrayEquals(new byte[] {51}, fragment.getValue(fragment.findEntry(new byte[] {9})));
        assertEquals(frame.length, input.position());
      }
    }
  }

  private static HOTLeafPage read(final byte[] frame) throws Exception {
    return (HOTLeafPage) new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(frame),
        SerializationType.DATA);
  }

  private static HOTLeafEntry select(final byte[] frame, final byte key) {
    return selectKey(frame, new byte[] {key});
  }

  private static HOTLeafEntry selectKey(final byte[] frame, final byte[] key) {
    final ByteArrayBytesIn input = new ByteArrayBytesIn(frame);
    input.position(1);
    return PageKind.deserializeHOTProjectionEntry(input, key, 123L);
  }

  /** Immutable images refuse mutation: main's assertion under -ea, IllegalStateException without. */
  private static void assertImmutable(final Executable mutation) {
    final Throwable refusal = assertThrows(Throwable.class, mutation);
    assertTrue(refusal instanceof IllegalStateException || refusal instanceof AssertionError, refusal.toString());
  }
}

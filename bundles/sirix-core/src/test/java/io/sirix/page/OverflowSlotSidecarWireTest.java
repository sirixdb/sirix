/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.Stream;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Wire-contract tests for the sparse logical-slot companions of record overflow references. */
@DisplayName("Overflow-slot sidecar wire format")
final class OverflowSlotSidecarWireTest {

  private static final long PAGE_KEY = 5L;
  private static final long PAGE_BASE = PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT;
  private static final int COMPLETE_BOOLEAN_SLOT = 7;
  private static final int STRING_DESCRIPTOR_SLOT = 31;
  private static final int NUMBER_DESCRIPTOR_SLOT = 511;
  private static final int BOOLEAN_NAME_KEY = 101;
  private static final int STRING_NAME_KEY = 102;
  private static final int NUMBER_NAME_KEY = 103;
  private static final long BOOLEAN_PARENT_KEY = 2_001L;
  private static final long STRING_PARENT_KEY = 2_002L;
  private static final long NUMBER_PARENT_KEY = 2_003L;
  private static final long BOOLEAN_PATH_KEY = 3_001L;
  private static final long STRING_PATH_KEY = 3_002L;
  private static final long NUMBER_PATH_KEY = 3_003L;
  private static final byte OVERFLOW_SIDECAR_ENVELOPE_FLAG = 0x02;
  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(256L * 1024 * 1024);
  }

  @ParameterizedTest(name = "chunked={0}")
  @ValueSource(booleans = {false, true})
  @DisplayName("complete and metadata-only fused images round-trip")
  void fusedSideImagesRoundTrip(final boolean chunked) {
    final ResourceConfiguration config = config();
    final SidecarFixture fixture = fixture(config);
    final byte[] wire;
    try {
      wire = serialize(config, fixture.page(), chunked);
    } finally {
      fixture.page().close();
    }

    assertTrue((wire[2] & OVERFLOW_SIDECAR_ENVELOPE_FLAG) != 0, "the envelope did not announce the sidecar tail");
    assertEquals(chunked, (wire[2] & ChunkedBodyConfig.FLAG_CHUNKED_BODY) != 0,
        "the body-shape flag disagrees with the requested writer");

    final KeyValueLeafPage read = deserialize(config, wire);
    final RegionsOnlyPage regionsOnly = deserializeRegionsOnly(config, wire);
    try {
      assertEquals(3, read.getSideSlotCount());
      assertSideImage(read, COMPLETE_BOOLEAN_SLOT, NodeKind.OBJECT_NAMED_BOOLEAN.getId(), fixture.booleanImage());
      assertSideImage(read, STRING_DESCRIPTOR_SLOT, NodeKind.OBJECT_NAMED_STRING.getId(), fixture.stringDescriptor());
      assertSideImage(read, NUMBER_DESCRIPTOR_SLOT, NodeKind.OBJECT_NAMED_NUMBER.getId(), fixture.numberDescriptor());

      assertEquals(BOOLEAN_PARENT_KEY, read.getSlotParentKey(COMPLETE_BOOLEAN_SLOT));
      assertEquals(BOOLEAN_NAME_KEY, read.getObjectKeyNameKeyFromSlot(COMPLETE_BOOLEAN_SLOT));
      assertEquals(BOOLEAN_PATH_KEY,
          read.getObjectKeyPathNodeKeyFromSlot(COMPLETE_BOOLEAN_SLOT, nodeKey(COMPLETE_BOOLEAN_SLOT)));
      assertTrue(read.getFusedObjectNamedBooleanValueFromSlot(COMPLETE_BOOLEAN_SLOT));

      assertEquals(STRING_PARENT_KEY, read.getSlotParentKey(STRING_DESCRIPTOR_SLOT));
      assertEquals(STRING_NAME_KEY, read.getObjectKeyNameKeyFromSlot(STRING_DESCRIPTOR_SLOT));
      assertEquals(STRING_PATH_KEY,
          read.getObjectKeyPathNodeKeyFromSlot(STRING_DESCRIPTOR_SLOT, nodeKey(STRING_DESCRIPTOR_SLOT)));
      assertTrue(read.isFusedObjectNamedStringOverflowDescriptor(STRING_DESCRIPTOR_SLOT));

      assertEquals(NUMBER_PARENT_KEY, read.getSlotParentKey(NUMBER_DESCRIPTOR_SLOT));
      assertEquals(NUMBER_NAME_KEY, read.getObjectKeyNameKeyFromSlot(NUMBER_DESCRIPTOR_SLOT));
      assertEquals(NUMBER_PATH_KEY,
          read.getObjectKeyPathNodeKeyFromSlot(NUMBER_DESCRIPTOR_SLOT, nodeKey(NUMBER_DESCRIPTOR_SLOT)));
      assertEquals(Long.MIN_VALUE, read.getFusedObjectNamedNumberValueLongFromSlot(NUMBER_DESCRIPTOR_SLOT),
          "the reserved descriptor marker must force the authoritative OverflowPage fallback");

      assertArrayEquals(new int[] {COMPLETE_BOOLEAN_SLOT}, read.getObjectKeySlotsForNameKey(BOOLEAN_NAME_KEY));
      assertArrayEquals(new int[] {STRING_DESCRIPTOR_SLOT}, read.getObjectKeySlotsForNameKey(STRING_NAME_KEY));
      assertArrayEquals(new int[] {NUMBER_DESCRIPTOR_SLOT}, read.getObjectKeySlotsForNameKey(NUMBER_NAME_KEY));

      assertEquals(3, regionsOnly.getPopulatedSlotCount(),
          "column-only decoding must count the logical side slots, not just the empty row heap");
      assertTrue(regionsOnly.definesSlot(COMPLETE_BOOLEAN_SLOT));
      assertTrue(regionsOnly.definesSlot(STRING_DESCRIPTOR_SLOT));
      assertTrue(regionsOnly.definesSlot(NUMBER_DESCRIPTOR_SLOT));
      assertFalse(regionsOnly.hasCompleteColumnCoverage(),
          "overflow values are absent from the PAX regions and require record fallback");
    } finally {
      read.close();
      regionsOnly.close();
    }
  }

  @ParameterizedTest(name = "{0}, chunked={1}")
  @MethodSource("malformedSidecars")
  @DisplayName("malformed sidecar headers and kinds are rejected")
  void malformedSidecarIsRejected(final Corruption corruption, final boolean chunked) {
    final ResourceConfiguration config = config();
    final SidecarFixture fixture = fixture(config);
    final byte[] wire;
    try {
      wire = serialize(config, fixture.page(), chunked);
    } finally {
      fixture.page().close();
    }

    final SidecarOffsets offsets = locateSidecar(wire);
    corruption.apply(wire, offsets);

    final SirixIOException exception = assertThrows(SirixIOException.class, () -> deserialize(config, wire));
    assertTrue(exception.getMessage().contains(corruption.expectedMessage),
        () -> "unexpected diagnostic: " + exception.getMessage());
  }

  private static Stream<Arguments> malformedSidecars() {
    return Arrays.stream(Corruption.values())
                 .flatMap(corruption -> Stream.of(Arguments.of(corruption, false), Arguments.of(corruption, true)));
  }

  private static void assertSideImage(final KeyValueLeafPage page, final int slot, final int kindId,
      final byte[] expectedImage) {
    assertTrue(page.hasSideSlot(slot));
    assertNull(page.getSlot(slot), "a logical side slot must not also occupy the row heap");
    assertEquals(kindId, page.getSideSlotNodeKindId(slot));
    assertEquals(kindId, page.getSlotNodeKindId(slot));
    final MemorySegment image = page.getSideSlotImage(slot);
    assertNotNull(image);
    assertArrayEquals(expectedImage, image.toArray(ValueLayout.JAVA_BYTE));
    final PageReference reference = page.getPageReference(nodeKey(slot));
    assertNotNull(reference, "every side image needs a same-key overflow reference");
    assertEquals(referenceKey(slot), reference.getKey());
  }

  private static SidecarFixture fixture(final ResourceConfiguration config) {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 3, null, null, false);

    final ObjectNamedBooleanNode booleanNode = new ObjectNamedBooleanNode(nodeKey(COMPLETE_BOOLEAN_SLOT),
        BOOLEAN_PARENT_KEY, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        BOOLEAN_NAME_KEY, BOOLEAN_PATH_KEY, 1, 3, 17L, true, HASH_FUNCTION, (byte[]) null);
    final byte[] booleanImage = completeImage(booleanNode);

    final ObjectNamedStringNode stringNode = new ObjectNamedStringNode(nodeKey(STRING_DESCRIPTOR_SLOT),
        STRING_PARENT_KEY, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        STRING_NAME_KEY, STRING_PATH_KEY, 1, 3, 19L, "overflow".getBytes(StandardCharsets.UTF_8), HASH_FUNCTION,
        (byte[]) null, false, null);
    final byte[] stringDescriptor = stringDescriptor(stringNode);

    final ObjectNamedNumberNode numberNode = new ObjectNamedNumberNode(nodeKey(NUMBER_DESCRIPTOR_SLOT),
        NUMBER_PARENT_KEY, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        NUMBER_NAME_KEY, NUMBER_PATH_KEY, 1, 3, 23L, BigInteger.ONE.shiftLeft(4096), HASH_FUNCTION, (byte[]) null);
    final byte[] numberDescriptor = numberDescriptor(numberNode);

    installSideImage(page, COMPLETE_BOOLEAN_SLOT, NodeKind.OBJECT_NAMED_BOOLEAN.getId(), booleanImage);
    installSideImage(page, STRING_DESCRIPTOR_SLOT, NodeKind.OBJECT_NAMED_STRING.getId(), stringDescriptor);
    installSideImage(page, NUMBER_DESCRIPTOR_SLOT, NodeKind.OBJECT_NAMED_NUMBER.getId(), numberDescriptor);
    return new SidecarFixture(page, booleanImage, stringDescriptor, numberDescriptor);
  }

  private static byte[] completeImage(final ObjectNamedBooleanNode node) {
    final byte[] scratch = new byte[PageConstants.MAX_RECORD_SIZE];
    final int length = node.serializeToHeap(MemorySegment.ofArray(scratch), 0L);
    return Arrays.copyOf(scratch, length);
  }

  private static byte[] stringDescriptor(final ObjectNamedStringNode node) {
    final byte[] scratch = new byte[PageConstants.MAX_RECORD_SIZE];
    final int length = node.serializeOverflowDescriptorToHeap(MemorySegment.ofArray(scratch), 0L);
    return Arrays.copyOf(scratch, length);
  }

  private static byte[] numberDescriptor(final ObjectNamedNumberNode node) {
    final byte[] scratch = new byte[PageConstants.MAX_RECORD_SIZE];
    final int length = node.serializeOverflowDescriptorToHeap(MemorySegment.ofArray(scratch), 0L);
    return Arrays.copyOf(scratch, length);
  }

  private static void installSideImage(final KeyValueLeafPage page, final int slot, final int kindId,
      final byte[] image) {
    final PageReference reference = new PageReference();
    reference.setKey(referenceKey(slot));
    page.setPageReference(nodeKey(slot), reference);
    final long token = page.prepareSideSlot(kindId, MemorySegment.ofArray(image), image.length);
    page.publishSideSlot(slot, token);
  }

  private static byte[] serialize(final ResourceConfiguration config, final KeyValueLeafPage page,
      final boolean chunked) {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunked);
    try {
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }

  private static KeyValueLeafPage deserialize(final ResourceConfiguration config, final byte[] wire) {
    final BytesIn<?> source = Bytes.wrapForRead(wire);
    source.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
  }

  private static RegionsOnlyPage deserializeRegionsOnly(final ResourceConfiguration config, final byte[] wire) {
    final BytesIn<?> source = Bytes.wrapForRead(wire);
    source.readByte();
    return PageKind.KEYVALUELEAFPAGE.deserializeRegionsOnlyPage(config, source, RegionTable.ALL_KINDS, 0);
  }

  private static SidecarOffsets locateSidecar(final byte[] wire) {
    final BytesIn<?> source = Bytes.wrapForRead(wire);
    source.readByte();
    final long regionTableOffset =
        PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(source, new long[5], new long[PageLayout.BITMAP_WORDS]);
    source.position(regionTableOffset);
    try (RegionTable ignored = RegionTable.read(source, RegionTable.ALL_KINDS, 0)) {
      final BitSet references = SerializationType.deserializeBitSet(source);
      final int referenceCount = source.readInt();
      assertEquals(referenceCount, references.cardinality());
      source.skip((long) referenceCount * Long.BYTES);

      final BitSet sideSlots = SerializationType.deserializeBitSet(source);
      assertEquals(3, sideSlots.cardinality());
      final int countOffset = Math.toIntExact(source.position());
      assertEquals(3, source.readInt());
      final int payloadLengthOffset = Math.toIntExact(source.position());
      assertTrue(source.readInt() > 0);
      final int firstKindOffset = Math.toIntExact(source.position());
      return new SidecarOffsets(countOffset, payloadLengthOffset, firstKindOffset);
    }
  }

  private static ResourceConfiguration config() {
    return new ResourceConfiguration.Builder("overflow-sidecar-wire").build();
  }

  private static long nodeKey(final int slot) {
    return PAGE_BASE + slot;
  }

  private static long referenceKey(final int slot) {
    return 10_000L + slot;
  }

  private record SidecarFixture(KeyValueLeafPage page, byte[] booleanImage, byte[] stringDescriptor,
      byte[] numberDescriptor) {
  }

  private record SidecarOffsets(int count, int payloadLength, int firstKind) {
  }

  private enum Corruption {
    COUNT("bitmap/count mismatch") {
      @Override
      void apply(final byte[] wire, final SidecarOffsets offsets) {
        Arrays.fill(wire, offsets.count(), offsets.count() + Integer.BYTES, (byte) 0);
      }
    },
    PAYLOAD_LENGTH("invalid overflow-sidecar payload length") {
      @Override
      void apply(final byte[] wire, final SidecarOffsets offsets) {
        Arrays.fill(wire, offsets.payloadLength(), offsets.payloadLength() + Integer.BYTES, (byte) 0xFF);
      }
    },
    KIND("unsupported node kind") {
      @Override
      void apply(final byte[] wire, final SidecarOffsets offsets) {
        wire[offsets.firstKind()] = 47;
      }
    };

    private final String expectedMessage;

    Corruption(final String expectedMessage) {
      this.expectedMessage = expectedMessage;
    }

    abstract void apply(byte[] wire, SidecarOffsets offsets);
  }
}

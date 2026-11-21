package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import java.lang.foreign.Arena;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionOverflowEncodingTest {
  private static final ResourceConfiguration CONFIG = new ResourceConfiguration.Builder("projection-overflow").build();
  private static final byte[] GOLDEN = HexFormat.of().parseHex("090003400000000300000000ff40be");

  @Test
  void projectionThresholdIncludesTheOuterFrame() {
    assertTrue(PageKind.keepsProjectionOverflowCodec(100, 74));
    assertTrue(PageKind.keepsProjectionOverflowCodec(100, 75));
    assertFalse(PageKind.keepsProjectionOverflowCodec(100, 76));
    assertFalse(PageKind.keepsProjectionOverflowCodec(100, Integer.MAX_VALUE));
  }

  @Test
  void projectionMarkerAndOuterCodecRoundTripIndependently() throws Exception {
    final OverflowPage golden = read(GOLDEN);
    assertTrue(golden.isProjectionPayload());
    assertArrayEquals(new byte[64], golden.getDataBytes());
    final byte[] opaque = {1, 2, 3, 4};
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut()) {
      PageKind.OVERFLOWPAGE.serializePage(CONFIG, out, OverflowPage.forProjectionPayload(opaque),
          SerializationType.DATA);
      assertArrayEquals(HexFormat.of().parseHex("0900020400000001020304"), out.toByteArray());
      final OverflowPage typed = read(out.toByteArray());
      assertTrue(typed.isProjectionPayload());
      assertArrayEquals(opaque, typed.getDataBytes());
      out.clear();
      PageKind.OVERFLOWPAGE.serializePage(CONFIG, out, new OverflowPage(opaque), SerializationType.DATA);
      assertArrayEquals(HexFormat.of().parseHex("0900000400000001020304"), out.toByteArray());
      final OverflowPage ordinary = read(out.toByteArray());
      assertFalse(ordinary.isProjectionPayload());
      assertArrayEquals(opaque, ordinary.getDataBytes());
    }
  }

  @Test
  void stagedProjectionPayloadKeepsItsModeAndQualifyingCompression() throws Exception {
    final byte[] payload = new byte[512];
    final OverflowPage source = OverflowPage.forProjectionPayload(payload);
    try (Arena arena = Arena.ofConfined(); MemorySegmentBytesOut out = new MemorySegmentBytesOut()) {
      final OverflowPage staged = source.stagedView(arena.allocate(521).asReadOnly(), 9);
      assertTrue(staged.isProjectionPayload());
      PageKind.OVERFLOWPAGE.serializePage(CONFIG, out, staged, SerializationType.DATA);
      final byte[] wire = out.toByteArray();
      assertEquals(3, wire[2]);
      assertTrue(PageKind.keepsProjectionOverflowCodec(payload.length, wire.length - 12));
      final OverflowPage decoded = read(wire);
      assertTrue(decoded.isProjectionPayload());
      assertArrayEquals(payload, decoded.getDataBytes());
    }
  }

  @Test
  void projectionOverflowRejectsUnknownModesAndTruncations() throws Exception {
    final byte[] invalidFlag = GOLDEN.clone();
    invalidFlag[2] |= 4;
    assertThrows(IllegalStateException.class, () -> read(invalidFlag));
    final byte[] invalidCodec = GOLDEN.clone();
    invalidCodec[11] = 1;
    final ByteArrayBytesIn invalidCodecSource = new ByteArrayBytesIn(invalidCodec);
    assertThrows(IllegalStateException.class,
        () -> new PagePersister().deserializePage(CONFIG, invalidCodecSource, SerializationType.DATA));
    assertEquals(12, invalidCodecSource.position());
    final byte[] trailing = Arrays.copyOf(GOLDEN, GOLDEN.length + 1);
    assertThrows(IllegalStateException.class, () -> read(trailing));
    trailing[2] = 1;
    final OverflowPage legacy = read(trailing);
    assertFalse(legacy.isProjectionPayload());
    assertArrayEquals(new byte[64], legacy.getDataBytes());
    for (int end = 7; end < GOLDEN.length; end++) {
      final byte[] truncated = Arrays.copyOf(GOLDEN, end);
      assertThrows(RuntimeException.class, () -> read(truncated));
    }
  }

  private static OverflowPage read(final byte[] bytes) throws Exception {
    return (OverflowPage) new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(bytes),
        SerializationType.DATA);
  }
}

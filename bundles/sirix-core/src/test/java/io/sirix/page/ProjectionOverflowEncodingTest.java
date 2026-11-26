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

  /**
   * Whatever codec the writer elects, both overflow routes must return the bytes they were given. The
   * payload shapes below are chosen to elect different codecs — all-zero and constant-byte runs, a
   * long repeating pattern, a run-free sequence and an incompressible one that keeps the raw route —
   * so the strict frame checks on the read side are exercised against real writer output rather than
   * only against hand-written frames.
   */
  @Test
  void everyElectedCodecRoundTripsOnBothOverflowRoutes() throws Exception {
    final byte[][] payloads = new byte[6][];
    payloads[0] = new byte[4096];
    payloads[1] = new byte[4096];
    Arrays.fill(payloads[1], (byte) 0x2a);
    payloads[2] = new byte[4096];
    for (int i = 0; i < payloads[2].length; i++) {
      payloads[2][i] = (byte) ("sirix-hot-leaf-".charAt(i % 15));
    }
    payloads[3] = new byte[4096];
    for (int i = 0; i < payloads[3].length; i++) {
      payloads[3][i] = (byte) (i * 7);
    }
    payloads[4] = new byte[4096];
    long state = 0x9e3779b97f4a7c15L;
    for (int i = 0; i < payloads[4].length; i++) {
      state = state * 6364136223846793005L + 1442695040888963407L;
      payloads[4][i] = (byte) (state >>> 33);
    }
    payloads[5] = new byte[64];
    Arrays.fill(payloads[5], (byte) 0x7f);
    for (final byte[] payload : payloads) {
      for (final boolean projection : new boolean[] {false, true}) {
        try (MemorySegmentBytesOut out = new MemorySegmentBytesOut()) {
          PageKind.OVERFLOWPAGE.serializePage(CONFIG, out, projection
              ? OverflowPage.forProjectionPayload(payload)
              : new OverflowPage(payload), SerializationType.DATA);
          final OverflowPage decoded = read(out.toByteArray());
          assertEquals(projection, decoded.isProjectionPayload());
          assertArrayEquals(payload, decoded.getDataBytes(),
              "payload of " + payload.length + " bytes, projection=" + projection);
        }
      }
    }
  }

  /**
   * The three encoded page images FMT-216's acceptance criteria supply, with the payloads that text
   * states: the zero-run fixture produces 64 zero bytes, the byte-run and LZ77 fixtures 64 copies of
   * {@code 2a}. Both bytes and expected payload come from the specification, not from the codecs.
   */
  @Test
  void specifiedCodecFixturesProduceTheirSpecifiedPayloads() throws Exception {
    final byte[] repeated = new byte[64];
    Arrays.fill(repeated, (byte) 0x2a);
    for (final String[] fixture : new String[][] {{"090003400000000300000000ff40be", "zero-run"},
        {"0900034000000005000000" + "02fe40fe402a", "byte-run"},
        {"0900034000000007000000" + "03fd401f2a01002c", "Sirix LZ77"}}) {
      final OverflowPage page = read(HexFormat.of().parseHex(fixture[0]));
      assertTrue(page.isProjectionPayload(), fixture[1] + " is a projection payload");
      assertArrayEquals(fixture[1].equals("zero-run")
          ? new byte[64]
          : repeated, page.getDataBytes(), fixture[1] + " payload");
    }
  }

  /**
   * FMT-216 requires a reader to refuse malformed or truncated tokens, counts and repeat bytes
   * outside the opcode table, and length disagreement, "before exposing payload bytes" — and to bound
   * every span by the stored length rather than by the reusable scratch that holds it.
   *
   * <p>
   * Each frame below is a valid page envelope carrying one defect the specification names. The loop
   * decodes a valid 64-byte payload before every one of them, so the decode scratch holds
   * recognisable bytes a frame reading past its stored length would publish: that is the "after
   * scratch reuse" case of the acceptance criteria, and it is how
   * {@code 090003400000000300000002fe403f} returned 64 bytes of an earlier payload.
   */
  @Test
  void projectionOverflowRefusesFramesThatReadBeyondTheirStoredLength() throws Exception {
    final byte[] primed = new byte[64];
    Arrays.fill(primed, (byte) 0x2a);
    for (final String malformed : new String[] {
        // Byte-run literal run of 64 bytes with no literal byte inside the stored length.
        "090003400000000300000002fe403f",
        // Byte-run long repeat count below two.
        "0900034000000005000000" + "02fe40fe012a",
        // Byte-run long repeat run of the zero byte, which the zero opcodes own.
        "0900034000000005000000" + "02fe40fe4000",
        // Byte-run short repeat run of the zero byte.
        "0900034000000004000000" + "02fe40c000",
        // Byte-run unassigned opcode ff.
        "0900034000000003000000" + "02fe40ff",
        // Byte-run long zero-run count below two.
        "0900034000000004000000" + "02fe40fd01",
        // LZ77 literal-length extension truncated at the stored length.
        "0900034000000003000000" + "03fd40f0",
        // LZ77 literal run reaching past the stored length.
        "0900034000000004000000" + "03fd40702a",
        // LZ77 match distance pointing before the first decoded byte.
        "0900034000000005000000" + "03fd40040500"}) {
      assertArrayEquals(primed,
          read(HexFormat.of().parseHex("0900034000000005000000" + "02fe40fe402a")).getDataBytes());
      final byte[] frame = HexFormat.of().parseHex(malformed);
      final IllegalStateException refusal =
          assertThrows(IllegalStateException.class, () -> read(frame), "frame " + malformed + " must be refused");
      assertFalse(refusal.getMessage().isEmpty(), "refusal names the defect");
    }
  }

  /**
   * FMT-216: "the codec size MUST equal the outer decoded length. The reader MUST validate both
   * lengths and their agreement before allocating decoded bytes." The second frame declares a
   * destination of nearly 2 GiB from a three-byte codec frame; refusing it with a corruption error
   * rather than exhausting the heap is the evidence that nothing of the declared size was reserved.
   */
  @Test
  void projectionOverflowRefusesOuterLengthDisagreementBeforeReserving() {
    for (final String declared : new String[] {"41000000", "3f000000", "f0ffff7f"}) {
      final byte[] frame = HexFormat.of().parseHex("090003" + declared + "0300000000ff40be");
      final IllegalStateException refusal = assertThrows(IllegalStateException.class, () -> read(frame));
      assertTrue(refusal.getMessage().contains("frame declares 64"), refusal.getMessage());
    }
  }

  private static OverflowPage read(final byte[] bytes) throws Exception {
    return (OverflowPage) new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(bytes),
        SerializationType.DATA);
  }
}

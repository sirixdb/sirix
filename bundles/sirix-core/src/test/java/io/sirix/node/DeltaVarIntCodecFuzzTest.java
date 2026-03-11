package io.sirix.node;

import io.sirix.settings.Fixed;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fuzz tests for {@link DeltaVarIntCodec}.
 *
 * <p>Each test iteration uses a random seed derived from nanoTime.
 * On failure the seed is printed so the exact case can be replayed.
 */
class DeltaVarIntCodecFuzzTest {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  // ==================== DELTA ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzDeltaRoundtrip_BytesOutPath() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long baseKey = randomKey(rng);
    final long targetKey = rng.nextBoolean() ? NULL_KEY : randomKey(rng);

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeDelta(out, targetKey, baseKey);
      final byte[] encoded = out.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final long decoded = DeltaVarIntCodec.decodeDelta(in, baseKey);

      assertEquals(targetKey, decoded,
          "Delta roundtrip failed [seed=" + seed + ", base=" + baseKey + ", target=" + targetKey + "]");

      // All bytes must have been consumed
      assertEquals(0, in.remaining(),
          "Not all bytes consumed [seed=" + seed + ", encoded.length=" + encoded.length + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception during delta roundtrip [seed=" + seed + ", base=" + baseKey
          + ", target=" + targetKey + "]: " + e.getMessage(), e);
    }
  }

  @RepeatedTest(200)
  void fuzzDeltaRoundtrip_GrowingSegmentPath() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long baseKey = randomKey(rng);
    final long targetKey = rng.nextBoolean() ? NULL_KEY : randomKey(rng);

    try {
      final GrowingMemorySegment seg = new GrowingMemorySegment(16);
      DeltaVarIntCodec.encodeDelta(seg, targetKey, baseKey);
      final byte[] encoded = seg.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final long decoded = DeltaVarIntCodec.decodeDelta(in, baseKey);

      assertEquals(targetKey, decoded,
          "GrowingSegment delta roundtrip failed [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  @RepeatedTest(200)
  void fuzzDeltaRoundtrip_SegmentDecodePath() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long baseKey = randomKey(rng);
    final long targetKey = rng.nextBoolean() ? NULL_KEY : randomKey(rng);

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeDelta(out, targetKey, baseKey);
      final byte[] encoded = out.toByteArray();

      try (final Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(encoded.length);
        MemorySegment.copy(encoded, 0, segment, ValueLayout.JAVA_BYTE, 0, encoded.length);

        // Decode via int-offset overload
        final long decodedInt = DeltaVarIntCodec.decodeDeltaFromSegment(segment, 0, baseKey);
        assertEquals(targetKey, decodedInt,
            "Segment(int) delta decode failed [seed=" + seed + "]");

        // Decode via long-offset overload
        final long decodedLong = DeltaVarIntCodec.decodeDeltaFromSegment(segment, 0L, baseKey);
        assertEquals(targetKey, decodedLong,
            "Segment(long) delta decode failed [seed=" + seed + "]");

        // Length must match encoded length
        final int computedLen = DeltaVarIntCodec.deltaLength(segment, 0);
        assertEquals(encoded.length, computedLen,
            "deltaLength mismatch [seed=" + seed + "]");
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== SIGNED LONG ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzSignedLongRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long value = randomSignedLong(rng);

    try {
      // BytesOut path
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeSignedLong(out, value);
      final byte[] encoded = out.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final long decoded = DeltaVarIntCodec.decodeSignedLong(in);
      assertEquals(value, decoded,
          "SignedLong BytesOut roundtrip failed [seed=" + seed + ", value=" + value + "]");

      // MemorySegment decode path
      try (final Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(encoded.length);
        MemorySegment.copy(encoded, 0, segment, ValueLayout.JAVA_BYTE, 0, encoded.length);

        final long segDecoded = DeltaVarIntCodec.decodeSignedLongFromSegment(segment, 0);
        assertEquals(value, segDecoded,
            "SignedLong segment decode failed [seed=" + seed + "]");
      }

      // GrowingSegment encode path
      final GrowingMemorySegment seg = new GrowingMemorySegment(16);
      DeltaVarIntCodec.encodeSignedLong(seg, value);
      final byte[] segEncoded = seg.toByteArray();
      assertBytesEqual(encoded, segEncoded,
          "SignedLong GrowingSegment encode mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== SIGNED INT ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzSignedIntRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int value = rng.nextInt();

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeSigned(out, value);
      final byte[] encoded = out.toByteArray();

      // BytesIn decode
      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final int decoded = DeltaVarIntCodec.decodeSigned(in);
      assertEquals(value, decoded,
          "Signed int roundtrip failed [seed=" + seed + ", value=" + value + "]");

      // Segment decode
      try (final Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(encoded.length);
        MemorySegment.copy(encoded, 0, segment, ValueLayout.JAVA_BYTE, 0, encoded.length);

        final int segDecoded = DeltaVarIntCodec.decodeSignedFromSegment(segment, 0);
        assertEquals(value, segDecoded,
            "Signed int segment decode failed [seed=" + seed + "]");
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== ABSOLUTE KEY ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzAbsoluteKeyRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    // Absolute keys must be non-negative
    final long key = rng.nextLong() & Long.MAX_VALUE;

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeAbsolute(out, key);
      final byte[] encoded = out.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final long decoded = DeltaVarIntCodec.decodeAbsolute(in);
      assertEquals(key, decoded,
          "Absolute key roundtrip failed [seed=" + seed + ", key=" + key + "]");

      // GrowingSegment path
      final GrowingMemorySegment seg = new GrowingMemorySegment(16);
      DeltaVarIntCodec.encodeAbsolute(seg, key);
      assertBytesEqual(encoded, seg.toByteArray(),
          "Absolute key GrowingSegment mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  @Test
  void absoluteKeyRejectsNegative() {
    final MemorySegmentBytesOut out = new MemorySegmentBytesOut(16);
    assertThrows(IllegalArgumentException.class,
        () -> DeltaVarIntCodec.encodeAbsolute(out, -1));
    assertThrows(IllegalArgumentException.class,
        () -> DeltaVarIntCodec.encodeAbsolute(out, Long.MIN_VALUE));
  }

  // ==================== REVISION ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzRevisionRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    // Revisions can be -1 (NULL) or non-negative
    final int revision = rng.nextBoolean() ? -1 : rng.nextInt(Integer.MAX_VALUE);

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(16);
      DeltaVarIntCodec.encodeRevision(out, revision);
      final byte[] encoded = out.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final int decoded = DeltaVarIntCodec.decodeRevision(in);
      assertEquals(revision, decoded,
          "Revision roundtrip failed [seed=" + seed + ", revision=" + revision + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== WRITE-TO-SEGMENT ROUNDTRIP FUZZ ====================

  @RepeatedTest(200)
  void fuzzWriteDeltaToSegment() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long baseKey = randomKey(rng);
    final long targetKey = rng.nextBoolean() ? NULL_KEY : randomKey(rng);

    try (final Arena arena = Arena.ofConfined()) {
      // Write via direct segment path
      final MemorySegment segment = arena.allocate(16);
      final int bytesWritten = DeltaVarIntCodec.writeDeltaToSegment(segment, 0, targetKey, baseKey);

      // Decode back
      final long decoded = DeltaVarIntCodec.decodeDeltaFromSegment(segment, 0L, baseKey);
      assertEquals(targetKey, decoded,
          "writeDeltaToSegment roundtrip failed [seed=" + seed + "]");

      // Width must match
      final int computedWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(targetKey, baseKey);
      assertEquals(computedWidth, bytesWritten,
          "writeDeltaToSegment width mismatch [seed=" + seed + "]");

      // readDeltaEncodedWidth must also match
      final int readWidth = DeltaVarIntCodec.readDeltaEncodedWidth(segment, 0L);
      assertEquals(bytesWritten, readWidth,
          "readDeltaEncodedWidth mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  @RepeatedTest(200)
  void fuzzWriteSignedToSegment() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int value = rng.nextInt();

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segment = arena.allocate(16);
      final int bytesWritten = DeltaVarIntCodec.writeSignedToSegment(segment, 0, value);

      final int decoded = DeltaVarIntCodec.decodeSignedFromSegment(segment, 0L);
      assertEquals(value, decoded,
          "writeSignedToSegment roundtrip failed [seed=" + seed + ", value=" + value + "]");

      final int computedWidth = DeltaVarIntCodec.computeSignedEncodedWidth(value);
      assertEquals(computedWidth, bytesWritten,
          "writeSignedToSegment width mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  @RepeatedTest(200)
  void fuzzWriteSignedLongToSegment() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long value = randomSignedLong(rng);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segment = arena.allocate(16);
      final int bytesWritten = DeltaVarIntCodec.writeSignedLongToSegment(segment, 0, value);

      final long decoded = DeltaVarIntCodec.decodeSignedLongFromSegment(segment, 0L);
      assertEquals(value, decoded,
          "writeSignedLongToSegment roundtrip failed [seed=" + seed + ", value=" + value + "]");

      final int computedWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(value);
      assertEquals(computedWidth, bytesWritten,
          "writeSignedLongToSegment width mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== CROSS-PATH CONSISTENCY FUZZ ====================

  @RepeatedTest(200)
  void fuzzBytesOutVsGrowingSegmentConsistency() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final long baseKey = randomKey(rng);
    final long targetKey = rng.nextBoolean() ? NULL_KEY : randomKey(rng);

    try {
      // Encode via BytesOut
      final MemorySegmentBytesOut bytesOut = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeDelta(bytesOut, targetKey, baseKey);
      final byte[] fromBytesOut = bytesOut.toByteArray();

      // Encode via GrowingMemorySegment
      final GrowingMemorySegment growSeg = new GrowingMemorySegment(16);
      DeltaVarIntCodec.encodeDelta(growSeg, targetKey, baseKey);
      final byte[] fromGrowSeg = growSeg.toByteArray();

      // Encode via writeToSegment
      try (final Arena arena = Arena.ofConfined()) {
        final MemorySegment directSeg = arena.allocate(16);
        final int bytesWritten = DeltaVarIntCodec.writeDeltaToSegment(directSeg, 0, targetKey, baseKey);
        final byte[] fromDirect = new byte[bytesWritten];
        MemorySegment.copy(directSeg, ValueLayout.JAVA_BYTE, 0, fromDirect, 0, bytesWritten);

        assertBytesEqual(fromBytesOut, fromGrowSeg,
            "BytesOut vs GrowingSegment mismatch [seed=" + seed + "]");
        assertBytesEqual(fromBytesOut, fromDirect,
            "BytesOut vs writeDeltaToSegment mismatch [seed=" + seed + "]");
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== SEQUENTIAL MULTI-VALUE FUZZ ====================

  @RepeatedTest(50)
  void fuzzSequentialMultiValueDecode() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int count = rng.nextInt(20) + 3;
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = rng.nextLong() & Long.MAX_VALUE; // unsigned varints
    }

    try {
      // Encode all values sequentially
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(256);
      for (final long v : values) {
        DeltaVarIntCodec.encodeUnsignedLong(out, v);
      }
      final byte[] encoded = out.toByteArray();

      // Decode via BytesIn
      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      for (int i = 0; i < count; i++) {
        final long decoded = DeltaVarIntCodec.decodeUnsignedLong(in);
        assertEquals(values[i], decoded,
            "Sequential decode #" + i + " failed [seed=" + seed + "]");
      }

      // Decode via MemorySegment using varintLength to advance offset
      try (final Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(encoded.length);
        MemorySegment.copy(encoded, 0, segment, ValueLayout.JAVA_BYTE, 0, encoded.length);

        int offset = 0;
        for (int i = 0; i < count; i++) {
          final long decoded = DeltaVarIntCodec.readVarLongFromSegment(segment, offset);
          assertEquals(values[i], decoded,
              "Sequential segment decode #" + i + " failed [seed=" + seed + "]");
          offset += DeltaVarIntCodec.varintLength(segment, offset);
        }
        assertEquals(encoded.length, offset,
            "Total offset mismatch [seed=" + seed + "]");
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== EDGE CASE FOCUSED FUZZ ====================

  @RepeatedTest(100)
  void fuzzDeltaWithEdgeCaseDistribution() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    // Weighted toward edge-case deltas: ±1, 0, large, boundary
    final long baseKey = rng.nextLong() & Long.MAX_VALUE;
    final long targetKey;

    final int choice = rng.nextInt(12);
    targetKey = switch (choice) {
      case 0 -> NULL_KEY;
      case 1 -> baseKey; // delta=0 (self-reference)
      case 2 -> baseKey + 1; // delta=+1
      case 3 -> baseKey - 1; // delta=-1
      case 4 -> baseKey + 63; // max 1-byte positive zigzag
      case 5 -> baseKey - 64; // max 1-byte negative zigzag
      case 6 -> baseKey + 64; // min 2-byte positive zigzag
      case 7 -> baseKey - 65; // min 2-byte negative zigzag
      case 8 -> baseKey + 8191; // max 2-byte positive zigzag
      case 9 -> baseKey - 8192; // max 2-byte negative zigzag
      case 10 -> 0L;
      case 11 -> Long.MAX_VALUE;
      default -> randomKey(rng);
    };

    try {
      final MemorySegmentBytesOut out = new MemorySegmentBytesOut(64);
      DeltaVarIntCodec.encodeDelta(out, targetKey, baseKey);
      final byte[] encoded = out.toByteArray();

      final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
      final long decoded = DeltaVarIntCodec.decodeDelta(in, baseKey);

      assertEquals(targetKey, decoded,
          "Edge-case delta roundtrip failed [seed=" + seed
              + ", base=" + baseKey + ", target=" + targetKey
              + ", choice=" + choice + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", choice=" + choice + "]: " + e.getMessage(), e);
    }
  }

  // ==================== RESIZEFIELD FUZZ ====================

  @RepeatedTest(100)
  void fuzzResizeField() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int fieldCount = rng.nextInt(6) + 2; // 2..7 fields
    final int fieldIndex = rng.nextInt(fieldCount);

    try (final Arena arena = Arena.ofConfined()) {
      // Build a synthetic record: [nodeKind][offsetTable][data fields]
      // Each field is a random varint
      final long[] fieldValues = new long[fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        fieldValues[i] = rng.nextLong() & 0xFFFFFFL; // small-to-medium values
      }

      // Serialize the source record
      final MemorySegmentBytesOut tmpOut = new MemorySegmentBytesOut(256);
      tmpOut.writeByte((byte) 42); // nodeKind

      // Reserve offset table space
      final int[] fieldOffsets = new int[fieldCount];
      final int[] fieldLengths = new int[fieldCount];
      final long offsetTableStart = tmpOut.position();
      for (int i = 0; i < fieldCount; i++) {
        tmpOut.writeByte((byte) 0); // placeholder
      }
      final long dataStart = tmpOut.position();

      for (int i = 0; i < fieldCount; i++) {
        fieldOffsets[i] = (int) (tmpOut.position() - dataStart);
        DeltaVarIntCodec.encodeUnsignedLong(tmpOut, fieldValues[i]);
        fieldLengths[i] = (int) (tmpOut.position() - dataStart) - fieldOffsets[i];
      }

      // Patch offset table
      final byte[] srcData = tmpOut.toByteArray();
      for (int i = 0; i < fieldCount; i++) {
        srcData[(int) offsetTableStart + i] = (byte) fieldOffsets[i];
      }

      final int srcRecordLen = srcData.length;

      // Prepare source page
      final MemorySegment srcPage = arena.allocate(srcRecordLen + 64);
      MemorySegment.copy(srcData, 0, srcPage, ValueLayout.JAVA_BYTE, 0, srcRecordLen);

      // Prepare destination page
      final MemorySegment dstPage = arena.allocate(srcRecordLen + 64);

      // Generate a new value for the changed field
      final long newValue = rng.nextLong() & 0xFFFFFFL;

      final int newRecordLen = DeltaVarIntCodec.resizeField(
          srcPage, 0, srcRecordLen,
          fieldCount, fieldIndex,
          dstPage, 0,
          (target, offset) -> DeltaVarIntCodec.writeVarLongToSegment(target, offset, newValue));

      // Verify: unchanged fields should still decode correctly
      final long dstDataRegion = 1 + fieldCount;
      for (int i = 0; i < fieldCount; i++) {
        final int fieldOff = dstPage.get(ValueLayout.JAVA_BYTE, 1 + i) & 0xFF;
        final long absOff = dstDataRegion + fieldOff;

        final long expected = (i == fieldIndex) ? newValue : fieldValues[i];
        final long decoded = DeltaVarIntCodec.readVarLongFromSegment(dstPage, absOff);
        assertEquals(expected, decoded,
            "resizeField: field " + i + " mismatch [seed=" + seed
                + ", fieldIndex=" + fieldIndex + "]");
      }

      // Verify nodeKind byte is preserved
      assertEquals(42, dstPage.get(ValueLayout.JAVA_BYTE, 0) & 0xFF,
          "nodeKind corrupted [seed=" + seed + "]");

      assertTrue(newRecordLen > 0,
          "resizeField returned non-positive length [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception in resizeField fuzz [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== HELPERS ====================

  private static long randomKey(final Random rng) {
    // Weighted: 60% small keys (like typical node keys), 20% medium, 20% large
    final int r = rng.nextInt(10);
    if (r < 6) {
      return rng.nextInt(100_000);
    } else if (r < 8) {
      return rng.nextInt(Integer.MAX_VALUE);
    } else {
      return rng.nextLong() & Long.MAX_VALUE;
    }
  }

  private static long randomSignedLong(final Random rng) {
    // Weighted: 50% small, 20% int-range, 20% full-range, 10% boundaries
    final int r = rng.nextInt(10);
    if (r < 5) {
      return rng.nextInt(20001) - 10000; // [-10000, 10000]
    } else if (r < 7) {
      return rng.nextInt();
    } else if (r < 9) {
      return rng.nextLong();
    } else {
      return switch (rng.nextInt(4)) {
        case 0 -> Long.MIN_VALUE;
        case 1 -> Long.MAX_VALUE;
        case 2 -> 0L;
        default -> -1L;
      };
    }
  }

  private static void assertBytesEqual(final byte[] expected, final byte[] actual, final String msg) {
    assertEquals(expected.length, actual.length, msg + " (length)");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i], msg + " (byte[" + i + "])");
    }
  }
}

/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.bytepipe;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.sirix.cache.Allocators;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The pure-Java LZ4 block decoder has two loops: one on thread-owned heap arrays for blocks up to
 * {@link JavaLz4BlockDecoder#HEAP_DECODE_LIMIT}, one on the segments themselves above it.
 *
 * <p>
 * Three oracles, none of them the code under test. Hand-built blocks carry their expected output
 * from the definition of the format (literals, then {@code matchLength} bytes each copied from
 * {@code distance} bytes back). Blocks produced by the native compressor must decode to their
 * input. And on damaged blocks, where the format defines no output, the two loops must agree with
 * each other: both refuse, or both return the same bytes.
 */
@DisplayName("JavaLz4BlockDecoder decodes every block shape in both loops")
final class JavaLz4BlockDecoderTest {

  private static final int LIMIT = JavaLz4BlockDecoder.HEAP_DECODE_LIMIT;

  /** Bytes around the destination range that no decode may touch. */
  private static final int GUARD = 24;

  private static final byte SENTINEL = 0x5a;

  // ======================================================================
  // Hand-built blocks: every literal and match shape, both loops.
  // ======================================================================

  static Stream<Arguments> runShapes() {
    // Around the one-word and two-word copies (8, 16), the 15 that starts a length chain, and a
    // chain byte of 255 (15 + 255 literals; 4 + 15 + 255 match bytes).
    final int[] literalLengths = {0, 1, 7, 8, 9, 15, 16, 17, 30, 269, 270, 271, 600};
    final int[] distances = {1, 2, 3, 7, 8, 9, 15, 16, 17, 100, 300};
    final int[] matchLengths = {4, 5, 8, 9, 16, 17, 18, 19, 20, 273, 274, 275, 1000};
    final List<Arguments> shapes = new ArrayList<>();
    for (final int literalLength : literalLengths) {
      for (final int distance : distances) {
        for (final int matchLength : matchLengths) {
          shapes.add(Arguments.of(literalLength, distance, matchLength));
        }
      }
    }
    return shapes.stream();
  }

  @ParameterizedTest(name = "literals {0}, distance {1}, match {2}")
  @MethodSource("runShapes")
  @DisplayName("a literal run and a match of every shape decode to the format's definition")
  void everyRunShapeDecodes(final int literalLength, final int distance, final int matchLength) {
    final Random random = new Random(31L * literalLength + 17L * distance + matchLength);
    final BlockBuilder builder = new BlockBuilder();
    // 300 bytes of history, so that every distance under test reaches real output.
    builder.sequence(randomBytes(random, 300), 1, 4);
    builder.sequence(randomBytes(random, literalLength), distance, matchLength);
    builder.last(randomBytes(random, 5));
    final byte[] block = builder.block();
    final byte[] expected = builder.expected();

    assertArrayEquals(expected, decode(Loop.PUBLIC, block, expected.length), "heap loop");
    assertArrayEquals(expected, decode(Loop.SEGMENTS, block, expected.length), "segment loop");
    assertArrayEquals(expected, decode(Loop.PUBLIC, block, LIMIT + 1), "capacity above the heap limit");

    // One byte too little room: both loops refuse, and the heap loop has written nothing.
    for (final Loop loop : Loop.values()) {
      final Outcome short1 = attempt(loop, block, expected.length - 1);
      assertTrue(
          short1.failure() != null
              && short1.failure().getMessage().contains("output exceeds capacity " + (expected.length - 1)),
          loop + ": " + short1.failure());
    }
    assertTrue(attempt(Loop.PUBLIC, block, expected.length - 1).destinationUntouched(),
        "a block that does not decode must leave the destination as it was");
  }

  @ParameterizedTest(name = "{0} literals only")
  @ValueSource(ints = {0, 1, 8, 9, 14, 15, 16, 17, 269, 270, 271, 525, 5000})
  @DisplayName("a block of literals only ends at the input boundary")
  void literalOnlyBlocksDecode(final int length) {
    final byte[] literals = randomBytes(new Random(length), length);
    final byte[] block = new BlockBuilder().last(literals).block();

    assertArrayEquals(literals, decode(Loop.PUBLIC, block, length));
    assertArrayEquals(literals, decode(Loop.SEGMENTS, block, length));
  }

  @Test
  @DisplayName("an empty block decodes to nothing and negative lengths are refused")
  void degenerateLengths() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment source = arena.allocate(8);
      final MemorySegment destination = arena.allocate(8);
      assertEquals(0, JavaLz4BlockDecoder.decompressSafe(source, 0, 0, destination, 0, 8));
      assertEquals(0, JavaLz4BlockDecoder.decompressSafe(source, 0, 0, destination, 0, 0));
      final SirixIOException negativeSource = assertThrows(SirixIOException.class,
          () -> JavaLz4BlockDecoder.decompressSafe(source, 0, -1, destination, 0, 8));
      assertTrue(negativeSource.getMessage().contains("negative length (srcLen=-1, dstCapacity=8)"),
          negativeSource.getMessage());
      final SirixIOException negativeCapacity = assertThrows(SirixIOException.class,
          () -> JavaLz4BlockDecoder.decompressSafe(source, 0, 1, destination, 0, -1));
      assertTrue(negativeCapacity.getMessage().contains("negative length (srcLen=1, dstCapacity=-1)"),
          negativeCapacity.getMessage());
    }
  }

  // ======================================================================
  // Malformed blocks: each refusal, in both loops.
  // ======================================================================

  static Stream<Arguments> malformedBlocks() {
    final byte[] fourLiteralsThenMatch = {0x40, 'a', 'b', 'c', 'd', 0x01, 0x00};
    return Stream.of(
        // A complete sequence with a match, then nothing: the next token is missing.
        Arguments.of("missing token", fourLiteralsThenMatch, 64, "truncated block (missing token)"),
        Arguments.of("literal-length chain ends", new byte[] {(byte) 0xf0}, 64, "truncated literal-length chain"),
        Arguments.of("literal-length chain ends after 255", new byte[] {(byte) 0xf0, (byte) 0xff}, 64,
            "truncated literal-length chain"),
        Arguments.of("literals exceed the input", new byte[] {0x50, 'a', 'b'}, 64, "literal run exceeds input"),
        // The same after a first sequence, where the read position is no longer near zero.
        Arguments.of("later literals exceed the input",
            new byte[] {0x40, 'a', 'b', 'c', 'd', 0x01, 0x00, 0x50, 'x', 'y'}, 64, "literal run exceeds input"),
        Arguments.of("literals exceed the capacity", new byte[] {0x50, 'a', 'b', 'c', 'd', 'e'}, 4,
            "output exceeds capacity 4"),
        Arguments.of("match offset cut after one byte", new byte[] {0x10, 'a', 0x01}, 64, "truncated match offset"),
        Arguments.of("zero distance", new byte[] {0x10, 'a', 0x00, 0x00, 0x00}, 64, "zero match distance"),
        Arguments.of("match-length chain ends", new byte[] {0x1f, 'a', 0x01, 0x00}, 64, "truncated match-length chain"),
        Arguments.of("match-length chain ends after 255", new byte[] {0x1f, 'a', 0x01, 0x00, (byte) 0xff}, 64,
            "truncated match-length chain"),
        Arguments.of("distance before the output start", new byte[] {0x10, 'a', 0x02, 0x00, 0x00}, 64,
            "match distance 2 reaches before the output start"),
        Arguments.of("match exceeds the capacity", new byte[] {0x14, 'a', 0x01, 0x00, 0x00}, 8,
            "output exceeds capacity 8"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedBlocks")
  @DisplayName("a malformed block is refused with the same reason by both loops")
  void malformedBlocksAreRefused(final String name, final byte[] block, final int capacity, final String reason) {
    final Outcome heap = attempt(Loop.PUBLIC, block, capacity);
    final Outcome segments = attempt(Loop.SEGMENTS, block, capacity);
    assertTrue(heap.failure() != null && heap.failure().getMessage().contains(reason), name + ": " + heap.failure());
    assertTrue(segments.failure() != null && segments.failure().getMessage().contains(reason),
        name + ": " + segments.failure());
    assertTrue(heap.destinationUntouched(), name + ": the heap loop must not write a block it refuses");
  }

  @Test
  @DisplayName("a match far longer than any buffer is refused, not attempted")
  void anEnormousMatchIsRefused() {
    // One literal, then a match of 300,019 bytes at distance one: more than the heap limit, so a
    // decoder that tried it would run off its scratch before any later check could object.
    final ByteArrayOutputStream block = new ByteArrayOutputStream();
    block.write(0x1f);
    block.write('a');
    block.write(1);
    block.write(0);
    for (int rest = 300_000; rest >= 255; rest -= 255) {
      block.write(255);
    }
    block.write(300_000 % 255);
    for (final Loop loop : Loop.values()) {
      final Outcome outcome = attempt(loop, block.toByteArray(), 64);
      assertTrue(outcome.failure() != null && outcome.failure().getMessage().contains("output exceeds capacity 64"),
          loop + ": " + outcome.failure());
    }
  }

  @ParameterizedTest(name = "match of {0} bytes")
  @ValueSource(ints = {4, 8, 16, 17, 400})
  @DisplayName("a match that ends exactly at the capacity decodes")
  void aMatchMayFillTheCapacity(final int matchLength) {
    final BlockBuilder builder = new BlockBuilder();
    builder.sequence(new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}, 9, matchLength);
    builder.last(new byte[0]);
    final byte[] expected = builder.expected();
    assertEquals(9 + matchLength, expected.length);

    assertArrayEquals(expected, decode(Loop.PUBLIC, builder.block(), expected.length));
    assertArrayEquals(expected, decode(Loop.SEGMENTS, builder.block(), expected.length));
  }

  @Test
  @DisplayName("every truncation and every flipped bit of a block gets the same verdict from both loops")
  void damagedBlocksGetOneVerdict() {
    final Random random = new Random(0x4c5a34L);
    final BlockBuilder builder = new BlockBuilder();
    builder.sequence(randomBytes(random, 20), 3, 9);
    builder.sequence(randomBytes(random, 0), 12, 30);
    builder.sequence(randomBytes(random, 17), 1, 40);
    builder.last(randomBytes(random, 6));
    final byte[] block = builder.block();
    final int capacity = builder.expected().length;
    int refused = 0;
    int returned = 0;
    for (int length = 1; length < block.length; length++) {
      final Outcome agreed = assertSameVerdict(Arrays.copyOf(block, length), capacity, "truncated to " + length);
      refused += agreed.failure() != null
          ? 1
          : 0;
      returned += agreed.failure() == null
          ? 1
          : 0;
    }
    for (int at = 0; at < block.length; at++) {
      for (int bit = 0; bit < 8; bit++) {
        final byte[] flipped = block.clone();
        flipped[at] ^= (byte) (1 << bit);
        final Outcome agreed = assertSameVerdict(flipped, capacity, "bit " + bit + " of byte " + at + " flipped");
        refused += agreed.failure() != null
            ? 1
            : 0;
        returned += agreed.failure() == null
            ? 1
            : 0;
      }
    }
    assertTrue(refused > 50, "the damage must reach the refusals, got " + refused);
    assertTrue(returned > 50, "the damage must reach decodable blocks, got " + returned);
  }

  // ======================================================================
  // Which loop runs, and what a refused block leaves behind.
  // ======================================================================

  @Test
  @DisplayName("a block at the heap limit is decoded on the heap, one byte above it on the segments")
  void theLimitSelectsTheLoop() {
    // Eight bytes decode, then the next sequence has a zero distance. The segment loop has written
    // the eight bytes by then; the heap loop writes nothing unless the whole block decodes.
    final byte[] start = {0x40, 'a', 'b', 'c', 'd', 0x01, 0x00, 0x00, 0x00, 0x00};
    assertTrue(attempt(Loop.PUBLIC, start, LIMIT).destinationUntouched(), "capacity at the limit: heap loop");
    assertEquals(8, attempt(Loop.PUBLIC, start, LIMIT + 1).written(), "capacity above the limit: segment loop");

    final byte[] atLimit = Arrays.copyOf(start, LIMIT);
    assertTrue(attempt(Loop.PUBLIC, atLimit, 64).destinationUntouched(), "length at the limit: heap loop");
    final byte[] aboveLimit = Arrays.copyOf(start, LIMIT + 1);
    assertEquals(8, attempt(Loop.PUBLIC, aboveLimit, 64).written(), "length above the limit: segment loop");
  }

  @Test
  @DisplayName("scratch arrays double, start at 4 KiB and stop at the limit plus the word slack")
  void scratchCapacityIsBounded() {
    final int slack = JavaLz4BlockDecoder.WORD_SLACK;
    assertEquals(8, slack);
    assertEquals(4096, JavaLz4BlockDecoder.scratchCapacity(1));
    assertEquals(4096, JavaLz4BlockDecoder.scratchCapacity(4096 - slack));
    assertEquals(8192, JavaLz4BlockDecoder.scratchCapacity(4096 - slack + 1));
    assertEquals(8192, JavaLz4BlockDecoder.scratchCapacity(8192 - slack));
    assertEquals(16384, JavaLz4BlockDecoder.scratchCapacity(8192 - slack + 1));
    assertEquals(131072, JavaLz4BlockDecoder.scratchCapacity(131072 - slack));
    assertEquals(262144, JavaLz4BlockDecoder.scratchCapacity(131072 - slack + 1));
    assertEquals(262144, JavaLz4BlockDecoder.scratchCapacity(LIMIT - slack));
    assertEquals(LIMIT + slack, JavaLz4BlockDecoder.scratchCapacity(LIMIT - slack + 1));
    assertEquals(LIMIT + slack, JavaLz4BlockDecoder.scratchCapacity(LIMIT));
  }

  @Test
  @DisplayName("a block slightly larger than the scratch a thread already holds gets a larger scratch")
  void scratchGrowsForASlightlyLargerBlock() throws InterruptedException {
    // 4,000 literals size a fresh thread's two scratch arrays to 4 KiB. The next block is 4,100
    // bytes long, or decodes to 4,100 bytes: neither fits 4 KiB once the word slack is counted. A
    // reuse test that is off by the slack would keep the 4 KiB array and run past it.
    final Random random = new Random(41);
    final byte[] smallLiterals = randomBytes(random, 4000);
    final byte[] small = new BlockBuilder().last(smallLiterals).block();
    final byte[] longerLiterals = randomBytes(random, 4083);
    final byte[] longerInput = new BlockBuilder().last(longerLiterals).block();
    assertEquals(4100, longerInput.length, "one token, sixteen chain bytes and 4,083 literals");
    final BlockBuilder longerOutput = new BlockBuilder();
    longerOutput.sequence(new byte[] {7}, 1, 4099);
    longerOutput.last(new byte[0]);

    onFreshThread(() -> {
      assertArrayEquals(smallLiterals, decode(Loop.PUBLIC, small, 4000), "first block on a fresh thread");
      assertArrayEquals(longerLiterals, decode(Loop.PUBLIC, longerInput, 4083), "longer input");
    });
    onFreshThread(() -> {
      assertArrayEquals(smallLiterals, decode(Loop.PUBLIC, small, 4000), "first block on a fresh thread");
      assertArrayEquals(longerOutput.expected(), decode(Loop.PUBLIC, longerOutput.block(), 4100), "longer output");
    });
  }

  @Test
  @DisplayName("a match that would run past the scratch itself is refused before it is copied")
  void aMatchPastTheScratchIsRefused() throws InterruptedException {
    // On a fresh thread the output scratch for a capacity of 4,088 is exactly 4 KiB. After 4,000
    // literals a match of 200 bytes exceeds the capacity by 112 and the scratch by 104.
    final BlockBuilder builder = new BlockBuilder();
    builder.sequence(randomBytes(new Random(5), 4000), 1, 200);
    builder.last(new byte[0]);
    onFreshThread(() -> {
      final Outcome outcome = attempt(Loop.PUBLIC, builder.block(), 4088);
      assertTrue(outcome.failure() != null && outcome.failure().getMessage().contains("output exceeds capacity 4088"),
          String.valueOf(outcome.failure()));
      assertTrue(outcome.destinationUntouched());
    });
  }

  @Test
  @DisplayName("blocks of growing and shrinking size reuse the thread's scratch without mixing their bytes")
  void scratchIsReusedAcrossSizes() {
    final Random random = new Random(7);
    for (final int size : new int[] {10, 5000, 40, 70_000, 12, LIMIT, 100, 9000}) {
      final byte[] literals = randomBytes(random, size - 4);
      final BlockBuilder builder = new BlockBuilder();
      builder.sequence(Arrays.copyOf(literals, literals.length / 2), 1, 4);
      builder.last(Arrays.copyOfRange(literals, literals.length / 2, literals.length));
      assertEquals(size, builder.expected().length);
      assertArrayEquals(builder.expected(), decode(Loop.PUBLIC, builder.block(), size), "size " + size);
    }
  }

  // ======================================================================
  // Blocks from the native compressor.
  // ======================================================================

  @Test
  @DisplayName("blocks written by native LZ4 decode to their input in both loops")
  void nativeBlocksRoundTrip() {
    assumeTrue(FFILz4Compressor.isNativeAvailable(), "native LZ4 writes the blocks");
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final Random random = new Random(0x53314c5aL);
    final int[] sizes = {1, 2, 3, 4, 5, 7, 8, 9, 12, 13, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 255, 256, 1000, 4095,
        4096, 4097, 8192, 20_000, 65_535, 65_536, 65_537, LIMIT - 1, LIMIT, LIMIT + 1, 300_000};
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment plain = arena.allocate(300_000);
      final MemorySegment packed = arena.allocate(lz4.compressBound(300_000));
      for (int round = 0; round < 12; round++) {
        for (final int size : sizes) {
          final byte[] original = mixedContent(random, size, round);
          MemorySegment.copy(original, 0, plain, ValueLayout.JAVA_BYTE, 0, size);
          final int packedLength = lz4.compressSegmentFast(plain.asSlice(0, size), packed, 1);
          assertTrue(packedLength > 0, "native LZ4 must compress " + size + " bytes");
          final byte[] block = new byte[packedLength];
          MemorySegment.copy(packed, ValueLayout.JAVA_BYTE, 0, block, 0, packedLength);

          assertArrayEquals(original, decode(Loop.PUBLIC, block, size), "public entry, size " + size);
          assertArrayEquals(original, decode(Loop.SEGMENTS, block, size), "segment loop, size " + size);
          if (packedLength > 1) {
            assertSameVerdict(Arrays.copyOf(block, random.nextInt(packedLength - 1) + 1), size,
                "truncated, size " + size);
            final byte[] flipped = block.clone();
            flipped[random.nextInt(packedLength)] ^= (byte) (1 << random.nextInt(8));
            assertSameVerdict(flipped, size, "one bit flipped, size " + size);
          }
          assertSameVerdict(block, random.nextInt(size), "capacity too small, size " + size);
        }
      }
    }
  }

  @Test
  @DisplayName("the slot payload of a HOT leaf, compressed by native LZ4, decodes to the same bytes")
  void hotLeafPayloadRoundTrips() {
    assumeTrue(FFILz4Compressor.isNativeAvailable(), "native LZ4 writes the block");
    Allocators.getInstance().init(1L << 30);
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
    try (Arena arena = Arena.ofConfined()) {
      final Random random = new Random(12);
      for (int i = 0; i < 300; i++) {
        final byte[] key = {0x12, 0x34, 0x56, (byte) (i >>> 8), (byte) i};
        final byte[] value = new byte[40 + random.nextInt(60)];
        for (int j = 0; j < value.length; j += 1 + random.nextInt(9)) {
          value[j] = (byte) random.nextInt();
        }
        assertTrue(leaf.put(key, value), "the fixture leaf must take every entry");
      }
      final int used = leaf.getUsedSlotsSize();
      final byte[] payload = new byte[used];
      MemorySegment.copy(leaf.slots(), ValueLayout.JAVA_BYTE, 0, payload, 0, used);
      final MemorySegment packed = arena.allocate(lz4.compressBound(used));
      final int packedLength = lz4.compressSegmentFast(leaf.slots().asSlice(0, used), packed, 1);
      assertTrue(packedLength > 0 && packedLength < used, "the payload must compress: " + packedLength + " of " + used);
      final byte[] block = new byte[packedLength];
      MemorySegment.copy(packed, ValueLayout.JAVA_BYTE, 0, block, 0, packedLength);

      assertArrayEquals(payload, decode(Loop.PUBLIC, block, used));
      assertArrayEquals(payload, decode(Loop.SEGMENTS, block, used));
    } finally {
      leaf.close();
    }
  }

  // ======================================================================
  // Helpers.
  // ======================================================================

  private enum Loop {
    /** The public entry point: the heap loop unless a length exceeds the limit. */
    PUBLIC,
    /** The segment loop, whatever the lengths. */
    SEGMENTS
  }

  /**
   * What one decode did: the bytes it returned or the refusal, and what it left in the destination.
   */
  private record Outcome(byte[] bytes, SirixIOException failure, boolean destinationUntouched, int written) {
  }

  private static byte[] decode(final Loop loop, final byte[] block, final int capacity) {
    final Outcome outcome = attempt(loop, block, capacity);
    if (outcome.failure() != null) {
      throw new AssertionError(loop + " refused a valid block", outcome.failure());
    }
    return outcome.bytes();
  }

  /**
   * Decode {@code block} from a nonzero source offset to a nonzero destination offset. Whatever the
   * outcome, the bytes before the destination offset and after the capacity must be untouched.
   */
  private static Outcome attempt(final Loop loop, final byte[] block, final int capacity) {
    final int sourceOffset = 3;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment source = arena.allocate(sourceOffset + block.length + 5L);
      source.fill((byte) 0x33);
      MemorySegment.copy(block, 0, source, ValueLayout.JAVA_BYTE, sourceOffset, block.length);
      final MemorySegment destination = arena.allocate(GUARD + capacity + (long) GUARD);
      destination.fill(SENTINEL);
      byte[] bytes = null;
      SirixIOException failure = null;
      try {
        final int produced = loop == Loop.PUBLIC
            ? JavaLz4BlockDecoder.decompressSafe(source, sourceOffset, block.length, destination, GUARD, capacity)
            : JavaLz4BlockDecoder.decompressSegments(source, sourceOffset, block.length, destination, GUARD, capacity);
        assertTrue(produced >= 0 && produced <= capacity, "produced " + produced + " of capacity " + capacity);
        bytes = new byte[produced];
        MemorySegment.copy(destination, ValueLayout.JAVA_BYTE, GUARD, bytes, 0, produced);
      } catch (final SirixIOException refused) {
        failure = refused;
      }
      for (int i = 0; i < GUARD; i++) {
        assertEquals(SENTINEL, destination.get(ValueLayout.JAVA_BYTE, i), "byte before the destination range");
        assertEquals(SENTINEL, destination.get(ValueLayout.JAVA_BYTE, GUARD + capacity + (long) i),
            "byte after the destination capacity");
      }
      int written = 0;
      final int inspected = Math.min(capacity, 4096);
      for (int i = 0; i < inspected; i++) {
        if (destination.get(ValueLayout.JAVA_BYTE, GUARD + (long) i) != SENTINEL) {
          written = i + 1;
        }
      }
      return new Outcome(bytes, failure, written == 0, written);
    }
  }

  /**
   * The public entry and the segment loop must both refuse, or both return the same bytes. Returns
   * the agreed outcome.
   */
  private static Outcome assertSameVerdict(final byte[] block, final int capacity, final String what) {
    final Outcome heap = attempt(Loop.PUBLIC, block, capacity);
    final Outcome segments = attempt(Loop.SEGMENTS, block, capacity);
    assertEquals(segments.failure() == null, heap.failure() == null,
        what + ": one loop refused and the other did not; heap=" + heap.failure() + " segments=" + segments.failure());
    if (heap.failure() == null) {
      assertArrayEquals(segments.bytes(), heap.bytes(), what);
    } else if (block.length <= LIMIT && capacity <= LIMIT) {
      assertTrue(heap.destinationUntouched(), what + ": the heap loop must not write a block it refuses");
    }
    return heap;
  }

  /**
   * Builds an LZ4 block sequence by sequence and, independently of any decoder, the bytes the format
   * defines for it.
   */
  private static final class BlockBuilder {
    private final ByteArrayOutputStream block = new ByteArrayOutputStream();
    private byte[] expected = new byte[0];

    BlockBuilder sequence(final byte[] literals, final int distance, final int matchLength) {
      final int matchCode = matchLength - 4;
      block.write(Math.min(literals.length, 15) << 4 | Math.min(matchCode, 15));
      writeLengthChain(literals.length);
      block.writeBytes(literals);
      block.write(distance & 0xff);
      block.write(distance >>> 8);
      writeLengthChain(matchCode);
      final int start = expected.length + literals.length;
      expected = Arrays.copyOf(expected, start + matchLength);
      System.arraycopy(literals, 0, expected, start - literals.length, literals.length);
      for (int i = 0; i < matchLength; i++) {
        expected[start + i] = expected[start + i - distance];
      }
      return this;
    }

    BlockBuilder last(final byte[] literals) {
      block.write(Math.min(literals.length, 15) << 4);
      writeLengthChain(literals.length);
      block.writeBytes(literals);
      final int start = expected.length;
      expected = Arrays.copyOf(expected, start + literals.length);
      System.arraycopy(literals, 0, expected, start, literals.length);
      return this;
    }

    private void writeLengthChain(final int length) {
      if (length < 15) {
        return;
      }
      int rest = length - 15;
      while (rest >= 255) {
        block.write(255);
        rest -= 255;
      }
      block.write(rest);
    }

    byte[] block() {
      return block.toByteArray();
    }

    byte[] expected() {
      return expected;
    }
  }

  /** Runs {@code body} on a thread that has never decoded, so its scratch starts empty. */
  private static void onFreshThread(final Runnable body) throws InterruptedException {
    final Throwable[] failure = new Throwable[1];
    final Thread fresh = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable thrown) {
        failure[0] = thrown;
      }
    }, "lz4-fallback-fresh-scratch");
    fresh.start();
    fresh.join();
    if (failure[0] != null) {
      throw new AssertionError("decoding on a fresh thread failed", failure[0]);
    }
  }

  private static byte[] randomBytes(final Random random, final int length) {
    final byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return bytes;
  }

  /**
   * Incompressible bytes, zero runs, short repeats and long-period repeats, in runs of mixed length.
   */
  private static byte[] mixedContent(final Random random, final int size, final int round) {
    final byte[] data = new byte[size];
    int i = 0;
    while (i < size) {
      final int run = 1 + random.nextInt(round % 4 == 0
          ? 4
          : 64);
      final int kind = random.nextInt(4);
      final int period = 1 + random.nextInt(kind == 3
          ? 300
          : 12);
      for (int j = 0; j < run && i < size; j++, i++) {
        data[i] = switch (kind) {
          case 0 -> (byte) random.nextInt();
          case 1 -> 0;
          default -> i >= period
              ? data[i - period]
              : (byte) (i * 31);
        };
      }
    }
    return data;
  }
}

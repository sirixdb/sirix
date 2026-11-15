package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.LE;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Salt is an opaque 8-byte part of the header, never a reader discriminator. */
final class RecordPageSaltTest {
  // kind/version/flags + one-byte page key + revision + index type; independent wire-layout oracle.
  private static final int HEADER_START = 9;
  private static final int SALT_START = HEADER_START + 24;
  private static final byte[] RECORD = {1, 2, 3};

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void neighboringSaltsCannotReconstructTheRemovedSalt(final boolean disposable) {
    final long[] predictable = new long[4];
    for (int i = 0; i < predictable.length; i++) {
      predictable[i] = ThreadLocalRandom.current().nextLong();
    }
    assertEquals(unmixMurmur(predictable[1]), recoveredState(predictable, false));
    final SplittableRandom split = new SplittableRandom(17);
    for (int i = 0; i < predictable.length; i++) {
      predictable[i] = split.nextLong();
    }
    assertEquals(unmixStafford(predictable[1]), recoveredState(predictable, true));
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment header = arena.allocate(32);
      final long[] salts = new long[4];
      for (int i = 0; i < salts.length; i++) {
        header.set(LE.LONG, 24, 0L);
        if (disposable) {
          salts[i] = PageLayout.saltForWrite(header, true);
          assertEquals(0L, header.get(LE.LONG, 24));
        } else {
          PageLayout.initializeSalt(header, true);
          salts[i] = header.get(LE.LONG, 24);
        }
      }
      assertNotEquals(unmixMurmur(salts[1]), recoveredState(salts, false));
      assertNotEquals(unmixStafford(salts[1]), recoveredState(salts, true));
    }
  }

  private static long recoveredState(final long[] salts, final boolean stafford) {
    final long first = stafford
        ? unmixStafford(salts[0])
        : unmixMurmur(salts[0]);
    final long third = stafford
        ? unmixStafford(salts[2])
        : unmixMurmur(salts[2]);
    final long fourth = stafford
        ? unmixStafford(salts[3])
        : unmixMurmur(salts[3]);
    return first + (fourth - third);
  }

  private static long unmixMurmur(long value) {
    value = (value ^ (value >>> 33)) * 0x9cb4b2f8129337dbL;
    value = (value ^ (value >>> 33)) * 0x4f74430c22a54005L;
    return value ^ (value >>> 33);
  }

  private static long unmixStafford(long value) {
    value = (value ^ (value >>> 31) ^ (value >>> 62)) * 0x319642b2d24d8ec3L;
    value = (value ^ (value >>> 27) ^ (value >>> 54)) * 0x96de1b173f119089L;
    return value ^ (value >>> 30) ^ (value >>> 60);
  }

  @Test
  void zeroDrawUsesOneWithoutDiscardingOtherSaltBits() {
    assertEquals(1L, PageLayout.nonzeroSalt(0L));
    for (final long value : new long[] {1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
      assertEquals(value, PageLayout.nonzeroSalt(value));
    }
  }

  @Test
  void disposableEncodingSaltsOutputWithoutMutatingTheSourceHeader() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("disposable").build();
    try (final KeyValueLeafPage page = newPage(config); final BytesOut<?> output = Bytes.elasticHeapByteBuffer()) {
      PageKind.KEYVALUELEAFPAGE.serializeDisposablePage(config, output, page, SerializationType.DATA);
      assertNotEquals(0L, salt(output.toByteArray()));
      assertEquals(0L, page.getSlottedPage().get(LE.LONG, 24),
          "the caller may still refuse the disposable encoding after serialization returns");
      try (final KeyValueLeafPage read = deserialize(config, output.toByteArray(), false)) {
        assertArrayEquals(RECORD, read.getSlotAsByteArray(7));
      }
      try (final KeyValueLeafPage ordinary = newPage(config)) {
        final byte[] expected = serialize(config, ordinary);
        final byte[] actual = output.toByteArray();
        Arrays.fill(expected, SALT_START, SALT_START + 8, (byte) 0);
        Arrays.fill(actual, SALT_START, SALT_START + 8, (byte) 0);
        assertArrayEquals(expected, actual, "disposable encoding must preserve every byte outside the salt");
      }
    }
  }

  @Test
  void independentPagesAreSaltedAndRepeatedWritesKeepTheirBytes() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("salt").build();
    final HashSet<Long> salts = new HashSet<>();
    for (int i = 0; i < 64; i++) {
      try (final KeyValueLeafPage page = newPage(config)) {
        final byte[] first = serialize(config, page);
        final long salt = salt(first);
        assertNotEquals(0L, salt);
        assertTrue(salts.add(salt), "independent pages must not reuse salts");
        assertArrayEquals(first, serialize(config, page));
        try (final KeyValueLeafPage copy = page.deepCopy()) {
          assertEquals(salt, salt(serialize(config, copy)));
        }
        try (final KeyValueLeafPage reloaded = deserialize(config, first, false)) {
          assertArrayEquals(RECORD, reloaded.getSlotAsByteArray(7));
          assertEquals(salt, salt(serialize(config, reloaded)));
        }
      }
    }
  }

  @Test
  void deterministicModeChangesOnlyTheEightSaltBytes() {
    final ResourceConfiguration random = ResourceConfiguration.newBuilder("random").build();
    final ResourceConfiguration deterministic =
        ResourceConfiguration.newBuilder("fixture").saltRecordPages(false).build();
    try (final KeyValueLeafPage page = newPage(random); final KeyValueLeafPage fixture = newPage(deterministic)) {
      final byte[] salted = serialize(random, page);
      final byte[] expected = serialize(deterministic, fixture);
      assertEquals(0L, salt(expected));
      assertNotEquals(0L, salt(salted));
      Arrays.fill(salted, SALT_START, SALT_START + 8, (byte) 0);
      assertArrayEquals(expected, salted);
      // The setting applies even to a copied or deserialized header that already carries a salt.
      try (final KeyValueLeafPage copy = page.deepCopy()) {
        assertArrayEquals(expected, serialize(deterministic, copy));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void arbitrarySaltBitsAreIgnoredByEagerLazyAndProbeReaders(final boolean chunked) {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunked);
    try {
      assertArbitrarySaltBitsAreIgnored();
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }

  private static void assertArbitrarySaltBitsAreIgnored() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("fixture").saltRecordPages(false).build();
    final Random random = new Random(0x5a17edL);
    try (final KeyValueLeafPage page = newPage(config)) {
      final byte[] wire = serialize(config, page);
      for (int iteration = 0; iteration < 128; iteration++) {
        final long bits = switch (iteration) {
          case 0 -> 0L;
          case 1 -> -1L;
          case 2 -> Long.MIN_VALUE;
          case 3 -> Long.MAX_VALUE;
          default -> random.nextLong();
        };
        MemorySegment.ofArray(wire).set(LE.LONG, SALT_START, bits);
        for (final boolean lazy : new boolean[] {false, true}) {
          try (final KeyValueLeafPage read = deserialize(config, wire, lazy)) {
            assertEquals(1, read.getPageKey());
            assertEquals(3, read.getRevision());
            assertArrayEquals(RECORD, read.getSlotAsByteArray(7));
          }
        }
        {
          final BytesIn<?> input = Bytes.wrapForRead(wire);
          input.readByte();
          final long[] metadata = new long[5];
          final long[] bitmap = new long[16];
          PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(input, metadata, bitmap);
          assertEquals(1, metadata[0]);
          assertEquals(3, metadata[1]);
          assertEquals(1, metadata[2]);
          assertEquals(1L << 7, bitmap[0]);
        }
      }
    }
  }

  @Test
  void resetCreatesANewSaltWhileClearingTheOldPage() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("salt").build();
    try (final KeyValueLeafPage page = newPage(config)) {
      final long first = salt(serialize(config, page));
      page.reset();
      page.setSlot(RECORD, 7);
      final long second = salt(serialize(config, page));
      assertNotEquals(0L, second);
      assertNotEquals(first, second);
    }
  }

  @Test
  void initializationPreservesEveryExistingNonzeroSaltAndDeterministicModeClearsIt() {
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment header = arena.allocate(32);
      for (final long value : new long[] {1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
        header.set(LE.LONG, 24, value);
        PageLayout.initializeSalt(header, true);
        assertEquals(value, header.get(LE.LONG, 24));
        PageLayout.initializeSalt(header, false);
        assertEquals(0L, header.get(LE.LONG, 24));
      }
      PageLayout.initializeSalt(header, true);
      assertNotEquals(0L, header.get(LE.LONG, 24));
    }
  }

  private static KeyValueLeafPage newPage(final ResourceConfiguration config) {
    final KeyValueLeafPage page = new KeyValueLeafPage(1, IndexType.DOCUMENT, config, 3, null, null);
    page.setSlot(RECORD, 7);
    return page;
  }

  private static long salt(final byte[] wire) {
    return MemorySegment.ofArray(wire).get(LE.LONG, SALT_START);
  }

  private static byte[] serialize(final ResourceConfiguration config, final KeyValueLeafPage page) {
    try (final BytesOut<?> output = Bytes.elasticHeapByteBuffer()) {
      PageKind.KEYVALUELEAFPAGE.serializePage(config, output, page, SerializationType.DATA);
      return output.toByteArray();
    }
  }

  private static KeyValueLeafPage deserialize(final ResourceConfiguration config, final byte[] wire,
      final boolean lazy) {
    {
      final BytesIn<?> input = Bytes.wrapForRead(wire);
      input.readByte();
      return (KeyValueLeafPage) (lazy
          ? PageKind.KEYVALUELEAFPAGE.deserializePageLazily(config, input, SerializationType.DATA, null)
          : PageKind.KEYVALUELEAFPAGE.deserializePage(config, input, SerializationType.DATA));
    }
  }
}

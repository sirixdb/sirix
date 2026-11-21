package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.settings.VersioningType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.LE;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Salt is an opaque 8-byte part of the header, never a reader discriminator. */
final class PageKindTest {
  // kind/version/flags + one-byte page key + revision + index type; independent wire-layout oracle.
  private static final int HEADER_START = 9;
  private static final int SALT_START = HEADER_START + 24;
  private static final byte[] RECORD = {1, 2, 3};

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aSlotThatRunsPastTheUsedBytesIsRefusedNotWritten(final boolean allowPackedBody) {
    Allocators.getInstance().init(1L << 30);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("corrupt-directory").build();
    try (Arena arena = Arena.ofConfined(); BytesOut<?> sink = Bytes.elasticHeapByteBuffer()) {
      final MemorySegment slots = arena.allocate(HOTLeafPage.DEFAULT_SIZE);
      // Two six-byte slots at offsets 0 and 8, so the second one ends at byte 14.
      writeSixByteSlot(slots, 0, (byte) 1, (byte) 42);
      writeSixByteSlot(slots, 8, (byte) 2, (byte) 43);
      final char[] offsets = new char[HOTLeafPage.MAX_ENTRIES];
      offsets[1] = 8;
      // The leaf claims twelve used bytes, exactly the sum of both slots. Writing "twelve bytes, already
      // dense" would publish a page whose second entry points two bytes past its own payload.
      try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION, slots, null, offsets, 2, 12)) {
        assertThrows(IllegalStateException.class, () -> PageKind.serializeHOTLeaf(config, sink, page, allowPackedBody));
        final long position = sink.position();
        try (HOTLeafPage older = new HOTLeafPage(1, 0, IndexType.PROJECTION)) {
          page.setCompletePageRef(older);
          page.markEntryDirty(1);
          final ResourceConfiguration sliding =
              ResourceConfiguration.newBuilder("corrupt-sparse-directory")
                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                   .build();
          assertThrows(IllegalStateException.class,
              () -> PageKind.serializeHOTLeaf(sliding, sink, page, allowPackedBody));
          if (allowPackedBody) {
            assertEquals(0, position, "packed selection must reject the source before writing its envelope");
            assertEquals(0, sink.position());
          }
        }
      }
    }
  }

  /**
   * One slot as the leaf stores it: suffix length, suffix, value length, value; lengths
   * little-endian.
   */
  private static void writeSixByteSlot(final MemorySegment slots, final long offset, final byte suffix,
      final byte value) {
    slots.set(ValueLayout.JAVA_BYTE, offset, (byte) 1);
    slots.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) 0);
    slots.set(ValueLayout.JAVA_BYTE, offset + 2, suffix);
    slots.set(ValueLayout.JAVA_BYTE, offset + 3, (byte) 1);
    slots.set(ValueLayout.JAVA_BYTE, offset + 4, (byte) 0);
    slots.set(ValueLayout.JAVA_BYTE, offset + 5, value);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void hotWriterModesPreservePayloadAndSideReferences(final boolean allowPackedBody) {
    Allocators.getInstance().init(1L << 30);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("writer-modes").build();
    final byte[] value = new byte[256];
    try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        BytesOut<?> sink = Bytes.elasticHeapByteBuffer()) {
      assertTrue(page.put(new byte[] {7}, value));
      page.setPageReference(7, new PageReference().setKey(123));
      PageKind.serializeHOTLeaf(config, sink, page, allowPackedBody);
      final byte[] wire = sink.toByteArray();
      assertEquals(HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS, wire[2] & HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS);
      assertEquals(HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS, wire[2] & HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS);
      final ByteArrayBytesIn source = new ByteArrayBytesIn(wire);
      source.position(1);
      try (HOTLeafPage decoded =
          (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, source, SerializationType.DATA)) {
        assertArrayEquals(value, decoded.getValue(decoded.findEntry(new byte[] {7})));
        assertEquals(123, decoded.getPageReference(7).getKey());
        assertEquals(0, source.remaining());
      }
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void optionalExtensionsPreserveSaltAndRecordsWhileRequiredEntriesRefuse(final boolean chunked) {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunked);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("salted-extensions").build();
    try (final KeyValueLeafPage page = newPage(config)) {
      final byte[] original = serialize(config, page);
      final long originalSalt = salt(original);
      assertNotEquals(0L, originalSalt);
      // Independent extension-block vector: two entry bytes, optional tag 62, zero payload.
      final byte[] extended = new byte[original.length + 3];
      System.arraycopy(original, 0, extended, 0, 3);
      extended[2] |= (byte) 0x80;
      extended[3] = 2;
      extended[4] = 62;
      extended[5] = 0;
      System.arraycopy(original, 3, extended, 6, original.length - 3);
      for (final boolean lazy : new boolean[] {false, true}) {
        try (final KeyValueLeafPage read = deserialize(config, extended, lazy)) {
          assertArrayEquals(RECORD, read.getSlotAsByteArray(7));
          assertEquals(originalSalt, read.getSlottedPage().get(LE.LONG, 24));
          assertArrayEquals(original, serialize(config, read), "rewrite drops the unknown entry and retains salt");
        }
      }
      final BytesIn<?> originalInput = Bytes.wrapForRead(original);
      originalInput.readByte();
      final long originalOffset = PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(originalInput, new long[5], null);
      final BytesIn<?> extendedInput = Bytes.wrapForRead(extended);
      extendedInput.readByte();
      assertEquals(originalOffset + 3,
          PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(extendedInput, new long[5], null));
      extended[4] = (byte) 190;
      for (final boolean lazy : new boolean[] {false, true}) {
        assertTrue(assertThrows(IllegalStateException.class,
            () -> deserialize(config, extended, lazy)).getMessage().contains("required page extension 190"));
      }
      final BytesIn<?> requiredInput = Bytes.wrapForRead(extended);
      requiredInput.readByte();
      assertThrows(IllegalStateException.class,
          () -> PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(requiredInput, new long[5], null));
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

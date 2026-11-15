package io.sirix.page;

import io.sirix.node.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreFeaturesTest {
  private static final HexFormat HEX = HexFormat.of();

  @Test
  void zeroFeaturesKeepTheLegacyBytes() throws IOException {
    assertArrayEquals(HEX.parseHex("03000001000000"), encode(new UberPage(1)));
    assertEquals(StoreFeatures.NONE, decode(HEX.parseHex("03000001000000")).getFeatures());
  }

  @Test
  void featureWordsHaveIndependentLittleEndianBytePin() throws IOException {
    final var features = new StoreFeatures(0x12345678, 0x80000000, 0x01020304);
    final byte[] fixture = HEX.parseHex("0300800ec10c78563412000000800403020107000000");
    assertArrayEquals(fixture, encode(new UberPage(7, features)));
    final UberPage decoded = decode(fixture);
    assertEquals(7, decoded.getRevisionCount());
    assertEquals(features, decoded.getFeatures());
  }

  @Test
  void serializingMarksBothKindsOfBootstrapCommitted() throws IOException {
    final var empty = new UberPage();
    encode(empty);
    assertFalse(empty.isBootstrap());
    final var active = new UberPage();
    active.addFeatures(new StoreFeatures(0, 0, 1));
    encode(active);
    assertFalse(active.isBootstrap());
  }

  @Test
  void everyUnknownBitFollowsTheAccessMatrix() {
    for (int bit = 0; bit < Integer.SIZE; bit++) {
      final int mask = 1 << bit;
      final var incompat = new StoreFeatures(mask, 0, 0);
      final var readOnly = new StoreFeatures(0, mask, 0);
      final var compat = new StoreFeatures(0, 0, mask);
      assertFalse(incompat.isEmpty());
      assertFalse(readOnly.isEmpty());
      assertFalse(compat.isEmpty());
      assertThrows(UnsupportedOperationException.class, incompat::requireReadable);
      assertThrows(UnsupportedOperationException.class, incompat::requireWritable);
      assertThrows(UnsupportedOperationException.class, incompat::requireToolCompatible);
      assertDoesNotThrow(readOnly::requireReadable);
      assertThrows(UnsupportedOperationException.class, readOnly::requireWritable);
      assertThrows(UnsupportedOperationException.class, readOnly::requireToolCompatible);
      assertDoesNotThrow(compat::requireReadable);
      assertDoesNotThrow(compat::requireWritable);
      assertThrows(UnsupportedOperationException.class, compat::requireToolCompatible);
    }
    assertTrue(StoreFeatures.NONE.isEmpty());
    assertDoesNotThrow(StoreFeatures.NONE::requireReadable);
    assertDoesNotThrow(StoreFeatures.NONE::requireWritable);
    assertDoesNotThrow(StoreFeatures.NONE::requireToolCompatible);
    final var failure =
        assertThrows(UnsupportedOperationException.class, new StoreFeatures(0, Integer.MIN_VALUE, 0)::requireWritable);
    assertTrue(failure.getMessage().contains("readOnlyCompat"));
    assertTrue(failure.getMessage().contains("0x80000000"));
    assertTrue(failure.getMessage().contains("upgrade"));
  }

  @Test
  void cloningAndTruncationPreserveWordsAndStagingDoesNotMutateCommittedState() {
    final var committed = new UberPage(4, new StoreFeatures(1, 2, 4));
    final var pending = new UberPage(committed);
    pending.addFeatures(new StoreFeatures(8, 16, 32));
    assertEquals(new StoreFeatures(9, 18, 36), pending.getFeatures());
    pending.addFeatures(new StoreFeatures(9, 18, 36));
    assertEquals(new StoreFeatures(9, 18, 36), pending.getFeatures(), "activation is idempotent");
    assertEquals(new StoreFeatures(1, 2, 4), committed.getFeatures());
    assertEquals(5, pending.getRevisionCount());
    assertThrows(IllegalArgumentException.class, () -> committed.withRevisionCount(0));
    assertThrows(IllegalArgumentException.class, () -> committed.withRevisionCount(-1));
    assertEquals(1, committed.withRevisionCount(1).getRevisionCount());
    final var truncated = committed.withRevisionCount(2);
    assertEquals(2, truncated.getRevisionCount());
    assertEquals(committed.getFeatures(), truncated.getFeatures());
    final var bootstrap = new UberPage();
    bootstrap.addFeatures(new StoreFeatures(1, 2, 4));
    assertEquals(bootstrap.getFeatures(), new UberPage(bootstrap).getFeatures());
  }

  @Test
  void truncatedAndMalformedFeaturePayloadsAreRejected() throws IOException {
    final byte[] valid = HEX.parseHex("0300800ec10c01000000020000000400000001000000");
    for (int length = 0; length < valid.length; length++) {
      final byte[] truncated = Arrays.copyOf(valid, length);
      assertThrows(RuntimeException.class, () -> decode(truncated), "length " + length);
    }
    for (int length = 0; length < 256; length++) {
      if (length == 12) {
        continue;
      }
      final byte[] corrupt = valid.clone();
      corrupt[5] = (byte) length;
      assertThrows(RuntimeException.class, () -> decode(corrupt), "payload length " + length);
    }
  }

  @Test
  void extensionBoundsAndTagsAreCheckedBeforeReadingTheBody() throws IOException {
    assertEquals(StoreFeatures.NONE, decode(HEX.parseHex("030080030101ab01000000")).getFeatures());
    for (final String malformed : new String[] {"03000101000000", // unknown flag
        "0301800001000000", // unknown version
        "03008002810001000000", // unknown required tag
        "030080040100010001000000", // repeated optional tag
        "030080040200010001000000", // descending tags
        "030080010101000000", // tag without a length
        "03008002018001000000", // continuation must not consume revision bytes
        "0300807f01000000", // block extends beyond page
        "030080ffffffffffffffffff0100000000" // overflowing length
    }) {
      assertThrows(RuntimeException.class, () -> decode(HEX.parseHex(malformed)), malformed);
    }
  }

  @Test
  void multiByteLengthsCanSkipOptionalEntriesBeforeFeatures() throws IOException {
    // 145 = one tag + two length bytes + 128 payload bytes + the 14-byte feature entry.
    final ByteBuffer fixture = ByteBuffer.allocate(154).order(ByteOrder.LITTLE_ENDIAN);
    fixture.put(new byte[] {3, 0, (byte) 0x80, (byte) 0x91, 1, 1, (byte) 0x80, 1});
    fixture.put(new byte[128]);
    fixture.put((byte) 0xc1).put((byte) 12).putInt(1).putInt(2).putInt(4).putInt(7);
    final var decoded = decode(fixture.array());
    assertEquals(7, decoded.getRevisionCount());
    assertEquals(new StoreFeatures(1, 2, 4), decoded.getFeatures());
  }

  @Test
  void malformedVarintNeverReadsOutsideItsDeclaredBlock() throws IOException {
    final byte[] bytes = HEX.parseHex("03008002018001000000");
    final var source = Bytes.wrapForRead(bytes);
    assertThrows(IllegalStateException.class,
        () -> new PagePersister().deserializePage(null, source, SerializationType.DATA));
    assertEquals(6, source.position(), "the four-byte revision body must remain unread");
  }

  @Test
  void invalidLengthsAreRejectedBeforeConsumingRevisionBodyBytes() {
    final String[] fixtures = {"0300808000000000", // unterminated block length before the four-byte revision body
        "0300800201000000", // declared block leaves fewer than four revision bytes
        "03008002010201000000" // optional payload claims bytes beyond its enclosing block
    };
    final long[] rejectionPositions = {4, 4, 6};
    for (int i = 0; i < fixtures.length; i++) {
      final var source = Bytes.wrapForRead(HEX.parseHex(fixtures[i]));
      assertThrows(IllegalStateException.class,
          () -> new PagePersister().deserializePage(null, source, SerializationType.DATA));
      assertEquals(rejectionPositions[i], source.position(), fixtures[i]);
    }
  }

  @Test
  void seededWordsRoundTripIncludingUnsignedHighBits() throws IOException {
    final var random = new Random(0x51F17E);
    for (int i = 0; i < 1000; i++) {
      final var features = new StoreFeatures(random.nextInt(), random.nextInt(), random.nextInt());
      final int revisions = 1 + random.nextInt(100000);
      final var decoded = decode(encode(new UberPage(revisions, features)));
      assertEquals(features, decoded.getFeatures());
      assertEquals(revisions, decoded.getRevisionCount());
    }
  }

  private static byte[] encode(final UberPage page) throws IOException {
    try (final var sink = Bytes.elasticHeapByteBuffer()) {
      new PagePersister().serializePage(null, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    }
  }

  private static UberPage decode(final byte[] bytes) throws IOException {
    return (UberPage) new PagePersister().deserializePage(null, Bytes.wrapForRead(bytes), SerializationType.DATA);
  }
}

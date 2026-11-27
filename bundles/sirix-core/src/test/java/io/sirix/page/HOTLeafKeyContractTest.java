package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FMT-212/213: physical prefix boundaries cannot change any logical key accessor. */
final class HOTLeafKeyContractTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("key-contract").versioningApproach(VersioningType.FULL).build();

  @Test
  void everyPagePrefixBoundaryPreservesKeyWordsAndDistinguishingBits() throws Exception {
    for (int prefix = 0; prefix <= Long.BYTES; prefix++) {
      final byte[][] keys = new byte[prefix == Long.BYTES
          ? 1
          : 3][Long.BYTES];
      try (HOTLeafPage source = new HOTLeafPage(7, 1, IndexType.PATH);
          MemorySegmentBytesOut output = new MemorySegmentBytesOut()) {
        for (int i = 0; i < keys.length; i++) {
          for (int pos = 0; pos < Long.BYTES; pos++) {
            keys[i][pos] = (byte) (0x81 + pos * 7);
          }
          if (prefix < Long.BYTES) {
            keys[i][prefix] = (byte) (0x40 + i);
          }
          assertTrue(source.put(keys[i], new byte[] {(byte) (42 + i)}));
        }
        PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, output, source, SerializationType.DATA);
        final byte[] frame = output.toByteArray();
        final ByteArrayBytesIn fragmentInput = new ByteArrayBytesIn(frame);
        fragmentInput.position(1);
        try (
            HOTLeafPage packed = (HOTLeafPage) new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(frame),
                SerializationType.DATA);
            HOTLeafPage compact = PageKind.deserializeHOTLeafFragment(fragmentInput);
            HOTLeafPage mutable = packed.copyForRead()) {
          assertTrue(packed.hasPackedBody(), "the fixture must exercise the packed representation");
          for (final HOTLeafPage page : new HOTLeafPage[] {source, packed, compact, mutable}) {
            assertLogicalKeys(page, keys);
          }
        }
      }
    }
  }

  private static void assertLogicalKeys(final HOTLeafPage page, final byte[][] keys) {
    for (int i = 0; i < keys.length; i++) {
      final byte[] key = keys[i];
      assertArrayEquals(key, page.getKey(i));
      assertEquals(i, page.findEntry(key));
      assertEquals(Long.BYTES, page.getKeyLength(i));
      assertEquals(ByteBuffer.wrap(key).getLong(), page.decodeKey8BE(i));
      for (int pos = 0; pos <= key.length - Integer.BYTES; pos++) {
        assertEquals(ByteBuffer.wrap(key).getInt(pos), page.readKeyIntBE(i, pos));
      }
      assertDistinguishingBits(page, i, key);
      final int index = i;
      assertThrows(IllegalStateException.class, () -> page.msdbWith(index, key));
      assertThrows(IndexOutOfBoundsException.class, () -> page.readKeyIntBE(index, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> page.readKeyIntBE(index, Long.BYTES - 3));
    }
  }

  private static void assertDistinguishingBits(final HOTLeafPage page, final int index, final byte[] key) {
    // Oracle: changing exactly bit b in byte p puts the first difference at 8*p+b (MSB first).
    for (int pos = 0; pos < key.length; pos++) {
      for (int bit = 0; bit < Byte.SIZE; bit++) {
        final byte[] probe = key.clone();
        probe[pos] ^= (byte) (0x80 >>> bit);
        assertEquals(pos * Byte.SIZE + bit, page.msdbWith(index, probe));
        final byte[] padded = Arrays.copyOf(probe, probe.length + 3);
        Arrays.fill(padded, probe.length, padded.length, (byte) -1);
        assertEquals(pos * Byte.SIZE + bit, page.msdbWith(index, padded, probe.length));
      }
    }
    final byte[] longer = Arrays.copyOf(key, key.length + 1);
    longer[key.length] = 0x40;
    assertEquals(Long.SIZE + 1, page.msdbWith(index, longer), "missing key bytes are zero");
  }
}

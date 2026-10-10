package io.sirix.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HOTLeafPageKeyWordTest {
  @Test
  void readsEveryWordAcrossPrefixAndSuffixBoundaries() {
    final byte[] first = {(byte) 0x80, (byte) 0xFF, 0x12, 0x34, (byte) 0xCD, 0x56, 0x01, (byte) 0x89, (byte) 0xAB,
        (byte) 0xCD, (byte) 0xEF, 0x23};
    final byte[] second = first.clone();
    second[6] = 2;
    try (final HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.VALIDTIME)) {
      assertTrue(leaf.put(first, new byte[] {1}));
      assertTrue(leaf.put(second, new byte[] {2}));
      assertEquals(6, leaf.getCommonPrefixLen());
      for (int index = 0; index < 2; index++) {
        final ByteBuffer expected = ByteBuffer.wrap(index == 0
            ? first
            : second);
        for (int offset = 0; offset <= first.length - Integer.BYTES; offset++) {
          assertEquals(expected.getInt(offset), leaf.readKeyIntBE(index, offset), "word at " + offset);
        }
      }
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readKeyIntBE(-1, 0));
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readKeyIntBE(2, 0));
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readKeyIntBE(0, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readKeyIntBE(0, first.length - 3));
    }
  }
}
